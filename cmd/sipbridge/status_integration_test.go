package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sipbridge/internal/controlpb"
)

func TestValidStatusKeepsControlDuringBackendOutage(t *testing.T) {
	target := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if target == "" {
		t.Skip("disposable Redis required")
	}
	proxy := reserveStartupEndpoint(t)
	stopProxy := startStartupRedisProxy(t, proxy, target)
	var stopRestored func()
	t.Cleanup(func() {
		if stopRestored != nil {
			stopRestored()
		}
	})
	t.Setenv("SIPBRIDGE_STATE_TEST_REDIS", proxy)
	testStateStartupWithHook(t, "redis", false, false, "mtls", false, func(t *testing.T, controller *websocket.Conn, hello *pb.SessionHelloAck, public, admin string, config *tls.Config) {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		var mu sync.Mutex
		send := func(frame *pb.Envelope) error {
			frame.SessionId = hello.SessionId
			wire, err := protojson.Marshal(frame)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			return controller.Write(ctx, websocket.MessageText, wire)
		}
		replies := make(chan *pb.Envelope, 8)
		failures := make(chan error, 1)
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			for {
				_, wire, err := controller.Read(ctx)
				if err != nil {
					if ctx.Err() == nil {
						failures <- err
					}
					return
				}
				frame := new(pb.Envelope)
				if err = protojson.Unmarshal(wire, frame); err == nil {
					if ping := frame.GetPing(); ping != nil {
						err = send(&pb.Envelope{CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
					} else if request := frame.GetRequest(); request != nil {
						if request.Snapshot.ApplicationLeaseTerm != hello.ApplicationLease.Term {
							err = fmt.Errorf("fresh request changed the healthy controller term")
						} else {
							err = send(&pb.Envelope{CorrelationId: "deny-after-outage", RemainingBudgetMs: 1000, Payload: &pb.Envelope_RequestDecision{RequestDecision: &pb.RequestDecision{Context: request.Snapshot.Context, RequestId: request.RequestId, Sequence: request.Snapshot.CommandSequence + 1, Status: 403}}})
						}
					} else {
						select {
						case replies <- frame:
						case <-ctx.Done():
							return
						}
					}
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}()
		defer func() { cancel(); <-stopped }()
		await := func(correlation string) *pb.Envelope {
			t.Helper()
			for {
				select {
				case frame := <-replies:
					if frame.CorrelationId == correlation {
						return frame
					}
				case err := <-failures:
					t.Fatal("same control session closed", err)
				case <-ctx.Done():
					t.Fatal("control observation deadline", ctx.Err())
				}
			}
		}
		httpClient := &http.Client{Timeout: 200 * time.Millisecond}
		ready := func(want int) {
			t.Helper()
			for ctx.Err() == nil {
				response, err := httpClient.Get("http://" + admin + "/sip-readyz")
				if err == nil {
					response.Body.Close()
					if response.StatusCode == want {
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatal("readiness observation deadline", ctx.Err())
		}
		stopProxy()
		ready(http.StatusServiceUnavailable)
		if err := send(&pb.Envelope{CorrelationId: "outage-status", Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}}); err != nil {
			t.Fatal(err)
		}
		if err := send(&pb.Envelope{CorrelationId: "status-barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "status-barrier"}}}); err != nil {
			t.Fatal(err)
		}
		if frame := await("status-barrier"); frame.SessionId != hello.SessionId || frame.GetPong().GetNonce() != "status-barrier" {
			t.Fatal("Status did not preserve its exact authenticated session")
		}
		ready(http.StatusServiceUnavailable)
		command := &pb.Command{Context: &pb.ContextRef{Id: "outage-uac"}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "fixture", ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli())}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Network: &pb.NetworkTarget{Transport: "tls"}}}}
		if err := send(&pb.Envelope{CorrelationId: "fenced-uac", RemainingBudgetMs: 1000, Payload: &pb.Envelope_Command{Command: command}}); err != nil {
			t.Fatal(err)
		}
		result := await("fenced-uac").GetResult()
		if result == nil || result.Certainty != pb.OperationResult_UNKNOWN || !strings.HasPrefix(result.Error, "initial UAC worker binding:") || result.Snapshot != nil {
			t.Fatal("Status granted publication authority during outage", result)
		}
		stopRestored = newStartupRedisProxy(t, proxy, target)
		ready(http.StatusOK)
		stored := redis.NewClient(&redis.Options{Addr: target, MaxRetries: -1})
		defer stored.Close()
		exists, err := stored.Exists(ctx, prefixForStartup(hello.StateAuthority.Service)+"core:context:outage-uac").Result()
		if err != nil || exists != 0 {
			t.Fatal("fenced command published a context", err)
		}
		// A fresh INVITE verifies the same controller still decides shared work.
		// Its non-2xx ACK duty also supplies the retained binary context checked
		// by the outer fixture; REGISTER transactions now remain process-local.
		// The failed OpenUAC above was neither retried nor restored.
		phone, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", public, config.Clone())
		if err != nil {
			t.Fatal(err)
		}
		defer phone.Close()
		phone.SetDeadline(time.Now().Add(2 * time.Second))
		_, err = fmt.Fprint(phone, "INVITE sip:fixture.test SIP/2.0\r\nVia: SIP/2.0/TLS 127.0.0.1:5061;branch=z9hG4bK-after-outage;rport\r\nFrom: <sip:phone@fixture.test>;tag=phone\r\nTo: <sip:phone@fixture.test>\r\nContact: <sip:phone@127.0.0.1:5061;transport=tls>\r\nCall-ID: status-outage-invite\r\nCSeq: 1 INVITE\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n")
		if err != nil {
			t.Fatal(err)
		}
		line, err := bufio.NewReader(phone).ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "SIP/2.0 403 ") {
			t.Fatal("same controller could not decide fresh work after backend returned", line, err)
		}
	})
}
