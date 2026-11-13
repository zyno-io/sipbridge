package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sipbridge/internal/controlpb"
)

// This exercises the production startup, control session, TLS ingress and the
// real Reserve/Complete socket writer. All identities and credentials are fixture-only.
func TestResidentMemoryProductionTLSCallAndCancellation(t *testing.T) {
	testStateStartupWithHook(t, "memory", false, false, "mtls", false, runResidentTLSCall)
}

func runResidentTLSCall(t *testing.T, controller *websocket.Conn, hello *pb.SessionHelloAck, address string, config *tls.Config) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
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
	present := make(chan uint32, 8)
	acknowledged := make(chan struct{}, 1)
	failures := make(chan error, 1)
	defer func() {
		if t.Failed() {
			select {
			case err := <-failures:
				t.Logf("fixture control failure: %v", err)
			default:
			}
		}
	}()
	stopped := make(chan struct{})
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	go func() {
		defer close(stopped)
		for {
			_, wire, err := controller.Read(ctx)
			if err != nil {
				if ctx.Err() == nil {
					fail(err)
				}
				return
			}
			frame := &pb.Envelope{}
			if err = protojson.Unmarshal(wire, frame); err != nil {
				fail(err)
				return
			}
			reply := func(payload *pb.Envelope) {
				payload.CorrelationId = frame.CorrelationId
				if err := send(payload); err != nil {
					fail(err)
				}
			}
			switch body := frame.Payload.(type) {
			case *pb.Envelope_Ingress:
				fail(fmt.Errorf("unexpected old ingress handshake: %T", body))
				return
			case *pb.Envelope_Request:
				request := body.Request
				parsed, parseErr := sip.ParseMessage(request.Message.Wire)
				if parseErr != nil {
					fail(parseErr)
					return
				}
				seq := parsed.CSeq().SeqNo
				present <- seq
				if parsed.CSeq().MethodName == sip.INVITE && seq == 3 {
					continue
				}
				decision := &pb.RequestDecision{Context: request.Snapshot.Context, RequestId: request.RequestId, Sequence: request.Snapshot.CommandSequence + 1,
					Admission: &pb.CommandAdmission{DirectoryEpoch: "fixture-directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), ExpectedDelivery: request.Snapshot.Delivery},
					Basis:     pb.OwnerIngest_AUTHENTICATED_BINDING, PrincipalId: "fixture-phone", BindingId: "fixture-binding", Status: 200}
				if proposal := request.FlowAdoption; proposal != nil {
					decision.Basis = pb.OwnerIngest_TLS_DIALOG_CONTINUITY
					decision.Delivery = &pb.ContextDelivery{Flow: proposal.Flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "fixture-directory", RegistrationTicket: "fixture"}
				}
				if err = send(&pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: 2000, Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}}); err != nil {
					fail(err)
					return
				}
			case *pb.Envelope_FlowAdoption:
				p := body.FlowAdoption
				reply(&pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: p.Flow, NativeRevision: p.NativeRevision, DirectoryEpoch: "fixture-directory", RegistrationTicket: "fixture"}}}})
			case *pb.Envelope_DialogEvent:
				reply(&pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				if body.DialogEvent.Kind == pb.DialogEvent_ACKNOWLEDGED {
					select {
					case acknowledged <- struct{}{}:
					default:
					}
				}
			case *pb.Envelope_Result:
				if body.Result.Error != "" {
					fail(fmt.Errorf("control result: %s", body.Result.Error))
				}
			case *pb.Envelope_Ping:
				reply(&pb.Envelope{Payload: &pb.Envelope_Pong{Pong: body.Ping}})
			}
		}
	}()
	defer func() { cancel(); <-stopped }()
	if err := send(&pb.Envelope{CorrelationId: uuid.NewString(), Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitCalls: true, AdmitSubscriptions: true}}}); err != nil {
		t.Fatal(err)
	}
	dial := func() *tls.Conn {
		t.Helper()
		dialer := &net.Dialer{Timeout: 2 * time.Second}
		c, err := tls.DialWithDialer(dialer, "tcp", address, config.Clone())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		_ = c.SetDeadline(time.Now().Add(8 * time.Second))
		return c
	}
	phone := dial()
	reader := bufio.NewReader(phone)
	initial := fmt.Sprintf("INVITE sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/TLS phone.example.test:5061;branch=z9hG4bK-resident-wire;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: <sip:service@example.test>\r\nCall-ID: resident-wire-%s\r\nCSeq: 1 INVITE\r\nContact: <sip:phone@phone.example.test:5061;transport=tls>\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n", uuid.NewString())
	if _, err := phone.Write([]byte(initial)); err != nil {
		t.Fatal(err)
	}
	final := readResidentFinal(t, reader, 1)
	origin := portable.Message{Wire: []byte(initial), Transport: "tls"}
	remote, err := portable.NewDialog(portable.UAC, origin, portable.Message{Wire: final, Transport: "tls"}, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-resident-ack")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = phone.Write(ack.Wire); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acknowledged:
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("initial ACK was not committed")
	}
	replacement := dial()
	replacementReader := bufio.NewReader(replacement)
	request := func(method sip.RequestMethod, seq uint32, branch string) portable.Message {
		m, err := portable.BuildDialogRequest(remote, method, seq, nil, branch)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	sendWire := func(m portable.Message) {
		if _, err := replacement.Write(m.Wire); err != nil {
			t.Fatal(err)
		}
	}
	reinvite := request(sip.INVITE, 2, "z9hG4bK-resident-reinvite")
	sendWire(reinvite)
	requireResidentStatus(t, readResidentFinal(t, replacementReader, 2), 200)
	sendWire(request(sip.ACK, 2, "z9hG4bK-resident-ack-two"))
	cancelled := request(sip.INVITE, 3, "z9hG4bK-resident-cancelled")
	sendWire(cancelled)
	for {
		select {
		case seq := <-present:
			if seq == 3 {
				goto presented
			}
		case err := <-failures:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("cancelled re-INVITE not presented")
		}
	}
presented:
	cancelRequest, err := portable.BuildCancel(cancelled)
	if err != nil {
		t.Fatal(err)
	}
	sendWire(cancelRequest)
	var rejected []byte
	statuses := map[int]bool{}
	for range 2 {
		wire := readResidentFinal(t, replacementReader, 3)
		parsed, err := sip.ParseMessage(wire)
		if err != nil {
			t.Fatal(err)
		}
		response := parsed.(*sip.Response)
		statuses[response.StatusCode] = true
		if response.StatusCode == 487 {
			rejected = wire
		}
	}
	if !statuses[200] || !statuses[487] {
		t.Fatalf("CANCEL statuses: %v", statuses)
	}
	cancelAck, err := portable.BuildNon2xxACK(cancelled, portable.Message{Wire: rejected, Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	sendWire(cancelAck)
	fresh := request(sip.INVITE, 4, "z9hG4bK-resident-fresh")
	sendWire(fresh)
	requireResidentStatus(t, readResidentFinal(t, replacementReader, 4), 200)
	sendWire(request(sip.ACK, 4, "z9hG4bK-resident-ack-four"))
	sendWire(request(sip.BYE, 5, "z9hG4bK-resident-bye"))
	requireResidentStatus(t, readResidentFinal(t, replacementReader, 5), 200)
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
}

func readResidentFinal(t *testing.T, reader *bufio.Reader, seq uint32) []byte {
	t.Helper()
	for {
		var header strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			header.WriteString(line)
			if line == "\r\n" {
				break
			}
		}
		length := 0
		for _, line := range strings.Split(header.String(), "\r\n") {
			name, value, ok := strings.Cut(line, ":")
			if ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
				length, _ = strconv.Atoi(strings.TrimSpace(value))
			}
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			t.Fatal(err)
		}
		wire := append([]byte(header.String()), body...)
		parsed, err := sip.ParseMessage(wire)
		if err != nil {
			t.Fatal(err)
		}
		response, ok := parsed.(*sip.Response)
		if !ok || response.CSeq().SeqNo != seq {
			t.Fatalf("unexpected SIP response: %s", wire)
		}
		if response.StatusCode >= 200 {
			return wire
		}
	}
}

func requireResidentStatus(t *testing.T, wire []byte, status int) {
	t.Helper()
	parsed, err := sip.ParseMessage(wire)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := parsed.(*sip.Response)
	if !ok || response.StatusCode != status {
		t.Fatalf("expected SIP %d, got %s", status, wire)
	}
}
