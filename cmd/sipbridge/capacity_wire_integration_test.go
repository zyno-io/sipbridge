package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sipbridge/internal/controlpb"
)

func TestBinaryRedisTwoLeggedCallCapacity(t *testing.T) {
	const target = 512
	if os.Getenv("SIPBRIDGE_CAPACITY_WIRE") == "" {
		t.Skip("set SIPBRIDGE_CAPACITY_WIRE to qualify 512 two-legged calls")
	}
	t.Run("single", func(t *testing.T) {
		testStateStartupObservingRedis(t, "redis", false, false, "mtls", false, nil, func(t *testing.T, ws *websocket.Conn, hello *pb.SessionHelloAck, address, _ string, config *tls.Config, redis *redis.Client, root string) {
			runTwoLeggedCapacity(t, ws, hello, address, config, redis, root, target, nil)
		})
	})
	t.Run("shared", func(t *testing.T) {
		address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
		if address == "" {
			t.Skip("disposable Redis required")
		}
		namespace := "capacity-" + uuid.NewString()
		client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
		t.Cleanup(func() {
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			keys, err := client.Keys(ctx, prefixForStartup(namespace)+"*").Result()
			if err != nil {
				t.Error(err)
				return
			}
			if len(keys) > 0 {
				if err := client.Del(ctx, keys...).Err(); err != nil {
					t.Error(err)
				}
			}
		})
		var arrived atomic.Int32
		both := make(chan struct{})
		for _, instance := range []string{"capacity-a", "capacity-b"} {
			t.Run(instance, func(t *testing.T) {
				t.Parallel()
				domain := &startupRedisDomain{namespace: namespace, instance: instance}
				testStateStartupObservingRedis(t, "redis", false, false, "mtls", false, domain, func(t *testing.T, ws *websocket.Conn, hello *pb.SessionHelloAck, address, _ string, config *tls.Config, redis *redis.Client, root string) {
					verifyBoth := func(ctx context.Context) {
						t.Helper()
						if arrived.Add(1) == 2 {
							close(both)
						}
						select {
						case <-both:
						case <-time.After(40 * time.Second):
							t.Fatal("second instance did not reach the call target")
						}
						count, err := redis.HGet(ctx, root+"core:capacity", "contexts").Int()
						if err != nil || count < 4*target {
							t.Fatalf("shared namespace did not hold both instances' calls: %d %v", count, err)
						}
					}
					runTwoLeggedCapacity(t, ws, hello, address, config, redis, root, target, verifyBoth)
				})
			})
		}
	})
}

func runTwoLeggedCapacity(t *testing.T, ws *websocket.Conn, hello *pb.SessionHelloAck, address string, config *tls.Config, redis *redis.Client, root string, target int, verifyBoth func(context.Context)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	send := func(frame *pb.Envelope) error {
		frame.SessionId = hello.SessionId
		wire, err := protojson.Marshal(frame)
		if err != nil {
			return err
		}
		return ws.Write(ctx, websocket.MessageText, wire)
	}
	received := make(chan *pb.Envelope, 16)
	receiveError := make(chan error, 1)
	go func() {
		for {
			_, wire, err := ws.Read(ctx)
			if err != nil {
				receiveError <- err
				return
			}
			frame := &pb.Envelope{}
			if err := protojson.Unmarshal(wire, frame); err != nil {
				receiveError <- err
				return
			}
			var reply *pb.Envelope
			switch body := frame.Payload.(type) {
			case *pb.Envelope_Ping:
				reply = &pb.Envelope{Payload: &pb.Envelope_Pong{Pong: body.Ping}}
			case *pb.Envelope_DialogEvent:
				reply = &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}}
			}
			if reply != nil {
				reply.CorrelationId = frame.CorrelationId
				if err := send(reply); err != nil {
					receiveError <- err
					return
				}
				continue
			}
			select {
			case received <- frame:
			case <-ctx.Done():
				return
			}
		}
	}()
	next := func() *pb.Envelope {
		t.Helper()
		select {
		case frame := <-received:
			return frame
		case err := <-receiveError:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Second):
			t.Fatal("expected controller frame did not arrive")
		}
		return nil
	}
	emit := func(frame *pb.Envelope) {
		t.Helper()
		if err := send(frame); err != nil {
			t.Fatal(err)
		}
	}
	result := func(correlation string) {
		t.Helper()
		frame := next()
		answer := frame.GetResult()
		if frame.CorrelationId != correlation || answer == nil || answer.Error != "" || answer.Certainty != pb.OperationResult_ACCEPTED {
			t.Fatalf("operation %s failed: %v", correlation, frame)
		}
	}
	dial := func() (*tls.Conn, *bufio.Reader) {
		t.Helper()
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", address, config.Clone())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn, bufio.NewReader(conn)
	}
	write := func(conn *tls.Conn, wire []byte) {
		t.Helper()
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(wire); err != nil {
			t.Fatal(err)
		}
	}
	request := func(method string, i int, body string) []byte {
		content := ""
		if body != "" {
			content = "Content-Type: application/sdp\r\n"
		}
		return []byte(fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/TLS phone.example.test:5061;branch=z9hG4bK-capacity-%d;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: <sip:service@example.test>\r\nCall-ID: %s-%d\r\nCSeq: 1 %s\r\nContact: <sip:phone@phone.example.test:5061;transport=tls>\r\nMax-Forwards: 70\r\n%sContent-Length: %d\r\n\r\n%s", method, i, hello.Sipbridge.InstanceId, i, method, content, len(body), body))
	}
	decide := func(presentation *pb.RequestPresented, operation string, body []byte) {
		t.Helper()
		decision := &pb.RequestDecision{Context: presentation.Snapshot.Context, RequestId: presentation.RequestId, Sequence: 1,
			Admission: &pb.CommandAdmission{DirectoryEpoch: "capacity-fixture", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli())},
			Basis:     pb.OwnerIngest_AUTHENTICATED_BINDING, PrincipalId: "phone", BindingId: "binding", Status: 200, Body: body}
		if len(body) != 0 {
			decision.Headers = []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}
		}
		emit(&pb.Envelope{CorrelationId: operation, RemainingBudgetMs: 2000, Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}})
		result(operation)
	}
	callee, calleeReader := dial()
	write(callee, request("REGISTER", 0, ""))
	registration := next().GetRequest()
	if registration == nil || registration.Message.Method != "REGISTER" {
		t.Fatalf("callee registration missing: %v", registration)
	}
	decide(registration, "register", nil)
	requireResidentStatus(t, readResidentFinal(t, calleeReader, 1), 200)
	caller, callerReader := dial()
	const sdp = "v=0\r\no=- 1 1 IN IP4 192.0.2.1\r\ns=fixture\r\nc=IN IP4 192.0.2.1\r\nt=0 0\r\nm=audio 4000 RTP/AVP 0\r\n"
	type call struct {
		inbound, outbound string
		caller, callee    portable.Dialog
	}
	var admissionPrefix string
	saturatePending := func() func() {
		t.Helper()
		capacityKey := root + "core:capacity"
		field := admissionPrefix + ":pending"
		count, err := redis.HGet(ctx, capacityKey, field).Result()
		if err != nil || count != "1" {
			t.Fatalf("fixture requires exactly one pending decision: %s %v", count, err)
		}
		if err := redis.HSet(ctx, capacityKey, field, "128").Err(); err != nil {
			t.Fatal(err)
		}
		overflow, reader := dial()
		initial := request("INVITE", target*10, sdp)
		write(overflow, initial)
		response := readResidentFinal(t, reader, 1)
		requireResidentStatus(t, response, 503)
		ack, err := portable.BuildNon2xxACK(portable.RestoreMessage(initial, "tls", "", ""), portable.RestoreMessage(response, "tls", "", ""))
		if err != nil {
			t.Fatal(err)
		}
		write(overflow, ack.WireBytes())
		return func() {
			t.Helper()
			if err := redis.HSet(ctx, capacityKey, field, count).Err(); err != nil {
				t.Fatal(err)
			}
		}
	}
	open := func(index int) call {
		t.Helper()
		inviteWire := request("INVITE", index, sdp)
		write(caller, inviteWire)
		presentation := next().GetRequest()
		if presentation == nil || presentation.Message.Method != "INVITE" {
			t.Fatalf("initial call %d missing: %v", index, presentation)
		}
		inboundID := presentation.Snapshot.Context.Id
		if admissionPrefix == "" {
			scope, err := redis.HGet(ctx, root+"core:context:"+inboundID, "admission").Result()
			pod, _, ok := strings.Cut(scope, ":")
			if err != nil || !ok {
				t.Fatalf("admitting instance missing: %v", err)
			}
			admissionPrefix = "admission:" + pod
		}
		var restorePending func()
		if index == target {
			restorePending = saturatePending()
		}
		outboundID := hello.Sipbridge.InstanceId + "-outbound-" + strconv.Itoa(index)
		command := &pb.Command{Context: &pb.ContextRef{Id: outboundID}, Sequence: 1,
			Admission: &pb.CommandAdmission{DirectoryEpoch: "capacity-fixture", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), SelectedFlow: registration.Flow},
			Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@phone.example.test:5061;transport=tls", Transport: "tls",
				Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}, {Name: "Content-Type", Value: "application/sdp"}}, Body: []byte(sdp)}}}
		emit(&pb.Envelope{CorrelationId: outboundID, RemainingBudgetMs: 2000, Payload: &pb.Envelope_Command{Command: command}})
		if err := callee.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		outbound := portable.RestoreMessage(readResidentWire(t, calleeReader), "tls", "", "")
		result(outboundID)
		final, err := portable.BuildResponseWithHeaders(outbound, 200, "OK", []byte(sdp), "callee", "sip:phone@phone.example.test:5061;transport=tls", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
		if err != nil {
			t.Fatal(err)
		}
		write(callee, final.WireBytes())
		ack, err := sip.ParseMessage(readResidentWire(t, calleeReader))
		if err != nil || ack.CSeq().MethodName != sip.ACK {
			t.Fatalf("callee did not receive ACK: %v %v", ack, err)
		}
		if restorePending != nil {
			restorePending()
		}
		operation := "answer-" + strconv.Itoa(index)
		decide(presentation, operation, []byte(sdp))
		finalWire := readResidentFinal(t, callerReader, 1)
		requireResidentStatus(t, finalWire, 200)
		inDialog, err := portable.NewDialog(portable.UAC, portable.RestoreMessage(inviteWire, "tls", "", ""), portable.RestoreMessage(finalWire, "tls", "", ""), portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
		inACK, err := portable.BuildDialogRequest(inDialog, sip.ACK, 1, nil, "z9hG4bK-"+uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		write(caller, inACK.WireBytes())
		outDialog, err := portable.NewDialog(portable.UAS, outbound, final, portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
		return call{inbound: inboundID, outbound: outboundID, caller: inDialog, callee: outDialog}
	}
	end := func(pair call) {
		t.Helper()
		for _, leg := range []struct {
			conn   *tls.Conn
			reader *bufio.Reader
			dialog portable.Dialog
		}{{caller, callerReader, pair.caller}, {callee, calleeReader, pair.callee}} {
			bye, err := portable.BuildDialogRequest(leg.dialog, sip.BYE, 2, nil, "z9hG4bK-"+uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			write(leg.conn, bye.WireBytes())
			requireResidentStatus(t, readResidentFinal(t, leg.reader, 2), 200)
		}
	}
	calls := make([]call, 0, target)
	started := time.Now()
	for i := 1; i <= target; i++ {
		if i%128 == 0 {
			write(callee, request("REGISTER", target*2+i, ""))
			refresh := next().GetRequest()
			if refresh == nil || refresh.Message.Method != "REGISTER" {
				t.Fatalf("callee registration refresh missing: %v", refresh)
			}
			decide(refresh, "register-"+strconv.Itoa(i), nil)
			requireResidentStatus(t, readResidentFinal(t, calleeReader, 1), 200)
		}
		calls = append(calls, open(i))
		if verifyBoth != nil {
			time.Sleep(15 * time.Millisecond)
		}
	}
	t.Logf("established %d two-legged TLS calls through one process in %s", len(calls), time.Since(started))
	if verifyBoth != nil {
		verifyBoth(ctx)
	}
	for _, direction := range []string{"inbound", "outbound"} {
		count, err := redis.HGet(ctx, root+"core:capacity", admissionPrefix+":"+direction).Int()
		if err != nil || count != target {
			t.Fatalf("expected %d %s shared call legs, got %d %v", target, direction, count, err)
		}
	}
	var retained, credited, redisBytes, maxProofs int64
	keys := make([]string, 0, 2*len(calls))
	for _, pair := range calls {
		keys = append(keys, root+"core:context:"+pair.inbound, root+"core:context:"+pair.outbound)
	}
	for _, key := range keys {
		record, err := redis.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		retained += fixtureCapacityInt(t, record["bytes"])
		credited += fixtureCapacityInt(t, record["reserved"])
		size, err := redis.MemoryUsage(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		redisBytes += size
		var proofs int64
		for field := range record {
			if strings.HasPrefix(field, "o:") && strings.HasSuffix(field, ":a") {
				proofs++
			}
		}
		maxProofs = max(maxProofs, proofs)
	}
	t.Logf("held calls: %d selected record bytes, %d remaining completion credit, %d Redis context-hash allocation bytes, at most %d physical attempts per context observed", retained, credited, redisBytes, maxProofs)
	quietBy := time.Now().Add(36 * time.Second)
	for time.Now().Before(quietBy) {
		var remaining int64
		for _, key := range keys {
			credit, err := redis.HGet(ctx, key, "reserved").Int64()
			if err != nil {
				t.Fatal(err)
			}
			remaining += credit
		}
		if remaining < credited*9/10 {
			t.Logf("remaining completion credit after initial replay timers: %d bytes", remaining)
			break
		}
		if time.Until(quietBy) <= time.Second {
			t.Fatalf("initial replay retirement failed to release setup credit: held=%d current=%d", credited, remaining)
		}
		time.Sleep(time.Second)
	}
	for i := range min(16, len(calls)) {
		end(calls[i])
		calls[i] = open(target + i + 1)
	}
	for _, pair := range calls {
		end(pair)
	}
}

func fixtureCapacityInt(t *testing.T, text string) int64 {
	t.Helper()
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
