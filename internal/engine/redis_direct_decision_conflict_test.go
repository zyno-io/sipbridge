package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestRedisNativeDirectDecisionReselectsReadConflict(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int
	}{{"one-conflict", 12}, {"bounded-conflicts", 13}, {"uncertain-read", 14}, {"expired-owner", 15}, {"expired-policy", 16}, {"concurrent-acceptance", 17}} {
		t.Run(tc.name, func(t *testing.T) { testRedisNativeOpenUACCommand(t, tc.mode) })
	}
}

func TestRedisNativeDirectDecisionReselectsPublicationConflict(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int
	}{{"post-selection-read", 18}, {"prewrite-CAS", 19}, {"bounded-CAS", 20}, {"uncertain-publication", 21}, {"concurrent-publication", 22}} {
		t.Run(tc.name, func(t *testing.T) { testRedisNativeOpenUACCommand(t, tc.mode) })
	}
}

func testRedisDirectDecisionReadConflict(t *testing.T, ctx context.Context, e *redisNative, ws *websocket.Conn, hello *pb.SessionHelloAck, flow *pb.FlowRef, ref *pb.ContextRef, store *rediscore.Store, identity rediscore.Identity, origin, final portable.Message, ack []byte, dialogField, root string, proxy *redisReservationFaultProxy, mode int, written <-chan []byte) {
	t.Helper()
	read := func(match func(*pb.Envelope) bool) *pb.Envelope {
		for {
			value := readControllerEnvelope(t, ctx, ws)
			if ping := value.GetPing(); ping != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Pong{Pong: ping}})
				continue
			}
			if value.GetDialogEvent() != nil {
				writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: value.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
				if !match(value) {
					continue
				}
			}
			if !match(value) {
				t.Fatalf("unexpected direct-decision control frame %T", value.Payload)
			}
			return value
		}
	}
	read(func(value *pb.Envelope) bool { return value.GetDialogEvent().GetKind() == pb.DialogEvent_ANSWERED })
	peerDialog, err := portable.NewDialog(portable.UAS, origin, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	peerDialog, _, err = portable.StepDialog(peerDialog, portable.DialogInput{Kind: portable.DialogSendResponse, Message: final, Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	peerDialog, _, err = portable.StepDialog(peerDialog, portable.DialogInput{Kind: portable.DialogReceiveRequest, Message: portable.RestoreMessage(ack, "tls", "", ""), Now: time.Now().UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	request, err := portable.BuildDialogRequest(peerDialog, sip.REFER, 2, nil, "z9hG4bK-selected-read-conflict")
	if err != nil {
		t.Fatal(err)
	}
	request = portable.RestoreMessage(request.WireBytes(), "tls", "192.0.2.12:5061", "")
	view, err := store.Read(ctx, identity, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	peer, err := e.control.Peer(current.Application)
	if err != nil {
		t.Fatal(err)
	}
	message, err := projectNativeMessage(request, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.ReceiveRequest(ctx, peer, flow, message); err != nil {
		t.Fatal(err)
	}
	presented := read(func(value *pb.Envelope) bool { return value.GetRequest() != nil }).GetRequest()

	admin := redis.NewClient(&redis.Options{Addr: proxy.upstream, MaxRetries: -1})
	defer admin.Close()
	profile, err := admin.Get(ctx, root+"core:profile").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	info, err := admin.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, found := strings.CutPrefix(line, "run_id:"); found {
			primary = value
		}
	}
	independent, err := rediscore.New(admin, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	decision := &pb.RequestDecision{Context: ref, RequestId: presented.RequestId, Sequence: 2, Basis: pb.OwnerIngest_EXISTING_FLOW, Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), ExpectedDelivery: cloneResidentDelivery(presented.Snapshot.Delivery)}}
	if mode >= 18 {
		decision.Status = 202
	}
	if mode == 16 {
		decision.Admission.ExpiresAtMs = uint64(time.Now().Add(-time.Second).UnixMilli())
	}
	var hits atomic.Int32
	proxy.mu.Lock()
	proxy.beforeCommand = func(command redisFaultFrame) error {
		if command.kind != '*' || len(command.children) < 3 {
			return nil
		}
		args := command.children
		if string(args[0].scalar) != "evalsha" && string(args[0].scalar) != "eval" {
			return nil
		}
		postSelection := mode == 18
		publication := mode >= 19
		if publication {
			if len(args) < 30 || string(args[3].scalar) != root+"core:profile" || string(args[4].scalar) != root+"core:context:"+identity.Context {
				return nil
			}
			receipt := false
			for _, arg := range args {
				receipt = receipt || string(arg.scalar) == "r:result-2"
			}
			if !receipt {
				return nil
			}
		} else {
			// A selected read ends with mode, owner, lease term, attachment, and primary.
			if len(args) != 15 || string(args[2].scalar) != "2" || string(args[len(args)-5].scalar) != "exact" {
				return nil
			}
			owner := args[len(args)-4].scalar
			if postSelection {
				if len(owner) != 0 || !strings.HasPrefix(string(args[9].scalar), "r:uac-procedure-") {
					return nil
				}
			} else if len(owner) == 0 {
				return nil
			}
		}
		hasContext, hasDialog := false, false
		for _, arg := range args[3:] {
			hasContext = hasContext || string(arg.scalar) == root+"core:context:"+identity.Context
			hasDialog = hasDialog || string(arg.scalar) == dialogField
		}
		if !hasContext || !publication && !postSelection && !hasDialog {
			return nil
		}
		n := hits.Add(1)
		if mode != 13 && mode != 20 && n != 1 {
			return nil
		}
		if mode == 14 || mode == 21 {
			return errors.New("fixture interrupted selected read")
		}
		if mode == 15 {
			return independent.ExpireApplication(ctx, rediscore.ApplicationLeaseRef{Application: identity.Owner, Term: identity.Term})
		}
		if mode == 17 || mode == 22 {
			result, x := e.decideDirect(ctx, peer, decision, "concurrent-decision", time.Now().Add(time.Second))
			if x != nil {
				return x
			}
			if result == nil || result.Certainty != pb.OperationResult_ACCEPTED {
				return errors.New("fixture concurrent decision was not accepted")
			}
			return nil
		}
		selected, x := independent.Read(ctx, identity, "r:context")
		if x != nil {
			return x
		}
		actual, x := decodeNativeContext(selected.Records["r:context"])
		if x != nil {
			return x
		}
		actual.Revision++
		data, x := encodeNativeContext(actual)
		if x != nil {
			return x
		}
		_, x = independent.Commit(ctx, rediscore.Publication{Identity: identity, Revision: selected.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: data}}})
		return x
	}
	proxy.mu.Unlock()
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "selected-read-conflict", RemainingBudgetMs: 2000, Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}})
	result := read(func(value *pb.Envelope) bool { return value.CorrelationId == "selected-read-conflict" }).GetResult()
	after, err := independent.Read(ctx, identity, "r:context", "r:result-2")
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeNativeContext(after.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	if mode == 18 || mode == 19 || mode == 22 {
		select {
		case wire := <-written:
			message := portable.RestoreMessage(wire, flow.Transport, "", "")
			facts, x := message.Facts()
			if x != nil || facts.IsRequest || facts.Status != 202 || facts.CSeqMethod != sip.REFER {
				t.Fatal("decision did not write its one selected final", x)
			}
		case <-ctx.Done():
			t.Fatal("accepted response not written", ctx.Err())
		}
		// The synthetic writer exposes bytes before Complete returns. Await
		// actual returned-consumer acknowledgment before fixture teardown.
		outputField := "r:dialog-output-" + presented.RequestId + "-2"
		outputView, x := independent.Read(ctx, identity, outputField)
		if x != nil {
			t.Fatal(x)
		}
		output, x := decodeNativeOutput(outputView.Records[outputField])
		if x != nil {
			t.Fatal(x)
		}
		outputRef, x := nativeOutputRef(output.Write)
		if x != nil {
			t.Fatal(x)
		}
		for {
			_, x = independent.InspectOutput(ctx, outputRef)
			if errors.Is(x, rediscore.ErrAbsent) {
				break
			}
			if x != nil {
				t.Fatal(x)
			}
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				t.Fatal("response consumer did not acknowledge", ctx.Err())
			}
		}

	}
	if mode != 12 && mode != 18 && mode != 19 {
		wantHits := int32(1)
		if mode == 13 || mode == 20 {
			wantHits = 3
		}
		if mode == 16 || mode == 17 || mode == 22 {
			wantHits = 2
		}
		wantSequence := uint64(1)
		if mode == 17 || mode == 22 {
			wantSequence = 2
		}
		if result.GetCertainty() != pb.OperationResult_UNKNOWN || result.Error == "" || hits.Load() != wantHits || state.CommandSequence != wantSequence || (len(after.Records["r:result-2"]) != 0) != (mode == 17 || mode == 22) {
			t.Fatalf("read conflict negative lost fence: mode=%d certainty=%s error=%q reads=%d sequence=%d receipt=%t", mode, result.GetCertainty(), result.Error, hits.Load(), state.CommandSequence, len(after.Records["r:result-2"]) != 0)
		}
		if mode == 17 || mode == 22 {
			receipt, x := decodeNativeResult(after.Records["r:result-2"])
			if x != nil || receipt.OperationId != "concurrent-decision" {
				t.Fatal("concurrent acceptance was overwritten", x)
			}
		}
		return
	}
	if result.GetCertainty() != pb.OperationResult_ACCEPTED || result.Error != "" || state.CommandSequence != 2 || len(after.Records["r:result-2"]) == 0 {
		t.Fatalf("read-only revision conflict lost original decision: certainty=%s error=%q sequence=%d receipt=%t", result.GetCertainty(), result.Error, state.CommandSequence, len(after.Records["r:result-2"]) != 0)
	}
	receipt, err := decodeNativeResult(after.Records["r:result-2"])
	if err != nil || receipt.OperationId != "selected-read-conflict" || receipt.ServerRequestId != presented.RequestId || receipt.Sequence != 2 || mode == 12 && (receipt.Pending || receipt.Output != "") {
		t.Fatal("decision receipt changed", err)
	}
	if hits.Load() != 2 {
		t.Fatal("original decision was not reselected exactly once", hits.Load())
	}
	if mode >= 18 {

		before := hits.Load()
		writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: hello.SessionId, CorrelationId: "already-accepted", RemainingBudgetMs: 2000, Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}})
		duplicate := read(func(value *pb.Envelope) bool { return value.CorrelationId == "already-accepted" }).GetResult()
		if duplicate.GetCertainty() != pb.OperationResult_UNKNOWN || hits.Load() != before {
			t.Fatal("accepted decision was replayed")
		}
		select {
		case <-written:
			t.Fatal("decision emitted a second response")
		default:
		}
	}
}

func TestRedisDirectDecisionConflictRequiresDefiniteNoResult(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result *pb.OperationResult
		err    error
		retry  bool
	}{
		{name: "definite conflict", err: rediscore.ErrConflict, retry: true},
		{name: "unknown conflict", err: errors.Join(rediscore.ErrUnknown, rediscore.ErrConflict)},
		{name: "unknown", err: rediscore.ErrUnknown},
		{name: "authority", err: rediscore.ErrAuthority},
		{name: "expired", err: rediscore.ErrExpired},
		{name: "semantic", err: errors.New("native semantic refusal")},
		{name: "accepted result", result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, err: rediscore.ErrConflict},
		{name: "refused result", result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED}, err: rediscore.ErrConflict},
		{name: "unknown result", result: &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN}, err: rediscore.ErrConflict},
		{name: "success", result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}},
		{name: "no error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if actual := redisDirectDecisionConflict(tc.result, tc.err); actual != tc.retry {
				t.Fatalf("retry=%t, want %t", actual, tc.retry)
			}
		})
	}
}
