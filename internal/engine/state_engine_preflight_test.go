package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

func preflightIngest(ref *pb.ContextRef, f nativeOwnerFixture, method string, sequence int) stateEngineInput {
	wire := stateTestRequest(method, sequence, "", fmt.Sprintf("preflight-%d", sequence)).Wire
	if method == "SUBSCRIBE" {
		wire = bytes.Replace(wire, []byte("Content-Length:"), []byte("Event: message-summary\r\nExpires: 90\r\nContent-Length:"), 1)
	}
	return stateEngineInput{Kind: "ingest", Context: ref, Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow,
			Wire: wire, PrincipalId: "principal", BindingId: "binding"}}
}

func preflightFixture(t *testing.T, method string) (nativeOwnerFixture, *pb.ContextRef, stateEngineInput) {
	t.Helper()
	t.Setenv("SIPBRIDGE_STATE_TEST_REDIS", preflightRedisProxy(t))
	f := newNativeOwnerFixture(t)
	in := preflightIngest(nil, f, method, 1)
	message, err := Observe(in.Ingest.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	in.Context, in.Ingest.Context = ref, ref
	return f, ref, in
}

func TestStateControlPreflightClassificationIsConservative(t *testing.T) {
	request := stateTestRequest("REGISTER", 1, "", "registration")
	in := stateEngineInput{Kind: "ingest", Ingest: &pb.OwnerIngest{Flow: &pb.FlowRef{Transport: "tls"}, Wire: request.Wire}}
	ordinary := &stateContext{Snapshot: &pb.ContextSnapshot{Kind: pb.ContextSnapshot_TRANSACTION}, Origin: portable.RestoreMessage(request.Wire, "tls", "", "")}
	if !stateSkipSubscriptionPreflight(in, nil) || !stateSkipSubscriptionPreflight(in, ordinary) {
		t.Fatal("proven ordinary registration could not reuse its preflight classification")
	}
	for _, method := range []string{"SUBSCRIBE", "INVITE", "REGISTER"} {
		candidate := stateEngineInput{Kind: "command", Command: &pb.Command{Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Method: method}}}}
		if stateSkipSubscriptionPreflight(candidate, ordinary) {
			t.Fatalf("%s command skipped its original preflight", method)
		}
	}
	for _, wire := range [][]byte{
		stateTestRequest("SUBSCRIBE", 1, "", "initial-subscribe").Wire,
		[]byte("malformed"),
		bytes.Replace(request.Wire, []byte("CSeq: 1 REGISTER"), []byte("CSeq: 1 SUBSCRIBE"), 1),
		bytes.Replace(request.Wire, []byte("REGISTER sip:service@example.test SIP/2.0"), []byte("SIP/2.0 200 OK"), 1),
	} {
		candidate := in
		candidate.Ingest = &pb.OwnerIngest{Flow: in.Ingest.Flow, Wire: wire}
		if stateSkipSubscriptionPreflight(candidate, nil) || stateSkipSubscriptionPreflight(candidate, ordinary) {
			t.Fatal("subscription, response or ambiguous input skipped the original preflight")
		}
	}
	for _, kind := range []string{"actual", "snapshot", "origin", "transaction", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			c := *ordinary
			c.Snapshot = &pb.ContextSnapshot{Kind: ordinary.Snapshot.Kind}
			subscribe := portable.RestoreMessage(stateTestRequest("SUBSCRIBE", 1, "", "potential").Wire, "tls", "", "")
			switch kind {
			case "actual":
				c.Subscription = &stateSubscription{}
			case "snapshot":
				c.Snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
			case "origin":
				c.Origin = subscribe
			case "transaction":
				c.Transactions = map[string]*stateTransaction{"pending": {Native: portable.Transaction{Origin: subscribe}}}
			case "unknown":
				c.Origin = portable.RestoreMessage(nil, c.Origin.Transport(), c.Origin.Source(), c.Origin.Destination())
			}
			if stateSkipSubscriptionPreflight(in, &c) {
				t.Fatal("actual or potential subscription skipped the separate fresh preflight")
			}
		})
	}
}

func preflightEvalCalls(t *testing.T, f nativeOwnerFixture) int64 {
	t.Helper()
	value, ok := preflightRedisCounters.Load(f.config.Redis.Address)
	if !ok {
		t.Fatal("fixture Redis counter unavailable")
	}
	return value.(*atomic.Int64).Load()
}

func TestStateControlPreflightRedisReadCounts(t *testing.T) {
	for _, mode := range []string{"initial-register", "existing-register", "subscribe-before-context", "potential-subscribe", "actual-subscribe", "command"} {
		t.Run(mode, func(t *testing.T) {
			method := "REGISTER"
			if strings.Contains(mode, "subscribe") {
				method = "SUBSCRIBE"
			}
			f, ref, in := preflightFixture(t, method)
			if mode == "existing-register" || mode == "potential-subscribe" || mode == "actual-subscribe" {
				if result := f.apply(t, ref, "initial", in); result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			if mode == "actual-subscribe" {
				_, group := stateTestLoad(t, f.engine, ref)
				var requestID string
				for id := range group.Contexts[ref.Id].Requests {
					requestID = id
				}
				result := f.apply(t, ref, "accept", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
					Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202,
						Authority: &pb.ResponseAuthority{DirectoryEpoch: "directory"}, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				_, group = stateTestLoad(t, f.engine, ref)
				if group.Contexts[ref.Id].Subscription == nil {
					t.Fatal("fixture did not create an actual subscription")
				}
			}
			if mode == "command" {
				in = stateEngineInput{Kind: "command", Context: ref, Application: f.app, ApplicationLeaseTerm: f.term, Command: &pb.Command{Context: ref}}
			}
			group, _ := f.engine.group(ref)
			before := preflightEvalCalls(t, f)
			if err := f.engine.preflightControl(context.Background(), group, in, time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			want := int64(2)
			if mode == "initial-register" || mode == "existing-register" {
				want = 1
			}
			if got := preflightEvalCalls(t, f) - before; got != want {
				t.Fatalf("preflight made %d guarded Redis reads; want %d", got, want)
			}
		})
	}
}

func preflightMutate(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef, change func(*stateContext)) {
	t.Helper()
	group, _ := f.engine.group(ref)
	runtime, err := executionfixture.New(f.engine.store.(*state.Store), execution.ReducerFunc(func(record state.Record, _ state.Input, _ int64) (state.Payload, []byte, error) {
		decoded, err := decodeStateGroup(record)
		if err != nil {
			return record.Payload, nil, err
		}
		change(decoded.Contexts[ref.Id])
		data, err := marshalRetainedFixture(decoded)
		payload := record.Payload
		payload.ProtocolState = data
		return payload, nil, err
	}), "independent-preflight-writer", 1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second).UnixMilli()
	_, err = runtime.Apply(context.Background(), group, state.Input{ID: "fixture-" + uuid.NewString(), Kind: "fixture", Fingerprint: "fixture",
		DeadlineMS: deadline, ReceiptUntilMS: deadline, Proof: true})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStateControlPreflightDoesNotCacheReceiptOrCSeq(t *testing.T) {
	f, ref, in := preflightFixture(t, "REGISTER")
	group, _ := f.engine.group(ref)
	if err := f.engine.preflightControl(context.Background(), group, in, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	first := f.apply(t, ref, "original-input", in)
	before, decoded := stateTestLoad(t, f.engine, ref)
	if first.Error != "" || len(decoded.Contexts[ref.Id].Transactions) != 1 {
		t.Fatalf("original REGISTER not admitted exactly once: %+v", first)
	}
	if err := f.engine.preflightControl(context.Background(), group, in, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	replay := f.apply(t, ref, "original-input", in)
	after, _ := stateTestLoad(t, f.engine, ref)
	if replay.Error != "" || after.Revision != before.Revision || !bytes.Equal(after.Payload.ProtocolState, before.Payload.ProtocolState) {
		t.Fatal("replayed invocation changed SIP state or consumed another CSeq")
	}
	beforeJSON, _ := marshalRetainedFixture(before.Receipts)
	afterJSON, _ := marshalRetainedFixture(after.Receipts)
	if !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatal("replay regenerated an original receipt or admission deadline")
	}
}

func TestStateControlPreflightPreservesDifferentPendingInput(t *testing.T) {
	f, ref, first := preflightFixture(t, "REGISTER")
	group, _ := f.engine.group(ref)
	first.Operation = "different-pending"
	data, _ := marshalRetainedFixture(first)
	sum := sha256.Sum256(data)
	deadline := time.Now().Add(2 * time.Second).UnixMilli()
	begun, err := f.engine.store.(*state.Store).Begin(context.Background(), group, "crashed-preflight-worker", state.Input{ID: first.Operation, Kind: first.Kind,
		Fingerprint: hex.EncodeToString(sum[:]), Payload: data, DeadlineMS: deadline, ReceiptUntilMS: deadline + 120000,
		ApplicationLeaseIDs: []string{f.engine.applicationLeaseID(f.app)}})
	if err != nil || begun.Step == nil {
		t.Fatal("fixture did not retain the different input", err)
	}
	key := f.prefix + "{p0000}:group:" + group.ID
	if err := f.client.HSet(context.Background(), key, "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	in := preflightIngest(ref, f, "REGISTER", 2)
	before := preflightEvalCalls(t, f)
	if err := f.engine.preflightControl(context.Background(), group, in, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := preflightEvalCalls(t, f) - before; got != 2 {
		t.Fatalf("pending input skipped the original separate preflight: %d reads", got)
	}
	beforeRecord, err := f.engine.store.Load(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	in.Operation = "new-input"
	_, err = f.engine.apply(ctx, group, "new-input", in, time.Now().Add(150*time.Millisecond), false)
	if !errors.Is(err, execution.ErrUncommitted) || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("CAS did not promptly refuse beside retained legacy input: %v", err)
	}
	after, err := f.engine.store.Load(context.Background(), group)
	if err != nil || after.Revision != beforeRecord.Revision || !reflect.DeepEqual(after.Pending, beforeRecord.Pending) ||
		!reflect.DeepEqual(after.Payload, beforeRecord.Payload) || !reflect.DeepEqual(after.Receipts, beforeRecord.Receipts) {
		t.Fatalf("CAS pending-input refusal changed retained work: %v", err)
	}
}

func TestStateControlPreflightDecisionCannotBypassLaterOwnerFence(t *testing.T) {
	f, ref, initial := preflightFixture(t, "REGISTER")
	if result := f.apply(t, ref, "initial", initial); result.Error != "" {
		t.Fatal(result.Error)
	}
	in := preflightIngest(ref, f, "REGISTER", 2)
	group, _ := f.engine.group(ref)
	if err := f.engine.preflightControl(context.Background(), group, in, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	preflightMutate(t, f, ref, func(c *stateContext) { c.OwnerLost = true })
	_, beforeGroup := stateTestLoad(t, f.engine, ref)
	result := f.apply(t, ref, "after-fence", in)
	_, afterGroup := stateTestLoad(t, f.engine, ref)
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || len(afterGroup.Contexts[ref.Id].Transactions) != 1 ||
		afterGroup.Contexts[ref.Id].Snapshot.CommandSequence != beforeGroup.Contexts[ref.Id].Snapshot.CommandSequence {
		t.Fatalf("stale classification bypassed the current owner fence: %+v", result)
	}
}

func TestStateControlPreflightFailureAndCancellationDoNotAuthorizeWork(t *testing.T) {
	for _, failure := range []string{"cancelled", "corrupt"} {
		t.Run(failure, func(t *testing.T) {
			f, ref, in := preflightFixture(t, "REGISTER")
			if result := f.apply(t, ref, "initial", in); result.Error != "" {
				t.Fatal(result.Error)
			}
			group, _ := f.engine.group(ref)
			want := context.Canceled
			switch failure {
			case "corrupt":
				if err := f.client.HSet(context.Background(), f.prefix+"{p0000}:group:"+group.ID, "data", "malformed").Err(); err != nil {
					t.Fatal(err)
				}
				want = state.ErrCorrupt
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "cancelled" {
				cancel()
			}
			err := f.engine.preflightControl(ctx, group, in, time.Now().Add(2*time.Second))
			if !errors.Is(err, want) {
				t.Fatalf("failure provenance changed: got %v, want %v", err, want)
			}
		})
	}
}
