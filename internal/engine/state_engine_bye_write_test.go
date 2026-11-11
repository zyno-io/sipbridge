package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func publicByeFixture(t *testing.T, backends ...string) (nativeOwnerFixture, *pb.ContextRef, *pb.FlowWrite) {
	t.Helper()
	backend := state.RedisBackend
	if len(backends) > 0 {
		backend = backends[0]
	}
	f := newNativeOwnerFixtureBackend(t, backend)
	f.flow.Transport = "udp" // Exercise native retransmissions as well as the first write.
	ref := f.admit(t)
	_, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	var request string
	for id := range c.Requests {
		request = id
	}
	result := f.apply(t, ref, "answer", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Admission: policyDecision(nil), Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	c = group.Contexts[ref.Id]
	var remote portable.Dialog
	for _, tx := range c.Transactions {
		if tx.Native.Family != portable.InviteServer || tx.Native.LastResponse == nil {
			continue
		}
		var err error
		remote, err = portable.NewDialog(portable.UAC, tx.Native.Origin, *tx.Native.LastResponse, portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
	}
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 1, nil, "z9hG4bK-public-bye-ack")
	if err != nil {
		t.Fatal(err)
	}
	result = f.apply(t, ref, "ack", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: ack.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	result = f.apply(t, ref, "public-bye", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 2, Admission: policyDecision(f.flow), Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if effect.Kind == "deliver" && unmarshalRetainedFixture(effect.Payload, &body) == nil && body.Command == "public-bye" && body.WireMethod == "BYE" {
			return f, ref, &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
		}
	}
	t.Fatal("public BYE has no native physical intent")
	return f, ref, nil
}

func TestStatePublicByeWriteReleasesLocalTeardownBeforePeerResponse(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) { testStatePublicByeWriteReleasesLocalTeardownBeforePeerResponse(t, backend) })
	}
}

func testStatePublicByeWriteReleasesLocalTeardownBeforePeerResponse(t *testing.T, backend string) {
	f, ref, write := publicByeFixture(t, backend)
	ctx := context.Background()
	reserved, err := f.engine.Reserve(ctx, write)
	if err != nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatalf("reserve public BYE: %v", err)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	if writtenPublicBye(group.Contexts[ref.Id]) != nil {
		t.Fatal("reservation fabricated a write receipt")
	}
	if err = f.engine.Complete(ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	logical, _ := f.engine.group(ref)
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	result, err := f.engine.awaitOperation(bounded, logical, ref.Id, "public-bye", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, false)
	if err != nil || result.Certainty != pb.OperationResult_ACCEPTED || result.Request == nil || !bytes.Equal(result.Request.Wire, reserved.Wire) || result.Response != nil || result.Snapshot.Ended {
		t.Fatalf("written BYE did not release local teardown without claiming peer completion: %v %v", result, err)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	if c.Result.Request != nil || writtenPublicBye(c) == nil {
		t.Fatal("write proof duplicated the parsed request instead of using the existing written bit")
	}
	inspected, err := f.engine.inspectContext(ctx, f.app, ref)
	if err != nil || inspected.GetRequest().GetMethod() != "BYE" || !bytes.Equal(inspected.Request.Wire, reserved.Wire) || inspected.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("read-only recovery did not project the exact written request")
	}
	var bye *stateTransaction
	for _, tx := range c.Transactions {
		if tx.Command == "public-bye" {
			bye = tx
		}
	}
	if bye == nil || bye.Native.Phase != portable.Trying || !bye.Native.Timers[portable.TimerE].Active {
		t.Fatal("local teardown stopped the native transaction/retransmission timer")
	}
	response := forkReply(t, bye, 200, c.Snapshot.LocalTag)
	result = f.apply(t, ref, "bye-response", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: response.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	c = group.Contexts[ref.Id]
	if c.Result.Certainty != pb.OperationResult_COMPLETED || c.Result.GetResponse().GetStatus() != 200 || !c.Snapshot.Ended {
		t.Fatal("the eventual peer response did not complete the native BYE normally")
	}
}

func TestStatePublicByeUncertainFirstWriteCannotCreateReceipt(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) { testStatePublicByeUncertainFirstWriteCannotCreateReceipt(t, backend) })
	}
}

func testStatePublicByeUncertainFirstWriteCannotCreateReceipt(t *testing.T, backend string) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_UNKNOWN, pb.DeliveryResult_NOT_WRITTEN} {
		t.Run(outcome.String(), func(t *testing.T) {
			f, ref, write := publicByeFixture(t, backend)
			if _, err := f.engine.Reserve(context.Background(), write); err != nil {
				t.Fatal(err)
			}
			if err := f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome}); err != nil {
				t.Fatal(err)
			}
			replay, err := f.engine.Reserve(context.Background(), write)
			if err != nil || !replay.Known || len(replay.Wire) != 0 {
				t.Fatalf("physical replay gained a new write: %+v %v", replay, err)
			}
			_, group := stateTestLoad(t, f.engine, ref)
			if group.Contexts[ref.Id].Result.Request != nil || writtenPublicBye(group.Contexts[ref.Id]) != nil {
				t.Fatal("unwritten or uncertain BYE fabricated a request receipt")
			}
		})
	}
}

func TestStatePublicByeWrittenReceiptSurvivesFailedRetransmission(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_UNKNOWN, pb.DeliveryResult_NOT_WRITTEN} {
		t.Run(outcome.String(), func(t *testing.T) {
			f, ref, write := publicByeFixture(t)
			if _, err := f.engine.Reserve(context.Background(), write); err != nil {
				t.Fatal(err)
			}
			if err := f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
				t.Fatal(err)
			}
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			before := proto.Clone(c.Result).(*pb.OperationResult)
			r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, result: &pb.OperationResult{}, now: time.Now().UnixMilli()}
			for _, tx := range c.Transactions {
				if tx.Command != "public-bye" {
					continue
				}
				timer := tx.Native.Timers[portable.TimerE]
				r.now = timer.At
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerE, Generation: timer.Generation, Now: timer.At}); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.prepareAutonomousEffects(); err != nil {
				t.Fatal(err)
			}
			index := len(r.payload.Effects) - 1
			var body stateEngineEffect
			effect := r.payload.Effects[index]
			if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil || effect.Kind != "deliver" || body.WireMethod != "BYE" || effect.ID == write.Effect.Id {
				t.Fatalf("missing distinct native retransmission: %v", err)
			}
			retry := &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
			r.in = stateEngineInput{Write: retry, Executor: retry.Flow.Ingress}
			if err := r.reserve(); err != nil {
				t.Fatal(err)
			}
			r.in.Delivery = &pb.DeliveryResult{Effect: retry.Effect, Outcome: outcome}
			if err := r.complete(); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(before, c.Result) {
				t.Fatal("a later failed physical attempt revoked the original written receipt")
			}
			_, retained, _, err := r.findEffect(effect.ID)
			if err != nil || outcome == pb.DeliveryResult_UNKNOWN && retained.Status != state.EffectUnknown || outcome == pb.DeliveryResult_NOT_WRITTEN && retained.Status != state.EffectNotWritten {
				t.Fatal("preserving the command receipt overwrote an individual physical outcome")
			}
		})
	}
}

func TestStatePublicByeProofRejectsOtherOperationsAndCleanup(t *testing.T) {
	for _, change := range []string{"operation", "cleanup", "wire", "authority", "policy"} {
		t.Run(change, func(t *testing.T) {
			f, ref, write := publicByeFixture(t)
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload}
			_, _, body, err := r.findEffect(write.Effect.Id)
			if err != nil {
				t.Fatal(err)
			}
			tx := c.Transactions[body.Transaction]
			switch change {
			case "operation":
				body.Command = "another-operation"
			case "cleanup":
				tx.Cleanup = true
			case "wire":
				body.WireHash = "different-wire"
			case "authority":
				body.Authorization.AuthorityClass = pb.EffectAuthorization_TERMINAL_CLEANUP
			case "policy":
				tx.Policy = nil
			}
			r.recordWrittenPublicBye(c, body)
			if tx.Written || writtenPublicBye(c) != nil {
				t.Fatal("unrelated physical evidence established a public dialog BYE write")
			}
		})
	}
}

func TestStateInitialOutOfDialogByeIsNotWrittenDialogProof(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.outboundRef(t)
	result := f.apply(t, ref, "initial-bye", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Admission: policyDecision(f.flow), Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{
			Method: "BYE", Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", PrincipalId: "phone"}}}})
	if result.Error != "" || result.GetRequest().GetMethod() != "BYE" {
		t.Fatalf("initial BYE fixture: %v", result)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	if writtenPublicBye(group.Contexts[ref.Id]) != nil {
		t.Fatal("a retained initial request was mistaken for a physical write")
	}
	waited := false
	_, err := f.engine.awaitOperationPolling(context.Background(), record.Ref, ref.Id, "initial-bye", result, false, stateOperationPolling{
		load: func(context.Context, state.Ref) (state.Record, error) { return record, nil },
		wait: func(context.Context, time.Duration) bool { waited = true; return false },
		now:  time.Now,
	})
	if err != nil || !waited {
		t.Fatal("initial out-of-dialog BYE returned early as written dialog proof")
	}
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if effect.Kind != "deliver" || unmarshalRetainedFixture(effect.Payload, &body) != nil || body.Command != "initial-bye" {
			continue
		}
		write := &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
		if _, err := f.engine.Reserve(context.Background(), write); err != nil {
			t.Fatal(err)
		}
		if err := f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
			t.Fatal(err)
		}
		_, after := stateTestLoad(t, f.engine, ref)
		if after.Contexts[ref.Id].Result.Certainty != pb.OperationResult_UNKNOWN {
			t.Fatal("a retained initial request hid physical uncertainty")
		}
		return
	}
	t.Fatal("initial BYE has no physical attempt")
}
