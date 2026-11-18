package engine

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func pressureUntilReservedBoundary(t *testing.T, r *stateReduction) {
	t.Helper()
	source := r.group.Contexts[r.in.Context.GetId()]
	if source == nil {
		for _, c := range r.group.Contexts {
			source = c
			break
		}
	}
	snapshot := proto.Clone(source.Snapshot).(*pb.ContextSnapshot)
	snapshot.Context.Id = "pressure"
	snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	r.group.Contexts["pressure"] = &stateContext{Snapshot: snapshot, Origin: source.Origin, Flow: source.Flow, Transactions: make(map[string]*stateTransaction), Requests: make(map[string]string), Dialogs: make(map[string]portable.Dialog)}
	budget, err := r.completionBudget()
	if err != nil {
		t.Fatal(err)
	}
	count := r.engine.store.Limits().Effects - len(r.payload.Effects) - budget.Effects - 1
	if count < 1 {
		t.Fatalf("profile has no pressure headroom: %+v effects%d", budget, len(r.payload.Effects))
	}
	encoded, _ := marshalRetainedFixture(stateEngineEffect{Context: "pressure"})
	for n := 0; n < count; n++ {
		r.payload.Effects = append(r.payload.Effects, state.Effect{ID: fmt.Sprintf("pressure-%d", n), Kind: "deliver", Status: state.EffectUnknown, AttemptID: "attempt", DeadlineMS: r.now + 1000000, Payload: encoded})
	}
	if !stateTestCapacityFits(r) {
		t.Fatal("test pressure consumed the reserved completion budget")
	}
}

// Tests mutate candidates directly between checks. Recompute their budget rather
// than reusing the production reducer's latest finished candidate.
func stateTestCapacityFits(r *stateReduction) bool {
	budget, err := r.completionBudget()
	return err == nil && r.capacityFits(budget)
}

func TestStateDerivedCapacityAdmitsMandatorySixteenForkCompletion(t *testing.T) {
	testStateDerivedCapacityAdmitsMandatorySixteenForkCompletion(t, nil)
}

func testStateDerivedCapacityAdmitsMandatorySixteenForkCompletion(t *testing.T, policy *statePolicyAdmission) {
	t.Helper()
	engine, _ := historicalStateTestEngine(t)
	r, c, tx := forkFixture(t)
	r.engine.store = engine.store
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
		t.Fatal(err)
	}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: forkReply(t, tx, 200, "winner")}); err != nil {
		t.Fatal(err)
	}
	// Capacity arithmetic is shared by the native profiles. Install the retained
	// selection after this legacy wire fixture's initial emissions; real owner
	// and policy admission are exercised separately against Redis.
	tx.Policy = policy
	pressureUntilReservedBoundary(t, r)
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	originalRecord := r.record
	originalRecord.Payload = payload
	fresh, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.OPTIONS, URI: "sip:controller@example.test", From: "<sip:phone@example.test>", CallID: r.record.Ref.CallID, LocalTag: "fresh", Branch: "z9hG4bK-fresh", CSeq: 1, Advertise: "192.0.2.19:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	freshRef := engine.contextRef(r.record.Ref, "fresh")
	input := stateEngineInput{Kind: "ingest", Context: freshRef, Application: c.Snapshot.Application, Ingest: &pb.OwnerIngest{Context: freshRef, Application: c.Snapshot.Application, Flow: c.Flow, Wire: fresh.WireBytes()}}
	encoded, _ := marshalRetainedFixture(input)
	refused, outcome, err := engine.reduce(originalRecord, state.Input{ID: "fresh", Kind: "ingest", Payload: encoded, DeadlineMS: r.now + 2000, ReceiptUntilMS: r.now + 122000}, r.now)
	if err != nil {
		t.Fatal(err)
	}
	var result pb.OperationResult
	if err = unmarshalRetainedFixture(outcome, &result); err != nil {
		t.Fatal(err)
	}
	originalRecord.Payload = refused
	group, err := decodeStateGroup(originalRecord)
	if err != nil {
		t.Fatal(err)
	}
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || group.Contexts["fresh"] != nil {
		t.Fatal("ordinary input consumed already-admitted closure slots")
	}
	for n := 1; n < stateForkDialogs; n++ {
		start := len(r.payload.Effects)
		response := forkReply(t, tx, 200, fmt.Sprintf("fork-%d", n))
		if err = r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: response}); err != nil {
			t.Fatal(err)
		}
		methods, _ := forkWireEffects(t, r.payload.Effects[start:])
		if len(methods) != 2 || methods[0] != "ACK" || methods[1] != "BYE" {
			t.Fatalf("reserved late fork%d lost mandatory ACK/BYE: %v", n, methods)
		}
		if _, err = r.finish(); err != nil {
			t.Fatal(err)
		}
		if !stateTestCapacityFits(r) {
			t.Fatalf("late fork%d exceeded its pre-admitted reserve", n)
		}
		if !engine.statePayloadFits(r.record, r.payload) {
			t.Fatalf("late fork%d exceeded pre-admitted bytes", n)
		}
	}
	if len(c.Dialogs) != stateForkDialogs {
		t.Fatal("full supported fork profile not retained")
	}
}

func TestStateTerminatedInviteRetainsACKAndUnseenForkCapacity(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	r, c, tx := forkFixture(t)
	r.engine.store = engine.store
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
		t.Fatal(err)
	}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: forkReply(t, tx, 486, "rejected")}); err != nil {
		t.Fatal(err)
	}
	if tx.Native.Phase != portable.Terminated || tx.Native.ACK == nil {
		t.Fatal("fixture did not retain a reliable rejected INVITE ACK")
	}
	ack := tx.Native.ACK.Clone()
	budget, err := r.completionBudget()
	if err != nil {
		t.Fatal(err)
	}
	tx.Native.ACK = nil
	withoutACK, err := r.completionBudget()
	tx.Native.ACK = &ack
	if err != nil {
		t.Fatal(err)
	}
	remaining := stateTransactionEmissions - int(tx.Emissions)
	if budget.Effects-withoutACK.Effects != remaining || budget.Bytes <= withoutACK.Bytes || budget.Dialogs < stateForkDialogs {
		t.Fatalf("terminated transaction dropped ACK/fork slot or byte reserve: %+v without ACK %+v", budget, withoutACK)
	}
	pressureUntilReservedBoundary(t, r)
	fresh, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.OPTIONS, URI: "sip:controller@example.test", From: "<sip:phone@example.test>", CallID: r.record.Ref.CallID, LocalTag: "fresh", Branch: "z9hG4bK-fresh", CSeq: 1, Advertise: "192.0.2.19:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.NewTransaction(portable.NonInviteServer, fresh, portable.DefaultTimers(), "fresh")
	if err != nil {
		t.Fatal(err)
	}
	c.Transactions[native.Key] = &stateTransaction{Native: native, Flow: c.Flow}
	if stateTestCapacityFits(r) {
		t.Fatal("fresh procedure consumed retained ACK/fork completion reserve")
	}
	delete(c.Transactions, native.Key)
	for emission := 0; emission < remaining; emission++ {
		r.now += 2001 // Each retained emission has passed its own start deadline.
		if err = r.send(c, tx, ack, true); err != nil {
			t.Fatal(err)
		}
		if !stateTestCapacityFits(r) {
			t.Fatalf("reserved ACK emission%d exhausted native completion slots", emission)
		}
	}
	if tx.Emissions != stateTransactionEmissions {
		t.Fatal("retained ACK replay did not use its finite reserved budget")
	}
	before := len(r.payload.Effects)
	if err = r.send(c, tx, ack, true); err != nil || len(r.payload.Effects) != before {
		t.Fatalf("retained ACK replay escaped the native emission cap: %v", err)
	}
	if _, err = r.forkResponse(c, tx, forkReply(t, tx, 200, "late")); err != nil || !stateTestCapacityFits(r) {
		t.Fatalf("retained late successful fork lost its independent completion reserve: %v", err)
	}
	if _, err = r.finish(); err != nil || !engine.statePayloadFits(r.record, r.payload) {
		t.Fatalf("retained ACK/fork completion exceeded reserved bytes: %v", err)
	}
}

func TestStateEndedForkACKsRetainWireBytesAtGroupBoundary(t *testing.T) {
	engine, _ := historicalStateTestEngine(t, state.Limits{GroupBytes: 8 << 20})
	r, c, tx := forkFixture(t)
	r.engine.store = engine.store
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	for branch := 0; branch < stateForkDialogs; branch++ {
		if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, fmt.Sprintf("ended-%d", branch))); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.dialogEffectsForClass(c, c.Dialog, []portable.Effect{{Kind: portable.NeedBYE}}, true); err != nil {
		t.Fatal(err)
	}
	c.Snapshot.Ended = true
	cancel, err := portable.BuildCancel(tx.Native.Origin)
	if err != nil {
		t.Fatal(err)
	}
	nativeCancel, err := portable.NewTransaction(portable.NonInviteClient, cancel, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	c.Transactions[nativeCancel.Key] = &stateTransaction{Native: nativeCancel, Flow: c.Flow}
	for _, retained := range c.Transactions {
		retained.Native.Phase, retained.Native.Timers, retained.RetireMS = portable.Terminated, nil, r.now+120000
	}
	for key, dialog := range c.Dialogs {
		dialog.Phase, dialog.Timers = portable.DialogEnded, nil
		c.Dialogs[key] = dialog
	}
	// Previous ACK/BYE outcomes are settled; only the retained native procedures
	// and their possible exact ACK replays consume future completion capacity.
	r.payload.Effects = nil
	budget, err := r.completionBudget()
	if err != nil {
		t.Fatal(err)
	}
	delete(c.Transactions, tx.Native.Key)
	withoutReplay, err := r.completionBudget()
	c.Transactions[tx.Native.Key] = tx
	if err != nil {
		t.Fatal(err)
	}
	want := (budget.Effects-withoutReplay.Effects)*stateProofMetadataBytes + base64.StdEncoding.EncodedLen(2*stateForkDialogs*stateWireLimit)
	if budget.Dialogs != 0 || budget.Transactions != 0 || budget.Bytes-withoutReplay.Bytes != want {
		t.Fatalf("ended ACK replay lost wire reserve or inflated live state: %+v without %+v want delta%d", budget, withoutReplay, want)
	}
	if _, err := r.finish(); err != nil {
		t.Fatal(err)
	}
	limits := engine.store.Limits()
	encoded, _ := marshalRetainedFixture(r.payload)
	history, _ := marshalRetainedFixture(r.record.Receipts)
	marker := base64.StdEncoding.EncodedLen(limits.OutcomeBytes) + 1024
	padding := limits.GroupBytes - len(encoded) - len(history) - 4096 - 2*marker - r.payload.ReservedCompletionBytes - 2048
	if padding < stateWireLimit {
		t.Fatal("fixture has insufficient byte pressure headroom")
	}
	pressure, _ := marshalRetainedFixture(map[string]string{"context": c.Snapshot.Context.Id, "padding": strings.Repeat("x", padding)})
	r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "byte-pressure", Kind: "deliver", Status: state.EffectUnknown, AttemptID: "attempt", DeadlineMS: r.now + 1000000, Payload: pressure})
	if _, err := r.finish(); err != nil || !engine.statePayloadFits(r.record, r.payload) {
		t.Fatalf("reserved ended ACK bytes do not fit near group boundary: %v", err)
	}
	// Fresh policy must be refused at the byte boundary, while the already
	// reserved branch completion can still materialize its immutable envelope.
	fresh := stateEngineEffect{Context: c.Snapshot.Context.Id, Wire: []byte(strings.Repeat("x", stateWireLimit))}
	freshBytes, _ := marshalRetainedFixture(fresh)
	candidate := r.payload
	candidate.Effects = append(append([]state.Effect(nil), r.payload.Effects...), state.Effect{ID: "fresh", Kind: "wire", Status: state.EffectPending, Payload: freshBytes})
	if engine.statePayloadFits(r.record, candidate) {
		t.Fatal("fresh payload consumed reserved ended ACK bytes")
	}
	start := len(r.payload.Effects)
	if _, err = r.forkResponse(c, tx, forkReply(t, tx, 200, "ended-0")); err != nil || len(r.payload.Effects) != start+1 {
		t.Fatalf("ended branch ACK replay did not materialize its reserved intent: %v", err)
	}
	if _, err = r.finish(); err != nil || !engine.statePayloadFits(r.record, r.payload) {
		t.Fatalf("ended branch ACK replay exhausted reserved group bytes: %v", err)
	}
}

func TestStateReservedACKTimeoutCanCreateCleanupAtEffectBoundary(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	ctx := context.Background()
	invite := stateTestRequest("INVITE", 1, "", "timeout")
	ref, err := engine.Locate(ctx, invite)
	if err != nil {
		t.Fatal(err)
	}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: engine.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.20", SourcePort: 50000, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, engine, ref, "ingest", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: invite.Wire}})
	_, group := stateTestLoad(t, engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	stateTestApply(t, engine, ref, "answer", stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	record, group := stateTestLoad(t, engine, ref)
	c := group.Contexts[ref.Id]
	c.CompletionScope = "scope"
	c.DialogScopes = map[string]string{c.Dialog: "scope"}
	r := &stateReduction{engine: engine, record: record, group: group, now: c.Dialogs[c.Dialog].Timers[portable.TimerACK].At - 1, result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}, in: stateEngineInput{Context: ref}}
	// The prior successful response is known/settled; this test pressures later
	// mandatory timeout work, independent of the physical writer qualification.
	r.payload = record.Payload
	r.payload.Effects = nil
	pressureUntilReservedBoundary(t, r)
	timer := c.Dialogs[c.Dialog].Timers[portable.TimerACK]
	r.now = timer.At
	r.in = stateEngineInput{Kind: "timer", Timer: r.timerID(ref.Id, "dialog", c.Dialog, portable.TimerACK), Generation: strconv.FormatUint(timer.Generation, 10)}
	start := len(r.payload.Effects)
	if err = r.timerByIdentity(); err != nil {
		t.Fatal(err)
	}
	methods, grants := forkWireEffects(t, r.payload.Effects[start:])
	if len(methods) != 1 || methods[0] != "BYE" || grants[0].AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP {
		t.Fatalf("reserved ACK timeout lost cleanup: %v", methods)
	}
	if _, err = r.finish(); err != nil {
		t.Fatal(err)
	}
	if !stateTestCapacityFits(r) || !engine.statePayloadFits(record, r.payload) {
		t.Fatal("mandatory timeout consumed unreserved effect/timer/byte capacity")
	}
}

func TestStateProfileBoundAndSmallByteBudgetRefuseBeforeOutput(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	effects, timers := stateSlotBound()
	if effects != 4096 || timers != 4096 || engine.store.Limits().Effects != effects || engine.store.Limits().Timers != timers {
		t.Fatalf("default limits drift from native derivation: %d/%d %+v", effects, timers, engine.store.Limits())
	}
	small, _ := historicalStateTestEngine(t, state.Limits{GroupBytes: 8 << 20})
	ref, err := small.Locate(context.Background(), stateTestRequest("INVITE", 1, "", "small"))
	if err != nil {
		t.Fatal(err)
	}
	outcome := stateTestApply(t, small, ref, "too-large-profile", stateEngineInput{Kind: "command", Application: &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Body: []byte("v=0\r\n")}}}})
	record, group := stateTestLoad(t, small, ref)
	if outcome.Certainty != pb.OperationResult_NOT_ACCEPTED || len(record.Payload.Effects) != 0 || len(group.Contexts) != 0 {
		t.Fatal("fresh unsupported byte profile reached transport/output")
	}
}

func TestStateReplayStormHasFiniteACKAndProgressBudget(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
		t.Fatal(err)
	}
	response := forkReply(t, tx, 200, "winner")
	for n := 0; n < 100; n++ {
		r.now++
		if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: response}); err != nil {
			t.Fatal(err)
		}
		// An UNKNOWN physical attempt stays charged and distinct from a later
		// standards-driven retransmission; a packet storm still has a ceiling.
		for index, effect := range r.payload.Effects {
			if effect.Kind != "wire" {
				continue
			}
			var body stateEngineEffect
			if unmarshalRetainedFixture(effect.Payload, &body) != nil || body.WireMethod != "ACK" {
				continue
			}
			effect.Kind = "deliver"
			effect.Status = state.EffectUnknown
			effect.AttemptID = body.Authorization.Effect.AttemptId
			body.Permit = &pb.SendPermit{Authorization: body.Authorization, Flow: body.Flow, DirectoryEpoch: "directory", OrderingProof: "proof"}
			body.Wire = nil
			body.Envelope = nil
			r.setEffect(index, effect, body)
		}
	}
	if c.ACKEmissions[c.Dialog].Count != stateProfileTransmissions {
		t.Fatal("ACK retransmission budget drift")
	}
	attempts := 0
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if unmarshalRetainedFixture(effect.Payload, &body) == nil && body.WireMethod == "ACK" {
			attempts++
		}
	}
	if attempts != stateProfileTransmissions {
		t.Fatalf("duplicate2xx storm allocated%d ACK attempts", attempts)
	}
	for n := 0; n < 1000; n++ {
		r.event(c, pb.DialogEvent_PROVISIONAL, mustObserve(forkReply(t, tx, 180, fmt.Sprintf("tag-%d", n%100)).WireBytes(), false), "open")
	}
	if len(c.ProgressEmissions) > stateForkDialogs+1 {
		t.Fatal("progress map grew beyond supported branch profile")
	}
	total := 0
	for _, count := range c.ProgressEmissions {
		if count > stateProfileTransmissions {
			t.Fatal("unbounded progress count")
		}
		total += int(count)
	}
	if total > stateProfileTransmissions*(stateForkDialogs+1) {
		t.Fatal("provisional packet storm exceeded derived profile")
	}
}

func TestStateDefaultTimerScheduleMatchesEmissionDerivation(t *testing.T) {
	origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.OPTIONS, URI: "sip:phone@example.test", CallID: "profile", LocalTag: "local", Branch: "z9hG4bK-profile", CSeq: 1, Advertise: "192.0.2.1:5060", Transport: "udp"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := portable.NewTransaction(portable.NonInviteClient, origin, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1})
	if err != nil {
		t.Fatal(err)
	}
	sends := 0
	for _, effect := range effects {
		if effect.Kind == portable.Send {
			sends++
		}
	}
	for tx.Phase != portable.Terminated {
		var selected portable.Timer
		var due portable.Deadline
		for name, timer := range tx.Timers {
			if timer.Active && (due.At == 0 || timer.At < due.At) {
				selected = name
				due = timer
			}
		}
		if due.At == 0 {
			t.Fatal("native non-INVITE procedure has no terminal timer")
		}
		tx, effects, err = portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.At, Timer: selected, Generation: due.Generation})
		if err != nil {
			t.Fatal(err)
		}
		for _, effect := range effects {
			if effect.Kind == portable.Send {
				sends++
			}
		}
	}
	if sends != stateProfileTransmissions {
		t.Fatalf("native timer schedule emitted%d, profile reserves%d", sends, stateProfileTransmissions)
	}
}
