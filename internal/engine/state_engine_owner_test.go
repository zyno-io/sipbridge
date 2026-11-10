package engine

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func ownerFixture(t *testing.T, confirmed bool) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, tx := forkFixture(t)
	r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{c.Snapshot.Context.Id: c}, Selectors: make(map[string]string)}
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	c.OwnerValidated, c.OwnerDirectoryEpoch = true, "directory"
	tx.Application, tx.CompletionScope = c.Snapshot.Application, "initial-scope"
	if confirmed {
		if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "remote")); err != nil {
			t.Fatal(err)
		}
	}
	return r, c, tx
}

func ownerStatusInput(t *testing.T, r *stateReduction, c *stateContext, application *pb.ProcessRef, live bool) stateEngineInput {
	t.Helper()
	if err := r.ensureOwnerProbe(c, application); err != nil {
		t.Fatal(err)
	}
	_, effect, body, found := r.ownerProbe(c)
	if !found {
		t.Fatal("owner query not retained")
	}
	return stateEngineInput{Kind: "owner-result", Effect: effect.ID, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: proto.Clone(body.Envelope.GetProbeApplication().Context).(*pb.ContextRef), Application: proto.Clone(application).(*pb.ProcessRef), DirectoryEpoch: "directory", Live: live, CheckedAtMs: uint64(r.now)}}}}
}

func TestStateOwnerLossFencesEpochAndRetainsPhysicalUnknown(t *testing.T) {
	r, c, _ := ownerFixture(t, true)
	old := proto.Clone(c.Snapshot.Application).(*pb.ProcessRef)
	for _, kind := range []string{"present", "event", "adopt", "credential"} {
		body, _ := json.Marshal(stateEngineEffect{Context: c.Snapshot.Context.Id, Application: old})
		r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "unknown-" + kind, Kind: kind, Status: state.EffectUnknown, AttemptID: "attempt", DeadlineMS: 1, Payload: body})
	}
	physical, _ := json.Marshal(stateEngineEffect{Context: c.Snapshot.Context.Id, Application: old, Authorization: &pb.EffectAuthorization{AuthorityClass: pb.EffectAuthorization_APPLICATION}})
	r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "physical", Kind: "deliver", Status: state.EffectUnknown, AttemptID: "attempt", DeadlineMS: 1, Payload: physical})
	r.in = ownerStatusInput(t, r, c, old, false)
	if err := r.ownerProbeResult(); err != nil {
		t.Fatal(err)
	}
	if !c.OwnerLost || !sameProcess(c.Snapshot.Application, old) || c.OwnerProbeEffect != "" {
		t.Fatal("negative authoritative proof did not permanently fence the exact old owner")
	}
	_, _, bye := causalEffect(t, r, "BYE", "remote")
	if bye.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || bye.Authorization.CompletionScope != "initial-scope" {
		t.Fatal("confirmed owner loss required a new application grant")
	}
	for _, effect := range r.payload.Effects {
		if effect.ID == "physical" {
			if effect.Status != state.EffectUnknown {
				t.Fatal("owner loss guessed away a physical attempt")
			}
		} else if effect.ID == "unknown-present" || effect.ID == "unknown-event" || effect.ID == "unknown-adopt" || effect.ID == "unknown-credential" {
			if effect.Status != state.EffectCancelled {
				t.Fatal("dead epoch policy RPC remained unreclaimable")
			}
		}
	}
	command := &pb.Command{Context: c.Snapshot.Context, Sequence: c.Snapshot.CommandSequence + 1, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}
	if err := r.ownerGuardCommand(c, command); !errors.Is(err, errStateApplicationOwner) {
		t.Fatal("fenced owner can issue another business command")
	}
	r.in = ownerStatusInput(t, r, c, old, true)
	if err := r.ownerProbeResult(); err != nil || !c.OwnerLost {
		t.Fatal("resumed old owner reopened a fenced call")
	}
	r.now = c.OwnerCleanupAtMS
	r.in = stateEngineInput{Timer: r.timerID(c.Snapshot.Context.Id, "owner", "probe", ""), Generation: strconv.FormatInt(c.OwnerCleanupAtMS, 10)}
	if handled, err := r.ownerTimer(); err != nil || !handled || !c.Snapshot.Ended || r.contextRetirementProven(c.Snapshot.Context.Id, c) {
		t.Fatalf("bounded owner cleanup discarded physical uncertainty: %v", err)
	}
}

func TestStateOwnerProbeOutageAndChangedEpochNeverProveDeath(t *testing.T) {
	r, c, _ := ownerFixture(t, true)
	in := ownerStatusInput(t, r, c, c.Snapshot.Application, false)
	id := c.OwnerProbeEffect
	r.in = stateEngineInput{Kind: "owner-result", Effect: id, Failure: "unavailable"}
	if err := r.ownerProbeResult(); err != nil || c.OwnerLost {
		t.Fatal("outage was treated as owner death")
	}
	r.now = c.OwnerProbeAtMS
	r.in = stateEngineInput{Timer: r.timerID(c.Snapshot.Context.Id, "owner", "probe", ""), Generation: strconv.FormatInt(c.OwnerProbeAtMS, 10)}
	if handled, err := r.ownerTimer(); err != nil || !handled || c.OwnerProbeEffect != id {
		t.Fatal("retry accumulated a second query instead of coalescing")
	}
	r.in = in
	r.in.Reply.GetApplicationStatus().DirectoryEpoch = "different-primary"
	r.in.Reply.GetApplicationStatus().CheckedAtMs = uint64(r.now)
	if err := r.ownerProbeResult(); err == nil || c.OwnerLost {
		t.Fatal("unproven directory epoch change fenced a live call")
	}
	r.in.Reply.GetApplicationStatus().DirectoryEpoch = "directory"
	r.in.Reply.GetApplicationStatus().CheckedAtMs = uint64(r.now - stateOwnerProofAge - 1)
	if err := r.ownerProbeResult(); err == nil || c.OwnerLost {
		t.Fatal("stale owner proof fenced a call")
	}
}

func TestStateInitialPresentationWaitsForLiveOwnerProofInRedis(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	request := stateTestRequest("INVITE", 1, "", "owner-proof")
	ref, err := a.Locate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "owner"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "incoming", SourceIp: "192.0.2.10", SourcePort: 50000, Listener: "sip.example.test:5061", Transport: "tls"}
	result := stateTestApply(t, a, ref, "owner-ingest", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: request.Wire}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, b, ref)
	c := group.Contexts[ref.Id]
	if c.OwnerProbeEffect == "" || c.OwnerValidated {
		t.Fatal("initial live owner query missing")
	}
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "present" {
			t.Fatal("business presentation ran before initial owner proof")
		}
	}
	proof := &pb.ApplicationStatus{Context: ref, Application: application, DirectoryEpoch: "directory", Live: true, CheckedAtMs: uint64(time.Now().UnixMilli())}
	result = stateTestApply(t, b, ref, "owner-live", stateEngineInput{Kind: "owner-result", Effect: c.OwnerProbeEffect, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: proof}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group = stateTestLoad(t, a, ref)
	c = group.Contexts[ref.Id]
	presented, queries := 0, 0
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "present" {
			presented++
		}
		if effect.Kind == "owner" {
			queries++
		}
	}
	if !c.OwnerValidated || c.OwnerDirectoryEpoch != "directory" || presented != 1 || queries != 0 {
		t.Fatal("positive proof did not atomically admit one presentation and reclaim query")
	}
}

func TestStateOwnerInitialUnavailableFailsWithoutFalseOwnerFence(t *testing.T) {
	r, c, tx := ownerFixture(t, false)
	c.OwnerValidated, c.OwnerDirectoryEpoch = false, ""
	tx.Native.Family = portable.InviteServer
	server, err := portable.NewTransaction(portable.InviteServer, tx.Native.Origin, portable.DefaultTimers(), c.Tag)
	if err != nil {
		t.Fatal(err)
	}
	tx.Native, _, err = portable.StepTransaction(server, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		t.Fatal(err)
	}
	if deferred, err := r.deferForOwnerProof(c, tx); err != nil || !deferred {
		t.Fatal("initial presentation not gated")
	}
	_, query, _, _ := r.ownerProbe(c)
	r.now = query.DeadlineMS
	r.in = stateEngineInput{Timer: r.timerID(c.Snapshot.Context.Id, "owner", "probe", ""), Generation: strconv.FormatInt(query.DeadlineMS, 10)}
	if _, err := r.ownerTimer(); err != nil || c.OwnerLost || !c.Snapshot.Ended || tx.Native.LastResponse == nil {
		t.Fatalf("initial proof outage did not fail setup conservatively: %v", err)
	}
	parsed, _ := tx.Native.LastResponse.Parse()
	if parsed.(*sip.Response).StatusCode != 503 {
		t.Fatal("unavailable owner produced wrong native final")
	}
}

func TestStateOwnerLossPreservesUASACKAndAcceptedSubscriptionLease(t *testing.T) {
	t.Run("UAS_ACK_then_cleanup_on_replacement_flow", func(t *testing.T) {
		r, c, clientTX := ownerFixture(t, false)
		response := forkReply(t, clientTX, 200, "server")
		dialog, err := portable.NewDialog(portable.UAS, clientTX.Native.Origin, response, portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
		dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogSendResponse, Now: r.now, Message: response})
		if err != nil {
			t.Fatal(err)
		}
		c.Dialog = dialog.LocalTag + "/" + dialog.RemoteTag
		c.Dialogs = map[string]portable.Dialog{c.Dialog: dialog}
		c.DialogScopes = map[string]string{c.Dialog: "uas-scope"}
		c.Snapshot.Answered = true
		c.Transactions = make(map[string]*stateTransaction)
		r.in = ownerStatusInput(t, r, c, c.Snapshot.Application, false)
		if err := r.ownerProbeResult(); err != nil || c.Dialogs[c.Dialog].LocalBye != nil {
			t.Fatalf("UAS owner loss skipped ACK obligation: %v", err)
		}
		client, err := portable.NewDialog(portable.UAC, clientTX.Native.Origin, response, portable.DefaultTimers())
		if err != nil {
			t.Fatal(err)
		}
		origin, err := clientTX.Native.Origin.Parse()
		if err != nil {
			t.Fatal(err)
		}
		ack, err := portable.BuildDialogRequest(client, sip.ACK, origin.CSeq().SeqNo, nil, "z9hG4bK-owner-ack")
		if err != nil {
			t.Fatal(err)
		}
		next, _, err := portable.StepDialog(c.Dialogs[c.Dialog], portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: r.now, Message: ack})
		if err != nil {
			t.Fatal(err)
		}
		c.Dialogs[c.Dialog] = next
		old := proto.Clone(c.Flow).(*pb.FlowRef)
		replacement := proto.Clone(old).(*pb.FlowRef)
		replacement.ConnectionId = "reconnected"
		if err := r.ownerAcceptedACK(c, c.Dialog, replacement); err != nil {
			t.Fatal(err)
		}
		if err := r.continueOwnerLoss(c); err != nil || !proto.Equal(c.Flow, old) {
			t.Fatalf("owner loss ACK published a context route: %v", err)
		}
		_, _, bye := causalEffect(t, r, "BYE", dialog.RemoteTag)
		if !proto.Equal(bye.Flow, replacement) || bye.Authorization.CompletionScope != "uas-scope" {
			t.Fatal("owner cleanup ignored exact ACK flow or retained UAS scope")
		}
	})
	t.Run("subscription_keeps_lease_and_can_adopt_exact_new_processor", func(t *testing.T) {
		r, c, _ := ownerFixture(t, false)
		c.Subscription = &stateSubscription{Event: "dialog", ExpiresMS: r.now + 60000, Target: "sip:phone@example.test"}
		c.Snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
		c.Transactions = make(map[string]*stateTransaction)
		expires, old := c.Subscription.ExpiresMS, proto.Clone(c.Snapshot.Application).(*pb.ProcessRef)
		r.in = ownerStatusInput(t, r, c, old, false)
		if err := r.ownerProbeResult(); err != nil || !c.OwnerLost || c.Snapshot.Ended || c.Subscription.ExpiresMS != expires || c.Subscription.Terminating {
			t.Fatalf("processor loss ended accepted subscription early: %v", err)
		}
		next := &pb.ProcessRef{InstanceId: "controller-next", Epoch: "new"}
		c.Snapshot.Application = next // only the accepted adoption receipt may do this
		if err := r.ownerAdopted(c, old, next, "directory"); err != nil || c.OwnerLost || !c.OwnerValidated || c.Subscription.ExpiresMS != expires {
			t.Fatalf("approved stateless processor rehome failed: %v", err)
		}
	})
}

func TestStateOwnerCleanupHasFixedBoundAndPureCredentialExpiry(t *testing.T) {
	r, c, _ := ownerFixture(t, true)
	dialog := c.Dialogs[c.Dialog]
	dialog.LocalCSeq = math.MaxInt32
	c.Dialogs[c.Dialog] = dialog
	r.in = ownerStatusInput(t, r, c, c.Snapshot.Application, false)
	if err := r.ownerProbeResult(); err != nil || !c.OwnerLost {
		t.Fatal("unsendable BYE prevented committing the owner fence")
	}
	r.now = c.OwnerCleanupAtMS
	r.in = stateEngineInput{Timer: r.timerID(c.Snapshot.Context.Id, "owner", "probe", ""), Generation: strconv.FormatInt(c.OwnerCleanupAtMS, 10)}
	if _, err := r.ownerTimer(); err != nil || !c.Snapshot.Ended || c.OwnerCleanupAtMS != 0 {
		t.Fatal("unsendable cleanup leaked confirmed protocol state forever")
	}
	r, c, _ = ownerFixture(t, false)
	body, _ := json.Marshal(stateEngineEffect{Context: c.Snapshot.Context.Id, Application: c.Snapshot.Application})
	r.payload.Effects = []state.Effect{{ID: "credential", Kind: "credential", Status: state.EffectDispatching, AttemptID: "compute", DeadlineMS: 1, Payload: body}}
	r.now = 130000
	if err := r.expireOwnerComputations(); err != nil || !c.Snapshot.Ended || c.OwnerLost || c.Result.Error == "" || r.payload.Effects[0].Status != state.EffectCancelled {
		t.Fatal("expired pure credential computation leaked setup or falsely declared owner dead")
	}
}

func TestStateOwnerSubscriptionControllerProofDoesNotRefreshLeaseOrFlow(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	initial := stateTestRequest("SUBSCRIBE", 1, "", "control-only-subscription")
	wire := strings.Replace(string(initial.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 90\r\nContent-Length:", 1)
	initial, err := Observe([]byte(wire), false)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := a.Locate(context.Background(), initial)
	if err != nil {
		t.Fatal(err)
	}
	appA := &pb.ProcessRef{InstanceId: "controller-a", Epoch: "a"}
	appB := &pb.ProcessRef{InstanceId: "controller-b", Epoch: "b"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "accepted", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, a, ref, "initial-subscription", stateEngineInput{Kind: "ingest", Application: appA, Ingest: &pb.OwnerIngest{Context: ref, Application: appA, Flow: flow, Wire: initial.Wire, PrincipalId: "principal", BindingId: "binding"}})
	_, group := stateTestLoad(t, a, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	accepted := stateTestApply(t, a, ref, "accept-subscription", stateEngineInput{Kind: "command", Application: appA, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}}}})
	if accepted.Error != "" {
		t.Fatal(accepted.Error)
	}
	_, before := stateTestLoad(t, a, ref)
	old := before.Contexts[ref.Id]
	ready, _, ctx, _ := stateControllerPair(t, b, appA, appB)
	notify := &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Body: []byte("Messages-Waiting: no\r\n")}}}
	in := stateEngineInput{Kind: "command", Context: ref, Application: appB, Command: notify, Operation: "standalone-notify"}
	stateRef, err := b.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.preflightControl(ctx, stateRef, in, time.Now().Add(2*time.Second)) }()
	query := readControllerEnvelope(t, ctx, ready)
	probe := query.GetProbeApplication()
	if probe == nil || !sameProcess(probe.Application, appB) {
		t.Fatal("standalone NOTIFY did not ask for exact controller proof")
	}
	writeControllerEnvelope(t, ctx, ready, &pb.Envelope{SessionId: query.SessionId, CorrelationId: query.CorrelationId, Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: probe.Context, Application: appB, DirectoryEpoch: "directory", Live: true, CheckedAtMs: uint64(time.Now().UnixMilli())}}})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_, adopted := stateTestLoad(t, a, ref)
	c := adopted.Contexts[ref.Id]
	if !sameProcess(c.Snapshot.Application, appB) || c.OwnerLost || !proto.Equal(c.Flow, old.Flow) || !proto.Equal(c.Delivery, old.Delivery) || c.DeliveryRevision != old.DeliveryRevision ||
		c.Subscription.ExpiresMS != old.Subscription.ExpiresMS || c.Subscription.Target != old.Subscription.Target || c.Subscription.RemoteSequence != old.Subscription.RemoteSequence ||
		c.Subscription.LocalSequence != old.Subscription.LocalSequence || c.Snapshot.CommandSequence != old.Snapshot.CommandSequence {
		t.Fatal("control-only proof mutated accepted SIP state, sequence, route or lease")
	}
	denied := stateTestApply(t, a, ref, "delayed-old-controller-notify", stateEngineInput{Kind: "command", Application: appA, Command: notify})
	if denied.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("delayed old controller could notify after control-only adoption")
	}
	allowed := stateTestApply(t, b, ref, "standalone-notify", in)
	if allowed.Error != "" || allowed.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatalf("new controller could not notify without handset renewal: %+v", allowed)
	}
	_, notified := stateTestLoad(t, a, ref)
	if notified.Contexts[ref.Id].Subscription.LocalSequence != old.Subscription.LocalSequence+1 {
		t.Fatal("standalone NOTIFY did not allocate one native CSeq")
	}
}

func TestStateOwnerLossCancelsPendingUACAndCompletesLateSuccess(t *testing.T) {
	r, c, tx := ownerFixture(t, false)
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
		t.Fatal(err)
	}
	r.in = ownerStatusInput(t, r, c, c.Snapshot.Application, false)
	if err := r.ownerProbeResult(); err != nil || !c.OwnerLost || !tx.Native.Cancelled {
		t.Fatalf("pending UAC was not natively cancelled: %v", err)
	}
	provisional := forkReply(t, tx, 180, "ringing")
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: provisional}); err != nil {
		t.Fatal(err)
	}
	_, _, cancel := causalEffect(t, r, "CANCEL", "")
	if cancel.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || cancel.Authorization.CompletionScope != "initial-scope" {
		t.Fatal("pending cancellation required another application owner")
	}
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: forkReply(t, tx, 200, "late")}); err != nil {
		t.Fatal(err)
	}
	_, _, ack := causalEffect(t, r, "ACK", "late")
	_, _, bye := causalEffect(t, r, "BYE", "late")
	if ack.Authorization.AuthorityClass != pb.EffectAuthorization_NATIVE_COMPLETION || bye.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || bye.AfterACK == "" {
		t.Fatal("late ownerless success lost ACK-before-BYE cleanup")
	}
}

func TestStateOwnerSubscriptionControlProofRejectsAbsenceAndChangedEpoch(t *testing.T) {
	r, c, _ := ownerFixture(t, false)
	c.Subscription = &stateSubscription{Event: "message-summary", ExpiresMS: r.now + 60000, Target: "sip:phone@example.test"}
	c.Snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
	old := proto.Clone(c.Snapshot.Application).(*pb.ProcessRef)
	next := &pb.ProcessRef{InstanceId: "new-controller", Epoch: "new"}
	proof := &pb.ApplicationStatus{Context: c.Snapshot.Context, Application: next, DirectoryEpoch: "directory", Live: false, CheckedAtMs: uint64(r.now)}
	r.in = stateEngineInput{Kind: "owner-control", Context: c.Snapshot.Context, Application: next, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: proof}}}
	if err := r.ownerControlResult(); !errors.Is(err, errStateApplicationOwner) || !sameProcess(c.Snapshot.Application, old) {
		t.Fatal("negative control proof adopted an unrelated live application")
	}
	proof.Live, proof.DirectoryEpoch = true, "changed-directory"
	if err := r.ownerControlResult(); !errors.Is(err, errStateApplicationOwner) || !sameProcess(c.Snapshot.Application, old) {
		t.Fatal("changed directory epoch authorized subscription takeover")
	}
	proof.DirectoryEpoch = "directory"
	if err := r.ownerControlResult(); err != nil || !sameProcess(c.Snapshot.Application, next) {
		t.Fatalf("exact positive proof did not adopt controller: %v", err)
	}
	stale := stateTestRequest("SUBSCRIBE", 1, c.Tag, "stale-control-only")
	staleWire := strings.Replace(string(stale.Wire), "Content-Length:", "Event: message-summary\r\nContent-Length:", 1)
	parsed, err := (portable.Message{Wire: []byte(staleWire), Transport: "tls"}).Parse()
	if err != nil {
		t.Fatal(err)
	}
	c.Snapshot.LocalTag, c.Snapshot.RemoteTag = tag(parsed.To().Params), tag(parsed.From().Params)
	c.Snapshot.LocalUri, c.Snapshot.RemoteUri = parsed.To().Address.String(), parsed.From().Address.String()
	c.Subscription.RemoteSequence = 1
	validation := r.validateSubscription(c, parsed.(*sip.Request), &pb.OwnerIngest{})
	var staleSequence *portable.ProtocolError
	if !errors.As(validation, &staleSequence) || staleSequence.Status != 500 || !sameProcess(c.Snapshot.Application, next) {
		t.Fatal("SIP rejection rolled back approved control-only adoption")
	}
}

func TestStateOwnerCleanupUsesLatestAcceptedWinnerFlow(t *testing.T) {
	r, c, _ := ownerFixture(t, true)
	original := proto.Clone(c.Flow).(*pb.FlowRef)
	original.ConnectionId = "response-only-original"
	losing := proto.Clone(original).(*pb.FlowRef)
	losing.ConnectionId = "losing-branch"
	c.DialogCompletionFlows = map[string]*pb.FlowRef{c.Dialog: original, "local/loser": losing}
	accepted := proto.Clone(c.Flow).(*pb.FlowRef)
	accepted.ConnectionId = "accepted-in-dialog-reconnect"
	if err := r.proposeFlow(c, nil, accepted, c.Snapshot.Application, false); err != nil {
		t.Fatal(err)
	}
	effect := r.payload.Effects[len(r.payload.Effects)-1]
	index, effect, body, err := r.findEffect(effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	proposal := body.Envelope.GetFlowAdoption()
	r.in = stateEngineInput{Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: accepted, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}}}}}
	if err := r.finishFlowAdoption(index, effect, body, c); err != nil {
		t.Fatal(err)
	}
	r.in = ownerStatusInput(t, r, c, c.Snapshot.Application, false)
	if err := r.ownerProbeResult(); err != nil {
		t.Fatal(err)
	}
	_, _, bye := causalEffect(t, r, "BYE", "remote")
	if !proto.Equal(bye.Flow, accepted) || !proto.Equal(c.DialogCompletionFlows["local/loser"], losing) {
		t.Fatal("owner cleanup preferred stale response flow or changed another branch")
	}
}

func TestStateOwnerResponsePreflightDoesNotRequireControllerHandoff(t *testing.T) {
	r, c, tx := ownerFixture(t, false)
	response := forkReply(t, tx, 200, "native-response")
	// No store or controller exists in this fixture. Pure response completion
	// must reach native transaction matching instead of performing business RPC.
	e := &StateEngine{}
	in := stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: &pb.ProcessRef{InstanceId: "relay", Epoch: "other"}, Ingest: &pb.OwnerIngest{Wire: response.Wire, Flow: c.Flow}}
	if _, err := e.preflightSubscriptionController(context.Background(), r.record.Ref, in, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("native response attempted controller handoff: %v", err)
	}
}

func ownerUnboundSetupFixture(t *testing.T) (*stateReduction, *stateContext) {
	t.Helper()
	r, c, _ := ownerFixture(t, false)
	c.OwnerValidated, c.OwnerDirectoryEpoch, c.CompletionScope = false, "", ""
	c.Transactions = make(map[string]*stateTransaction)
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
	return r, c
}

func TestStateOwnerOpenComputationAndTerminalNotificationHaveFixedWindows(t *testing.T) {
	for _, outcome := range []string{"unknown", "error", "pending-expiry"} {
		t.Run(outcome, func(t *testing.T) {
			r, c := ownerUnboundSetupFixture(t)
			body, _ := json.Marshal(stateEngineEffect{Context: c.Snapshot.Context.Id, Application: c.Snapshot.Application, Executor: r.engine.cfg.Process, Command: c.Operation})
			r.payload.Effects = []state.Effect{{ID: "open", Kind: "open", Status: state.EffectDispatching, AttemptID: "transport-compute", DeadlineMS: 1000, Payload: body}}
			switch outcome {
			case "unknown":
				r.now = 120999
				if err := r.expireOwnerComputations(); err != nil || c.Snapshot.Ended || r.payload.Effects[0].Status != state.EffectDispatching {
					t.Fatal("transport computation deadline moved or ended before its reconciliation window")
				}
				r.now = 121000
				if err := r.expireOwnerComputations(); err != nil || !c.Snapshot.Ended || c.Result.Error != "SIP setup transport deadline elapsed" || r.payload.Effects[0].Status != state.EffectCancelled {
					t.Fatalf("expired pure transport computation pinned setup: %v", err)
				}
			case "error":
				r.in = stateEngineInput{Effect: "open", Executor: r.engine.cfg.Process, Failure: "transport unavailable"}
				if err := r.effectResult(); err != nil || !c.Snapshot.Ended {
					t.Fatalf("transport failure fixture: %v", err)
				}
			case "pending-expiry":
				r.payload.Effects[0].Status = state.EffectPending
				r.in = stateEngineInput{Timer: r.timerID("group", "effects", "recovery", "")}
				if _, err := r.lifecycleTimer(); err != nil || !c.Snapshot.Ended {
					t.Fatalf("pending transport expiry fixture: %v", err)
				}
			}
			var notification state.Effect
			for index, effect := range r.payload.Effects {
				if effect.Kind == "event" {
					effect.Status, effect.AttemptID = state.EffectUnknown, "notification-attempt"
					r.payload.Effects[index], notification = effect, effect
				}
			}
			if notification.ID == "" || c.OwnerLost || c.Result == nil || c.Result.Error == "" {
				t.Fatal("terminal setup did not retain bounded failure result and notification")
			}
			result := proto.Clone(c.Result).(*pb.OperationResult)
			r.now = notification.DeadlineMS + 119999
			if err := r.expireOwnerComputations(); err != nil {
				t.Fatal(err)
			}
			_, before, _, err := r.findEffect(notification.ID)
			if err != nil || before.Status != state.EffectUnknown {
				t.Fatal("terminal notification retired before its fixed reporting window")
			}
			r.now++
			if err := r.expireOwnerComputations(); err != nil {
				t.Fatal(err)
			}
			_, after, _, err := r.findEffect(notification.ID)
			if err != nil || after.Status != state.EffectCancelled || !proto.Equal(result, c.Result) || c.OwnerLost {
				t.Fatal("bounded failed-setup reporting leaked or discarded durable command result")
			}
		})
	}
}

func TestStateOwnerTerminalNotificationNeverExpiresActiveOrPhysicalWork(t *testing.T) {
	for _, protected := range []string{"active", "directory-anchor", "physical-unknown", "authorization-unknown"} {
		t.Run(protected, func(t *testing.T) {
			r, c := ownerUnboundSetupFixture(t)
			c.Snapshot.Ended = protected != "active"
			if protected == "directory-anchor" {
				c.OwnerDirectoryEpoch = "directory"
			}
			r.event(c, pb.DialogEvent_FAILED, nil, c.Operation)
			event := r.payload.Effects[0]
			event.Status, event.AttemptID = state.EffectUnknown, "reporting"
			r.payload.Effects[0] = event
			if protected == "physical-unknown" || protected == "authorization-unknown" {
				kind := "deliver"
				if protected == "authorization-unknown" {
					kind = "wire"
				}
				body, _ := json.Marshal(stateEngineEffect{Context: c.Snapshot.Context.Id, Application: c.Snapshot.Application})
				r.payload.Effects = append(r.payload.Effects, state.Effect{ID: "protected", Kind: kind, Status: state.EffectUnknown, AttemptID: "physical", DeadlineMS: 1, Payload: body})
			}
			r.now = event.DeadlineMS + 120000
			if err := r.expireOwnerComputations(); err != nil {
				t.Fatal(err)
			}
			for _, effect := range r.payload.Effects {
				if effect.Status != state.EffectUnknown {
					t.Fatal("bounded reporting policy abandoned active, anchored or physically uncertain work")
				}
			}
		})
	}
}

func TestStateOwnerEndedContextProbesUntilPolicyResolvedAndDropsPureQuery(t *testing.T) {
	r, c := ownerUnboundSetupFixture(t)
	c.Snapshot.Ended, c.OwnerValidated, c.OwnerDirectoryEpoch = true, true, "directory"
	r.event(c, pb.DialogEvent_FAILED, nil, c.Operation)
	r.payload.Effects[0].Status, r.payload.Effects[0].AttemptID = state.EffectUnknown, "reporting"
	if err := r.appendOwnerTimers(); err != nil || len(r.payload.Timers) == 0 {
		t.Fatalf("ended context stopped probing unresolved policy: %v", err)
	}
	r.now = c.OwnerProbeAtMS
	r.in = stateEngineInput{Timer: r.timerID(c.Snapshot.Context.Id, "owner", "probe", ""), Generation: strconv.FormatInt(c.OwnerProbeAtMS, 10)}
	if handled, err := r.ownerTimer(); err != nil || !handled || c.OwnerProbeEffect == "" {
		t.Fatalf("ended context did not query its retained owner: %v", err)
	}
	r.in = ownerStatusInput(t, r, c, c.Snapshot.Application, false)
	if err := r.ownerProbeResult(); err != nil || !c.OwnerLost || r.payload.Effects[0].Status != state.EffectCancelled {
		t.Fatalf("ended context could not abandon dead-epoch reporting: %v", err)
	}
	r, c = ownerUnboundSetupFixture(t)
	c.Snapshot.Ended = true
	if err := r.ensureOwnerProbe(c, c.Snapshot.Application); err != nil {
		t.Fatal(err)
	}
	if err := r.appendOwnerTimers(); err != nil || c.OwnerProbeEffect != "" || len(r.payload.Effects) != 0 {
		t.Fatalf("unneeded pure owner query pinned terminal setup: %v", err)
	}
}

func TestStateOwnerOpenExpiryPreservesSeparatelyAdmittedPhysicalUnknown(t *testing.T) {
	r, c := ownerUnboundSetupFixture(t)
	body, _ := json.Marshal(stateEngineEffect{Context: c.Snapshot.Context.Id, Application: c.Snapshot.Application})
	r.payload.Effects = []state.Effect{{ID: "open", Kind: "open", Status: state.EffectUnknown, AttemptID: "compute", DeadlineMS: 1, Payload: body}, {ID: "physical", Kind: "deliver", Status: state.EffectUnknown, AttemptID: "socket-attempt", DeadlineMS: 1, Payload: body}}
	r.now = 121001
	if err := r.expireOwnerComputations(); err != nil || !c.Snapshot.Ended || r.payload.Effects[0].Status != state.EffectCancelled || r.payload.Effects[1].Status != state.EffectUnknown {
		t.Fatalf("pure computation expiry changed a separately admitted socket attempt: %v", err)
	}
}
