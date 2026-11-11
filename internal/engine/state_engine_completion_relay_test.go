package engine

import (
	"bytes"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

var completionRelayApplication = &pb.ProcessRef{InstanceId: "controller-b", Epoch: "relay-epoch"}

func completionRelayIngest(r *stateReduction, c *stateContext, message portable.Message, flow *pb.FlowRef, admission pb.OwnerIngest_Admission) error {
	r.deadline = r.now + 2000 // Each synthetic receive is a new immutable control input.
	r.in = stateEngineInput{Kind: "ingest", Application: completionRelayApplication, Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: completionRelayApplication, Flow: flow, Wire: message.Wire, Admission: admission}}
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	return r.ingest()
}

func completionRelayUAS(t *testing.T) (*stateReduction, *stateContext, portable.Dialog, portable.Message) {
	t.Helper()
	r, c, tx := ownerFixture(t, false)
	capacityEngine, _ := historicalStateTestEngine(t)
	r.engine.store = capacityEngine.store
	response := forkReply(t, tx, 200, "server")
	dialog, err := portable.NewDialog(portable.UAS, tx.Native.Origin, response, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogSendResponse, Now: r.now, Message: response})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := portable.NewDialog(portable.UAC, tx.Native.Origin, response, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	c.Tag = dialog.LocalTag
	c.Dialog = dialog.LocalTag + "/" + dialog.RemoteTag
	c.Dialogs = map[string]portable.Dialog{c.Dialog: dialog}
	c.DialogScopes = map[string]string{c.Dialog: "uas-scope"}
	c.Transactions = make(map[string]*stateTransaction)
	c.Snapshot.Kind, c.Snapshot.Answered = pb.ContextSnapshot_UAS, true
	c.Snapshot.LocalTag, c.Snapshot.RemoteTag = dialog.LocalTag, dialog.RemoteTag
	r.deadline = r.now + 2000
	ack, err := portable.BuildDialogRequest(remote, sip.ACK, 12, nil, "z9hG4bK-relay-ack")
	if err != nil {
		t.Fatal(err)
	}
	return r, c, remote, ack
}

func TestStateCompletionRelayACKSeparatesOriginalOwnerPublicationFromCleanup(t *testing.T) {
	for _, window := range []string{"live", "lease-grace", "owner-lost"} {
		t.Run(window, func(t *testing.T) {
			r, c, _, ack := completionRelayUAS(t)
			original, old := proto.Clone(c.Snapshot.Application).(*pb.ProcessRef), proto.Clone(c.Flow).(*pb.FlowRef)
			flow := proto.Clone(old).(*pb.FlowRef)
			flow.ConnectionId = "relay-replacement"
			if window == "owner-lost" {
				r.in = ownerStatusInput(t, r, c, original, false)
				if err := r.ownerProbeResult(); err != nil {
					t.Fatal(err)
				}
			}
			if err := completionRelayIngest(r, c, ack, flow, pb.OwnerIngest_TLS_DIALOG_CONTINUITY); err != nil {
				t.Fatal(err)
			}
			if !sameProcess(c.Snapshot.Application, original) || !c.Snapshot.InitialAck || !proto.Equal(c.Flow, old) || !proto.Equal(c.DialogCompletionFlows[c.Dialog], flow) {
				t.Fatal("relay lost completion evidence or changed ordinary owner/flow before receipt")
			}
			var adoption state.Effect
			for _, effect := range r.payload.Effects {
				var body stateEngineEffect
				if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
					t.Fatal(err)
				}
				if !sameProcess(body.Application, original) {
					t.Fatal("effect escaped original owner")
				}
				if effect.Kind == "present" {
					t.Fatal("ACK created business delivery")
				}
				if effect.Kind == "adopt" {
					adoption = effect
					if !sameProcess(body.Envelope.GetFlowAdoption().Application, original) {
						t.Fatal("relay became publication owner")
					}
				}
			}
			beforeReplay, _ := marshalRetainedFixture(c)
			beforeEffects := len(r.payload.Effects)
			if err := completionRelayIngest(r, c, ack, old, pb.OwnerIngest_EXISTING_FLOW); err != nil {
				t.Fatal(err)
			}
			afterReplay, _ := marshalRetainedFixture(c)
			if !bytes.Equal(beforeReplay, afterReplay) || len(r.payload.Effects) != beforeEffects {
				t.Fatal("old-flow ACK replay replaced completion or allocated publication")
			}
			if window == "live" {
				if adoption.ID == "" {
					t.Fatal("live original owner has no separate publication")
				}
				index, effect, body, err := r.findEffect(adoption.ID)
				if err != nil {
					t.Fatal(err)
				}
				proposal := body.Envelope.GetFlowAdoption()
				r.in = stateEngineInput{Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}}}}}
				if err := r.finishFlowAdoption(index, effect, body, c); err != nil {
					t.Fatal(err)
				}
				published, _ := marshalRetainedFixture(c)
				publishedEffects := len(r.payload.Effects)
				if err := completionRelayIngest(r, c, ack, old, pb.OwnerIngest_TLS_DIALOG_CONTINUITY); err != nil {
					t.Fatal(err)
				}
				replayed, _ := marshalRetainedFixture(c)
				if !bytes.Equal(published, replayed) || len(r.payload.Effects) != publishedEffects {
					t.Fatal("old-socket ACK rolled back validated original-owner publication")
				}
				r.in = stateEngineInput{Application: original, Operation: "app-bye", Command: &pb.Command{Context: c.Snapshot.Context, Sequence: c.Snapshot.CommandSequence + 1, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "BYE"}}}}
				if err := r.command(); err != nil {
					t.Fatal(err)
				}
				_, _, bye := causalEffect(t, r, "BYE", c.Snapshot.RemoteTag)
				if !proto.Equal(bye.Flow, flow) {
					t.Fatal("original owner's later BYE ignored its validated receipt")
				}
			} else {
				if window == "owner-lost" && adoption.ID != "" {
					t.Fatal("fenced owner received a new publication")
				}
				if window == "lease-grace" {
					if c.OwnerLost {
						t.Fatal("relay arrival guessed owner death")
					}
					r.in = ownerStatusInput(t, r, c, original, false)
					if err := r.ownerProbeResult(); err != nil {
						t.Fatal(err)
					}
				}
				if err := r.continueOwnerLoss(c); err != nil {
					t.Fatal(err)
				}
				_, _, bye := causalEffect(t, r, "BYE", c.Snapshot.RemoteTag)
				if !proto.Equal(bye.Flow, flow) || bye.Authorization.CompletionScope != "uas-scope" {
					t.Fatal("cleanup required dead-owner publication")
				}
				for _, effect := range r.payload.Effects {
					if effect.ID == adoption.ID && effect.Status != state.EffectCancelled {
						t.Fatal("owner loss retained stale publication")
					}
				}
			}
		})
	}
}

func TestStateCompletionRelayRejectsBusinessAndForeignNativeIdentityWithoutMutation(t *testing.T) {
	for _, scenario := range []string{"INVITE", "UPDATE", "REGISTER", "INFO", "tag", "sequence", "unadmitted-flow", "transport", "principal"} {
		t.Run(scenario, func(t *testing.T) {
			r, c, remote, message := completionRelayUAS(t)
			flow := proto.Clone(c.Flow).(*pb.FlowRef)
			admission := pb.OwnerIngest_TLS_DIALOG_CONTINUITY
			switch scenario {
			case "INVITE", "UPDATE", "REGISTER", "INFO":
				var err error
				message, err = portable.BuildDialogRequest(remote, sip.RequestMethod(scenario), 13, nil, "z9hG4bK-relay-invalid")
				if err != nil {
					t.Fatal(err)
				}
			case "tag":
				message.Wire = []byte(strings.Replace(string(message.Wire), "tag=server", "tag=foreign", 1))
			case "sequence":
				message.Wire = []byte(strings.Replace(string(message.Wire), "CSeq: 12 ACK", "CSeq: 13 ACK", 1))
			case "unadmitted-flow":
				flow.ConnectionId, admission = "unknown", pb.OwnerIngest_EXISTING_FLOW
			case "transport":
				flow.Transport = "tcp"
			case "principal":
				c.Snapshot.PrincipalId = "retained"
			}
			before, _ := marshalRetainedFixture(c)
			if scenario == "principal" {
				r.in = stateEngineInput{Application: completionRelayApplication, Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: completionRelayApplication, Flow: flow, Wire: message.Wire, PrincipalId: "foreign"}}
				if err := r.ingest(); err == nil {
					t.Fatal("foreign principal accepted")
				}
			} else if err := completionRelayIngest(r, c, message, flow, admission); err == nil {
				t.Fatal("invalid relay accepted")
			}
			after, _ := marshalRetainedFixture(c)
			if !bytes.Equal(before, after) || len(r.payload.Effects) != 0 {
				t.Fatal("invalid relay mutated native state or effects")
			}
		})
	}
}

func TestStateCompletionRelayRemoteBYEAndACKReplayNeverPresentToRelay(t *testing.T) {
	r, c, remote, ack := completionRelayUAS(t)
	if err := completionRelayIngest(r, c, ack, c.Flow, pb.OwnerIngest_EXISTING_FLOW); err != nil {
		t.Fatal(err)
	}
	if err := completionRelayIngest(r, c, ack, c.Flow, pb.OwnerIngest_EXISTING_FLOW); err != nil {
		t.Fatal(err)
	}
	bye, err := portable.BuildDialogRequest(remote, sip.BYE, 13, nil, "z9hG4bK-relay-bye")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := completionRelayIngest(r, c, bye, c.Flow, pb.OwnerIngest_EXISTING_FLOW); err != nil {
			t.Fatal(err)
		}
	}
	for _, tx := range c.Transactions {
		if tx.Application != nil && !sameProcess(tx.Application, c.Snapshot.Application) {
			t.Fatal("relay owns transaction")
		}
	}
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if effect.Kind == "present" || !sameProcess(body.Application, c.Snapshot.Application) {
			t.Fatal("relay adopted native BYE")
		}
	}
}

func TestStateCompletionRelayCANCELAndNon2xxACKRequireExactRetainedServerTransaction(t *testing.T) {
	for _, method := range []string{"CANCEL", "ACK"} {
		for _, foreign := range []string{"", "branch", "tag", "sequence"} {
			t.Run(method+"/"+foreign, func(t *testing.T) {
				r, c, tx := ownerFixture(t, false)
				r.deadline = r.now + 2000 // Real retained inputs always carry their original budget.
				native, err := portable.NewTransaction(portable.InviteServer, tx.Native.Origin, portable.DefaultTimers(), "server-final")
				if err != nil {
					t.Fatal(err)
				}
				native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
				if err != nil {
					t.Fatal(err)
				}
				tx.Native = native
				c.Tag, c.Snapshot.Kind = "server-final", pb.ContextSnapshot_UAS
				c.Transactions = map[string]*stateTransaction{native.Key: tx}
				var message portable.Message
				if method == "ACK" {
					final := forkReply(t, tx, 487, "server-final")
					tx.Native, _, err = portable.StepTransaction(tx.Native, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: final})
					if err != nil {
						t.Fatal(err)
					}
					message, err = portable.BuildNon2xxACK(tx.Native.Origin, final)
				} else {
					message, err = portable.BuildCancel(tx.Native.Origin)
				}
				if err != nil {
					t.Fatal(err)
				}
				switch foreign {
				case "branch":
					message.Wire = []byte(strings.Replace(string(message.Wire), "z9hG4bK-initial", "z9hG4bK-foreign", 1))
				case "tag":
					if method == "ACK" {
						message.Wire = []byte(strings.Replace(string(message.Wire), "tag=server-final", "tag=foreign", 1))
					} else {
						message.Wire = []byte(strings.Replace(string(message.Wire), "tag=local", "tag=foreign", 1))
					}
				case "sequence":
					message.Wire = []byte(strings.Replace(string(message.Wire), "CSeq: 12 ", "CSeq: 13 ", 1))
				}
				before, _ := marshalRetainedFixture(c)
				err = completionRelayIngest(r, c, message, c.Flow, pb.OwnerIngest_EXISTING_FLOW)
				if foreign == "" {
					if err != nil {
						t.Fatal(err)
					}
					for _, effect := range r.payload.Effects {
						var body stateEngineEffect
						if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
							t.Fatal(err)
						}
						if !sameProcess(body.Application, c.Snapshot.Application) || effect.Kind == "present" {
							t.Fatal("completion changed application owner")
						}
					}
				} else {
					if err == nil {
						t.Fatal("foreign transaction completion accepted")
					}
					after, _ := marshalRetainedFixture(c)
					if !bytes.Equal(before, after) || len(r.payload.Effects) != 0 {
						t.Fatal("foreign completion mutated retained transaction")
					}
				}
			})
		}
	}
}

func TestStateCompletionRelayResponseRejectsForeignTransactionAndUnadmittedFlow(t *testing.T) {
	for _, foreign := range []string{"branch", "from-tag", "sequence", "unadmitted-flow", "transport"} {
		t.Run(foreign, func(t *testing.T) {
			r, c, tx := ownerFixture(t, false)
			message := forkReply(t, tx, 200, "late")
			flow := proto.Clone(c.Flow).(*pb.FlowRef)
			admission := pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY
			switch foreign {
			case "branch":
				message.Wire = []byte(strings.Replace(string(message.Wire), "z9hG4bK-initial", "z9hG4bK-foreign", 1))
			case "from-tag":
				message.Wire = []byte(strings.Replace(string(message.Wire), "tag=local", "tag=foreign", 1))
			case "sequence":
				message.Wire = []byte(strings.Replace(string(message.Wire), "CSeq: 12 INVITE", "CSeq: 13 INVITE", 1))
			case "unadmitted-flow":
				flow.ConnectionId, admission = "unknown", pb.OwnerIngest_EXISTING_FLOW
			case "transport":
				flow.Transport = "tcp"
			}
			before, _ := marshalRetainedFixture(c)
			if err := completionRelayIngest(r, c, message, flow, admission); err == nil {
				t.Fatal("foreign response accepted")
			}
			after, _ := marshalRetainedFixture(c)
			if !bytes.Equal(before, after) || len(r.payload.Effects) != 0 {
				t.Fatal("foreign response mutated retained state")
			}
		})
	}
}

func TestStateCompletionRelayLateForksAcrossRedisWorkersPreserveOriginalOwner(t *testing.T) {
	a, b := historicalStateTestEngine(t)
	owner := &pb.ProcessRef{InstanceId: "controller-a", Epoch: "original-owner"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "old-a", SourceIp: "192.0.2.10", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	ref := stateOpenOnFlow(t, a, b, owner, flow)
	record, _ := stateTestLoad(t, a, ref)
	var initialEffect state.Effect
	var initialBody stateEngineEffect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "wire" {
			initialEffect = effect
			if err := unmarshalRetainedFixture(effect.Payload, &initialBody); err != nil {
				t.Fatal(err)
			}
		}
	}
	if initialEffect.ID == "" || initialBody.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
		t.Fatal("initial INVITE application intent unavailable")
	}
	const originalScope = "relay-original-invite"
	permit := &pb.SendPermit{Authorization: initialBody.Authorization, Flow: flow, DirectoryEpoch: "directory", OrderingProof: "proof", CompletionScope: originalScope}
	stateTestApply(t, a, ref, "relay-start-authorize", stateEngineInput{Kind: "effect-start", Effect: initialEffect.ID})
	authorized := stateTestApply(t, a, ref, "relay-authorize", stateEngineInput{Kind: "effect-result", Effect: initialEffect.ID, Executor: a.cfg.Process, Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}})
	if authorized.Error != "" {
		t.Fatal(authorized.Error)
	}
	result := stateTestApply(t, a, ref, "relay-cancel", stateEngineInput{Kind: "command", Application: owner, Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, b, ref)
	c := group.Contexts[ref.Id]
	var initial *stateTransaction
	for _, tx := range c.Transactions {
		if tx.Native.Family == portable.InviteClient {
			initial = tx
		}
	}
	if initial == nil {
		t.Fatal("initial transaction unavailable")
	}
	replacement := proto.Clone(flow).(*pb.FlowRef)
	replacement.Ingress, replacement.ConnectionId = b.cfg.Process, "new-b"
	for _, remote := range []string{"late-one", "late-two"} {
		message := forkReply(t, initial, 200, remote)
		result = stateTestApply(t, b, ref, "relay-"+remote, stateEngineInput{Kind: "ingest", Application: completionRelayApplication, Ingest: &pb.OwnerIngest{Context: ref, Application: completionRelayApplication, Flow: replacement, Wire: message.Wire, Admission: pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY}})
		if result.Error != "" {
			t.Fatal(result.Error)
		}
		record, loaded := stateTestLoad(t, a, ref)
		retained := loaded.Contexts[ref.Id]
		if !sameProcess(retained.Snapshot.Application, owner) || retained.Snapshot.Answered || retained.Dialog != "" || !proto.Equal(retained.Flow, flow) {
			t.Fatal("late fork migrated or answered cancelled context")
		}
		for _, method := range []string{"ACK", "BYE"} {
			wire := stateWireBody(t, record, method, remote)
			if !sameProcess(wire.Application, owner) || !proto.Equal(wire.Flow, replacement) || wire.Authorization.CompletionScope != originalScope {
				t.Fatal("late branch lost original owner, scope or completion flow")
			}
		}
	}
	result = stateTestApply(t, b, ref, "relay-business", stateEngineInput{Kind: "command", Application: completionRelayApplication, Command: &pb.Command{Context: ref, Sequence: 3, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "INVITE"}}}})
	if result.Error == "" {
		t.Fatal("relay received application command authority")
	}
}

func completionRelayFinalFixture(t *testing.T, method sip.RequestMethod, status uint32) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, remote, ack := completionRelayUAS(t)
	if err := completionRelayIngest(r, c, ack, c.Flow, pb.OwnerIngest_EXISTING_FLOW); err != nil {
		t.Fatal(err)
	}
	request, err := portable.BuildDialogRequest(remote, method, 13, []byte("v=0\r\n"), "z9hG4bK-cached-final")
	if err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Kind: "ingest", Application: c.Snapshot.Application, Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: request.Wire}}
	r.deadline = r.now + 2000
	if err := r.ingest(); err != nil {
		t.Fatal(err)
	}
	var selected *stateTransaction
	for _, tx := range c.Transactions {
		if bytes.Equal(tx.Native.Origin.Wire, request.Wire) {
			selected = tx
		}
	}
	if selected == nil {
		t.Fatal("in-dialog server transaction not retained")
	}
	r.deadline = r.now + 20000
	r.in = stateEngineInput{Kind: "command", Application: c.Snapshot.Application, Operation: "cached-final", Command: &pb.Command{Context: c.Snapshot.Context, Sequence: c.Snapshot.CommandSequence + 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: selected.RequestID, Status: status, Body: []byte("v=0\r\n")}}}}
	if err := r.command(); err != nil {
		t.Fatal(err)
	}
	return r, c, selected
}

func TestStateCompletionRelayCachedFinalReplayPreservesUnknownAndBusinessState(t *testing.T) {
	for _, scenario := range []struct {
		method sip.RequestMethod
		status uint32
	}{{sip.INFO, 200}, {sip.UPDATE, 488}, {sip.INVITE, 200}} {
		t.Run(string(scenario.method), func(t *testing.T) {
			r, c, tx := completionRelayFinalFixture(t, scenario.method, scenario.status)
			_, original, _ := latestFinalEffect(t, r)
			completeLatestResponse(t, r, pb.DeliveryResult_UNKNOWN)
			beforeSnapshot := proto.Clone(c.Snapshot).(*pb.ContextSnapshot)
			beforeDialogs, _ := marshalRetainedFixture(c.Dialogs)
			oldFlow := proto.Clone(c.Flow).(*pb.FlowRef)
			oldDeadline, oldPhase := tx.RetireMS, tx.Native.Phase
			beforeEffects := len(r.payload.Effects)
			flow := proto.Clone(c.Flow).(*pb.FlowRef)
			flow.ConnectionId, flow.SourcePort = "relay-final-return", flow.SourcePort+1
			if err := completionRelayIngest(r, c, tx.Native.Origin, flow, pb.OwnerIngest_TLS_DIALOG_CONTINUITY); err != nil {
				t.Fatal(err)
			}
			_, replay, body := latestFinalEffect(t, r)
			if replay.ID == original.ID || body.WireStatus != scenario.status || !proto.Equal(body.Flow, flow) || !sameProcess(body.Application, c.Snapshot.Application) {
				t.Fatal("cached final did not return on admitted relay flow with original identity")
			}
			expected, err := stateRenderResponse(*tx.Native.LastResponse, flow)
			if err != nil || !bytes.Equal(expected.Wire, body.Wire) {
				t.Fatal("relay changed selected final bytes beyond native return-flow rendering")
			}
			_, retained, _, err := r.findEffect(original.ID)
			if err != nil || retained.Status != state.EffectUnknown {
				t.Fatal("explicit replay settled old physical uncertainty")
			}
			afterDialogs, _ := marshalRetainedFixture(c.Dialogs)
			if !proto.Equal(c.Snapshot, beforeSnapshot) || !bytes.Equal(beforeDialogs, afterDialogs) || !proto.Equal(c.Flow, oldFlow) || tx.RetireMS != oldDeadline || tx.Native.Phase != oldPhase {
				t.Fatal("cached replay changed business, dialog, ordinary route or transaction lifetime")
			}
			for _, effect := range r.payload.Effects[beforeEffects:] {
				if effect.Kind != "wire" {
					t.Fatal("cached replay allocated presentation or publication work")
				}
			}
		})
	}
}

func TestStateCompletionRelayCachedFinalRejectsFreshAlteredAndExcludedRequests(t *testing.T) {
	for _, scenario := range []string{"payload", "branch", "sequence", "to-tag", "from-tag", "no-final", "provisional", "wrong-final", "unadmitted-flow", "REGISTER", "SUBSCRIBE", "NOTIFY", "subscription", "transaction-context", "out-of-dialog"} {
		t.Run(scenario, func(t *testing.T) {
			method, status := sip.INFO, uint32(200)
			if scenario == "REGISTER" || scenario == "SUBSCRIBE" || scenario == "NOTIFY" {
				method, status = sip.RequestMethod(scenario), 488
			}
			r, c, tx := completionRelayFinalFixture(t, method, status)
			message := tx.Native.Origin.Clone()
			flow := proto.Clone(c.Flow).(*pb.FlowRef)
			admission := pb.OwnerIngest_TLS_DIALOG_CONTINUITY
			switch scenario {
			case "payload":
				message.Wire = []byte(strings.Replace(string(message.Wire), "v=0", "v=1", 1))
			case "branch":
				message.Wire = []byte(strings.Replace(string(message.Wire), "z9hG4bK-cached-final", "z9hG4bK-fresh", 1))
			case "sequence":
				message.Wire = []byte(strings.Replace(string(message.Wire), "CSeq: 13 ", "CSeq: 14 ", 1))
			case "to-tag":
				message.Wire = []byte(strings.Replace(string(message.Wire), "tag=server", "tag=foreign", 1))
			case "from-tag":
				message.Wire = []byte(strings.Replace(string(message.Wire), "tag=local", "tag=foreign", 1))
			case "no-final":
				tx.Native.LastResponse = nil
			case "provisional":
				final := forkReply(t, tx, 180, c.Tag)
				tx.Native.LastResponse = &final
			case "wrong-final":
				tx.Native.LastResponse.Wire = []byte(strings.Replace(string(tx.Native.LastResponse.Wire), "CSeq: 13 ", "CSeq: 14 ", 1))
			case "unadmitted-flow":
				flow.ConnectionId, admission = "unadmitted-final", pb.OwnerIngest_EXISTING_FLOW
			case "subscription":
				c.Subscription = &stateSubscription{Event: "dialog"}
			case "transaction-context":
				c.Snapshot.Kind = pb.ContextSnapshot_TRANSACTION
			case "out-of-dialog":
				message.Wire = []byte(strings.Replace(string(message.Wire), ";tag=server", "", 1))
				tx.Native.Origin = message.Clone()
			}
			before, _ := marshalRetainedFixture(c)
			effects := len(r.payload.Effects)
			if err := completionRelayIngest(r, c, message, flow, admission); err == nil {
				t.Fatal("request without exact eligible cached final was relayed")
			}
			after, _ := marshalRetainedFixture(c)
			if !bytes.Equal(before, after) || len(r.payload.Effects) != effects {
				t.Fatal("rejected cached-final request mutated native state")
			}
		})
	}
}
