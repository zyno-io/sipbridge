package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func policyDecision(flow *pb.FlowRef) *pb.CommandAdmission {
	return &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli()), SelectedFlow: flow}
}

func TestCommandPolicyRegisteredOpenUsesOnlyNativePhysicalLedger(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.outboundRef(t)
	decision := policyDecision(f.flow)
	logical, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	result, err := f.engine.apply(context.Background(), logical, "admitted-open", stateEngineInput{Kind: "command", Context: ref,
		Operation: "admitted-open", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Admission: decision, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{
			Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", PrincipalId: "phone"}}}}, deadline, false)
	if err != nil || result.Error != "" {
		t.Fatalf("admitted open failed: %v %v", result, err)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	if c.OwnerDirectoryEpoch != "directory" || !c.OwnerValidated || c.ScopeOrigin == nil {
		t.Fatal("policy owner/initial completion anchor missing")
	}
	var write *pb.FlowWrite
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "wire" {
			t.Fatal("new application send requested a second trusted permit")
		}
		if effect.Kind != "deliver" {
			continue
		}
		var body stateEngineEffect
		if err = unmarshalRetainedFixture(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		tx := c.Transactions[body.Transaction]
		if !body.Native || effect.Status != state.EffectAuthorized || effect.ApplicationLease == nil ||
			tx.Policy == nil || tx.Policy.Command != "admitted-open" || tx.Policy.Term != f.term ||
			tx.Policy.Decision.ExpiresAtMs != uint64(deadline.UnixMilli()) || body.Permit.DirectoryEpoch != "directory" ||
			!proto.Equal(body.Permit.Flow, f.flow) || int64(body.Authorization.DeadlineMs) > deadline.UnixMilli() {
			t.Fatal("native send lost original policy, flow, deadline or atomic owner fence")
		}
		write = &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
	}
	if write == nil {
		t.Fatal("no native application intent")
	}
	reserved, err := f.engine.Reserve(context.Background(), write)
	if err != nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatalf("native application reservation failed: %+v %v", reserved, err)
	}
}

func TestCommandPolicyRefusalDoesNotConsumeResponseSequence(t *testing.T) {
	for _, kind := range []string{"epoch", "delivery", "capacity", "route", "expired"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.admit(t)
			_, group := stateTestLoad(t, f.engine, ref)
			var request string
			for id := range group.Contexts[ref.Id].Requests {
				request = id
			}
			decision := policyDecision(nil)
			switch kind {
			case "epoch":
				preflightMutate(t, f, ref, func(c *stateContext) { c.OwnerDirectoryEpoch = "other-directory" })
			case "delivery":
				decision.ExpectedDelivery = &pb.ContextDelivery{Flow: f.flow, NativeRevision: 1, DirectoryEpoch: "directory", RegistrationTicket: "directory:1"}
			case "capacity":
				flow := proto.Clone(f.flow).(*pb.FlowRef)
				flow.Listener = strings.Repeat("\x01", 1024)
				decision.ExpectedDelivery = &pb.ContextDelivery{Flow: flow, NativeRevision: 1, DirectoryEpoch: "directory", RegistrationTicket: "directory:1"}
			case "route":
				decision.SelectedFlow = f.flow
			case "expired":
				decision.ExpiresAtMs = uint64(time.Now().Add(-time.Second).UnixMilli())
			}
			result := f.apply(t, ref, "refused-policy", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Admission: decision, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 180}}}})
			_, after := stateTestLoad(t, f.engine, ref)
			if result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.Error == "" || after.Contexts[ref.Id].Snapshot.CommandSequence != 0 {
				t.Fatalf("invalid %s decision consumed a response sequence: %s", kind, result)
			}
			if kind == "capacity" && !strings.Contains(result.Error, "capacity") {
				t.Fatalf("scope used protobuf size instead of actual escaped JSON: %s", result)
			}
		})
	}
}

func TestCommandPolicyResponsesKeepEarlierPhysicalExpiry(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	first := policyDecision(nil)
	respond := func(op string, seq uint64, status uint32, decision *pb.CommandAdmission) {
		t.Helper()
		r := f.apply(t, ref, op, stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
			Command: &pb.Command{Context: ref, Sequence: seq, Admission: decision, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: status}}}})
		if r.Error != "" {
			t.Fatal(r.Error)
		}
	}
	respond("first-response", 1, 180, first)
	before, _ := stateTestLoad(t, f.engine, ref)
	var original state.Effect
	var originalBody stateEngineEffect
	for _, e := range before.Payload.Effects {
		var b stateEngineEffect
		if e.Kind == "deliver" && unmarshalRetainedFixture(e.Payload, &b) == nil && b.Command == "first-response" {
			original = e
			originalBody = b
		}
	}
	if original.ID == "" {
		t.Fatal("first response physical evidence missing")
	}
	write := &pb.FlowWrite{Flow: originalBody.Permit.Flow, Effect: originalBody.Authorization.Effect, Permit: originalBody.Permit}
	reserved, err := f.engine.Reserve(context.Background(), write)
	if err != nil || reserved.Known || len(reserved.Wire) == 0 {
		t.Fatalf("first physical response did not start: %+v %v", reserved, err)
	}
	respond("next-response", 2, 183, policyDecision(nil))
	after, g := stateTestLoad(t, f.engine, ref)
	var current *stateTransaction
	for _, tx := range g.Contexts[ref.Id].Transactions {
		if tx.RequestID == request {
			current = tx
		}
	}
	if current == nil || current.Policy.Command != "next-response" {
		t.Fatal("new current response selection missing")
	}
	found := false
	for _, e := range after.Payload.Effects {
		if e.ID == original.ID {
			found = true
			var body stateEngineEffect
			if err = unmarshalRetainedFixture(e.Payload, &body); err != nil {
				t.Fatal(err)
			}
			if e.DeadlineMS != original.DeadlineMS || e.Status != state.EffectDispatching ||
				!proto.Equal(body.Authorization, originalBody.Authorization) || !proto.Equal(body.Permit, originalBody.Permit) {
				t.Fatal("later response replaced the original reserved physical authority")
			}
		}
	}
	if !found {
		t.Fatal("later response removed an in-flight physical attempt")
	}
	if err = f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}); err != nil {
		t.Fatal(err)
	}
	_, g = stateTestLoad(t, f.engine, ref)
	if c := g.Contexts[ref.Id]; c.Operation != "next-response" || c.Result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("late old write completed the replacement response selection")
	}
}

func TestCommandPolicyPreservesLegacyInitialCompletionAnchor(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	authority := f.responseAuthority(t, ref, request)
	legacy := f.apply(t, ref, "legacy-provisional", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 180, Authority: authority}}}})
	if legacy.Error != "" {
		t.Fatal(legacy.Error)
	}
	_, before := stateTestLoad(t, f.engine, ref)
	prior := before.Contexts[ref.Id]
	if prior.CompletionScope == "" {
		t.Fatal("legacy completion anchor missing")
	}
	admitted := f.apply(t, ref, "admitted-final", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 2, Admission: policyDecision(nil), Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200}}}})
	if admitted.Error != "" {
		t.Fatal(admitted.Error)
	}
	_, after := stateTestLoad(t, f.engine, ref)
	current := after.Contexts[ref.Id]
	if current.CompletionScope != prior.CompletionScope || current.ScopeOrigin == nil || prior.ScopeOrigin == nil ||
		!bytes.Equal(current.ScopeOrigin.WireBytes(), prior.ScopeOrigin.WireBytes()) ||
		current.Transactions[current.Requests[request]].CompletionScope != prior.CompletionScope {
		t.Fatal("new command changed an established native completion anchor")
	}
}

func TestCommandPolicyMaximumEncodingKeepsSixteenForkCompletionCapacity(t *testing.T) {
	flow := sendFingerprintFlow()
	flow.Listener = "a"
	policy := &statePolicyAdmission{Command: "open", Term: "term", Decision: &pb.CommandAdmission{
		DirectoryEpoch: "directory", ExpiresAtMs: 1000000, SelectedFlow: flow}}
	encoded, err := marshalRetainedFixture(policy)
	if err != nil {
		t.Fatal(err)
	}
	remaining := statePolicyAdmissionBytes - len(encoded) + 1
	flow.Listener = strings.Repeat("\x01", remaining/6) + strings.Repeat("a", remaining%6)
	encoded, err = marshalRetainedFixture(policy)
	if err != nil || len(encoded) != statePolicyAdmissionBytes || !policy.valid() {
		t.Fatalf("maximum encoded policy fixture invalid: %d %v", len(encoded), err)
	}
	var decoded statePolicyAdmission
	if err = unmarshalRetainedFixture(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	testStateDerivedCapacityAdmitsMandatorySixteenForkCompletion(t, &decoded)
}

func TestCommandPolicyFirstReservationChecksOwnerAndDeliveryAgain(t *testing.T) {
	for _, change := range []string{"owner", "delivery", "epoch"} {
		t.Run(change, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.outboundRef(t)
			result := f.apply(t, ref, "admitted-open", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Admission: policyDecision(f.flow), Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{
					Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", PrincipalId: "phone"}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			var write *pb.FlowWrite
			for _, e := range record.Payload.Effects {
				var b stateEngineEffect
				if e.Kind == "deliver" && unmarshalRetainedFixture(e.Payload, &b) == nil && b.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION {
					write = &pb.FlowWrite{Flow: b.Permit.Flow, Effect: b.Authorization.Effect, Permit: b.Permit}
				}
			}
			if write == nil {
				t.Fatal("application physical intent missing")
			}
			if change == "owner" {
				f.expireUnobserved(t)
			} else {
				preflightMutate(t, f, ref, func(c *stateContext) {
					if change == "delivery" {
						c.Delivery = &pb.ContextDelivery{Flow: f.flow, NativeRevision: 1, DirectoryEpoch: "directory", RegistrationTicket: "directory:1"}
					} else {
						c.OwnerDirectoryEpoch = "replacement"
					}
				})
			}
			reserved, err := f.engine.Reserve(context.Background(), write)
			if err == nil && (reserved == nil || !reserved.Known) || reserved != nil && len(reserved.Wire) != 0 {
				t.Fatalf("%s change authorized first physical I/O: %+v %v", change, reserved, err)
			}
		})
	}
}

func TestCommandPolicyNetworkDigestKeepsOriginalSelectionAcrossWorkers(t *testing.T) {
	for _, variation := range []string{"active", "expired", "cancelled"} {
		t.Run(variation, func(t *testing.T) {
			a := newNativeOwnerFixture(t)
			b := a.secondWorker(t)
			ref := a.outboundRef(t)
			result := a.apply(t, ref, "policy-network-open", stateEngineInput{Kind: "command", Application: a.app, ApplicationLeaseTerm: a.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Admission: policyDecision(nil), Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{
					Uri: "sip:called@carrier.example.test", Network: &pb.NetworkTarget{Transport: "tls", Credential: &pb.CredentialRef{Id: "credential-ref", Version: "version-1"}}}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ := stateTestLoad(t, b.engine, ref)
			var open state.Effect
			for _, e := range record.Payload.Effects {
				if e.Kind == "open" {
					open = e
				}
			}
			if open.ID == "" {
				t.Fatal("bounded network-open intent missing")
			}
			b.apply(t, ref, "start-open", stateEngineInput{Kind: "effect-start", Effect: open.ID})
			flow := proto.Clone(a.flow).(*pb.FlowRef)
			flow.Ingress = b.engine.cfg.Process
			result = b.apply(t, ref, "network-opened", stateEngineInput{Kind: "effect-result", Effect: open.ID, Executor: b.engine.cfg.Process, Flow: flow})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group := stateTestLoad(t, a.engine, ref)
			c := group.Contexts[ref.Id]
			var original *stateTransaction
			for _, tx := range c.Transactions {
				original = tx
			}
			if original == nil || original.Policy == nil {
				t.Fatal("network result lost original policy selection")
			}
			decision := proto.Clone(original.Policy.Decision).(*pb.CommandAdmission)
			challenge := digestReply(t, original.Native.Origin, 407)
			result = a.apply(t, ref, "challenge", stateEngineInput{Kind: "ingest", Application: a.app, ApplicationLeaseTerm: a.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: a.app, Flow: flow, Wire: challenge.WireBytes(), Admission: pb.OwnerIngest_EXISTING_FLOW}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ = stateTestLoad(t, b.engine, ref)
			var credential state.Effect
			var retained stateDigestChallenge
			for _, e := range record.Payload.Effects {
				if e.Kind == "credential" {
					credential = e
					var body stateEngineEffect
					if unmarshalRetainedFixture(e.Payload, &body) != nil || unmarshalRetainedFixture(body.Wire, &retained) != nil {
						t.Fatal("derived challenge corrupt")
					}
				}
			}
			if credential.ID == "" {
				t.Fatal("credential lookup missing")
			}
			if variation == "expired" {
				preflightMutate(t, a, ref, func(c *stateContext) {
					for _, tx := range c.Transactions {
						if tx.Policy != nil {
							tx.Policy.Decision.ExpiresAtMs = uint64(time.Now().Add(-time.Second).UnixMilli())
						}
					}
				})
			} else if variation == "cancelled" {
				result = a.apply(t, ref, "cancel-setup", stateEngineInput{Kind: "command", Application: a.app, ApplicationLeaseTerm: a.term,
					Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			derived := digestDerived(t, retained)
			result = b.apply(t, ref, "derived-credential", stateEngineInput{Kind: "digest-result", Effect: credential.ID, Message: derived})
			_, group = stateTestLoad(t, a.engine, ref)
			var retry *stateTransaction
			invites := 0
			for _, tx := range group.Contexts[ref.Id].Transactions {
				if tx.Native.Family == original.Native.Family {
					invites++
					if tx.Native.Key != original.Native.Key {
						retry = tx
					}
				}
			}
			if variation != "active" {
				if invites != 1 || retry != nil {
					t.Fatal("expired or cancelled setup admitted an authenticated retry")
				}
				return
			}
			if result.Error != "" || retry == nil || retry.Policy == nil || !proto.Equal(retry.Policy.Decision, decision) ||
				retry.Policy.Command != "policy-network-open" || retry.Policy.Term != a.term {
				t.Fatalf("digest retry lost or renewed original policy: %s", result)
			}
		})
	}
}
