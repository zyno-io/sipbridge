package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func ownerRegistrationRequest(t *testing.T, sequence int, branch string) *pb.SipMessage {
	t.Helper()
	request := stateTestRequest("REGISTER", sequence, "", branch)
	wire := strings.ReplaceAll(string(request.Wire), "SIP/2.0/TLS", "SIP/2.0/UDP")
	wire = strings.ReplaceAll(wire, "transport=tls", "transport=udp")
	message, err := Observe([]byte(wire), false)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func ownerFinalRegistration(t *testing.T) (*StateEngine, *StateEngine, *pb.ContextRef, *pb.ProcessRef, *pb.FlowRef, *pb.SipMessage) {
	t.Helper()
	a, b := historicalStateTestEngine(t)
	message := ownerRegistrationRequest(t, 1, "idle-registration")
	ref, err := a.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "registration-owner"}
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "registration-source", SourceIp: "192.0.2.19", SourcePort: 50900, Listener: "sip.example.test:5060", Transport: "udp"}
	stateTestApply(t, a, ref, "registration-ingest", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: message.Wire}})
	_, group := stateTestLoad(t, a, ref)
	c := group.Contexts[ref.Id]
	proof := &pb.ApplicationStatus{Context: ref, Application: application, DirectoryEpoch: "directory", Live: true, CheckedAtMs: uint64(time.Now().UnixMilli())}
	if result := stateTestApply(t, b, ref, "registration-owner-proof", stateEngineInput{Kind: "owner-result", Effect: c.OwnerProbeEffect, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: proof}}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, a, ref)
	c = group.Contexts[ref.Id]
	var tx *stateTransaction
	for _, candidate := range c.Transactions {
		if candidate.RequestID != "" {
			tx = candidate
			break
		}
	}
	if tx == nil || tx.OwnerDeferred || !tx.OwnerValidated {
		t.Fatal("registration owner proof did not complete")
	}
	if result := stateTestApply(t, a, ref, "registration-accepted", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200}}}}); result.Error != "" {
		t.Fatal(result.Error)
	}
	return a, b, ref, application, flow, message
}

func TestStateOwnerFinalRegistrationStopsProbesWithoutChangingNativeEvidence(t *testing.T) {
	a, _, ref, application, _, _ := ownerFinalRegistration(t)
	record, group := stateTestLoad(t, a, ref)
	c := group.Contexts[ref.Id]
	if c.Snapshot.Ended || !stateFinalResponseServerContext(c) || c.OwnerProbeEffect != "" {
		t.Fatal("completed UDP registration retained periodic owner work")
	}
	r := &stateReduction{engine: a, record: record, group: group, payload: record.Payload, now: time.Now().UnixMilli(), result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	for _, timer := range record.Payload.Timers {
		if timer.ID == r.timerID(ref.Id, "owner", "probe", "") {
			t.Fatal("final response-only context retained an owner timer")
		}
	}
	nativeBefore, _ := marshalRetainedFixture(c.Transactions)
	physicalBefore, _ := marshalRetainedFixture(record.Payload.Effects)
	if err := r.ensureOwnerProbe(c, application); err != nil {
		t.Fatal(err)
	}
	// Existing workers may have armed a pure probe before the final response.
	// Removing that obsolete query must not change any physical/native record.
	r.payload.Timers = nil
	if err := r.appendOwnerTimers(); err != nil || c.OwnerProbeEffect != "" || len(r.payload.Timers) != 0 {
		t.Fatalf("idle transaction continued probing: %v", err)
	}
	nativeAfter, _ := marshalRetainedFixture(c.Transactions)
	physicalAfter, _ := marshalRetainedFixture(r.payload.Effects)
	if string(nativeBefore) != string(nativeAfter) || string(physicalBefore) != string(physicalAfter) {
		t.Fatal("owner-probe suppression changed native timers/response or separately retained physical effects")
	}
}

func TestStateOwnerIdleRegistrationRequiresFreshProofAndReplaysCachedResponse(t *testing.T) {
	for _, ownerLive := range []bool{false, true} {
		name := "dead-owner"
		if ownerLive {
			name = "live-owner"
		}
		t.Run(name, func(t *testing.T) {
			a, b, ref, application, flow, original := ownerFinalRegistration(t)
			request := ownerRegistrationRequest(t, 2, "fresh-after-idle")
			result := stateTestApply(t, b, ref, "new-registration-request", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: request.Wire}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, group := stateTestLoad(t, a, ref)
			c := group.Contexts[ref.Id]
			var fresh *stateTransaction
			for _, tx := range c.Transactions {
				if string(tx.Native.Origin.Wire) == string(request.Wire) {
					fresh = tx
				}
			}
			if !c.OwnerValidated || fresh == nil || fresh.OwnerValidated || !fresh.OwnerDeferred || c.OwnerProbeEffect == "" {
				t.Fatal("context's old proof admitted fresh policy work after idling")
			}
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "present" {
					t.Fatal("fresh request reached policy before its own exact owner proof")
				}
			}
			proof := &pb.ApplicationStatus{Context: ref, Application: application, DirectoryEpoch: "directory", Live: true, CheckedAtMs: uint64(time.Now().Add(-time.Duration(stateOwnerProofAge+1) * time.Millisecond).UnixMilli())}
			stale := stateTestApply(t, b, ref, "stale-registration-proof", stateEngineInput{Kind: "owner-result", Effect: c.OwnerProbeEffect, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: proof}}})
			if stale.Certainty != pb.OperationResult_NOT_ACCEPTED {
				t.Fatal("stale prior-owner observation admitted fresh registration business work")
			}
			proof.Live, proof.CheckedAtMs = ownerLive, uint64(time.Now().UnixMilli())
			result = stateTestApply(t, a, ref, "fresh-registration-proof", stateEngineInput{Kind: "owner-result", Effect: c.OwnerProbeEffect, Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: proof}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, group = stateTestLoad(t, b, ref)
			c = group.Contexts[ref.Id]
			presented := 0
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "present" {
					presented++
				}
			}
			if ownerLive {
				if c.OwnerLost || presented != 0 || !c.Transactions[fresh.Native.Key].OwnerValidated {
					t.Fatal("fresh live proof did not admit exactly one policy presentation")
				}
				check := &stateReduction{engine: a, group: group, payload: record.Payload, now: time.Now().UnixMilli()}
				if deferred, err := check.deferForOwnerProof(c, c.Transactions[fresh.Native.Key]); err != nil || deferred {
					t.Fatal("the same admitted transaction could not retain its exact proof")
				}
				return
			}
			if !c.OwnerLost || presented != 0 {
				t.Fatal("dead prior owner resumed fresh registration policy")
			}
			response, err := c.Transactions[fresh.Native.Key].Native.LastResponse.Parse()
			if err != nil || response.(*sip.Response).StatusCode != 503 {
				t.Fatal("dead owner's newly deferred request did not receive native503")
			}
			result = stateTestApply(t, b, ref, "cached-registration-response", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: original.Wire}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, group = stateTestLoad(t, a, ref)
			if group.Contexts[ref.Id].OwnerProbeEffect != "" {
				t.Fatal("cached response replay created owner policy work")
			}
			cached := false
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "present" || effect.Kind == "owner" {
					t.Fatal("cached response replay invoked business policy")
				}
				var body stateEngineEffect
				if effect.Kind == "wire" && unmarshalRetainedFixture(effect.Payload, &body) == nil {
					message, err := (portable.Message{Wire: body.Wire, Transport: "udp"}).Parse()
					if response, ok := message.(*sip.Response); err == nil && ok && response.StatusCode == 200 {
						cached = body.Authorization.GetAuthorityClass() == pb.EffectAuthorization_RESPONSE
					}
				}
			}
			if !cached {
				t.Fatal("cached final response lost independent native response authority")
			}
		})
	}
}

func TestStateOwnerIdleProbeSuppressionExcludesCallsSubscriptionsClientsAndBusiness(t *testing.T) {
	a, _, ref, _, _, _ := ownerFinalRegistration(t)
	_, group := stateTestLoad(t, a, ref)
	c := group.Contexts[ref.Id]
	if stateOwnerProbeNeeded(c, nil) || !stateOwnerProbeNeeded(c, map[string]bool{ref.Id: true}) {
		t.Fatal("pending business work did not preserve owner probing")
	}
	for _, kind := range []pb.ContextSnapshot_Kind{pb.ContextSnapshot_UAC, pb.ContextSnapshot_UAS, pb.ContextSnapshot_SUBSCRIPTION} {
		c.Snapshot.Kind = kind
		if !stateOwnerProbeNeeded(c, nil) {
			t.Fatal("active call/subscription owner probing was suppressed")
		}
	}
	c.Snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	for _, tx := range c.Transactions {
		tx.Native.Family = portable.NonInviteClient
	}
	if !stateOwnerProbeNeeded(c, nil) {
		t.Fatal("client-transaction owner probing was suppressed")
	}
}
