package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// These regressions exercise the Redis-native service after retiring its
// independently implemented resident Engine. They use actual 1/1 CAS admission;
// explicit reducer times below cover timer boundaries without wall-clock sleeps.
func migrationEstablished(t *testing.T, f nativeOwnerFixture) *pb.ContextRef {
	t.Helper()
	ref := f.admit(t)
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	result := f.apply(t, ref, "migration-answer", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200, Authority: f.responseAuthority(t, ref, request)}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group = stateTestLoad(t, f.engine, ref)
	ack := stateTestRequest("ACK", 1, group.Contexts[ref.Id].Snapshot.LocalTag, "migration-ack")
	result = f.apply(t, ref, "migration-ack", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: ack.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	return ref
}

func migrationSubscribe(t *testing.T, f nativeOwnerFixture, expires int) *pb.ContextRef {
	t.Helper()
	initial := stateTestRequest("SUBSCRIBE", 1, "", "migration-subscribe")
	wire := bytes.Replace(initial.Wire, []byte("Content-Length:"), []byte("Event: message-summary\r\nExpires: 90\r\nContent-Length:"), 1)
	initial, err := Observe(wire, false)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := f.engine.Locate(context.Background(), initial)
	if err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, "migration-subscribe", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: initial.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	var request string
	for id := range group.Contexts[ref.Id].Requests {
		request = id
	}
	result = f.apply(t, ref, "migration-subscribe-accept", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: strconv.Itoa(expires)}}}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	return ref
}

func TestStateMigratedTransportClassAndDirectionalAdmission(t *testing.T) {
	for _, field := range []string{"transport", "call-id", "local-tag", "remote-tag", "swapped-tags", "principal", "binding"} {
		t.Run(field, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := migrationEstablished(t, f)
			before, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			request := stateTestRequest("INVITE", 2, c.Snapshot.LocalTag, "migration-invalid")
			flow := proto.Clone(f.flow).(*pb.FlowRef)
			flow.ConnectionId = "replacement"
			parsed, err := sip.ParseMessage(request.Wire)
			if err != nil {
				t.Fatal(err)
			}
			switch field {
			case "transport":
				flow.Transport = "tcp"
			case "call-id":
				id := sip.CallIDHeader("other-call")
				parsed.(*sip.Request).ReplaceHeader(&id)
			case "local-tag":
				parsed.To().Params.Add("tag", "other-local")
			case "remote-tag":
				parsed.From().Params.Add("tag", "other-remote")
			case "swapped-tags":
				parsed.To().Params.Add("tag", c.Snapshot.RemoteTag)
				parsed.From().Params.Add("tag", c.Snapshot.LocalTag)
			}
			principal, binding := "phone", "binding"
			mode := pb.OwnerIngest_TLS_DIALOG_CONTINUITY
			if field == "principal" {
				principal = "other-phone"
				mode = pb.OwnerIngest_AUTHENTICATED_BINDING
			}
			if field == "binding" {
				binding = "other-binding"
				mode = pb.OwnerIngest_AUTHENTICATED_BINDING
			}
			result := f.apply(t, ref, "migration-invalid-"+field, stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: flow, Wire: []byte(parsed.String()), PrincipalId: principal, BindingId: binding, Admission: mode}})
			after, g := stateTestLoad(t, f.engine, ref)
			if result.Error == "" {
				t.Fatal("altered directional identity gained admission")
			}
			if !proto.Equal(g.Contexts[ref.Id].Flow, c.Flow) || g.Contexts[ref.Id].Delivery != nil {
				t.Fatal("refusal changed delivery authority")
			}
			for _, effect := range after.Payload.Effects[len(before.Payload.Effects):] {
				if effect.Kind == "present" || effect.Kind == "adopt" {
					t.Fatal("refused identity published application work")
				}
			}
		})
	}
}

func TestStateMigratedReplacementPublicationOrdersApplicationWork(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, accept := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/accepted-%t", transport, accept), func(t *testing.T) {
				f := newNativeOwnerFixture(t)
				f.flow.Transport = transport
				ref := migrationEstablished(t, f)
				before, group := stateTestLoad(t, f.engine, ref)
				c := group.Contexts[ref.Id]
				flow := proto.Clone(f.flow).(*pb.FlowRef)
				flow.ConnectionId = "replacement"
				mode := pb.OwnerIngest_AUTHENTICATED_BINDING
				if transport == "tls" {
					mode = pb.OwnerIngest_TLS_DIALOG_CONTINUITY
				}
				request := stateTestRequest("INVITE", 2, c.Snapshot.LocalTag, "migration-reconnect")
				result := f.apply(t, ref, "migration-reconnect", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
					Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: flow, Wire: request.Wire, PrincipalId: "phone", BindingId: "binding", Admission: mode}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				record, g := stateTestLoad(t, f.engine, ref)
				var adoption state.Effect
				for _, effect := range record.Payload.Effects[len(before.Payload.Effects):] {
					if effect.Kind == "present" {
						t.Fatal("request presented before receipt")
					}
					if effect.Kind == "adopt" {
						adoption = effect
					}
				}
				if adoption.ID == "" || !proto.Equal(g.Contexts[ref.Id].Flow, c.Flow) {
					t.Fatal("publication barrier missing")
				}
				var body stateEngineEffect
				if err := body.UnmarshalJSON(adoption.Payload); err != nil {
					t.Fatal(err)
				}
				receipt := &pb.ContextDelivery{Flow: flow, NativeRevision: body.Envelope.GetFlowAdoption().NativeRevision, DirectoryEpoch: "directory"}
				reply := &pb.FlowAdoptionResult{Error: "fixture refusal"}
				if accept {
					reply = &pb.FlowAdoptionResult{Receipt: receipt}
				}
				f.apply(t, ref, "migration-adopt-start", stateEngineInput{Kind: "effect-start", Effect: adoption.ID})
				f.apply(t, ref, "migration-adopt-result", stateEngineInput{Kind: "effect-result", Effect: adoption.ID, Executor: f.engine.cfg.Process,
					Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: reply}}})
				after, g := stateTestLoad(t, f.engine, ref)
				presented := 0
				for _, effect := range after.Payload.Effects[len(before.Payload.Effects):] {
					if effect.Kind == "present" {
						presented++
					}
				}
				if accept {
					if !proto.Equal(g.Contexts[ref.Id].Flow, flow) || !proto.Equal(g.Contexts[ref.Id].Delivery, receipt) || presented != 1 {
						t.Fatal("receipt did not atomically publish route and release application work")
					}
					return
				}
				if !proto.Equal(g.Contexts[ref.Id].Flow, c.Flow) || g.Contexts[ref.Id].Delivery != nil || presented != 0 {
					t.Fatal("denied publication changed accepted authority or released application work")
				}
				f.apply(t, ref, "migration-adopt-late-success", stateEngineInput{Kind: "effect-result", Effect: adoption.ID, Executor: f.engine.cfg.Process,
					Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: receipt}}}})
				_, g = stateTestLoad(t, f.engine, ref)
				if !proto.Equal(g.Contexts[ref.Id].Flow, c.Flow) || g.Contexts[ref.Id].Delivery != nil {
					t.Fatal("late success revived denied publication")
				}
			})
		}
	}
}

func TestStateMigratedSameFlowReplayToleratesResolvedPrincipal(t *testing.T) {
	for _, method := range []string{"INVITE", "REGISTER", "SUBSCRIBE"} {
		t.Run(method, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			message := stateTestRequest(method, 1, "", "migration-scope")
			if method == "SUBSCRIBE" {
				message.Wire = bytes.Replace(message.Wire, []byte("Content-Length:"), []byte("Event: message-summary\r\nExpires: 90\r\nContent-Length:"), 1)
			}
			ref, err := f.engine.Locate(context.Background(), message)
			if err != nil {
				t.Fatal(err)
			}
			ingest := &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}
			result := f.apply(t, ref, "migration-unresolved", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: ingest})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			before, group := stateTestLoad(t, f.engine, ref)
			ingest.PrincipalId, ingest.BindingId = "phone", "binding"
			result = f.apply(t, ref, "migration-resolved-replay", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term, Ingest: ingest})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			after, g := stateTestLoad(t, f.engine, ref)
			if len(g.Contexts[ref.Id].Transactions) != len(group.Contexts[ref.Id].Transactions) || g.Contexts[ref.Id].Snapshot.PrincipalId != "" || g.Contexts[ref.Id].Snapshot.BindingId != "" || g.Contexts[ref.Id].Delivery != nil {
				t.Fatal("response replay granted account authority")
			}
			for _, effect := range after.Payload.Effects[len(before.Payload.Effects):] {
				if effect.Kind == "present" || effect.Kind == "adopt" {
					t.Fatal("response replay repeated application work")
				}
			}
		})
	}
}

func TestStateMigratedSubscriptionGraceAndTerminalNotify(t *testing.T) {
	for _, expires := range []int{0, 10} {
		t.Run(fmt.Sprint(expires), func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := migrationSubscribe(t, f, expires)
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, now: c.Subscription.ExpiresMS}
			timer := r.timerID(ref.Id, "subscription", "expiry", "")
			r.in = stateEngineInput{Timer: timer, Generation: strconv.FormatInt(c.Subscription.ExpiresMS, 10)}
			if expires > 0 {
				if handled, err := r.lifecycleTimer(); !handled || err != nil {
					t.Fatalf("expiry: %v %v", handled, err)
				}
			}
			terminal := c.Subscription.TerminalMS
			if c.Snapshot.Ended || !c.Subscription.Terminating || terminal < r.now+4990 {
				t.Fatal("subscription lost five-second delivery grace")
			}
			before := len(r.payload.Effects)
			if err := r.notifySubscription(c, &pb.DialogRequest{Method: "NOTIFY"}); err != errSubscriptionTerminating {
				t.Fatalf("active NOTIFY admitted during termination: %v", err)
			}
			if len(r.payload.Effects) != before {
				t.Fatal("refused active NOTIFY emitted SIP")
			}
			r.now = terminal - 1
			r.in.Generation = strconv.FormatInt(terminal, 10)
			if _, err := r.lifecycleTimer(); err != nil || c.Snapshot.Ended {
				t.Fatal("subscription retired before terminal deadline")
			}
			r.now = terminal
			if _, err := r.lifecycleTimer(); err != nil || !c.Snapshot.Ended {
				t.Fatal("subscription retained beyond terminal deadline")
			}
			if stateCountEvents(t, r, pb.DialogEvent_ENDED) != 1 {
				t.Fatal("terminal timer did not emit exactly one end")
			}
			if _, err := r.lifecycleTimer(); err != nil || stateCountEvents(t, r, pb.DialogEvent_ENDED) != 1 {
				t.Fatal("terminal timer replay emitted another end")
			}
		})
	}
}

func TestStateMigratedSIPSNetworkCannotDowngrade(t *testing.T) {
	for _, transport := range []string{"udp", "tcp"} {
		t.Run(transport, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.outboundRef(t)
			result := f.apply(t, ref, "migration-sips-"+transport, stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sips:peer@example.test", Network: &pb.NetworkTarget{Transport: transport}}}}})
			if result.Error == "" {
				t.Fatal("SIPS opened an insecure native context")
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "open" || effect.Kind == "deliver" {
					t.Fatal("SIPS refusal emitted a network operation")
				}
			}
		})
	}
}

func TestStateMigratedPendingCancelRequiresExactIdentity(t *testing.T) {
	for _, field := range []string{"call-id", "from", "to", "cseq", "uri", "sent-by", "valid"} {
		t.Run(field, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.admit(t)
			before, group := stateTestLoad(t, f.engine, ref)
			initial := stateTestRequest("INVITE", 1, "", "native-owner")
			parsed, err := sip.ParseMessage(initial.Wire)
			if err != nil {
				t.Fatal(err)
			}
			request := parsed.(*sip.Request)
			request.Method, request.CSeq().MethodName = sip.CANCEL, sip.CANCEL
			switch field {
			case "call-id":
				id := sip.CallIDHeader("different-call")
				request.ReplaceHeader(&id)
			case "from":
				request.From().Params.Add("tag", "different-from")
			case "to":
				request.To().Params.Add("tag", "different-to")
			case "cseq":
				request.CSeq().SeqNo++
			case "uri":
				request.Recipient.User = "different-target"
			case "sent-by":
				request.Via().Host = "different.invalid"
			}
			flow := proto.Clone(f.flow).(*pb.FlowRef)
			flow.ConnectionId = "replacement"
			result := f.apply(t, ref, "migration-cancel-"+field, stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: flow, Wire: []byte(request.String()), Admission: pb.OwnerIngest_TLS_DIALOG_CONTINUITY}})
			after, g := stateTestLoad(t, f.engine, ref)
			if (result.Error == "") != (field == "valid") {
				t.Fatalf("CANCEL %s: %+v", field, result)
			}
			if !proto.Equal(g.Contexts[ref.Id].Flow, group.Contexts[ref.Id].Flow) || g.Contexts[ref.Id].Delivery != nil {
				t.Fatal("CANCEL published delivery authority")
			}
			for _, effect := range after.Payload.Effects[len(before.Payload.Effects):] {
				if effect.Kind == "present" || effect.Kind == "adopt" {
					t.Fatal("CANCEL created application work")
				}
			}
		})
	}
}

func TestStateMigratedSameFlowSubscriptionPublication(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, accept := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/accepted-%t", transport, accept), func(t *testing.T) {
				f := newNativeOwnerFixture(t)
				f.flow.Transport = transport
				ref := migrationSubscribe(t, f, 30)
				before, group := stateTestLoad(t, f.engine, ref)
				c := group.Contexts[ref.Id]
				if c.Delivery != nil || c.DeliveryRevision != 0 {
					t.Fatal("initial subscription required publication")
				}
				renewal := stateTestRequest("SUBSCRIBE", 2, c.Snapshot.LocalTag, "migration-renewal")
				wire := bytes.Replace(renewal.Wire, []byte("Content-Length:"), []byte("Event: message-summary\r\nExpires: 90\r\nContent-Length:"), 1)
				wire = bytes.Replace(wire, []byte("Contact: <sip:alice@phone.example.test"), []byte("Contact: <sip:alice@renewed.example.test"), 1)
				result := f.apply(t, ref, "migration-renewal", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
					Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: wire, PrincipalId: "phone", BindingId: "binding"}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				_, g := stateTestLoad(t, f.engine, ref)
				var handle string
				for id, key := range g.Contexts[ref.Id].Requests {
					if bytes.Equal(g.Contexts[ref.Id].Transactions[key].Native.Origin.Wire, wire) {
						handle = id
					}
				}
				if handle == "" {
					t.Fatal("renewal transaction missing")
				}
				result = f.apply(t, ref, "migration-renewal-accept", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
					Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: handle, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "90"}}}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				record, g := stateTestLoad(t, f.engine, ref)
				pending := g.Contexts[ref.Id]
				if pending.Subscription.Target != c.Subscription.Target || pending.Subscription.ExpiresMS != c.Subscription.ExpiresMS || pending.Delivery != nil {
					t.Fatal("renewal changed accepted state before receipt")
				}
				var effect state.Effect
				var body stateEngineEffect
				for _, candidate := range record.Payload.Effects[len(before.Payload.Effects):] {
					if candidate.Kind == "adopt" {
						effect = candidate
						if err := body.UnmarshalJSON(candidate.Payload); err != nil {
							t.Fatal(err)
						}
					}
				}
				proposal := body.Envelope.GetFlowAdoption()
				if effect.ID == "" || proposal == nil || !proposal.Subscription || !proto.Equal(proposal.Flow, f.flow) || proposal.BindingId != "binding" || proposal.PrincipalId != "phone" {
					t.Fatal("renewal skipped scoped same-flow publication")
				}
				f.apply(t, ref, "migration-renewal-start", stateEngineInput{Kind: "effect-start", Effect: effect.ID})
				reply := &pb.FlowAdoptionResult{Error: "fixture denied"}
				if accept {
					reply = &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: f.flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory", RegistrationTicket: "ticket"}}
				}
				f.apply(t, ref, "migration-renewal-published", stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: f.engine.cfg.Process,
					Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: reply}}})
				after, g := stateTestLoad(t, f.engine, ref)
				accepted := g.Contexts[ref.Id]
				if accept {
					if accepted.Subscription.Target == c.Subscription.Target || accepted.Subscription.ExpiresMS <= c.Subscription.ExpiresMS || !proto.Equal(accepted.Delivery, reply.Receipt) {
						t.Fatal("accepted renewal lost target, lease or receipt")
					}
				} else if accepted.Subscription.Target != c.Subscription.Target || accepted.Subscription.ExpiresMS != c.Subscription.ExpiresMS || accepted.Delivery != nil || accepted.Subscription.Pending != "" {
					t.Fatal("denied renewal changed accepted state or fenced the next refresh")
				}
				status := uint32(503)
				if accept {
					status = 202
				}
				found := false
				for _, candidate := range after.Payload.Effects {
					var b stateEngineEffect
					if err := b.UnmarshalJSON(candidate.Payload); err != nil {
						t.Fatal(err)
					}
					if b.WireMethod == "SUBSCRIBE" && b.WireStatus == status {
						found = true
					}
				}
				if !found || !proto.Equal(accepted.Flow, f.flow) || accepted.Snapshot.PrincipalId != "phone" || accepted.Snapshot.BindingId != "binding" {
					t.Fatal("renewal lost response or account/flow scope")
				}
			})
		}
	}
}

func TestStateMigratedUnsubscribeAdmitsOnlyTerminalNotify(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := migrationSubscribe(t, f, 0)
	active := f.apply(t, ref, "migration-active-notify", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY"}}}})
	if active.Certainty != pb.OperationResult_COMPLETED || active.SipStatus != 481 {
		t.Fatalf("active NOTIFY termination guard: %v", active)
	}
	final := f.apply(t, ref, "migration-terminal-notify", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 3, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Subscription-State", Value: "terminated;reason=timeout"}}}}}})
	if final.Error != "" {
		t.Fatal(final.Error)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	if !group.Contexts[ref.Id].Subscription.Final {
		t.Fatal("terminal NOTIFY not retained")
	}
	count := 0
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if err := body.UnmarshalJSON(effect.Payload); err != nil {
			t.Fatal(err)
		}
		if body.WireMethod == "NOTIFY" {
			count++
			parsed, err := sip.ParseMessage(body.Wire)
			if err != nil || header(parsed, "Subscription-State") != "terminated;reason=timeout" {
				t.Fatal("wrong terminal NOTIFY wire")
			}
		}
	}
	if count != 1 {
		t.Fatalf("terminal NOTIFY intents=%d, want 1", count)
	}
}

func TestStateMigratedRemoteByeHasOneFinalAndOneEndAcrossWriteOutcomes(t *testing.T) {
	for _, outcome := range []pb.DeliveryResult_Outcome{pb.DeliveryResult_WRITTEN, pb.DeliveryResult_UNKNOWN} {
		t.Run(outcome.String(), func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := migrationEstablished(t, f)
			before, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			bye := stateTestRequest("BYE", 2, c.Snapshot.LocalTag, "migration-bye")
			result := f.apply(t, ref, "migration-bye", stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: bye.Wire}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, g := stateTestLoad(t, f.engine, ref)
			if !g.Contexts[ref.Id].Snapshot.Ended {
				t.Fatal("remote BYE did not end native context")
			}
			var selected *pb.FlowWrite
			finals, ends := 0, 0
			for _, effect := range record.Payload.Effects[len(before.Payload.Effects):] {
				var body stateEngineEffect
				if err := body.UnmarshalJSON(effect.Payload); err != nil {
					t.Fatal(err)
				}
				if body.WireMethod == "BYE" && body.WireStatus >= 200 {
					finals++
					if body.WireStatus != 200 {
						t.Fatal("remote BYE emitted alternate final")
					}
					selected = &pb.FlowWrite{Flow: body.Permit.Flow, Effect: body.Authorization.Effect, Permit: body.Permit}
				}
				if body.Envelope.GetDialogEvent().GetKind() == pb.DialogEvent_ENDED {
					ends++
				}
			}
			if finals != 1 || ends != 1 || selected == nil {
				t.Fatalf("remote BYE finals=%d ends=%d", finals, ends)
			}
			if _, err := f.engine.Reserve(context.Background(), selected); err != nil {
				t.Fatal(err)
			}
			if err := f.engine.Complete(context.Background(), selected, &pb.DeliveryResult{Effect: selected.Effect, Outcome: outcome}); err != nil {
				t.Fatal(err)
			}
			after, g := stateTestLoad(t, f.engine, ref)
			if !g.Contexts[ref.Id].Snapshot.Ended {
				t.Fatal("write result revived ended native context")
			}
			for _, effect := range after.Payload.Effects[len(record.Payload.Effects):] {
				var body stateEngineEffect
				if err := body.UnmarshalJSON(effect.Payload); err != nil {
					t.Fatal(err)
				}
				if body.WireMethod == "BYE" && body.WireStatus >= 200 || body.Envelope.GetDialogEvent().GetKind() == pb.DialogEvent_ENDED {
					t.Fatal("write completion emitted another final or terminal event")
				}
			}
		})
	}
}

func TestStateMigratedSubscriptionProcessorChallengesBeforePublication(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			f.flow.Transport = transport
			ref := migrationSubscribe(t, f, 30)
			_, group := stateTestLoad(t, f.engine, ref)
			original := group.Contexts[ref.Id]
			next := &pb.ProcessRef{InstanceId: "successor", Epoch: "new-epoch"}
			lease, err := f.engine.ApplicationLease(context.Background(), next, "")
			if err != nil {
				t.Fatal(err)
			}
			result := f.apply(t, ref, "migration-controller", stateEngineInput{Kind: "owner-control", Application: next, ApplicationLeaseTerm: lease.Term,
				Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: ref, Application: next, DirectoryEpoch: "directory", Live: true, CheckedAtMs: uint64(time.Now().UnixMilli())}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			f.app, f.term = next, lease.Term
			_, group = stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			if !sameProcess(c.Snapshot.Application, next) || c.Subscription.ExpiresMS != original.Subscription.ExpiresMS || c.Subscription.Target != original.Subscription.Target || !proto.Equal(c.Flow, original.Flow) || c.Delivery != nil {
				t.Fatal("controller approval changed accepted SIP lease or route")
			}
			build := func(sequence int, event string) []byte {
				message := stateTestRequest("SUBSCRIBE", sequence, c.Snapshot.LocalTag, fmt.Sprintf("migration-processor-%d-%s", sequence, event))
				return bytes.Replace(message.Wire, []byte("Content-Length:"), []byte("Event: "+event+"\r\nExpires: 90\r\nContent-Length:"), 1)
			}
			ingest := func(id string, wire []byte) *pb.OperationResult {
				return f.apply(t, ref, id, stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
					Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: wire, PrincipalId: "phone", BindingId: "binding"}})
			}
			for _, invalid := range []struct {
				name     string
				sequence int
				event    string
			}{{"wrong-event", 2, "dialog"}, {"stale", 1, "message-summary"}} {
				refused := ingest("migration-processor-"+invalid.name, build(invalid.sequence, invalid.event))
				if refused.Error == "" {
					t.Fatal("new processor admitted invalid renewal", invalid.name)
				}
			}
			respond := func(operation string, commandSeq uint64, request []byte, status uint32) {
				t.Helper()
				_, g := stateTestLoad(t, f.engine, ref)
				handle := ""
				for id, key := range g.Contexts[ref.Id].Requests {
					if bytes.Equal(g.Contexts[ref.Id].Transactions[key].Native.Origin.Wire, request) {
						handle = id
					}
				}
				if handle == "" {
					t.Fatal("processor renewal missing")
				}
				result := f.apply(t, ref, operation, stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
					Command: &pb.Command{Context: ref, Sequence: commandSeq, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: handle, Status: status, Headers: []*pb.Header{{Name: "Expires", Value: "90"}, {Name: "WWW-Authenticate", Value: `Digest realm="fixture",nonce="fresh"`}}}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
			}
			challenge := build(2, "message-summary")
			if result := ingest("migration-processor-challenge", challenge); result.Error != "" {
				t.Fatal(result.Error)
			}
			respond("migration-processor-challenge-response", 2, challenge, 401)
			record, group := stateTestLoad(t, f.engine, ref)
			c = group.Contexts[ref.Id]
			if c.Subscription.Target != original.Subscription.Target || c.Subscription.ExpiresMS != original.Subscription.ExpiresMS || !proto.Equal(c.Flow, original.Flow) || c.Delivery != nil || c.Subscription.Pending != "" {
				t.Fatal("401 changed accepted subscription authority")
			}
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "adopt" {
					t.Fatal("401 published a replacement subscription flow")
				}
			}
			retry := build(3, "message-summary")
			if result := ingest("migration-processor-authenticated-retry", retry); result.Error != "" {
				t.Fatal(result.Error)
			}
			respond("migration-processor-accept", 3, retry, 202)
			record, group = stateTestLoad(t, f.engine, ref)
			c = group.Contexts[ref.Id]
			found := false
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "adopt" {
					var body stateEngineEffect
					if err := body.UnmarshalJSON(effect.Payload); err != nil {
						t.Fatal(err)
					}
					proposal := body.Envelope.GetFlowAdoption()
					if proposal == nil || !sameProcess(proposal.Application, next) || !proposal.Subscription {
						t.Fatal("renewal publication retained old processor")
					}
					found = true
				}
			}
			if !found || c.Subscription.ExpiresMS != original.Subscription.ExpiresMS || c.Delivery != nil {
				t.Fatal("authenticated renewal committed before successor publication receipt")
			}
		})
	}
}

func TestStateMigratedDigestRetryUsesLiveResponseIdentity(t *testing.T) {
	for _, status := range []int{401, 407} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.outboundRef(t)
			result := f.apply(t, ref, "migration-digest-open", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:called@carrier.example.test;user=phone?Subject=test", Body: []byte("v=0\r\n"), Network: &pb.NetworkTarget{Transport: "tls", Credential: &pb.CredentialRef{Id: "credential-ref", Version: "version-1"}}}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			var open state.Effect
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "open" {
					open = effect
				}
			}
			if open.ID == "" {
				t.Fatal("network-open intent missing")
			}
			f.apply(t, ref, "migration-digest-open-start", stateEngineInput{Kind: "effect-start", Effect: open.ID})
			result = f.apply(t, ref, "migration-digest-opened", stateEngineInput{Kind: "effect-result", Effect: open.ID, Executor: f.engine.cfg.Process, Flow: f.flow})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group := stateTestLoad(t, f.engine, ref)
			var original *stateTransaction
			for _, tx := range group.Contexts[ref.Id].Transactions {
				original = tx
			}
			if original == nil {
				t.Fatal("initial INVITE missing")
			}
			challenge := digestReply(t, original.Native.Origin, status)
			result = f.apply(t, ref, "migration-digest-challenge", stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: challenge.Wire}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ = stateTestLoad(t, f.engine, ref)
			var credential state.Effect
			var retained stateDigestChallenge
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "credential" {
					credential = effect
					var body stateEngineEffect
					if err := body.UnmarshalJSON(effect.Payload); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(body.Wire, &retained); err != nil {
						t.Fatal(err)
					}
				}
			}
			if credential.ID == "" {
				t.Fatal("credential effect missing")
			}
			derived := digestDerived(t, retained)
			result = f.apply(t, ref, "migration-digest-derived", stateEngineInput{Kind: "digest-result", Effect: credential.ID, Message: derived})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group = stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			var retry *stateTransaction
			for _, tx := range c.Transactions {
				if tx.DigestAttempts == 1 {
					retry = tx
				}
			}
			if retry == nil || retry.Native.Key == original.Native.Key || c.Snapshot.RemoteTag != "" {
				t.Fatal("digest retry adopted challenge tag or reused initial transaction")
			}
			first, _ := original.Native.Origin.Parse()
			next, _ := retry.Native.Origin.Parse()
			auth := "Authorization"
			if status == 407 {
				auth = "Proxy-Authorization"
			}
			if next.CSeq().SeqNo != first.CSeq().SeqNo+1 || header(next, auth) == "" {
				t.Fatal("digest retry lost CSeq or authorization")
			}
			accepted := forkReply(t, retry, 200, "accepted-tag")
			for _, field := range []string{"cseq", "branch", "call-id", "from-tag"} {
				parsed, err := accepted.Parse()
				if err != nil {
					t.Fatal(err)
				}
				response := parsed.(*sip.Response)
				switch field {
				case "cseq":
					response.CSeq().SeqNo++
				case "branch":
					response.Via().Params.Add("branch", "z9hG4bK-foreign")
				case "call-id":
					id := sip.CallIDHeader("foreign-call")
					response.ReplaceHeader(&id)
				case "from-tag":
					response.From().Params.Add("tag", "foreign-from")
				}
				refused := f.apply(t, ref, "migration-digest-invalid-"+field, stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Application: f.app, ApplicationLeaseTerm: f.term,
					Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: []byte(response.String())}})
				if refused.Error == "" {
					t.Fatal("digest retry accepted foreign response", field)
				}
			}
			result = f.apply(t, ref, "migration-digest-accepted", stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: accepted.Wire}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, group = stateTestLoad(t, f.engine, ref)
			c = group.Contexts[ref.Id]
			if !c.Snapshot.Answered || c.Snapshot.RemoteTag != "accepted-tag" || !proto.Equal(c.Flow, f.flow) || c.Delivery != nil {
				t.Fatal("retry final lost accepted dialog identity or granted delivery authority")
			}
			ack := stateWireBody(t, record, "ACK", "accepted-tag")
			parsed, err := sip.ParseMessage(ack.Wire)
			if err != nil || parsed.CSeq().SeqNo != next.CSeq().SeqNo {
				t.Fatal("ACK used stale initial INVITE identity")
			}
		})
	}
}
