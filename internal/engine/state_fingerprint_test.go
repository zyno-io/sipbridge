package engine

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func sendFingerprintFixture() (*pb.ContextSnapshot, *pb.SipMessage, *pb.EffectAuthorization) {
	snapshot := &pb.ContextSnapshot{
		Context: &pb.ContextRef{Owner: &pb.ProcessRef{InstanceId: "sip-a", Epoch: "a1"}, Id: "ctx-1", State: &pb.StateRef{
			Authority: &pb.StateAuthority{Service: "edge-alpha", Generation: "g1", PrimaryId: "redis-1", SchemaVersion: 1},
			GroupId:   "group-1", Incarnation: "i1", CallId: "call-雪@例.test", Partition: 7,
		}},
		Application: &pb.ProcessRef{InstanceId: "controller-a", Epoch: "app-1"},
		PrincipalId: "alice", BindingId: "reg-alice", Transport: "tls",
	}
	message := &pb.SipMessage{Method: "INVITE", Wire: []byte("INVITE sip:alice@example.test SIP/2.0\r\nCall-ID: call-雪@例.test\r\n\r\n")}
	auth := &pb.EffectAuthorization{Effect: &pb.StateEffectRef{Context: proto.Clone(snapshot.Context).(*pb.ContextRef), AttemptId: "emission-1"},
		AuthorityClass: pb.EffectAuthorization_APPLICATION, DeadlineMs: 1234567890123}
	return snapshot, message, auth
}

func sendFingerprintFlow() *pb.FlowRef {
	return &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "sip-b", Epoch: "b1"}, ConnectionId: "c1", SourceIp: "2001:db8::1", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
}

func TestSendFingerprintSharedUTF8Vectors(t *testing.T) {
	// Expected values were independently computed from the canonical framing in
	// Python for the public controller identity used below.
	snapshot, message, auth := sendFingerprintFixture()
	snapshot.ApplicationLeaseTerm, auth.ApplicationLeaseTerm = "term-1", "term-1"
	for _, test := range []struct {
		name     string
		flow     *pb.FlowRef
		expected string
	}{
		{"no_flow", nil, "a37f0990c4b94dc9281ed6e0116ef9eb4d1459ea187f4963de1e597b48327c9d"},
		{"full_flow", sendFingerprintFlow(), "0ef35ff9807267305153d0da2a16affdcdb990ef05ff2f07f900c210add30b46"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := SendFingerprint(snapshot, message, auth, test.flow)
			if err != nil || got != test.expected {
				t.Fatalf("fingerprint %q, %v; want %s", got, err, test.expected)
			}
			prepared := proto.Clone(auth).(*pb.EffectAuthorization)
			prepared.Fingerprint, prepared.Effect.Id = got, "e-"+got
			if replay, err := SendFingerprint(snapshot, message, prepared, test.flow); err != nil || replay != got {
				t.Fatalf("exact intent cannot be revalidated: %q %v", replay, err)
			}
		})
	}
}

func TestSendFingerprintIgnoresExecutorMetadata(t *testing.T) {
	snapshot, message, auth := sendFingerprintFixture()
	want, err := SendFingerprint(snapshot, message, auth, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Context.Owner = &pb.ProcessRef{InstanceId: "sip-after-takeover", Epoch: "new-epoch"}
	snapshot.Executor = &pb.ProcessRef{InstanceId: "sip-after-takeover", Epoch: "new-epoch"}
	// Schema compatibility is validated by the runtime; it is not logical state
	// identity and is deliberately absent from the canonical fingerprint fields.
	auth.Effect.Context.State.Authority.SchemaVersion = 2
	got, err := SendFingerprint(snapshot, message, auth, nil)
	if err != nil || got != want {
		t.Fatalf("takeover changed immutable send identity: %q %v", got, err)
	}
}

func TestSendFingerprintBindsEveryIntentDimension(t *testing.T) {
	base, wire, intent := sendFingerprintFixture()
	want, err := SendFingerprint(base, wire, intent, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*pb.ContextSnapshot, *pb.SipMessage, *pb.EffectAuthorization){
		"authority_class": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.AuthorityClass = pb.EffectAuthorization_NATIVE_COMPLETION
		},
		"completion_scope": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.CompletionScope = "scope-1"
		},
		"deadline": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) { a.DeadlineMs++ },
		"emission": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.Effect.AttemptId = "emission-2"
		},
		"application_epoch": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.Application.Epoch = "app-2"
		},
		"principal": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) { s.PrincipalId = "bob" },
		"binding":   func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) { s.BindingId = "reg-bob" },
		"transport": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) { s.Transport = "tcp" },
		"method":    func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) { m.Method = "BYE" },
		"status":    func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) { m.Status = 200 },
		"wire_bytes": func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) {
			m.Wire = append(m.Wire, 0, 255)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, m, a := proto.Clone(base).(*pb.ContextSnapshot), proto.Clone(wire).(*pb.SipMessage), proto.Clone(intent).(*pb.EffectAuthorization)
			change(s, m, a)
			got, err := SendFingerprint(s, m, a, nil)
			if err != nil || got == want {
				t.Fatalf("dimension unbound: %q %v", got, err)
			}
		})
	}
	flow := sendFingerprintFlow()
	one, err := SendFingerprint(base, wire, intent, flow)
	if err != nil {
		t.Fatal(err)
	}
	flow.ConnectionId = "c2"
	two, err := SendFingerprint(base, wire, intent, flow)
	if err != nil || one == two {
		t.Fatal("physical flow not bound")
	}
}

func TestSendFingerprintExactUint64DeadlineAndLengthFraming(t *testing.T) {
	s, m, a := sendFingerprintFixture()
	a.DeadlineMs = 9007199254740992
	one, err := SendFingerprint(s, m, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.DeadlineMs++
	two, err := SendFingerprint(s, m, a, nil)
	if err != nil || one == two {
		t.Fatal("uint64 deadline rounded above 2^53")
	}
	s.PrincipalId, s.BindingId = "a", "bc"
	one, err = SendFingerprint(s, m, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.PrincipalId, s.BindingId = "ab", "c"
	two, err = SendFingerprint(s, m, a, nil)
	if err != nil || one == two {
		t.Fatal("length boundaries ambiguous")
	}
}

func TestSendFingerprintRejectsMalformedInput(t *testing.T) {
	for name, change := range map[string]func(*pb.ContextSnapshot, *pb.SipMessage, *pb.EffectAuthorization){
		"no_state": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) { s.Context.State = nil },
		"no_authority": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.Context.State.Authority = nil
		},
		"foreign_effect_context": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.Effect.Context.State.Incarnation = "different"
		},
		"foreign_call_id": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.Effect.Context.State.CallId = "different"
		},
		"no_application": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) { s.Application = nil },
		"invalid_application_ref": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.Application.Epoch = "invalid/epoch"
		},
		"invalid_schema": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.Context.State.Authority.SchemaVersion = 0
		},
		"partition_overflow": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.Context.State.Partition = 4096
		},
		"invalid_call_id": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.Context.State.CallId = "call\r\ninjected"
		},
		"invalid_utf8": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.PrincipalId = string([]byte{255})
		},
		"oversized_utf8": func(s *pb.ContextSnapshot, _ *pb.SipMessage, _ *pb.EffectAuthorization) {
			s.BindingId = strings.Repeat("雪", 86)
		},
		"missing_attempt": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) { a.Effect.AttemptId = "" },
		"missing_effect":  func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) { a.Effect = nil },
		"unknown_class":   func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) { a.AuthorityClass = 99 },
		"zero_deadline":   func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) { a.DeadlineMs = 0 },
		"invalid_method":  func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) { m.Method = "INVITE\r\n" },
		"invalid_status":  func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) { m.Status = 99 },
		"empty_wire":      func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) { m.Wire = nil },
		"oversized_wire":  func(_ *pb.ContextSnapshot, m *pb.SipMessage, _ *pb.EffectAuthorization) { m.Wire = make([]byte, 65537) },
		"wrong_effect_id": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.Effect.Id = "e-" + strings.Repeat("0", 64)
		},
		"wrong_supplied_fingerprint": func(_ *pb.ContextSnapshot, _ *pb.SipMessage, a *pb.EffectAuthorization) {
			a.Fingerprint = strings.Repeat("0", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, m, a := sendFingerprintFixture()
			change(s, m, a)
			if got, err := SendFingerprint(s, m, a, nil); err == nil || got != "" {
				t.Fatal("malformed send accepted")
			}
		})
	}
	s, m, a := sendFingerprintFixture()
	if _, err := SendFingerprint(nil, m, a, nil); err == nil {
		t.Fatal("nil snapshot accepted")
	}
	if _, err := SendFingerprint(s, nil, a, nil); err == nil {
		t.Fatal("nil message accepted")
	}
	if _, err := SendFingerprint(s, m, nil, nil); err == nil {
		t.Fatal("nil authorization accepted")
	}
	for name, change := range map[string]func(*pb.FlowRef){"ingress": func(f *pb.FlowRef) { f.Ingress = nil }, "connection": func(f *pb.FlowRef) { f.ConnectionId = "" }, "ip": func(f *pb.FlowRef) { f.SourceIp = "hostname" }, "port": func(f *pb.FlowRef) { f.SourcePort = 65536 }, "listener": func(f *pb.FlowRef) { f.Listener = "" }, "transport": func(f *pb.FlowRef) { f.Transport = "TCP" }} {
		t.Run(name, func(t *testing.T) {
			flow := sendFingerprintFlow()
			change(flow)
			if _, err := SendFingerprint(s, m, a, flow); err == nil {
				t.Fatal("partial or invalid flow accepted")
			}
		})
	}
}
