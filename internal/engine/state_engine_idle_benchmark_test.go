package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type idleProfileFixture struct {
	Engine    *StateEngine
	Leases    map[string]state.ApplicationLease
	Record    state.Record
	Payload   []byte
	Config    state.Config
	Process   *pb.ProcessRef
	Authority state.Authority
	Input     state.Input
	Now       int64
}

// Construct a completed subscription through the normal native admission,
// reservation, write and receiver-settlement paths outside the timed loop.
// The minimum is SUBSCRIBE plus its initial NOTIFY; that next transition also
// collects the preceding response's settled physical intent.
func idleSubscriptionOwnerFixture(t testing.TB, transactions int, backend ...string) idleProfileFixture {
	return idleSubscriptionOwnerFixtureObserved(t, transactions, nil, backend...)
}

// observe sees real store reads before reduction; it is only used to capture
// immutable benchmark inputs outside timing, never by the runtime.
func idleSubscriptionOwnerFixtureObserved(t testing.TB, transactions int, observe func(state.Record, state.Input, int64), backend ...string) idleProfileFixture {
	return subscriptionHistoryFixtureObserved(t, transactions, false, observe, backend...)
}

func subscriptionHistoryFixtureObserved(t testing.TB, transactions int, mixed bool, observe func(state.Record, state.Input, int64), backend ...string) idleProfileFixture {
	t.Helper()
	selected := state.MemoryBackend
	if len(backend) > 0 {
		selected = backend[0]
	}
	f := newNativeOwnerFixtureBackend(t, selected)
	if observe != nil {
		var err error
		f.engine.runtime, err = execution.NewCAS(f.engine.store, execution.ReducerFunc(func(record state.Record, input state.Input, now int64) (state.Payload, []byte, error) {
			observe(record, input, now)
			return f.engine.reduce(record, input, now)
		}), 8)
		if err != nil {
			t.Fatal(err)
		}
	}
	ref := migrationSubscribe(t, f, 3600)
	var settle func()
	settle = func() {
		t.Helper()
		record, _ := stateTestLoad(t, f.engine, ref)
		for _, effect := range record.Payload.Effects {
			var body stateEngineEffect
			if err := body.UnmarshalBinary(effect.Payload); err != nil {
				t.Fatal(err)
			}
			if effect.Kind == "adopt" {
				proposal := body.Envelope.GetFlowAdoption()
				f.apply(t, ref, "profile-adopt-start-"+effect.ID, stateEngineInput{Kind: "effect-start", Effect: effect.ID})
				result := f.apply(t, ref, "profile-adopt-result-"+effect.ID, stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: f.engine.cfg.Process,
					Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: proposal.Flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory", RegistrationTicket: "fixture-ticket"}}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				settle()
				return
			}
			if effect.Kind == "present" || effect.Kind == "event" {
				result := f.apply(t, ref, "profile-ack-"+effect.ID, stateEngineInput{Kind: "effect-result", Effect: effect.ID, Executor: f.engine.cfg.Process,
					Reply: &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}}})
				if result.Error != "" {
					t.Fatal(result.Error)
				}
				continue
			}
			if effect.Kind != "deliver" || effect.Status != state.EffectAuthorized || !body.Native {
				continue
			}
			f.engine.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
				reserved, err := f.engine.Reserve(ctx, write)
				if err != nil || reserved.Known || len(reserved.Wire) == 0 {
					t.Fatal("fixture reservation failed", err)
				}
				result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN, Recorded: true}
				if err := f.engine.Complete(ctx, write, result); err != nil {
					t.Fatal(err)
				}
				return result
			}
			f.engine.dispatchNativeCompletion(context.Background(), record.Ref, effect, body, func() {})
		}
	}
	settle()
	for n := 2; n <= transactions; n++ {
		if mixed && n > 2 {
			_, g := stateTestLoad(t, f.engine, ref)
			c := g.Contexts[ref.Id]
			request := stateTestRequest("SUBSCRIBE", n-1, c.Snapshot.LocalTag, "profile-refresh-"+strconv.Itoa(n))
			wire := bytes.Replace(request.Wire, []byte("Content-Length:"), []byte("Event: message-summary\r\nExpires: 90\r\nContent-Length:"), 1)
			result := f.apply(t, ref, "profile-refresh-"+strconv.Itoa(n), stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: wire, PrincipalId: "phone", BindingId: "binding"}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, g = stateTestLoad(t, f.engine, ref)
			c = g.Contexts[ref.Id]
			tx := c.Transactions[c.Subscription.Pending]
			if tx == nil {
				t.Fatal("refresh not pending")
			}
			result = f.apply(t, ref, "profile-refresh-accept-"+strconv.Itoa(n), stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: c.Snapshot.CommandSequence + 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 202, Headers: []*pb.Header{{Name: "Expires", Value: "3600"}}}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			settle()
		}
		_, beforeNotify := stateTestLoad(t, f.engine, ref)
		sequence := beforeNotify.Contexts[ref.Id].Snapshot.CommandSequence + 1
		result := f.apply(t, ref, "profile-notify-"+strconv.Itoa(n), stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
			Command: &pb.Command{Context: ref, Sequence: sequence, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY", Body: []byte("Messages-Waiting: no\r\n"), Headers: []*pb.Header{{Name: "Event", Value: "message-summary"}, {Name: "Content-Type", Value: "application/simple-message-summary"}, {Name: "Subscription-State", Value: "active;expires=3600"}}}}}})
		if result.Error != "" {
			t.Fatal(result.Error)
		}
		settle()
		_, g := stateTestLoad(t, f.engine, ref)
		var request *sip.Request
		for _, tx := range g.Contexts[ref.Id].Transactions {
			parsed, err := sip.ParseMessage(tx.Native.Origin.WireBytes())
			if err != nil {
				t.Fatal(err)
			}
			if r, ok := parsed.(*sip.Request); ok && r.Method == sip.NOTIFY && (request == nil || r.CSeq().SeqNo > request.CSeq().SeqNo) {
				request = r
			}
		}
		if request == nil {
			t.Fatal("missing fixture NOTIFY")
		}
		response := sip.NewResponseFromRequest(request, 200, "OK", nil)
		result = f.apply(t, ref, "profile-notify-response-"+strconv.Itoa(n), stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
			Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: []byte(response.String()), PrincipalId: "phone", BindingId: "binding"}})
		if result.Error != "" {
			t.Fatal(result.Error)
		}
		settle()
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	id := f.engine.applicationLeaseID(f.app)
	lease := record.ApplicationLeases[id]
	now := lease.CheckedAtMS
	lease.CheckedAtMS, lease.ExpiresAtMS = now, now+30000
	record.ApplicationLeases[id] = lease
	in := stateEngineInput{Kind: "owner-check", Operation: "profile-owner-check"}
	encoded, err := in.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := record.Payload.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	capture := idleProfileFixture{Engine: f.engine, Leases: record.ApplicationLeases, Record: record, Payload: payload, Config: f.config, Process: f.engine.cfg.Process, Authority: f.engine.store.Authority(), Now: now,
		Input: state.Input{ID: "profile-owner-check", Kind: "owner-check", Fingerprint: "profile-owner-check", Proof: true, Payload: encoded, DeadlineMS: now + 2000, ReceiptUntilMS: now + 122000}}
	capture.Record.Payload = state.Payload{}
	return capture
}

type idleProfileStore struct {
	state.Backend
	authority state.Authority
}

func (s idleProfileStore) Authority() state.Authority { return s.authority }

var idleProfileResult state.Payload

// Includes fresh outer-payload/group decode and a validated healthy-owner no-op.
// Normal operation does not schedule this check per healthy context.
// Excludes Redis, CAS, dispatch, physical I/O and fixture construction; it is not
// an application workload, fleet capacity or staging CPU measurement.
func BenchmarkIdleSubscriptionOwnerTransition(b *testing.B) {
	for _, transactions := range []int{2, 6, 16} {
		b.Run(strconv.Itoa(transactions), func(b *testing.B) {
			capture := idleSubscriptionOwnerFixture(b, transactions)
			benchmarkIdleSubscriptionOwnerTransition(b, capture)
		})
	}
}

func benchmarkIdleSubscriptionOwnerTransition(b *testing.B, capture idleProfileFixture) {
	store, err := state.NewMemory(capture.Config)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	backend := idleProfileStore{Backend: store, authority: capture.Authority}
	e := &StateEngine{cfg: Config{Process: capture.Process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:bridge@sip.example.test:5061;transport=tls", StateIdentityKey: capture.Config.NamespaceKey}, store: backend, ctx: context.Background()}
	e.runtime, err = execution.NewCAS(backend, execution.ReducerFunc(e.reduce), 8)
	if err != nil {
		b.Fatal(err)
	}
	record := capture.Record
	record.ApplicationLeases = capture.Leases
	if err = record.Payload.UnmarshalBinary(capture.Payload); err != nil {
		b.Fatal(err)
	}
	before, err := decodeStateGroup(record)
	if err != nil {
		b.Fatal(err)
	}
	checked, outcome, err := e.reduce(record, capture.Input, capture.Now)
	if !errors.Is(err, execution.ErrReadOnly) {
		b.Fatal(err)
	}
	var result pb.OperationResult
	if err = json.Unmarshal(outcome, &result); err != nil || result.Error != "" {
		b.Fatal("profile fixture did not perform healthy owner transition", err, result.Error)
	}
	after, err := decodeStateGroup(state.Record{Payload: checked, Versions: record.Versions})
	if err != nil {
		b.Fatal(err)
	}
	for id, c := range before.Contexts {
		next := after.Contexts[id]
		if next == nil || next.OwnerProbeAtMS != c.OwnerProbeAtMS || next.OwnerLost || next.Snapshot.CommandSequence != c.Snapshot.CommandSequence || len(next.Transactions) != len(c.Transactions) || len(checked.Effects) != len(record.Payload.Effects) {
			b.Fatal("profile fixture changed more than healthy owner maintenance")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err = record.Payload.UnmarshalBinary(capture.Payload); err != nil {
			b.Fatal(err)
		}
		result, _, err := e.reduce(record, capture.Input, capture.Now)
		if !errors.Is(err, execution.ErrReadOnly) {
			b.Fatal(err)
		}
		idleProfileResult = result
	}
	b.StopTimer()
}
