package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

type nativeOwnerFixture struct {
	engine     *StateEngine
	client     *redis.Client
	prefix     string
	app        *pb.ProcessRef
	term       string
	config     state.Config
	flow       *pb.FlowRef
	admissions map[string]*pb.CommandAdmission
}

func newNativeOwnerFixture(t *testing.T) nativeOwnerFixture {
	return newNativeOwnerFixtureBackend(t, state.RedisBackend)
}

func newNativeOwnerFixtureBackend(t *testing.T, backend string) nativeOwnerFixture {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if address == "" && backend == state.RedisBackend {
		t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for native owner qualification")
	}
	ctx := context.Background()
	cfg := state.Config{Redis: state.RedisOptions{Address: address}, Prefix: "sipstate:native-owner-test", Namespace: "test-" + uuid.NewString(),
		NamespaceKey: bytes.Repeat([]byte{31}, 32), Partitions: 1, Generation: "generation", Versions: NativeStateVersions()}
	var store state.Backend
	var err error
	if backend == state.MemoryBackend {
		store, err = state.NewMemory(cfg)
	} else {
		store, err = state.Open(ctx, cfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: address})
	prefix := cfg.Prefix + ":" + cfg.Namespace + ":"
	t.Cleanup(func() {
		defer store.Close()
		defer client.Close()
		if backend == state.MemoryBackend {
			return
		}
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 128).Result()
			if err != nil {
				t.Error(err)
				return
			}
			if len(keys) > 0 {
				if err = client.Del(ctx, keys...).Err(); err != nil {
					t.Error(err)
				}
			}
			if next == 0 {
				return
			}
			cursor = next
		}
	})
	e := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "sip-a", Epoch: "epoch"}, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tls", StateIdentityKey: cfg.NamespaceKey}, store: store, ctx: ctx}
	e.runtime, err = execution.NewCAS(store, execution.ReducerFunc(e.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "controller-epoch"}
	lease, err := e.ApplicationLease(ctx, app, "")
	if err != nil {
		t.Fatal(err)
	}
	return nativeOwnerFixture{engine: e, client: client, prefix: prefix, app: app, term: lease.Term, config: cfg, admissions: make(map[string]*pb.CommandAdmission),
		flow: &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "flow-a", SourceIp: "192.0.2.10", SourcePort: 50000, Listener: "sip.example.test:5061", Transport: "tls"}}
}

func (f nativeOwnerFixture) apply(t *testing.T, ref *pb.ContextRef, id string, in stateEngineInput) *pb.OperationResult {
	t.Helper()
	group, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	in.Context = ref
	in.Operation = id
	if in.Kind == "command" && in.Command != nil && in.Command.GetCancel() == nil && in.Command.Admission == nil {
		f.admitCommand(t, ref, id, in.Command)
	}
	result, err := f.engine.apply(context.Background(), group, id, in, time.Now().Add(2*time.Second), false)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// The fixture acts as an admitted Controller producer. Retried commands retain the
// original decision bytes, including its fixed expiry and delivery observation.
func (f nativeOwnerFixture) admitCommand(t *testing.T, ref *pb.ContextRef, operation string, command *pb.Command) {
	t.Helper()
	if decision := f.admissions[operation]; decision != nil {
		command.Admission = proto.Clone(decision).(*pb.CommandAdmission)
		return
	}
	_, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	decision := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli())}
	if c != nil {
		if c.OwnerDirectoryEpoch != "" {
			decision.DirectoryEpoch = c.OwnerDirectoryEpoch
		}
		if c.Delivery != nil {
			decision.ExpectedDelivery = proto.Clone(c.Delivery).(*pb.ContextDelivery)
		}
	}
	if respond := command.GetRespond(); respond != nil && respond.Authority != nil {
		decision.DirectoryEpoch = respond.Authority.DirectoryEpoch
	}
	if command.GetOpenUac().GetBindingId() != "" || command.GetDialogRequest() != nil {
		flow := f.flow
		if c != nil && c.Delivery.GetFlow() != nil {
			flow = c.Delivery.Flow
		} else if c != nil && c.Flow != nil {
			flow = c.Flow
		}
		if flow != nil {
			decision.SelectedFlow = proto.Clone(flow).(*pb.FlowRef)
		}
	}
	command.Admission = decision
	f.admissions[operation] = proto.Clone(decision).(*pb.CommandAdmission)
}

func (f nativeOwnerFixture) admit(t *testing.T) *pb.ContextRef {
	t.Helper()
	return f.admitCall(t, "native-owner")
}
func (f nativeOwnerFixture) admitCall(t *testing.T, callID string) *pb.ContextRef {
	t.Helper()
	message := stateTestRequest("INVITE", 1, "", callID)
	if callID != "native-owner" {
		var err error
		message, err = Observe(bytes.ReplaceAll(message.Wire, []byte("redis-native-call@example.test"), []byte(callID+"@example.test")), false)
		if err != nil {
			t.Fatal(err)
		}
	}
	ref, err := f.engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	result := f.apply(t, ref, "initial", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	return ref
}

func (f nativeOwnerFixture) outboundRef(t *testing.T) *pb.ContextRef {
	t.Helper()
	placeholder := &pb.ContextRef{Owner: f.engine.cfg.Process, Id: "outgoing-" + uuid.NewString()}
	group, err := f.engine.bootstrapGroup(f.app, placeholder)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.apply(context.Background(), group, uuid.NewString(), stateEngineInput{Kind: "locate"}, time.Now().Add(2*time.Second), true); err != nil {
		t.Fatal(err)
	}
	record, err := f.engine.store.Load(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	return f.engine.contextRef(record.Ref, placeholder.Id)
}

func fixtureResponseAuthority(snapshot *pb.ContextSnapshot, origin *pb.SipMessage, epoch string) *pb.ResponseAuthority {
	authority := &pb.ResponseAuthority{DirectoryEpoch: epoch}
	if origin.Method != "INVITE" {
		return authority
	}
	logical := &pb.ContextRef{Id: snapshot.Context.Id, State: snapshot.Context.State}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(logical)
	hash := sha256.New()
	hash.Write([]byte("fixture-response-authority-v1:" + epoch))
	hash.Write(encoded)
	hash.Write(origin.Wire)
	authority.CompletionScope = hex.EncodeToString(hash.Sum(nil))
	return authority
}

func (f nativeOwnerFixture) responseAuthority(t *testing.T, ref *pb.ContextRef, requestID string) *pb.ResponseAuthority {
	t.Helper()
	_, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	tx := c.Transactions[c.Requests[requestID]]
	if tx == nil {
		t.Fatal("fixture response request unavailable")
	}
	message, err := Observe(tx.Native.Origin.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	epoch := c.OwnerDirectoryEpoch
	if epoch == "" {
		epoch = "directory"
	}
	return fixtureResponseAuthority(c.Snapshot, message, epoch)
}

func (f nativeOwnerFixture) expire(t *testing.T, app *pb.ProcessRef, remaining int64) {
	t.Helper()
	id := f.engine.applicationLeaseID(app)
	err := f.client.Eval(context.Background(), `
local clock=redis.call('TIME');local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local expiry=now+tonumber(ARGV[2]);redis.call('HSET',KEYS[1],'expiresAtMs',string.format('%.0f',expiry),'recoveryUntilMs',tonumber(ARGV[2])<=0 and string.format('%.0f',now-1) or '0')
redis.call('ZADD',KEYS[2],expiry,ARGV[1]);return expiry
`, []string{f.prefix + "{p0000}:application-lease:" + id, f.prefix + "{p0000}:application-leases"}, id, remaining).Err()
	if err != nil {
		t.Fatal(err)
	}
}

func (f nativeOwnerFixture) expireUnobserved(t *testing.T) {
	t.Helper()
	id := f.engine.applicationLeaseID(f.app)
	if err := f.client.HSet(context.Background(), f.prefix+"{p0000}:application-lease:"+id,
		"expiresAtMs", time.Now().Add(-time.Minute).UnixMilli(), "recoveryUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
}

func (f nativeOwnerFixture) secondWorker(t *testing.T) nativeOwnerFixture {
	t.Helper()
	store, err := state.Open(context.Background(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := &StateEngine{cfg: f.engine.cfg, store: store, ctx: context.Background()}
	e.cfg.Process = &pb.ProcessRef{InstanceId: "sip-b", Epoch: "epoch-b"}
	e.runtime, err = execution.NewCAS(store, execution.ReducerFunc(e.reduce), 8)
	if err != nil {
		t.Fatal(err)
	}
	f.engine = e
	return f
}

func TestNativeOwnerOtherWorkerObservesExpiryBeforeConnectedOwnerRecovers(t *testing.T) {
	a := newNativeOwnerFixture(t)
	ref := a.admit(t)
	// The only authenticated Controller socket is on A. B sees the expiration first,
	// reproducing scheduler order independently of heartbeat timing.
	stateControllerPair(t, a.engine, a.app, &pb.ProcessRef{InstanceId: "unrelated", Epoch: "epoch"})
	b := a.secondWorker(t)
	a.expireUnobserved(t)
	result := b.apply(t, ref, "b-observes-expiry-first", stateEngineInput{Kind: "locate"})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, b.engine, ref)
	lease := record.ApplicationLeases[a.engine.applicationLeaseID(a.app)]
	if group.Contexts[ref.Id].OwnerLost || lease.RecoveryUntilMS <= lease.CheckedAtMS {
		t.Fatal("worker without the session sealed the original owner immediately")
	}
	peer, err := a.engine.control.Peer(a.app)
	if err != nil {
		t.Fatal(err)
	}
	if err = peer.ResumeApplicationLease(); err != nil {
		t.Fatal(err)
	}
	result = a.apply(t, ref, "a-resumes-exact-owner", stateEngineInput{Kind: "locate"})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	result = b.apply(t, ref, "b-observes-renewal", stateEngineInput{Kind: "locate"})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group = stateTestLoad(t, b.engine, ref)
	lease = record.ApplicationLeases[a.engine.applicationLeaseID(a.app)]
	if group.Contexts[ref.Id].OwnerLost || lease.Term != a.term || lease.ExpiresAtMS <= lease.CheckedAtMS || lease.RecoveryUntilMS != 0 {
		t.Fatalf("shared recovery lost the live original Controller term: %+v", lease)
	}
}

func TestNativeOwnerDeadProcessSealsAfterFixedSharedRecoveryWindow(t *testing.T) {
	a := newNativeOwnerFixture(t)
	ref := a.admit(t)
	b := a.secondWorker(t)
	a.expireUnobserved(t)
	b.apply(t, ref, "first-expired-observation", stateEngineInput{Kind: "locate"})
	record, group := stateTestLoad(t, b.engine, ref)
	deadline := record.ApplicationLeases[a.engine.applicationLeaseID(a.app)].RecoveryUntilMS
	if group.Contexts[ref.Id].OwnerLost || deadline <= time.Now().UnixMilli() {
		t.Fatal("missing recovery window")
	}
	// A second worker and repeated readers cannot restart the shared clock.
	a.apply(t, ref, "another-reader", stateEngineInput{Kind: "locate"})
	record, _ = stateTestLoad(t, a.engine, ref)
	if record.ApplicationLeases[a.engine.applicationLeaseID(a.app)].RecoveryUntilMS != deadline {
		t.Fatal("reader extended owner loss indefinitely")
	}
	timer := time.NewTimer(time.Until(time.UnixMilli(deadline)) + 20*time.Millisecond)
	defer timer.Stop()
	<-timer.C
	b.apply(t, ref, "owner-death-after-fixed-window", stateEngineInput{Kind: "locate"})
	_, group = stateTestLoad(t, a.engine, ref)
	if !group.Contexts[ref.Id].OwnerLost {
		t.Fatal("genuinely dead owner survived beyond fixed grace")
	}
	if _, err := a.engine.ApplicationLease(context.Background(), a.app, a.term); err == nil {
		t.Fatal("sealed term revived after cleanup")
	}
}

func TestNativeOwnerEndedGroupOffersSharedTermRecoveryWithoutRevivingCall(t *testing.T) {
	a := newNativeOwnerFixture(t)
	ended, active := a.admitCall(t, "ended-owner-group"), a.admitCall(t, "active-owner-group")
	record, group := stateTestLoad(t, a.engine, ended)
	group.Contexts[ended.Id].Snapshot.Ended = true
	group.Contexts[ended.Id].RetireMS = time.Now().Add(time.Minute).UnixMilli()
	encoded, err := json.Marshal(group)
	if err != nil {
		t.Fatal(err)
	}
	input := state.Input{ID: "fixture-ended", Kind: "fixture", Fingerprint: "fixture-ended", DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli()}
	step, err := a.engine.store.(*state.Store).Begin(context.Background(), record.Ref, "fixture", input)
	if err != nil {
		t.Fatal(err)
	}
	next := step.Step.Record.Payload
	next.ProtocolState = encoded
	if _, err = a.engine.store.(*state.Store).Commit(context.Background(), *step.Step, next, nil); err != nil {
		t.Fatal(err)
	}
	stateControllerPair(t, a.engine, a.app, &pb.ProcessRef{InstanceId: "unrelated", Epoch: "epoch"})
	b := a.secondWorker(t)
	a.expireUnobserved(t)
	b.apply(t, ended, "b-sees-ended-group-first", stateEngineInput{Kind: "locate"})
	peer, err := a.engine.control.Peer(a.app)
	if err != nil {
		t.Fatal(err)
	}
	if err = peer.ResumeApplicationLease(); err != nil {
		t.Fatal(err)
	}
	a.apply(t, ended, "a-resumes-from-ended-group", stateEngineInput{Kind: "locate"})
	_, group = stateTestLoad(t, a.engine, ended)
	if !group.Contexts[ended.Id].Snapshot.Ended || group.Contexts[ended.Id].OwnerLost {
		t.Fatal("ended context revived or fenced the shared owner")
	}
	b.apply(t, active, "active-group-after-ended-recovery", stateEngineInput{Kind: "locate"})
	record, group = stateTestLoad(t, b.engine, active)
	lease := record.ApplicationLeases[a.engine.applicationLeaseID(a.app)]
	if group.Contexts[active.Id].OwnerLost || group.Contexts[active.Id].Snapshot.Ended || lease.Term != a.term || lease.ExpiresAtMS <= lease.CheckedAtMS {
		t.Fatal("ended group caused another active call's owner loss")
	}
}

func TestNativeOwnerAdmissionPinsLeaseWithoutControllerProbe(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	if c.Snapshot.ApplicationLeaseTerm != f.term || !c.OwnerValidated || c.OwnerProbeEffect != "" || len(record.Payload.ApplicationLeaseIDs) != 1 {
		t.Fatal("native admission did not pin its exact original lease")
	}
	var requestID string
	for id := range c.Requests {
		requestID = id
	}
	var response, presentation bool
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "owner" {
			t.Fatal("native admission depended on a Controller liveness probe")
		}
		presentation = presentation || effect.Kind == "present"
	}
	responseResult := f.apply(t, ref, "select-final", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 403, Authority: f.responseAuthority(t, ref, requestID)}}}})
	if responseResult.Error != "" {
		t.Fatal(responseResult.Error)
	}
	record, _ = stateTestLoad(t, f.engine, ref)
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if err := json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if effect.Kind == "owner" {
			t.Fatal("native admission depended on a Controller liveness probe")
		}
		if effect.Kind == "present" {
			presentation = true
		}
		if body.Authorization != nil {
			response = true
			if body.Authorization.ApplicationLeaseTerm != f.term || body.Authorization.CompletionScope != f.engine.nativeCompletionScope(ref) || effect.ApplicationLease != nil || effect.Kind != "deliver" || effect.Status != state.EffectAuthorized {
				t.Fatal("native response did not retain independent completion authority")
			}
		}
	}
	if !response || !presentation {
		t.Fatalf("admission omitted its protocol response or business presentation: response=%v presentation=%v effects=%d", response, presentation, len(record.Payload.Effects))
	}
}

func TestNativeOwnerExpiryFenceSurvivesCommandRefusalAndSameEpochReacquire(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	f.expire(t, f.app, 0)
	command := &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	result := f.apply(t, ref, "expired-command", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, Command: command})
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("expired owner command was accepted")
	}
	_, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	if !c.OwnerLost || c.Snapshot.CommandSequence != 0 || c.OwnerCleanupAtMS == 0 {
		t.Fatal("command rollback discarded the permanent native fence")
	}
	lease, err := f.engine.ApplicationLease(context.Background(), f.app, "")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Term == f.term {
		t.Fatal("fresh handshake revived its expired term")
	}
	result = f.apply(t, ref, "successor-command", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: lease.Term, Command: command})
	_, group = stateTestLoad(t, f.engine, ref)
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || !group.Contexts[ref.Id].OwnerLost || group.Contexts[ref.Id].Snapshot.ApplicationLeaseTerm != f.term {
		t.Fatal("same-epoch successor rebound an old context")
	}
}

func TestNativeOwnerLiveReconnectRejectsStaleTermWithoutFalseDeath(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	_, group := stateTestLoad(t, f.engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	command := &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 403, Authority: f.responseAuthority(t, ref, requestID)}}}
	result := f.apply(t, ref, "wrong-term", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: uuid.NewString(), Command: command})
	_, group = stateTestLoad(t, f.engine, ref)
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED || group.Contexts[ref.Id].OwnerLost {
		t.Fatal("stale session was admitted or declared a live owner dead")
	}
	lease, err := f.engine.ApplicationLease(context.Background(), f.app, "")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Term != f.term {
		t.Fatal("living control reconnect changed the term")
	}
	result = f.apply(t, ref, "live-term", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: lease.Term, Command: command})
	if result.Error != "" {
		t.Fatalf("living owner could not continue: %s", result.Error)
	}
}

func TestNativeOwnerApplicationDeadlineAndReservationGuard(t *testing.T) {
	f := newNativeOwnerFixture(t)
	f.expire(t, f.app, 500)
	ref := f.outboundRef(t)
	command := &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding"}}}
	result := f.apply(t, ref, "open", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, Command: command})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	lease := record.ApplicationLeases[f.engine.applicationLeaseID(f.app)]
	var found bool
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if err := json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if body.Authorization == nil || body.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
			continue
		}
		found = true
		if body.Authorization.ApplicationLeaseTerm != f.term || int64(body.Authorization.DeadlineMs) > lease.ExpiresAtMS || effect.DeadlineMS > lease.ExpiresAtMS || effect.ApplicationLease == nil || effect.ApplicationLease.Term != f.term || group.Contexts[ref.Id].Snapshot.ApplicationLeaseTerm != f.term {
			t.Fatal("application wire escaped its pinned lease horizon or atomic guard")
		}
	}
	if !found {
		t.Fatal("outbound admission produced no application authorization")
	}
}

func TestNativeOwnerDirectoryProofCannotReplaceLeaseAuthority(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	record, group := stateTestLoad(t, f.engine, ref)
	r := &stateReduction{engine: f.engine, record: record, group: group, payload: record.Payload, now: record.ApplicationLeases[f.engine.applicationLeaseID(f.app)].CheckedAtMS,
		in: stateEngineInput{Kind: "owner-result", Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: ref, Application: f.app, Live: false}}}}}
	if err := r.ownerProbeResult(); err == nil || group.Contexts[ref.Id].OwnerLost {
		t.Fatal("trusted-directory liveness replaced native lease authority")
	}
}

func TestNativeOwnerSubscriptionAdoptionPinsOnlyApprovedSuccessor(t *testing.T) {
	for _, recoverable := range []bool{false, true} {
		name := "legacy"
		if recoverable {
			name = "recoverable"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			f.engine.control = &control.Server{}
			message := stateTestRequest("SUBSCRIBE", 1, "", "native-subscription")
			wire := strings.Replace(string(message.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 120\r\nContent-Length:", 1)
			message, err := Observe([]byte(wire), false)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := f.engine.Locate(context.Background(), message)
			if err != nil {
				t.Fatal(err)
			}
			result := f.apply(t, ref, "subscribe", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire, PrincipalId: "phone", BindingId: "binding"}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group := stateTestLoad(t, f.engine, ref)
			var requestID string
			for id := range group.Contexts[ref.Id].Requests {
				requestID = id
			}
			result = f.apply(t, ref, "accept-subscribe", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
				Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202,
					Headers: []*pb.Header{{Name: "Expires", Value: "120"}}, Authority: f.responseAuthority(t, ref, requestID)}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group = stateTestLoad(t, f.engine, ref)
			accepted := *group.Contexts[ref.Id].Subscription
			other := &pb.ProcessRef{InstanceId: "controller-b", Epoch: "epoch-b"}
			var lease *pb.ApplicationLease
			if recoverable {
				owner := state.ApplicationLeaseOwner{ApplicationLeaseHolder: state.ApplicationLeaseHolder{
					PrincipalHash: controllerProofHash("subscription-principal"), HolderID: other.InstanceId, HolderEpoch: other.Epoch},
					TokenHash: controllerProofHash("subscription-recovery-token"), ReattachTimeoutMS: controllerReattachTimeoutMS}
				acquired, acquireErr := f.engine.store.AcquireRecoverableApplicationLease(context.Background(), f.engine.applicationLeaseID(other), uuid.NewString(), owner)
				if acquireErr != nil {
					t.Fatal(acquireErr)
				}
				lease = f.engine.controllerLease(other, acquired, "")
			} else {
				lease, err = f.engine.ApplicationLease(context.Background(), other, "")
				if err != nil {
					t.Fatal(err)
				}
			}
			command := &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "NOTIFY"}}}
			f.admitCommand(t, ref, "unclaimed-controller", command)
			logical, err := f.engine.group(ref)
			if err != nil {
				t.Fatal(err)
			}
			result, err = f.engine.apply(context.Background(), logical, "unclaimed-controller", stateEngineInput{Kind: "command", Context: ref,
				Operation: "unclaimed-controller", Application: other, ApplicationLeaseTerm: lease.Term, ApplicationGeneration: lease.AttachmentGeneration, Command: command}, time.Now().Add(2*time.Second), false)
			if !errors.Is(err, errStateApplicationOwner) && (err != nil || result == nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED) {
				t.Fatalf("a live unrelated lease adopted the subscription: %+v %v", result, err)
			}
			clock, err := f.client.Time(context.Background()).Result()
			if err != nil {
				t.Fatal(err)
			}
			result = f.apply(t, ref, "approved-controller", stateEngineInput{Kind: "owner-control", Application: other, ApplicationLeaseTerm: lease.Term, ApplicationGeneration: lease.AttachmentGeneration,
				Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: &pb.ApplicationStatus{Context: ref, Application: other,
					DirectoryEpoch: "directory", Live: true, CheckedAtMs: uint64(clock.UnixMilli())}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group = stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			if !sameProcess(c.Snapshot.Application, other) || c.Snapshot.ApplicationLeaseTerm != lease.Term || c.Snapshot.AttachmentGeneration != lease.AttachmentGeneration || c.Subscription.ExpiresMS != accepted.ExpiresMS || c.Subscription.Target != accepted.Target {
				t.Fatal("approved controller failed to pin its term or changed the accepted subscription")
			}
			notify := proto.Clone(command).(*pb.Command)
			notify.Admission = nil
			result = f.apply(t, ref, "approved-notify", stateEngineInput{Kind: "command", Application: other,
				ApplicationLeaseTerm: lease.Term, ApplicationGeneration: lease.AttachmentGeneration, Command: notify})
			if result.Error != "" || result.Certainty == pb.OperationResult_NOT_ACCEPTED {
				t.Fatalf("approved successor NOTIFY refused: %+v", result)
			}
			_, group = stateTestLoad(t, f.engine, ref)
			c = group.Contexts[ref.Id]
			if c.OwnerLost || c.Snapshot.AttachmentGeneration != lease.AttachmentGeneration || c.Subscription.ExpiresMS != accepted.ExpiresMS || c.Subscription.Target != accepted.Target {
				t.Fatal("successor business work lost its generation or changed accepted subscription metadata")
			}
			// A same-owner refresh completes through an internal callback with
			// no controller input generation. Its retained owner must remain pinned.
			refresh := stateTestRequest("SUBSCRIBE", 2, c.Snapshot.LocalTag, "native-subscription-refresh")
			refreshWire := strings.Replace(string(refresh.Wire), "Content-Length:", "Event: message-summary\r\nExpires: 180\r\nContent-Length:", 1)
			refresh, err = Observe([]byte(refreshWire), false)
			if err != nil {
				t.Fatal(err)
			}
			result = f.apply(t, ref, "same-owner-refresh", stateEngineInput{Kind: "ingest", Application: other,
				ApplicationLeaseTerm: lease.Term, ApplicationGeneration: lease.AttachmentGeneration,
				Ingest: &pb.OwnerIngest{Context: ref, Application: other, Flow: f.flow, Wire: refresh.Wire, PrincipalId: "phone", BindingId: "binding"}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group = stateTestLoad(t, f.engine, ref)
			requestID = ""
			for id, key := range group.Contexts[ref.Id].Requests {
				if bytes.Equal(group.Contexts[ref.Id].Transactions[key].Native.Origin.Wire, refresh.Wire) {
					requestID = id
				}
			}
			if requestID == "" {
				t.Fatal("same-owner refresh request not retained")
			}
			result = f.apply(t, ref, "accept-same-owner-refresh", stateEngineInput{Kind: "command", Application: other,
				ApplicationLeaseTerm: lease.Term, ApplicationGeneration: lease.AttachmentGeneration,
				Command: &pb.Command{Context: ref, Sequence: 3, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 202,
					Headers: []*pb.Header{{Name: "Expires", Value: "180"}}, Authority: f.responseAuthority(t, ref, requestID)}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, _ := stateTestLoad(t, f.engine, ref)
			var adoption state.Effect
			var proposal *pb.FlowAdoptionRequest
			for _, effect := range record.Payload.Effects {
				if effect.Kind == "adopt" && effect.Status == state.EffectPending {
					var body stateEngineEffect
					if err := json.Unmarshal(effect.Payload, &body); err != nil {
						t.Fatal(err)
					}
					adoption, proposal = effect, body.Envelope.GetFlowAdoption()
				}
			}
			if proposal == nil || !proposal.Subscription || !sameProcess(proposal.Application, other) || !proto.Equal(proposal.Flow, f.flow) {
				t.Fatal("same-owner refresh did not retain its exact publication proposal")
			}
			f.apply(t, ref, "same-owner-publication-start", stateEngineInput{Kind: "effect-start", Effect: adoption.ID})
			receipt := &pb.ContextDelivery{Flow: f.flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "directory"}
			result = f.apply(t, ref, "same-owner-publication-complete", stateEngineInput{Kind: "effect-result", Effect: adoption.ID, Executor: f.engine.cfg.Process,
				Reply: &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: receipt}}}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			_, group = stateTestLoad(t, f.engine, ref)
			c = group.Contexts[ref.Id]
			if c.OwnerLost || !sameProcess(c.Snapshot.Application, other) || c.Snapshot.AttachmentGeneration != lease.AttachmentGeneration ||
				c.Snapshot.ApplicationLeaseTerm != lease.Term || !proto.Equal(c.Delivery, receipt) || c.Subscription.ExpiresMS <= accepted.ExpiresMS {
				t.Fatal("same-owner publication lost its owner generation or failed to accept the refresh")
			}
			command = proto.Clone(command).(*pb.Command)
			command.Sequence = 4
			command.Admission = nil
			f.admitCommand(t, ref, "old-controller", command)
			if _, err = f.engine.ApplicationLease(context.Background(), f.app, f.term); err != nil {
				t.Fatal(err)
			}
			result, err = f.engine.apply(context.Background(), logical, "old-controller", stateEngineInput{Kind: "command", Context: ref,
				Operation: "old-controller", Application: f.app, ApplicationLeaseTerm: f.term, Command: command}, time.Now().Add(2*time.Second), false)
			_, group = stateTestLoad(t, f.engine, ref)
			if (!errors.Is(err, errStateApplicationOwner) && (err != nil || result == nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED)) ||
				!sameProcess(group.Contexts[ref.Id].Snapshot.Application, other) || group.Contexts[ref.Id].Snapshot.ApplicationLeaseTerm != lease.Term {
				t.Fatal("old controller heartbeat or command retook the adopted subscription")
			}
		})
	}
}

func TestNativeOwnerExpiryPreservesReservedApplicationUnknown(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.outboundRef(t)
	var err error
	result := f.apply(t, ref, "open-unknown", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding"}}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, f.engine, ref)
	var selected state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		var candidate stateEngineEffect
		if err = json.Unmarshal(effect.Payload, &candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.Authorization != nil && candidate.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION {
			selected, body = effect, candidate
			break
		}
	}
	if selected.ID == "" {
		t.Fatal("initial application authorization missing")
	}
	if !body.Native || body.Permit == nil || selected.Kind != "deliver" || selected.Status != state.EffectAuthorized {
		t.Fatal("application policy did not produce a native-local authorized write")
	}
	permit := proto.Clone(body.Permit).(*pb.SendPermit)
	write := &pb.FlowWrite{Flow: permit.Flow, Effect: body.Authorization.Effect, Permit: permit}
	reserved, err := f.engine.Reserve(context.Background(), write)
	if err != nil || reserved == nil || len(reserved.Wire) == 0 {
		t.Fatalf("real native reservation failed: %+v %v", reserved, err)
	}
	if err = f.engine.Complete(context.Background(), write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_UNKNOWN}); err != nil {
		t.Fatal(err)
	}
	f.expire(t, f.app, 0)
	result = f.apply(t, ref, "fenced-command", stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}})
	if result.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("expired application command was admitted")
	}
	record, group := stateTestLoad(t, f.engine, ref)
	if !group.Contexts[ref.Id].OwnerLost {
		t.Fatal("native owner was not fenced")
	}
	for _, effect := range record.Payload.Effects {
		if effect.ID != selected.ID {
			continue
		}
		if err = json.Unmarshal(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if effect.Status != state.EffectUnknown || effect.AttemptID != write.Effect.AttemptId || !proto.Equal(body.Permit, permit) || body.Settled {
			t.Fatal("owner expiry erased or settled a real physical UNKNOWN")
		}
		return
	}
	t.Fatal("owner expiry discarded the unresolved physical effect")
}

func TestNativeOwnerConsumedRefusalStillHasAtomicCommitFence(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	group, err := f.engine.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	in := stateEngineInput{Kind: "command", Context: ref, Operation: "invalid-command", Application: f.app, ApplicationLeaseTerm: f.term,
		Command: &pb.Command{Context: ref, Sequence: 1}}
	f.admitCommand(t, ref, in.Operation, in.Command)
	encoded, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	candidate := state.Input{ID: in.Operation, Kind: in.Kind, Fingerprint: "invalid-command", Payload: encoded,
		DeadlineMS: time.Now().Add(time.Second).UnixMilli(), ReceiptUntilMS: time.Now().Add(time.Minute).UnixMilli(),
		ApplicationLeaseIDs: []string{f.engine.applicationLeaseID(f.app)}}
	begun, err := f.engine.store.(*state.Store).Begin(context.Background(), group, "paused", candidate)
	if err != nil {
		t.Fatal(err)
	}
	next, outcome, err := f.engine.reduce(begun.Step.Record, begun.Step.Input, begun.Step.NowMS)
	if err != nil {
		t.Fatal(err)
	}
	var refusal pb.OperationResult
	if err = json.Unmarshal(outcome, &refusal); err != nil || refusal.Certainty != pb.OperationResult_NOT_ACCEPTED || len(next.CommitLeaseGuards) != 1 {
		t.Fatalf("consumed command refusal lost its live term fence: %+v guards=%v %v", &refusal, next.CommitLeaseGuards, err)
	}
	f.expire(t, f.app, 0)
	if _, err = f.engine.store.(*state.Store).Commit(context.Background(), *begun.Step, next, outcome); !errors.Is(err, state.ErrStale) {
		t.Fatalf("command refusal committed a sequence after lease expiry: %v", err)
	}
	resumed, err := f.engine.store.(*state.Store).Resume(context.Background(), group, "recovery")
	if err != nil {
		t.Fatal(err)
	}
	next, outcome, err = f.engine.reduce(resumed.Step.Record, resumed.Step.Input, resumed.Step.NowMS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.engine.store.(*state.Store).Commit(context.Background(), *resumed.Step, next, outcome); err != nil {
		t.Fatal(err)
	}
	_, current := stateTestLoad(t, f.engine, ref)
	if !current.Contexts[ref.Id].OwnerLost || current.Contexts[ref.Id].Snapshot.CommandSequence != 0 {
		t.Fatal("fresh expired-term reduction lost the permanent fence or consumed a business sequence")
	}
}
