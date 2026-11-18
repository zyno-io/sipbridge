package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

type nativeInspectionFixture struct {
	ctx     context.Context
	client  *redis.Client
	store   *rediscore.Store
	profile rediscore.ProfileConfig
	input   redisUACQualification
}

func newNativeInspectionFixture(t *testing.T) *nativeInspectionFixture {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("disposable Redis required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	t.Cleanup(func() { client.Close() })
	root := "sipstate:test:native-inspect-" + uuid.NewString() + ":"
	profile := rediscore.ProfileConfig{Root: root, Service: "fixture", Generation: "one", IdentityKey: bytes.Repeat([]byte{8}, 32), RecordBudget: 32 << 20}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address}, Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	store, err := connection.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys, e := client.Keys(context.Background(), root+"*").Result()
		if e != nil {
			t.Error(e)
			return
		}
		if len(keys) > 0 {
			if e = client.Del(context.Background(), keys...).Err(); e != nil {
				t.Error(e)
			}
		}
	})
	process, app := &pb.ProcessRef{InstanceId: "sip-fixture", Epoch: "epoch"}, &pb.ProcessRef{InstanceId: "controller-fixture", Epoch: "epoch"}
	worker, err := nativeWorkerReference(process)
	if err != nil {
		t.Fatal(err)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			primary = strings.TrimPrefix(line, "run_id:")
		}
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: worker, Epoch: process.Epoch, PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	appID, err := nativeApplicationReference(profile.IdentityKey, app)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireApplication(ctx, appID, "fixture-term")
	if err != nil {
		t.Fatal(err)
	}
	authority := connection.Authority()
	ref := &pb.ContextRef{Id: "call", Owner: process, State: &pb.StateRef{Authority: authority, GroupId: "call", Incarnation: "incarnation", CallId: "fixture-inspection@example.test"}}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", Transport: "udp", SourceIp: "192.0.2.1", SourcePort: 5060, Listener: "sip.example.test:5060"}
	deadline := time.Now().Add(5 * time.Second)
	command := &pb.Command{Context: &pb.ContextRef{Id: ref.Id}, Sequence: 1, Admission: &pb.CommandAdmission{DirectoryEpoch: "fixture-directory", ExpiresAtMs: uint64(deadline.UnixMilli()), SelectedFlow: flow}, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Method: "INVITE", Uri: "sip:secret-target@example.test", Transport: "udp", Headers: []*pb.Header{{Name: "X-Fixture-Secret", Value: "must-not-export-this-header"}}}}}
	input := redisUACQualification{Key: profile.IdentityKey, Context: ref, Authority: authority, Application: app, Executor: process, Lease: lease.ApplicationLeaseRef, Flow: flow, Command: command, Operation: "fixture-open", Advertisement: nativeSIPAdvertisement{contactURI: "sip:sip.example.test:5060", address: "sip.example.test:5060"}, ExecutorTimer: rediscore.TimerAssignment{Worker: worker, Epoch: process.Epoch, Term: process.Epoch}, Credit: uint64(residentInviteBudget), Deadline: deadline}
	if _, err = admitRedisUACStart(ctx, store, input, false); err != nil {
		t.Fatal(err)
	}
	v, err := store.ObserveContext(ctx, ref.Id, ref.State.Incarnation, "r:context")
	if err != nil {
		t.Fatal(err)
	}
	c, err := decodeNativeContext(v.View.Records["r:context"])
	if err != nil {
		t.Fatal(err)
	}
	pView, err := store.Read(ctx, v.Identity, c.InitialProcedures[0])
	if err != nil {
		t.Fatal(err)
	}
	p, err := readPrivateProcedureRecord(pView.Records[c.InitialProcedures[0]])
	if err != nil {
		t.Fatal(err)
	}
	wire, err := store.Read(ctx, v.Identity, p.Origin)
	if err != nil {
		t.Fatal(err)
	}
	origin, err := selectedmessage.DecodeOpaque(wire.Records[p.Origin])
	if err != nil {
		t.Fatal(err)
	}
	built, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote-tag", "sip:secret-contact@example.test;transport=udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := portable.ParseMessage(built.WireBytes(), flow.Transport, net.JoinHostPort(flow.SourceIp, "5060"), "")
	if err != nil {
		t.Fatal(err)
	}
	if handled, e := receiveRedisUACProvisional(ctx, store, flow, response, profile.IdentityKey, input.ExecutorTimer, time.Now().Add(time.Second)); e != nil || !handled {
		t.Fatal("actual provisional transition", handled, e)
	}
	return &nativeInspectionFixture{ctx, client, store, profile, input}
}
func nativeFixtureDump(t *testing.T, f *nativeInspectionFixture) map[string]string {
	t.Helper()
	keys, err := f.client.Keys(f.ctx, f.profile.Root+"*").Result()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string)
	for _, key := range keys {
		value, e := f.client.Dump(f.ctx, key).Result()
		if e != nil {
			t.Fatal(e)
		}
		out[key] = value
	}
	return out
}
func TestNativeFixtureReadOnlyActualOwnersAndProvenance(t *testing.T) {
	f := newNativeInspectionFixture(t)
	user, password := "fixture-"+uuid.NewString(), "fixture-only-password"
	if err := f.client.ACLSetUser(f.ctx, user, "on", ">"+password, "~"+f.profile.Root+"*", "+@read", "+info", "+time", "+eval", "+evalsha").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.client.ACLDelUser(context.Background(), user).Err(); err != nil {
			t.Error(err)
		}
	})
	readonly := redis.NewClient(&redis.Options{Addr: os.Getenv("SIPBRIDGE_CORE_TEST_REDIS"), Username: user, Password: password, MaxRetries: -1})
	defer readonly.Close()
	if err := readonly.Set(f.ctx, f.profile.Root+"must-not-write", "bad", 0).Err(); err == nil {
		t.Fatal("fixture read-only ACL allowed mutation")
	}
	before := nativeFixtureDump(t, f)
	query := NativeFixtureQuery{CallID: f.input.Context.State.CallId, Application: f.input.Application}
	got, err := InspectNativeFixtureRedis(f.ctx, readonly, f.profile, query)
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != "sipbridge.fixture.native.v1" || got.Provenance.Root != f.profile.Root || !strings.HasSuffix(got.Provenance.SchemaIdentity, ":"+got.Provenance.NativeSchemaSHA256) || len(got.Provenance.NativeDescriptorSHA256) != 64 || len(got.Provenance.ProfileSHA256) != 64 || got.Redis.PrimaryID == "" || got.Redis.Role != "master" || got.Redis.CheckedAtMS == 0 {
		t.Fatal("missing actual native provenance")
	}
	if got.Application == nil || got.Application.State != "live" || got.Application.Term != f.input.Lease.Term || got.Application.ExpiresAtMS <= got.Application.CheckedAtMS {
		t.Fatal("missing actual application lease")
	}
	if len(got.Contexts) != 1 {
		t.Fatal("selected context missing")
	}
	c := got.Contexts[0]
	if c.Identity.Context != "call" || c.Revision == 0 || len(c.Procedures) != 1 || len(c.Dialogs) != 1 || len(c.Events) != 1 || len(c.Outputs) != 1 || len(c.Results) != 1 {
		t.Fatal("actual native owner graph not projected")
	}
	p := c.Procedures[0]
	if p.Key == "" || p.Family != string(portable.InviteClient) || p.Phase != string(portable.Proceeding) || p.Origin == nil || p.Final == nil || p.CommandResult == nil || !p.CommandResult.Present {
		t.Fatal("selected native procedure evidence missing")
	}
	if c.Events[0].Kind != "PROVISIONAL" || c.Events[0].Message == nil || c.Events[0].Root != p.Field || len(c.Events[0].Owners) == 0 || c.Dialogs[0].RemoteTag != "remote-tag" {
		t.Fatal("actual dialog/event association missing")
	}
	if !c.Outputs[0].Proof.Present || c.Outputs[0].Proof.State != "P" || c.Outputs[0].Proof.Settled == nil || *c.Outputs[0].Proof.Settled {
		t.Fatal("pending output proof misrepresented")
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"sip:secret-target@", "sip:secret-contact@", "must-not-export-this-header", "SIP/2.0", "registrationTicket"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatal("fixture projection leaked unapproved bytes", secret)
		}
	}
	if !reflect.DeepEqual(before, nativeFixtureDump(t, f)) {
		t.Fatal("read-only inspector changed native namespace")
	}
	dialog := c.Dialogs[0]
	exact, err := InspectNativeFixtureRedis(f.ctx, readonly, f.profile, NativeFixtureQuery{CallID: f.input.Context.State.CallId, LocalTag: dialog.LocalTag, RemoteTag: dialog.RemoteTag})
	if err != nil || len(exact.Contexts) != 1 {
		t.Fatal("canonical exact dialog selector", err)
	}
	known, err := InspectNativeFixtureRedis(f.ctx, readonly, f.profile, NativeFixtureQuery{ContextID: "call", Incarnation: "incarnation"})
	if err != nil || len(known.Contexts) != 1 {
		t.Fatal("exact context query", err)
	}
	appOnly, err := InspectNativeFixtureRedis(f.ctx, readonly, f.profile, NativeFixtureQuery{Application: f.input.Application})
	if err != nil || appOnly.Contexts == nil || len(appOnly.Contexts) != 0 {
		t.Fatal("app-only observation fabricated contexts", err)
	}
	if err = f.store.ExpireApplication(f.ctx, f.input.Lease); err != nil {
		t.Fatal(err)
	}
	expiredBefore := nativeFixtureDump(t, f)
	expired, err := InspectNativeFixtureRedis(f.ctx, readonly, f.profile, NativeFixtureQuery{Application: f.input.Application})
	if err != nil || expired.Application == nil || expired.Application.State != "expired" || expired.Application.Term != f.input.Lease.Term || expired.Application.ExpiresAtMS > expired.Application.CheckedAtMS {
		t.Fatal("actual expired metadata not observed", err)
	}
	if !reflect.DeepEqual(expiredBefore, nativeFixtureDump(t, f)) {
		t.Fatal("expired inspection mutated owner metadata")
	}
	absent, err := InspectNativeFixtureRedis(f.ctx, readonly, f.profile, NativeFixtureQuery{Application: &pb.ProcessRef{InstanceId: "never-created", Epoch: "epoch"}})
	if err != nil || absent.Application == nil || absent.Application.State != "absent" || absent.Application.Term != "" {
		t.Fatal("absent application evidence fabricated", err)
	}
}

type nativeFixtureRevisionHook struct {
	admin *redis.Client
	key   string
	once  sync.Once
}

func (h *nativeFixtureRevisionHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *nativeFixtureRevisionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *nativeFixtureRevisionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err != nil {
			return err
		}
		if cmd.Name() == "evalsha" || cmd.Name() == "eval" {
			if value, ok := cmd.(*redis.Cmd); ok {
				raw, e := value.Slice()
				if e == nil && len(raw) == 9 {
					h.once.Do(func() { err = h.admin.HIncrBy(ctx, h.key, "revision", 1).Err() })
				}
			}
		}
		return err
	}
}
func TestNativeFixtureRefusesMixedRevisionAndUnprovisionedRoot(t *testing.T) {
	f := newNativeInspectionFixture(t)
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("SIPBRIDGE_CORE_TEST_REDIS"), MaxRetries: -1})
	defer client.Close()
	client.AddHook(&nativeFixtureRevisionHook{admin: f.client, key: f.profile.Root + "core:context:call"})
	_, err := InspectNativeFixtureRedis(f.ctx, client, f.profile, NativeFixtureQuery{ContextID: "call", Incarnation: "incarnation"})
	if !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("mixed-revision proof projected", err)
	}
	absent := f.profile
	absent.Root = "sipstate:test:absent-" + uuid.NewString() + ":"
	if _, err = InspectNativeFixtureRedis(f.ctx, f.client, absent, NativeFixtureQuery{Application: f.input.Application}); err == nil {
		t.Fatal("unprovisioned root accepted")
	}
	if n, err := f.client.Exists(f.ctx, absent.Root+"core:profile").Result(); err != nil || n != 0 {
		t.Fatal("inspector provisioned an absent root", err)
	}
}

func TestNativeFixtureMessageProofUsesExactWireHash(t *testing.T) {
	wire := stateTestRequest("REGISTER", 1, "", "proof").Wire
	message, err := portable.ParseMessage(wire, "udp", "192.0.2.1:5060", "")
	if err != nil {
		t.Fatal(err)
	}
	proof, err := fixtureMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wire)
	if proof.WireSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatal("wire proof was reconstructed")
	}
}
