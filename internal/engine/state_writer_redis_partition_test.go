package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

// Separate clients are essential: only A is partitioned by the proxy. B and the
// namespace cleanup keep an independent path to the same isolated test Redis.
func redisReservationFaultEngines(t *testing.T) (*StateEngine, *StateEngine, *redisReservationFaultProxy, string) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_STATE_TEST_REDIS")
	if address == "" {
		t.Skip("set SIPBRIDGE_STATE_TEST_REDIS for real Redis reservation fault qualification")
	}
	proxy := newRedisReservationFaultProxy(t, address)
	cfg := state.Config{Redis: state.RedisOptions{Address: address}, Prefix: "sipstate:reservation-fault-test",
		Namespace: "test-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{71}, 32), Partitions: 1, Generation: "generation",
		Versions: state.Versions{Schema: "1", Reducer: "7", Timers: "1"}}
	direct, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal("open direct reservation-fault Redis client")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
		defer client.Close()
		defer direct.Close()
		var cursor uint64
		for {
			keys, next, err := client.Scan(cleanup, cursor, cfg.Prefix+":"+cfg.Namespace+":*", 128).Result()
			if err != nil {
				t.Error("reservation-fault namespace cleanup scan failed")
				return
			}
			if len(keys) != 0 {
				if err := client.Del(cleanup, keys...).Err(); err != nil {
					t.Error("reservation-fault namespace cleanup delete failed")
					return
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	})
	if err := direct.Ready(context.Background()); err != nil {
		t.Fatal("bootstrap reservation-fault namespace")
	}
	cfg.Redis.Address = proxy.address
	proxied, err := state.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal("open proxied reservation-fault Redis client")
	}
	t.Cleanup(func() { proxied.Close() })
	makeEngine := func(name string, store *state.Store) *StateEngine {
		engine := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: name, Epoch: "epoch"}, AdvertiseAddress: "sip.example.test:5061",
			ContactURI: "sip:sipbridge@sip.example.test:5061;transport=tcp", StateIdentityKey: cfg.NamespaceKey}, store: store, ctx: context.Background()}
		runtime, err := executionfixture.New(store, execution.ReducerFunc(engine.reduce), name, 8)
		if err != nil {
			t.Fatal("create reservation-fault execution runtime")
		}
		engine.runtime = runtime
		return engine
	}
	return makeEngine("sip-a", proxied), makeEngine("sip-b", direct), proxy, cfg.Prefix + ":" + cfg.Namespace + ":"
}

// This observer delegates every outcome unchanged. The fault is a real dropped
// Redis response, never an injected Reserve/Complete error.
type redisReservationObservedWriter struct {
	engine   *StateEngine
	reserves atomic.Int32
	results  chan error
}

func (w *redisReservationObservedWriter) Reserve(ctx context.Context, command *pb.FlowWrite) (*CommittedWrite, error) {
	w.reserves.Add(1)
	reserved, err := w.engine.Reserve(ctx, command)
	w.results <- err
	return reserved, err
}

func (w *redisReservationObservedWriter) Complete(ctx context.Context, command *pb.FlowWrite, result *pb.DeliveryResult) error {
	return w.engine.Complete(ctx, command, result)
}

func (w *redisReservationObservedWriter) InspectWrite(ctx context.Context, command *pb.FlowWrite) (*pb.DeliveryResult, error) {
	return w.engine.InspectWrite(ctx, command)
}

type redisReservationSocket struct {
	net.Conn
	writes atomic.Int32
}

func (c *redisReservationSocket) Write(wire []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(wire)
}

type redisReservationCall struct {
	engine      *StateEngine
	ingress     *Ingress
	command     *pb.FlowWrite
	socket      *redisReservationSocket
	remote      net.Conn
	writer      *redisReservationObservedWriter
	ref         state.Ref
	context     *pb.ContextRef
	effect      state.Effect
	native      portable.Transaction
	wire        []byte
	wireHash    string
	effectIDs   map[string]bool
	application *pb.ProcessRef
}

func newRedisReservationCall(t *testing.T, engine *StateEngine, name string) *redisReservationCall {
	t.Helper()
	client, remote := outboundStreamPair(t, "tcp")
	socket := &redisReservationSocket{Conn: client}
	ingress := NewIngress(engine.cfg.Process, new(control.Server), "sip.example.test:5060", 4)
	writer := &redisReservationObservedWriter{engine: engine, results: make(chan error, 8)}
	if err := ingress.ConfigureStateRuntime(writer, func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) { return nil, nil }); err != nil {
		t.Fatal("configure reservation-fault physical writer")
	}
	host, portText, err := net.SplitHostPort(remote.LocalAddr().String())
	if err != nil {
		t.Fatal("read reservation-fault peer address")
	}
	var port int
	if _, err := fmt.Sscan(portText, &port); err != nil {
		t.Fatal("read reservation-fault peer port")
	}
	flow, err := ingress.addStream(socket, host, port, "tcp")
	if err != nil {
		t.Fatal("add reservation-fault physical stream")
	}
	t.Cleanup(func() { socket.Close(); ingress.remove(flow) })
	// Distinct Call-IDs ensure B proves progress in an unrelated state group.
	seed := stateTestRequest("OPTIONS", 1, "", name)
	wire := bytes.ReplaceAll(seed.Wire, []byte("redis-native-call@example.test"), []byte(name+"@reservation.test"))
	message, err := Observe(wire, false)
	if err != nil {
		t.Fatal("observe reservation-fault context seed")
	}
	ref, err := engine.Locate(context.Background(), message)
	if err != nil {
		t.Fatal("allocate reservation-fault context")
	}
	application := &pb.ProcessRef{InstanceId: "controller-fixture", Epoch: "original-owner"}
	result := stateTestApply(t, engine, ref, name+"-open", stateEngineInput{Kind: "command", Application: application,
		Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Method: "OPTIONS",
			BindingId: "fixture-binding", PrincipalId: "fixture-phone", Transport: "tcp"}}}})
	if result.Error != "" {
		t.Fatal("reservation-fault OPTIONS creation refused")
	}
	record, _ := stateTestLoad(t, engine, ref)
	var selected state.Effect
	var body stateEngineEffect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "wire" {
			selected = effect
			if unmarshalRetainedFixture(effect.Payload, &body) != nil {
				t.Fatal("reservation-fault authorization body invalid")
			}
			break
		}
	}
	if selected.ID == "" || body.Authorization == nil || body.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
		t.Fatal("reservation-fault application intent missing")
	}
	permit := &pb.SendPermit{Authorization: body.Authorization, Flow: flow.ref, DirectoryEpoch: "directory", OrderingProof: "fixture-proof"}
	stateTestApply(t, engine, ref, name+"-authorize-start", stateEngineInput{Kind: "effect-start", Effect: selected.ID})
	granted := stateTestApply(t, engine, ref, name+"-authorize-result", stateEngineInput{Kind: "effect-result", Effect: selected.ID, Executor: engine.cfg.Process,
		Reply: &pb.Envelope{Payload: &pb.Envelope_SendPermit{SendPermit: permit}}})
	if granted.Error != "" {
		t.Fatal("reservation-fault application grant refused")
	}
	record, group := stateTestLoad(t, engine, ref)
	call := &redisReservationCall{engine: engine, ingress: ingress, socket: socket, remote: remote, writer: writer, context: ref, ref: record.Ref,
		command: &pb.FlowWrite{Flow: flow.ref, Effect: body.Authorization.Effect, Permit: permit}, effectIDs: make(map[string]bool), application: application}
	for _, effect := range record.Payload.Effects {
		call.effectIDs[effect.ID] = true
		if effect.ID == selected.ID {
			call.effect = effect
			var authorized stateEngineEffect
			if unmarshalRetainedFixture(effect.Payload, &authorized) != nil || effect.Kind != "deliver" || effect.Status != state.EffectAuthorized {
				t.Fatal("reservation-fault physical effect was not authorized")
			}
			call.wire = append([]byte(nil), authorized.Wire...)
			call.wireHash = authorized.WireHash
			call.native = group.Contexts[ref.Id].Transactions[authorized.Transaction].Native
		}
	}
	if len(call.wire) == 0 {
		t.Fatal("reservation-fault retained request wire missing")
	}
	return call
}

func (c *redisReservationCall) assertIdentity(t *testing.T, direct *StateEngine) state.Effect {
	t.Helper()
	record, group := stateTestLoad(t, direct, c.context)
	current := group.Contexts[c.context.Id]
	if current == nil || !sameProcess(current.Snapshot.Application, c.application) || current.Snapshot.CommandSequence != 1 || !proto.Equal(current.Flow, c.command.Flow) {
		t.Fatal("reservation recovery changed logical ownership, command sequence or selected flow")
	}
	tx := current.Transactions[c.native.Key]
	if len(current.Transactions) != 1 || tx == nil || !reflect.DeepEqual(tx.Native, c.native) {
		t.Fatal("reservation recovery changed transaction identity or native deadlines")
	}
	if len(record.Payload.Effects) != len(c.effectIDs) {
		t.Fatal("reservation recovery allocated another effect")
	}
	var selected state.Effect
	for _, effect := range record.Payload.Effects {
		if !c.effectIDs[effect.ID] {
			t.Fatal("reservation recovery allocated another effect identity")
		}
		if effect.ID == c.command.Effect.Id {
			selected = effect
			var body stateEngineEffect
			if unmarshalRetainedFixture(effect.Payload, &body) != nil || effect.DeadlineMS != c.effect.DeadlineMS || !proto.Equal(body.Permit, c.command.Permit) || !proto.Equal(body.Authorization, c.command.Permit.Authorization) || body.WireHash != c.wireHash || effect.AttemptID != c.command.Effect.AttemptId {
				t.Fatal("reservation recovery changed effect, attempt, permit or original deadline")
			}
		}
	}
	if selected.ID == "" {
		t.Fatal("reservation recovery lost the original effect")
	}
	return selected
}

func awaitRedisReservationResult(t *testing.T, results <-chan *pb.DeliveryResult) *pb.DeliveryResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("reservation-fault physical result did not return within the test bound")
		return nil
	}
}

func assertRedisReservationNoBytes(t *testing.T, call *redisReservationCall) {
	t.Helper()
	if call.socket.writes.Load() != 0 {
		t.Fatal("unknown reservation entered a physical socket syscall")
	}
	_ = call.remote.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	n, err := call.remote.Read(make([]byte, 1))
	var timeout net.Error
	if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatal("unknown reservation emitted SIP or closed its otherwise healthy stream")
	}
	_ = call.remote.SetReadDeadline(time.Time{})
}

// Complete a physical, unrelated client transaction while A's Redis path is
// partitioned, rather than treating a successful Redis PING as call progress.
func assertRedisReservationOtherCallProgress(t *testing.T, engine *StateEngine) {
	t.Helper()
	call := newRedisReservationCall(t, engine, "unrelated-b")
	result := call.ingress.committedResult(context.Background(), call.command)
	if result.Outcome != pb.DeliveryResult_WRITTEN || !result.Recorded || call.socket.writes.Load() != 1 {
		t.Fatal("unrelated B OPTIONS did not complete its physical write")
	}
	_ = call.remote.SetReadDeadline(time.Now().Add(5 * time.Second))
	wire := make([]byte, len(call.wire))
	if _, err := io.ReadFull(call.remote, wire); err != nil || !bytes.Equal(wire, call.wire) {
		t.Fatal("unrelated B OPTIONS did not reach its actual peer")
	}
	request, err := sip.ParseMessage(wire)
	if err != nil {
		t.Fatal("unrelated B physical request was invalid")
	}
	options, ok := request.(*sip.Request)
	if !ok || options.Method != sip.OPTIONS {
		t.Fatal("unrelated B physical request was not OPTIONS")
	}
	response := sip.NewResponseFromRequest(options, 200, "OK", nil)
	response.To().Params.Add("tag", "unrelated-phone")
	reply := []byte(response.String())
	if _, err := call.remote.Write(reply); err != nil {
		t.Fatal("unrelated B peer could not return its response")
	}
	_ = call.socket.SetReadDeadline(time.Now().Add(5 * time.Second))
	received := make([]byte, len(reply))
	if _, err := io.ReadFull(call.socket, received); err != nil || !bytes.Equal(received, reply) {
		t.Fatal("unrelated B response did not traverse the actual stream")
	}
	native := stateTestApply(t, engine, call.context, "unrelated-response", stateEngineInput{Kind: "ingest", Application: call.application,
		Ingest: &pb.OwnerIngest{Context: call.context, Application: call.application, Flow: call.command.Flow, Wire: received, Admission: pb.OwnerIngest_EXISTING_FLOW}})
	if native.Error != "" {
		t.Fatal("unrelated B native response was refused")
	}
	_, group := stateTestLoad(t, engine, call.context)
	current := group.Contexts[call.context.Id]
	inspected, err := engine.inspectContext(context.Background(), call.application, call.context)
	if current == nil || err != nil || inspected.GetCertainty() != pb.OperationResult_COMPLETED || inspected.GetResponse().GetStatus() != 200 {
		t.Fatal("unrelated B OPTIONS did not reach its native final result")
	}
}

func TestCommittedReserveRealRedisReplyLoss(t *testing.T) {
	for _, partition := range []bool{false, true} {
		name := "reply-only"
		if partition {
			name = "reply-and-writer-partition"
		}
		t.Run(name, func(t *testing.T) {
			a, b, proxy, root := redisReservationFaultEngines(t)
			call := newRedisReservationCall(t, a, "target-a")
			operation := a.id("reserve", call.command.Effect.Id, call.command.Effect.AttemptId)
			proxy.arm(redisReservationFaultTarget{groupKey: fmt.Sprintf("%s{p%04d}:group:%s", root, call.ref.Partition, call.ref.ID),
				operation: operation, effect: call.command.Effect.Id, attempt: call.command.Effect.AttemptId, partition: partition})
			t.Cleanup(proxy.releaseReplyLoss)
			results := make(chan *pb.DeliveryResult, 1)
			go func() { results <- call.ingress.committedResult(context.Background(), call.command) }()
			var hit redisReservationCommitHit
			select {
			case hit = <-proxy.hit:
			case <-time.After(5 * time.Second):
				t.Fatal("proxy did not consume the exact successful reservation Commit response")
			}
			// A's actual Redis mutation is verified independently before its
			// successful acknowledgement is dropped at the proxy barrier.
			record, _ := stateTestLoad(t, b, call.context)
			if record.Pending != nil || record.Revision != hit.receipt.Revision || record.Proof == nil || record.Proof.OperationID != operation ||
				record.Proof.Revision != hit.receipt.Revision || record.Proof.InputDigest != hit.receipt.InputDigest || record.Proof.Fingerprint != hit.receipt.Fingerprint {
				t.Fatal("direct Redis client could not prove the exact reservation commit")
			}
			selected := call.assertIdentity(t, b)
			var body stateEngineEffect
			if selected.Status != state.EffectDispatching || unmarshalRetainedFixture(selected.Payload, &body) != nil || len(body.Wire) != 0 || call.socket.writes.Load() != 0 {
				t.Fatal("server reservation did not precede all physical socket activity")
			}
			proxy.releaseReplyLoss()
			first := awaitRedisReservationResult(t, results)
			var reserveErr error
			select {
			case reserveErr = <-call.writer.results:
			case <-time.After(5 * time.Second):
				t.Fatal("real native reservation did not return")
			}
			if partition {
				if !errors.Is(reserveErr, state.ErrUnknown) || first.Outcome != pb.DeliveryResult_NOT_WRITTEN || first.Recorded {
					t.Fatal("partitioned reservation lost local no-start evidence or claimed authoritative certainty")
				}
				assertRedisReservationNoBytes(t, call)
				if selected := call.assertIdentity(t, b); selected.Status != state.EffectDispatching {
					t.Fatal("unrecorded local no-start was treated as authoritative native completion")
				}
				memo := call.command.Effect.Id + "/" + call.command.Effect.AttemptId
				call.ingress.mu.Lock()
				attempt := call.ingress.stateAttempts[memo]
				retained := attempt != nil && attempt.noStart && attempt.result != nil && attempt.result.Outcome == pb.DeliveryResult_NOT_WRITTEN
				call.ingress.mu.Unlock()
				if !retained {
					t.Fatal("physical writer did not retain its exclusive positive local no-start memo")
				}
				duplicate := call.ingress.committedResult(context.Background(), call.command)
				if duplicate.Outcome != pb.DeliveryResult_NOT_WRITTEN || duplicate.Recorded || call.writer.reserves.Load() != 1 {
					t.Fatal("delivery replay repeated an unknown reservation or fabricated recorded certainty")
				}
				assertRedisReservationOtherCallProgress(t, b)
				assertRedisReservationNoBytes(t, call)
				proxy.heal()
				reconciled := call.ingress.inspectCommitted(context.Background(), call.command)
				if reconciled.Outcome != pb.DeliveryResult_NOT_WRITTEN || !reconciled.Recorded || call.writer.reserves.Load() != 1 {
					t.Fatal("same-attempt no-start did not become authoritative after connectivity returned")
				}
				if selected := call.assertIdentity(t, b); selected.Status != state.EffectNotWritten {
					t.Fatal("exact old attempt was not recorded as NOT_WRITTEN")
				}
				call.ingress.mu.Lock()
				memoRetained := call.ingress.stateAttempts[memo] != nil
				call.ingress.mu.Unlock()
				if memoRetained {
					t.Fatal("recorded no-start retained its physical evidence slot")
				}
				historical, err := b.InspectWrite(context.Background(), call.command)
				if err != nil || !historical.Recorded || historical.Outcome != pb.DeliveryResult_NOT_WRITTEN {
					t.Fatal("another executor could not inspect the exact recorded no-start")
				}
				known, err := a.Reserve(context.Background(), call.command)
				if err != nil || !known.Known || known.Outcome != pb.DeliveryResult_NOT_WRITTEN || len(known.Wire) != 0 {
					t.Fatal("reconciled attempt returned fresh socket permission")
				}
				again := call.ingress.committedResult(context.Background(), call.command)
				if again.Outcome != pb.DeliveryResult_NOT_WRITTEN || !again.Recorded {
					t.Fatal("exact known no-start replay did not recover its authoritative recorded outcome")
				}
				call.ingress.mu.Lock()
				memoRetained = call.ingress.stateAttempts[memo] != nil
				call.ingress.mu.Unlock()
				if memoRetained {
					t.Fatal("exact known no-start replay leaked another physical evidence memo")
				}
				assertRedisReservationNoBytes(t, call)
			} else {
				if reserveErr != nil || first.Outcome != pb.DeliveryResult_WRITTEN || !first.Recorded || call.socket.writes.Load() != 1 {
					t.Fatal("lost reply did not recover the same receipt and one physical write")
				}
				_ = call.remote.SetReadDeadline(time.Now().Add(5 * time.Second))
				wire := make([]byte, len(call.wire))
				if _, err := io.ReadFull(call.remote, wire); err != nil || !bytes.Equal(wire, call.wire) {
					t.Fatal("receipt recovery changed the selected SIP bytes")
				}
				if selected := call.assertIdentity(t, b); selected.Status != state.EffectWritten {
					t.Fatal("single recovered socket write was not recorded")
				}
				again := call.ingress.committedResult(context.Background(), call.command)
				if again.Outcome != pb.DeliveryResult_WRITTEN || !again.Recorded || call.socket.writes.Load() != 1 {
					t.Fatal("delivery replay physically restarted the recorded attempt")
				}
			}
			commits, proxyErr := proxy.status()
			if commits != 1 || proxyErr != nil {
				t.Fatal("proxy did not observe exactly one successful matching reservation Commit")
			}
		})
	}
}
