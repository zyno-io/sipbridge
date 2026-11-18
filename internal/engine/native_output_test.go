package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func nativeOutputFixture(t *testing.T) (*rediscore.Store, *redis.Client, rediscore.Identity, *pb.FlowWrite, []rediscore.Change, rediscore.OutputIntent, string) {
	t.Helper()
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated qualification Redis required")
	}
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	info, err := c.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	runID := ""
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "run_id:") {
			runID = strings.TrimPrefix(line, "run_id:")
		}
	}
	root := "sipstate:test:output-" + uuid.NewString() + ":"
	profile := []byte{8, 1, 18, 2, 0, 255}
	store, err := rediscore.New(c, rediscore.Config{Root: root, Profile: profile, RecordBudget: 1 << 20, PrimaryRunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	id := rediscore.Identity{Context: "context1", Incarnation: "incarnation1"}
	process := &pb.ProcessRef{InstanceId: "worker1", Epoch: "epoch1"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "connection1", SourceIp: "192.0.2.1", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	effect := &pb.StateEffectRef{Context: &pb.ContextRef{Id: id.Context, State: &pb.StateRef{Incarnation: id.Incarnation}}, Id: "effect1", AttemptId: "attempt1"}
	auth := &pb.EffectAuthorization{Effect: effect, AuthorityClass: pb.EffectAuthorization_RESPONSE, DeadlineMs: uint64(time.Now().Add(time.Minute).UnixMilli())}
	write := &pb.FlowWrite{Flow: flow, Effect: effect, Permit: &pb.SendPermit{Flow: flow, Authorization: auth}}
	request := stateTestRequest("REGISTER", 1, "", "output-test")
	message, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := &nativepb.Output{Write: write, Procedure: "r:procedure", Drain: &nativepb.DrainObservation{Transaction: make([]byte, 32), Ack: make([]byte, 32), Request: true}}
	intent, changes, err := nativePreparePhysicalOutput(metadata, message, "r:wire", "r:metadata", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	changes = append(changes, rediscore.Change{Field: "r:procedure", Value: []byte("procedure")})
	if err = c.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err = c.HSet(ctx, root+"core:worker:"+intent.Worker, "epoch", "epoch1", "expires_ms", time.Now().Add(time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		keys := []string{root + "core:profile", root + "core:context:" + id.Context, root + "core:capacity", root + "core:worker:" + intent.Worker, root + "core:ready:" + intent.Worker}
		if e := c.Del(ctx, keys...).Err(); e != nil {
			t.Error(e)
		}
		if e := c.Close(); e != nil {
			t.Error(e)
		}
	})
	return store, c, id, write, changes, intent, root
}
func TestNativePhysicalOutputReserveComplete(t *testing.T) {
	store, _, id, write, changes, intent, _ := nativeOutputFixture(t)
	ctx := context.Background()
	if _, err := store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}}); err != nil {
		t.Fatal(err)
	}
	writes, err := nativeReadyWrites(ctx, store, intent.Worker, intent.Epoch, 2)
	if err != nil || len(writes) != 1 || !proto.Equal(writes[0], write) {
		t.Fatal("ready metadata", err)
	}
	writer := nativeRedisWriter{store: store}
	altered := proto.Clone(write).(*pb.FlowWrite)
	altered.Permit.Authorization.Fingerprint = "different"
	if _, err = writer.Reserve(ctx, altered); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("altered proof accepted", err)
	}
	result, err := writer.Reserve(ctx, write)
	if err != nil || result.Known || !bytes.Equal(result.Wire, stateTestRequest("REGISTER", 1, "", "output-test").Wire) || result.Drain == nil || !result.Drain.Request {
		t.Fatal("reserved output", result, err)
	}
	again, err := writer.Reserve(ctx, write)
	if err != nil || !again.Known || again.Outcome != pb.DeliveryResult_UNKNOWN {
		t.Fatal("started attempt repeated", again, err)
	}
	delivery := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
	if err = writer.Complete(ctx, write, delivery); err != nil {
		t.Fatal(err)
	}
	done, err := writer.Reserve(ctx, write)
	if err != nil || !done.Known || done.Outcome != pb.DeliveryResult_WRITTEN {
		t.Fatal("known result lost", done, err)
	}
}
func TestNativePhysicalDeliveryTagAndClass(t *testing.T) {
	_, _, _, write, _, _, _ := nativeOutputFixture(t)
	a, err := nativeDeliveryTag(nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := nativeDeliveryTag(&pb.ContextDelivery{})
	if err != nil || a == b {
		t.Fatal("nil delivery collapsed", err)
	}
	write.Permit.Authorization.AuthorityClass = pb.EffectAuthorization_APPLICATION
	write.Permit.Authorization.ApplicationLeaseTerm = "term1"
	request := stateTestRequest("REGISTER", 1, "", "tag-test")
	message, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	intent, changes, err := nativePreparePhysicalOutput(&nativepb.Output{Write: write}, message, "r:wire", "r:meta", &rediscore.ApplicationLeaseRef{Application: "app1", Term: "term1"}, nil)
	if err != nil || intent.Application == nil || intent.DeliveryTag != a || len(changes) != 2 {
		t.Fatal("APP output tag mismatch", err)
	}
	tag, err := nativeDeliveryTagChange(nil)
	if err != nil || !bytes.Equal(tag.Value, a[:]) {
		t.Fatal("context delivery tag mismatch", err)
	}
	before := proto.Clone(write).(*pb.FlowWrite)
	h, err := nativeFlowWriteHash(write)
	if err != nil {
		t.Fatal(err)
	}
	write.Permit.Authorization.DeadlineMs++
	after, err := nativeFlowWriteHash(write)
	if err != nil || h == after || !proto.Equal(before.Effect, write.Effect) {
		t.Fatal("proof did not bind entire write", err)
	}
}

func TestNativePhysicalDependenciesAreBoundedAndDeduplicated(t *testing.T) {
	_, _, _, write, _, _, _ := nativeOutputFixture(t)
	request := stateTestRequest("REGISTER", 1, "", "dependency-test")
	message, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	metadata := &nativepb.Output{Write: write, Procedure: "r:procedure", Result: "r:procedure", Dialog: "r:dialog"}
	intent, _, err := nativePreparePhysicalOutput(metadata, message, "r:wire", "r:metadata", nil, nil)
	if err != nil || len(intent.Dependencies) != 2 || intent.Dependencies[0] != "r:procedure" || intent.Dependencies[1] != "r:dialog" {
		t.Fatal("incorrect dependencies", intent.Dependencies, err)
	}
	metadata.Dialog = "r:wire"
	if _, _, err = nativePreparePhysicalOutput(metadata, message, "r:wire", "r:metadata", nil, nil); err == nil {
		t.Fatal("wire record accepted as mutable dependency")
	}
}

func TestNativePhysicalCorruptMetadataCannotWrite(t *testing.T) {
	store, c, id, write, changes, intent, root := nativeOutputFixture(t)
	ctx := context.Background()
	if _, err := store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Minute), Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}}); err != nil {
		t.Fatal(err)
	}
	// Corrupt only this task-owned test record, retaining its length/accounting.
	meta := bytes.Clone(changes[1].Value)
	meta[0] ^= 1
	if err := c.HSet(ctx, root+"core:context:"+id.Context, intent.MetadataField, meta).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := (nativeRedisWriter{store: store}).Reserve(ctx, write)
	if err != nil || !got.Known || got.Outcome != pb.DeliveryResult_UNKNOWN || len(got.Wire) != 0 {
		t.Fatal("corrupt metadata granted wire", got, err)
	}
}
