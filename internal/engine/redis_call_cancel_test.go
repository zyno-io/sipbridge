package engine

import (
	"bytes"
	"context"
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
	"sipbridge/internal/selectedmessage"
)

func TestRedisServerCancelSelectsExactPairWithoutMutation(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:cancel-" + uuid.NewString() + ":"
	t.Cleanup(func() {
		defer client.Close()
		for _, name := range []string{"profile", "capacity", "selectors", "context:ctx", "deadlines:ctx"} {
			if err := client.Del(context.Background(), root+"core:"+name).Err(); err != nil {
				t.Error(err)
			}
		}
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, found := strings.CutPrefix(line, "run_id:"); found {
			primary = value
		}
	}
	profile := []byte{2, 3, 4}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	oldProcess := &pb.ProcessRef{InstanceId: "original", Epoch: "old"}
	local := &pb.ProcessRef{InstanceId: "replacement", Epoch: "epoch"}
	oldFlow := &pb.FlowRef{Ingress: oldProcess, ConnectionId: "old", SourceIp: "192.0.2.3", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	flow := &pb.FlowRef{Ingress: local, ConnectionId: "new", SourceIp: "192.0.2.3", SourcePort: 6789, Listener: "sip.example.test:5061", Transport: "tls"}
	invite, err := portable.ParseMessage(stateTestRequest("INVITE", 1, "", "cancel-original").Wire, "tls", "192.0.2.3:5061", "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := invite.Facts()
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err := nativeServerStart(invite, "local", time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	procedureField, originField := "r:procedure-"+requestID, "r:origin-"+requestID
	ref := &pb.ContextRef{Id: "ctx", Owner: oldProcess, State: &pb.StateRef{Authority: &pb.StateAuthority{Service: "test", Generation: "one", PrimaryId: "primary", SchemaVersion: 1}, GroupId: "ctx", Incarnation: "inc", CallId: facts.CallID}}
	current := &nativepb.Context{Context: ref, Kind: pb.ContextSnapshot_UAS, CallId: facts.CallID, Transport: "tls", Flow: oldFlow,
		Application: &pb.ProcessRef{InstanceId: "absent-controller", Epoch: "epoch"}, InitialProcedures: []string{procedureField}}
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	procedure, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField}, &nativepb.Procedure{RequestId: requestID, Direct: true, DecisionPending: true, Flow: oldFlow})
	if err != nil {
		t.Fatal(err)
	}
	origin, err := selectedmessage.Marshal(invite)
	if err != nil {
		t.Fatal(err)
	}
	id := rediscore.Identity{Context: "ctx", Incarnation: "inc"}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 32768,
		Changes:   []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: procedureField, Value: procedure}, {Field: originField, Value: origin}},
		Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorServerTransaction, tx.Key), Record: procedureField}}})
	if err != nil {
		t.Fatal(err)
	}
	cancel, err := portable.BuildCancel(invite)
	if err != nil {
		t.Fatal(err)
	}
	cancel = portable.RestoreMessage(cancel.WireBytes(), "tls", "192.0.2.3:6789", "")
	selected, handled, err := selectRedisServerCancel(ctx, store, local, flow, cancel, bytes.Repeat([]byte{7}, 32), time.Now().Add(time.Second))
	if err != nil || !handled || selected == nil {
		t.Fatal("replacement-flow CANCEL selection failed", handled, err)
	}
	if selected.nextInvite.Phase != portable.Completed || selected.nextCancel.Family != portable.NonInviteServer {
		t.Fatal("CANCEL pair not advanced")
	}
	statuses := make(map[int]bool)
	for _, effect := range selected.effects {
		if effect.Kind == portable.Send && effect.Message != nil {
			response, e := effect.Message.Facts()
			if e != nil {
				t.Fatal(e)
			}
			statuses[response.Status] = true
		}
	}
	if !statuses[200] || !statuses[487] || !proto.Equal(selected.flow, flow) || selected.selected.Identity.Owner != "" {
		t.Fatal("paired CANCEL response or unbound authority changed")
	}
	after, err := store.Read(ctx, id, procedureField, "r:context", selected.cancelField)
	if err != nil || after.Revision != 1 || !bytes.Equal(after.Records[procedureField], procedure) || !bytes.Equal(after.Records["r:context"], contextData) || len(after.Records[selected.cancelField]) != 0 {
		t.Fatal("CANCEL selection published state", err)
	}
	wrongFlow := proto.Clone(flow).(*pb.FlowRef)
	wrongFlow.SourcePort++
	if _, _, err = selectRedisServerCancel(ctx, store, local, wrongFlow, cancel, bytes.Repeat([]byte{7}, 32), time.Now().Add(time.Second)); err == nil {
		t.Fatal("CANCEL accepted a different observed flow")
	}
	// A retained exact CANCEL on the same observed socket must reuse the
	// already prepared incoming holder, rather than prepare the stored copy.
	priorContext := proto.Clone(current).(*nativepb.Context)
	priorContext.NoninitialProcedures = []string{selected.cancelField}
	priorContextData, err := encodeNativeContext(priorContext)
	if err != nil {
		t.Fatal(err)
	}
	priorData, err := encodeNativeProcedure(selected.nextCancel, nativeProcedureRefs{Origin: selected.cancelOrigin, LastResponse: selected.cancelResponse}, &nativepb.Procedure{RequestId: uuid.NewString(), Flow: flow})
	if err != nil {
		t.Fatal(err)
	}
	priorOrigin, err := selectedmessage.Marshal(selected.nextCancel.Origin)
	if err != nil {
		t.Fatal(err)
	}
	priorResponse, err := selectedmessage.Marshal(*selected.nextCancel.LastResponse)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: id, Revision: after.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true,
		Changes: []rediscore.Change{{Field: "r:context", Value: priorContextData}, {Field: selected.cancelField, Value: priorData}, {Field: selected.cancelOrigin, Value: priorOrigin}, {Field: selected.cancelResponse, Value: priorResponse}}})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, handled, err := selectRedisServerCancel(ctx, store, local, flow, cancel, bytes.Repeat([]byte{7}, 32), time.Now().Add(time.Second))
	if err != nil || !handled || duplicate == nil || duplicate.prior == nil || duplicate.nextCancel.Origin != cancel {
		t.Fatal("exact prior CANCEL prepared a second holder", handled, err)
	}
	final, err := store.Read(ctx, id, selected.cancelField)
	if err != nil || final.Revision != after.Revision+1 || !bytes.Equal(final.Records[selected.cancelField], priorData) {
		t.Fatal("duplicate CANCEL selection mutated state", err)
	}
}
