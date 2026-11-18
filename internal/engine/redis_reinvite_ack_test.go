package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func assertRedisPeerACKRetention(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, identity rediscore.Identity, procedureField, dialogField string, ack portable.Message, outcome string) {
	t.Helper()
	view, err := store.Read(ctx, identity, "r:context", procedureField, dialogField)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(view.Records[procedureField])
	if err != nil || len(owner.Events) != 1 {
		t.Fatal("ACK has no bounded reciprocal owner", err)
	}
	field := owner.Events[0]
	dialog, err := readPrivateDialogRecord(view.Records[dialogField])
	if err != nil {
		t.Fatal(err)
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil || !current.InitialAck || !bytes.Equal(current.RemoteSdp, peerACKAnswer) {
		t.Fatal("late-offer ACK snapshot", err)
	}
	eventView, err := store.Read(ctx, identity, field)
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(eventView.Records[field])
	if err != nil || event.Kind != pb.DialogEvent_REINVITE_ACKNOWLEDGED || event.Root != procedureField || event.Message == dialog.RemoteInvite.Ack {
		t.Fatal("ACK event wire lacks independent lifetime", err)
	}
	// The accepted native duplicate must not allocate a second event or change
	// either revision, even while this first event is still undelivered.
	flow := owner.Flow
	selectedACK, err := selectRedisCallDialog(ctx, store, flow.Ingress, flow, ack, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := publishRedisCallDialogACK(ctx, store, selectedACK, flow, bytes.Repeat([]byte{7}, 32), view.Timer, time.Now().Add(time.Second), true); err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.Read(ctx, identity, field)
	if err != nil || duplicate.Revision != view.Revision {
		t.Fatal("duplicate ACK mutated its event", err)
	}
	// Later media state cannot replace the event's frozen answer or snapshot.
	current.RemoteSdp = []byte("later media negotiation")
	current.Revision++
	data, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: duplicate.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: data}}})
	if err != nil {
		t.Fatal(err)
	}
	observe := func() *rediscore.DueContext {
		t.Helper()
		latest, err := store.Read(ctx, identity, field)
		if err != nil {
			t.Fatal(err)
		}
		now, err := client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		return &rediscore.DueContext{Identity: identity, Revision: latest.Revision, Assignment: latest.Timer, Member: strings.TrimPrefix(field, "r:"), DeadlineMS: event.NextMs, CheckedAtMS: max(now.UnixMilli(), event.NextMs), Observed: time.Now()}
	}
	due := observe()
	selected, public, err := selectRedisNativeLifecycle(ctx, store, due)
	if err != nil || public == nil || public.InviteCseq != 14 || public.Message == nil || !bytes.Equal(public.Message.Wire, ack.WireBytes()) || !bytes.Equal(public.Snapshot.RemoteSdp, peerACKAnswer) {
		t.Fatal("frozen ACK evidence replaced by current state", err)
	}
	original := proto.Clone(public).(*pb.DialogEvent)
	staged, err := prepareRedisNativeLifecycleAttempt(ctx, store, selected, time.Now().Add(time.Second), time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := completeRedisNativeLifecycle(ctx, store, staged, false, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	held, err := store.Read(ctx, identity, field, event.Message)
	if err != nil || len(held.Records[field]) == 0 || len(held.Records[event.Message]) == 0 {
		t.Fatal("unacknowledged ACK lost delivery debt", err)
	}
	if outcome == "expired" {
		now, err := client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		event.DeadlineMs, event.NextMs = now.Add(-time.Millisecond).UnixMilli(), now.Add(-time.Millisecond).UnixMilli()
		data, err := encodeNativeLifecycle(event)
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: held.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: field, Value: data}}})
		if err != nil {
			t.Fatal(err)
		}
		due = observe()
		selected, public, err = selectRedisNativeLifecycle(ctx, store, due)
		if err != nil || public != nil {
			t.Fatal("expired ACK was projected", err)
		}
	} else {
		// Settle the staged immutable delivery after an unrelated revision.
		selected = staged
		if !proto.Equal(public, original) {
			t.Fatal("staged ACK changed")
		}
	}
	if err := completeRedisNativeLifecycle(ctx, store, selected, outcome == "accepted", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	closed, err := store.Read(ctx, identity, field, event.Message, procedureField, dialog.RemoteInvite.Ack)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = readPrivateProcedureRecord(closed.Records[procedureField])
	if err != nil || len(owner.Events) != 0 || len(closed.Records[field]) != 0 || len(closed.Records[event.Message]) != 0 || len(closed.Records[dialog.RemoteInvite.Ack]) == 0 {
		t.Fatal("ACK settlement lost native wire or retained lifecycle debt", err)
	}
}
