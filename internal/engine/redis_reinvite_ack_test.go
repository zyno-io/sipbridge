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
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func assertRedisPeerACKRetention(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, identity rediscore.Identity, procedureField, dialogField string, ack portable.Message, outcome string) {
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
	expectedCurrent := peerACKAnswer
	expectedEvent := peerACKAnswer
	if strings.HasPrefix(outcome, "fork-") {
		expectedCurrent = []byte("selected remote SDP")
		if outcome == "fork-early" {
			expectedEvent = []byte("v=0\r\ns=hold\r\n")
		}
	}
	if err != nil || !current.InitialAck || !bytes.Equal(current.RemoteSdp, expectedCurrent) {
		t.Fatal("late-offer ACK snapshot", err)
	}
	eventView, err := store.Read(ctx, identity, field)
	if err != nil {
		t.Fatal(err)
	}
	event, err := decodeNativeLifecycle(eventView.Records[field])
	if err != nil || event.Kind != pb.DialogEvent_ACKNOWLEDGED || event.Root != procedureField || event.Message == dialog.RemoteInvite.Ack {
		t.Fatal("ACK event wire lacks independent lifetime", err)
	}
	if event.DeadlineMs-event.NextMs != (2 * time.Minute).Milliseconds() {
		t.Fatal("ACK debt changed its original bounded cutoff")
	}
	if strings.HasPrefix(outcome, "fork-") && (!bytes.Equal(current.LocalSdp, []byte("selected local SDP")) || !bytes.Equal(event.Snapshot.LocalSdp, []byte("v=0\r\ns=answer\r\n")) || event.Snapshot.RemoteTag == current.RemoteTag) {
		t.Fatal("fork ACK borrowed the selected leg's identity or local SDP")
	}
	// The accepted native duplicate must not allocate a second event or change
	// either revision, even while this first event is still undelivered.
	flow := owner.Flow
	selectedACK, err := selectRedisCallDialog(ctx, store, flow.Ingress, flow, ack, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := publishRedisCallDialogACK(ctx, store, selectedACK, flow, bytes.Repeat([]byte{7}, 32), view.Timer, time.Now().Add(time.Second)); err != nil {
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
	if err != nil || public == nil || public.InviteCseq != 14 || public.Message == nil || !bytes.Equal(public.Message.Wire, ack.WireBytes()) || !bytes.Equal(public.Snapshot.RemoteSdp, expectedEvent) {
		t.Fatal("frozen ACK evidence replaced by current state", err)
	}
	if outcome == "no-capability" || outcome == "changed-term" || outcome == "without-local-session" {
		assertRedisPeerACKDeliverySession(t, ctx, store, identity, current, event, field, outcome)
		assertRedisPeerACKSettled(t, ctx, store, client, root, identity, field, event.Message, procedureField, dialog.RemoteInvite.Ack)
		return
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
	if err := completeRedisNativeLifecycle(ctx, store, selected, outcome != "expired", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	assertRedisPeerACKSettled(t, ctx, store, client, root, identity, field, event.Message, procedureField, dialog.RemoteInvite.Ack)
}

func assertRedisPeerACKSettled(t *testing.T, ctx context.Context, store *rediscore.Store, client *redis.Client, root string, identity rediscore.Identity, field, messageField, procedureField, nativeACK string) {
	t.Helper()
	closed, err := store.Read(ctx, identity, field, messageField, procedureField, nativeACK)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := readPrivateProcedureRecord(closed.Records[procedureField])
	if err != nil || len(owner.Events) != 0 || len(closed.Records[field]) != 0 || len(closed.Records[messageField]) != 0 || len(closed.Records[nativeACK]) == 0 {
		t.Fatal("ACK settlement lost native wire or retained lifecycle debt", err)
	}
	_, err = client.ZScore(ctx, root+"core:deadlines:"+identity.Context, strings.TrimPrefix(field, "r:")).Result()
	if err != redis.Nil {
		t.Fatal("settled ACK retained a retry deadline", err)
	}
}

func assertRedisPeerACKDeliverySession(t *testing.T, ctx context.Context, store *rediscore.Store, identity rediscore.Identity, current *nativepb.Context, event *nativepb.Lifecycle, field, outcome string) {
	t.Helper()
	transport, peer, ws, sessionCtx, session, _, _ := residentControlFixture(t, true, outcome != "no-capability")
	// Model an event captured under the original term, before a different
	// negotiated session became the executor's delivery target.
	term := peer.ApplicationLease().GetTerm()
	if outcome == "changed-term" {
		term = "previous-controller-term"
	}
	current.Application, current.ApplicationLeaseTerm = peer.Application(), term
	event.Snapshot.Application, event.Snapshot.ApplicationLeaseTerm = peer.Application(), term
	contextData, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	eventData, err := encodeNativeLifecycle(event)
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.Read(ctx, identity, field)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := store.Commit(ctx, rediscore.Publication{Identity: identity, Revision: view.Revision, Deadline: time.Now().Add(time.Second), SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: field, Value: eventData}}})
	if err != nil {
		t.Fatal(err)
	}
	due := &rediscore.DueContext{Identity: identity, Revision: revision, Assignment: view.Timer, Member: strings.TrimPrefix(field, "r:"), DeadlineMS: event.NextMs, CheckedAtMS: max(time.Now().UnixMilli(), event.NextMs), Observed: time.Now()}
	executor := &redisNative{control: transport.control, eventWork: make(chan struct{}, 1), events: make(map[redisNativeEventKey]struct{})}
	if err := executor.dispatchLifecycle(ctx, store, due); err != nil {
		t.Fatal(err)
	}
	if outcome == "without-local-session" {
		frame := peerACKRead(t, sessionCtx, ws, session)
		public := frame.GetDialogEvent()
		if public == nil || public.Kind != pb.DialogEvent_ACKNOWLEDGED || public.InviteCseq != 14 || public.Message == nil || !bytes.Equal(public.Message.Body, peerACKAnswer) {
			t.Fatal("receiving worker failed to create ACK for another worker's session", frame)
		}
		writeControllerEnvelope(t, sessionCtx, ws, &pb.Envelope{SessionId: session, CorrelationId: frame.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}})
	}
	done := make(chan struct{})
	go func() { executor.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-sessionCtx.Done():
		t.Fatal("capability/term mismatch retried instead of settling")
	}
	// A ping barrier detects any event sent to a mismatched session or any
	// additional delivery after settlement.
	writeControllerEnvelope(t, sessionCtx, ws, &pb.Envelope{SessionId: session, CorrelationId: "drop-barrier", Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: "drop-barrier"}}})
	if frame := peerACKRead(t, sessionCtx, ws, session); frame.GetPong().GetNonce() != "drop-barrier" {
		t.Fatal("mismatched session received ACK", frame)
	}
}
