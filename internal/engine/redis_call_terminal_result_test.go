package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

func TestRedisTerminalRetainsSharedOriginalResultMarker(t *testing.T) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	root := "sipstate:test:terminal-marker-" + uuid.NewString() + ":"
	t.Cleanup(func() {
		for _, name := range []string{"profile", "capacity", "selectors", "context:call", "deadlines:call", "worker:worker", "timers:worker", "ready:worker", "owner-expiry", "coordinator"} {
			if e := client.Del(ctx, root+"core:"+name).Err(); e != nil {
				t.Error(e)
			}
		}
		_ = client.Close()
	})
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	primary := ""
	for _, line := range strings.Split(info, "\r\n") {
		if value, ok := strings.CutPrefix(line, "run_id:"); ok {
			primary = value
		}
	}
	profile := []byte{1, 9}
	if err = client.Set(ctx, root+"core:profile", profile, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := rediscore.New(client, rediscore.Config{Root: root, Profile: profile, PrimaryRunID: primary, RecordBudget: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.RenewWorker(ctx, rediscore.WorkerLeaseRequest{Worker: "worker", Epoch: "epoch", PrimaryRunID: primary, Acquire: true}); err != nil {
		t.Fatal(err)
	}
	assignment := rediscore.TimerAssignment{Worker: "worker", Epoch: "epoch", Term: "epoch"}
	identity := rediscore.Identity{Context: "call", Incarnation: "inc"}
	now, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	cutoff := now.Add(-time.Second).UnixMilli()
	current := &nativepb.Context{Context: &pb.ContextRef{Id: "call", State: &pb.StateRef{Incarnation: "inc"}}, Kind: pb.ContextSnapshot_UAC, CallId: "terminal-marker@test", Ended: true, CallRetireAtMs: cutoff, CommandSequence: 2, InitialProcedures: []string{"r:anchor-a", "r:anchor-b"}, Dialogs: []string{"r:dialog"}}
	currentBytes, err := encodeNativeContext(current)
	if err != nil {
		t.Fatal(err)
	}
	changes := []rediscore.Change{{Field: "r:context", Value: currentBytes}}
	var selectors []rediscore.SelectorChange
	var firstInvite portable.Message
	var firstKey string
	var childBytes []byte
	for i, field := range current.InitialProcedures {
		branch := "z9hG4bK-terminal-marker-" + strconv.Itoa(i)
		message, e := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:service@example.test>", CallID: current.CallId, LocalTag: "local", Branch: branch, CSeq: 1, Advertise: "192.0.2.1:5061", Contact: "<sip:service@192.0.2.1:5061;transport=tls>", Transport: "tls"})
		if e != nil {
			t.Fatal(e)
		}
		tx, e := portable.NewTransaction(portable.InviteClient, message, portable.DefaultTimers(), "")
		if e != nil {
			t.Fatal(e)
		}
		tx, _, e = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
		if e != nil {
			t.Fatal(e)
		}
		tx, _, e = portable.StepTransaction(tx, portable.Input{Kind: portable.Terminate, Now: now.UnixMilli()})
		if e != nil {
			t.Fatal(e)
		}
		wireField := "r:wire-a"
		metadata := &nativepb.Procedure{CommandResult: "r:result-1"}
		if i == 1 {
			wireField = "r:wire-b"
			metadata.Root = "r:anchor-a"
		} else {
			metadata.CreditContribution = 1 << 16
			firstInvite, firstKey = message, tx.Key
		}
		value, e := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: wireField}, metadata)
		if e != nil {
			t.Fatal(e)
		}
		if i == 1 {
			childBytes = bytes.Clone(value)
		}
		wire, e := selectedmessage.Marshal(message)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: field, Value: value}, rediscore.Change{Field: wireField, Value: wire})
		selectors = append(selectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorClientTransaction, tx.Key), Record: field})
	}
	final, err := portable.BuildResponseWithHeaders(firstInvite, 200, "OK", nil, "remote", "sip:phone@192.0.2.2:5061;transport=tls", nil)
	if err != nil {
		t.Fatal(err)
	}
	dialog, err := portable.NewDialog(portable.UAC, firstInvite, final, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: now.UnixMilli(), Branch: "z9hG4bK-marker-ack"})
	if err != nil {
		t.Fatal(err)
	}
	dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogTerminate, Now: now.UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	dialogBytes, err := encodeNativeDialog(dialog, nativeDialogRefs{Local: &nativeInviteRefs{Request: "r:wire-a", Final: "r:final", ACK: "r:ack"}}, &nativepb.Dialog{Transaction: firstKey})
	if err != nil {
		t.Fatal(err)
	}
	changes = append(changes, rediscore.Change{Field: "r:dialog", Value: dialogBytes})
	for field, message := range map[string]portable.Message{"r:final": *dialog.LocalInvite.Final, "r:ack": *dialog.LocalInvite.ACK} {
		data, e := selectedmessage.Marshal(message)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: field, Value: data})
	}
	a, b := dialog.LocalTag, dialog.RemoteTag
	if a > b {
		a, b = b, a
	}
	selectors = append(selectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorDialog, dialog.CallID, a, b), Record: "r:dialog"})
	original := &nativepb.Result{Certainty: pb.OperationResult_COMPLETED, Sequence: 1, OperationId: "original", Fingerprint: bytes.Repeat([]byte{1}, 32), DecidingLeaseTerm: "term", CutoffMs: now.Add(time.Minute).UnixMilli(), Request: "r:wire-a"}
	currentResult := &nativepb.Result{Certainty: pb.OperationResult_COMPLETED, Sequence: 2, OperationId: "current", Fingerprint: bytes.Repeat([]byte{2}, 32), DecidingLeaseTerm: "term", CutoffMs: cutoff, Request: "r:wire-a"}
	for _, item := range []struct {
		field  string
		result *nativepb.Result
	}{{"r:result-1", original}, {"r:result-2", currentResult}} {
		value, e := encodeNativeResult(item.result)
		if e != nil {
			t.Fatal(e)
		}
		changes = append(changes, rediscore.Change{Field: item.field, Value: value})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: identity, Create: true, Deadline: time.Now().Add(time.Second), CreditDelta: 1 << 16, Changes: changes, Selectors: selectors, Timers: &rediscore.TimerMutation{Target: assignment, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(cutoff)}}}})
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := store.ObserveContext(ctx, "call", "inc", "r:anchor-b")
	if err != nil {
		t.Fatal(err)
	}
	invalidChild, err := readPrivateProcedureRecord(invalid.View.Records["r:anchor-b"])
	if err != nil {
		t.Fatal(err)
	}
	invalidChild.Root = "r:missing-root"
	invalidBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, invalidChild)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: invalid.Identity, Revision: invalid.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:anchor-b", Value: invalidBytes}}})
	if err != nil {
		t.Fatal(err)
	}
	invalidDue, err := store.PeekDue(ctx, "worker", "epoch")
	if err != nil || invalidDue == nil {
		t.Fatal("invalid root duty missing", err)
	}
	if finished, e := retireRedisQuiescentCall(ctx, store, invalidDue, time.Now().Add(time.Second)); e == nil || finished {
		t.Fatal("missing digest root accepted", e)
	}
	retainedInvalid, err := store.ObserveContext(ctx, "call", "inc", "r:dialog", "r:anchor-b")
	if err != nil || !bytes.Equal(retainedInvalid.View.Records["r:dialog"], dialogBytes) || !bytes.Equal(retainedInvalid.View.Records["r:anchor-b"], invalidBytes) {
		t.Fatal("invalid root changed dialog or child", err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: retainedInvalid.Identity, Revision: retainedInvalid.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:anchor-b", Value: childBytes}}})
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.PeekDue(ctx, "worker", "epoch")
	if err != nil || due == nil {
		t.Fatal("terminal duty missing", err)
	}
	owners, err := selectRedisTerminalOwners(ctx, store, due, current, time.Now().Add(time.Second))
	if err != nil || owners == nil || len(owners.procedures) != 2 || len(owners.dialogs) != 1 || len(owners.results) != 2 || owners.output != "" {
		t.Fatal("terminal actual-owner selection", err)
	}
	stale := *due
	stale.Revision++
	if _, err = selectRedisTerminalOwners(ctx, store, &stale, current, time.Now().Add(time.Second)); !errors.Is(err, rediscore.ErrConflict) {
		t.Fatal("stale terminal owner selection accepted", err)
	}
	if finished, e := retireRedisQuiescentCall(ctx, store, due, time.Now().Add(time.Second)); e != nil || finished {
		t.Fatal("original result cutoff not retained", e)
	}
	score, err := client.ZScore(ctx, root+"core:deadlines:call", "call-retire").Result()
	if err != nil || int64(score) != original.CutoffMs {
		t.Fatal("original cutoff not indexed", score, err)
	}
	view, err := store.ObserveContext(ctx, "call", "inc", "r:result-1")
	if err != nil {
		t.Fatal(err)
	}
	original.CutoffMs = cutoff
	markerBytes, err := encodeNativeResult(original)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: view.Identity, Revision: view.View.Revision, Deadline: time.Now().Add(time.Second), Changes: []rediscore.Change{{Field: "r:result-1", Value: markerBytes}}, Timers: &rediscore.TimerMutation{Expected: view.View.Timer, Target: view.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "call-retire", Due: time.UnixMilli(cutoff)}}}})
	if err != nil {
		t.Fatal(err)
	}
	for stage := 0; stage < 4; stage++ {
		due, err = store.PeekDue(ctx, "worker", "epoch")
		if err != nil || due == nil {
			t.Fatal("terminal duty missing", stage, err)
		}
		finished, e := retireRedisQuiescentCall(ctx, store, due, time.Now().Add(time.Second))
		if e != nil || finished != (stage == 3) {
			t.Fatal("terminal marker stage", stage, finished, e)
		}
		if stage < 3 {
			selected, e := store.ObserveContext(ctx, "call", "inc", "r:anchor-a", "r:anchor-b", "r:result-1", "r:result-2", "r:wire-a")
			if e != nil {
				t.Fatal(e)
			}
			if stage == 1 && (len(selected.View.Records["r:anchor-b"]) != 0 || len(selected.View.Records["r:anchor-a"]) == 0 || len(selected.View.Records["r:result-1"]) == 0) {
				t.Fatal("digest child/root marker ordering broken")
			}
			if stage == 2 && (len(selected.View.Records["r:anchor-a"]) != 0 || len(selected.View.Records["r:result-1"]) != 0 || len(selected.View.Records["r:result-2"]) == 0) {
				t.Fatal("last original marker not retired exactly")
			}
			if len(selected.View.Records["r:wire-a"]) == 0 {
				t.Fatal("current result lost shared wire")
			}
		}
	}
	if _, err = store.ObserveContext(ctx, "call", "inc"); !errors.Is(err, rediscore.ErrAbsent) {
		t.Fatal("terminal context retained", err)
	}
}
