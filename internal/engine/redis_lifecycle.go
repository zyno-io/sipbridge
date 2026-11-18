package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// selectedNativeLifecycle is operation-local. A caller serializes the
// physical controller call for this event and rechecks its due observation.
type selectedNativeLifecycle struct {
	due    rediscore.DueContext
	field  string
	record *nativepb.Lifecycle
	data   []byte
}

func selectRedisNativeLifecycle(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext) (selectedNativeLifecycle, *pb.DialogEvent, error) {
	var out selectedNativeLifecycle
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "event-") || due.CheckedAtMS < due.DeadlineMS {
		return out, nil, errNativeRecord
	}
	field := "r:" + due.Member
	if !nativeMessageField(field) {
		return out, nil, errNativeRecord
	}
	view, err := store.Read(ctx, due.Identity, field)
	if err != nil {
		return out, nil, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return out, nil, rediscore.ErrConflict
	}
	record, err := decodeNativeLifecycle(view.Records[field])
	if err != nil {
		return out, nil, err
	}
	allowed := false
	messageEvent := record.Message != ""
	switch record.Snapshot.Kind {
	case pb.ContextSnapshot_UAS:
		allowed = messageEvent && record.Kind == pb.DialogEvent_REINVITE_ACKNOWLEDGED || !messageEvent && (record.Kind == pb.DialogEvent_ACKNOWLEDGED || record.Kind == pb.DialogEvent_CANCELLED) || record.Kind == pb.DialogEvent_ENDED
	case pb.ContextSnapshot_UAC:
		allowed = messageEvent && record.Kind == pb.DialogEvent_REINVITE_ACKNOWLEDGED || !messageEvent && (record.Kind == pb.DialogEvent_ACKNOWLEDGED || record.Kind == pb.DialogEvent_CANCELLED) || record.Kind == pb.DialogEvent_ENDED ||
			record.Kind == pb.DialogEvent_FAILED && record.Snapshot.Ended && record.Error == "" && validSendText(record.OperationId, 160, false) ||
			messageEvent && (record.Kind == pb.DialogEvent_PROVISIONAL || record.Kind == pb.DialogEvent_ANSWERED) && validSendText(record.OperationId, 160, false)
	}
	if !allowed || record.Snapshot.Context == nil || record.Snapshot.Context.State == nil || record.Snapshot.Context.Id != due.Identity.Context || record.Snapshot.Context.State.Incarnation != due.Identity.Incarnation || record.NextMs != due.DeadlineMS || record.DeadlineMs <= 0 || !messageEvent && len(record.Selectors) != 0 {
		return out, nil, errors.New("native lifecycle evidence changed")
	}
	if !messageEvent && record.Root != "" {
		ownerView, e := store.Read(ctx, due.Identity, "r:context", record.Root)
		if e != nil {
			return out, nil, e
		}
		if ownerView.Revision != view.Revision || ownerView.Timer != view.Timer {
			return out, nil, rediscore.ErrConflict
		}
		current, e := decodeNativeContext(ownerView.Records["r:context"])
		if e != nil || current.Context == nil || !proto.Equal(current.Context, record.Snapshot.Context) || current.Kind != record.Snapshot.Kind || !slices.Contains(current.NoninitialProcedures, record.Root) {
			return out, nil, errNativeRecord
		}
		owner, e := readPrivateProcedureRecord(ownerView.Records[record.Root])
		id := strings.TrimPrefix(record.Root, "r:procedure-")
		parsed, parseErr := uuid.Parse(id)
		if e != nil || parseErr != nil || parsed.String() != id || record.Root != "r:procedure-"+id || owner.RequestId != id || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.InviteServer) || !slices.Contains(owner.Events, field) {
			return out, nil, errNativeRecord
		}
	}
	out = selectedNativeLifecycle{due: *due, field: field, record: record, data: view.Records[field]}
	if due.CheckedAtMS >= record.DeadlineMs {
		return out, nil, nil
	}
	public := &pb.DialogEvent{Kind: record.Kind, Snapshot: proto.Clone(record.Snapshot).(*pb.ContextSnapshot), OperationId: record.OperationId, Error: record.Error}
	if record.Kind == pb.DialogEvent_REINVITE_ACKNOWLEDGED {
		if err := projectRedisReinviteACK(ctx, store, due, view.Revision, field, record, public); err != nil {
			return out, nil, err
		}
		return out, public, nil
	}
	if messageEvent {
		wireView, e := store.Read(ctx, due.Identity, "r:context", record.Root, record.Message)
		if e != nil {
			return out, nil, e
		}
		if wireView.Revision != view.Revision || wireView.Timer != view.Timer {
			return out, nil, rediscore.ErrConflict
		}
		current, e := decodeNativeContext(wireView.Records["r:context"])
		if e != nil {
			return out, nil, e
		}
		if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || !proto.Equal(current.Context, record.Snapshot.Context) || current.Kind != record.Snapshot.Kind || current.CallId != record.Snapshot.CallId || !slices.Contains(current.InitialProcedures, record.Root) {
			return out, nil, errNativeRecord
		}
		root, e := readPrivateProcedureRecord(wireView.Records[record.Root])
		if e != nil {
			return out, nil, e
		}
		if root.Native.Version != portable.Version || root.Root != "" || root.Native.Family != string(portable.InviteClient) && root.Native.Family != string(portable.InviteServer) {
			return out, nil, errNativeRecord
		}
		found := false
		for _, retained := range root.Events {
			if retained == field {
				found = true
				break
			}
		}
		if !found {
			return out, nil, errNativeRecord
		}
		memo, e := selectedmessage.NewMemo(1)
		if e != nil {
			return out, nil, e
		}
		source := nativeSelectedMessages{records: wireView.Records, memo: memo, provenance: "admitted"}
		message, e := source.get(record.Message)
		if e != nil {
			return out, nil, e
		}
		facts, e := message.Facts()
		if e != nil || facts.IsRequest || facts.CallID != record.Snapshot.CallId || facts.Status < 100 || facts.Status > 699 {
			return out, nil, errNativeRecord
		}
		valid := false
		switch record.Kind {
		case pb.DialogEvent_PROVISIONAL:
			valid = facts.CSeqMethod == sip.INVITE && facts.Status < 200
		case pb.DialogEvent_ANSWERED:
			valid = facts.CSeqMethod == sip.INVITE && facts.Status >= 200 && facts.Status < 300 && record.Snapshot.Answered
		case pb.DialogEvent_FAILED:
			valid = facts.CSeqMethod == sip.INVITE && facts.Status >= 300
		case pb.DialogEvent_ENDED:
			valid = facts.CSeqMethod == sip.BYE && facts.Status >= 200 && record.Snapshot.Ended
		}
		if !valid {
			return out, nil, errNativeRecord
		}
		public.Message, e = projectNativeMessage(message, false)
		if e != nil {
			return out, nil, e
		}
		public.Message.Selectors = append([]string(nil), record.Selectors...)
	}
	return out, public, nil
}

// prepareRedisNativeLifecycleAttempt publishes the next real retry
// before controller I/O. Only a definite successful publication permits Call.
func prepareRedisNativeLifecycleAttempt(ctx context.Context, store *rediscore.Store, selected selectedNativeLifecycle, callDeadline, deadline time.Time) (selectedNativeLifecycle, error) {
	remaining, ok := lifecycleRemaining(selected)
	if store == nil || !ok || callDeadline == callDeadline.Round(0) || !time.Now().Before(callDeadline) || !time.Now().Before(deadline) {
		return selectedNativeLifecycle{}, errNativeRecord
	}
	callOffset := callDeadline.Sub(selected.due.Observed)
	if callOffset < 0 || callOffset > time.Duration(remaining)*time.Millisecond {
		return selectedNativeLifecycle{}, errNativeRecord
	}
	next := proto.Clone(selected.record).(*nativepb.Lifecycle)
	if next.BackoffMs == 0 {
		next.BackoffMs = 1000
	} else if next.BackoffMs > 0 && next.BackoffMs <= 10000 {
		next.BackoffMs = min(10000, next.BackoffMs*2)
	} else {
		return selectedNativeLifecycle{}, errNativeRecord
	}
	next.NextMs = selected.due.CheckedAtMS + min(remaining, callOffset.Milliseconds()+next.BackoffMs)
	encoded, err := encodeNativeLifecycle(next)
	if err != nil {
		return selectedNativeLifecycle{}, err
	}
	revision, err := store.Commit(ctx, rediscore.Publication{Identity: selected.due.Identity, Revision: selected.due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: selected.field, Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: selected.due.Assignment, Target: selected.due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: selected.due.Member, Due: time.UnixMilli(next.NextMs)}}}})
	if err != nil {
		return selectedNativeLifecycle{}, err
	}
	selected.due.Revision, selected.record, selected.data = revision, next, encoded
	return selected, nil
}

// deferRedisNativeLifecycle advances only NextMs when no worker slot is
// available. It never changes the event's exponential retry backoff.
func deferRedisNativeLifecycle(ctx context.Context, store *rediscore.Store, selected selectedNativeLifecycle, deadline time.Time) error {
	remaining, ok := lifecycleRemaining(selected)
	if store == nil || !ok || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	elapsed := time.Since(selected.due.Observed)
	if elapsed < 0 {
		return errNativeRecord
	}
	next := proto.Clone(selected.record).(*nativepb.Lifecycle)
	next.NextMs = selected.due.CheckedAtMS + min(remaining, min(remaining, elapsed.Milliseconds())+1000)
	encoded, err := encodeNativeLifecycle(next)
	if err != nil {
		return err
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.due.Identity, Revision: selected.due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: selected.field, Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: selected.due.Assignment, Target: selected.due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: selected.due.Member, Due: time.UnixMilli(next.NextMs)}}}})
	return err
}

// lifecycleRemaining bounds conversion to a monotonic duration. CheckedAtMS
// and Observed describe the same Redis time observation, without a wall-clock
// conversion after that observation.
func lifecycleRemaining(selected selectedNativeLifecycle) (int64, bool) {
	observed := selected.due.Observed
	if selected.record == nil || observed == observed.Round(0) || selected.due.CheckedAtMS <= 0 || selected.record.DeadlineMs > 1<<53-1 || selected.due.CheckedAtMS >= selected.record.DeadlineMs {
		return 0, false
	}
	remaining := selected.record.DeadlineMs - selected.due.CheckedAtMS
	if remaining > int64((1<<63-1)/int64(time.Millisecond)) {
		return 0, false
	}
	return remaining, true
}

// completeRedisNativeLifecycle never repeats controller I/O. It accepts
// unrelated context revisions only while the exact staged event and executor
// assignment remain unchanged; expiry uses the original Redis due observation.
func completeRedisNativeLifecycle(ctx context.Context, store *rediscore.Store, selected selectedNativeLifecycle, accepted bool, deadline time.Time) error {
	if store == nil || selected.record == nil || selected.field != "r:"+selected.due.Member || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	if !accepted && selected.due.CheckedAtMS < selected.record.DeadlineMs {
		return nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		call := selected.record.Snapshot.Kind == pb.ContextSnapshot_UAS || selected.record.Snapshot.Kind == pb.ContextSnapshot_UAC
		fields := []string{selected.field}
		if call {
			fields = append(fields, "r:context")
		}
		latest, err := store.ObserveContext(ctx, selected.due.Identity.Context, selected.due.Identity.Incarnation, fields...)
		if err != nil {
			return err
		}
		if len(latest.View.Records[selected.field]) == 0 {
			// Event identities are never reused. A final may have superseded
			// transient provisional debt while its captured call was in flight.
			return nil
		}
		if latest.View.Timer != selected.due.Assignment {
			return rediscore.ErrConflict
		}
		currentEvent := selected.record
		if !bytes.Equal(latest.View.Records[selected.field], selected.data) {
			currentEvent, err = decodeNativeLifecycle(latest.View.Records[selected.field])
			if err != nil {
				return err
			}
			if !sameNativeLifecycleDelivery(selected.record, currentEvent) {
				return rediscore.ErrConflict
			}
		}
		due := selected.due
		due.Identity, due.Revision = latest.Identity, latest.View.Revision
		if call {
			current, e := decodeNativeContext(latest.View.Records["r:context"])
			if e != nil {
				return e
			}
			if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != selected.record.Snapshot.Kind {
				return errNativeRecord
			}
			timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
			if current.Ended {
				if current.CallRetireAtMs <= 0 || current.CallRetireAtMs > 1<<53-1 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
			}
			changes := []rediscore.Change{{Field: selected.field, Delete: true}}
			if (currentEvent.Message == "" || currentEvent.Kind == pb.DialogEvent_REINVITE_ACKNOWLEDGED) && currentEvent.Root != "" {
				ownerView, x := store.Read(ctx, due.Identity, currentEvent.Root)
				if x != nil {
					return x
				}
				if ownerView.Revision != due.Revision || ownerView.Timer != due.Assignment {
					continue
				}
				owner, x := readPrivateProcedureRecord(ownerView.Records[currentEvent.Root])
				if x != nil || !slices.Contains(current.NoninitialProcedures, currentEvent.Root) || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.InviteServer) {
					return errNativeRecord
				}
				id := strings.TrimPrefix(currentEvent.Root, "r:procedure-")
				parsed, x := uuid.Parse(id)
				index := slices.Index(owner.Events, selected.field)
				if x != nil || parsed.String() != id || currentEvent.Root != "r:procedure-"+id || owner.RequestId != id || index < 0 {
					return errNativeRecord
				}
				owner.Events = slices.Delete(owner.Events, index, index+1)
				data, x := marshalNativeRecord(storagecodec.NativeProcedure, owner)
				if x != nil {
					return x
				}
				changes = append(changes, rediscore.Change{Field: currentEvent.Root, Value: data})
				if currentEvent.Kind == pb.DialogEvent_REINVITE_ACKNOWLEDGED {
					if !validReinviteACKLifecycle(currentEvent, selected.field) {
						return errNativeRecord
					}
					changes = append(changes, rediscore.Change{Field: currentEvent.Message, Delete: true})
				}
				if owner.Native.Phase == string(portable.Terminated) && len(owner.Events) == 0 && len(owner.Outputs) == 0 {
					timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + id, Due: time.UnixMilli(due.CheckedAtMS)})
				}
			}
			if currentEvent.Message != "" && currentEvent.Kind != pb.DialogEvent_REINVITE_ACKNOWLEDGED {
				release, x := planRedisLifecycleMessageRelease(ctx, store, due.Identity, due.Revision, due.Assignment, current, selected.field, currentEvent)
				if errors.Is(x, rediscore.ErrConflict) {
					continue
				}
				if x != nil {
					return x
				}
				changes = append(changes, release...)
				for _, ownerField := range currentEvent.Owners {
					if !strings.HasPrefix(ownerField, "r:call-cleanup-") || !slices.Contains(current.NoninitialProcedures, ownerField) {
						continue
					}
					id := strings.TrimPrefix(ownerField, "r:call-cleanup-")
					if len(id) != 32 {
						return errNativeRecord
					}
					outputField := "r:call-cleanup-output-" + id
					ownerView, x := store.Read(ctx, due.Identity, ownerField, outputField)
					if x != nil {
						return x
					}
					if ownerView.Revision != due.Revision || ownerView.Timer != due.Assignment {
						return rediscore.ErrConflict
					}
					owner, x := readPrivateProcedureRecord(ownerView.Records[ownerField])
					if x != nil {
						return x
					}
					index := slices.Index(owner.Events, selected.field)
					if index < 0 {
						continue
					}
					output, x := decodeNativeOutput(ownerView.Records[outputField])
					if x != nil || !owner.Cleanup || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.NonInviteClient) || owner.LastResponse != currentEvent.Message || output.Procedure != ownerField || output.Method != "BYE" || output.Result != "" {
						return errNativeRecord
					}
					owner.Events = slices.Delete(owner.Events, index, index+1)
					data, x := marshalNativeRecord(storagecodec.NativeProcedure, owner)
					if x != nil {
						return x
					}
					changes = append(changes, rediscore.Change{Field: ownerField, Value: data})
					if len(owner.Events) == 0 && owner.Native.Phase == string(portable.Terminated) {
						timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
					}
				}
			}
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
				Changes: changes,
				Timers:  &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
			if errors.Is(e, rediscore.ErrConflict) {
				continue
			}
			return e
		}
		return errNativeRecord
	}
	return rediscore.ErrConflict
}
