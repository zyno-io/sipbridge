package engine

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisClientCancelTimer advances only the actual paired transaction's F/K
// semantic deadline; Timer E stays a process-local derivative.
func fireRedisClientCancelTimer(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "cancel-client-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	parts := strings.Split(strings.TrimPrefix(due.Member, prefix), "-")
	if len(parts) != 2 || len(parts[0]) != 32 {
		return errNativeRecord
	}
	timer := portable.Timer(parts[1])
	if timer != portable.TimerF && timer != portable.TimerK {
		return errNativeRecord
	}
	field := "r:cancel-client-" + parts[0]
	view, err := store.Read(ctx, due.Identity, "r:context", field)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	proc, err := decodeNativeProcedure(view.Records[field])
	if err != nil {
		return err
	}
	d := proc.Native.Timers[timer]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.NoninitialProcedures, field) || proc.Native.Version != portable.Version || proc.Native.Family != portable.NonInviteClient || !proc.Metadata.Cleanup || proc.Metadata.CommandResult != "" || proc.Metadata.CreditContribution != 0 || proc.Refs.Origin != "r:cancel-client-origin-"+parts[0] || !d.Active || d.Generation == 0 || d.At != due.DeadlineMS {
		return errNativeRecord
	}
	fields := []string{proc.Refs.Origin}
	if proc.Refs.LastResponse != "" {
		fields = append(fields, proc.Refs.LastResponse)
	}
	wires, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if wires.Revision != view.Revision || wires.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return err
	}
	tx, err := proc.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, proc.Refs.LastResponse != "", false)
	if err != nil {
		return err
	}
	if _, err = selectRedisClientCancelParent(ctx, store, due.Identity, view.Revision, view.Timer, current, field, proc.Native.Key, proc.Refs.Origin, proc.Metadata.Flow, tx.Origin, key); err != nil {
		return err
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: timer, Generation: d.Generation})
	if err != nil {
		return err
	}
	data, err := encodeNativeProcedure(next, proc.Refs, proc.Metadata)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	for _, effect := range effects {
		switch effect.Kind {
		case portable.CancelTimer:
			if effect.Timer != portable.TimerE && effect.Timer != timer {
				timers = append(timers, rediscore.TimerChange{Member: prefix + parts[0] + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerE {
				timers = append(timers, rediscore.TimerChange{Member: prefix + parts[0] + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.Ended, portable.Diagnostic:
		default:
			return errNativeRecord
		}
	}
	if next.Phase == portable.Terminated {
		if len(proc.Metadata.Outputs) == 0 {
			timers = append(timers, rediscore.TimerChange{Member: prefix + "retire-" + parts[0], Due: time.UnixMilli(due.CheckedAtMS)})
		} else if proc.Metadata.OutputConsumed {
			for _, outputField := range proc.Metadata.Outputs {
				out, e := store.Read(ctx, due.Identity, outputField)
				if e != nil {
					return e
				}
				if out.Revision != due.Revision || out.Timer != due.Assignment {
					return rediscore.ErrConflict
				}
				output, e := decodeNativeOutput(out.Records[outputField])
				if e != nil || output.Write.Permit.Authorization.DeadlineMs == 0 {
					return errNativeRecord
				}
				timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
			}
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: field, Value: data}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}

// retireRedisClientCancel removes only a terminated, fully consumed child. Its
// exact selected parent is re-driven after the atomic ownership release.
func retireRedisClientCancel(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "cancel-client-retire-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 {
		return errNativeRecord
	}
	field := "r:cancel-client-" + id
	view, err := store.Read(ctx, due.Identity, "r:context", field)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	proc, err := readPrivateProcedureRecord(view.Records[field])
	if err != nil {
		return err
	}
	if current.Context == nil || !slices.Contains(current.NoninitialProcedures, field) || proc.Native.Version != portable.Version || proc.Native.Family != string(portable.NonInviteClient) || proc.Native.Phase != string(portable.Terminated) || !proc.Cleanup || proc.CommandResult != "" || proc.CreditContribution != 0 || proc.Origin != "r:cancel-client-origin-"+id || len(proc.Outputs) != 0 || len(proc.Events) != 0 || proc.Native.Timers != nil && !nativeDeadlinesInactive(proc.Native.Timers.Values) {
		return errNativeRecord
	}
	wires := []string{proc.Origin}
	if proc.LastResponse != "" {
		wires = append(wires, proc.LastResponse)
	}
	selected, err := store.Read(ctx, due.Identity, wires...)
	if err != nil {
		return err
	}
	if selected.Revision != due.Revision || selected.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	origin, err := selectedmessage.DecodeOpaque(selected.Records[proc.Origin])
	if err != nil {
		return err
	}
	parent, err := selectRedisClientCancelParent(ctx, store, due.Identity, due.Revision, due.Assignment, current, field, proc.Native.Key, proc.Origin, proc.Flow, origin, key)
	if err != nil {
		return err
	}
	next := proto.Clone(current).(*nativepb.Context)
	index, ok := slices.BinarySearch(next.NoninitialProcedures, field)
	if !ok {
		return errNativeRecord
	}
	next.NoninitialProcedures = slices.Delete(next.NoninitialProcedures, index, index+1)
	data, err := encodeNativeContext(next)
	if err != nil {
		return err
	}
	changes := []rediscore.Change{{Field: "r:context", Value: data}, {Field: field, Delete: true}, {Field: proc.Origin, Delete: true}}
	if proc.LastResponse != "" {
		changes = append(changes, rediscore.Change{Field: proc.LastResponse, Delete: true})
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if strings.HasPrefix(parent.Record, "r:app-invite-") {
		parentID := strings.TrimPrefix(parent.Record, "r:app-invite-")
		if len(parentID) != 32 {
			return errNativeRecord
		}
		parentRecord, e := readPrivateProcedureRecord(parent.View.Records[parent.Record])
		if e != nil {
			return e
		}
		if parentRecord.Native.Phase == string(portable.Terminated) && len(parentRecord.Outputs) == 0 && len(parentRecord.Events) == 0 {
			timers = append(timers, rediscore.TimerChange{Member: "app-invite-retire-" + parentID, Due: time.UnixMilli(due.CheckedAtMS)})
		}
	}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorClientTransaction, proc.Native.Key), Record: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
