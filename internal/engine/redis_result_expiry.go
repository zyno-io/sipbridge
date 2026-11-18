package engine

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// expireRedisSupersededResult removes one old result after its original waiter
// cutoff and actual native/output ownership end; other owners keep exact wire.
func expireRedisSupersededResult(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "orig-result-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	text := strings.TrimPrefix(due.Member, prefix)
	sequence, err := strconv.ParseUint(text, 10, 53)
	if err != nil || sequence == 0 || text != strconv.FormatUint(sequence, 10) {
		return errNativeRecord
	}
	resultField := "r:result-" + text
	view, err := store.Read(ctx, due.Identity, "r:context", resultField)
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
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC {
		return errNativeRecord
	}
	result, err := decodeNativeResult(view.Records[resultField])
	if err != nil {
		return err
	}
	if result.Sequence != sequence || result.Pending || result.Output != "" || current.CommandSequence <= sequence || result.CutoffMs != due.DeadlineMS {
		return errNativeRecord
	}
	if result.Certainty == pb.OperationResult_NOT_ACCEPTED && result.ServerRequestId == "" {
		timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
		if current.Ended {
			if current.CallRetireAtMs <= 0 {
				return errNativeRecord
			}
			timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
		}
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: resultField, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return err
	}
	origin, response := result.Request, result.Response
	fields := append(append(append([]string(nil), current.InitialProcedures...), current.NoninitialProcedures...), current.Dialogs...)
	records := make(map[string][]byte, len(fields))
	read := func(fields []string) error {
		for len(fields) > 0 {
			n := min(128, len(fields))
			part, e := store.Read(ctx, due.Identity, fields[:n]...)
			if e != nil {
				return e
			}
			if part.Revision != due.Revision || part.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			for field, value := range part.Records {
				records[field] = value
			}
			fields = fields[n:]
		}
		return nil
	}
	if origin != "" && !slices.Contains(fields, origin) {
		fields = append(fields, origin)
	}
	if response != "" && !slices.Contains(fields, response) {
		fields = append(fields, response)
	}
	if err = read(fields); err != nil {
		return err
	}
	if origin != "" && len(records[origin]) == 0 || response != "" && len(records[response]) == 0 {
		return errNativeRecord
	}
	heldOrigin, heldResponse := false, false
	var eventFields []string
	var otherResults []string
	var procedureChanges []rediscore.Change
	linkedACK := false
	for _, field := range append(append([]string(nil), current.InitialProcedures...), current.NoninitialProcedures...) {
		p, e := readPrivateProcedureRecord(records[field])
		if e != nil {
			return e
		}
		if p.CommandResult == resultField {
			return errNativeRecord
		}
		if p.SuccessfulAck != nil && p.SuccessfulAck.AckResult == resultField {
			id, idErr := redisApplicationByeID(result.Fingerprint)
			if linkedACK || idErr != nil || result.Request == "" || result.Request != p.SuccessfulAck.Ack || result.Request != "r:app-ack-wire-"+id {
				return errNativeRecord
			}
			linkedACK = true
			p.SuccessfulAck.AckResult = ""
			data, encodeErr := marshalNativeRecord(storagecodec.NativeProcedure, p)
			if encodeErr != nil {
				return encodeErr
			}
			procedureChanges = append(procedureChanges, rediscore.Change{Field: field, Value: data})
		}
		heldOrigin = heldOrigin || origin != "" && nativeProcedureReferencesWire(p, origin)
		heldResponse = heldResponse || response != "" && nativeProcedureReferencesWire(p, response)
		for _, event := range p.Events {
			if !slices.Contains(eventFields, event) {
				eventFields = append(eventFields, event)
			}
		}
		if p.CommandResult != "" && p.CommandResult != resultField && !slices.Contains(otherResults, p.CommandResult) {
			otherResults = append(otherResults, p.CommandResult)
		}
	}
	var dialogChanges []rediscore.Change
	for _, field := range current.Dialogs {
		d, e := readPrivateDialogRecord(records[field])
		if e != nil {
			return e
		}
		heldOrigin = heldOrigin || origin != "" && nativeDialogReferencesWire(d, origin)
		heldResponse = heldResponse || response != "" && nativeDialogReferencesWire(d, response)
		if d.AckResult == resultField {
			id, idErr := redisApplicationByeID(result.Fingerprint)
			if idErr != nil || d.LocalInvite == nil || d.LocalInvite.Ack != "r:app-ack-wire-"+id || result.Request != "" && result.Request != d.LocalInvite.Ack {
				return errNativeRecord
			}
			d.AckResult = ""
			data, encodeErr := marshalNativeRecord(storagecodec.NativeDialog, d)
			if encodeErr != nil {
				return encodeErr
			}
			dialogChanges = append(dialogChanges, rediscore.Change{Field: field, Value: data})
		}
	}
	currentResult := "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
	if currentResult != resultField && !slices.Contains(otherResults, currentResult) {
		otherResults = append(otherResults, currentResult)
	}
	if err = read(append(eventFields, otherResults...)); err != nil {
		return err
	}
	for _, field := range otherResults {
		r, e := decodeNativeResult(records[field])
		if e != nil {
			return e
		}
		heldOrigin = heldOrigin || origin != "" && (r.Request == origin || r.Response == origin)
		heldResponse = heldResponse || response != "" && (r.Request == response || r.Response == response)
	}
	changes := []rediscore.Change{{Field: resultField, Delete: true}}
	changes = append(changes, procedureChanges...)
	changes = append(changes, dialogChanges...)
	for _, field := range eventFields {
		event, e := decodeNativeLifecycle(records[field])
		if e != nil {
			return e
		}
		if origin != "" && event.Message == origin {
			heldOrigin = true
		}
		if response != "" && event.Message == response {
			heldResponse = true
		}
		if index := slices.Index(event.Owners, resultField); index >= 0 {
			if event.Message == "" || event.Message != result.Request && event.Message != result.Response {
				return errNativeRecord
			}
			next := proto.Clone(event).(*nativepb.Lifecycle)
			next.Owners = slices.Delete(next.Owners, index, index+1)
			data, e := encodeNativeLifecycle(next)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: field, Value: data})
		}
	}
	if origin != "" && !heldOrigin && len(records[origin]) != 0 {
		changes = append(changes, rediscore.Change{Field: origin, Delete: true})
	}
	if response != "" && response != origin && !heldResponse && len(records[response]) != 0 {
		changes = append(changes, rediscore.Change{Field: response, Delete: true})
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if current.Ended {
		if current.CallRetireAtMs <= 0 {
			return errNativeRecord
		}
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
