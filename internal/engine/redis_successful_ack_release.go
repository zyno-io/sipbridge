package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// planRedisSuccessfulACKRelease removes the previous successful ACK projection
// only once its original M and every replay attempt are closed. Each read is
// fenced to the same selected revision and worker assignment as the M CAS.
func planRedisSuccessfulACKRelease(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, current *nativepb.Context, field string, candidate *nativepb.Procedure) ([]rediscore.Change, bool, error) {
	if store == nil || due == nil || current == nil || candidate == nil || candidate.Native == nil || candidate.SuccessfulAck == nil || candidate.Native.Phase != string(portable.Terminated) {
		return nil, false, errNativeRecord
	}
	for _, output := range candidate.Outputs {
		if strings.HasPrefix(output, "r:retained-ack-output-") {
			return nil, false, nil
		}
	}
	tuple := candidate.SuccessfulAck
	if !validNativeSuccessfulACK(tuple) {
		return nil, false, errNativeRecord
	}
	// A missing dialog can only arise during teardown. Terminal retirement owns
	// the remaining tuple in that case, rather than guessing about wire owners.
	if current.CallId == "" || current.LocalTag == "" || current.RemoteTag == "" {
		return nil, false, nil
	}
	tags := []string{current.LocalTag, current.RemoteTag}
	slices.Sort(tags)
	dialogLookup, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorDialog, current.CallId, tags[0], tags[1]))
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if dialogLookup.Identity != due.Identity || dialogLookup.View.Revision != due.Revision || dialogLookup.View.Timer != due.Assignment {
		return nil, false, rediscore.ErrConflict
	}
	if !slices.Contains(current.Dialogs, dialogLookup.Record) {
		return nil, false, errNativeRecord
	}
	dialog, err := readPrivateDialogRecord(dialogLookup.View.Records[dialogLookup.Record])
	if err != nil {
		return nil, false, err
	}
	if dialog.Native.Version != portable.Version || dialog.Native.CallId != current.CallId || dialog.Native.LocalTag != current.LocalTag || dialog.Native.RemoteTag != current.RemoteTag {
		return nil, false, errNativeRecord
	}
	fields := append([]string(nil), current.InitialProcedures...)
	add := func(f string) {
		if f != "" && !slices.Contains(fields, f) {
			fields = append(fields, f)
		}
	}
	add(candidate.CommandResult)
	add(tuple.AckResult)
	if current.CommandSequence > 0 {
		add("r:result-" + strconv.FormatUint(current.CommandSequence, 10))
	}
	records, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return nil, false, err
	}
	if records.Revision != due.Revision || records.Timer != due.Assignment {
		return nil, false, rediscore.ErrConflict
	}
	var root *nativepb.Procedure
	var rootField string
	for _, anchor := range current.InitialProcedures {
		p, e := readPrivateProcedureRecord(records.Records[anchor])
		if e != nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteClient) && p.Native.Family != string(portable.InviteServer) {
			return nil, false, errNativeRecord
		}
		if p.Root == "" {
			if root != nil {
				return nil, false, errNativeRecord
			}
			root, rootField = p, anchor
		}
	}
	if root == nil || current.Kind == pb.ContextSnapshot_UAC && root.Native.Family != string(portable.InviteClient) || current.Kind == pb.ContextSnapshot_UAS && root.Native.Family != string(portable.InviteServer) || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS {
		return nil, false, errNativeRecord
	}
	for _, anchor := range current.InitialProcedures {
		p, e := readPrivateProcedureRecord(records.Records[anchor])
		if e != nil || anchor != rootField && (p.Root != rootField || p.Native.Family != string(portable.InviteClient) || p.CommandResult == "" || p.CommandResult != root.CommandResult) {
			return nil, false, errNativeRecord
		}
	}
	if field == rootField {
		root = candidate
	}
	var associated *nativepb.Procedure
	if dialog.LocalInvite != nil && dialog.LocalInvite.Request != "" {
		origin := dialog.LocalInvite.Request
		var associatedField string
		switch {
		case strings.HasPrefix(origin, "r:uac-origin-"):
			associatedField = "r:uac-procedure-" + strings.TrimPrefix(origin, "r:uac-origin-")
		case strings.HasPrefix(origin, "r:app-invite-origin-"):
			associatedField = "r:app-invite-" + strings.TrimPrefix(origin, "r:app-invite-origin-")
		default:
			return nil, false, errNativeRecord
		}
		if !slices.Contains(current.InitialProcedures, associatedField) && !slices.Contains(current.NoninitialProcedures, associatedField) {
			return nil, false, errNativeRecord
		}
		selected, e := store.Read(ctx, due.Identity, associatedField)
		if e != nil {
			return nil, false, e
		}
		if selected.Revision != due.Revision || selected.Timer != due.Assignment {
			return nil, false, rediscore.ErrConflict
		}
		associated, e = readPrivateProcedureRecord(selected.Records[associatedField])
		if e != nil || associated.Native.Family != string(portable.InviteClient) || associated.Origin != origin {
			return nil, false, errNativeRecord
		}
		if associatedField == field {
			associated = candidate
		}
	}
	resultFields := []string{}
	addResult := func(f string) {
		if f != "" && !slices.Contains(resultFields, f) {
			resultFields = append(resultFields, f)
		}
	}
	addResult(candidate.CommandResult)
	addResult(tuple.AckResult)
	addResult(root.CommandResult)
	if associated != nil {
		addResult(associated.CommandResult)
	}
	if current.CommandSequence > 0 {
		addResult("r:result-" + strconv.FormatUint(current.CommandSequence, 10))
	}
	results, err := store.Read(ctx, due.Identity, resultFields...)
	if err != nil {
		return nil, false, err
	}
	if results.Revision != due.Revision || results.Timer != due.Assignment {
		return nil, false, rediscore.ErrConflict
	}
	held := map[string]bool{tuple.FinalResponse: false, tuple.Ack: false}
	for _, f := range resultFields {
		result, e := decodeNativeResult(results.Records[f])
		if e != nil {
			return nil, false, e
		}
		if result.Sequence == 0 || result.Sequence > current.CommandSequence || f != "r:result-"+strconv.FormatUint(result.Sequence, 10) {
			return nil, false, errNativeRecord
		}
		if f == tuple.AckResult && result.Request != tuple.Ack {
			return nil, false, errNativeRecord
		}
		for wire := range held {
			if result.Request == wire || result.Response == wire {
				held[wire] = true
			}
		}
	}
	p := proto.Clone(candidate).(*nativepb.Procedure)
	p.SuccessfulAck = nil
	for wire := range held {
		if nativeProcedureReferencesWire(p, wire) || nativeProcedureReferencesWire(root, wire) && rootField != field || associated != nil && associated != candidate && nativeProcedureReferencesWire(associated, wire) || nativeDialogReferencesWire(dialog, wire) {
			held[wire] = true
		}
	}
	events := append([]string(nil), root.Events...)
	for _, event := range p.Events {
		if !slices.Contains(events, event) {
			events = append(events, event)
		}
	}
	var changes []rediscore.Change
	if len(events) > 0 {
		view, e := store.Read(ctx, due.Identity, events...)
		if e != nil {
			return nil, false, e
		}
		if view.Revision != due.Revision || view.Timer != due.Assignment {
			return nil, false, rediscore.ErrConflict
		}
		for _, eventField := range events {
			event, x := decodeNativeLifecycle(view.Records[eventField])
			if x != nil || event.Root != rootField {
				return nil, false, errNativeRecord
			}
			if _, ok := held[event.Message]; ok {
				held[event.Message] = true
				if slices.Contains(event.Owners, field) && !nativeProcedureReferencesWire(p, event.Message) {
					event.Owners = slices.DeleteFunc(event.Owners, func(owner string) bool { return owner == field })
					if field != rootField {
						p.Events = slices.DeleteFunc(p.Events, func(ref string) bool { return ref == eventField })
					}
					value, x := encodeNativeLifecycle(event)
					if x != nil {
						return nil, false, x
					}
					changes = append(changes, rediscore.Change{Field: eventField, Value: value})
				}
			}
		}
	}
	for wire, keep := range held {
		if !keep {
			changes = append(changes, rediscore.Change{Field: wire, Delete: true})
		}
	}
	candidate.SuccessfulAck = nil
	candidate.Events = p.Events
	return changes, true, nil
}
