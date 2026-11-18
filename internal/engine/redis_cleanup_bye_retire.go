package engine

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// nativeCleanupCanonicalRoot follows the one permitted digest-retry backlink
// within the already selected bounded initial-procedure records.
func nativeCleanupCanonicalRoot(current *nativepb.Context, records map[string][]byte, transaction string) (string, *nativepb.Procedure, error) {
	var field string
	var selected *nativepb.Procedure
	for _, candidate := range current.InitialProcedures {
		p, err := readPrivateProcedureRecord(records[candidate])
		if err != nil {
			return "", nil, err
		}
		if p.Native.Key == transaction {
			if selected != nil {
				return "", nil, errNativeRecord
			}
			field, selected = candidate, p
		}
	}
	if selected == nil || selected.Native.Version != portable.Version || selected.Native.Family != string(portable.InviteServer) && selected.Native.Family != string(portable.InviteClient) {
		return "", nil, errNativeRecord
	}
	if selected.Root != "" {
		if selected.Native.Family != string(portable.InviteClient) || !slices.Contains(current.InitialProcedures, selected.Root) {
			return "", nil, errNativeRecord
		}
		lineage := selected.CommandResult
		field = selected.Root
		var err error
		selected, err = readPrivateProcedureRecord(records[field])
		if err != nil {
			return "", nil, err
		}
		if selected.Native.Family != string(portable.InviteClient) || lineage == "" || selected.CommandResult != lineage {
			return "", nil, errNativeRecord
		}
	}
	if selected.Root != "" || selected.Native.Version != portable.Version || selected.Native.Family != string(portable.InviteServer) && selected.Native.Family != string(portable.InviteClient) {
		return "", nil, errNativeRecord
	}
	return field, selected, nil
}

// planRedisCleanupByeRetirement releases only this terminated procedure. The
// dialog still owns its LocalBye wire; a lifecycle event independently owns
// the response until its own acknowledgement or cutoff.
func planRedisCleanupByeRetirement(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, current *nativepb.Context, dialog *nativepb.Dialog, procedureField string, procedure *nativepb.Procedure, results ...*nativepb.Result) ([]rediscore.Change, rediscore.SelectorChange, error) {
	var selector rediscore.SelectorChange
	generic := strings.HasPrefix(procedureField, "r:app-client-")
	if current == nil || dialog == nil || procedure == nil || procedure.Origin == "" || !generic && dialog.LocalBye != procedure.Origin || len(procedure.Events) != 0 || len(results) > 1 {
		return nil, selector, errNativeRecord
	}
	var result *nativepb.Result
	if len(results) == 1 {
		result = results[0]
	}
	if procedure.CommandResult == "" && result != nil || procedure.CommandResult != "" && (result == nil || result.Sequence == 0 || procedure.CommandResult != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.Pending || result.Request != "" && result.Request != procedure.Origin || result.Response != "" && result.Response != procedure.LastResponse) {
		return nil, selector, errNativeRecord
	}
	if procedure.Native.Version != portable.Version || procedure.Native.Family != string(portable.NonInviteClient) || procedure.Native.Key == "" {
		return nil, selector, errNativeRecord
	}
	selector = rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorClientTransaction, procedure.Native.Key), Record: procedureField, Delete: true}
	if generic {
		changes := []rediscore.Change{}
		if result == nil || result.Request != procedure.Origin {
			changes = append(changes, rediscore.Change{Field: procedure.Origin, Delete: true})
		}
		if procedure.LastResponse != "" && (result == nil || result.Response != procedure.LastResponse) {
			changes = append(changes, rediscore.Change{Field: procedure.LastResponse, Delete: true})
		}
		index, found := slices.BinarySearch(current.NoninitialProcedures, procedureField)
		if !found {
			return nil, selector, errNativeRecord
		}
		current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, index, index+1)
		encoded, err := encodeNativeContext(current)
		if err != nil {
			return nil, selector, err
		}
		return append(changes, rediscore.Change{Field: "r:context", Value: encoded}, rediscore.Change{Field: procedureField, Delete: true}), selector, nil
	}
	view, err := store.Read(ctx, due.Identity, current.InitialProcedures...)
	if err != nil {
		return nil, selector, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return nil, selector, rediscore.ErrConflict
	}
	remoteRequest, remoteFinal := "", ""
	if dialog.RemoteInvite != nil {
		remoteRequest, remoteFinal = dialog.RemoteInvite.Request, dialog.RemoteInvite.FinalResponse
	}
	rootField, root, _, err := resolveRedisCleanupRoot(ctx, store, due.Identity, due.Revision, due.Assignment, current, view.Records, dialog.Transaction, remoteRequest, remoteFinal)
	if err != nil {
		return nil, selector, err
	}
	var changes []rediscore.Change
	held := result != nil && procedure.LastResponse != "" && result.Response == procedure.LastResponse
	if len(root.Events) > 0 {
		events, e := store.Read(ctx, due.Identity, root.Events...)
		if e != nil {
			return nil, selector, e
		}
		if events.Revision != due.Revision || events.Timer != due.Assignment {
			return nil, selector, rediscore.ErrConflict
		}
		for _, field := range root.Events {
			event, x := decodeNativeLifecycle(events.Records[field])
			if x != nil || event.Root != rootField {
				return nil, selector, errNativeRecord
			}
			if procedure.LastResponse != "" && event.Message == procedure.LastResponse {
				held = true
			}
			index := slices.Index(event.Owners, procedureField)
			if index >= 0 {
				if event.Message == "" || event.Message != procedure.LastResponse {
					return nil, selector, errNativeRecord
				}
				held = true
				event.Owners = slices.Delete(event.Owners, index, index+1)
				encoded, x := encodeNativeLifecycle(event)
				if x != nil {
					return nil, selector, x
				}
				changes = append(changes, rediscore.Change{Field: field, Value: encoded})
			}
		}
	}
	if procedure.LastResponse != "" && !held {
		changes = append(changes, rediscore.Change{Field: procedure.LastResponse, Delete: true})
	}
	index, found := slices.BinarySearch(current.NoninitialProcedures, procedureField)
	if !found {
		return nil, selector, errNativeRecord
	}
	current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, index, index+1)
	encoded, err := encodeNativeContext(current)
	if err != nil {
		return nil, selector, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: encoded}, rediscore.Change{Field: procedureField, Delete: true})
	return changes, selector, nil
}
