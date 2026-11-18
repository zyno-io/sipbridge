package engine

import (
	"bytes"
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// Owners can change as the same immutable response acquires or releases a
// receipt reference. Only that bookkeeping is excluded from the delivery proof.
func sameNativeLifecycleDelivery(a, b *nativepb.Lifecycle) bool {
	return a != nil && b != nil && a.Kind == b.Kind && proto.Equal(a.Snapshot, b.Snapshot) && a.OperationId == b.OperationId && a.Error == b.Error &&
		a.Message == b.Message && a.Root == b.Root && slices.Equal(a.Selectors, b.Selectors) && a.DeadlineMs == b.DeadlineMs && a.NextMs == b.NextMs && a.BackoffMs == b.BackoffMs && bytes.Equal(a.ProtoReflect().GetUnknown(), b.ProtoReflect().GetUnknown())
}

func nativeProcedureReferencesWire(p *nativepb.Procedure, field string) bool {
	return p.Origin == field || p.LastResponse == field || p.Ack == field || p.SuccessfulAck != nil && (p.SuccessfulAck.FinalResponse == field || p.SuccessfulAck.Ack == field)
}

func nativeDialogReferencesWire(d *nativepb.Dialog, field string) bool {
	if d.LocalBye == field {
		return true
	}
	for _, refs := range []*nativepb.InviteRefs{d.LocalInvite, d.RemoteInvite} {
		if refs != nil && (refs.Request == field || refs.FinalResponse == field || refs.Ack == field) {
			return true
		}
	}
	return false
}

// planRedisLifecycleMessageRelease reads only the canonical anchor and the
// selected response's bounded actual owners. It neither reads SIP wire nor
// rebuilds a call graph. Its returned changes join event deletion in one CAS.
func planRedisLifecycleMessageRelease(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, eventField string, event *nativepb.Lifecycle) ([]rediscore.Change, error) {
	if store == nil || current == nil || event == nil || event.Message == "" || event.Root == "" || !slices.Contains(current.InitialProcedures, event.Root) {
		return nil, errNativeRecord
	}
	fields := []string{event.Root}
	for _, owner := range event.Owners {
		if owner != event.Root {
			fields = append(fields, owner)
		}
	}
	view, err := store.Read(ctx, identity, fields...)
	if err != nil {
		return nil, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return nil, rediscore.ErrConflict
	}
	root, err := readPrivateProcedureRecord(view.Records[event.Root])
	if err != nil {
		return nil, err
	}
	if root.Native.Version != portable.Version || root.Native.Family != string(portable.InviteClient) && root.Native.Family != string(portable.InviteServer) || root.Root != "" {
		return nil, errNativeRecord
	}
	index, found := slices.BinarySearch(root.Events, eventField)
	if !found {
		return nil, errNativeRecord
	}
	held := nativeProcedureReferencesWire(root, event.Message)
	for _, owner := range event.Owners {
		data := view.Records[owner]
		if data == nil {
			// A removed owner cannot retain this field or recreate its old
			// non-reused record identity. The event still owns its wire.
			continue
		}
		switch {
		case slices.Contains(current.InitialProcedures, owner), slices.Contains(current.NoninitialProcedures, owner):
			procedure := root
			if owner != event.Root {
				procedure, err = readPrivateProcedureRecord(data)
				if err != nil {
					return nil, err
				}
			}
			if procedure.Native.Version != portable.Version {
				return nil, errNativeRecord
			}
			held = held || nativeProcedureReferencesWire(procedure, event.Message)
		case slices.Contains(current.Dialogs, owner):
			dialog, e := readPrivateDialogRecord(data)
			if e != nil {
				return nil, e
			}
			if dialog.Native.Version != portable.Version || dialog.Native.CallId != current.CallId {
				return nil, errNativeRecord
			}
			held = held || nativeDialogReferencesWire(dialog, event.Message)
		case strings.HasPrefix(owner, "r:result-"):
			text := strings.TrimPrefix(owner, "r:result-")
			sequence, e := strconv.ParseUint(text, 10, 53)
			if e != nil || sequence == 0 || sequence > current.CommandSequence || strconv.FormatUint(sequence, 10) != text {
				return nil, errNativeRecord
			}
			result, e := decodeNativeResult(data)
			if e != nil {
				return nil, e
			}
			if result.Sequence != sequence {
				return nil, errNativeRecord
			}
			held = held || result.Request == event.Message || result.Response == event.Message
		default:
			return nil, errNativeRecord
		}
	}
	root.Events = slices.Delete(root.Events, index, index+1)
	encoded, err := marshalNativeRecord(storagecodec.NativeProcedure, root)
	if err != nil {
		return nil, err
	}
	changes := []rediscore.Change{{Field: event.Root, Value: encoded}}
	if !held {
		changes = append(changes, rediscore.Change{Field: event.Message, Delete: true})
	}
	return changes, nil
}
