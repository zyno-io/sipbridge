package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// planRedisReinviteReplacement selects only the actual previous remote INVITE
// owner; the failed INVITE's key need not equal the last successful association.
func planRedisReinviteReplacement(ctx context.Context, store *rediscore.Store, candidate redisDialogDecision) ([]rediscore.Change, error) {
	refs := candidate.dialog.Refs.Remote
	previous := candidate.previous.RemoteInvite
	if refs == nil && previous == nil {
		return nil, nil
	}
	if refs == nil || previous == nil || refs.Request == "" {
		return nil, errNativeRecord
	}
	key, err := previous.Request.ServerTxKey()
	if err != nil {
		return nil, err
	}
	owner, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, key))
	var retained *nativeProcedureRecord
	if err == nil {
		if owner.Identity != candidate.selected.Identity || owner.View.Revision != candidate.selected.View.Revision || owner.View.Timer != candidate.selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		value, e := decodeNativeProcedure(owner.View.Records[owner.Record])
		if e != nil || value.Native.Family != portable.InviteServer || value.Native.Key != key || value.Refs.Origin != refs.Request || !slices.Contains(candidate.current.InitialProcedures, owner.Record) && !slices.Contains(candidate.current.NoninitialProcedures, owner.Record) {
			return nil, errNativeRecord
		}
		retained = &value
	} else if !errors.Is(err, rediscore.ErrAbsent) {
		return nil, err
	}
	keep := func(field string) bool {
		if field == "" {
			return true
		}
		local := candidate.dialog.Refs.Local
		return local != nil && (field == local.Request || field == local.Final || field == local.ACK) || field == candidate.dialog.Refs.LocalBye
	}
	var changes []rediscore.Change
	if !keep(refs.Request) && retained == nil {
		changes = append(changes, rediscore.Change{Field: refs.Request, Delete: true})
	}
	if refs.Final != "" && retained != nil && retained.Refs.LastResponse != refs.Final {
		return nil, errNativeRecord
	}
	if refs.Final != "" && !keep(refs.Final) && retained == nil {
		changes = append(changes, rediscore.Change{Field: refs.Final, Delete: true})
	}
	if refs.ACK != "" && !keep(refs.ACK) {
		digest := sha256.Sum256([]byte(candidate.procedure.Metadata.RequestDialog + "/" + strconv.FormatUint(uint64(previous.Sequence), 10)))
		if refs.ACK != "r:call-ack-"+hex.EncodeToString(digest[:16]) {
			return nil, errNativeRecord
		}
		changes = append(changes, rediscore.Change{Field: refs.ACK, Delete: true})
	}
	return changes, nil
}

func planRedisSupersededInviteRetirement(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, oldKey, newKey string, now time.Time) ([]rediscore.TimerChange, error) {
	if oldKey == "" || oldKey == newKey {
		return nil, nil
	}
	owner, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, oldKey))
	if errors.Is(err, rediscore.ErrAbsent) {
		client, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorClientTransaction, oldKey))
		if e != nil {
			return nil, e
		}
		if client.Identity != selected.Identity || client.View.Revision != selected.View.Revision || client.View.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		procedure, e := readPrivateProcedureRecord(client.View.Records[client.Record])
		if e != nil || procedure.Native.Family != string(portable.InviteClient) || procedure.Native.Key != oldKey || !slices.Contains(current.InitialProcedures, client.Record) && !slices.Contains(current.NoninitialProcedures, client.Record) {
			return nil, errNativeRecord
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if owner.Identity != selected.Identity || owner.View.Revision != selected.View.Revision || owner.View.Timer != selected.View.Timer {
		return nil, rediscore.ErrConflict
	}
	procedure, err := readPrivateProcedureRecord(owner.View.Records[owner.Record])
	if err != nil || procedure.Native.Family != string(portable.InviteServer) || procedure.Native.Key != oldKey {
		return nil, errNativeRecord
	}
	if slices.Contains(current.InitialProcedures, owner.Record) {
		return nil, nil
	}
	if !slices.Contains(current.NoninitialProcedures, owner.Record) || !strings.HasPrefix(owner.Record, "r:procedure-") || procedure.RequestId != strings.TrimPrefix(owner.Record, "r:procedure-") {
		return nil, errNativeRecord
	}
	if procedure.Native.Phase != string(portable.Terminated) || len(procedure.Outputs) != 0 {
		return nil, nil
	}
	return []rediscore.TimerChange{{Member: "call-proc-" + procedure.RequestId, Due: now}}, nil
}

// replaceRedisRemoteInvite stores native scalars and separately owned message
// references, without serializing any retained packet into the dialog record.
func replaceRedisRemoteInvite(record *nativepb.Dialog, dialog portable.Dialog, request, final string) error {
	if record == nil || dialog.RemoteInvite == nil || request == "" {
		return errNativeRecord
	}
	native := storageEncodePortableDialog(dialog)
	if native.LocalInvite != nil {
		native.LocalInvite.Request, native.LocalInvite.Final, native.LocalInvite.Ack = nil, nil, nil
	}
	if native.RemoteInvite != nil {
		native.RemoteInvite.Request, native.RemoteInvite.Final, native.RemoteInvite.Ack = nil, nil, nil
	}
	native.LocalBye = nil
	record.Native = native
	record.RemoteInvite = &nativepb.InviteRefs{Request: request, FinalResponse: final}
	return nil
}
