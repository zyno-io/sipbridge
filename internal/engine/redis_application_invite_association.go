package engine

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func candidateRedisApplicationByeAssociation(dialog *nativepb.Dialog) bool {
	if dialog == nil || dialog.LocalTransaction == "" || !strings.HasPrefix(dialog.LocalBye, "r:app-bye-origin-") {
		return false
	}
	id := strings.TrimPrefix(dialog.LocalBye, "r:app-bye-origin-")
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == id
}

// validateRedisInviteACKAssociation follows only the current exact local BYE
// when it has replaced a still-owned INVITE transaction on the same dialog.
func validateRedisInviteACKAssociation(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, dialog *nativepb.Dialog, procedure *nativepb.Procedure, output *nativepb.Output) error {
	return validateRedisInviteACKLocalAssociation(ctx, store, identity, revision, assignment, current, dialog.Native.CallId, dialog.Native.LocalTag, dialog.Native.RemoteTag, dialog.Native.Transport, dialog.Native.Phase, dialog.LocalBye, dialog.LocalTransaction, procedure.Native.Key, output.CleanupAfterAck)
}

func validateRedisInviteACKLocalAssociation(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, callID, localTag, remoteTag, transport, phase, localBye, localTransaction, procedureKey string, cleanupAfterACK bool) error {
	if validUACDialogLocalAssociation(localBye, localTransaction, procedureKey) {
		return nil
	}
	if phase == string(portable.DialogEnded) && !cleanupAfterACK {
		return nil
	}
	var id, owner string
	switch {
	case strings.HasPrefix(localBye, "r:app-bye-origin-"):
		id = strings.TrimPrefix(localBye, "r:app-bye-origin-")
		owner = "r:app-bye-" + id
	case strings.HasPrefix(localBye, "r:call-cleanup-origin-"):
		id = strings.TrimPrefix(localBye, "r:call-cleanup-origin-")
		owner = "r:call-cleanup-" + id
	default:
		return errNativeRecord
	}
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != id || localTransaction == "" {
		return errNativeRecord
	}
	fields := []string{localBye}
	if slices.Contains(current.NoninitialProcedures, owner) {
		fields = append(fields, owner)
	}
	view, err := store.Read(ctx, identity, fields...)
	if err != nil {
		return err
	}
	if view.Revision != revision || view.Timer != assignment {
		return rediscore.ErrConflict
	}
	bye, err := selectedmessage.DecodeOpaque(view.Records[localBye])
	if err != nil {
		return err
	}
	facts, err := bye.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.BYE || facts.CSeqMethod != sip.BYE || facts.CallID != callID || facts.FromTag != localTag || facts.ToTag != remoteTag || bye.Transport() != transport {
		return errNativeRecord
	}
	tx, err := portable.NewTransaction(portable.NonInviteClient, bye, portable.DefaultTimers(), "")
	if err != nil || tx.Key != localTransaction {
		return errNativeRecord
	}
	if len(fields) == 2 {
		p, e := readPrivateProcedureRecord(view.Records[owner])
		if e != nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.NonInviteClient) || p.Native.Key != tx.Key || p.Origin != localBye {
			return errNativeRecord
		}
	}
	return nil
}
