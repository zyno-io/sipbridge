package engine

import (
	"encoding/hex"
	"strconv"
	"strings"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
)

// validReadyApplicationInvite rejects an unsupported output before socket I/O.
func validReadyApplicationInvite(metadata *nativepb.Output, field string) bool {
	const prefix = "r:app-invite-output-"
	if metadata == nil || metadata.Write == nil || metadata.Write.Effect == nil || metadata.Write.Effect.Context == nil || metadata.Write.Effect.Context.State == nil || metadata.Write.Flow == nil || metadata.Write.Permit == nil || metadata.Write.Permit.Authorization == nil || metadata.Method != "INVITE" || metadata.Status != 0 || !strings.HasPrefix(field, prefix) {
		return false
	}
	id := strings.TrimPrefix(field, prefix)
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 || id != hex.EncodeToString(decoded) || metadata.Procedure != "r:app-invite-"+id || metadata.Dialog == "" || metadata.Operation == "" || metadata.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION || !strings.HasPrefix(metadata.Result, "r:result-") {
		return false
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(metadata.Result, "r:result-"), 10, 53)
	return err == nil && sequence > 0 && metadata.Result == "r:result-"+strconv.FormatUint(sequence, 10)
}
