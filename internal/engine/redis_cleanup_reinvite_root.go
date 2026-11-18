package engine

import (
	"context"
	"slices"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// resolveRedisCleanupRoot preserves the original call event root when the
// current dialog's accepted server INVITE is a noninitial re-INVITE.
func resolveRedisCleanupRoot(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, records map[string][]byte, transaction, remoteRequest, remoteFinal string) (string, *nativepb.Procedure, string, error) {
	if store == nil || current == nil || transaction == "" {
		return "", nil, "", errNativeRecord
	}
	rootField, root, err := nativeCleanupCanonicalRoot(current, records, transaction)
	if err == nil {
		return rootField, root, "", nil
	}
	if remoteRequest == "" || remoteFinal == "" {
		return "", nil, "", errNativeRecord
	}
	selected, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, transaction))
	if err != nil {
		return "", nil, "", err
	}
	if selected.Identity != identity || selected.View.Revision != revision || selected.View.Timer != assignment {
		return "", nil, "", rediscore.ErrConflict
	}
	parent, err := readPrivateProcedureRecord(selected.View.Records[selected.Record])
	if err != nil || !slices.Contains(current.NoninitialProcedures, selected.Record) || parent.Native.Version != portable.Version || parent.Native.Family != string(portable.InviteServer) || parent.Native.Key != transaction || parent.Origin != remoteRequest || parent.LastResponse != remoteFinal {
		return "", nil, "", errNativeRecord
	}
	rootField, root = "", nil
	for _, field := range current.InitialProcedures {
		candidate, e := readPrivateProcedureRecord(records[field])
		if e != nil {
			return "", nil, "", e
		}
		if candidate.Root == "" {
			if root != nil {
				return "", nil, "", errNativeRecord
			}
			rootField, root = field, candidate
		}
	}
	if root == nil || root.Native.Version != portable.Version || current.Kind == pb.ContextSnapshot_UAS && root.Native.Family != string(portable.InviteServer) || current.Kind == pb.ContextSnapshot_UAC && root.Native.Family != string(portable.InviteClient) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC {
		return "", nil, "", errNativeRecord
	}
	for _, field := range current.InitialProcedures {
		candidate, e := readPrivateProcedureRecord(records[field])
		if e != nil {
			return "", nil, "", e
		}
		if field != rootField && (candidate.Root != rootField || candidate.Native.Family != string(portable.InviteClient) || candidate.CommandResult == "" || candidate.CommandResult != root.CommandResult) {
			return "", nil, "", errNativeRecord
		}
	}
	return rootField, root, selected.Record, nil
}
