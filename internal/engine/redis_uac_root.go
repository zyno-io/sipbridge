package engine

import (
	"context"
	"slices"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// selectRedisUACRoot selects the one original credit/event owner for an actual
// initial INVITE attempt. Digest children point directly to that retained root.
func selectRedisUACRoot(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, field string, proc nativeProcedureRecord) (string, *nativepb.Procedure, error) {
	root := proc.Metadata.Root
	if root == "" {
		return field, proc.Metadata, nil
	}
	if root == field || proc.Metadata.CreditContribution != 0 || !slices.Contains(current.InitialProcedures, root) {
		return "", nil, errNativeRecord
	}
	view, err := store.Read(ctx, identity, root)
	if err != nil {
		return "", nil, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return "", nil, rediscore.ErrConflict
	}
	record, err := readPrivateProcedureRecord(view.Records[root])
	if err != nil {
		return "", nil, err
	}
	if record.Root != "" || record.Native.Version != portable.Version || record.Native.Family != string(portable.InviteClient) || record.CommandResult != proc.Metadata.CommandResult || record.CreditContribution == 0 {
		return "", nil, errNativeRecord
	}
	return root, record, nil
}

// validateRedisUACOutputRoot preserves the original result backlink while a
// digest child's physical output independently pins that result.
func validateRedisUACOutputRoot(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, field string, proc *nativepb.Procedure, outputField string, result *nativepb.Result) (*nativepb.Procedure, error) {
	if proc.Root == "" {
		if result.Output != outputField {
			return nil, errNativeRecord
		}
		return proc, nil
	}
	if result.Output == outputField || proc.Root == field || proc.CreditContribution != 0 || !slices.Contains(current.InitialProcedures, proc.Root) {
		return nil, errNativeRecord
	}
	view, err := store.Read(ctx, identity, proc.Root)
	if err != nil {
		return nil, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return nil, rediscore.ErrConflict
	}
	root, err := readPrivateProcedureRecord(view.Records[proc.Root])
	if err != nil {
		return nil, err
	}
	if root.Root != "" || root.Native.Version != portable.Version || root.Native.Family != string(portable.InviteClient) || root.CommandResult != proc.CommandResult || root.CreditContribution == 0 || result.Request != "" && result.Request != root.Origin {
		return nil, errNativeRecord
	}
	return root, nil
}
