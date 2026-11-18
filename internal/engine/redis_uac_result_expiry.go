package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func uacInitialResultTimer(root string) (string, error) {
	const prefix = "r:uac-procedure-"
	if !strings.HasPrefix(root, prefix) || len(strings.TrimPrefix(root, prefix)) != 32 {
		return "", errNativeRecord
	}
	return "uac-result-" + strings.TrimPrefix(root, prefix), nil
}

// planRedisUACInitialResultExpiry is joined to the later command admission.
// It schedules compaction at the original cutoff, never deleting the marker.
func planRedisUACInitialResultExpiry(root string, result *nativepb.Result, nextSequence uint64) (rediscore.TimerChange, error) {
	member, err := uacInitialResultTimer(root)
	if err != nil || result == nil || result.Sequence != 1 || result.CutoffMs <= 0 || result.CutoffMs > 1<<53-1 || nextSequence <= result.Sequence {
		return rediscore.TimerChange{}, errNativeRecord
	}
	return rediscore.TimerChange{Member: member, Due: time.UnixMilli(result.CutoffMs)}, nil
}

// expireRedisUACInitialResult compacts an invisible original receipt while its
// retained initial anchor keeps scalar operation identity for late forks.
// It reads bounded actual metadata owners, never SIP packets or Redis scans.
func expireRedisUACInitialResult(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, deadline time.Time) error {
	const prefix = "uac-result-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || due.CheckedAtMS < due.DeadlineMS || len(key) < 32 || len(key) > 1024 || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	root := "r:uac-procedure-" + strings.TrimPrefix(due.Member, prefix)
	member, err := uacInitialResultTimer(root)
	if err != nil || member != due.Member {
		return errNativeRecord
	}
	first, err := store.Read(ctx, due.Identity, "r:context", root, "r:result-1")
	if err != nil {
		return err
	}
	if first.Revision != due.Revision || first.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(first.Records["r:context"])
	if err != nil {
		return err
	}
	original, err := readPrivateProcedureRecord(first.Records[root])
	if err != nil {
		return err
	}
	result, err := decodeNativeResult(first.Records["r:result-1"])
	if err != nil {
		return err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAC || current.CommandSequence <= 1 || !slices.Contains(current.InitialProcedures, root) || original.Native.Version != portable.Version || original.Native.Family != string(portable.InviteClient) || original.Root != "" || original.CommandResult != "r:result-1" || result.Sequence != 1 || result.CutoffMs != due.DeadlineMS || result.Pending {
		return errNativeRecord
	}
	// Initial and digest anchors are bounded by the native call profile. Their
	// scalar references establish whether either packet is still natively owned.
	fields := make([]string, 0, len(current.InitialProcedures)+len(original.Events)+2)
	for _, field := range current.InitialProcedures {
		if field != root {
			fields = append(fields, field)
		}
	}
	fields = append(fields, original.Events...)
	currentResult := "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
	fields = append(fields, currentResult)
	winner := ""
	if result.Response != "" && current.Answered && current.LocalTag != "" && current.RemoteTag != "" {
		winner = "r:uac-dialog-" + nativeIdentity(key, "uac-dialog", current.Context.State.Authority.PrimaryId, current.Context.State.Incarnation, current.Context.Id, current.LocalTag, current.RemoteTag)[:32]
		if slices.Contains(current.Dialogs, winner) {
			fields = append(fields, winner)
		} else {
			winner = ""
		}
	}
	view, err := store.Read(ctx, due.Identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return rediscore.ErrConflict
	}
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	owners := make(map[string]bool, 4)
	add := func(values ...string) {
		for _, v := range values {
			if v != "" {
				owners[v] = true
			}
		}
	}
	add(original.Origin, original.LastResponse, original.Ack)
	if original.SuccessfulAck != nil {
		add(original.SuccessfulAck.FinalResponse, original.SuccessfulAck.Ack)
	}
	for _, field := range current.InitialProcedures {
		if field == root {
			continue
		}
		p, e := readPrivateProcedureRecord(view.Records[field])
		if e != nil {
			return e
		}
		if p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteClient) || p.Root != root || p.CommandResult != "r:result-1" {
			return errNativeRecord
		}
		add(p.Origin, p.LastResponse, p.Ack)
		if p.SuccessfulAck != nil {
			add(p.SuccessfulAck.FinalResponse, p.SuccessfulAck.Ack)
		}
	}
	if winner != "" {
		dialog, e := readPrivateDialogRecord(view.Records[winner])
		if e != nil {
			return e
		}
		if dialog.Native.Version != portable.Version || dialog.Native.Role != string(portable.UAC) || dialog.Native.CallId != current.CallId || dialog.Native.LocalTag != current.LocalTag || dialog.Native.RemoteTag != current.RemoteTag || dialog.LocalInvite == nil {
			return errNativeRecord
		}
		add(dialog.LocalInvite.Request, dialog.LocalInvite.FinalResponse, dialog.LocalInvite.Ack, dialog.LocalBye)
	}
	latest, err := decodeNativeResult(view.Records[currentResult])
	if err != nil || latest.Sequence != current.CommandSequence {
		return errNativeRecord
	}
	add(latest.Request, latest.Response)
	var changes []rediscore.Change
	for _, field := range original.Events {
		event, e := decodeNativeLifecycle(view.Records[field])
		if e != nil {
			return e
		}
		if event.Root != root || event.Message == "" {
			return errNativeRecord
		}
		add(event.Message)
		if event.Message == result.Request || event.Message == result.Response {
			if index, ok := slices.BinarySearch(event.Owners, "r:result-1"); ok {
				event.Owners = slices.Delete(event.Owners, index, index+1)
				data, e := encodeNativeLifecycle(event)
				if e != nil {
					return e
				}
				changes = append(changes, rediscore.Change{Field: field, Value: data})
			}
		}
	}
	// The still-live initial graph is the authority for late matching; its
	// origin cannot be discarded merely because the receipt is superseded.
	if result.Request != "" && !owners[result.Request] {
		return errors.New("UAC original request lost native owner")
	}
	seen := map[string]bool{}
	for _, field := range []string{result.Request, result.Response} {
		if field != "" && !seen[field] && !owners[field] {
			changes = append(changes, rediscore.Change{Field: field, Delete: true})
			seen[field] = true
		}
	}
	result.Request = ""
	result.Response = ""
	result.Error = ""
	data, err := encodeNativeResult(result)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: "r:result-1", Value: data})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
	return err
}
