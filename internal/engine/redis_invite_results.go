package engine

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

func inviteResultTimer(requestID string, sequence uint64) string {
	return "call-result-" + strconv.FormatUint(sequence, 10) + "-" + requestID
}

// planPreviousInviteResult schedules only an already unpinned prior receipt.
// A retained exact output owns re-drive after its own metadata is removed.
func planPreviousInviteResult(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, procedure *nativepb.Procedure, prior *nativepb.Result, requestID string) ([]rediscore.TimerChange, error) {
	if selected == nil || selected.View == nil || procedure == nil || prior == nil || prior.Sequence == 0 || prior.CutoffMs <= 0 {
		return nil, errNativeRecord
	}
	resultField := "r:result-" + strconv.FormatUint(prior.Sequence, 10)
	if prior.Output != "" {
		view, err := store.Read(ctx, selected.Identity, prior.Output)
		if err != nil {
			return nil, err
		}
		if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
			return nil, rediscore.ErrConflict
		}
		output, e := decodeNativeOutput(view.Records[prior.Output])
		if e != nil || output.Result != resultField || output.Operation != prior.OperationId {
			return nil, errNativeRecord
		}
		return nil, nil
	}
	// An absent backlink proves nothing for a producer that has not adopted
	// this contract. Restrict the unpinned case to this initial procedure.
	if prior.Pending || prior.Request != "" || prior.Response != "" || prior.Certainty != pb.OperationResult_NOT_ACCEPTED && procedure.CommandResult != resultField && prior.ServerRequestId != requestID {
		return nil, errNativeRecord
	}
	return []rediscore.TimerChange{{Member: inviteResultTimer(requestID, prior.Sequence), Due: time.UnixMilli(prior.CutoffMs)}}, nil
}

// expireRedisInviteResult deletes only an unpinned superseded response receipt.
func expireRedisInviteResult(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "call-result-") || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	rest := strings.TrimPrefix(due.Member, "call-result-")
	separator := strings.IndexByte(rest, '-')
	if separator < 1 {
		return errNativeRecord
	}
	sequence, err := strconv.ParseUint(rest[:separator], 10, 53)
	id := rest[separator+1:]
	parsed, idErr := uuid.Parse(id)
	if err != nil || sequence == 0 || idErr != nil || parsed.String() != id || inviteResultTimer(id, sequence) != due.Member {
		return errNativeRecord
	}
	procedureField, resultField := "r:procedure-"+id, "r:result-"+strconv.FormatUint(sequence, 10)
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, resultField)
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
	result, err := decodeNativeResult(view.Records[resultField])
	if err != nil {
		return err
	}
	initial := slices.Contains(current.InitialProcedures, procedureField)
	noninitial := slices.Contains(current.NoninitialProcedures, procedureField)
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || result.Sequence != sequence || current.CommandSequence <= sequence || result.Pending || result.Output != "" || result.Request != "" || result.Response != "" || result.CutoffMs != due.DeadlineMS || result.ServerRequestId != "" && result.ServerRequestId != id {
		return errNativeRecord
	}
	var procedure *nativepb.Procedure
	if initial || noninitial {
		procedure, err = readPrivateProcedureRecord(view.Records[procedureField])
		if err != nil || procedure.Native.Version != portable.Version || initial && procedure.Native.Family != string(portable.InviteServer) || noninitial && procedure.Native.Family != string(portable.NonInviteServer) && procedure.Native.Family != string(portable.InviteServer) || procedure.RequestId != id {
			return errNativeRecord
		}
	} else {
		if len(view.Records[procedureField]) != 0 || result.ServerRequestId != id {
			return errNativeRecord
		}
		if len(current.InitialProcedures) != 0 {
			anchors, e := store.Read(ctx, due.Identity, current.InitialProcedures...)
			if e != nil {
				return e
			}
			if anchors.Revision != due.Revision || anchors.Timer != due.Assignment {
				return rediscore.ErrConflict
			}
			for _, field := range current.InitialProcedures {
				anchor, e := readPrivateProcedureRecord(anchors.Records[field])
				if e != nil || anchor.CommandResult == resultField {
					return errNativeRecord
				}
			}
		}
	}
	var changes []rediscore.Change
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if initial && procedure.CommandResult == resultField && result.Error != "" {
		result.Error = ""
		encoded, e := encodeNativeResult(result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: resultField, Value: encoded})
	}
	if !initial || procedure.CommandResult != resultField {
		changes = append(changes, rediscore.Change{Field: resultField, Delete: true})
		if noninitial && procedure.CommandResult == resultField {
			procedure.CommandResult = ""
			encoded, e := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
			if e != nil {
				return e
			}
			changes = append(changes, rediscore.Change{Field: procedureField, Value: encoded})
			if procedure.Native.Phase == string(portable.Terminated) && len(procedure.Outputs) == 0 {
				timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + id, Due: time.UnixMilli(due.DeadlineMS)})
			}
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
