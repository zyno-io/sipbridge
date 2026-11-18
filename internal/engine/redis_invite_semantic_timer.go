package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisInviteServerTimer executes only H/I/L semantic completion. Timer G
// remains a local derivative, and L cannot discharge the separate dialog ACK.
func fireRedisInviteServerTimer(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "call-tx-") || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	value := strings.TrimPrefix(due.Member, "call-tx-")
	if len(value) < 38 || value[36] != '-' {
		return errNativeRecord
	}
	id, suffix := value[:36], value[37:]
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return errNativeRecord
	}
	timerKind := portable.Timer(suffix)
	if timerKind != portable.TimerH && timerKind != portable.TimerI && timerKind != portable.TimerL {
		return errNativeRecord
	}
	procedureField := "r:procedure-" + id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField)
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
	procedure, err := decodeNativeProcedure(view.Records[procedureField])
	if err != nil {
		return err
	}
	timer := procedure.Native.Timers[timerKind]
	initial := current.Kind == pb.ContextSnapshot_UAS && slices.Contains(current.InitialProcedures, procedureField)
	noninitial := (current.Kind == pb.ContextSnapshot_UAS || current.Kind == pb.ContextSnapshot_UAC) && slices.Contains(current.NoninitialProcedures, procedureField)
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || !initial && !noninitial || procedure.Native.Family != portable.InviteServer || procedure.Metadata.RequestId != id || procedure.Refs.Origin != "r:origin-"+id || !timer.Active || timer.Generation == 0 || timer.At != due.DeadlineMS {
		return errNativeRecord
	}
	fields := []string{procedure.Refs.Origin}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	if procedure.Refs.ACK != "" {
		fields = append(fields, procedure.Refs.ACK)
	}
	messages, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if messages.Revision != view.Revision || messages.Timer != view.Timer {
		return rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields))
	if err != nil {
		return err
	}
	transaction, err := procedure.attach(&nativeSelectedMessages{records: messages.Records, memo: memo, provenance: "admitted"}, true, true, true)
	if err != nil {
		return err
	}
	facts, err := transaction.Origin.Facts()
	if err != nil || initial && facts.ToTag != "" && !nativeInitialRefusalInviteProcedure(current, due.Identity, procedureField) || noninitial && facts.ToTag == "" {
		return errNativeRecord
	}
	next, effects, err := portable.StepTransaction(transaction, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: timerKind, Generation: timer.Generation})
	if err != nil {
		return err
	}
	if next.Phase != portable.Terminated {
		return errors.New("INVITE semantic timer did not terminate")
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Ended:
		case portable.CancelTimer:
			if effect.Timer != portable.TimerG && effect.Timer != timerKind {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
			}
		default:
			return errors.New("unexpected INVITE semantic timer effect")
		}
	}
	if noninitial && len(procedure.Metadata.Outputs) == 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + id, Due: time.UnixMilli(due.DeadlineMS)})
	}
	encoded, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
