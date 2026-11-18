package engine

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
)

// fireRedisRegisterTimerJ runs the non-INVITE server's only UDP protocol
// timer. It never emits a retransmission; duplicates are separate inputs.
func fireRedisRegisterTimerJ(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "native-") || !strings.HasSuffix(due.Member, "-J") || !time.Now().Before(deadline) || due.CheckedAtMS < due.DeadlineMS {
		return errors.New("invalid REGISTER Timer J")
	}
	requestID := strings.TrimSuffix(strings.TrimPrefix(due.Member, "native-"), "-J")
	if !state.ValidReference(requestID) || len(requestID) > 180 {
		return errNativeRecord
	}
	procedureField, originField, responseField := "r:procedure-"+requestID, "r:origin-"+requestID, "r:response-"+requestID
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, responseField)
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
	timer := procedure.Native.Timers[portable.TimerJ]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_TRANSACTION || procedure.Metadata.RetireAtMs != 0 || procedure.Metadata.RequestId != requestID || procedure.Metadata.DecisionPending || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || procedure.Native.Family != portable.NonInviteServer || procedure.Native.Reliable || !timer.Active || timer.At != due.DeadlineMS || timer.Generation == 0 {
		return errors.New("REGISTER Timer J evidence changed")
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || (facts.Method != sip.REGISTER && !nativeInitialRefusalContext(current)) || tx.LastResponse == nil || !portable.MatchResponse(tx.Origin, *tx.LastResponse) {
		return errors.New("REGISTER Timer J origin changed")
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: portable.TimerJ, Generation: timer.Generation})
	if err != nil {
		return err
	}
	ended := 0
	for _, effect := range effects {
		if effect.Kind != portable.Ended {
			return errors.New("unexpected REGISTER Timer J effect")
		}
		ended++
	}
	if ended != 1 || next.Phase != portable.Terminated {
		return errors.New("REGISTER Timer J did not terminate")
	}
	metadata := proto.Clone(procedure.Metadata).(*nativepb.Procedure)
	encoded, err := encodeNativeProcedure(next, procedure.Refs, metadata)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	if metadata.FinalResponseWrittenAcknowledged {
		field := "r:output-" + strconv.FormatUint(current.CommandSequence, 10)
		selected, e := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, field)
		if e != nil {
			return e
		}
		if selected.Identity != due.Identity || selected.View.Revision != view.Revision {
			return rediscore.ErrConflict
		}
		output, e := decodeNativeOutput(selected.View.Records[field])
		if e != nil || output.Procedure != procedureField {
			return errors.New("REGISTER post-J output changed")
		}
		// Keep an indexed re-drive until the terminal publication succeeds.
		timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(field, "r:"), Due: time.UnixMilli(due.DeadlineMS)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: procedureField, Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	if err != nil {
		return err
	}
	if metadata.FinalResponseWrittenAcknowledged {
		return retryRedisRegisterRetirement(ctx, store, due.Identity.Context, due.Identity.Incarnation, deadline)
	}
	return nil
}
