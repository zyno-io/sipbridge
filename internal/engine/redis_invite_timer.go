package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisInviteTrying retains the automatic 100 while unbound, or stages it
// for the already accepted owner under the same transaction publication.
func fireRedisInviteTrying(ctx context.Context, store *rediscore.Store, key []byte, due *rediscore.DueContext, deadline time.Time) error {
	suffix := "-" + string(portable.TimerTrying)
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "call-tx-") || !strings.HasSuffix(due.Member, suffix) || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	requestID := strings.TrimSuffix(strings.TrimPrefix(due.Member, "call-tx-"), suffix)
	id, err := uuid.Parse(requestID)
	if err != nil || id.String() != requestID {
		return errNativeRecord
	}
	procedureField, originField, responseField := "r:procedure-"+requestID, "r:origin-"+requestID, "r:response-"+requestID+"-100"
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
	if slices.Contains(current.NoninitialProcedures, procedureField) {
		return fireRedisDialogInviteTrying(ctx, store, key, due, requestID, deadline)
	}
	timer := procedure.Native.Timers[portable.TimerTrying]
	unbound := nativeHoldUnboundTrying(procedure.Metadata.Direct, procedure.Metadata.DecisionPending, due.Identity.Owner == "", portable.TimerTrying)
	bound := procedure.Metadata.Direct && !procedure.Metadata.DecisionPending && due.Identity.Owner != "" && current.ApplicationLeaseTerm == due.Identity.Term
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS || procedure.Native.Family != portable.InviteServer || procedure.Metadata.RequestId != requestID || !unbound && !bound || procedure.Metadata.Emissions != 0 || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != "" || len(view.Records[responseField]) != 0 || !timer.Active || timer.At != due.DeadlineMS || timer.Generation == 0 {
		return errors.New("unbound INVITE Trying evidence changed")
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, false, false)
	if err != nil {
		return err
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Now: due.CheckedAtMS, Timer: portable.TimerTrying, Generation: timer.Generation})
	if err != nil {
		return err
	}
	if len(effects) != 1 || effects[0].Kind != portable.Send || effects[0].Message == nil || next.LastResponse == nil || !effects[0].Message.EqualWire(*next.LastResponse) {
		return errNativeRecord
	}
	facts, err := next.LastResponse.Facts()
	if err != nil || facts.IsRequest || facts.CSeqMethod != sip.INVITE || facts.Status != 100 {
		return errNativeRecord
	}
	refs := procedure.Refs
	refs.LastResponse = responseField
	var changes []rediscore.Change
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	var outputs []rediscore.OutputIntent
	shared := false
	if bound {
		snapshot, e := nativeProjectSnapshot(current, nil, nil, nil)
		if e != nil {
			return e
		}
		until := minTime(deadline, time.Now().Add(2*time.Second))
		outputField := "r:output-" + requestID + "-100"
		intent, outputChanges, sameWire, e := prepareRedisCallResponseOutput("INVITE", 100, snapshot, procedure.Metadata.Flow, key, *next.LastResponse, procedureField, "", responseField, "r:physical-"+requestID+"-100", outputField, until, false)
		if e != nil {
			return e
		}
		procedure.Metadata.Emissions++
		procedure.Metadata.ProvisionalEmissions++
		procedure.Metadata.Outputs = append(procedure.Metadata.Outputs, outputField)
		slices.Sort(procedure.Metadata.Outputs)
		outputs = append(outputs, intent)
		shared = sameWire
		changes = append(changes, outputChanges...)
		timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: until})
	}
	if !shared {
		response, e := selectedmessage.Marshal(*next.LastResponse)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: response})
	}
	encoded, err := encodeNativeProcedure(next, refs, procedure.Metadata)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: encoded})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: outputs, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
