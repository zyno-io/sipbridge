package engine

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
)

// deferRedisRegisterUnknown handles an original physical cutoff. It records a
// started attempt as UNKNOWN and moves its one indexed duty to the fixed
// original deadline plus two minutes. No command outcome is changed.
func deferRedisRegisterUnknown(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) (rediscore.OutputState, error) {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "out-") || !time.Now().Before(deadline) {
		return "", errors.New("invalid REGISTER output cutoff")
	}
	field := "r:" + strings.TrimPrefix(due.Member, "out-")
	if !nativeMessageField(field) || field == "" {
		return "", errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", field)
	if err != nil {
		return "", err
	}
	if selected.Identity != due.Identity || selected.View.Revision != due.Revision || selected.View.Timer != due.Assignment {
		return "", rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return "", err
	}
	output, err := decodeNativeOutput(selected.View.Records[field])
	if err != nil {
		return "", err
	}
	if current.Kind != pb.ContextSnapshot_TRANSACTION || !proto.Equal(current.Context, output.Write.Effect.Context) || !nativeRegisterOrRefusalOutput(current, output) || output.Status < 200 || output.Status > 699 || output.Write.Permit.Authorization.DeadlineMs != uint64(due.DeadlineMS) {
		return "", errors.New("REGISTER output cutoff evidence changed")
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return "", err
	}
	phase, err := store.ExpireOutput(ctx, due, ref)
	if err != nil {
		return "", err
	}
	if phase != rediscore.OutputUnknown {
		return phase, nil
	}
	if due.DeadlineMS > (1<<53-1)-int64((2*time.Minute)/time.Millisecond) {
		return "", errNativeRecord
	}
	obsolete := time.UnixMilli(due.DeadlineMS).Add(2 * time.Minute)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Reserved: selected.View.Reserved, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: obsolete}}}})
	if err != nil {
		return "", err
	}
	return phase, nil
}

// obsoleteRedisRegisterUnknown closes only the documented REGISTER-only
// uncertainty window. The persisted ended marker precedes release of UNKNOWN
// proof, so a lost reply is reconciled through the same selected records.
func obsoleteRedisRegisterUnknown(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "out-") || !time.Now().Before(deadline) || due.CheckedAtMS < due.DeadlineMS {
		return errors.New("invalid REGISTER obsolete cutoff")
	}
	field := "r:" + strings.TrimPrefix(due.Member, "out-")
	if !nativeMessageField(field) || field == "" {
		return errNativeRecord
	}
	initial, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", field)
	if err != nil {
		return err
	}
	if initial.Identity != due.Identity || initial.View.Revision != due.Revision || initial.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(initial.View.Records["r:context"])
	if err != nil {
		return err
	}
	output, err := decodeNativeOutput(initial.View.Records[field])
	if err != nil {
		return err
	}
	original := output.Write.Permit.Authorization.DeadlineMs
	if original > (1<<53-1)-uint64(rediscore.OutputObsoleteGrace/time.Millisecond) || int64(original)+int64(rediscore.OutputObsoleteGrace/time.Millisecond) != due.DeadlineMS || field != "r:output-"+strconv.FormatUint(current.CommandSequence, 10) || current.Kind != pb.ContextSnapshot_TRANSACTION || !proto.Equal(current.Context, output.Write.Effect.Context) || !nativeRegisterOrRefusalOutput(current, output) || output.Status < 200 || !strings.HasPrefix(output.Procedure, "r:procedure-") {
		return errors.New("REGISTER obsolete identity changed")
	}
	next, err := store.PeekContextDeadline(ctx, due.Identity, due.Revision, due.Member)
	if err != nil {
		return err
	}
	if next != nil {
		if !strings.HasPrefix(next.Member, "result-") {
			return errors.New("REGISTER obsolete protocol duty remains")
		}
		// The result expiry atomically reinstates this same fixed cutoff.
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Reserved: initial.View.Reserved, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return err
	}
	requestID := strings.TrimPrefix(output.Procedure, "r:procedure-")
	if !state.ValidReference(requestID) || len(requestID) > 180 {
		return errNativeRecord
	}
	originField, responseField := "r:origin-"+requestID, "r:response-"+requestID
	fields := []string{output.Procedure, originField, responseField}
	resultField := ""
	if current.CommandSequence != 0 {
		resultField = "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
		fields = append(fields, resultField)
	}
	selected, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, fields...)
	if err != nil {
		return err
	}
	if selected.Identity != due.Identity || selected.View.Revision != due.Revision || selected.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[output.Procedure])
	if err != nil {
		return err
	}
	if procedure.Metadata.RequestId != requestID || procedure.Metadata.DecisionPending || procedure.Metadata.FinalResponseWrittenAcknowledged || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || procedure.Native.Phase != portable.Terminated {
		return errors.New("REGISTER obsolete procedure not terminal")
	}
	if output.Result != "" && output.Result != resultField {
		return errors.New("REGISTER obsolete result changed")
	}
	if resultField != "" {
		result, e := decodeNativeResult(selected.View.Records[resultField])
		if e != nil || result.Sequence != current.CommandSequence || result.CutoffMs > due.CheckedAtMS || result.CutoffMs > procedure.Metadata.PresentDeadlineMs || (output.Result != "" && (result.Certainty != pb.OperationResult_ACCEPTED || result.SipStatus != output.Status)) {
			return errors.New("REGISTER obsolete waiter remains")
		}
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	if tx.LastResponse == nil {
		return errNativeRecord
	}
	origin, e1 := tx.Origin.Facts()
	final, e2 := tx.LastResponse.Facts()
	if e1 != nil || e2 != nil || !origin.IsRequest || string(origin.Method) != output.Method || final.IsRequest || final.Status != int(output.Status) || !portable.MatchResponse(tx.Origin, *tx.LastResponse) {
		return errors.New("REGISTER obsolete message mismatch")
	}
	for _, timer := range tx.Timers {
		if timer.Active {
			return errors.New("REGISTER obsolete native timer remains")
		}
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	inspection, inspectErr := store.InspectOutput(ctx, ref)
	if inspectErr != nil && !(errors.Is(inspectErr, rediscore.ErrAbsent) && current.Ended) {
		return inspectErr
	}
	if inspectErr == nil && (inspection.State != rediscore.OutputUnknown || inspection.MetadataField != field) {
		return errors.New("REGISTER obsolete output is not unknown")
	}
	if !current.Ended {
		if current.Revision >= 1<<53-1 {
			return errNativeRecord
		}
		current.Ended = true
		current.Revision++
		encoded, e := encodeNativeContext(current)
		if e != nil {
			return e
		}
		revision, e := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: "r:context", Value: encoded}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(due.DeadlineMS)}}}})
		if e != nil {
			return e
		}
		copyDue := *due
		copyDue.Revision = revision
		due = &copyDue
	}
	if inspectErr == nil {
		if err = store.ForgetObsoleteOutput(ctx, due, ref, true); err != nil {
			return err
		}
	}
	return retireRedisRegister(ctx, store, output.Write, field, deadline, true, false)
}
