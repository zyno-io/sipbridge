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
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// redisRegisterDueResult reports an atomically staged physical output. The
// normal ready-output wake/peek sends it; this is not a retained callback.
type redisRegisterDueResult struct{ OutputReady bool }

// dispatchRedisRegisterDue executes one fenced REGISTER semantic duty. It
// never acquires an application lease or starts a socket write.
func dispatchRedisRegisterDue(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, key []byte, contact string, deadline time.Time) (redisRegisterDueResult, error) {
	if due == nil {
		return redisRegisterDueResult{}, errors.New("REGISTER duty required")
	}
	switch {
	case strings.HasPrefix(due.Member, "native-") && strings.HasSuffix(due.Member, "-J"):
		return redisRegisterDueResult{}, fireRedisRegisterTimerJ(ctx, store, due, deadline)
	case strings.HasPrefix(due.Member, "out-"):
		return redisRegisterDueResult{}, dispatchRedisRegisterOutputCutoff(ctx, store, due, deadline)
	default:
		return redisRegisterDueResult{}, errors.New("unsupported REGISTER duty")
	}
}

// dispatchRedisRegisterOutputCutoff is an actual recovered consumer of one
// exact physical instruction. It does no socket I/O and does not infer an old
// dispatcher acknowledgement from elapsed time.
func dispatchRedisRegisterOutputCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || !strings.HasPrefix(due.Member, "out-") || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errors.New("invalid REGISTER output duty")
	}
	field := "r:" + strings.TrimPrefix(due.Member, "out-")
	if !nativeMessageField(field) || field == "" {
		return errNativeRecord
	}
	selected, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", field)
	if err != nil {
		return err
	}
	if selected.Identity != due.Identity || selected.View.Revision != due.Revision || selected.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	output, err := decodeNativeOutput(selected.View.Records[field])
	if err != nil {
		return err
	}
	if current.Kind != pb.ContextSnapshot_TRANSACTION || !proto.Equal(current.Context, output.Write.Effect.Context) || field != "r:output-"+strconv.FormatUint(current.CommandSequence, 10) || !nativeRegisterOrRefusalOutput(current, output) || output.Status < 200 || output.Status > 699 {
		return errors.New("invalid REGISTER cutoff metadata")
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	inspection, err := store.InspectOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrAbsent) {
		// Physical absence is not a delivery proof. Only a separately persisted
		// native marker may authorize retirement.
		return yieldRedisRegisterOutput(ctx, store, due, output, field, deadline)
	}
	if err != nil {
		return err
	}
	if inspection.MetadataField != field {
		return rediscore.ErrConflict
	}
	if output.Write.Permit.Authorization.DeadlineMs == 0 || output.Write.Permit.Authorization.DeadlineMs > (1<<53-1)-uint64(rediscore.OutputObsoleteGrace/time.Millisecond) {
		return errNativeRecord
	}
	original := int64(output.Write.Permit.Authorization.DeadlineMs)
	if inspection.State == rediscore.OutputUnknown && due.DeadlineMS == original+int64(rediscore.OutputObsoleteGrace/time.Millisecond) {
		return obsoleteRedisRegisterUnknown(ctx, store, due, deadline)
	}
	phase := inspection.State
	if due.DeadlineMS == original {
		phase, err = deferRedisRegisterUnknown(ctx, store, due, deadline)
		if err != nil {
			return err
		}
		if phase == rediscore.OutputUnknown {
			return nil
		}
	} else if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten {
		return errors.New("REGISTER cutoff lacks exact physical state")
	}
	if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten {
		return errors.New("REGISTER cutoff did not produce definite proof")
	}
	outcome := pb.DeliveryResult_NOT_WRITTEN
	if phase == rediscore.OutputWritten {
		outcome = pb.DeliveryResult_WRITTEN
	}
	if err = completeRedisRegisterOutput(ctx, store, output.Write, &pb.DeliveryResult{Effect: output.Write.Effect, Outcome: outcome}); err != nil {
		return err
	}
	// This call is the acknowledgement of this recovered consumption.
	if err = acknowledgeRedisRegisterOutput(ctx, store, output.Write, deadline); err != nil {
		return err
	}
	return yieldRedisRegisterOutput(ctx, store, due, output, field, deadline)
}

// yieldRedisRegisterOutput either completes a marked retirement or transfers
// the indexed duty to the exact Timer J / result duty which will re-drive it.
func yieldRedisRegisterOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, output *nativepb.Output, field string, deadline time.Time) error {
	if err := retryRedisRegisterRetirement(ctx, store, due.Identity.Context, due.Identity.Incarnation, deadline); err != nil {
		return err
	}
	selected, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", output.Procedure)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[output.Procedure])
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return err
	}
	_, proofErr := store.InspectOutput(ctx, ref)
	if proofErr != nil && !errors.Is(proofErr, rediscore.ErrAbsent) {
		return proofErr
	}
	if errors.Is(proofErr, rediscore.ErrAbsent) && !procedure.Metadata.FinalResponseWrittenAcknowledged && !current.Ended {
		return errors.New("REGISTER output proof absence is not a completion marker")
	}
	if selected.View.Timer.Worker == "" || selected.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	timer := procedure.Native.Timers[portable.TimerJ]
	change := rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(field, "r:")}
	if timer.Active && procedure.Native.Phase != portable.Terminated {
		change.Due = time.UnixMilli(timer.At)
	} else {
		next, e := store.PeekContextDeadline(ctx, selected.Identity, selected.View.Revision, change.Member)
		if e != nil {
			return e
		}
		if next == nil || !strings.HasPrefix(next.Member, "result-") {
			return errors.New("REGISTER retirement has no remaining re-drive duty")
		}
		change.Delete = true
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: deadline, Reserved: selected.View.Reserved, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, RequireLease: true, Changes: []rediscore.TimerChange{change}}})
	return err
}

// retryRedisRegisterRetirement re-drives only the deterministic current output
// after a real receipt duty ends. Missing output means the decision is pending.
func retryRedisRegisterRetirement(ctx context.Context, store *rediscore.Store, contextID, incarnation string, deadline time.Time) error {
	selected, err := store.ObserveContext(ctx, contextID, incarnation, "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return err
	}
	field := "r:output-" + strconv.FormatUint(current.CommandSequence, 10)
	selected, err = store.ObserveContext(ctx, contextID, incarnation, field)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	if selected.View.Records[field] == nil {
		return nil
	}
	output, err := decodeNativeOutput(selected.View.Records[field])
	if err != nil {
		return err
	}
	selected, err = store.ObserveContext(ctx, contextID, incarnation, output.Procedure)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[output.Procedure])
	if err != nil {
		return err
	}
	if procedure.Native.Phase != portable.Terminated {
		return nil
	}
	if current.Ended && !procedure.Metadata.FinalResponseWrittenAcknowledged {
		err = retireRedisRegister(ctx, store, output.Write, field, deadline, true, false)
	} else if !procedure.Metadata.FinalResponseWrittenAcknowledged {
		return nil
	} else if output.Write.Flow.Transport == "udp" {
		err = retireRedisRegisterPostJ(ctx, store, output.Write, field, deadline)
	} else {
		err = retireRedisRegisterWritten(ctx, store, output.Write, field, deadline)
	}
	if errors.Is(err, rediscore.ErrConflict) || errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	return err
}
