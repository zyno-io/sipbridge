package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

// forgetRedisUnboundCancelledSetup closes only an independently terminated,
// unbound initial setup. It never claims an uncertain output was consumed.
func forgetRedisUnboundCancelledSetup(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, current *nativepb.Context, deadline time.Time) (bool, error) {
	if store == nil || due == nil || current == nil || due.Member != "call-retire" || !time.Now().Before(deadline) {
		return false, errNativeRecord
	}
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Ended || current.Answered || current.Kind != pb.ContextSnapshot_UAS || len(current.Dialogs) != 0 || current.CommandSequence != 0 || due.Identity.Owner != "" || due.Identity.Term != "" || len(current.InitialProcedures) != 1 || len(current.NoninitialProcedures) > 1 || current.CallRetireAtMs <= 0 || due.CheckedAtMS < current.CallRetireAtMs {
		return false, nil
	}
	fields := append([]string(nil), current.InitialProcedures...)
	fields = append(fields, current.NoninitialProcedures...)
	view, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return false, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	anchor, err := readPrivateProcedureRecord(view.Records[current.InitialProcedures[0]])
	if err != nil {
		return false, err
	}
	const finalPrefix = "r:call-cancel-invite-final-"
	id := strings.TrimPrefix(anchor.LastResponse, finalPrefix)
	requestID, parseErr := uuid.Parse(anchor.RequestId)
	if len(id) != 32 || parseErr != nil || requestID.String() != anchor.RequestId || anchor.LastResponse != finalPrefix+id || anchor.Native.Family != string(portable.InviteServer) || !anchor.Native.Cancelled || !anchor.Direct || anchor.Origin != "r:origin-"+anchor.RequestId || current.InitialProcedures[0] != "r:procedure-"+anchor.RequestId {
		return false, errNativeRecord
	}
	procedures := []*nativepb.Procedure{anchor}
	if len(current.NoninitialProcedures) != 0 {
		if current.NoninitialProcedures[0] != "r:call-cancel-"+id {
			return false, errNativeRecord
		}
		cancel, e := readPrivateProcedureRecord(view.Records[current.NoninitialProcedures[0]])
		if e != nil || cancel.Native.Family != string(portable.NonInviteServer) || cancel.RequestId != id || cancel.Origin != "r:call-cancel-origin-"+id || cancel.LastResponse != "r:call-cancel-final-"+id {
			return false, errNativeRecord
		}
		procedures = append(procedures, cancel)
	}
	var outputs []string
	for _, p := range procedures {
		if p.Native.Version != portable.Version || p.Native.Phase != string(portable.Terminated) || p.DecisionPending || len(p.Events) != 0 || len(p.Outputs) > 1 || p.Native.Timers != nil && !nativeDeadlinesInactive(p.Native.Timers.Values) {
			return false, nil
		}
		outputs = append(outputs, p.Outputs...)
	}
	if len(outputs) == 0 {
		return false, nil
	}
	selected, err := store.Read(ctx, due.Identity, outputs...)
	if err != nil {
		return false, err
	}
	if selected.Revision != due.Revision || selected.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	type retained struct {
		field  string
		output *nativepb.Output
		owner  *nativepb.Procedure
		own    bool
		ref    rediscore.OutputRef
		absent bool
	}
	var candidates []retained
	for _, field := range outputs {
		output, e := decodeNativeOutput(selected.Records[field])
		if e != nil || !proto.Equal(output.Write.Effect.Context, current.Context) || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE || output.Result != "" || output.Operation != "" || output.Dialog != "" {
			return false, errNativeRecord
		}
		owner, own := anchor, false
		if output.Procedure != current.InitialProcedures[0] {
			if len(procedures) != 2 || output.Procedure != current.NoninitialProcedures[0] || output.Method != "CANCEL" || output.Status != 200 && output.Status != 481 || field != "r:call-cancel-output-"+id {
				return false, errNativeRecord
			}
			owner, own = procedures[1], true
		} else if output.Method != "INVITE" || output.Status != 487 || field != "r:call-cancel-invite-output-"+id {
			return false, errNativeRecord
		}
		if !slices.Contains(owner.Outputs, field) || !proto.Equal(output.Write.Flow, owner.Flow) || owner.Native.Reliable != (output.Write.Flow.Transport != "udp") {
			return false, errNativeRecord
		}
		ref, e := nativeOutputRef(output.Write)
		if e != nil || ref.RequireApplication {
			return false, errNativeRecord
		}
		proof, e := store.InspectOutput(ctx, ref)
		if errors.Is(e, rediscore.ErrAbsent) {
			if own && owner.OutputConsumed {
				_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "out-" + field[2:], Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))}, {Member: due.Member, Delete: true}}}})
				return e == nil, e
			}
			candidates = append(candidates, retained{field: field, output: output, owner: owner, own: own, ref: ref, absent: true})
			continue
		}
		if e != nil {
			return false, e
		}
		wirePrefix := "r:call-cancel-invite-"
		if own {
			wirePrefix = "r:call-cancel-"
		}
		if proof.MetadataField != field || proof.MessageField != wirePrefix+"final-"+id && proof.MessageField != wirePrefix+"wire-"+id || !bytes.Equal(proof.Metadata, selected.Records[field]) || proof.DeadlineMS != int64(output.Write.Permit.Authorization.DeadlineMs) {
			return false, rediscore.ErrConflict
		}
		if proof.State == rediscore.OutputWritten || proof.State == rediscore.OutputNotWritten {
			if e = settleRedisCancelOutput(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output}, due.Assignment, deadline); e != nil {
				return false, e
			}
			if e = acknowledgeRedisCancelOutput(ctx, store, output.Write, deadline); e != nil {
				return false, e
			}
			return true, nil
		}
		if proof.State != rediscore.OutputUnknown || proof.Acknowledged || proof.Settled {
			return false, errNativeRecord
		}
		candidates = append(candidates, retained{field: field, output: output, owner: owner, own: own, ref: ref})
	}
	chosen := candidates[0]
	physical := chosen.output.Write.Permit.Authorization.DeadlineMs
	if physical == 0 || physical > uint64((1<<53-1)-rediscore.OutputObsoleteGrace.Milliseconds()) {
		return false, errNativeRecord
	}
	cutoff := max(current.CallRetireAtMs, int64(physical)+rediscore.OutputObsoleteGrace.Milliseconds())
	if due.DeadlineMS != cutoff {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(cutoff)}}}})
		return err == nil, err
	}
	if due.CheckedAtMS < cutoff {
		return false, nil
	}
	if !chosen.absent {
		err = store.ForgetObsoleteCallOutput(ctx, due, chosen.ref, current.CallRetireAtMs, true)
		return err == nil, err
	}
	// Absence under the same immutable terminal witness is historical UNKNOWN,
	// not a write or consumer acknowledgement. Keep the terminal head through
	// this exact owner removal and let ordinary setup retirement finish later.
	changes := []rediscore.Change{{Field: chosen.field, Delete: true}}
	var selectors []rediscore.SelectorChange
	physicalField := "r:call-cancel-invite-wire-" + id
	if chosen.own {
		physicalField = "r:call-cancel-wire-" + id
		if chosen.owner.Native.Key == "" || len(chosen.owner.Outputs) != 1 {
			return false, errNativeRecord
		}
		current.NoninitialProcedures = nil
		contextBytes, e := encodeNativeContext(current)
		if e != nil {
			return false, e
		}
		cancelField := "r:call-cancel-" + id
		changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes}, rediscore.Change{Field: cancelField, Delete: true}, rediscore.Change{Field: chosen.owner.Origin, Delete: true}, rediscore.Change{Field: chosen.owner.LastResponse, Delete: true})
		selectors = append(selectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorServerTransaction, chosen.owner.Native.Key), Record: cancelField, Delete: true})
	} else {
		index, ok := slices.BinarySearch(anchor.Outputs, chosen.field)
		if !ok {
			return false, errNativeRecord
		}
		anchor.Outputs = slices.Delete(anchor.Outputs, index, index+1)
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, anchor)
		if e != nil {
			return false, e
		}
		changes = append(changes, rediscore.Change{Field: current.InitialProcedures[0], Value: data})
	}
	extra, err := store.Read(ctx, due.Identity, physicalField)
	if err != nil {
		return false, err
	}
	if extra.Revision != due.Revision || extra.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	if len(extra.Records[physicalField]) != 0 {
		changes = append(changes, rediscore.Change{Field: physicalField, Delete: true})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: selectors, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(current.CallRetireAtMs)}}}})
	return err == nil, err
}
