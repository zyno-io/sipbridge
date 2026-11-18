package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// dispatchRedisCancelOutputCutoff consumes one typed native response. A
// retained unknown loses its expired scheduling head, never its native owner.
func dispatchRedisCancelOutputCutoff(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if store == nil || due == nil || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	prefix, wirePrefix := "out-call-cancel-invite-output-", "r:call-cancel-invite-"
	own := strings.HasPrefix(due.Member, "out-call-cancel-output-")
	if own {
		prefix, wirePrefix = "out-call-cancel-output-", "r:call-cancel-"
	}
	id := strings.TrimPrefix(due.Member, prefix)
	if len(id) != 32 || due.Member != prefix+id {
		return errNativeRecord
	}
	outputField := "r:" + strings.TrimPrefix(due.Member, "out-")
	responseField, physicalField := wirePrefix+"final-"+id, wirePrefix+"wire-"+id
	observed, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", outputField)
	if err != nil {
		return err
	}
	if observed.Identity != due.Identity || observed.View.Revision != due.Revision || observed.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	metadataBytes := observed.View.Records[outputField]
	output, err := decodeNativeOutput(metadataBytes)
	if err != nil || output.Write.Permit.Authorization.DeadlineMs != uint64(due.DeadlineMS) || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_RESPONSE {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		if proof.MetadataField != outputField || !bytes.Equal(proof.Metadata, metadataBytes) {
			return rediscore.ErrConflict
		}
		selected, e := inspectRedisCancelOutput(ctx, store, output.Write, &nativeOutputCapture{ref: ref, proof: proof, metadata: output})
		if e != nil {
			return e
		}
		if selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment || selected.own != own {
			return rediscore.ErrConflict
		}
		phase, e := store.ExpireOutput(ctx, due, ref)
		if e != nil {
			return e
		}
		if phase == rediscore.OutputUnknown {
			_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
				Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
			return e
		}
		if phase != rediscore.OutputWritten && phase != rediscore.OutputNotWritten {
			return errNativeRecord
		}
		if e = settleRedisCancelOutput(ctx, store, output.Write, nil, due.Assignment, deadline); e != nil {
			return e
		}
		if e = acknowledgeRedisCancelOutput(ctx, store, output.Write, deadline); e != nil {
			return e
		}
	} else if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	// Settlement and actual consumption may each advance the revision. Select
	// the unchanged output again before touching its precise native ownership.
	currentView, err := store.ObserveContext(ctx, due.Identity.Context, due.Identity.Incarnation, "r:context", output.Procedure, outputField, responseField, physicalField)
	if err != nil {
		return err
	}
	if currentView.Identity != due.Identity || currentView.View.Timer != due.Assignment || !bytes.Equal(currentView.View.Records[outputField], metadataBytes) {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(currentView.View.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := readPrivateProcedureRecord(currentView.View.Records[output.Procedure])
	if err != nil {
		return err
	}
	if current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !proto.Equal(current.Context, output.Write.Effect.Context) || !proto.Equal(procedure.Flow, output.Write.Flow) || procedure.Native.Version != portable.Version || output.Dialog != "" || output.Result != "" || output.Operation != "" {
		return errNativeRecord
	}
	index, found := slices.BinarySearch(procedure.Outputs, outputField)
	if !found {
		return errNativeRecord
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	changes := []rediscore.Change{{Field: outputField, Delete: true}}
	var selectors []rediscore.SelectorChange
	if own {
		procedureIndex, associated := slices.BinarySearch(current.NoninitialProcedures, output.Procedure)
		if !associated || output.Procedure != "r:call-cancel-"+id || procedure.RequestId != id || procedure.Native.Family != string(portable.NonInviteServer) || procedure.Origin != "r:call-cancel-origin-"+id || procedure.LastResponse != responseField || output.Method != "CANCEL" || output.Status != 200 && output.Status != 481 || !procedure.OutputConsumed || len(procedure.Outputs) != 1 {
			return errNativeRecord
		}
		if procedure.Native.Phase == string(portable.Completed) && !procedure.Native.Reliable {
			if procedure.Native.Timers == nil {
				return errNativeRecord
			}
			j := procedure.Native.Timers.Values[string(portable.TimerJ)]
			if j == nil || !j.Active || j.Generation == 0 || j.At <= due.CheckedAtMS {
				// The separately indexed semantic J must terminate the native
				// transaction before this physical owner can be retired.
				return rediscore.ErrConflict
			}
			_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: currentView.View.Revision, Deadline: deadline, SpendReservation: true,
				Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(j.At)}}}})
			return err
		}
		if procedure.Native.Phase != string(portable.Terminated) {
			return errNativeRecord
		}
		if procedure.Native.Timers != nil {
			for _, timer := range procedure.Native.Timers.Values {
				if timer == nil || timer.Active {
					return errNativeRecord
				}
			}
		}
		if procedure.Native.Key == "" {
			return errNativeRecord
		}
		originView, e := store.Read(ctx, due.Identity, procedure.Origin)
		if e != nil {
			return e
		}
		if originView.Revision != currentView.View.Revision || originView.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		cancel, e := selectedmessage.DecodeOpaque(originView.Records[procedure.Origin])
		if e != nil {
			return e
		}
		inviteKey, e := cancel.CancelServerTxKey()
		if e != nil {
			return e
		}
		parent, e := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, inviteKey))
		if e != nil {
			return e
		}
		if parent.Identity != due.Identity || parent.View.Revision != currentView.View.Revision || parent.View.Timer != due.Assignment {
			return rediscore.ErrConflict
		}
		owner, e := readPrivateProcedureRecord(parent.View.Records[parent.Record])
		if e != nil || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.InviteServer) || owner.Native.Key != inviteKey || !slices.Contains(current.InitialProcedures, parent.Record) && !slices.Contains(current.NoninitialProcedures, parent.Record) {
			return errNativeRecord
		}
		if slices.Contains(current.NoninitialProcedures, parent.Record) && owner.Native.Phase == string(portable.Terminated) && len(owner.Events) == 0 && len(owner.Outputs) == 0 {
			timers = append(timers, rediscore.TimerChange{Member: "call-proc-" + owner.RequestId, Due: time.UnixMilli(due.CheckedAtMS)})
		}
		selectors = []rediscore.SelectorChange{{Selector: nativeSelector(rediscore.SelectorServerTransaction, procedure.Native.Key), Record: output.Procedure, Delete: true}}
		current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, procedureIndex, procedureIndex+1)
		data, e := encodeNativeContext(current)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:context", Value: data}, rediscore.Change{Field: output.Procedure, Delete: true}, rediscore.Change{Field: procedure.Origin, Delete: true}, rediscore.Change{Field: responseField, Delete: true})
		timers = append(timers, rediscore.TimerChange{Member: "call-cancel-tx-" + id + "-J", Delete: true})
	} else {
		if !slices.Contains(current.InitialProcedures, output.Procedure) || procedure.Native.Family != string(portable.InviteServer) || procedure.LastResponse != responseField || procedure.Origin != "r:origin-"+procedure.RequestId || output.Procedure != "r:procedure-"+procedure.RequestId || output.Method != "INVITE" || output.Status != 487 || procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Confirmed) && procedure.Native.Phase != string(portable.Terminated) {
			return errNativeRecord
		}
		procedure.Outputs = slices.Delete(procedure.Outputs, index, index+1)
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: output.Procedure, Value: data})
	}
	if len(currentView.View.Records[physicalField]) != 0 {
		changes = append(changes, rediscore.Change{Field: physicalField, Delete: true})
	}
	if current.CallRetireAtMs > 0 {
		timers = append(timers, rediscore.TimerChange{Member: "call-retire", Due: time.UnixMilli(current.CallRetireAtMs)})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: currentView.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: selectors,
		Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
