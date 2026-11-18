package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// A final publication re-arms only actual earlier 1xx attempts. They may have
// lost their original head after UNKNOWN; no periodic retirement scan is needed.
func planRedisProvisionalOutputRedrive(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, procedureField string, procedure *nativepb.Procedure) ([]rediscore.TimerChange, error) {
	if len(procedure.Outputs) == 0 {
		return nil, nil
	}
	view, err := store.Read(ctx, selected.Identity, procedure.Outputs...)
	if err != nil {
		return nil, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return nil, rediscore.ErrConflict
	}
	var timers []rediscore.TimerChange
	for _, field := range procedure.Outputs {
		output, err := decodeNativeOutput(view.Records[field])
		if err != nil {
			return nil, err
		}
		if output.Procedure != procedureField || output.Method != "INVITE" {
			return nil, errNativeRecord
		}
		if output.Status >= 100 && output.Status < 200 {
			if _, err = nativeReadyInviteTrying(output, field); err != nil {
				return nil, err
			}
			timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(field, "r:"), Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))})
		}
	}
	return timers, nil
}

func redisProvisionalSuperseded(procedure *nativepb.Procedure, output *nativepb.Output, response string) bool {
	if output.Status < 100 || output.Status >= 200 || output.Dialog != "" || procedure.LastResponse == "" || procedure.LastResponse == response {
		return false
	}
	switch portable.Phase(procedure.Native.Phase) {
	case portable.Accepted, portable.Completed, portable.Confirmed, portable.Terminated:
		return true
	}
	return false
}

// At the fixed grace head, forget only superseded initial-UAS provisional
// evidence. The exact native metadata and head remain until ordinary absence
// cleanup commits, including when the proof-release reply is lost.
func forgetRedisProvisionalOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, selected *rediscore.ObservedContext, current *nativepb.Context, procedure *nativepb.Procedure, output *nativepb.Output, outputField, responseField string, deadline time.Time) (bool, error) {
	physical := int64(output.Write.Permit.Authorization.DeadlineMs)
	if output.Status < 100 || output.Status >= 200 || physical <= 0 || physical > (1<<53-1)-rediscore.OutputObsoleteGrace.Milliseconds() || due.DeadlineMS != physical+rediscore.OutputObsoleteGrace.Milliseconds() || due.CheckedAtMS < due.DeadlineMS || current.Kind != pb.ContextSnapshot_UAS || !slices.Contains(current.InitialProcedures, output.Procedure) || !slices.Contains(procedure.Outputs, outputField) || output.Dialog != "" {
		return false, errNativeRecord
	}
	if !redisProvisionalSuperseded(procedure, output, responseField) {
		// The final publication will re-arm this exact owner if it happens
		// later. This is a conservative early wake, never a polling duty.
		_, err := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
			Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return false, err
	}
	fields := []string{procedure.LastResponse}
	if output.Result != "" {
		fields = append(fields, output.Result)
	}
	view, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return false, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return false, err
	}
	messages := nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	final, err := messages.get(procedure.LastResponse)
	if err != nil {
		return false, err
	}
	facts, err := final.Facts()
	if err != nil || facts.IsRequest || facts.CSeqMethod != "INVITE" || facts.CallID != current.CallId {
		return false, errNativeRecord
	}
	if facts.Status < 200 {
		// A transport failure can terminate a transaction with only a 1xx
		// selected. Keep that proof for ordinary terminal retirement.
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
			Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
		return false, err
	}
	var result *nativepb.Result
	if output.Result != "" {
		result, err = decodeNativeResult(view.Records[output.Result])
		if err != nil || result.Pending || result.Output != outputField || result.OperationId != output.Operation || result.Request != "" || result.Response != "" {
			return false, errNativeRecord
		}
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication {
		return false, errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrAbsent) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if proof.MetadataField != outputField || proof.DeadlineMS != physical || !bytes.Equal(proof.Metadata, selected.View.Records[outputField]) {
		return false, rediscore.ErrConflict
	}
	if proof.State != rediscore.OutputUnknown {
		// A definite late completion (or an unexpired reservation) must use
		// its ordinary consumer. Forgetting never bypasses that continuation.
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
			Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(physical)}}}})
		return false, err
	}
	if result != nil && result.Certainty != pb.OperationResult_UNKNOWN {
		return false, errNativeRecord
	}
	if err = store.ForgetObsoleteOutput(ctx, due, ref, true); err != nil {
		return false, err
	}
	return true, nil
}
