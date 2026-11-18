package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/storagecodec"
)

func settleRedisStandaloneOutput(ctx context.Context, store *rediscore.Store, capture *nativeOutputCapture, executor rediscore.TimerAssignment, deadline time.Time) error {
	if capture == nil || !validReadyStandaloneOutput(capture.metadata, capture.proof.MetadataField) {
		return errNativeRecord
	}
	id := strings.TrimPrefix(capture.proof.MetadataField, "r:transaction-output-")
	s, err := selectRedisTransaction(ctx, store, capture.ref.Context, capture.ref.Incarnation, id)
	if err != nil {
		return err
	}
	if err = redisTransactionProof(s, capture); err != nil {
		return err
	}
	proof := capture.proof
	if proof.State == rediscore.OutputStarted {
		return nil
	}
	if proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten && proof.State != rediscore.OutputUnknown {
		return errNativeRecord
	}
	var changes []rediscore.Change
	if proof.State != rediscore.OutputWritten && s.result.Pending {
		s.result.Pending = false
		s.result.Certainty = pb.OperationResult_UNKNOWN
		s.result.Error = "SIP delivery unavailable or uncertain"
		value, e := encodeNativeResult(s.result)
		if e != nil {
			return e
		}
		changes = append(changes, rediscore.Change{Field: "r:result-1", Value: value})
	}
	var timers []rediscore.TimerChange
	if proof.State != rediscore.OutputUnknown && !proof.Settled {
		timers = append(timers, rediscore.TimerChange{Member: s.names.member, Due: time.UnixMilli(proof.DeadlineMS)})
	}
	if len(changes) != 0 || len(timers) != 0 {
		if err = redisTransactionCommit(ctx, store, s, changes, timers, executor, deadline, len(changes) != 0, nil); err != nil {
			return err
		}
	}
	if proof.State == rediscore.OutputUnknown || proof.Settled {
		return nil
	}
	return store.SettleOutput(ctx, capture.ref, false, true)
}

func acknowledgeRedisStandaloneOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, deadline time.Time) error {
	ref, err := nativeOutputRef(write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil {
		return err
	}
	capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}
	s, err := selectRedisTransaction(ctx, store, ref.Context, ref.Incarnation, strings.TrimPrefix(proof.MetadataField, "r:transaction-output-"))
	if err != nil {
		return err
	}
	if err = redisTransactionProof(s, capture); err != nil {
		return err
	}
	if !proof.Settled || proof.State != rediscore.OutputWritten && proof.State != rediscore.OutputNotWritten {
		return errNativeRecord
	}
	if !s.procedure.Metadata.OutputConsumed {
		record, e := readPrivateProcedureRecord(s.selected.View.Records[s.names.procedure])
		if e != nil {
			return e
		}
		record.OutputConsumed = true
		value, e := marshalNativeRecord(storagecodec.NativeProcedure, record)
		if e != nil {
			return e
		}
		if e = redisTransactionCommit(ctx, store, s, []rediscore.Change{{Field: s.names.procedure, Value: value}}, nil, s.selected.View.Timer, deadline, false, nil); e != nil {
			return e
		}
	}
	return store.SettleOutput(ctx, ref, true, false)
}

// The physical member is retained until exact native/result/output closure.
// U has one fixed grace wake; no periodic polling or control history is kept.
func retireRedisStandaloneOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	if due == nil || due.CheckedAtMS < due.DeadlineMS || !strings.HasPrefix(due.Member, "out-transaction-output-") {
		return errNativeRecord
	}
	s, err := selectRedisTransaction(ctx, store, due.Identity.Context, due.Identity.Incarnation, strings.TrimPrefix(due.Member, "out-transaction-output-"))
	if err != nil {
		return err
	}
	if s.selected.Identity != due.Identity || s.selected.View.Revision != due.Revision || s.selected.View.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	s.requireLease = true
	ref, err := nativeOutputRef(s.output.Write)
	if err != nil {
		return err
	}
	physical := int64(s.output.Write.Permit.Authorization.DeadlineMs)
	if physical <= 0 || physical > (1<<53-1)-rediscore.OutputObsoleteGrace.Milliseconds() {
		return errNativeRecord
	}
	grace := physical + rediscore.OutputObsoleteGrace.Milliseconds()
	proof, err := store.InspectOutput(ctx, ref)
	if err == nil {
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: s.output}
		if err = redisTransactionProof(s, capture); err != nil {
			return err
		}
		if proof.State == rediscore.OutputPending || proof.State == rediscore.OutputStarted {
			if due.DeadlineMS != physical {
				return rediscore.ErrConflict
			}
			if _, err = store.ExpireOutput(ctx, due, ref); err != nil {
				return err
			}
			proof, err = store.InspectOutput(ctx, ref)
			if err != nil {
				return err
			}
			capture.proof = proof
		}
		if proof.State == rediscore.OutputWritten || proof.State == rediscore.OutputNotWritten {
			if err = settleRedisStandaloneOutput(ctx, store, capture, due.Assignment, deadline); err != nil {
				return err
			}
			return acknowledgeRedisStandaloneOutput(ctx, store, s.output.Write, deadline)
		}
		if proof.State != rediscore.OutputUnknown {
			return errNativeRecord
		}
		if s.result.Pending {
			return settleRedisStandaloneOutput(ctx, store, capture, due.Assignment, deadline)
		}
		if due.CheckedAtMS < grace {
			return redisTransactionCommit(ctx, store, s, nil, []rediscore.TimerChange{{Member: s.names.member, Due: time.UnixMilli(grace)}}, due.Assignment, deadline, false, nil)
		}
		if !redisStandaloneTerminated(s) {
			return redisTransactionCommit(ctx, store, s, nil, []rediscore.TimerChange{{Member: s.names.member, Delete: true}}, due.Assignment, deadline, false, nil)
		}
		if due.CheckedAtMS < s.result.CutoffMs {
			return redisTransactionCommit(ctx, store, s, nil, []rediscore.TimerChange{{Member: s.names.member, Due: time.UnixMilli(s.result.CutoffMs)}}, due.Assignment, deadline, false, nil)
		}
		if due.DeadlineMS != grace {
			return redisTransactionCommit(ctx, store, s, nil, []rediscore.TimerChange{{Member: s.names.member, Due: time.UnixMilli(grace)}}, due.Assignment, deadline, false, nil)
		}
		return store.ForgetObsoleteOutput(ctx, due, ref, true)
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	if !s.procedure.Metadata.OutputConsumed && (due.CheckedAtMS < grace || !redisStandaloneTerminated(s)) {
		return errNativeRecord
	}
	if !redisStandaloneTerminated(s) {
		return redisTransactionCommit(ctx, store, s, nil, []rediscore.TimerChange{{Member: s.names.member, Delete: true}}, due.Assignment, deadline, false, nil)
	}
	if s.result.Pending || due.CheckedAtMS < s.result.CutoffMs {
		if due.CheckedAtMS < s.result.CutoffMs {
			return redisTransactionCommit(ctx, store, s, nil, []rediscore.TimerChange{{Member: s.names.member, Due: time.UnixMilli(s.result.CutoffMs)}}, due.Assignment, deadline, false, nil)
		}
		return errNativeRecord
	}
	tx, err := s.attach(ctx, store, nil)
	if err != nil {
		return err
	}
	selectors, err := nativeSelectorIDs(tx.Origin, true, false)
	if err != nil || len(selectors) != 1 || selectors[0].Kind != rediscore.SelectorClientTransaction {
		return errNativeRecord
	}
	fields := []string{"r:context", s.names.procedure, s.names.origin, s.names.output, "r:result-1", "r:delivery-tag"}
	if s.procedure.Refs.LastResponse != "" {
		fields = append(fields, s.names.response)
	}
	changes := make([]rediscore.Change, 0, len(fields))
	for _, field := range fields {
		changes = append(changes, rediscore.Change{Field: field, Delete: true})
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, Terminal: true, CreditDelta: -int64(s.procedure.Metadata.CreditContribution), Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: s.names.procedure, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Delete: true}}}})
	return err
}

func redisStandaloneTerminated(s selectedRedisTransaction) bool {
	if s.procedure.Native.Phase != "terminated" || !s.current.Ended {
		return false
	}
	for _, timer := range s.procedure.Native.Timers {
		if timer.Active {
			return false
		}
	}
	return true
}
