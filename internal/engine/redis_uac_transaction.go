package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// Standalone methods create no dialog or subscription. Their final response is
// the command result, including a challenge; credential continuation is separate.
func redisStandaloneMethod(method string) bool {
	switch method {
	case "OPTIONS", "MESSAGE", "NOTIFY", "REGISTER", "PUBLISH":
		return true
	}
	return false
}

type redisTransactionFields struct{ id, procedure, origin, response, output, member string }

func redisTransactionNames(id string) (redisTransactionFields, error) {
	if len(id) != 32 || strings.IndexFunc(id, func(r rune) bool { return !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') }) >= 0 {
		return redisTransactionFields{}, errNativeRecord
	}
	p := "r:transaction-"
	return redisTransactionFields{id, p + "procedure-" + id, p + "origin-" + id, p + "response-" + id, p + "output-" + id, "out-transaction-output-" + id}, nil
}

type selectedRedisTransaction struct {
	names        redisTransactionFields
	selected     *rediscore.ObservedContext
	current      *nativepb.Context
	procedure    nativeProcedureRecord
	result       *nativepb.Result
	output       *nativepb.Output
	requireLease bool
}

// Only the one actual owner is read. No call graph, controller or SIP packet is
// needed for physical settlement or proof-absence reconciliation.
func selectRedisTransaction(ctx context.Context, store *rediscore.Store, contextID, incarnation, id string) (selectedRedisTransaction, error) {
	var out selectedRedisTransaction
	n, err := redisTransactionNames(id)
	if err != nil {
		return out, err
	}
	selected, err := store.ObserveContext(ctx, contextID, incarnation, "r:context", n.procedure, "r:result-1", n.output)
	if err != nil {
		return out, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return out, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[n.procedure])
	if err != nil {
		return out, err
	}
	result, err := decodeNativeResult(selected.View.Records["r:result-1"])
	if err != nil {
		return out, err
	}
	output, err := decodeNativeOutput(selected.View.Records[n.output])
	if err != nil {
		return out, err
	}
	if current.Kind != pb.ContextSnapshot_TRANSACTION || current.Context == nil || current.Context.State == nil || current.Context.Id != contextID || current.Context.State.Incarnation != incarnation || current.Context.State.CallId != current.CallId || current.CommandSequence != 1 || len(current.InitialProcedures) != 1 || current.InitialProcedures[0] != n.procedure || len(current.NoninitialProcedures) != 0 || len(current.Dialogs) != 0 || current.CallRetireAtMs != 0 || procedure.Native.Version != portable.Version || procedure.Native.Key == "" || procedure.Native.Family != portable.NonInviteClient || procedure.Native.Reliable != (current.Transport != "udp") || procedure.Refs.Origin != n.origin || procedure.Refs.LastResponse != "" && procedure.Refs.LastResponse != n.response || procedure.Refs.ACK != "" || procedure.Metadata.Root != "" || procedure.Metadata.Cleanup || procedure.Metadata.DecisionPending || len(procedure.Metadata.Events) != 0 || len(procedure.Metadata.Outputs) != 1 || procedure.Metadata.Outputs[0] != n.output || procedure.Metadata.CommandResult != "r:result-1" || procedure.Metadata.CreditContribution == 0 || result.Sequence != 1 || result.CutoffMs <= 0 || result.CutoffMs > 1<<53-1 || result.Certainty != pb.OperationResult_UNKNOWN && result.Certainty != pb.OperationResult_COMPLETED || result.Pending && result.Certainty != pb.OperationResult_UNKNOWN || result.Response != "" && result.Request != n.origin || result.Output != n.output || result.ServerRequestId != "" || result.Request != "" && result.Request != n.origin || result.Response != "" && result.Response != n.response || result.RetirementProof != nil || !validReadyStandaloneOutput(output, n.output) || !proto.Equal(output.Write.Effect.Context, current.Context) || !proto.Equal(procedure.Metadata.Flow, output.Write.Flow) || !proto.Equal(current.Flow, output.Write.Flow) || result.OperationId != output.Operation || result.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
		return out, errNativeRecord
	}
	out = selectedRedisTransaction{names: n, selected: selected, current: current, procedure: procedure, result: result, output: output}
	return out, nil
}

func validReadyStandaloneOutput(output *nativepb.Output, field string) bool {
	if output == nil || !redisStandaloneMethod(output.Method) || output.Status != 0 || output.Dialog != "" || output.Replay || output.Result != "r:result-1" || output.Operation == "" || output.Write == nil || output.Write.Effect == nil || output.Write.Permit == nil || output.Write.Permit.Authorization == nil || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION {
		return false
	}
	id := strings.TrimPrefix(field, "r:transaction-output-")
	n, err := redisTransactionNames(id)
	return err == nil && field == n.output && output.Procedure == n.procedure
}

func (s selectedRedisTransaction) attach(ctx context.Context, store *rediscore.Store, incoming *portable.Message) (portable.Transaction, error) {
	fields := []string{s.names.origin}
	if s.procedure.Refs.LastResponse != "" {
		fields = append(fields, s.names.response)
	}
	wires, err := store.Read(ctx, s.selected.Identity, fields...)
	if err != nil {
		return portable.Transaction{}, err
	}
	if wires.Revision != s.selected.View.Revision || wires.Timer != s.selected.View.Timer {
		return portable.Transaction{}, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields) + 1)
	if err != nil {
		return portable.Transaction{}, err
	}
	if incoming != nil {
		if _, err = memo.Adopt(*incoming, "admitted"); err != nil {
			return portable.Transaction{}, err
		}
	}
	tx, err := s.procedure.attach(&nativeSelectedMessages{records: wires.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return tx, err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || string(facts.Method) != s.output.Method || facts.ToTag != "" || facts.CallID != s.current.CallId || tx.Origin.Transport() != s.output.Write.Flow.Transport {
		return portable.Transaction{}, errNativeRecord
	}
	return tx, nil
}

func redisTransactionTimers(id string, effects []portable.Effect) ([]rediscore.TimerChange, error) {
	var changes []rediscore.TimerChange
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Schedule, portable.CancelTimer:
			if effect.Timer == portable.TimerE {
				continue
			}
			if effect.Timer != portable.TimerF && effect.Timer != portable.TimerK {
				return nil, errNativeRecord
			}
			change := rediscore.TimerChange{Member: "transaction-" + id + "-" + string(effect.Timer), Delete: effect.Kind == portable.CancelTimer}
			if !change.Delete {
				change.Due = time.UnixMilli(effect.Deadline.At)
			}
			changes = append(changes, change)
		case portable.Ended, portable.Diagnostic, portable.ReportResponse:
		default:
			return nil, errNativeRecord
		}
	}
	return changes, nil
}

func redisTransactionCommit(ctx context.Context, store *rediscore.Store, s selectedRedisTransaction, changes []rediscore.Change, timers []rediscore.TimerChange, executor rediscore.TimerAssignment, deadline time.Time, hint bool, udp *rediscore.UDPHint) error {
	target := s.selected.View.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	pub := rediscore.Publication{Identity: s.selected.Identity, Revision: s.selected.View.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, UDPHint: udp}
	if len(timers) != 0 {
		pub.Timers = &rediscore.TimerMutation{Expected: s.selected.View.Timer, Target: target, RequireLease: s.requireLease, Changes: timers}
	}
	if hint {
		pub.ResultHint = &rediscore.ResultHint{Context: s.selected.Identity.Context, Incarnation: s.selected.Identity.Incarnation, Sequence: 1}
	}
	_, err := store.Commit(ctx, pub)
	if !s.requireLease && errors.Is(err, rediscore.ErrExpired) && pub.Timers != nil && target != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		pub.Timers.Target = executor
		_, err = store.Commit(ctx, pub)
	}
	return err
}

func redisTransactionProof(s selectedRedisTransaction, capture *nativeOutputCapture) error {
	if capture == nil || capture.metadata == nil || capture.ref.Context != s.selected.Identity.Context || capture.ref.Incarnation != s.selected.Identity.Incarnation || !capture.ref.RequireApplication || !proto.Equal(capture.metadata, s.output) || capture.proof.MetadataField != s.names.output || capture.proof.MessageField != s.names.origin || !bytes.Equal(capture.proof.Metadata, s.selected.View.Records[s.names.output]) || capture.proof.DeadlineMS != int64(s.output.Write.Permit.Authorization.DeadlineMs) {
		return rediscore.ErrConflict
	}
	return nil
}
