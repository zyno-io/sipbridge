package engine

import (
	"context"
	"crypto/sha256"
	"slices"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// commitRedisInviteDeferral binds a fresh UAS only with its exact admitted
// status-zero decision. It does not create a business response or socket write.
func commitRedisInviteDeferral(ctx context.Context, store *rediscore.Store, key []byte, application *pb.ProcessRef, lease rediscore.ApplicationLeaseRef, decision *pb.RequestDecision, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if store == nil || decision == nil || decision.Context == nil || decision.Context.State == nil || decision.Status != 0 || decision.Sequence != 1 || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return nil, errNativeRecord
	}
	if decision.NativeCallId != nil && !validCaptureUUID(decision.GetNativeCallId()) {
		return nil, errNativeRecord
	}
	app, err := nativeApplicationReference(key, application)
	if err != nil || app != lease.Application {
		return nil, rediscore.ErrAuthority
	}
	procedureField, originField := "r:procedure-"+decision.RequestId, "r:origin-"+decision.RequestId
	responseField := "r:response-" + decision.RequestId + "-100"
	selected, err := store.ObserveApplicationContext(ctx, decision.Context.Id, decision.Context.State.Incarnation, lease, "r:context", procedureField, originField, responseField, "r:result-1")
	if err != nil {
		return nil, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[procedureField])
	if err != nil {
		return nil, err
	}
	if selected.Identity.Owner != "" || current.Context == nil || !proto.Equal(current.Context, decision.Context) || current.Kind != pb.ContextSnapshot_UAS || current.CommandSequence != 0 || current.Revision >= 1<<53-1 || procedure.Metadata.RequestId != decision.RequestId || procedure.Refs.Origin != originField || len(selected.View.Records["r:result-1"]) != 0 {
		return nil, rediscore.ErrConflict
	}
	held := procedure.Refs.LastResponse != "" && procedure.Metadata.Emissions == 0
	if held && procedure.Refs.LastResponse != responseField {
		return nil, errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return nil, err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, held, false)
	if err != nil {
		return nil, err
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	candidate, err := nativePrepareDecision(nativeDecisionInput{decision: decision, transaction: tx, direct: procedure.Metadata.Direct, decisionPending: procedure.Metadata.DecisionPending, unbound: true, presentDeadline: time.UnixMilli(procedure.Metadata.PresentDeadlineMs), now: time.Now(), deadline: deadline, transactionApplication: procedure.Metadata.Application, application: application, leaseTerm: lease.Term, snapshot: snapshot, currentFlow: current.Flow, transactionFlow: procedure.Metadata.Flow, proposal: procedure.Metadata.Proposal, operation: operation})
	if err != nil {
		return nil, err
	}
	if candidate.request.Method != sip.INVITE || !candidate.bindOwner || candidate.policy == nil {
		return nil, errNativeRecord
	}
	until := minTime(deadline, time.UnixMilli(procedure.Metadata.PresentDeadlineMs))
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(decision)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(wire)
	result := &nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, CutoffMs: until.UnixMilli(), Sequence: decision.Sequence, OperationId: operation, Fingerprint: fingerprint[:], DecidingLeaseTerm: lease.Term, ServerRequestId: decision.RequestId}
	resultBytes, err := encodeNativeResult(result)
	if err != nil {
		return nil, err
	}
	next := proto.Clone(current).(*nativepb.Context)
	next.NativeCallId = decision.GetNativeCallId()
	next.Application = proto.Clone(application).(*pb.ProcessRef)
	next.ApplicationLeaseTerm = lease.Term
	next.PrincipalId, next.BindingId = decision.PrincipalId, decision.BindingId
	next.DirectoryEpoch = candidate.policy.DirectoryEpoch
	next.CommandSequence = decision.Sequence
	next.Revision++
	if decision.Delivery != nil {
		next.Delivery = proto.Clone(decision.Delivery).(*pb.ContextDelivery)
		next.Flow = proto.Clone(procedure.Metadata.Flow).(*pb.FlowRef)
	}
	contextBytes, err := encodeNativeContext(next)
	if err != nil {
		return nil, err
	}
	finalSnapshot, err := nativeProjectSnapshot(next, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	private, err := readPrivateProcedureRecord(selected.View.Records[procedureField])
	if err != nil {
		return nil, err
	}
	private.DecisionPending = false
	private.Proposal = nil
	private.RequestDialog = ""
	private.CommandResult = "r:result-" + strconv.FormatUint(decision.Sequence, 10)
	changes := []rediscore.Change{{Field: "r:context", Value: contextBytes}, {Field: private.CommandResult, Value: resultBytes}}
	timers := []rediscore.TimerChange{{Member: "call-decision-" + decision.RequestId, Delete: true}}
	var outputs []rediscore.OutputIntent
	if held {
		if tx.LastResponse == nil {
			return nil, errNativeRecord
		}
		outputField := "r:output-" + decision.RequestId + "-100"
		intent, outputChanges, _, outputErr := prepareRedisCallResponseOutput("INVITE", 100, finalSnapshot, procedure.Metadata.Flow, key, *tx.LastResponse, procedureField, "", responseField, "r:physical-"+decision.RequestId+"-100", outputField, until, false)
		if outputErr != nil {
			return nil, outputErr
		}
		private.Emissions++
		private.ProvisionalEmissions++
		private.Outputs = append(private.Outputs, outputField)
		slices.Sort(private.Outputs)
		outputs = append(outputs, intent)
		changes = append(changes, outputChanges...)
		timers = append(timers, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: until})
	}
	procedureBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, private)
	if err != nil {
		return nil, err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: procedureBytes})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Deadline: until, Application: &lease, BindApplication: true, Admission: rediscore.Admission{Class: rediscore.AdmissionInbound}, SpendReservation: true, Changes: changes, NewOutputs: outputs, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, Changes: timers}})
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil || !time.Now().Before(until) {
		return nil, rediscore.ErrUnknown
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: finalSnapshot}, nil
}
