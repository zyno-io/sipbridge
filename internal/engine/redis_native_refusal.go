package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// Native local attribution is not an application owner or a synthetic lease.
func nativeInitialRefusalContext(current *nativepb.Context) bool {
	if current == nil || current.Context == nil || !current.Ended ||
		!(current.Kind == pb.ContextSnapshot_TRANSACTION && current.CommandSequence == 0 || current.Kind == pb.ContextSnapshot_UAS && current.CommandSequence == 1) ||
		len(current.Dialogs) != 0 || current.Answered || current.ApplicationLeaseTerm != "" || current.PrincipalId != "" || current.BindingId != "" || current.Delivery != nil ||
		!validSendProcess(current.Executor) || !sameProcess(current.Application, current.Executor) {
		return false
	}
	// A final INVITE refusal may acquire only its existing exact CANCEL child.
	// This class check never substitutes for the actual child/parent validators.
	for _, field := range current.NoninitialProcedures {
		id := strings.TrimPrefix(field, "r:call-cancel-")
		decoded, err := hex.DecodeString(id)
		if current.Kind != pb.ContextSnapshot_UAS || field != "r:call-cancel-"+id || err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != id {
			return false
		}
	}
	return true
}

// Tagged local refusals still own one initial server transaction, without
// joining the departed application's dialog or acquiring its authority.
func nativeInitialRefusalInviteProcedure(current *nativepb.Context, identity rediscore.Identity, field string) bool {
	return nativeInitialRefusalContext(current) && current.Kind == pb.ContextSnapshot_UAS && identity.Owner == "" && identity.Term == "" &&
		len(current.InitialProcedures) == 1 && current.InitialProcedures[0] == field
}

func nativeRegisterOrRefusalOutput(current *nativepb.Context, output *nativepb.Output) bool {
	return output != nil && (output.Method == "REGISTER" || nativeInitialRefusalContext(current) &&
		current.Kind == pb.ContextSnapshot_TRANSACTION && output.Status >= 300 && output.Status <= 699 &&
		output.Method != "INVITE" && output.Method != "ACK" && output.Result == "" && output.Operation == "")
}

func nativeZeroServerOutput(output *nativepb.Output, field string) bool {
	if output == nil || output.Write == nil || output.Write.Permit == nil || output.Write.Permit.Authorization == nil {
		return false
	}
	authorization := output.Write.Permit.Authorization
	id := strings.TrimPrefix(output.Procedure, "r:procedure-")
	parsed, err := uuid.Parse(id)
	return field == "r:output-0" && err == nil && parsed.String() == id && output.Procedure == "r:procedure-"+id &&
		validSendText(output.Method, 32, false) && output.Method != "INVITE" && output.Method != "ACK" &&
		output.Status >= 300 && output.Status <= 699 && output.Dialog == "" && output.Result == "" && output.Operation == "" &&
		!output.Replay && !output.CleanupAfterAck && output.AckSequence == 0 &&
		authorization.AuthorityClass == pb.EffectAuthorization_RESPONSE
}

func nativeLocalRefusalOutput(output *nativepb.Output, field string) bool {
	return nativeZeroServerOutput(output, field) && output.Write.Permit.Authorization.ApplicationLeaseTerm == ""
}

func nativeReliableRegisterOrRefusalMayRetire(current *nativepb.Context, method string, status uint32, transport string) bool {
	if method == "REGISTER" {
		return nativeReliableRegisterMayRetire(nativeReliableRegisterFinal{Method: method, Status: status, Transport: transport, Written: true, Settled: true, DispatcherAcknowledged: true})
	}
	return nativeInitialRefusalContext(current) && current.Kind == pb.ContextSnapshot_TRANSACTION &&
		method != "" && method != "INVITE" && method != "ACK" && status >= 300 && status <= 699 && transport != "udp"
}

// One atomic selector publication fences a refusal against racing application
// admission. No controller, application lease or RPC result is manufactured.
func (e *redisNative) rejectInitialNative(parent context.Context, flow *pb.FlowRef, message portable.Message, status uint32) error {
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || status < 300 || status > 699 ||
		!validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) || message.Transport() != flow.Transport ||
		message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) {
		return errNativeRecord
	}
	if facts.Method == sip.ACK {
		return nil
	}
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return err
	}
	defer cancel()
	serverKey, err := message.ServerTxKey()
	if err != nil {
		return err
	}
	selectors := []rediscore.Selector{nativeSelector(rediscore.SelectorServerTransaction, serverKey)}
	// Existing admission, including another refusal, remains authoritative.
	_, err = store.LookupSelector(ctx, selectors[0], "r:context")
	if err == nil {
		return nil
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}

	now := time.Now()
	id, incarnation, requestID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	ref := &pb.ContextRef{Id: id, Owner: proto.Clone(e.cfg.Process).(*pb.ProcessRef), State: &pb.StateRef{
		Authority: e.StateAuthority(), GroupId: id, Incarnation: incarnation, CallId: facts.CallID}}
	localTag := nativeCompactIdentity(e.cfg.StateIdentityKey, "native-refusal-tag", id, incarnation)
	if facts.ToTag != "" {
		localTag = facts.ToTag
	}
	var started portable.Transaction
	if facts.Method == sip.CANCEL {
		// An unmatched CANCEL owns only its refusal server transaction; it
		// cannot invent or mutate the absent INVITE's paired state.
		started, err = portable.NewTransaction(portable.NonInviteServer, message, portable.DefaultTimers(), localTag)
		if err == nil {
			started, _, err = portable.StepTransaction(started, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
		}
	} else {
		started, _, err = nativeServerStart(message, localTag, now.UnixMilli())
	}
	if err != nil {
		return err
	}
	contact, _, err := advertisement(e.cfg, flow.Transport)
	if err != nil {
		return err
	}
	response, err := portable.BuildResponseWithHeaders(started.Origin, int(status), reason(status), nil, localTag, contact, nil)
	if err != nil {
		return err
	}
	response, err = response.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
	if err != nil {
		return err
	}
	transaction, effects, err := portable.StepTransaction(started, portable.Input{Kind: portable.SendResponse, Message: response, Now: now.UnixMilli()})
	if err != nil {
		return err
	}
	sent := 0
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Send, portable.Respond:
			if effect.Message == nil || !effect.Message.EqualWire(response) {
				return errNativeRecord
			}
			sent++
		case portable.Schedule, portable.CancelTimer, portable.Ended:
		default:
			return errors.New("unexpected native refusal effect")
		}
	}
	if sent != 1 || transaction.LastResponse == nil || !transaction.LastResponse.EqualWire(response) {
		return errNativeRecord
	}
	publicSelectors, err := nativeMessageSelectors(message, false)
	if err != nil {
		return err
	}
	procedureField, originField := "r:procedure-"+requestID, "r:origin-"+requestID
	responseField, outputField := "r:response-"+requestID, "r:output-0"
	current := &nativepb.Context{Context: ref, Application: proto.Clone(e.cfg.Process).(*pb.ProcessRef),
		Executor: proto.Clone(e.cfg.Process).(*pb.ProcessRef), Flow: proto.Clone(flow).(*pb.FlowRef), Kind: pb.ContextSnapshot_TRANSACTION,
		CallId: facts.CallID, LocalTag: localTag, RemoteTag: facts.FromTag, LocalUri: facts.ToURI, RemoteUri: facts.FromURI,
		Transport: flow.Transport, Selectors: publicSelectors, Ended: true, Revision: 1}
	metadata := &nativepb.Procedure{Flow: proto.Clone(flow).(*pb.FlowRef), Application: proto.Clone(e.cfg.Process).(*pb.ProcessRef),
		RequestId: requestID, Emissions: 1, DeadlineMs: deadline.UnixMilli()}
	publication := rediscore.Publication{Identity: rediscore.Identity{Context: id, Incarnation: incarnation}, Create: true, Admission: redisInstanceAdmission(e.cfg.StateIdentityKey, e.cfg.Process, rediscore.AdmissionNativeRefusal, ""), Deadline: deadline,
		Reserved: residentSmallBudget, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: procedureField}},
		Timers: &rediscore.TimerMutation{Target: rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}}}
	if facts.Method == sip.INVITE {
		current.Kind, current.CommandSequence, current.CallRetireAtMs = pb.ContextSnapshot_UAS, 1, now.UnixMilli()
		current.InitialProcedures = []string{procedureField}
		responseField, outputField = "r:invite-response-"+requestID+"-1", "r:invite-output-"+requestID+"-1"
		metadata.Outputs, metadata.CreditContribution = []string{outputField}, uint64(residentSmallBudget)
		publication.Reserved, publication.CreditDelta = 0, residentSmallBudget
		publication.Timers.Changes = append(publication.Timers.Changes, rediscore.TimerChange{Member: "call-retire", Due: now})
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return err
	}
	intent, changes, shared, err := prepareRedisCallResponseOutput(string(facts.Method), status, snapshot, flow, e.cfg.StateIdentityKey,
		response, procedureField, "", responseField, "r:tw-"+requestID, outputField, deadline, false)
	if err != nil {
		return err
	}
	if !shared {
		encoded, encodeErr := selectedmessage.Marshal(response)
		if encodeErr != nil {
			return encodeErr
		}
		changes = append(changes, rediscore.Change{Field: responseField, Value: encoded})
	}
	contextBytes, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	procedureBytes, err := encodeNativeProcedure(transaction, nativeProcedureRefs{Origin: originField, LastResponse: responseField}, metadata)
	if err != nil {
		return err
	}
	originBytes, err := selectedmessage.Marshal(message)
	if err != nil {
		return err
	}
	publication.Changes = append(changes, rediscore.Change{Field: "r:context", Value: contextBytes},
		rediscore.Change{Field: procedureField, Value: procedureBytes}, rediscore.Change{Field: originField, Value: originBytes})
	publication.NewOutputs = []rediscore.OutputIntent{intent}
	publication.Timers.Changes = append(publication.Timers.Changes, rediscore.TimerChange{Member: "out-" + outputField[2:], Due: intent.Deadline})
	for timer, value := range transaction.Timers {
		if !value.Active {
			continue
		}
		member := ""
		switch timer {
		case portable.TimerH, portable.TimerI:
			member = "call-tx-" + requestID + "-" + string(timer)
		case portable.TimerJ:
			member = "native-" + requestID + "-J"
		case portable.TimerG:
			continue // The existing local derivative owns server retransmissions.
		default:
			return errors.New("unexpected native refusal timer")
		}
		publication.Timers.Changes = append(publication.Timers.Changes, rediscore.TimerChange{Member: member, Due: time.UnixMilli(value.At)})
	}
	_, err = store.Commit(ctx, publication)
	return err
}

// The ready queue supplies an output identity, not permission to reinterpret an
// application response as a local refusal. Validate its native owner before I/O.
func validateRedisInitialRefusalOutput(ctx context.Context, store *rediscore.Store, ready rediscore.ReadyOutput, output *nativepb.Output) error {
	selected, err := store.ObserveContext(ctx, ready.Ref.Context, ready.Ref.Incarnation, "r:context", ready.MetadataField, output.Procedure)
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil || !nativeRegisterOrRefusalOutput(current, output) ||
		current.ApplicationLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm ||
		!proto.Equal(current.Context, output.Write.Effect.Context) || !proto.Equal(current.Flow, output.Write.Flow) ||
		!bytes.Equal(selected.View.Records[ready.MetadataField], ready.Metadata) {
		return errNativeRecord
	}
	procedure, err := readPrivateProcedureRecord(selected.View.Records[output.Procedure])
	if err != nil || output.Procedure != "r:procedure-"+procedure.RequestId || procedure.Native.Version != portable.Version ||
		procedure.Native.Reliable != (current.Flow.Transport != "udp") || procedure.DecisionPending || procedure.CommandResult != "" || procedure.Origin != "r:origin-"+procedure.RequestId ||
		procedure.LastResponse != "r:response-"+procedure.RequestId || !proto.Equal(procedure.Flow, current.Flow) ||
		procedure.Native.Family != string(portable.NonInviteServer) || procedure.Native.ResponseTag != current.LocalTag ||
		procedure.Native.Phase != string(portable.Completed) && procedure.Native.Phase != string(portable.Terminated) {
		return errNativeRecord
	}
	if !nativeInitialRefusalContext(current) && !(current.Kind == pb.ContextSnapshot_TRANSACTION && current.CommandSequence == 0 &&
		!current.Ended && current.ApplicationLeaseTerm != "" && current.ApplicationLeaseTerm == output.Write.Permit.Authorization.ApplicationLeaseTerm && output.Method == "REGISTER" && output.Status == 503 && procedure.Direct) {
		return errNativeRecord
	}
	return nil
}
