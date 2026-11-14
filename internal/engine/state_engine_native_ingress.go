package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (i *Ingress) ConfigureNativeCompletion(handler func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error)) {
	i.stateCompletion = handler // Startup only, before listeners accept traffic.
}

// TryNativeCompletion never creates a group/context and never invokes business
// routing. Selection and reduction share the same authoritative update read.
func (e *StateEngine) TryNativeCompletion(parent context.Context, flow *pb.FlowRef, received *pb.SipMessage) (bool, error) {
	if !e.autonomousCompletion() || received == nil || flow == nil {
		return false, nil
	}
	parsed, err := parseObservedMessage(received.Wire)
	if err != nil {
		return false, err
	}
	message := projectObservedMessage(received.Wire, parsed, false)
	if message.Status == 0 && (message.Method == "REGISTER" || message.Method == "SUBSCRIBE" || message.Method == "NOTIFY") {
		return false, nil
	}
	completion := message.Status > 0 || message.Method == "ACK" || message.Method == "CANCEL" || message.Method == "BYE"
	to, from := message.Addresses["to"].GetValues(), message.Addresses["from"].GetValues()
	if !completion && (len(to) == 0 || len(from) == 0) {
		return false, nil
	}
	if !completion && (to[0].GetParams()["tag"] == "" || from[0].GetParams()["tag"] == "") {
		return false, nil
	}
	e.lifecycleMu.Lock()
	if e.closed {
		e.lifecycleMu.Unlock()
		return false, errors.New("native state engine closed")
	}
	select {
	case e.work <- struct{}{}:
	default:
		e.lifecycleMu.Unlock()
		return false, errors.New("native completion capacity")
	}
	e.wg.Add(1)
	e.lifecycleMu.Unlock()
	defer e.wg.Done()
	defer func() { <-e.work }()
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	callID := parsed.CallID().Value()
	ref, err := e.store.GroupForCallID(callID)
	if err != nil {
		return false, err
	}
	in := stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Flow: flow, Message: &pb.SipMessage{Wire: message.Wire}}
	result, committed, err := e.applyCommitted(ctx, ref, uuid.NewString(), in, time.Now().Add(2*time.Second), false)
	if errors.Is(err, state.ErrNotFound) || errors.Is(err, errNativeCompletionUnmatched) {
		return false, nil
	}
	if errors.Is(err, errNativeCompletionAmbiguous) {
		return true, rejectIngest(481, "ambiguous native completion context")
	}
	if err != nil {
		return true, err
	}
	if result.Error != "" {
		status := result.SipStatus
		if status < 300 {
			status = 481
		}
		return true, rejectIngest(status, "native completion refused")
	}
	if committed != nil {
		e.dispatchCommitted(committed.Ref, committed, "", "")
	}
	return true, nil
}

var errNativeCompletionUnmatched = errors.New("native completion unmatched")
var errNativeCompletionAmbiguous = errors.New("native completion ambiguous")

// selectNativeCompletion derives authority only from this update's record and time.
func (r *stateReduction) selectNativeCompletion() error {
	if r.in.Flow == nil || r.in.Message == nil {
		return state.ErrCorrupt
	}
	message := r.incomingMessage(r.in.Message.Wire, r.in.Flow, true)
	facts, err := message.Facts()
	if err != nil {
		return err
	}
	if facts.IsRequest {
		if _, err := message.ClientTxKey(); err != nil {
			return err
		}
	}
	completion := !facts.IsRequest || facts.Method == "ACK" || facts.Method == "CANCEL" || facts.Method == "BYE"
	var selected *pb.OwnerIngest
	var selectedContext *stateContext
	for _, c := range r.group.Contexts {
		if !completion && !c.OwnerLost {
			continue
		}
		admission := pb.OwnerIngest_EXISTING_FLOW
		if r.in.Flow.Transport == "tls" {
			admission = pb.OwnerIngest_TLS_DIALOG_CONTINUITY
			if !facts.IsRequest {
				admission = pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY
			}
		}
		candidate := &pb.OwnerIngest{Context: r.engine.contextRef(r.record.Ref, c.Snapshot.Context.Id), Application: c.Snapshot.Application,
			Flow: r.in.Flow, Wire: r.in.Message.Wire, PrincipalId: c.Snapshot.PrincipalId, BindingId: c.Snapshot.BindingId, Admission: admission}
		if r.validateCompletionRelay(c, message, candidate) != nil {
			continue
		}
		if selected != nil {
			return errNativeCompletionAmbiguous
		}
		selected, selectedContext = candidate, c
	}
	if selected == nil {
		return errNativeCompletionUnmatched
	}
	r.in.Context = selected.Context
	r.in.Ingest = selected
	r.in.Application = selectedContext.Snapshot.Application
	r.in.ApplicationLeaseTerm = selectedContext.Snapshot.ApplicationLeaseTerm
	return nil
}

func (r *stateReduction) ingestNativeCompletion() error {
	c, err := r.context()
	if err != nil || r.in.Ingest == nil || !r.in.NativeCompletion {
		return &portable.ProtocolError{Status: 481, Reason: "retained completion context required"}
	}
	message := r.incomingMessage(r.in.Ingest.Wire, r.in.Ingest.Flow, true)
	if err := r.validateCompletionRelay(c, message, r.in.Ingest); err != nil {
		return err
	}
	if c.Rejection && r.in.Ingest != nil {
		facts, err := message.Facts()
		if err != nil {
			return err
		}
		if facts.CSeqMethod == "ACK" {
			return r.ackRejection(message)
		}
		return &portable.ProtocolError{Status: 481, Reason: "private refusal cannot admit SIP work"}
	}
	return r.ingest()
}
