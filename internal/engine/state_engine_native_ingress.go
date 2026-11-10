package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (i *Ingress) ConfigureNativeCompletion(handler func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error)) {
	i.stateCompletion = handler // Startup only, before listeners accept traffic.
}

// TryNativeCompletion never creates a group/context and never invokes business
// routing. A read-side candidate remains a hint; the reducer proves it again.
func (e *StateEngine) TryNativeCompletion(parent context.Context, flow *pb.FlowRef, received *pb.SipMessage) (bool, error) {
	if !e.autonomousCompletion() || received == nil || flow == nil {
		return false, nil
	}
	message, err := Observe(received.Wire, false)
	if err != nil {
		return false, err
	}
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
	callID, err := stateMessageCallID(message.Wire)
	if err != nil {
		return false, err
	}
	ref, err := e.store.GroupForCallID(callID)
	if err != nil {
		return false, err
	}
	record, err := e.store.Load(ctx, ref)
	if errors.Is(err, state.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, classifyStateReadFailure(err)
	}
	group, err := decodeStateGroup(record)
	if err != nil {
		return false, classifyStateReadFailure(err)
	}
	var selected *pb.OwnerIngest
	for _, c := range group.Contexts {
		if !completion && !c.OwnerLost {
			continue // Exact final replay is allowed only after the owner fence.
		}
		admission := pb.OwnerIngest_EXISTING_FLOW
		if flow.Transport == "tls" {
			admission = pb.OwnerIngest_TLS_DIALOG_CONTINUITY
			if message.Status > 0 {
				admission = pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY
			}
		}
		candidate := &pb.OwnerIngest{Context: e.contextRef(record.Ref, c.Snapshot.Context.Id), Application: c.Snapshot.Application,
			Flow: flow, Wire: message.Wire, PrincipalId: c.Snapshot.PrincipalId, BindingId: c.Snapshot.BindingId, Admission: admission}
		r := &stateReduction{engine: e, record: record, group: group, now: time.Now().UnixMilli()}
		if r.validateCompletionRelay(c, portable.Message{Wire: message.Wire, Transport: flow.Transport}, candidate) != nil {
			continue
		}
		if selected != nil {
			return true, rejectIngest(481, "ambiguous native completion context")
		}
		selected = candidate
	}
	if selected == nil {
		return false, nil
	}
	c := group.Contexts[selected.Context.Id]
	in := stateEngineInput{Kind: "native-ingest", NativeCompletion: true, Context: selected.Context, Flow: flow, Ingest: selected,
		Application: proto.Clone(c.Snapshot.Application).(*pb.ProcessRef), ApplicationLeaseTerm: c.Snapshot.ApplicationLeaseTerm}
	result, committed, err := e.applyCommitted(ctx, record.Ref, uuid.NewString(), in, time.Now().Add(2*time.Second), false)
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
	e.dispatchCommitted(record.Ref, committed, "", "")
	return true, nil
}

func (r *stateReduction) ingestNativeCompletion() error {
	c, err := r.context()
	if err != nil || r.in.Ingest == nil || !r.in.NativeCompletion {
		return &portable.ProtocolError{Status: 481, Reason: "retained completion context required"}
	}
	message := portable.Message{Wire: r.in.Ingest.Wire, Transport: r.in.Ingest.Flow.GetTransport()}
	if err := r.validateCompletionRelay(c, message, r.in.Ingest); err != nil {
		return err
	}
	if c.Rejection && r.in.Ingest != nil {
		parsed, err := message.Parse()
		if err != nil {
			return err
		}
		if parsed.CSeq().MethodName == "ACK" {
			return r.ackRejection(message)
		}
		return &portable.ProtocolError{Status: 481, Reason: "private refusal cannot admit SIP work"}
	}
	return r.ingest()
}
