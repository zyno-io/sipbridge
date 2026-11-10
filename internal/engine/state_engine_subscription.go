package engine

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type stateSubscription struct {
	Event          string   `json:"event"`
	Target         string   `json:"target"`
	Routes         []string `json:"routes,omitempty"`
	LocalSequence  uint32   `json:"localSequence"`
	RemoteSequence uint32   `json:"remoteSequence"`
	Pending        string   `json:"pending,omitempty"`
	ExpiresMS      int64    `json:"expiresMs"`
	TerminalMS     int64    `json:"terminalMs"`
	Terminating    bool     `json:"terminating"`
	Final          bool     `json:"final"`
}

func (r *stateReduction) validateSubscription(c *stateContext, request *sip.Request, in *pb.OwnerIngest) error {
	sub := c.Subscription
	event, err := eventKey(request)
	if request.Method != sip.SUBSCRIBE || err != nil || event != sub.Event || request.Contact() == nil ||
		tag(request.To().Params) != c.Snapshot.LocalTag || tag(request.From().Params) != c.Snapshot.RemoteTag ||
		request.From().Address.String() != c.Snapshot.RemoteUri || request.To().Address.String() != c.Snapshot.LocalUri || sub.Terminating {
		return &portable.ProtocolError{Status: 481, Reason: "subscription identity mismatch"}
	}
	if c.Snapshot.PrincipalId != "" && in.PrincipalId != "" && c.Snapshot.PrincipalId != in.PrincipalId || c.Snapshot.BindingId != "" && in.BindingId != "" && c.Snapshot.BindingId != in.BindingId {
		return &portable.ProtocolError{Status: 481, Reason: "subscription binding mismatch"}
	}
	if request.CSeq().SeqNo <= sub.RemoteSequence {
		return &portable.ProtocolError{Status: 500, Reason: "stale subscription sequence"}
	}
	if sub.Pending != "" {
		return &portable.ProtocolError{Status: 503, Reason: "subscription renewal pending"}
	}
	return nil
}
func (r *stateReduction) presentRequest(c *stateContext, tx *stateTransaction) error {
	request, _ := tx.Native.Origin.Parse()
	if request.CSeq().MethodName == sip.SUBSCRIBE {
		if c.Subscription != nil {
			c.Subscription.Pending = tx.Native.Key
		}
	} else if !proto.Equal(c.Flow, tx.Flow) || c.Delivery != nil {
		tx.DeferredPresent = true
		return r.proposeFlow(c, tx, tx.Flow, tx.Application, false)
	}
	return r.presentNow(c, tx)
}
func (r *stateReduction) presentNow(c *stateContext, tx *stateTransaction) error {
	if deferred, err := r.deferForOwnerProof(c, tx); deferred || err != nil {
		return err
	}
	message, err := Observe(tx.Native.Origin.Wire, false)
	if err != nil {
		return err
	}
	snapshot := stateSnapshot(c, r.engine.cfg.Process)
	application := tx.Application
	if application == nil {
		application = c.Snapshot.Application
	}
	if c.Subscription != nil {
		snapshot.Application = application
	}
	r.addEffect("present", c, stateEngineEffect{Application: application, Transaction: tx.Native.Key, Envelope: &pb.Envelope{Payload: &pb.Envelope_Request{Request: &pb.RequestPresented{Snapshot: snapshot, RequestId: tx.RequestID, Message: message, Flow: tx.Flow}}}}, r.now+2000)
	tx.DeferredPresent = false
	return nil
}
func (r *stateReduction) proposeFlow(c *stateContext, tx *stateTransaction, flow *pb.FlowRef, application *pb.ProcessRef, subscription bool) error {
	if flow == nil || application == nil {
		return errors.New("flow publication identity required")
	}
	if c.Delivery == nil && c.DeliveryRevision == 0 && proto.Equal(c.Flow, flow) && (!subscription || c.Subscription == nil && sameProcess(c.Snapshot.Application, application)) {
		if tx != nil && tx.DeferredPresent {
			return r.presentNow(c, tx)
		}
		return nil
	}
	if c.DeliveryRevision == math.MaxUint64 {
		return errors.New("delivery revision exhausted")
	}
	c.DeliveryRevision++
	request := &pb.FlowAdoptionRequest{Context: stateSnapshot(c, r.engine.cfg.Process).Context, Application: application, PrincipalId: c.Snapshot.PrincipalId, BindingId: c.Snapshot.BindingId, Flow: flow, NativeRevision: c.DeliveryRevision, Subscription: subscription}
	body := stateEngineEffect{Application: application, Envelope: &pb.Envelope{Payload: &pb.Envelope_FlowAdoption{FlowAdoption: request}}}
	if tx != nil {
		body.Transaction = tx.Native.Key
	}
	r.addEffect("adopt", c, body, r.now+2000)
	return nil
}
func (r *stateReduction) finishFlowAdoption(index int, effect state.Effect, body stateEngineEffect, c *stateContext) error {
	proposal := body.Envelope.GetFlowAdoption()
	reply := r.in.Reply.GetFlowAdoptionResult()
	var tx *stateTransaction
	if body.Transaction != "" {
		tx = c.Transactions[body.Transaction]
	}
	if r.in.Failure != "" || reply == nil || reply.Error != "" {
		effect.Status = state.EffectNotWritten
		r.setEffect(index, effect, body)
		if tx != nil {
			tx.DeferredPresent = false
			tx.PendingResponse = nil
			if c.Subscription != nil {
				c.Subscription.Pending = ""
			}
			c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, SipStatus: 503, Error: "context publication refused"}
			tx.RespondStatus = 503
			m, err := r.buildResponse(c, tx.Native.Origin, 503, "Context publication failed", nil, nil)
			if err != nil {
				return err
			}
			return r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: m})
		}
		return nil
	}
	receipt := reply.Receipt
	if proposal == nil || receipt == nil || receipt.NativeRevision != proposal.NativeRevision || receipt.DirectoryEpoch == "" || !proto.Equal(receipt.Flow, proposal.Flow) {
		return errors.New("invalid publication receipt")
	}
	if c.Delivery != nil && c.Delivery.NativeRevision > receipt.NativeRevision {
		return errors.New("stale publication receipt")
	}
	oldApplication := proto.Clone(c.Snapshot.Application).(*pb.ProcessRef)
	c.Delivery = proto.Clone(receipt).(*pb.ContextDelivery)
	c.Flow = proto.Clone(receipt.Flow).(*pb.FlowRef)
	if !proposal.Subscription && c.Dialog != "" {
		if c.DialogCompletionFlows == nil {
			c.DialogCompletionFlows = make(map[string]*pb.FlowRef)
		}
		c.DialogCompletionFlows[c.Dialog] = proto.Clone(receipt.Flow).(*pb.FlowRef)
	}
	if proposal.Subscription {
		c.Snapshot.Application = proto.Clone(proposal.Application).(*pb.ProcessRef)
	}
	effect.Status = state.EffectWritten
	r.setEffect(index, effect, body)
	if proposal.Subscription {
		if err := r.ownerAdopted(c, oldApplication, c.Snapshot.Application, receipt.DirectoryEpoch); err != nil {
			return err
		}
	}
	r.touch(c)
	if tx != nil {
		if tx.PendingResponse != nil {
			response := tx.PendingResponse
			tx.PendingResponse = nil
			return r.commitSubscription(c, tx, response)
		}
		if tx.DeferredPresent {
			return r.presentNow(c, tx)
		}
	}
	return nil
}
func (r *stateReduction) acceptSubscription(c *stateContext, tx *stateTransaction, response *pb.Respond) error {
	request, err := tx.Native.Origin.Parse()
	if err != nil {
		return err
	}
	req := request.(*sip.Request)
	event, err := eventKey(req)
	if err != nil {
		return err
	}
	if req.Contact() == nil {
		return errors.New("SUBSCRIBE Contact required")
	}
	if c.Subscription != nil && c.Subscription.Event != event {
		return errors.New("subscription event mismatch")
	}
	tx.Command = r.in.Operation
	if c.Subscription != nil || c.Delivery != nil || c.DeliveryRevision > 0 || !proto.Equal(c.Flow, tx.Flow) || !sameProcess(c.Snapshot.Application, tx.Application) {
		tx.PendingResponse = proto.Clone(response).(*pb.Respond)
		if err = r.proposeFlow(c, tx, tx.Flow, tx.Application, true); err != nil {
			return err
		}
		c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
		r.result = proto.Clone(c.Result).(*pb.OperationResult)
		return nil
	}
	return r.commitSubscription(c, tx, response)
}
func (r *stateReduction) commitSubscription(c *stateContext, tx *stateTransaction, response *pb.Respond) error {
	parsed, _ := tx.Native.Origin.Parse()
	req := parsed.(*sip.Request)
	event, err := eventKey(req)
	if err != nil {
		return err
	}
	expires := 3600
	for _, h := range response.Headers {
		if strings.EqualFold(h.Name, "Expires") {
			expires, err = strconv.Atoi(h.Value)
			if err != nil || expires < 0 || expires > 10800 {
				return errors.New("invalid granted subscription expiry")
			}
		}
	}
	if c.Subscription == nil {
		c.Subscription = &stateSubscription{Event: event}
		for _, h := range req.GetHeaders("Record-Route") {
			c.Subscription.Routes = append(c.Subscription.Routes, h.Value())
		}
	}
	sub := c.Subscription
	sub.Target = req.Contact().Address.String()
	sub.RemoteSequence = req.CSeq().SeqNo
	sub.Pending = ""
	sub.ExpiresMS = r.now + int64(expires)*1000
	sub.Terminating = expires == 0
	if sub.Terminating {
		sub.TerminalMS = r.now + 5000
	} else {
		sub.TerminalMS = 0
	}
	c.Snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
	c.Snapshot.LocalTag = c.Tag
	c.Snapshot.RemoteTag = tag(req.From().Params)
	c.Snapshot.DialogId = sip.DialogIDMake(c.Snapshot.CallId, c.Tag, c.Snapshot.RemoteTag)
	c.Snapshot.RemoteTarget = sub.Target
	m, err := r.buildResponse(c, tx.Native.Origin, int(response.Status), reason(response.Status), response.Body, stateHeaders(response.Headers))
	if err != nil {
		return err
	}
	if err = r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: m}); err != nil {
		return err
	}
	r.index(c, m, true)
	r.touch(c)
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
	r.result = proto.Clone(c.Result).(*pb.OperationResult)
	return nil
}
func (r *stateReduction) notifySubscription(c *stateContext, command *pb.DialogRequest) error {
	sub := c.Subscription
	if command.Method != "NOTIFY" {
		return errors.New("subscription only supports NOTIFY")
	}
	stateValue := "active"
	for _, h := range command.Headers {
		if strings.EqualFold(h.Name, "Subscription-State") {
			stateValue = h.Value
		}
	}
	final := strings.HasPrefix(stateValue, "terminated")
	if c.Snapshot.Ended || sub.Final || sub.Terminating && !final {
		return errSubscriptionTerminating
	}
	if !final {
		remaining := (sub.ExpiresMS - r.now + 999) / 1000
		if remaining < 1 {
			return errors.New("subscription expired")
		}
		stateValue = fmt.Sprintf("active;expires=%d", remaining)
	}
	if sub.LocalSequence >= math.MaxInt32 {
		return errors.New("subscription CSeq exhausted")
	}
	sub.LocalSequence++
	headers := []portable.Header{{Name: "Event", Value: sub.Event}, {Name: "Subscription-State", Value: stateValue}}
	for _, h := range command.Headers {
		if strings.EqualFold(h.Name, "Content-Type") {
			headers = append(headers, portable.Header{Name: h.Name, Value: h.Value})
		}
	}
	contact, sentBy, err := r.engine.advertisement(c.Snapshot.Transport)
	if err != nil {
		return err
	}
	d := portable.Dialog{Version: portable.Version, Role: portable.UAS, Phase: portable.DialogConfirmed, CallID: c.Snapshot.CallId, LocalTag: c.Snapshot.LocalTag, RemoteTag: c.Snapshot.RemoteTag, LocalURI: c.Snapshot.LocalUri, RemoteURI: c.Snapshot.RemoteUri, Target: sub.Target, Routes: sub.Routes, Contact: contact, SentBy: sentBy, Transport: c.Snapshot.Transport}
	m, err := portable.BuildDialogRequestWithHeaders(d, sip.NOTIFY, sub.LocalSequence, command.Body, sip.RFC3261BranchMagicCookie+r.engine.id("notify", r.record.Ref.Incarnation, r.in.Operation)[:32], headers)
	if err != nil {
		return err
	}
	if final {
		sub.Terminating = true
		sub.Final = true
		sub.TerminalMS = r.now + 5000
	}
	native, err := portable.NewTransaction(portable.NonInviteClient, m, portable.DefaultTimers(), "")
	if err != nil {
		return err
	}
	tx := &stateTransaction{Native: native, Flow: c.Flow, Command: c.Operation, Policy: r.policy}
	c.Transactions[native.Key] = tx
	r.index(c, m, true)
	return r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now})
}

func (r *stateReduction) validateReplayFlow(c *stateContext, tx *stateTransaction, request *sip.Request, in *pb.OwnerIngest) error {
	if proto.Equal(tx.Flow, in.Flow) {
		return nil
	}
	if request.Method == sip.REGISTER || request.Method == sip.SUBSCRIBE {
		return &portable.ProtocolError{Status: 481, Reason: "initial replay requires exact current flow"}
	}
	if in.Flow.Transport != "tls" || c.Snapshot.Transport != "tls" {
		return &portable.ProtocolError{Status: 481, Reason: "replacement replay requires TLS"}
	}
	if c.Snapshot.PrincipalId != "" && in.PrincipalId != "" && c.Snapshot.PrincipalId != in.PrincipalId || c.Snapshot.BindingId != "" && in.BindingId != "" && c.Snapshot.BindingId != in.BindingId {
		return &portable.ProtocolError{Status: 481, Reason: "replay binding mismatch"}
	}
	original, _ := tx.Native.Origin.Parse()
	if request.IsInvite() && tag(original.To().Params) == "" && bytes.Equal(tx.Native.Origin.Wire, in.Wire) {
		return nil
	}
	if in.Admission == pb.OwnerIngest_TLS_DIALOG_CONTINUITY {
		return nil
	}
	if in.Admission == pb.OwnerIngest_AUTHENTICATED_BINDING && c.Snapshot.PrincipalId != "" && c.Snapshot.BindingId != "" && c.Snapshot.PrincipalId == in.PrincipalId && c.Snapshot.BindingId == in.BindingId {
		return nil
	}
	return &portable.ProtocolError{Status: 481, Reason: "replacement replay admission required"}
}
