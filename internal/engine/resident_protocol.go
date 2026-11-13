package engine

import (
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// residentStartServer stages the initial native transition without changing an
// index or context. Its caller publishes the returned record and obligations
// together after admission and capacity checks.
func residentStartServer(message portable.Message, flow *pb.FlowRef, application *pb.ProcessRef, localTag, requestID string, deadline time.Time, now int64) (*residentTransaction, []portable.Effect, error) {
	if flow == nil || application == nil || requestID == "" || localTag == "" {
		return nil, nil, errors.New("server transaction identity required")
	}
	parsed, err := message.Parse()
	if err != nil {
		return nil, nil, err
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.IsAck() || request.IsCancel() {
		return nil, nil, errors.New("new server transaction requires an ordinary request")
	}
	family := portable.NonInviteServer
	if request.IsInvite() {
		family = portable.InviteServer
	}
	native, err := portable.NewTransaction(family, message, portable.DefaultTimers(), localTag)
	if err != nil {
		return nil, nil, err
	}
	next, effects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now})
	if err != nil {
		return nil, nil, err
	}
	return &residentTransaction{native: next, flow: proto.Clone(flow).(*pb.FlowRef), application: proto.Clone(application).(*pb.ProcessRef), requestID: requestID, presentDeadline: deadline}, effects, nil
}

// prepareResidentInitial creates candidate records only; the admission proof,
// accounting and indexes are committed by the caller under the local locks.
func (e *ResidentMemory) prepareResidentInitial(in *pb.OwnerIngest, deadline time.Time, now time.Time) (*residentContext, *residentTransaction, []portable.Effect, error) {
	if in == nil || in.Context == nil || in.Flow == nil || !validSendProcess(in.Application) {
		return nil, nil, nil, errors.New("inbound authority required")
	}
	if err := e.validateRef(in.Context); err != nil {
		return nil, nil, nil, err
	}
	message := portable.Message{Wire: in.Wire, Transport: in.Flow.Transport,
		Source: net.JoinHostPort(in.Flow.SourceIp, strconv.Itoa(int(in.Flow.SourcePort)))}
	parsed, err := message.Parse()
	if err != nil {
		return nil, nil, nil, err
	}
	request, ok := parsed.(*sip.Request)
	if !ok || request.To() == nil || request.From() == nil || parsed.CallID() == nil || parsed.CallID().Value() != in.Context.State.CallId || tag(request.To().Params) != "" || request.IsAck() || request.IsCancel() {
		return nil, nil, nil, errors.New("invalid initial request identity")
	}
	localTag := e.id("resident-tag", in.Context.State.Incarnation, in.Context.Id)[:32]
	requestID := "r-" + e.id("resident-request", in.Context.State.Incarnation, in.Context.Id, string(in.Wire))[:48]
	presentDeadline := now.Add(2 * time.Second)
	if deadline.Before(presentDeadline) {
		presentDeadline = deadline
	}
	tx, effects, err := residentStartServer(message, in.Flow, in.Application, localTag, requestID, presentDeadline, now.UnixMilli())
	if err != nil {
		return nil, nil, nil, err
	}
	snapshot := &pb.ContextSnapshot{Context: proto.Clone(in.Context).(*pb.ContextRef), Application: proto.Clone(in.Application).(*pb.ProcessRef),
		BindingId: in.BindingId, PrincipalId: in.PrincipalId, Kind: pb.ContextSnapshot_TRANSACTION, CallId: parsed.CallID().Value(),
		RemoteTag: tag(request.From().Params), RemoteUri: request.From().Address.String(), LocalUri: request.To().Address.String(),
		Transport: in.Flow.Transport, LocalTag: localTag}
	if request.IsInvite() {
		snapshot.Kind = pb.ContextSnapshot_UAS
	}
	c := &residentContext{snapshot: snapshot, flow: proto.Clone(in.Flow).(*pb.FlowRef),
		transactions: map[string]*residentTransaction{tx.native.Key: tx}, requests: map[string]string{requestID: tx.native.Key},
		dialogs: make(map[string]*residentDialog), outputs: make(map[string]*residentOutput)}
	return c, tx, effects, nil
}

// Reserve candidate growth before output publication. Decreases are released
// only after commit, so another context cannot consume bytes needed by rollback.
func (e *ResidentMemory) stageResidentTransactionBytesLocked(c *residentContext, tx *residentTransaction, next portable.Transaction) (int64, int64, error) {
	candidate := *tx
	candidate.native = next
	delta := residentTransactionBytes(&candidate) - residentTransactionBytes(tx)
	var borrowed int64
	var err error
	if delta > 0 {
		borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, delta)
	}
	return delta, borrowed, err
}
func (e *ResidentMemory) abortResidentTransactionBytesLocked(c *residentContext, tx *residentTransaction, delta, borrowed int64) {
	if delta > 0 {
		_ = e.refundResidentBudgetLocked(c, tx.budget, delta, borrowed)
	}
}
func (e *ResidentMemory) commitResidentTransactionBytesLocked(c *residentContext, tx *residentTransaction, delta, borrowed int64) {
	if delta < 0 {
		released := min(tx.borrowed, -delta)
		_ = e.refundResidentBudgetLocked(c, tx.budget, -delta, released)
		tx.borrowed -= released
	}
	tx.borrowed += borrowed
	tx.charged += delta
}

// A later native timer or retransmission cannot turn uncertain physical
// evidence into permission for another attempt of the same response.
func residentSuppressUncertainReplayLocked(c *residentContext, tx *residentTransaction, effects []portable.Effect) []portable.Effect {
	uncertain := false
	for _, output := range c.outputs {
		if output.transaction == tx.native.Key && (output.state == residentOutputStarted || output.state == residentOutputUnknown) {
			uncertain = true
			break
		}
	}
	if !uncertain {
		return effects
	}
	filtered := make([]portable.Effect, 0, len(effects))
	for _, effect := range effects {
		if (effect.Kind == portable.Send || effect.Kind == portable.Respond) && effect.Replay {
			continue
		}
		filtered = append(filtered, effect)
	}
	return filtered
}

func residentReplayFlowAllowed(c *residentContext, tx *residentTransaction, in *pb.OwnerIngest, request *sip.Request) bool {
	if proto.Equal(tx.flow, in.Flow) {
		return true
	}
	if !validPolicyFlow(in.Flow) || in.Flow.Transport != "tls" || c.snapshot.Transport != "tls" || request.Method == sip.REGISTER || request.Method == sip.SUBSCRIBE {
		return false
	}
	if c.snapshot.PrincipalId != "" && in.PrincipalId != "" && c.snapshot.PrincipalId != in.PrincipalId || c.snapshot.BindingId != "" && in.BindingId != "" && c.snapshot.BindingId != in.BindingId {
		return false
	}
	if request.IsInvite() && request.To() != nil && tag(request.To().Params) == "" {
		return true
	}
	if in.Admission == pb.OwnerIngest_TLS_DIALOG_CONTINUITY {
		return true
	}
	return in.Admission == pb.OwnerIngest_AUTHENTICATED_BINDING && c.snapshot.PrincipalId != "" && c.snapshot.BindingId != "" && c.snapshot.PrincipalId == in.PrincipalId && c.snapshot.BindingId == in.BindingId
}
