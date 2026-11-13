package engine

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// residentDigest applies a challenged transaction before making a
// bounded credential lookup. The secret is used only by stateBuildDigestResult
// and the exact transaction is checked again before the retry is published.
func (e *ResidentMemory) residentDigest(ctx context.Context, c *residentContext, tx *residentTransaction, challenge portable.Message, response *sip.Response) (bool, error) {
	if tx.command == nil || tx.deadline.IsZero() || !time.Now().Before(tx.deadline) || tx.credential == nil || tx.credential.Id == "" || tx.credential.Version == "" || tx.digestAttempts >= stateDigestRetryLimit || response.StatusCode != 401 && response.StatusCode != 407 {
		return false, nil
	}
	header := "WWW-Authenticate"
	if response.StatusCode == 407 {
		header = "Proxy-Authenticate"
	}
	if response.GetHeader(header) == nil {
		return false, nil
	}
	if tx.digestHandled {
		return true, nil
	}
	if c.snapshot.Revision >= 1<<53-1 {
		return true, errors.New("observation revision exhausted")
	}
	next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.ReceiveResponse, Now: time.Now().UnixMilli(), Message: challenge})
	if err != nil {
		return true, err
	}
	delta := residentClientTransactionDelta(tx, next)
	growth, err := e.prepareResidentClientGrowth(c, tx.budget, delta)
	if err != nil {
		return true, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, nil, tx.command.operation, time.Now().Add(2*time.Second))
	if err != nil {
		e.rollbackResidentClientGrowth(c, growth)
		return true, err
	}
	tx.native = next
	c.snapshot.Revision++
	c.revision++
	tx.digestHandled = true
	tx.charged += delta
	e.commitResidentClientGrowth(c, growth, &tx.borrowed)
	tx.digestAttempts++
	attempt := tx.digestAttempts
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	identity := e.id("digest", c.snapshot.Context.State.Incarnation, c.snapshot.Context.Id, tx.native.Key, strconv.FormatUint(uint64(attempt), 10), string(challenge.Wire))
	work := stateDigestChallenge{Origin: tx.native.Origin.Clone(), Challenge: challenge.Clone(), Credential: proto.Clone(tx.credential).(*pb.CredentialRef),
		Cnonce: identity[:32], Count: int(attempt), Branch: sip.RFC3261BranchMagicCookie + identity[32:], Attempt: attempt}
	ref := proto.Clone(c.snapshot.Context).(*pb.ContextRef)
	app := proto.Clone(c.snapshot.Application).(*pb.ProcessRef)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	lookupDeadline := time.Now().Add(2 * time.Second)
	if tx.deadline.Before(lookupDeadline) {
		lookupDeadline = tx.deadline
	}
	lookupCtx, cancel := context.WithDeadline(ctx, lookupDeadline)
	peer, err := e.control.Peer(app)
	var message *pb.SipMessage
	if err == nil {
		var reply *pb.Envelope
		reply, err = peer.Call(lookupCtx, &pb.Envelope{CorrelationId: uuid.NewString(), RemainingBudgetMs: 2000,
			Payload: &pb.Envelope_CredentialLookup{CredentialLookup: &pb.CredentialLookup{Context: ref, Credential: work.Credential}}})
		if err == nil {
			message, err = stateBuildDigestResult(work, reply.GetCredentialResult())
		}
	}
	cancel()
	c.mu.Lock()
	e.mu.Lock()
	lease := e.leases[residentControllerKey(app)]
	live := !e.closed && e.contexts[ref.Id] == c && lease != nil && lease.term == c.snapshot.ApplicationLeaseTerm && time.Now().Before(lease.expires)
	e.mu.Unlock()
	policyValid := tx.policy != nil && time.Now().UnixMilli() < int64(tx.policy.ExpiresAtMs) && tx.policy.DirectoryEpoch == c.snapshot.DirectoryEpoch && proto.Equal(tx.policy.ExpectedDelivery, c.snapshot.Delivery)
	if err != nil && live && c.transactions[tx.native.Key] == tx && tx.digestAttempts == attempt && !c.cancelled && !c.snapshot.Ended && c.snapshot.Revision < 1<<53-1 {
		oldSnapshot := c.snapshot
		c.snapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
		c.snapshot.Ended = true
		c.snapshot.Revision++
		event, eventErr := e.stageResidentEventLocked(c, pb.DialogEvent_FAILED, nil, tx.command.operation, time.Now().Add(2*time.Minute), tx.budget)
		if eventErr != nil {
			c.snapshot = oldSnapshot
			return true, eventErr
		}
		c.revision++
		e.scheduleResidentContextRetirementLocked(c)
		e.scheduleResidentEventsLocked(c, []*residentEvent{event})
		finishResidentCommandLocked(c, tx.command, &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: "SIP setup authentication unavailable", Request: mustObserve(tx.native.Origin.Wire, true), Response: mustObserve(challenge.Wire, false)})
		return true, nil
	}
	if err != nil || !live || !policyValid || c.snapshot.Revision >= 1<<53-1 || !time.Now().Before(tx.deadline) || c.transactions[tx.native.Key] != tx || tx.digestAttempts != attempt || c.cancelled || c.snapshot.Ended {
		finishResidentCommandLocked(c, tx.command, &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "SIP credential lookup unavailable"})
		if err != nil {
			return true, err
		}
		return true, errors.New("digest transaction changed")
	}
	origin := portable.Message{Wire: message.Wire, Transport: tx.flow.Transport}
	native, err := portable.NewTransaction(tx.native.Family, origin, portable.DefaultTimers(), "")
	if err != nil {
		return true, err
	}
	started, retryEffects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: time.Now().UnixMilli()})
	if err != nil {
		return true, err
	}
	retry := &residentTransaction{native: started, flow: proto.Clone(tx.flow).(*pb.FlowRef), application: app, command: tx.command,
		credential: proto.Clone(tx.credential).(*pb.CredentialRef), digestAttempts: attempt, deadline: tx.deadline, budget: tx.budget, policy: proto.Clone(tx.policy).(*pb.CommandAdmission), selectors: append([]string(nil), message.Selectors...)}
	retry.charged = residentTransactionBytes(retry)
	retry.borrowed, err = e.chargeResidentBudgetLocked(c, retry.budget, retry.charged)
	if err != nil {
		return true, err
	}
	writes, err = e.stageResidentOutputsLocked(c, retry, retryEffects, retry.policy, tx.command.operation, tx.deadline)
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, retry.budget, retry.charged, retry.borrowed)
		return true, err
	}
	retainResidentCommandLocked(retry.command)
	c.transactions[retry.native.Key] = retry
	c.snapshot.Revision++
	c.revision++
	e.mu.Lock()
	for _, selector := range retry.selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, retry, retryEffects)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	c.mu.Lock()
	return true, nil
}
