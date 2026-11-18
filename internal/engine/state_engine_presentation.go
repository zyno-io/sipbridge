package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
)

// presentCommitted consumes only the one-shot projection of a definite CAS.
// It never reloads a retained aggregate or schedules recovery.
func (e *StateEngine) presentCommitted(parent context.Context, committed *execution.Committed, source callbackSource) {
	if committed == nil {
		return
	}
	presentations := committed.LocalPresentations
	committed.LocalPresentations = nil
	for _, presentation := range presentations {
		deadline := time.UnixMilli(presentation.DeadlineMS)
		ctx, cancel := context.WithDeadline(parent, deadline)
		accepted := false
		if ctx.Err() == nil {
			peer, err := e.control.Peer(presentation.Application)
			if err == nil {
				remaining := time.Until(deadline).Milliseconds()
				if remaining > 0 {
					request := &pb.Envelope{RemainingBudgetMs: uint32(min(2000, remaining)), Payload: &pb.Envelope_Request{Request: presentation.Request}}
					e.callbackMetrics.callback("present", source, callbackAttempt)
					reply, err := peer.Call(ctx, request)
					if err != nil {
						e.callbackMetrics.callback("present", source, callbackError)
					} else {
						e.callbackMetrics.callback("present", source, callbackResponse)
						result := reply.GetResult()
						accepted = result != nil && result.Error == "" && (result.Certainty == pb.OperationResult_ACCEPTED || result.Certainty == pb.OperationResult_COMPLETED)
					}
				}
			}
		}
		cancel()
		if !accepted {
			contextRef := presentation.Request.GetSnapshot().GetContext()
			in := stateEngineInput{Kind: "presentation-failed", Context: contextRef, Operation: presentation.Request.RequestId}
			_, next, err := e.applyCommitted(e.ctx, committed.Ref, e.id("presentation-failed", committed.Ref.Incarnation, contextRef.GetId(), in.Operation), in, time.Now().Add(2*time.Second), false)
			if err == nil {
				e.dispatchCommitted(committed.Ref, next, "", "")
			}
		}
	}
}

// presentationFailed competes with application response selection on the same
// transaction. A selected final, including one awaiting flow publication, wins.
func (r *stateReduction) presentationFailed() error {
	c := r.group.Contexts[r.in.Context.GetId()]
	if c == nil {
		return nil
	}
	tx := c.Transactions[c.Requests[r.in.Operation]]
	if tx == nil || tx.RequestID != r.in.Operation || !stateServerTransaction(tx) || tx.RespondStatus >= 200 || tx.PendingResponse != nil || tx.FinalResponse != nil || tx.Native.Phase == portable.Terminated || tx.Native.Phase == portable.Completed || tx.Native.Phase == portable.Confirmed {
		return nil
	}
	if tx.Native.LastResponse != nil {
		final, err := tx.Native.LastResponse.Facts()
		if err != nil {
			return err
		}
		if final.IsRequest {
			return errors.New("invalid retained server response")
		}
		if final.Status >= 200 {
			return nil
		}
	}
	if !stateResponseEmissionAvailable(tx, 503) {
		return nil
	}
	tx.RespondStatus = 503
	return r.ownerRefuseRequest(c, tx)
}
