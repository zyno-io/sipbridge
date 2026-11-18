package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// Handle bounds shared serving control work and never retains decoded protocol
// state between requests. The composed runtime handles local phone work separately.
func (e *redisNative) Handle(peer *control.Peer, envelope *pb.Envelope) error {
	if peer == nil || envelope == nil || envelope.CorrelationId == "" || envelope.RemainingBudgetMs == 0 {
		return errors.New("bounded control identity required")
	}
	deadline := time.Now().Add(time.Duration(envelope.RemainingBudgetMs) * time.Millisecond)
	correlation := envelope.CorrelationId
	decision := envelope.GetRequestDecision()
	command := envelope.GetCommand()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("native Redis engine closed")
	}
	select {
	case e.work <- struct{}{}:
	default:
		e.mu.Unlock()
		return peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "state execution capacity"}}})
	}
	e.wg.Add(1)
	e.mu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() { <-e.work }()
		ctx, cancel := context.WithDeadline(e.ctx, deadline)
		defer cancel()
		stop := context.AfterFunc(peer.Context(), cancel)
		defer stop()
		var result *pb.OperationResult
		var err error
		if inspect := envelope.GetInspect(); inspect != nil {
			result, err = e.inspectNativeContext(ctx, peer, inspect.Context, deadline)
		} else if command != nil && command.GetOpenUac() != nil {
			result, err = e.commandOpenUAC(ctx, peer, command, correlation, deadline)
		} else if command != nil && command.GetCancel() != nil {
			result, err = e.commandClientCancel(ctx, peer, command, correlation, deadline)
		} else if command != nil && command.GetDialogRequest() != nil && command.GetDialogRequest().Method == "ACK" {
			result, err = e.commandApplicationAnswerACK(ctx, peer, command, correlation, deadline)
		} else if command != nil && command.GetDialogRequest() != nil && command.GetDialogRequest().Method == "NOTIFY" {
			result, err = e.commandNotify(ctx, peer, command, correlation, deadline)
		} else if command != nil && command.GetDialogRequest() != nil && command.GetDialogRequest().Method == "INVITE" {
			result, err = e.commandApplicationBye(ctx, peer, command, correlation, deadline)
		} else if command != nil && command.GetDialogRequest() != nil && redisApplicationClientMethod(command.GetDialogRequest().Method) {
			result, err = e.commandApplicationBye(ctx, peer, command, correlation, deadline)
		} else if command != nil && command.GetRespond() != nil {
			result, err = e.commandInitialInviteRespond(ctx, peer, command, correlation, deadline)
		} else {
			result, err = e.decideDirect(ctx, peer, decision, correlation, deadline)
		}
		if err != nil {
			result = &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: err.Error()}
		}
		if ctx.Err() == nil && time.Now().Before(deadline) {
			// Operation snapshots are fresh public DTOs; stored context and frozen
			// lifecycle metadata keep their existing executor and timer assignment.
			if result != nil && result.Snapshot != nil && !sameProcess(result.Snapshot.Executor, e.cfg.Process) {
				result.Snapshot.Executor = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
			}
			_ = peer.Send(&pb.Envelope{CorrelationId: correlation, Payload: &pb.Envelope_Result{Result: result}})
		}
	}()
	return nil
}

func (e *redisNative) due(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext) error {
	if due != nil && strings.HasPrefix(due.Member, "event-") {
		return e.dispatchLifecycle(ctx, store, due)
	}
	if due != nil {
		deadline := time.Now().Add(2 * time.Second)
		switch {
		case strings.HasPrefix(due.Member, "out-transaction-output-"):
			return retireRedisStandaloneOutput(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "transaction-"):
			return fireRedisStandaloneTimer(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "orig-result-"):
			return expireRedisSupersededResult(ctx, store, due, deadline)
		case due.Member == "call-retire":
			_, err := dispatchRedisCallRetirement(ctx, store, due, deadline)
			return err
		case strings.HasPrefix(due.Member, "out-cancel-client-output-"):
			return retireRedisClientCancelOutput(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "cancel-client-retire-"):
			return retireRedisClientCancel(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "cancel-client-"):
			return fireRedisClientCancelTimer(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "out-call-cancel-output-") || strings.HasPrefix(due.Member, "out-call-cancel-invite-output-"):
			return dispatchRedisCancelOutputCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-cancel-tx-"):
			return fireRedisServerCancelJ(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-call-bye-output-"):
			return dispatchRedisCallByeOutputCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-tx-") && strings.HasSuffix(due.Member, "-J") && len(strings.TrimSuffix(strings.TrimPrefix(due.Member, "call-tx-"), "-J")) == 32:
			return fireRedisCallByeJ(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-dialog-") && strings.HasSuffix(due.Member, "-"+string(portable.TimerACK)):
			return fireRedisInitialInviteACKTimeout(ctx, store, e.cfg.StateIdentityKey, due, deadline)
		case strings.HasPrefix(due.Member, "out-dialog-output-"):
			return dispatchRedisDialogOutputCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-proc-"):
			return retireRedisDialogServerProcedure(ctx, store, e.cfg.StateIdentityKey, due, deadline)
		case strings.HasPrefix(due.Member, "call-decision-"):
			return timeoutRedisDialogServer(ctx, store, e.cfg.StateIdentityKey, nativeSIPAdvertisement{contactURI: e.cfg.ContactURI, address: e.cfg.AdvertiseAddress, plainAddress: e.cfg.PlainAdvertiseAddress}, due, deadline)
		case strings.HasPrefix(due.Member, "call-tx-") && strings.HasSuffix(due.Member, "-J") && len(strings.TrimSuffix(strings.TrimPrefix(due.Member, "call-tx-"), "-J")) == 36:
			return fireRedisDialogServerJ(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-app-ack-output-"):
			return retireRedisApplicationACK(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "out-retained-ack-output-"):
			return retireRedisRetainedACK(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-app-invite-ack-output-"):
			return retireRedisApplicationInviteACK(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "out-app-invite-output-"):
			return dispatchRedisApplicationInviteCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "app-invite-retire-"):
			return retireRedisApplicationInvite(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "app-invite-"):
			return fireRedisApplicationInviteDeadline(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-uac-output-"):
			return retireRedisUACInitialOutput(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-uac-ack-output-"):
			return retireRedisUACFailureACK(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-uac-dialog-ack-output-"):
			return retireRedisUACSuccessACK(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "uac-result-"):
			return expireRedisUACInitialResult(ctx, store, due, e.cfg.StateIdentityKey, deadline)
		case strings.HasPrefix(due.Member, "out-call-cleanup-output-") || strings.HasPrefix(due.Member, "out-app-bye-output-") || strings.HasPrefix(due.Member, "out-app-client-output-"):
			return dispatchRedisCleanupByeCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-client-") || strings.HasPrefix(due.Member, "app-bye-") || strings.HasPrefix(due.Member, "app-client-"):
			return fireRedisCleanupByeTimer(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-invite-output-"):
			return dispatchRedisInitialInviteOutputCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "out-output-") && strings.HasSuffix(due.Member, "-100"):
			return dispatchRedisInviteTryingCutoff(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-tx-") && strings.HasSuffix(due.Member, "-"+string(portable.TimerTrying)):
			return fireRedisInviteTrying(ctx, store, e.cfg.StateIdentityKey, due, deadline)
		case strings.HasPrefix(due.Member, "call-tx-") && (strings.HasSuffix(due.Member, "-"+string(portable.TimerH)) || strings.HasSuffix(due.Member, "-"+string(portable.TimerI)) || strings.HasSuffix(due.Member, "-"+string(portable.TimerL))):
			return fireRedisInviteServerTimer(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-tx-") && (strings.HasSuffix(due.Member, "-"+string(portable.TimerB)) || strings.HasSuffix(due.Member, "-"+string(portable.TimerD)) || strings.HasSuffix(due.Member, "-"+string(portable.TimerM)) || strings.HasSuffix(due.Member, "-"+string(portable.TimerCancel))):
			return fireRedisUACDeadline(ctx, store, due, deadline)
		case strings.HasPrefix(due.Member, "call-result-"):
			return expireRedisInviteResult(ctx, store, due, deadline)

		}
	}
	// Response construction selects the retained request's transport; ContactURI
	// remains the configured advertisement rather than inferred socket metadata.
	_, err := dispatchRedisRegisterDue(ctx, store, due, e.cfg.StateIdentityKey, e.cfg.ContactURI, time.Now().Add(2*time.Second))
	return err
}
