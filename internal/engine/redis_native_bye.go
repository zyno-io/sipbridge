package engine

import (
	"context"
	"crypto/hmac"
	"errors"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
)

// commandApplicationBye keeps only the original caller's bounded wait. Its
// physical and native duties continue independently after that caller leaves.
type redisApplicationClientSelection struct {
	capture  redisNotifyWaitCapture
	waiter   *nativeResultWaiter
	selected *rediscore.ObservedContext
	current  *nativepb.Context
}

func (e *redisNative) commandApplicationBye(parent context.Context, peer *control.Peer, command *pb.Command, operation string, deadline time.Time, shared ...*redisApplicationClientSelection) (*pb.OperationResult, error) {
	if command == nil || command.GetDialogRequest() == nil || !redisApplicationClientMethod(command.GetDialogRequest().Method) && command.GetDialogRequest().Method != "INVITE" || command.Context == nil || command.Context.State == nil || !state.ValidReference(command.Context.Id) || command.Context.State.GroupId != command.Context.Id || command.Context.State.Partition != 0 || !state.ValidReference(command.Context.State.Incarnation) || !proto.Equal(command.Context.State.Authority, e.StateAuthority()) || command.Sequence == 0 || command.Sequence > 1<<53-1 || !validSendText(operation, 160, false) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid dialog command"}, nil
	}
	lease, err := e.controllerWitness(peer)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller attachment unavailable"}, nil
	}
	var capture redisNotifyWaitCapture
	if len(shared) == 1 && shared[0] != nil && shared[0].waiter != nil && shared[0].selected != nil && shared[0].current != nil {
		capture = shared[0].capture
	} else if len(shared) == 0 {
		capture, err = captureRedisNotifyWait(command)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errNativeRecord
	}
	hint := rediscore.ResultHint{Context: command.Context.Id, Incarnation: command.Context.State.Incarnation, Sequence: command.Sequence}
	var waiter *nativeResultWaiter
	if len(shared) == 1 {
		waiter = shared[0].waiter
	} else {
		waiter, err = e.registerResultWaiter(hint)
		if err != nil {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "state execution capacity"}, nil
		}
		defer e.releaseResultWaiter(hint, waiter)
	}
	store, workerLease, until, err := e.driver.Current()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	stop := context.AfterFunc(workerLease, cancel)
	defer stop()
	var identity rediscore.Identity
	for attempts := 0; attempts < 3; attempts++ {
		var selected *rediscore.ObservedContext
		var current *nativepb.Context
		var x error
		if attempts == 0 && len(shared) == 1 {
			selected, current = shared[0].selected, shared[0].current
		} else {
			selected, x = store.ObserveApplicationContext(ctx, hint.Context, hint.Incarnation, lease, "r:context")
			if x != nil {
				return nil, x
			}
			current, x = decodeNativeContext(selected.View.Records["r:context"])
			if x != nil {
				return nil, x
			}
		}
		if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, command.Context.State) || !sameProcess(current.Application, peer.Application()) || current.ApplicationLeaseTerm != lease.Term {
			return nil, rediscore.ErrAuthority
		}
		if command.Sequence <= current.CommandSequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command sequence already consumed"}, nil
		}
		if current.CommandSequence >= 1<<53-1 || current.CommandSequence+1 != command.Sequence {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "stale command sequence or sequence gap"}, nil
		}
		if command.GetDialogRequest().Method == "INVITE" {
			identity, x = publishRedisApplicationInvite(ctx, store, e.cfg.StateIdentityKey, peer.Application(), lease, command, capture, operation, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline, until)
		} else {
			identity, x = publishRedisApplicationBye(ctx, store, e.cfg.StateIdentityKey, peer.Application(), lease, command, capture, operation, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline)
		}
		if x == nil {
			break
		}
		if errors.Is(x, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) && attempts < 2 {
			continue
		}
		if errors.Is(x, errStatePolicyAdmission) || errors.Is(x, rediscore.ErrCapacity) {
			return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: x.Error()}, nil
		}
		return nil, x
	}
	method := command.GetDialogRequest().Method
	command = nil
	return e.awaitRedisApplicationByeResult(ctx, store, identity, lease, capture, operation, deadline, until, waiter, method)
}

func (e *redisNative) awaitRedisApplicationByeResult(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, lease rediscore.ApplicationLeaseRef, capture redisNotifyWaitCapture, operation string, deadline, leaseUntil time.Time, waiter *nativeResultWaiter, method string) (*pb.OperationResult, error) {
	if store == nil || capture.state == nil || capture.sequence == 0 || waiter == nil || identity.Owner != lease.Application || identity.Term != lease.Term || !time.Now().Before(deadline) || leaseUntil == leaseUntil.Round(0) {
		return nil, errNativeRecord
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	id, err := redisApplicationByeID(capture.fingerprint[:])
	if err != nil {
		return nil, err
	}
	field := "r:result-" + strconv.FormatUint(capture.sequence, 10)
	if method != "ACK" && method != "INVITE" && !redisApplicationClientMethod(method) {
		return nil, errNativeRecord
	}
	prefix := "r:" + redisApplicationClientPrefix(method)
	if method == "INVITE" {
		prefix = "r:app-invite-"
	}
	requestField, responseField := prefix+"origin-"+id, prefix+"response-"+id
	if method == "ACK" {
		prefix = "r:app-ack-"
		requestField, responseField = prefix+"wire-"+id, ""
	}
	for {
		signal := e.resultWaiterSignal(waiter)
		selected, err := store.ObserveApplicationWitness(ctx, identity.Context, identity.Incarnation, lease, "r:context", field)
		if err != nil {
			return nil, err
		}
		current, err := decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			return nil, err
		}
		result, err := decodeNativeResult(selected.View.Records[field])
		if err != nil {
			return nil, err
		}
		if current.Context == nil || !proto.Equal(current.Context.State, capture.state) || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.CommandSequence < capture.sequence || result.Sequence != capture.sequence || result.OperationId != operation || result.DecidingLeaseTerm != lease.Term || !hmac.Equal(result.Fingerprint, capture.fingerprint[:]) || result.ServerRequestId != "" || result.Request != "" && result.Request != requestField || result.Response != "" && (result.Request == "" || result.Response != responseField) {
			return nil, rediscore.ErrConflict
		}
		snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		if result.Pending {
			if result.Output != prefix+"output-"+id || result.CutoffMs <= 0 {
				return nil, errNativeRecord
			}
			offset := result.CutoffMs - leaseUntil.UnixMilli()
			if offset > int64((24*time.Hour)/time.Millisecond) || offset < -int64((24*time.Hour)/time.Millisecond) {
				return nil, errNativeRecord
			}
			cutoff := nativeUDPRetryDeadline(leaseUntil, result.CutoffMs)
			if !time.Now().Before(cutoff) {
				if ctx.Err() != nil || !time.Now().Before(deadline) {
					return nil, context.DeadlineExceeded
				}
				return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command outcome unavailable before deadline", Snapshot: snapshot}, nil
			}
			timer := time.NewTimer(time.Until(cutoff))
			select {
			case <-signal:
			case <-ctx.Done():
			case <-timer.C:
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		answer := &pb.OperationResult{Certainty: result.Certainty, Error: result.Error, SipStatus: result.SipStatus, Snapshot: snapshot}
		fields := make([]string, 0, 2)
		if result.Request != "" {
			fields = append(fields, result.Request)
		}
		if result.Response != "" {
			fields = append(fields, result.Response)
		}
		if len(fields) != 0 {
			wires, x := store.ObserveApplicationWitness(ctx, identity.Context, identity.Incarnation, lease, fields...)
			if x != nil {
				return nil, x
			}
			if wires.Identity != selected.Identity || wires.View.Revision != selected.View.Revision || wires.View.Timer != selected.View.Timer {
				continue
			}
			memo, x := selectedmessage.NewMemo(len(fields))
			if x != nil {
				return nil, x
			}
			messages := &nativeSelectedMessages{records: wires.View.Records, memo: memo, provenance: "admitted"}
			if result.Request != "" {
				message, x := messages.get(result.Request)
				if x != nil {
					return nil, x
				}
				answer.Request, x = projectNativeMessage(message, true)
				if x != nil {
					return nil, x
				}
			}
			if result.Response != "" {
				message, x := messages.get(result.Response)
				if x != nil {
					return nil, x
				}
				answer.Response, x = projectNativeMessage(message, false)
				if x != nil {
					return nil, x
				}
			}
		}
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		return answer, nil
	}
}
