package engine

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// tryCallCompletion is a private qualification seam. Matching native ACK/BYE/CANCEL
// work is independent of a controller's transport, lease or retained state.
func (e *redisNative) tryCallCompletion(parent context.Context, flow *pb.FlowRef, message portable.Message) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if !facts.IsRequest || facts.Method != sip.ACK && facts.Method != sip.BYE && facts.Method != sip.CANCEL {
		return false, nil
	}
	if !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
		return true, errNativeRecord
	}
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return true, err
	}
	defer cancel()
	executor := rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}
	for attempts := 0; attempts < 3; attempts++ {
		handled, matchErr := matchRedisCallCompletion(ctx, store, e.cfg.Process, flow, message, e.cfg.StateIdentityKey, executor, deadline)
		if !errors.Is(matchErr, rediscore.ErrConflict) || ctx.Err() != nil || !time.Now().Before(deadline) {
			return handled, matchErr
		}
	}
	return true, rediscore.ErrConflict
}

func matchRedisCallCompletion(ctx context.Context, store *rediscore.Store, local *pb.ProcessRef, flow *pb.FlowRef, message portable.Message, key []byte, executor rediscore.TimerAssignment, deadline time.Time) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if !facts.IsRequest || facts.Method != sip.ACK && facts.Method != sip.BYE && facts.Method != sip.CANCEL {
		return false, nil
	}
	if store == nil || !validSendProcess(local) || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, local) || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) || ctx.Err() != nil || !time.Now().Before(deadline) {
		return true, errNativeRecord
	}
	if facts.Method == sip.CANCEL {
		selected, matched, selectErr := selectRedisServerCancel(ctx, store, local, flow, message, key, deadline)
		if selectErr != nil || !matched {
			return matched, selectErr
		}
		attributeNativeCall(ctx, selected.context)
		if selected.prior != nil {
			return true, replayRedisServerCancel(ctx, store, selected, key, deadline)
		}
		return true, publishRedisInitialServerCancel(ctx, store, selected, key, executor, deadline)
	}
	if facts.Method == sip.ACK {
		selected, matched, selectErr := selectRedisCallTransactionACK(ctx, store, local, flow, message, deadline)
		if selectErr != nil || matched {
			if selectErr != nil {
				return true, selectErr
			}
			attributeNativeCall(ctx, selected.Context)
			return true, publishRedisCallTransactionACK(ctx, store, selected, flow, executor, deadline)
		}
	} else {
		handled, matchErr := matchRedisCallByeDuplicate(ctx, store, local, flow, message, key, deadline)
		if matchErr != nil || handled {
			return true, matchErr
		}
	}
	selected, err := selectRedisCallDialog(ctx, store, local, flow, message, deadline)
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	attributeNativeCall(ctx, selected.Context)
	if facts.Method == sip.ACK {
		return true, publishRedisCallDialogACK(ctx, store, selected, flow, key, executor, deadline)
	}
	return true, publishRedisCallRemoteBye(ctx, store, selected, flow, key, executor, deadline)
}
