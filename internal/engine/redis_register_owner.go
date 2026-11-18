package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// redisExpiredOwnerSelection is one bounded operation-local context read.
type redisExpiredOwnerSelection struct {
	identity rediscore.Identity
	view     *rediscore.View
	context  *nativepb.Context
}

// expireRedisOwner drains one bounded sample from one expired application term.
// The coordinator fences selection; exact expired owner/term and revision fence
// each release. Physical output and protocol timers remain independent.
func (e *redisNative) expireRedisOwner(ctx context.Context, store *rediscore.Store, term string) error {
	if e == nil || store == nil || ctx.Err() != nil {
		return rediscore.ErrAuthority
	}
	batch, err := store.PeekExpiredApplication(ctx, e.worker, e.cfg.Process.Epoch, term, rediscore.MaxExpiredApplicationBatch)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, item := range batch.Contexts {
		if err = ctx.Err(); err != nil {
			return err
		}
		view, selectErr := store.Read(ctx, item.Identity, "r:context")
		if selectErr == nil && view.Revision != item.Revision {
			selectErr = rediscore.ErrConflict
		}
		var current *nativepb.Context
		if selectErr == nil {
			current, selectErr = decodeNativeContext(view.Records["r:context"])
		}
		if selectErr != nil {
			err = selectErr
		} else {
			capture := &redisExpiredOwnerSelection{identity: item.Identity, view: view, context: current}
			deadline := deadlineOr(ctx, time.Now().Add(2*time.Second))
			switch current.Kind {
			case pb.ContextSnapshot_TRANSACTION:
				if len(current.InitialProcedures) == 1 && strings.HasPrefix(current.InitialProcedures[0], "r:transaction-procedure-") {
					err = releaseExpiredRedisStandalone(ctx, store, item, current, deadline)
				} else {
					err = errNativeRecord
				}

			case pb.ContextSnapshot_UAS, pb.ContextSnapshot_UAC:
				executor := rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}
				err = releaseExpiredRedisCall(ctx, store, item, e.cfg.StateIdentityKey, executor, deadline, capture)
			default:
				err = errors.New("unsupported expired native owner kind")
			}
		}
		if errors.Is(err, rediscore.ErrAuthority) && staleRedisExpiredOwner(ctx, store, item.Identity) {
			continue
		}
		if errors.Is(err, rediscore.ErrConflict) || errors.Is(err, rediscore.ErrAbsent) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// A failed exact-owner operation may race a successful release. This guarded
// reread classifies only that stale item; it never authorizes another write.
func staleRedisExpiredOwner(ctx context.Context, store *rediscore.Store, identity rediscore.Identity) bool {
	observed, err := store.ObserveContext(ctx, identity.Context, identity.Incarnation)
	if errors.Is(err, rediscore.ErrAbsent) {
		return true
	}
	return err == nil && observed.Identity != identity
}

// releaseExpiredRedisRegister releases only a completed direct REGISTER's
// application ownership. Its selected response, physical proof and deadlines
// are not altered, including STARTED/UNKNOWN output.
