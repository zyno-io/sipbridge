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

// expireRedisOwner releases one selected context. A bad context cannot abort
// work for other contexts owned by the same expired application.
func (e *redisNative) expireRedisOwner(ctx context.Context, store *rediscore.Store, item rediscore.ExpiredApplicationContext) error {
	if e == nil || store == nil || ctx.Err() != nil {
		return rediscore.ErrAuthority
	}
	view, err := store.Read(ctx, item.Identity, "r:context")
	if err != nil {
		if errors.Is(err, rediscore.ErrAuthority) && staleRedisExpiredOwner(ctx, store, item.Identity) {
			return rediscore.ErrConflict
		}
		return err
	}
	if view.Revision != item.Revision {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	deadline := deadlineOr(ctx, time.Now().Add(2*time.Second))
	switch current.Kind {
	case pb.ContextSnapshot_TRANSACTION:
		if len(current.InitialProcedures) != 1 || !strings.HasPrefix(current.InitialProcedures[0], "r:transaction-procedure-") {
			return errNativeRecord
		}
		err = releaseExpiredRedisStandalone(ctx, store, item, current, deadline)
	case pb.ContextSnapshot_UAS, pb.ContextSnapshot_UAC:
		capture := &redisExpiredOwnerSelection{identity: item.Identity, view: view, context: current}
		executor := rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}
		err = releaseExpiredRedisCall(ctx, store, item, e.cfg.StateIdentityKey, executor, deadline, capture)
	default:
		return errNativeRecord
	}
	if errors.Is(err, rediscore.ErrAuthority) && staleRedisExpiredOwner(ctx, store, item.Identity) {
		return rediscore.ErrConflict
	}
	return err
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
