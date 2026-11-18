package engine

import (
	"context"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func (e *redisNative) DisconnectApplication(parent context.Context, application *pb.ProcessRef, term string) error {
	app, err := nativeApplicationReference(e.cfg.StateIdentityKey, application)
	if err != nil {
		return err
	}
	ctx, cancel, store, err := e.operation(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	if err != nil {
		return err
	}
	defer cancel()
	return store.ExpireApplication(ctx, rediscore.ApplicationLeaseRef{Application: app, Term: term})
}
