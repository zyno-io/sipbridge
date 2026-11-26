package engine

import (
	"context"
	"time"

	"sipbridge/internal/control"
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

// DisconnectController revokes the exact negotiated attachment. The shared
// application term stays usable on its other connections and expires naturally
// once none are renewing it.
func (e *redisNative) DisconnectController(parent context.Context, peer *control.Peer) error {
	lease := peer.ApplicationLease()
	if lease == nil {
		return control.ErrApplicationLeaseFenced
	}
	attachment := e.controlAttachment(peer)
	if attachment == "" {
		return e.DisconnectApplication(parent, lease.Application, lease.Term)
	}
	app, err := nativeApplicationReference(e.cfg.StateIdentityKey, lease.Application)
	if err != nil {
		return err
	}
	ctx, cancel, store, err := e.operation(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	if err != nil {
		return err
	}
	defer cancel()
	return store.CloseApplicationAttachment(ctx, rediscore.ApplicationLeaseRef{
		Application: app, Term: lease.Term, Attachment: attachment,
	})
}
