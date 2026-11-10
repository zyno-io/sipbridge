package engine

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *StateEngine) applicationLeaseID(application *pb.ProcessRef) string {
	return e.id("application-lease", application.GetInstanceId(), application.GetEpoch())
}

func (e *StateEngine) autonomousCompletion() bool {
	return e.store != nil && e.store.Versions() == NativeStateVersions()
}

// ApplicationLease is called only for the authenticated current control session.
// Empty term means a fresh hello; a heartbeat may only renew its existing term.
func (e *StateEngine) ApplicationLease(ctx context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
	if !validSendProcess(application) {
		return nil, errors.New("application lease identity required")
	}
	id := e.applicationLeaseID(application)
	var lease state.ApplicationLease
	var err error
	if term == "" {
		lease, err = e.store.AcquireApplicationLease(ctx, id, uuid.NewString())
	} else {
		lease, err = e.store.ResumeApplicationLease(ctx, id, term)
	}
	if err != nil {
		if errors.Is(err, state.ErrExpired) || errors.Is(err, state.ErrStale) || errors.Is(err, state.ErrCorrupt) || errors.Is(err, state.ErrClock) {
			return nil, errors.Join(control.ErrApplicationLeaseFenced, err)
		}
		return nil, err
	}
	return &pb.ApplicationLease{Application: proto.Clone(application).(*pb.ProcessRef), Authority: e.StateAuthority(), Term: lease.Term,
		CheckedAtMs: uint64(lease.CheckedAtMS), ExpiresAtMs: uint64(lease.ExpiresAtMS)}, nil
}
