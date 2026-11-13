package engine

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type residentLease struct {
	application *pb.ProcessRef
	term        string
	expires     time.Time
	generation  uint64
}

func residentControllerKey(application *pb.ProcessRef) string {
	return application.GetInstanceId() + "/" + application.GetEpoch()
}

func (e *ResidentMemory) ApplicationLease(ctx context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validSendProcess(application) {
		return nil, errors.New("application lease identity required")
	}
	now := time.Now()
	id := residentControllerKey(application)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, errors.New("resident engine closed")
	}
	lease := e.leases[id]
	if term == "" {
		// A new authenticated hello can reuse its process identity only while
		// that process's lease is live. It cannot revive an expired term.
		if lease == nil || !now.Before(lease.expires) {
			if len(e.leases) >= 1024 && lease == nil {
				return nil, errors.New("controller capacity")
			}
			if lease != nil {
				e.scheduleResidentOwnerExpiryLocked(lease.application, lease.term)
			}
			lease = &residentLease{application: proto.Clone(application).(*pb.ProcessRef), term: uuid.NewString()}
			e.leases[id] = lease
		}
	} else if lease == nil || lease.term != term || !now.Before(lease.expires) {
		return nil, control.ErrApplicationLeaseFenced
	}
	lease.expires = now.Add(state.ApplicationLeaseLifetime)
	lease.generation++
	e.deadlines.set(residentDeadlineKey{kind: residentControllerDeadline, record: id}, lease.generation, lease.expires)
	e.wakeDeadline()
	return &pb.ApplicationLease{Application: proto.Clone(application).(*pb.ProcessRef), Authority: e.StateAuthority(), Term: lease.term,
		CheckedAtMs: uint64(now.UnixMilli()), ExpiresAtMs: uint64(lease.expires.UnixMilli()), LeaseLifetimeMs: uint32(state.ApplicationLeaseLifetime.Milliseconds())}, nil
}

func (e *ResidentMemory) ControllerSession(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
	if hello.GetRecovery() != nil {
		return nil, errors.New("controller recovery is unavailable for memory authority")
	}
	return e.ApplicationLease(ctx, peer.Process(), "")
}

func (e *ResidentMemory) ControllerRenew(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
	previous := peer.ApplicationLease()
	if previous == nil || previous.Term == "" {
		return nil, control.ErrApplicationLeaseFenced
	}
	return e.ApplicationLease(ctx, peer.Application(), previous.Term)
}

func (e *ResidentMemory) ControllerReady(ctx context.Context, peer *control.Peer) error {
	return e.validateController(ctx, peer)
}

func (e *ResidentMemory) validateController(ctx context.Context, peer *control.Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if peer == nil || !validSendProcess(peer.Application()) {
		return control.ErrApplicationLeaseFenced
	}
	current := peer.ApplicationLease()
	e.mu.Lock()
	lease := e.leases[residentControllerKey(peer.Application())]
	valid := !e.closed && current != nil && lease != nil && lease.term == current.Term && time.Now().Before(lease.expires)
	e.mu.Unlock()
	if !valid {
		return control.ErrApplicationLeaseFenced
	}
	return nil
}

// expireController is called with no engine or context lock held. Lease expiry
// removes one controller record and visits only that expired term's index.
func (e *ResidentMemory) expireController(item residentDeadline) bool {
	if item.key.kind != residentControllerDeadline {
		return false
	}
	id := item.key.record
	e.mu.Lock()
	lease := e.leases[id]
	var application *pb.ProcessRef
	var term string
	if lease != nil && lease.generation == item.generation && !time.Now().Before(lease.expires) {
		delete(e.leases, id)
		if !e.closed {
			application, term = lease.application, lease.term
			e.wg.Add(1)
		}
	}
	e.mu.Unlock()
	if application != nil {
		go func() { defer e.wg.Done(); e.expireResidentOwner(application, term) }()
	}
	return true
}

// The engine lock serializes this worker's admission with Close. Cleanup visits
// only the expired term's index and runs without holding the scheduler or mutex.
func (e *ResidentMemory) scheduleResidentOwnerExpiryLocked(application *pb.ProcessRef, term string) {
	if e.closed {
		return
	}
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.expireResidentOwner(application, term) }()
}
