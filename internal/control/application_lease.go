package control

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func validApplicationLease(lease *pb.ApplicationLease, application *pb.ProcessRef, authority *pb.StateAuthority) bool {
	return lease != nil && authority != nil && proto.Equal(lease.Application, application) && proto.Equal(lease.Authority, authority) &&
		reference.MatchString(lease.Term) && lease.CheckedAtMs > 0 && lease.ExpiresAtMs > lease.CheckedAtMs &&
		lease.ExpiresAtMs <= 1<<53-1 && lease.ExpiresAtMs-lease.CheckedAtMs >= 30000 && lease.ExpiresAtMs-lease.CheckedAtMs <= 30500
}

// A recovered owner keeps its original application identity, but only its current
// authenticated holder and attachment generation may use the lease.
func (p *session) validLease(lease *pb.ApplicationLease, authority *pb.StateAuthority, hello bool) bool {
	if !slices.Contains(p.capabilities, ControllerReattachCapability) {
		return validApplicationLease(lease, p.process, authority)
	}
	if lease == nil || !validProcess(lease.Application) || !proto.Equal(lease.Holder, p.process) ||
		!proto.Equal(lease.Authority, authority) || !reference.MatchString(lease.Term) || lease.AttachmentGeneration == 0 ||
		lease.CheckedAtMs == 0 || lease.ExpiresAtMs <= lease.CheckedAtMs || lease.ExpiresAtMs > 1<<53-1 ||
		lease.LeaseLifetimeMs == 0 || lease.LeaseLifetimeMs > 300000 || lease.ReattachTimeoutMs > 300000 || lease.ReattachUntilMs > 1<<53-1 {
		return false
	}
	remaining := lease.ExpiresAtMs - lease.CheckedAtMs
	if remaining > uint64(lease.LeaseLifetimeMs)+500 {
		return false
	}
	if hello && (len(lease.RecoveryToken) < 32 || len(lease.RecoveryToken) > 512 || strings.ContainsAny(lease.RecoveryToken, " \r\n\t")) {
		return false
	}
	return true
}

// ErrApplicationLeaseFenced marks a definitive owner-term conflict rather than
// a temporary datastore outage. Only the former terminates a healthy session.
var ErrApplicationLeaseFenced = errors.New("application lease authority fenced")

// The cached term is a session capability, never an authoritative liveness cache.
// Existing authenticated sessions may resume their exact retained, unsealed term.
func (p *session) renewApplicationLease(force ...bool) error {
	p.mu.Lock()
	lease := p.applicationLease
	if lease == nil || (len(force) == 0 || !force[0]) && time.Since(p.leaseRenewed) < 2*time.Second {
		p.mu.Unlock()
		return nil
	}
	term, generation := lease.Term, lease.AttachmentGeneration
	application := proto.Clone(lease.Application).(*pb.ProcessRef)
	p.mu.Unlock()
	if p.ctx.Err() != nil {
		return errors.New("application lease session closed")
	}
	ctx, cancel := context.WithTimeout(p.ctx, 2*time.Second)
	defer cancel()
	var renewed *pb.ApplicationLease
	var err error
	if slices.Contains(p.capabilities, ControllerReattachCapability) {
		renewed, err = p.server.cfg.OnControllerRenew(ctx, &Peer{p})
	} else if p.server.cfg.OnApplicationLease != nil {
		renewed, err = p.server.cfg.OnApplicationLease(ctx, p.process, term)
	} else {
		return errors.New("application renewal unavailable")
	}
	if err != nil {
		return err
	}
	if !p.validLease(renewed, p.server.cfg.StateAuthority, false) || renewed.Term != term ||
		renewed.AttachmentGeneration != generation || !proto.Equal(renewed.Application, application) {
		return fmt.Errorf("%w: renewal changed authority", ErrApplicationLeaseFenced)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil || p.applicationLease == nil || p.applicationLease.Term != term || p.applicationLease.AttachmentGeneration != generation {
		return errors.New("application lease session superseded")
	}
	if renewed.RecoveryToken == "" {
		renewed = proto.Clone(renewed).(*pb.ApplicationLease)
		renewed.RecoveryToken = p.applicationLease.RecoveryToken
	}
	p.applicationLease, p.leaseRenewed = proto.Clone(renewed).(*pb.ApplicationLease), time.Now()
	return nil
}

func (p *Peer) ApplicationLease() *pb.ApplicationLease {
	p.session.mu.Lock()
	defer p.session.mu.Unlock()
	if p.session.applicationLease == nil {
		return nil
	}
	return proto.Clone(p.session.applicationLease).(*pb.ApplicationLease)
}

// ResumeApplicationLease is called before recovered protocol work. It gives the
// original connected owner a renewal opportunity before expired-owner cleanup.
func (p *Peer) ResumeApplicationLease() error { return p.session.renewApplicationLease(true) }
