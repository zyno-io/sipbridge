package control

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// Peer is one fenced, authenticated Controller session. It never owns SIP contexts.
type Peer struct{ session *session }

func (p *Peer) Principal() string { return p.session.principal }

// Application is the stable owner lineage; Process remains the actual holder.
func (p *Peer) Application() *pb.ProcessRef {
	p.session.mu.Lock()
	defer p.session.mu.Unlock()
	if application := p.session.applicationLease.GetApplication(); application != nil {
		return proto.Clone(application).(*pb.ProcessRef)
	}
	return proto.Clone(p.session.process).(*pb.ProcessRef)
}

func (p *Peer) ApplicationGeneration() uint64 {
	p.session.mu.Lock()
	defer p.session.mu.Unlock()
	return p.session.applicationLease.GetAttachmentGeneration()
}

func (p *Peer) Process() *pb.ProcessRef   { return proto.Clone(p.session.process).(*pb.ProcessRef) }
func (p *Peer) Context() context.Context  { return p.session.ctx }
func (p *Peer) Send(e *pb.Envelope) error { return p.session.write(e) }
func (p *Peer) HasCapability(capability string) bool {
	return slices.Contains(p.session.capabilities, capability)
}

func (p *Peer) Call(ctx context.Context, e *pb.Envelope) (*pb.Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	invoked := time.Now()
	s := p.session
	e = proto.Clone(e).(*pb.Envelope)
	if e.CorrelationId == "" {
		e.CorrelationId = uuid.NewString()
	}
	waiter := make(chan *pb.Envelope, 1)
	s.mu.Lock()
	for id, until := range s.expired {
		if !until.After(time.Now()) {
			delete(s.expired, id)
		}
	}
	if len(s.pending) >= 128 || len(s.expired) >= 1024 {
		s.mu.Unlock()
		return nil, errors.New("control RPC capacity")
	}
	if _, exists := s.pending[e.CorrelationId]; exists {
		s.mu.Unlock()
		return nil, errors.New("duplicate control RPC")
	}
	s.pending[e.CorrelationId] = waiter
	s.mu.Unlock()
	admitted, completed := false, false
	defer func() {
		s.mu.Lock()
		if admitted && !completed {
			s.expired[e.CorrelationId] = time.Now().Add(5 * time.Minute)
		}
		delete(s.pending, e.CorrelationId)
		s.mu.Unlock()
	}()
	transport, cancel := context.WithTimeout(s.ctx, s.server.cfg.WriteTimeout)
	if err := s.acquireWrite(ctx, transport); err != nil {
		cancel()
		return nil, err
	}
	admitted = true
	written := make(chan error, 1)
	go func() {
		defer cancel()
		defer s.releaseWrite()
		written <- s.writeAdmitted(transport, ctx, e, invoked)
	}()
	select {
	case err := <-written:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
	select {
	case result := <-waiter:
		completed = true
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, errors.New("control session closed; outcome unknown")
	}
}

func (s *Server) Peer(process *pb.ProcessRef) (*Peer, error) {
	if process == nil {
		return nil, errors.New("missing Controller owner")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var selected *session
	var selectedGeneration uint64
	for _, claim := range s.claims {
		current := claim.current
		if current == nil || !current.established || current.ctx.Err() != nil {
			continue
		}
		current.mu.Lock()
		application := current.applicationLease.GetApplication()
		generation := current.applicationLease.GetAttachmentGeneration()
		matches := func(candidate *pb.ProcessRef) bool {
			return candidate != nil && candidate.InstanceId == process.InstanceId && (process.Epoch == "" || candidate.Epoch == process.Epoch)
		}
		matchesOwner := matches(application) || matches(current.process)
		current.mu.Unlock()
		if matchesOwner && (selected == nil || generation > selectedGeneration) {
			selected, selectedGeneration = current, generation
		}
	}
	if selected == nil {
		return nil, errors.New("controller owner unavailable")
	}
	return &Peer{selected}, nil
}

// HasReadyPeer reads the same cached eligibility as ReadyPeers without building
// a peer list or asking the controller to perform work.
func (s *Server) HasReadyPeer() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.claims {
		p := c.current
		if p == nil || !p.established || p.ctx.Err() != nil {
			continue
		}
		p.mu.Lock()
		ready := p.status.GetRetainedReady() && p.status.GetBindingsReady()
		p.mu.Unlock()
		if ready {
			return true
		}
	}
	return false
}

func (s *Server) ReadyPeers() []*Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	peers := make([]*Peer, 0)
	for _, c := range s.claims {
		p := c.current
		if p == nil || !p.established || p.ctx.Err() != nil {
			continue
		}
		p.mu.Lock()
		ready := p.status.GetRetainedReady() && p.status.GetBindingsReady()
		p.mu.Unlock()
		if ready {
			peers = append(peers, &Peer{p})
		}
	}
	return peers
}
