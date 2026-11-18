package control

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// ApplicationCapability is the actual authenticated session's local publication
// capability. It is not a second application lease authority. Its monotonic bound
// starts before the existing lease request, never when its reply arrives.
type ApplicationCapability struct {
	mu          sync.Mutex
	application *pb.ProcessRef
	term        string
	until       time.Time
	released    bool
	notify      sync.Once
	onRelease   func(*ApplicationCapability)
}

func newApplicationCapability(lease *pb.ApplicationLease, started time.Time, released func(*ApplicationCapability)) *ApplicationCapability {
	return &ApplicationCapability{application: proto.Clone(lease.Application).(*pb.ProcessRef), term: lease.Term,
		until: started.Add(time.Duration(lease.ExpiresAtMs-lease.CheckedAtMs) * time.Millisecond), onRelease: released}
}

// Guard serializes only bounded local publication with invalidation. The caller
// may hold its context lock; publish must do no I/O or callbacks and must never
// acquire Peer/session/Server locks. Native indexes are acquired last.
func (c *ApplicationCapability) Guard(application *pb.ProcessRef, term string, publish func(time.Time) error) error {
	if c == nil {
		return ErrApplicationLeaseFenced
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released || !time.Now().Before(c.until) || c.term != term || !proto.Equal(c.application, application) {
		return ErrApplicationLeaseFenced
	}
	if publish != nil {
		return publish(c.until)
	}
	return nil
}

func (c *ApplicationCapability) renew(lease *pb.ApplicationLease, started time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released || !time.Now().Before(c.until) || lease.Term != c.term || !proto.Equal(lease.Application, c.application) {
		return ErrApplicationLeaseFenced
	}
	until := started.Add(time.Duration(lease.ExpiresAtMs-lease.CheckedAtMs) * time.Millisecond)
	if !time.Now().Before(until) {
		return ErrApplicationLeaseFenced
	}
	c.until = until
	return nil
}

func (c *ApplicationCapability) invalidate() {
	if c != nil {
		c.mu.Lock()
		c.released = true
		c.mu.Unlock()
	}
}

// release runs only outside session, native context and capability locks. The
// established-session defer completes it before releasing the control claim.
func (c *ApplicationCapability) release() {
	if c == nil {
		return
	}
	c.invalidate()
	c.notify.Do(func() {
		if c.onRelease != nil {
			c.onRelease(c)
		}
	})
}

func (p *Peer) ApplicationCapability(server ...*Server) *ApplicationCapability {
	if len(server) > 1 || len(server) == 1 && p.session.server != server[0] {
		return nil
	}
	p.session.mu.Lock()
	defer p.session.mu.Unlock()
	return p.session.applicationCapability
}

// SetApplicationCapabilityRelease is wired once before serving. It connects the
// sole local owner to existing session invalidation, not a lease registry.
func (s *Server) SetApplicationCapabilityRelease(released func(*ApplicationCapability)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if released == nil || s.capabilityRelease != nil || len(s.all) != 0 || s.closed {
		return errors.New("application capability release must be wired before serving")
	}
	s.capabilityRelease = released
	return nil
}

// Identity is immutable; it is safe to read without the publication mutex.
func (c *ApplicationCapability) Application() *pb.ProcessRef {
	return proto.Clone(c.application).(*pb.ProcessRef)
}
func (c *ApplicationCapability) Term() string { return c.term }
