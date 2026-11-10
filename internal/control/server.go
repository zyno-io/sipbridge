package control

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

const Subprotocol = "sipbridge.control.json.v1"
const MaxEnvelope = 1024 * 1024
const LocalChallengeCapability = "sip.local-challenge.v1"

var reference = regexp.MustCompile(`^[A-Za-z0-9_.:@+\-]{1,96}$`)

type Config struct {
	Process                                                                         *pb.ProcessRef
	TLS                                                                             *TLSContext
	Authenticator                                                                   Authenticator
	AllowPlain                                                                      bool
	AllowedURIs                                                                     []string
	MaxSessions                                                                     int
	MaxClaims                                                                       int
	HelloTimeout, WriteTimeout, HeartbeatInterval, HeartbeatTimeout, ReloadInterval time.Duration
	OnReloadError                                                                   func(error)
	Capabilities                                                                    []string
	InboundAuthPolicy                                                               *pb.InboundAuthPolicy
	StateAuthority                                                                  *pb.StateAuthority
	OnMessage                                                                       func(*Peer, *pb.Envelope) error
	OnStatus                                                                        func(*Peer, *pb.SessionStatus) error
	OnApplicationLease                                                              func(context.Context, *pb.ProcessRef, string) (*pb.ApplicationLease, error)
	OnControllerSession                                                             func(context.Context, *Peer, *pb.SessionHello) (*pb.ApplicationLease, error)
	OnControllerRenew                                                               func(context.Context, *Peer) (*pb.ApplicationLease, error)
	OnDiagnostic                                                                    func(Diagnostic)
}

type claim struct {
	principal string
	pending   *session
	epoch     string
	attempt   uint64
	current   *session
	expires   time.Time
}

type Server struct {
	cfg    Config
	mu     sync.Mutex
	claims map[string]*claim
	all    map[*session]struct{}
	slots  chan struct{}
	closed bool
	http   *http.Server
}

func NewServer(cfg Config) (*Server, error) {
	if !validProcess(cfg.Process) {
		return nil, errors.New("control process identity is required")
	}
	if cfg.Authenticator == nil {
		authenticator, err := NewMTLSAuthenticator(cfg.AllowedURIs)
		if err != nil {
			return nil, err
		}
		cfg.Authenticator = authenticator
	}
	if !cfg.Authenticator.Valid() {
		return nil, errors.New("control authentication is unavailable")
	}
	if cfg.TLS == nil && !cfg.AllowPlain || cfg.TLS != nil && cfg.AllowPlain {
		return nil, errors.New("select TLS or explicit plaintext control transport")
	}
	if _, mutual := cfg.Authenticator.(*MTLSAuthenticator); mutual && (cfg.TLS == nil || cfg.TLS.public) {
		return nil, errors.New("mutual TLS authentication requires verified client certificates")
	}
	if _, bearer := cfg.Authenticator.(*BearerAuthenticator); bearer {
		if len(cfg.AllowedURIs) != 0 || cfg.TLS != nil && !cfg.TLS.public {
			return nil, errors.New("bearer authentication requires no client-certificate settings")
		}
	}
	if cfg.InboundAuthPolicy != nil && !slices.Contains(cfg.Capabilities, LocalChallengeCapability) {
		return nil, errors.New("initial authentication policy requires local challenge capability")
	}
	if HasStateCapability(cfg.Capabilities) {
		a := cfg.StateAuthority
		if !slices.Contains(cfg.Capabilities, "sip.v1") || a == nil || !reference.MatchString(a.Service) || !reference.MatchString(a.Generation) || !reference.MatchString(a.PrimaryId) || a.SchemaVersion == 0 {
			return nil, errors.New("state capability requires a valid state authority")
		}
	}
	if slices.Contains(cfg.Capabilities, NativeCompletionCapability) &&
		(!HasStateCapability(cfg.Capabilities) || cfg.StateAuthority.GetSchemaVersion() != 1 || cfg.OnApplicationLease == nil && cfg.OnControllerSession == nil) {
		return nil, errors.New("native completion requires schema 1 and an application lease authority")
	}
	if slices.Contains(cfg.Capabilities, ControllerReattachCapability) &&
		(!slices.Contains(cfg.Capabilities, NativeCompletionCapability) || cfg.OnControllerSession == nil || cfg.OnControllerRenew == nil) {
		return nil, errors.New("controller reattach requires session and renewal hooks")
	}
	for _, u := range cfg.AllowedURIs {
		if u == "" || strings.Contains(u, "*") {
			return nil, errors.New("URI identity allowlist must be exact")
		}
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = 64
	}
	if cfg.MaxClaims == 0 {
		cfg.MaxClaims = 256
	}
	if cfg.MaxSessions < 1 || cfg.MaxSessions > 64 || cfg.MaxClaims < cfg.MaxSessions || cfg.MaxClaims > 4096 {
		return nil, errors.New("invalid control admission bounds")
	}
	if cfg.HelloTimeout == 0 {
		cfg.HelloTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 2 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 6 * time.Second
	}
	if cfg.ReloadInterval == 0 {
		cfg.ReloadInterval = 5 * time.Second
	}
	if cfg.HelloTimeout <= 0 || cfg.HelloTimeout > 5*time.Second || cfg.WriteTimeout <= 0 || cfg.WriteTimeout > 2*time.Second || cfg.HeartbeatInterval < time.Millisecond || cfg.HeartbeatInterval > time.Minute || cfg.HeartbeatTimeout < 2*cfg.HeartbeatInterval || cfg.HeartbeatTimeout > 2*time.Minute || cfg.ReloadInterval <= 0 {
		return nil, errors.New("invalid control deadlines")
	}
	s := &Server{cfg: cfg, claims: make(map[string]*claim), all: make(map[*session]struct{}), slots: make(chan struct{}, cfg.MaxSessions)}
	s.http = &http.Server{Handler: s, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	return s, nil
}

func validProcess(p *pb.ProcessRef) bool {
	return p != nil && reference.MatchString(p.InstanceId) && reference.MatchString(p.Epoch)
}

// CredentialsValid reports only the selected control transport and authenticator.
// Public SIP TLS and state readiness have their own checks.
func (s *Server) CredentialsValid() bool {
	return s.cfg.Authenticator.Valid() && (s.cfg.TLS == nil || s.cfg.TLS.Valid())
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if s.cfg.TLS == nil {
		address, ok := listener.Addr().(*net.TCPAddr)
		if !ok || !address.IP.IsLoopback() {
			return errors.New("plaintext control requires a loopback listener")
		}
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(s.cfg.ReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				s.Close()
				return
			case <-ticker.C:
				if s.cfg.TLS != nil {
					if err := s.cfg.TLS.Reload(); err != nil && s.cfg.OnReloadError != nil {
						s.cfg.OnReloadError(err)
					}
				}
				if err := s.cfg.Authenticator.Reload(); err != nil && s.cfg.OnReloadError != nil {
					s.cfg.OnReloadError(err)
				}
			}
		}
	}()
	if s.cfg.TLS != nil {
		listener = tls.NewListener(listener, s.cfg.TLS.Config())
	}
	err := s.http.Serve(listener)
	s.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for p := range s.all {
		p.stop()
	}
	s.mu.Unlock()
	s.http.Close()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, err := s.cfg.Authenticator.Authenticate(r)
	if err != nil || principal == "" || r.TLS == nil && !s.cfg.AllowPlain {
		status := http.StatusForbidden
		if _, bearer := s.cfg.Authenticator.(*BearerAuthenticator); bearer {
			status = http.StatusUnauthorized
			w.Header().Set("WWW-Authenticate", "Bearer")
		}
		http.Error(w, "control authentication failed", status)
		return
	}
	if r.URL.Path == "/sip-readyz" {
		http.Error(w, "SIP service is not implemented", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Path != "/v1/control" || r.URL.RawQuery != "" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	protocols := strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",")
	for i := range protocols {
		protocols[i] = strings.TrimSpace(protocols[i])
	}
	if r.Header.Get("Origin") != "" || !slices.Contains(protocols, Subprotocol) {
		http.Error(w, "unsupported control upgrade", http.StatusBadRequest)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "control session capacity", http.StatusServiceUnavailable)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}, CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(MaxEnvelope)
	ctx, cancel := context.WithCancel(r.Context())
	p := &session{server: s, principal: principal, conn: conn, ctx: ctx, cancel: cancel, bound: MaxEnvelope, pending: make(map[string]chan *pb.Envelope), expired: make(map[string]time.Time)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return
	}
	s.all[p] = struct{}{}
	s.mu.Unlock()
	defer func() { p.stop(); s.release(p) }()
	if err := p.hello(); err != nil {
		conn.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	go p.heartbeat()
	for {
		envelope, err := p.read(p.ctx)
		if err != nil {
			return
		}
		if !p.handle(envelope) {
			p.diagnostic("protocol", nil, envelope, false)
			conn.Close(websocket.StatusPolicyViolation, "invalid control message")
			return
		}
	}
}

func (s *Server) release(p *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.all, p)
	if p.process != nil {
		if c := s.claims[p.process.InstanceId]; c != nil {
			if c.pending == p {
				c.pending = nil
			}
			if c.current == p {
				c.current = nil
				c.expires = time.Now().Add(5 * time.Minute)
			}
			if c.current == nil && c.pending == nil && c.epoch == "" {
				delete(s.claims, p.process.InstanceId)
			}
		}
	}
}

func (s *Server) reserveRecoveryClaim(p *session, hello *pb.SessionHello) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("server stopping")
	}
	now := time.Now()
	for id, c := range s.claims {
		if c.current == nil && c.pending == nil && !now.Before(c.expires) {
			delete(s.claims, id)
		}
	}
	c := s.claims[hello.Controller.InstanceId]
	if c != nil {
		if c.pending != nil {
			return errors.New("controller handshake in progress")
		}
		if c.principal != p.principal {
			return errors.New("controller identity claim conflict")
		}
		if c.epoch == hello.Controller.Epoch && hello.ConnectionAttempt <= c.attempt {
			return errors.New("stale connection attempt")
		}
		if c.epoch != hello.Controller.Epoch && c.current != nil && hello.Recovery == nil {
			return errors.New("active epoch collision")
		}
	} else {
		if len(s.claims) >= s.cfg.MaxClaims {
			return errors.New("claim capacity")
		}
		c = &claim{principal: p.principal}
		s.claims[hello.Controller.InstanceId] = c
	}
	p.process = proto.Clone(hello.Controller).(*pb.ProcessRef)
	c.pending = p
	return nil
}

func (s *Server) acceptClaim(p *session, hello *pb.SessionHello) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("server stopping")
	}
	now := time.Now()
	for id, c := range s.claims {
		if c.current == nil && c.pending == nil && !now.Before(c.expires) {
			delete(s.claims, id)
		}
	}
	c := s.claims[hello.Controller.InstanceId]
	if c != nil {
		if c.pending != nil && c.pending != p {
			return errors.New("controller handshake in progress")
		}
		if c.principal != p.principal {
			return errors.New("controller identity claim conflict")
		}
		if c.epoch == hello.Controller.Epoch && hello.ConnectionAttempt <= c.attempt {
			return errors.New("stale connection attempt")
		}
		if c.epoch != hello.Controller.Epoch && c.current != nil && (c.pending != p || hello.Recovery == nil) {
			return errors.New("active epoch collision")
		}
	} else if len(s.claims) >= s.cfg.MaxClaims {
		return errors.New("claim capacity")
	}
	if c != nil && c.current != nil {
		c.current.stop()
	}
	s.claims[hello.Controller.InstanceId] = &claim{principal: p.principal, epoch: hello.Controller.Epoch, attempt: hello.ConnectionAttempt, current: p}
	p.process = hello.Controller
	if slices.Contains(p.capabilities, ControllerReattachCapability) {
		application, generation := p.applicationLease.GetApplication(), p.applicationLease.GetAttachmentGeneration()
		for _, otherClaim := range s.claims {
			other := otherClaim.current
			if other == nil || other == p || other.principal != p.principal {
				continue
			}
			other.mu.Lock()
			stale := proto.Equal(other.applicationLease.GetApplication(), application) && other.applicationLease.GetAttachmentGeneration() < generation
			other.mu.Unlock()
			if stale {
				other.stop()
			}
		}
	}
	return nil
}

type session struct {
	server           *Server
	conn             *websocket.Conn
	ctx              context.Context
	cancel           context.CancelFunc
	process          *pb.ProcessRef
	principal        string
	established      bool // Published under server.mu after HelloAck is written.
	id               string
	bound            int
	mu               sync.Mutex
	writeOnce        sync.Once
	writeGate        chan struct{}
	nonce            string
	sent             time.Time
	capabilities     []string
	status           *pb.SessionStatus
	applicationLease *pb.ApplicationLease
	leaseRenewed     time.Time
	pending          map[string]chan *pb.Envelope
	expired          map[string]time.Time
}

const CommandBudgetCapability = "sip.command-budget.v1"
const CommandAdmissionCapability = "sip.command-admission.v1"
const DialogContinuityCapability = "sip.dialog-continuity.v1"
const StateCapability = "sip.state.v1"

// HasStateCapability requires the common state contract for every adapter.
func HasStateCapability(capabilities []string) bool {
	return slices.Contains(capabilities, StateCapability)
}

const NativeCompletionCapability = "sip.native-completion.v1"
const ControllerReattachCapability = "sip.controller-reattach.v1"

func (p *session) stop() { p.cancel(); p.conn.CloseNow() }

func (p *session) read(ctx context.Context) (*pb.Envelope, error) {
	kind, data, err := p.conn.Read(ctx)
	if err != nil {
		p.diagnostic("read-transport", err, nil, false)
		return nil, err
	}
	if kind != websocket.MessageText || len(data) == 0 || len(data) > p.bound {
		p.diagnostic("read-frame", nil, nil, false)
		return nil, errors.New("invalid frame")
	}
	e, err := decodeJSONEnvelope(data)
	if err != nil {
		p.diagnostic("read-decode", nil, nil, false)
		return nil, err
	}
	limit := uint32(2000)
	if e.GetCommand() != nil && slices.Contains(p.capabilities, CommandBudgetCapability) {
		limit = 40000
	}
	if !reference.MatchString(e.CorrelationId) || e.Payload == nil || e.RemainingBudgetMs > limit {
		p.diagnostic("read-envelope", nil, e, false)
		return nil, errors.New("invalid envelope")
	}
	return e, nil
}

// acquireWrite never starts a worker for an expired queued RPC. The token
// bounds detached physical writes to exactly one per authenticated session.
func (p *session) acquireWrite(caller, transport context.Context) error {
	if err := caller.Err(); err != nil {
		return err
	}
	if err := transport.Err(); err != nil {
		return err
	}
	p.writeOnce.Do(func() { p.writeGate = make(chan struct{}, 1) })
	select {
	case p.writeGate <- struct{}{}:
		if err := caller.Err(); err != nil {
			p.releaseWrite()
			return err
		}
		if err := transport.Err(); err != nil {
			p.releaseWrite()
			return err
		}
		return nil
	case <-caller.Done():
		return caller.Err()
	case <-transport.Done():
		return transport.Err()
	}
}

func (p *session) releaseWrite() { <-p.writeGate }

func (p *session) write(e *pb.Envelope) error {
	ctx, cancel := context.WithTimeout(p.ctx, p.server.cfg.WriteTimeout)
	defer cancel()
	if err := p.acquireWrite(ctx, ctx); err != nil {
		return err
	}
	defer p.releaseWrite()
	return p.writeAdmitted(ctx, p.ctx, e, time.Time{})
}

// Once admitted, the physical frame belongs to the session transport deadline,
// even when its RPC caller stops waiting. Only the writer releases its token.
func (p *session) writeAdmitted(transport, caller context.Context, e *pb.Envelope, invoked time.Time) error {
	if err := caller.Err(); err != nil {
		return err
	}
	if err := transport.Err(); err != nil {
		return err
	}
	if !invoked.IsZero() && e.RemainingBudgetMs > 0 {
		remaining := time.Duration(e.RemainingBudgetMs)*time.Millisecond - time.Since(invoked)
		if deadline, ok := caller.Deadline(); ok && time.Until(deadline) < remaining {
			remaining = time.Until(deadline)
		}
		if remaining < time.Millisecond {
			return context.DeadlineExceeded
		}
		e.RemainingBudgetMs = uint32(remaining / time.Millisecond)
	}
	e.SessionId = p.id
	data, err := encodeJSONEnvelope(e)
	if err != nil {
		return err
	}
	if len(data) > p.bound {
		return errors.New("outgoing envelope exceeds negotiated bound")
	}
	if err := caller.Err(); err != nil {
		return err
	}
	if err := transport.Err(); err != nil {
		return err
	}
	err = p.conn.Write(transport, websocket.MessageText, data)
	if err != nil {
		p.diagnostic("write", err, e, caller.Err() != nil)
	}
	return err
}

func (p *session) hello() error {
	ctx, cancel := context.WithTimeout(p.ctx, p.server.cfg.HelloTimeout)
	defer cancel()
	e, err := p.read(ctx)
	if err != nil {
		return errors.New("hello required")
	}
	h := e.GetHello()
	if e.SessionId != "" || e.RemainingBudgetMs != 0 || h == nil || !validProcess(h.Controller) || h.ConnectionAttempt == 0 || h.Major != 1 || h.MinMinor > h.MaxMinor || h.MinMinor > 0 || h.Bounds == nil || h.Bounds.MaxEnvelopeBytes < 512 || h.Bounds.MaxEnvelopeBytes > MaxEnvelope || len(h.Capabilities) > 32 {
		return errors.New("unsupported hello")
	}
	seen := make(map[string]bool)
	for _, capability := range h.Capabilities {
		if !reference.MatchString(capability) || seen[capability] {
			return errors.New("invalid capabilities")
		}
		seen[capability] = true
		if slices.Contains(p.server.cfg.Capabilities, capability) {
			p.capabilities = append(p.capabilities, capability)
		}
	}
	if HasStateCapability(p.server.cfg.Capabilities) && (!slices.Contains(p.capabilities, "sip.v1") || !HasStateCapability(p.capabilities)) {
		return errors.New("state runtime requires sip.v1 and a compatible state capability")
	}
	if slices.Contains(p.server.cfg.Capabilities, NativeCompletionCapability) && !slices.Contains(p.capabilities, NativeCompletionCapability) {
		return errors.New("state runtime requires sip.native-completion.v1")
	}
	if p.server.cfg.InboundAuthPolicy != nil && !slices.Contains(p.capabilities, LocalChallengeCapability) {
		return errors.New("configured initial authentication requires sip.local-challenge.v1")
	}
	if h.Recovery != nil && !slices.Contains(p.capabilities, ControllerReattachCapability) {
		return errors.New("controller recovery capability required")
	}
	if recovery := h.Recovery; recovery != nil && (!validProcess(recovery.Application) || !reference.MatchString(recovery.Term) ||
		!reference.MatchString(recovery.AttemptId) || recovery.AttachmentGeneration == 0 ||
		len(recovery.Token) < 32 || len(recovery.Token) > 512 || strings.ContainsAny(recovery.Token, " \r\n\t")) {
		return errors.New("invalid controller recovery request")
	}
	p.id = uuid.NewString()
	p.bound = int(h.Bounds.MaxEnvelopeBytes)
	p.conn.SetReadLimit(int64(p.bound))
	recoverable := slices.Contains(p.capabilities, ControllerReattachCapability)
	if recoverable {
		if err := p.server.reserveRecoveryClaim(p, h); err != nil {
			return err
		}
	} else if err := p.server.acceptClaim(p, h); err != nil {
		return err
	}
	var authority *pb.StateAuthority
	if HasStateCapability(p.capabilities) {
		authority = p.server.cfg.StateAuthority
	}
	if slices.Contains(p.capabilities, NativeCompletionCapability) {
		var lease *pb.ApplicationLease
		if slices.Contains(p.capabilities, ControllerReattachCapability) {
			lease, err = p.server.cfg.OnControllerSession(ctx, &Peer{p}, h)
		} else if p.server.cfg.OnApplicationLease != nil {
			lease, err = p.server.cfg.OnApplicationLease(ctx, p.process, "")
		} else {
			return errors.New("native application lease unavailable")
		}
		if err != nil || !p.validLease(lease, authority, true) {
			return errors.New("native application lease acquisition failed")
		}
		if slices.Contains(p.capabilities, ControllerReattachCapability) {
			if h.Recovery == nil && !proto.Equal(lease.Application, p.process) || h.Recovery != nil &&
				(!proto.Equal(lease.Application, h.Recovery.Application) || lease.Term != h.Recovery.Term) {
				return errors.New("controller recovery owner mismatch")
			}
		}
		p.applicationLease, p.leaseRenewed = proto.Clone(lease).(*pb.ApplicationLease), time.Now()
	}
	if recoverable {
		if err := p.server.acceptClaim(p, h); err != nil {
			return err
		}
	}
	if err := p.write(&pb.Envelope{CorrelationId: e.CorrelationId, Payload: &pb.Envelope_HelloAck{HelloAck: &pb.SessionHelloAck{
		Sipbridge: p.server.cfg.Process, SessionId: p.id, Major: 1, Minor: 0, Capabilities: p.capabilities, Bounds: &pb.Bounds{MaxEnvelopeBytes: uint32(p.bound)},
		HeartbeatIntervalMs: uint32(p.server.cfg.HeartbeatInterval / time.Millisecond), HeartbeatTimeoutMs: uint32(p.server.cfg.HeartbeatTimeout / time.Millisecond),
		StateAuthority: authority, ApplicationLease: p.applicationLease, InboundAuthPolicy: p.server.cfg.InboundAuthPolicy,
	}}}); err != nil {
		return err
	}
	p.server.mu.Lock()
	p.established = true
	p.server.mu.Unlock()
	return nil
}

func (p *session) handle(e *pb.Envelope) bool {
	if p.ctx.Err() != nil || e.SessionId != p.id {
		return false
	}
	switch payload := e.Payload.(type) {
	case *pb.Envelope_Hello, *pb.Envelope_HelloAck:
		return false
	case *pb.Envelope_Ping:
		if e.RemainingBudgetMs != 0 {
			return false
		}
		if !reference.MatchString(payload.Ping.GetNonce()) {
			return false
		}
		return p.write(&pb.Envelope{CorrelationId: e.CorrelationId, Payload: &pb.Envelope_Pong{Pong: payload.Ping}}) == nil
	case *pb.Envelope_Pong:
		if e.RemainingBudgetMs != 0 {
			return false
		}
		p.mu.Lock()
		if p.nonce == "" || payload.Pong.GetNonce() != p.nonce || e.CorrelationId != p.nonce || time.Since(p.sent) >= p.server.cfg.HeartbeatTimeout {
			p.mu.Unlock()
			return false
		}
		p.nonce = ""
		p.mu.Unlock()
		err := p.renewApplicationLease()
		return err == nil || !errors.Is(err, ErrApplicationLeaseFenced)
	case *pb.Envelope_Status:
		if e.RemainingBudgetMs != 0 {
			return false
		}
		s := payload.Status
		if s == nil {
			return false
		}
		if p.server.cfg.OnStatus == nil {
			return !s.AdmitCalls && !s.AdmitSubscriptions && !s.RetainedReady && !s.BindingsReady
		}
		if !slices.Contains(p.capabilities, "sip.v1") {
			return false
		}
		if p.server.cfg.OnStatus(&Peer{p}, s) != nil {
			return false
		}
		p.mu.Lock()
		p.status = proto.Clone(s).(*pb.SessionStatus)
		p.mu.Unlock()
		return true
	default:
		if !slices.Contains(p.capabilities, "sip.v1") {
			return false
		}
		if slices.Contains(p.capabilities, ControllerReattachCapability) && (e.GetIngest() != nil || e.GetCommand() != nil) {
			p.mu.Lock()
			ready := p.status.GetRetainedReady() && p.status.GetBindingsReady()
			p.mu.Unlock()
			if !ready {
				return p.write(&pb.Envelope{CorrelationId: e.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{
					Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "controller recovery is not ready",
				}}}) == nil
			}
		}
		if inventory := e.GetControllerContexts(); inventory != nil {
			if !slices.Contains(p.capabilities, ControllerReattachCapability) || inventory.Limit > 64 ||
				len(inventory.Cursor) > 32768 || strings.ContainsAny(inventory.Cursor, " \r\n\t") {
				return false
			}
		}
		if (e.GetSendPermit() != nil || e.GetCredentialResult() != nil || e.GetInspectEffect() != nil || e.GetDeliverEffect() != nil || e.GetCredentialLookup() != nil || e.GetProbeApplication() != nil || e.GetApplicationStatus() != nil) && !HasStateCapability(p.capabilities) {
			return false
		}
		if ingest := e.GetIngest(); ingest != nil && (ingest.Admission == pb.OwnerIngest_TLS_DIALOG_CONTINUITY || ingest.Admission == pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY) && !slices.Contains(p.capabilities, DialogContinuityCapability) {
			return false
		}
		if e.GetResult() != nil || e.GetDelivery() != nil || e.GetRouteResult() != nil || e.GetFlowAdoptionResult() != nil || e.GetSendPermit() != nil || e.GetCredentialResult() != nil || e.GetApplicationStatus() != nil {
			p.mu.Lock()
			waiter := p.pending[e.CorrelationId]
			_, expired := p.expired[e.CorrelationId]
			p.mu.Unlock()
			if waiter != nil {
				select {
				case waiter <- e:
				default:
				}
				return true
			}
			return expired
		}
		return p.server.cfg.OnMessage != nil && p.server.cfg.OnMessage(&Peer{p}, e) == nil
	}
}

func (p *session) heartbeat() {
	ticker := time.NewTicker(p.server.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case now := <-ticker.C:
			p.mu.Lock()
			if p.nonce != "" {
				expired := now.Sub(p.sent) >= p.server.cfg.HeartbeatTimeout
				p.mu.Unlock()
				if expired {
					p.diagnostic("heartbeat-timeout", nil, nil, false)
					p.stop()
					return
				}
				continue
			}
			id := uuid.NewString()
			p.nonce = id
			p.sent = now
			p.mu.Unlock()
			if p.write(&pb.Envelope{CorrelationId: id, Payload: &pb.Envelope_Ping{Ping: &pb.Heartbeat{Nonce: id}}}) != nil {
				p.stop()
				return
			}
		}
	}
}
