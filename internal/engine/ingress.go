package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/icholy/digest"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/capture"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type physicalFlow struct {
	ref           *pb.FlowRef
	conn          net.Conn
	write         sync.Mutex
	packet        net.PacketConn
	remote        net.Addr
	lastUsed      time.Time
	retainedUntil time.Time
	unusedUntil   time.Time
	unusedTimer   *time.Timer
	sipUsed       bool
	poisoned      bool
	closed        atomic.Bool
	capture       *capture.Exporter
	drainHints    map[drainHintKey]time.Time
	drainRoutes   int
	drainUnknown  bool
}

type udpRouteKey struct {
	flow   *physicalFlow
	callID string
}

// One ordinary control-operation window includes queueing, lookup and redirects.
const ingressRouteTimeout = 2 * time.Second

type Ingress struct {
	process          *pb.ProcessRef
	control          *control.Server
	listener         string
	preferred        string
	sourceIP         net.IP
	max              int
	mu               sync.Mutex
	flows            map[string]*physicalFlow
	sequence         atomic.Uint64
	dispatch         atomic.Uint64
	slots            chan struct{}
	writes           chan struct{}
	udp              net.PacketConn
	peers            map[string]*physicalFlow
	messages         chan struct{}
	udpRoutes        map[udpRouteKey]chan struct{}
	stateWriter      StateWriter
	stateLocate      func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error)
	stateReject      func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error
	stateCompletion  func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error)
	stateAttempts    map[string]*physicalAttempt
	capture          *capture.Exporter
	flowRejected     atomic.Uint64
	tlsFailed        atomic.Uint64
	invalidMessages  atomic.Uint64
	methodRejected   atomic.Uint64
	onRouteTiming    func(IngressRouteTiming)
	directRequest    func(context.Context, *control.Peer, *pb.FlowRef, *pb.SipMessage, sip.Message, portable.Message) error
	directCompletion func(context.Context, *pb.FlowRef, *pb.SipMessage, sip.Message, portable.Message) (bool, error)
	directReject     func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32, sip.Message, portable.Message) error
	methodPolicy     MethodPolicy
	authPolicy       *pb.InboundAuthPolicy
	challenges       localChallenges
	draining         atomic.Bool
	drainListeners   map[net.Listener]struct{}
	drainWake        chan struct{}
	drainHintCount   atomic.Int64
	drainReports     atomic.Int64
}

// IngressStatistics contains aggregate operational evidence without SIP data.
type IngressStatistics struct {
	FlowRejected    uint64
	TLSFailed       uint64
	InvalidMessages uint64
	MethodRejected  uint64
}

func (i *Ingress) Statistics() IngressStatistics {
	return IngressStatistics{FlowRejected: i.flowRejected.Load(), TLSFailed: i.tlsFailed.Load(), InvalidMessages: i.invalidMessages.Load(), MethodRejected: i.methodRejected.Load()}
}

type IngressOptions struct {
	DirectRequest       func(context.Context, *control.Peer, *pb.FlowRef, *pb.SipMessage, sip.Message, portable.Message) error
	DirectCompletion    func(context.Context, *pb.FlowRef, *pb.SipMessage, sip.Message, portable.Message) (bool, error)
	DirectReject        func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32, sip.Message, portable.Message) error
	PreferredController string
	SourceIP            net.IP
	Capture             *capture.Exporter
	OnRouteTiming       func(IngressRouteTiming)
	MethodPolicy        MethodPolicy
	AuthPolicy          *pb.InboundAuthPolicy
}

func NewIngress(process *pb.ProcessRef, server *control.Server, listener string, max int, options ...IngressOptions) *Ingress {
	var selected IngressOptions
	if len(options) > 0 {
		selected = options[0]
	}
	var authPolicy *pb.InboundAuthPolicy
	if selected.AuthPolicy != nil {
		authPolicy = proto.Clone(selected.AuthPolicy).(*pb.InboundAuthPolicy)
	}
	return &Ingress{process: process, control: server, listener: listener, preferred: selected.PreferredController, sourceIP: append(net.IP(nil), selected.SourceIP...), capture: selected.Capture, onRouteTiming: selected.OnRouteTiming, directRequest: selected.DirectRequest, directCompletion: selected.DirectCompletion, directReject: selected.DirectReject, methodPolicy: selected.MethodPolicy, authPolicy: authPolicy, max: max, slots: make(chan struct{}, max), writes: make(chan struct{}, 128), flows: make(map[string]*physicalFlow), peers: make(map[string]*physicalFlow), messages: make(chan struct{}, 128), drainWake: make(chan struct{}, 1)}
}

func (i *Ingress) Serve(ctx context.Context, listener net.Listener, cfg *tls.Config) error {
	if !i.registerDrainListener(listener) {
		_ = listener.Close()
		return nil
	}
	defer i.unregisterDrainListener(listener)
	go func() {
		<-ctx.Done()
		listener.Close()
		i.mu.Lock()
		for _, f := range i.flows {
			if f.conn != nil {
				f.conn.Close()
			}
		}
		i.mu.Unlock()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || i.draining.Load() {
				return nil
			}
			return err
		}
		select {
		case i.slots <- struct{}{}:
			go func() { defer func() { <-i.slots }(); i.connection(ctx, conn, cfg) }()
		default:
			i.flowRejected.Add(1)
			conn.Close()
		}
	}
}

func (i *Ingress) connection(ctx context.Context, raw net.Conn, cfg *tls.Config) {
	conn := raw
	network := "tcp"
	if cfg != nil {
		secure := tls.Server(raw, cfg)
		handshake, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := secure.HandshakeContext(handshake)
		cancel()
		if err != nil {
			i.tlsFailed.Add(1)
			raw.Close()
			return
		}
		conn = secure
		network = "tls"
	} else if _, ok := raw.(*tls.Conn); ok {
		network = "tls"
	}
	defer conn.Close()
	host, portText, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return
	}
	flow, err := i.addStream(conn, host, port, network)
	if err != nil {
		return
	}
	defer i.remove(flow)
	i.readStream(ctx, flow)
}

func (i *Ingress) readStream(ctx context.Context, flow *physicalFlow) {
	conn := flow.conn
	parser := sip.NewParser()
	parser.MaxMessageLength = 65536
	stream := parser.NewSIPStream()
	defer func() { stream.Close() }()
	buffer := make([]byte, 4096)
	var original []byte
	largeFrame := false
	for {
		n, readErr := conn.Read(buffer)
		if n > 0 {
			if len(original) == 0 && n <= 4 && len(bytes.Trim(buffer[:n], "\r\n")) == 0 {
				if n == 4 {
					flow.write.Lock()
					_, _ = flow.writeWire([]byte("\r\n"))
					flow.write.Unlock()
				}
				continue
			}
			original = append(original, buffer[:n]...)
			if len(original) > 65536+len(buffer) {
				i.invalidMessages.Add(1)
				return
			}
			if _, err := stream.Write(buffer[:n]); err != nil {
				i.invalidMessages.Add(1)
				return
			}
			for len(original) > 0 {
				m, size, err := stream.ParseNext()
				if errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				if err != nil {
					i.invalidMessages.Add(1)
					return
				}
				if size <= 0 || size > len(original) {
					i.invalidMessages.Add(1)
					return
				}
				wire := append([]byte(nil), original[:size]...)
				original = original[size:]
				largeFrame = largeFrame || size > 8192
				i.capture.Submit(capture.Observation{Timestamp: time.Now(), Direction: "in", Transport: flow.ref.Transport, Source: conn.RemoteAddr().String(), Destination: conn.LocalAddr().String(), Outcome: "received", Wire: wire})
				if err := validateObservedMessage(m); err != nil {
					i.invalidMessages.Add(1)
					return
				}
				message := i.projectInboundMessage(wire, m)
				flow.write.Lock()
				usable := flow.admitSIPUse()
				flow.write.Unlock()
				if !usable {
					return
				}
				i.routeParsed(ctx, flow, message, m, drainMessageObservation(m))
				if len(original) == 0 && largeFrame {
					// A rare large SDP/message must not pin its framing buffers for
					// the rest of this phone's connection lifetime.
					original = nil
					stream.Close()
					stream = parser.NewSIPStream()
					largeFrame = false
				}
			}
		}
		if readErr != nil {
			return
		}
	}
}

// Intrinsic OPTIONS needs no controller header/address/authentication DTO. The
// parsed message and original bytes still belong to this one ingress operation.
func projectIngressMessage(wire []byte, parsed sip.Message) *pb.SipMessage {
	if request, ok := parsed.(*sip.Request); ok && request.Method == sip.OPTIONS {
		return &pb.SipMessage{Wire: wire, Method: "OPTIONS"}
	}
	return projectObservedMessage(wire, parsed, false)
}

// Direct native completion needs only routing identity. Build the rich public
// projection after that probe, when a request is actually presented.
func (i *Ingress) projectInboundMessage(wire []byte, parsed sip.Message) *pb.SipMessage {
	if i.directCompletion == nil && i.directRequest == nil {
		return projectIngressMessage(wire, parsed)
	}
	if request, ok := parsed.(*sip.Request); ok && request.Method == sip.OPTIONS {
		return &pb.SipMessage{Wire: wire, Method: "OPTIONS"}
	}
	message := &pb.SipMessage{Wire: wire, Method: string(parsed.CSeq().MethodName), Selectors: observedMessageSelectors(parsed, false)}
	if request, ok := parsed.(*sip.Request); ok {
		message.Method = string(request.Method)
	} else if response, ok := parsed.(*sip.Response); ok {
		message.Status = uint32(response.StatusCode)
	}
	if i.authPolicy != nil && message.Status == 0 {
		if value := header(parsed, "Authorization"); value != "" {
			if auth, err := digest.ParseCredentials(value); err == nil {
				message.Authorization = &pb.DigestCredentials{Username: auth.Username}
			}
		}
		if to := parsed.To(); to != nil && tag(to.Params) != "" {
			message.Addresses = map[string]*pb.AddressList{"to": {Values: []*pb.Address{{Params: map[string]string{"tag": tag(to.Params)}}}}}
		}
		message.Headers = []*pb.Header{{Name: "from", Value: header(parsed, "From")}}
	}
	return message
}

func (i *Ingress) route(parent context.Context, flow *physicalFlow, message *pb.SipMessage, observation ...state.LocalDrainObservation) {
	i.routeParsed(parent, flow, message, nil, observation...)
}

func (i *Ingress) routeParsed(parent context.Context, flow *physicalFlow, message *pb.SipMessage, parsed sip.Message, observation ...state.LocalDrainObservation) {
	ctx, cancel := context.WithTimeout(parent, ingressRouteTimeout)
	defer cancel()
	i.routeWithinParsed(ctx, flow, message, parsed, func(status int, parsed sip.Message, native portable.Message) {
		i.rejectParsed(parent, flow, message, status, parsed, native)
	}, func(parsed sip.Message, native portable.Message) {
		i.rejectUnavailableParsed(parent, flow, message, parsed, native)
	}, observation...)
}

func (i *Ingress) routeWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, reject func(int), unavailable func(), observation ...state.LocalDrainObservation) {
	i.routeWithinParsed(ctx, flow, message, nil, func(status int, _ sip.Message, _ portable.Message) { reject(status) }, func(_ sip.Message, _ portable.Message) { unavailable() }, observation...)
}

func (i *Ingress) routeWithinParsed(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, parsed sip.Message, rejectReply func(int, sip.Message, portable.Message), unavailableReply func(sip.Message, portable.Message), observation ...state.LocalDrainObservation) {
	started := time.Now()
	timing := IngressRouteTiming{Outcome: "route-unavailable", InitialBudgetMS: ingressRemainingMS(ctx)}
	defer func() { i.reportRouteTiming(ctx, message, timing, started) }()
	completionOnly := false
	if len(observation) > 0 {
		if !i.beginDrainRoute(flow, observation[0]) {
			if !i.beginDrainReplay(flow) {
				return
			}
			completionOnly = true
		}
		defer func() {
			i.finishDrainRoute(flow, observation[0], timing.Outcome == "native-completed" || timing.Outcome == "accepted" || timing.Outcome == "local-challenge-ack")
		}()
	}
	var native portable.Message
	reject := func(status int) {
		timing.SipStatus = ingressDiagnosticStatus(status)
		started := time.Now()
		rejectReply(status, parsed, native)
		timing.RefusalMS += ingressElapsedMS(started)
	}
	unavailable := func() {
		started := time.Now()
		unavailableReply(parsed, native)
		timing.RefusalMS += ingressElapsedMS(started)
	}
	if ctx.Err() != nil {
		timing.Outcome = "expired"
		return
	}
	if message.GetStatus() == 0 && message.GetMethod() == "OPTIONS" {
		timing.Outcome = "local-options"
		timing.SipStatus = i.localOptionsWithin(ctx, flow, message, parsed)
		return
	}
	if !completionOnly && i.methodPolicy.disabled(message) {
		i.methodRejected.Add(1)
		timing.Outcome = "method-disabled"
		timing.SipStatus = 405
		i.localResponseWithParsedWithin(ctx, flow, message, parsed, 405, "policy-", []*pb.Header{{Name: "Allow", Value: i.methodPolicy.allow()}})
		return
	}
	if !completionOnly && i.localChallengeACK(message, flow.ref, parsed) {
		timing.Outcome = "local-challenge-ack"
		return
	}
	if !completionOnly && i.requiresLocalChallenge(message) {
		timing.Outcome = "local-challenge"
		i.challengeLocally(ctx, flow, message, parsed)
		return
	}
	if i.directCompletion != nil || i.directRequest != nil {
		if parsed == nil {
			var err error
			parsed, err = parseObservedMessage(message.Wire)
			if err != nil {
				reject(400)
				return
			}
		}
		// Native validation is consumed only at native admission; observed and
		// local-policy validation intentionally have different acceptance rules.
		native, _ = portable.FreezeMessage(message.Wire, parsed, flow.ref.Transport, net.JoinHostPort(flow.ref.SourceIp, strconv.Itoa(int(flow.ref.SourcePort))), "")
	}
	if i.stateCompletion != nil || i.directCompletion != nil {
		probeStarted := time.Now()
		var handled bool
		var err error
		if i.directCompletion != nil {
			handled, err = i.directCompletion(ctx, flow.ref, message, parsed, native)
		} else {
			handled, err = i.stateCompletion(ctx, flow.ref, message)
		}
		timing.CompletionProbeMS = ingressElapsedMS(probeStarted)
		if err != nil {
			timing.Outcome = "completion-error"
			var protocol *ingestError
			if errors.As(err, &protocol) {
				reject(int(protocol.status))
			} else if stateReadUnavailable(err) {
				unavailable()
			} else if !errors.Is(err, state.ErrUnknown) {
				reject(503)
			}
			return
		}
		if handled {
			timing.Outcome = "native-completed"
			return
		}
	}
	if completionOnly {
		timing.Outcome = "draining"
		reject(503)
		return
	}
	peers := i.control.ReadyPeers()
	if len(peers) == 0 {
		timing.Outcome = "no-ready-peer"
		if i.stateWriter != nil && i.stateReject != nil {
			refusalStarted := time.Now()
			err := i.rejectNative(ctx, flow.ref, message, 503, parsed, native)
			timing.RefusalMS += ingressElapsedMS(refusalStarted)
			if stateReadUnavailable(err) {
				unavailable()
			} else if errors.Is(err, state.ErrNotFound) && !errors.Is(err, state.ErrUnknown) {
				reject(481)
			}
			return
		}
		reject(503)
		return
	}
	peer := peers[int(i.dispatch.Add(1)-1)%len(peers)]
	for _, candidate := range peers {
		if candidate.Process().InstanceId == i.preferred {
			peer = candidate
			break
		}
	}
	if i.directRequest != nil {
		message = projectObservedMessage(message.Wire, parsed, false)
		if err := i.directRequest(ctx, peer, flow.ref, message, parsed, native); err != nil {
			timing.Outcome = "decision-notification-error"
			var protocol *ingestError
			if errors.As(err, &protocol) {
				reject(int(protocol.status))
			} else {
				reject(503)
			}
			return
		}
		timing.Outcome = "accepted"
		return
	}
	var logical *pb.ContextRef
	if i.stateLocate != nil {
		var err error
		var route *pb.ContextSnapshot
		locateStarted := time.Now()
		route, err = i.stateLocate(ctx, message)
		timing.LocateMS = ingressElapsedMS(locateStarted)
		if err != nil {
			timing.Outcome = "locate-error"
			var protocol *ingestError
			if errors.As(err, &protocol) {
				reject(int(protocol.status))
			} else if stateReadUnavailable(err) {
				unavailable()
			} else if !errors.Is(err, state.ErrUnknown) {
				reject(503)
			}
			return
		} // Unavailable state is not an unknown SIP dialog.
		if route != nil {
			logical = route.Context
			if !route.Ended && route.Kind != pb.ContextSnapshot_SUBSCRIPTION && route.ApplicationLeaseTerm != "" {
				for _, candidate := range peers {
					if sameProcess(candidate.Application(), route.Application) && candidate.ApplicationLease().GetTerm() == route.ApplicationLeaseTerm {
						peer = candidate
						break
					}
				}
			}
		}
	}
	id := uuid.NewString()
	visited := make(map[string]bool)
	for hops := 0; hops < 3; hops++ {
		key := peer.Process().InstanceId + ":" + peer.Process().Epoch
		if visited[key] {
			timing.Outcome = "redirect-loop"
			break
		}
		visited[key] = true
		remaining := time.Until(deadline(ctx))
		if remaining <= 0 {
			timing.Outcome = "expired"
			break
		}
		budget := uint32(remaining / time.Millisecond)
		if timing.Hops == 0 {
			timing.FirstHopBudgetMS = budget
		}
		timing.Hops++
		rpcStarted := time.Now()
		reply, err := peer.Call(ctx, &pb.Envelope{CorrelationId: id, RemainingBudgetMs: budget, Payload: &pb.Envelope_Ingress{Ingress: &pb.IngressReceived{Flow: flow.ref, Message: message, Context: logical}}})
		timing.RouteRPCMS += ingressElapsedMS(rpcStarted)
		if err != nil {
			timing.Outcome = "route-rpc-error"
			break
		}
		result := reply.GetRouteResult()
		if result == nil {
			timing.Outcome = "invalid-route-reply"
			break
		}
		switch result.Outcome {
		case pb.RouteResult_ACCEPTED:
			timing.Outcome = "accepted"
			return
		case pb.RouteResult_UNAVAILABLE:
			timing.Outcome = "route-unavailable"
			reject(503)
			return
		case pb.RouteResult_REJECTED:
			timing.Outcome = "rejected"
			reject(int(result.SipStatus))
			return
		case pb.RouteResult_REDIRECT:
			peer, err = i.control.Peer(result.Controller)
			if err != nil {
				timing.Outcome = "redirect-unavailable"
				reject(503)
				return
			}
		default:
			timing.Outcome = "invalid-route-outcome"
			reject(503)
			return
		}
	}
	reject(503)
}

// Admission runs synchronously in the UDP reader. A queued item retains its
// original budget, and a cancelled middle item cannot release a later item
// before its own predecessor has completed. Only transient transport work lives
// here; the native state authority still commits every protocol transition.
func (i *Ingress) routeUDP(parent context.Context, flow *physicalFlow, message *pb.SipMessage, callID string, observation ...state.LocalDrainObservation) bool {
	return i.routeUDPParsed(parent, flow, message, nil, callID, observation...)
}

func (i *Ingress) routeUDPParsed(parent context.Context, flow *physicalFlow, message *pb.SipMessage, parsed sip.Message, callID string, observation ...state.LocalDrainObservation) bool {
	select {
	case i.messages <- struct{}{}:
	default:
		return false
	}
	ctx, cancel := context.WithTimeout(parent, ingressRouteTimeout)
	key := udpRouteKey{flow: flow, callID: callID}
	done := make(chan struct{})
	i.mu.Lock()
	if i.udpRoutes == nil {
		i.udpRoutes = make(map[udpRouteKey]chan struct{})
	}
	previous := i.udpRoutes[key]
	i.udpRoutes[key] = done
	i.mu.Unlock()
	go func() {
		defer cancel()
		defer func() {
			i.mu.Lock()
			if i.udpRoutes[key] == done {
				delete(i.udpRoutes, key)
			}
			close(done)
			i.mu.Unlock()
			<-i.messages
		}()
		if previous != nil {
			<-previous
		}
		// Parent cancellation visits child contexts individually. A cancelled
		// predecessor can finish before this item's own child is visited.
		if parent.Err() != nil {
			return
		}
		i.routeWithinParsed(ctx, flow, message, parsed, func(status int, parsed sip.Message, native portable.Message) {
			i.rejectParsed(parent, flow, message, status, parsed, native)
		}, func(parsed sip.Message, native portable.Message) {
			i.rejectUnavailableParsed(parent, flow, message, parsed, native)
		}, observation...)
	}()
	return true
}
func deadline(ctx context.Context) time.Time { d, _ := ctx.Deadline(); return d }

func (i *Ingress) reject(parent context.Context, flow *physicalFlow, message *pb.SipMessage, status int) {
	i.rejectParsed(parent, flow, message, status, nil, portable.Message{})
}

func (i *Ingress) rejectParsed(parent context.Context, flow *physicalFlow, message *pb.SipMessage, status int, parsed sip.Message, native portable.Message) {
	ctx, cancel := context.WithTimeout(parent, 800*time.Millisecond)
	defer cancel()
	i.rejectWithinParsed(ctx, flow, message, status, parsed, native)
}

// A failed read proves only this invocation did not mutate. Another packet may
// already have admitted the same transaction, so first arbitrate using a fresh
// budget. The longer budget lets the store's independent two-second I/O limit
// expire before the caller does; only that independent read failure is an
// actionable best-effort outage, never expiry of this arbitration budget.
func (i *Ingress) rejectUnavailable(parent context.Context, flow *physicalFlow, message *pb.SipMessage) {
	i.rejectUnavailableParsed(parent, flow, message, nil, portable.Message{})
}

func (i *Ingress) rejectUnavailableParsed(parent context.Context, flow *physicalFlow, message *pb.SipMessage, parsed sip.Message, native portable.Message) {
	if i.stateWriter == nil || i.stateReject == nil || message == nil || message.Status != 0 || message.Method == "ACK" {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 2500*time.Millisecond)
	defer cancel()
	err := i.rejectNative(ctx, flow.ref, message, 503, parsed, native)
	if arbitrationExpired(ctx, time.Now()) || errors.Is(err, state.ErrUnknown) {
		return
	}
	if errors.Is(err, state.ErrNotFound) {
		i.rejectStatelessWithinParsed(ctx, flow, message, 481, true, parsed)
		return
	}
	if stateReadUnavailable(err) {
		refusal, cancel := context.WithTimeout(parent, 100*time.Millisecond)
		defer cancel()
		i.rejectStatelessWithinParsed(refusal, flow, message, 503, true, parsed)
	}
}

func arbitrationExpired(ctx context.Context, now time.Time) bool {
	// A nested I/O context sharing this deadline can expire before the parent
	// timer updates ctx.Err(). Compare the deadline as well as its error.
	due, bounded := ctx.Deadline()
	return ctx.Err() != nil || bounded && !now.Before(due)
}

func (i *Ingress) rejectWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, status int) {
	i.rejectWithinParsed(ctx, flow, message, status, nil, portable.Message{})
}

func (i *Ingress) rejectNative(ctx context.Context, flow *pb.FlowRef, message *pb.SipMessage, status uint32, parsed sip.Message, native portable.Message) error {
	if i.directReject != nil && parsed != nil && !native.IsZero() {
		return i.directReject(ctx, flow, message, status, parsed, native)
	}
	return i.stateReject(ctx, flow, message, status)
}

func (i *Ingress) rejectWithinParsed(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, status int, parsed sip.Message, native portable.Message) {
	if ctx.Err() != nil {
		return
	}
	if message.Status != 0 {
		return
	}
	if status < 300 || status > 699 {
		status = 503
	}
	fallback := i.stateWriter != nil
	if fallback {
		if i.stateReject == nil {
			return // Missing native arbitration is not no-admission evidence.
		} else {
			err := i.rejectNative(ctx, flow.ref, message, uint32(status), parsed, native)
			if errors.Is(err, state.ErrNotFound) && !errors.Is(err, state.ErrUnknown) {
				i.rejectStatelessWithinParsed(ctx, flow, message, 481, true, parsed)
			}
			// An unavailable/uncertain fence cannot select another final.
			return
		}
	}
	i.rejectStatelessWithinParsed(ctx, flow, message, status, fallback, parsed)
}

func (i *Ingress) rejectStatelessWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, status int, fallback bool) {
	i.rejectStatelessWithinParsed(ctx, flow, message, status, fallback, nil)
}

func (i *Ingress) rejectStatelessWithinParsed(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, status int, fallback bool, parsed sip.Message) {
	prefix := ""
	var headers []*pb.Header
	if fallback {
		prefix = "unavailable-"
		if status == 503 {
			headers = append(headers, &pb.Header{Name: "Retry-After", Value: "1"})
		}
	}
	i.localResponseWithParsedWithin(ctx, flow, message, parsed, status, prefix, headers)
}

// Callers must establish that this response cannot race an admitted business final.
func (i *Ingress) localResponseWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, status int, prefix string, headers []*pb.Header, tagger ...func(*sip.Request) string) {
	i.localResponseWithParsedWithin(ctx, flow, message, nil, status, prefix, headers, tagger...)
}

func (i *Ingress) localResponseWithParsedWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, parsed sip.Message, status int, prefix string, headers []*pb.Header, tagger ...func(*sip.Request) string) {
	if ctx.Err() != nil || message.Method == "ACK" || message.Status != 0 {
		return
	}
	if parsed == nil {
		var err error
		parsed, err = sip.ParseMessage(message.Wire)
		if err != nil {
			return
		}
	}
	req, ok := parsed.(*sip.Request)
	if !ok {
		return
	}
	i.localResponseParsedWithin(ctx, flow, message, req, status, prefix, headers, tagger...)
}

// Build against a private clone so the observed ingress parse remains unchanged.
func (i *Ingress) localResponseParsedWithin(ctx context.Context, flow *physicalFlow, message *pb.SipMessage, req *sip.Request, status int, prefix string, headers []*pb.Header, tagger ...func(*sip.Request) string) {
	if ctx.Err() != nil || req == nil || req.IsAck() || req.To() == nil || req.From() == nil || req.CallID() == nil || req.CSeq() == nil || req.Via() == nil {
		return
	}
	req = req.Clone()
	via := req.Via()
	if via != nil {
		via.Params.Add("received", flow.ref.SourceIp)
		if via.Params.Has("rport") {
			via.Params.Add("rport", strconv.Itoa(int(flow.ref.SourcePort)))
		}
	}
	response := sip.NewResponseFromRequest(req, status, reason(uint32(status)), nil)
	if prefix != "" {
		if to := response.To(); to != nil && req.To() != nil && !req.To().Params.Has("tag") {
			sum := sha256.Sum256(message.Wire)
			to.Params.Add("tag", prefix+hex.EncodeToString(sum[:12]))
			if len(tagger) > 0 {
				to.Params.Add("tag", tagger[0](req))
			}
		}
	}
	for _, header := range headers {
		response.AppendHeader(sip.NewHeader(header.Name, header.Value))
	}
	wire := []byte(response.String())
	if len(wire) > 65536 {
		return
	}
	flow.write.Lock()
	defer flow.write.Unlock()
	if flow.admitSIPUse() {
		if n, err := flow.writeWire(wire); err == nil && n == len(wire) {
			i.observeDrainLocked(flow, drainMessageObservation(response), true, time.Now())
		}
	}
	i.wakeDrain()
}

func (i *Ingress) Write(peer *control.Peer, envelope *pb.Envelope) error {
	select {
	case i.writes <- struct{}{}:
		go func() { defer func() { <-i.writes }(); _ = i.writeCommitted(peer, envelope) }()
		return nil
	default:
		return peer.Send(&pb.Envelope{CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Delivery{Delivery: &pb.DeliveryResult{Outcome: pb.DeliveryResult_NOT_WRITTEN, Error: "write capacity"}}})
	}
}

// Called under the flow write lock. Datagram writes preserve message boundaries.
func (f *physicalFlow) writeWire(wire []byte) (writtenResult int, resultErr error) {
	if f.capture != nil && len(bytes.Trim(wire, "\r\n")) > 0 {
		defer func() {
			outcome := "failed"
			if resultErr == nil && writtenResult == len(wire) {
				outcome = "written"
			} else if writtenResult > 0 {
				outcome = "partial"
			}
			var local, remote net.Addr
			if f.packet != nil {
				local, remote = f.packet.LocalAddr(), f.remote
			} else if f.conn != nil {
				local, remote = f.conn.LocalAddr(), f.conn.RemoteAddr()
			}
			if local != nil && remote != nil && f.ref != nil {
				f.capture.Submit(capture.Observation{Timestamp: time.Now(), Direction: "out", Transport: f.ref.Transport, Source: local.String(), Destination: remote.String(), Outcome: outcome, BytesWritten: writtenResult, Wire: wire})
			}
		}()
	}
	now := time.Now()
	if f.poisoned || f.closed.Load() || f.expireUnused(now) {
		return 0, net.ErrClosed
	}
	f.lastUsed = now
	if f.packet != nil {
		return f.packet.WriteTo(wire, f.remote)
	}
	if err := f.conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		f.poisoned = true
		_ = f.conn.Close()
		return 0, err
	}
	written := 0
	for written < len(wire) {
		n, err := f.conn.Write(wire[written:])
		if n < 0 || n > len(wire)-written {
			n, err = 0, io.ErrShortWrite
		}
		written += n
		if err != nil || n == 0 {
			if err == nil {
				err = io.ErrNoProgress
			}
			f.poisoned = true
			_ = f.conn.Close()
			return written, err
		}
	}
	return written, nil
}

func (i *Ingress) addStream(conn net.Conn, host string, port int, network string) (*physicalFlow, error) {
	ref := &pb.FlowRef{Ingress: i.process, ConnectionId: uuid.NewString(), SourceIp: host, SourcePort: uint32(port), Listener: i.listener, Transport: network}
	flow := &physicalFlow{ref: ref, conn: conn, lastUsed: time.Now(), capture: i.capture}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.draining.Load() {
		return nil, errors.New("SIP ingress draining")
	}
	if len(i.flows) >= i.max {
		i.flowRejected.Add(1)
		return nil, errors.New("flow capacity")
	}
	i.flows[ref.ConnectionId] = flow
	return flow, nil
}

func (i *Ingress) remove(flow *physicalFlow) {
	flow.closed.Store(true)
	flow.write.Lock()
	flow.stopUnusedTimer()
	for key := range flow.drainHints {
		i.removeDrainHintLocked(flow, key)
	}
	flow.write.Unlock()
	i.mu.Lock()
	delete(i.flows, flow.ref.ConnectionId)
	if flow.remote != nil {
		delete(i.peers, flow.remote.String())
	}
	i.mu.Unlock()
	i.wakeDrain()
	for _, peer := range i.control.ReadyPeers() {
		_ = peer.Send(&pb.Envelope{CorrelationId: uuid.NewString(), Payload: &pb.Envelope_FlowClosed{FlowClosed: &pb.FlowClosed{Flow: flow.ref}}})
	}
}

func (i *Ingress) packetFlow(remote net.Addr) (*physicalFlow, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.udp == nil {
		return nil, errors.New("UDP listener unavailable")
	}
	if flow := i.peers[remote.String()]; flow != nil {
		return flow, nil
	}
	if i.draining.Load() {
		return nil, errors.New("SIP ingress draining")
	}
	if len(i.flows) >= i.max {
		return nil, errors.New("flow capacity")
	}
	host, portText, err := net.SplitHostPort(remote.String())
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, err
	}
	ref := &pb.FlowRef{Ingress: i.process, ConnectionId: uuid.NewString(), SourceIp: host, SourcePort: uint32(port), Listener: i.listener, Transport: "udp"}
	flow := &physicalFlow{ref: ref, packet: i.udp, remote: remote, lastUsed: time.Now(), capture: i.capture}
	i.flows[ref.ConnectionId] = flow
	i.peers[remote.String()] = flow
	return flow, nil
}

func (i *Ingress) ServeUDP(ctx context.Context, conn net.PacketConn) error {
	i.mu.Lock()
	i.udp = conn
	i.mu.Unlock()
	go func() { <-ctx.Done(); conn.Close() }()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				i.mu.Lock()
				for key, flow := range i.peers {
					flow.write.Lock()
					if now.Sub(flow.lastUsed) > 10*time.Minute && now.After(flow.retainedUntil) {
						flow.closed.Store(true)
						delete(i.peers, key)
						delete(i.flows, flow.ref.ConnectionId)
					}
					flow.write.Unlock()
				}
				i.mu.Unlock()
			}
		}
	}()
	buffer := make([]byte, 65537)
	for {
		n, remote, err := conn.ReadFrom(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n == 0 || n > 65536 {
			i.invalidMessages.Add(1)
			continue
		}
		i.capture.Submit(capture.Observation{Timestamp: time.Now(), Direction: "in", Transport: "udp", Source: remote.String(), Destination: conn.LocalAddr().String(), Outcome: "received", Wire: buffer[:n]})
		flow, err := i.packetFlow(remote)
		if err != nil {
			i.flowRejected.Add(1)
			continue
		}
		flow.write.Lock()
		flow.lastUsed = time.Now()
		flow.write.Unlock()
		wire := append([]byte(nil), buffer[:n]...)
		parsed, err := parseObservedMessage(wire)
		if err != nil {
			i.invalidMessages.Add(1)
			continue
		}
		message := i.projectInboundMessage(wire, parsed)
		callID := parsed.CallID().Value()
		if callID == "" {
			continue
		}
		i.routeUDPParsed(ctx, flow, message, parsed, callID, drainMessageObservation(parsed))
	}
}

// OpenNetwork accepts only a target selected by the authenticated Controller owner.
// Subsequent writes still name the minted flow and require Controller acknowledgement.
func (i *Ingress) OpenNetwork(ctx context.Context, called string, target *pb.NetworkTarget) (*pb.FlowRef, error) {
	if i.draining.Load() {
		return nil, errors.New("SIP ingress draining")
	}
	unusedUntil := outboundStreamExpiry(ctx, time.Now())
	if target == nil {
		return nil, errors.New("network target required")
	}
	network := strings.ToLower(uacTransport(&pb.OpenUac{Uri: called, Network: target}))
	if network != "udp" && network != "tcp" && network != "tls" {
		return nil, errors.New("unsupported network transport")
	}
	next := called
	if target.Proxy != "" {
		next = target.Proxy
	}
	var uri sip.Uri
	if err := sip.ParseUri(next, &uri); err != nil || uri.Host == "" {
		return nil, errors.New("invalid network target")
	}
	port := uri.Port
	if port == 0 {
		port = 5060
		if network == "tls" {
			port = 5061
		}
	}
	address := net.JoinHostPort(uri.Host, strconv.Itoa(port))
	if network == "udp" {
		remote, err := net.ResolveUDPAddr("udp", address)
		if err != nil {
			return nil, err
		}
		flow, err := i.packetFlow(remote)
		if err != nil {
			return nil, err
		}
		flow.write.Lock()
		flow.retainedUntil = time.Now().Add(4 * time.Hour)
		flow.write.Unlock()
		return proto.Clone(flow.ref).(*pb.FlowRef), nil
	}
	select {
	case i.slots <- struct{}{}:
	default:
		return nil, errors.New("stream capacity")
	}
	dialer := net.Dialer{}
	if len(i.sourceIP) > 0 {
		dialer.LocalAddr = &net.TCPAddr{IP: i.sourceIP}
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		<-i.slots
		return nil, err
	}
	if network == "tls" {
		secure := tls.Client(conn, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: uri.Host})
		if err := secure.HandshakeContext(ctx); err != nil {
			conn.Close()
			<-i.slots
			return nil, err
		}
		conn = secure
	}
	host, portText, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		conn.Close()
		<-i.slots
		return nil, err
	}
	remotePort, _ := strconv.Atoi(portText)
	flow, err := i.addStream(conn, host, remotePort, network)
	if err != nil {
		conn.Close()
		<-i.slots
		return nil, err
	}
	flow.startUnusedLease(unusedUntil)
	go i.readOutboundStream(flow)
	return proto.Clone(flow.ref).(*pb.FlowRef), nil
}
