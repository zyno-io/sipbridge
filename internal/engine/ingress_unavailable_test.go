package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

type pendingDeadlineContext struct {
	context.Context
	due time.Time
}

func (c pendingDeadlineContext) Deadline() (time.Time, bool) { return c.due, true }

func TestArbitrationDeadlineCanElapseBeforeContextReportsIt(t *testing.T) {
	now := time.Now()
	ctx := pendingDeadlineContext{Context: context.Background(), due: now}
	if ctx.Err() != nil || !arbitrationExpired(ctx, now) {
		t.Fatal("expired arbitration was accepted while its timer callback was pending")
	}
	ctx.due = now.Add(time.Millisecond)
	if arbitrationExpired(ctx, now) {
		t.Fatal("live arbitration was considered expired")
	}
}

func TestRetainedBootstrapRedisOutageReturns503BeforeAdmissionMutation(t *testing.T) {
	base := newNativeOwnerFixture(t)
	proxy := newRedisReservationFaultProxy(t, base.config.Redis.Address)
	configuration := base.config
	configuration.Redis.Address = proxy.address
	store, err := state.Open(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if !store.Available() {
		t.Fatal("fixture never verified retained complete bootstrap")
	}
	engine := &StateEngine{cfg: base.engine.cfg, store: store, ctx: context.Background()}
	engine.runtime, err = executionfixture.New(store, execution.ReducerFunc(engine.reduce), "offline-admission", 8, engine.resumeConnectedOwners)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan error, 3)
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = engine
		i.stateReject = engine.Reject
		i.stateLocate = func(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
			ref, err := engine.LocateRoute(ctx, message)
			observed <- err
			return ref, err
		}
	})
	// Disconnect every already-validated Redis socket and refuse replacements.
	// This exercises real network failure after warm authority proof, not a
	// synthetic callback error or an uninitialized startup check.
	proxy.mu.Lock()
	proxy.isolated = true
	proxy.mu.Unlock()
	proxy.disconnect()
	for _, method := range []string{"REGISTER", "SUBSCRIBE", "INVITE"} {
		wire := fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK-warm-offline-%s;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: <sip:service@example.test>\r\nCall-ID: warm-offline-%s\r\nCSeq: 1 %s\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n", method, method, method, method)
		if _, err := f.remote.WriteTo([]byte(wire), f.listener.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		if err := f.remote.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 4096)
		n, _, err := f.remote.ReadFrom(buffer)
		if err != nil {
			t.Fatal("retained authority silenced the unavailable initial request", method, err)
		}
		parsed, err := sip.ParseMessage(buffer[:n])
		if err != nil {
			t.Fatal(err)
		}
		response, ok := parsed.(*sip.Response)
		if !ok || response.StatusCode != 503 {
			t.Fatalf("%s did not receive a concrete 503: %s", method, buffer[:n])
		}
		if err := <-observed; err == nil || errors.Is(err, state.ErrUnknown) {
			t.Fatal("positive read failure became mutation uncertainty", method, err)
		}
	}
	if store.Available() {
		t.Fatal("actual disconnected Redis remained operationally available")
	}
	proxy.heal()
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal("retained bootstrap could not recover after real network restoration", err)
	}
}

func TestUnavailableUDPRespondsAfterRealRedisReadBudgetExpires(t *testing.T) {
	// Accept TCP but never reply to Redis commands. This must exhaust routing's
	// real context rather than returning a synthetic immediate store error.
	blackhole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var sockets []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := blackhole.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			sockets = append(sockets, connection)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		blackhole.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, connection := range sockets {
			connection.Close()
		}
	})
	store, err := state.OpenDeferred(state.Config{Redis: state.RedisOptions{Address: blackhole.Addr().String()},
		Prefix: "sipstate:timeout", Namespace: "timeout", NamespaceKey: bytes.Repeat([]byte{44}, 32), Generation: "generation",
		Versions: state.Versions{Schema: "3", Reducer: "9", Timers: "1"}, Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	engine := &StateEngine{store: store}
	engine.runtime, err = execution.NewCAS(store, execution.ReducerFunc(engine.reduce), 1)
	if err != nil {
		t.Fatal(err)
	}
	expired := make(chan error, 1)
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = engine
		i.stateLocate = func(ctx context.Context, message *pb.SipMessage) (*pb.ContextSnapshot, error) {
			ref, err := engine.LocateRoute(ctx, message)
			expired <- ctx.Err()
			return ref, err
		}
		i.stateReject = engine.Reject
	})
	f.send(t, "REGISTER", "blackholed-redis", 1)
	// Routing expiry starts a separate 2.5s arbitration window before the
	// bounded stateless write. Allow that full path plus scheduling slack.
	replyWait := ingressRouteTimeout + 2500*time.Millisecond + time.Second
	if err := f.remote.SetReadDeadline(time.Now().Add(replyWait)); err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, 4096)
	n, _, err := f.remote.ReadFrom(wire)
	if err != nil {
		t.Fatal("expired Redis route budget suppressed SIP 503", err)
	}
	if !strings.HasPrefix(string(wire[:n]), "SIP/2.0 503 ") {
		t.Fatalf("unexpected unavailable reply: %s", wire[:n])
	}
	if err := <-expired; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("fixture did not exhaust the original routing budget", err)
	}
}

func TestExpiredPreflightArbitratesBeforeStatelessResponse(t *testing.T) {
	arbitrated := make(chan error, 1)
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = &StateEngine{}
		i.stateCompletion = func(ctx context.Context, _ *pb.FlowRef, _ *pb.SipMessage) (bool, error) {
			<-ctx.Done()
			return true, &stateReadFailure{ctx.Err()}
		}
		i.stateReject = func(ctx context.Context, _ *pb.FlowRef, _ *pb.SipMessage, _ uint32) error {
			arbitrated <- ctx.Err()
			return nil // The native fence handles an already-admitted transaction.
		}
	})
	f.send(t, "REGISTER", "expired-preflight", 1)
	select {
	case err := <-arbitrated:
		if err != nil {
			t.Fatalf("arbitration inherited the expired route budget: %v", err)
		}
	case <-time.After(ingressRouteTimeout + time.Second):
		t.Fatal("expired preflight did not attempt native arbitration")
	}
	if err := f.remote.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 4096)
	if n, _, err := f.remote.ReadFrom(buffer); err == nil {
		t.Fatalf("arbitrated transaction received an alternate stateless response: %s", buffer[:n])
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
}

func TestUnavailableArbitrationUncertaintySuppressesStatelessResponse(t *testing.T) {
	attempted := make(chan struct{}, 1)
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = &StateEngine{}
		i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) {
			return true, &stateReadFailure{state.ErrAuthority}
		}
		i.stateReject = func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error {
			attempted <- struct{}{}
			return errors.Join(state.ErrUnknown, &stateReadFailure{state.ErrAuthority})
		}
	})
	f.send(t, "REGISTER", "uncertain-arbitration", 1)
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("unavailable preflight did not attempt native arbitration")
	}
	if err := f.remote.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 4096)
	if n, _, err := f.remote.ReadFrom(buffer); err == nil {
		t.Fatalf("uncertain arbitration selected an alternate final: %s", buffer[:n])
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
}

func TestUnavailableArbitrationCallerExpirySuppressesFallback(t *testing.T) {
	for _, kind := range []string{"canceled", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			listener, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			remote, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer remote.Close()
			parent, cancel := context.WithCancel(context.Background())
			if kind == "canceled" {
				cancel()
			} else {
				defer cancel()
				var stop context.CancelFunc
				parent, stop = context.WithTimeout(parent, 25*time.Millisecond)
				defer stop()
			}
			ingress := &Ingress{stateWriter: &StateEngine{}, stateReject: func(ctx context.Context, _ *pb.FlowRef, _ *pb.SipMessage, _ uint32) error {
				<-ctx.Done()
				return &stateReadFailure{ctx.Err()}
			}}
			flow := &physicalFlow{ref: &pb.FlowRef{SourceIp: "127.0.0.1"}, packet: listener, remote: remote.LocalAddr()}
			ingress.rejectUnavailable(parent, flow, udpOrderMessage(t, "REGISTER", "expired-arbitration", 1))
			if err := remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, 4096)
			if n, _, err := remote.ReadFrom(buffer); err == nil {
				t.Fatalf("expired arbitration selected a stateless final: %s", buffer[:n])
			} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
				t.Fatal(err)
			}
		})
	}
}

func TestUnavailableStateReturnsSIP503OnExistingFlow(t *testing.T) {
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = &StateEngine{} // No effect starts; negative fallback only.
		i.stateLocate = func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) {
			return nil, &stateReadFailure{errors.New("Redis unavailable")}
		}
		i.stateReject = func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error {
			return &stateReadFailure{state.ErrAuthority}
		}
	})
	for _, method := range []string{"REGISTER", "SUBSCRIBE", "INVITE", "BYE"} {
		t.Run(method, func(t *testing.T) {
			wire := fmt.Sprintf("%s sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/UDP 127.0.0.1:5060;branch=z9hG4bK-unavailable-%s;rport\r\nFrom: <sip:phone@example.test>;tag=phone\r\nTo: <sip:service@example.test>\r\nCall-ID: unavailable-%s\r\nCSeq: 1 %s\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n", method, method, method, method)
			tag := ""
			for n := 0; n < 2; n++ {
				if _, err := f.remote.WriteTo([]byte(wire), f.listener.LocalAddr()); err != nil {
					t.Fatal(err)
				}
				if err := f.remote.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				buffer := make([]byte, 4096)
				count, _, err := f.remote.ReadFrom(buffer)
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := sip.ParseMessage(buffer[:count])
				if err != nil {
					t.Fatal(err)
				}
				response, ok := parsed.(*sip.Response)
				if !ok || response.StatusCode != 503 {
					t.Fatalf("expected 503, got %s", buffer[:count])
				}
				if !strings.Contains(response.String(), "Retry-After: 1") {
					t.Fatal("temporary failure lost retry instruction")
				}
				current, _ := response.To().Params.Get("tag")
				if current == "" || n > 0 && current != tag {
					t.Fatal("stateless retry changed To-tag", current, tag)
				}
				tag = current
			}
		})
	}
	f.send(t, "ACK", "unavailable-ack", 1)
	f.remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buffer := make([]byte, 4096)
	if _, _, err := f.remote.ReadFrom(buffer); err == nil {
		t.Fatal("outage fallback answered ACK")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
}

func TestMissingEstablishedStateReturnsSIP481(t *testing.T) {
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = &StateEngine{}
		i.stateLocate = func(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error) {
			return nil, rejectIngest(481, "retained dialog missing")
		}
		i.stateReject = func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error { return state.ErrNotFound }
	})
	f.send(t, "BYE", "missing-dialog", 2)
	f.remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 4096)
	count, _, err := f.remote.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := sip.ParseMessage(buffer[:count])
	if err != nil {
		t.Fatal(err)
	}
	response, ok := parsed.(*sip.Response)
	if !ok || response.StatusCode != 481 {
		t.Fatalf("missing dialog did not produce 481: %s", buffer[:count])
	}
}

func TestUncertainNativeMutationDoesNotEmitAlternate503(t *testing.T) {
	f := newUDPOrderFixture(t, func(i *Ingress) {
		i.stateWriter = &StateEngine{}
		i.stateCompletion = func(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error) { return true, state.ErrUnknown }
		i.stateReject = func(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error {
			t.Error("uncertain completion attempted alternate rejection")
			return nil
		}
	})
	f.send(t, "BYE", "uncertain-existing-final", 2)
	if err := f.remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	wire := make([]byte, 4096)
	if _, _, err := f.remote.ReadFrom(wire); err == nil {
		t.Fatal("uncertain mutation emitted an alternate final response")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal(err)
	}
}
