package engine

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"sipbridge/internal/control"
	"sipbridge/internal/state"
)

func TestNativeMissingTaggedRefusalDoesNotAllocate(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, backend)
			for _, method := range []string{"BYE", "INVITE", "INFO", "SUBSCRIBE", "NOTIFY"} {
				t.Run(method, func(t *testing.T) {
					message := udpOrderMessage(t, method, "absent-"+method, 2)
					if err := f.engine.Reject(context.Background(), f.flow, message, 503); !errors.Is(err, state.ErrNotFound) {
						t.Fatalf("known absent tagged request did not preserve absence: %v", err)
					}
					ref, err := f.engine.store.GroupForCallID("absent-" + method)
					if err != nil {
						t.Fatal(err)
					}
					if _, _, err := f.engine.store.ReadState(context.Background(), ref); !errors.Is(err, state.ErrNotFound) {
						t.Fatalf("missing-dialog refusal allocated a group: %v", err)
					}
				})
			}
		})
	}
}

func TestNativeMissingDialogWithoutReadyControllerReturns481(t *testing.T) {
	for _, backend := range []string{state.MemoryBackend, state.RedisBackend} {
		t.Run(backend, func(t *testing.T) {
			native := newNativeOwnerFixtureBackend(t, backend)
			native.engine.work = make(chan struct{}, 64)
			f := newUDPOrderFixture(t, func(i *Ingress) {
				i.control = &control.Server{} // No ready peer; use actual native reads/refusal.
				if err := i.ConfigureStateRuntime(native.engine, native.engine.LocateRoute, native.engine.Reject); err != nil {
					t.Fatal(err)
				}
				i.ConfigureNativeCompletion(native.engine.TryNativeCompletion)
			})
			var first []byte
			for n := 0; n < 2; n++ {
				f.send(t, "BYE", "erased-dialog", 2)
				if err := f.remote.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
					t.Fatal(err)
				}
				wire := make([]byte, 4096)
				count, _, err := f.remote.ReadFrom(wire)
				if err != nil {
					t.Fatal(err)
				}
				wire = wire[:count]
				parsed, err := sip.ParseMessage(wire)
				if err != nil {
					t.Fatal(err)
				}
				response, ok := parsed.(*sip.Response)
				if !ok || response.StatusCode != 481 || response.CSeq().SeqNo != 2 || response.CallID().Value() != "erased-dialog" {
					t.Fatalf("absent dialog without a controller did not receive exact 481: %s", wire)
				}
				if n > 0 && !bytes.Equal(first, wire) {
					t.Fatal("repeated absent-dialog refusal changed its response")
				}
				first = append([]byte(nil), wire...)
			}
			f.send(t, "ACK", "erased-dialog", 2)
			if err := f.remote.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.remote.ReadFrom(make([]byte, 4096)); err == nil {
				t.Fatal("unknown ACK received a refusal")
			} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
				t.Fatal(err)
			}
			ref, err := native.engine.store.GroupForCallID("erased-dialog")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := native.engine.store.ReadState(context.Background(), ref); !errors.Is(err, state.ErrNotFound) {
				t.Fatalf("wire refusal recreated erased group: %v", err)
			}
			select {
			case <-f.received:
				t.Fatal("missing dialog reached controller routing")
			default:
			}
		})
	}
}
