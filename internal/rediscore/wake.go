package rediscore

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Wake is a coalesced, generation-based invalidation signal, never authority.
// A consumer snapshots the generation before reading a head, verifies it after
// the read, and waits on the returned channel only when it is still unchanged.
// No drain operation can erase a notification concurrent with a head read.
type Wake struct {
	mu           sync.Mutex
	generation   uint64
	changed      chan struct{}
	connected    bool
	closed       bool
	subscription *redis.PubSub
	channel      string
	results      string
	onResult     func(ResultHint)
	onUDP        func(UDPHint)
	worker       string
}

// ResultHint identifies one transient result waiter. It is a wakeup only, not
// proof of a result or authority to read it.
type ResultHint struct {
	Context, Incarnation string
	Sequence             uint64
}

func encodeResultHint(h ResultHint) ([]byte, error) {
	if !reference(h.Context) || !reference(h.Incarnation) || h.Sequence == 0 || h.Sequence > 1<<53-1 {
		return nil, ErrConflict
	}
	b := []byte{1, byte(len(h.Context))}
	b = append(b, h.Context...)
	b = append(b, byte(len(h.Incarnation)))
	b = append(b, h.Incarnation...)
	b = binary.BigEndian.AppendUint64(b, h.Sequence)
	return b, nil
}

func decodeResultHint(b []byte) (ResultHint, error) {
	var h ResultHint
	if len(b) < 12 || b[0] != 1 {
		return h, ErrCorrupt
	}
	n := int(b[1])
	if n == 0 || len(b) < 3+n {
		return h, ErrCorrupt
	}
	h.Context = string(b[2 : 2+n])
	p := 2 + n
	n = int(b[p])
	p++
	if n == 0 || len(b) != p+n+8 {
		return ResultHint{}, ErrCorrupt
	}
	h.Incarnation = string(b[p : p+n])
	h.Sequence = binary.BigEndian.Uint64(b[p+n:])
	if !reference(h.Context) || !reference(h.Incarnation) || h.Sequence == 0 || h.Sequence > 1<<53-1 {
		return ResultHint{}, ErrCorrupt
	}
	return h, nil
}

// SubscribeWake waits for the actual subscription acknowledgement. The caller
// must continuously call Receive on one goroutine and must stop scheduling
// against a disconnected subscription until a new acknowledgement is observed.
func (s *Store) SubscribeWake(ctx context.Context, worker string) (*Wake, error) {
	return s.subscribeWake(ctx, worker, nil)
}

// SubscribeWakeResults acknowledges both channels on the same connection.
func (s *Store) SubscribeWakeResults(ctx context.Context, worker string, onResult func(ResultHint), onUDP ...func(UDPHint)) (*Wake, error) {
	if onResult == nil || worker == "results" {
		return nil, ErrConflict
	}
	if len(onUDP) > 1 {
		return nil, ErrConflict
	}
	wake, err := s.subscribeWake(ctx, worker, onResult)
	if err == nil && len(onUDP) == 1 {
		wake.onUDP = onUDP[0]
	}
	return wake, err
}

func (s *Store) subscribeWake(ctx context.Context, worker string, onResult func(ResultHint)) (*Wake, error) {
	if !reference(worker) {
		return nil, ErrConflict
	}
	channel := s.root + "core:wake:" + worker
	results := ""
	channels := []string{channel}
	if onResult != nil {
		results = s.root + "core:wake:results"
		channels = append(channels, results)
	}
	pub := s.client.Subscribe(ctx, channels...)
	seen := make(map[string]bool, len(channels))
	for len(seen) < len(channels) {
		v, err := pub.Receive(ctx)
		if err != nil {
			_ = pub.Close()
			return nil, err
		}
		switch x := v.(type) {
		case *redis.Subscription:
			if x.Kind != "subscribe" || (x.Channel != channel && x.Channel != results) || seen[x.Channel] {
				_ = pub.Close()
				return nil, ErrCorrupt
			}
			seen[x.Channel] = true
		case *redis.Message:
			// No binding or waiter exists until both acknowledgements arrive.
			if x.Channel != channel && x.Channel != results {
				_ = pub.Close()
				return nil, ErrCorrupt
			}
		default:
			_ = pub.Close()
			return nil, ErrCorrupt
		}
	}
	return &Wake{generation: 1, changed: make(chan struct{}), connected: true, subscription: pub, channel: channel, results: results, onResult: onResult, worker: worker}, nil
}

// Snapshot returns the current generation, a channel closed by the next
// invalidation, and whether the subscription has been acknowledged and is live.
func (w *Wake) Snapshot() (uint64, <-chan struct{}, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.generation, w.changed, w.connected && !w.closed
}

func (w *Wake) invalidate(connected bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.generation++
	w.connected = connected
	close(w.changed)
	w.changed = make(chan struct{})
}

// Receive observes messages and subscription epochs directly rather than via a
// lossy buffered library channel. It invalidates on both messages and renewed
// subscription acknowledgements; any receive failure marks the hint unusable.
func (w *Wake) Receive(ctx context.Context) error {
	v, err := w.subscription.Receive(ctx)
	if err != nil {
		w.invalidate(false)
		return err
	}
	switch x := v.(type) {
	case *redis.Message:
		_, _, connected := w.Snapshot()
		if !connected || (x.Channel != w.channel && x.Channel != w.results) {
			w.invalidate(false)
			return ErrAuthority
		}
		if w.onResult != nil && x.Channel == w.results {
			h, err := decodeResultHint([]byte(x.Payload))
			if err != nil {
				return nil
			}
			w.onResult(h)
			return nil
		}
		if len(x.Payload) > 0 && x.Payload[0] == 2 {
			if w.onUDP != nil {
				hint, err := decodeUDPHint(w.worker, []byte(x.Payload))
				if err == nil {
					w.onUDP(hint)
				}
			}
			return nil
		}
		w.invalidate(true)
	case *redis.Subscription:
		if w.onResult != nil {
			w.invalidate(false)
			return ErrAuthority
		}
		if x.Kind != "subscribe" || x.Channel != w.channel {
			w.invalidate(false)
			return ErrAuthority
		}
		w.invalidate(true)
	default:
		w.invalidate(false)
		return ErrCorrupt
	}
	return nil
}

func (w *Wake) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.connected = false
	w.generation++
	close(w.changed)
	w.mu.Unlock()
	return w.subscription.Close()
}
