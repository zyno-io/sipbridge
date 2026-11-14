package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/state"
)

const (
	drainFlowHints    = 64
	drainProcessHints = 8192
	drainHintLifetime = 2 * time.Minute
)

// DrainOptions bounds the entire withdrawal/transaction phase. Completion
// reporting has a separate fixed two-second tail while the runtime stays alive.
type DrainOptions struct{ WithdrawalGrace, Timeout time.Duration }

func (o DrainOptions) Validate() error {
	if o.WithdrawalGrace < 0 || o.WithdrawalGrace > 60*time.Second || o.Timeout <= o.WithdrawalGrace || o.Timeout > 2*time.Minute {
		return errors.New("SIP drain requires withdrawal grace 0..60s and timeout greater than grace and at most 2m")
	}
	return nil
}

type drainHintKey struct {
	Identity [32]byte
	Outbound bool
}

// Called only on an already validated pinned-parser value. These hashes are
// local shutdown hints, not native matching/admission or physical authority.
func drainMessageObservation(message sip.Message) state.LocalDrainObservation {
	var out state.LocalDrainObservation
	if message == nil || message.CallID() == nil || message.CSeq() == nil || message.From() == nil || message.To() == nil {
		return out
	}
	_, out.Request = message.(*sip.Request)
	method := message.CSeq().MethodName
	out.Invite, out.Ack, out.Cancel = method == sip.INVITE, method == sip.ACK, method == sip.CANCEL
	out.Initial = out.Request && tag(message.To().Params) == ""
	if response, ok := message.(*sip.Response); ok {
		out.Status = uint16(response.StatusCode)
	}
	if key, err := sip.ClientTxKeyMake(message); err == nil {
		out.Transaction = sha256.Sum256([]byte(strings.Join([]string{key, message.CallID().Value(), tag(message.From().Params), strconv.FormatUint(uint64(message.CSeq().SeqNo), 10)}, "\x00")))
	}
	// A 2xx ACK has a new Via branch, so its match uses directional dialog
	// identity and CSeq. Forks each get their own ACK obligation.
	out.ACK = sha256.Sum256([]byte(strings.Join([]string{message.CallID().Value(), tag(message.From().Params), tag(message.To().Params), strconv.FormatUint(uint64(message.CSeq().SeqNo), 10)}, "\x00")))
	return out
}

func drainNativeMessageObservation(message portable.Message, facts portable.MessageFacts) state.LocalDrainObservation {
	key, err := message.ClientTxKey()
	return drainFrozenMessageObservation(facts, key, err)
}

func drainObservedMessageObservation(message portable.Message, facts portable.MessageFacts) state.LocalDrainObservation {
	key, err := message.ObservedClientTxKey()
	return drainFrozenMessageObservation(facts, key, err)
}

func drainFrozenMessageObservation(facts portable.MessageFacts, key string, keyErr error) state.LocalDrainObservation {
	out := state.LocalDrainObservation{Request: facts.IsRequest, Status: uint16(facts.Status)}
	out.Invite, out.Ack, out.Cancel = facts.CSeqMethod == sip.INVITE, facts.CSeqMethod == sip.ACK, facts.CSeqMethod == sip.CANCEL
	out.Initial = facts.IsRequest && facts.ToTag == ""
	sequence := strconv.FormatUint(uint64(facts.CSeq), 10)
	if keyErr == nil {
		out.Transaction = sha256.Sum256([]byte(strings.Join([]string{key, facts.CallID, facts.FromTag, sequence}, "\x00")))
	}
	out.ACK = sha256.Sum256([]byte(strings.Join([]string{facts.CallID, facts.FromTag, facts.ToTag, sequence}, "\x00")))
	return out
}

func (i *Ingress) wakeDrain() {
	if !i.draining.Load() {
		return
	}
	select {
	case i.drainWake <- struct{}{}:
	default:
	}
}

// All hint access is under the existing physical-flow write lock.
func (i *Ingress) removeDrainHintLocked(flow *physicalFlow, key drainHintKey) {
	if _, exists := flow.drainHints[key]; exists {
		delete(flow.drainHints, key)
		i.drainHintCount.Add(-1)
		if len(flow.drainHints) == 0 {
			flow.drainHints = nil
		}
	}
}

func (i *Ingress) pruneDrainHintsLocked(flow *physicalFlow, now time.Time) {
	for key, until := range flow.drainHints {
		if !now.Before(until) {
			// Evict bounded advisory metadata, never infer peer completion
			// from its age. An unresolved old hint makes drain conservative.
			flow.drainUnknown = true
			i.removeDrainHintLocked(flow, key)
		}
	}
}

func (i *Ingress) addDrainHintLocked(flow *physicalFlow, key drainHintKey, now time.Time) {
	if _, exists := flow.drainHints[key]; exists {
		return
	}
	if len(flow.drainHints) >= drainFlowHints {
		flow.drainUnknown = true
		return
	}
	if i.drainHintCount.Add(1) > drainProcessHints {
		i.drainHintCount.Add(-1)
		flow.drainUnknown = true
		return
	}
	if flow.drainHints == nil {
		flow.drainHints = make(map[drainHintKey]time.Time)
	}
	flow.drainHints[key] = now.Add(drainHintLifetime)
}

func (i *Ingress) observeDrainLocked(flow *physicalFlow, message state.LocalDrainObservation, outbound bool, now time.Time) {
	if flow.conn == nil {
		return
	}
	i.pruneDrainHintsLocked(flow, now)
	if message.Transaction == [32]byte{} {
		flow.drainUnknown = true
		return
	}
	if message.Request {
		if message.Ack {
			i.removeDrainHintLocked(flow, drainHintKey{message.ACK, outbound})
		} else {
			i.addDrainHintLocked(flow, drainHintKey{message.Transaction, outbound}, now)
		}
	} else if message.Status >= 200 {
		i.removeDrainHintLocked(flow, drainHintKey{message.Transaction, !outbound})
		if message.Invite {
			i.addDrainHintLocked(flow, drainHintKey{message.ACK, !outbound}, now)
		}
	}
}

func (i *Ingress) beginDrainRoute(flow *physicalFlow, message state.LocalDrainObservation) bool {
	flow.write.Lock()
	defer flow.write.Unlock()
	if flow.closed.Load() {
		return false
	}
	if i.draining.Load() && message.Initial && !message.Ack && !message.Cancel {
		if _, admitted := flow.drainHints[drainHintKey{message.Transaction, false}]; !admitted {
			return false
		}
	}
	flow.drainRoutes++
	if message.Request && !message.Ack {
		i.observeDrainLocked(flow, message, false, time.Now())
	}
	// Native response handling may synchronously dispatch its ACK before route
	// returns. Publish the advisory obligation first so that write clears it.
	if !message.Request && message.Invite && message.Status >= 200 && flow.conn != nil {
		i.addDrainHintLocked(flow, drainHintKey{message.ACK, true}, time.Now())
	}
	return true
}

// A tagless request may be a replay of a completed INVITE whose local drain
// hint is gone. Only native shared-state admission can decide that question.
func (i *Ingress) beginDrainReplay(flow *physicalFlow) bool {
	flow.write.Lock()
	defer flow.write.Unlock()
	if flow.closed.Load() || !i.draining.Load() {
		return false
	}
	flow.drainRoutes++
	return true
}

func (i *Ingress) finishDrainRoute(flow *physicalFlow, message state.LocalDrainObservation, accepted bool) {
	flow.write.Lock()
	flow.drainRoutes--
	if accepted && !message.Request && message.Invite && message.Status >= 200 {
		i.removeDrainHintLocked(flow, drainHintKey{message.Transaction, true})
	} else if accepted && (!message.Request || message.Ack) {
		i.observeDrainLocked(flow, message, false, time.Now())
	}
	flow.write.Unlock()
	i.wakeDrain()
}

func (i *Ingress) registerDrainListener(listener net.Listener) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.draining.Load() {
		return false
	}
	if i.drainListeners == nil {
		i.drainListeners = make(map[net.Listener]struct{})
	}
	i.drainListeners[listener] = struct{}{}
	return true
}

func (i *Ingress) unregisterDrainListener(listener net.Listener) {
	i.mu.Lock()
	delete(i.drainListeners, listener)
	i.mu.Unlock()
}

func closeDrainConnection(conn net.Conn) {
	// Do not let a TLS close-notify write extend the shutdown bound. Closing
	// the underlying stream fences all writers and prompts peer reconnection.
	if wrapped, ok := conn.(interface{ NetConn() net.Conn }); ok {
		conn = wrapped.NetConn()
	}
	_ = conn.Close()
}

// Drain is invoked once after readiness withdrawal, before runtime cancellation.
// It never waits for a logical dialog to end or changes stored protocol state.
func (i *Ingress) Drain(ctx context.Context, options DrainOptions) error {
	if err := options.Validate(); err != nil {
		return err
	}
	start := time.Now()
	i.mu.Lock()
	if i.draining.Swap(true) {
		i.mu.Unlock()
		return errors.New("SIP ingress already draining")
	}
	listeners := make([]net.Listener, 0, len(i.drainListeners))
	for listener := range i.drainListeners {
		listeners = append(listeners, listener)
	}
	i.mu.Unlock()
	for _, listener := range listeners {
		_ = listener.Close()
	}
	grace, hard := start.Add(options.WithdrawalGrace), start.Add(options.Timeout)
	for {
		now := time.Now()
		forced := !now.Before(hard) || ctx.Err() != nil
		i.mu.Lock()
		flows := make([]*physicalFlow, 0, len(i.flows))
		for _, flow := range i.flows {
			flows = append(flows, flow)
		}
		udp := i.udp
		i.mu.Unlock()
		next := hard
		if now.Before(grace) {
			next = grace
		}
		busy := false
		for _, flow := range flows {
			if flow.closed.Load() {
				continue
			}
			if forced {
				// Closing a socket interrupts an already started syscall; its
				// writer retains its actual partial/uncertain outcome.
				flow.closed.Store(true)
				if flow.conn != nil {
					closeDrainConnection(flow.conn)
				}
				continue
			}
			if !flow.write.TryLock() {
				busy = true
				continue
			}
			i.pruneDrainHintsLocked(flow, now)
			for _, until := range flow.drainHints {
				if until.Before(next) {
					next = until
				}
			}
			idle := flow.conn != nil && flow.drainRoutes == 0 && len(flow.drainHints) == 0 && !flow.drainUnknown && i.drainReports.Load() == 0
			if !now.Before(grace) && idle {
				flow.closed.Store(true)
				closeDrainConnection(flow.conn)
			} else {
				busy = true
			}
			flow.write.Unlock()
		}
		if forced || !busy && !now.Before(grace) {
			if udp != nil {
				_ = udp.Close()
			}
			break
		}
		timer := time.NewTimer(max(0, time.Until(next)))
		select {
		case <-ctx.Done():
		case <-i.drainWake:
		case <-timer.C:
		}
		timer.Stop()
	}
	// Keep Redis/control and exact completion reporting alive after the last
	// socket closes. This is bounded even when Redis is unavailable.
	tail := time.NewTimer(2 * time.Second)
	defer tail.Stop()
	for i.drainReports.Load() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tail.C:
			return nil
		case <-i.drainWake:
		}
	}
	return ctx.Err()
}
