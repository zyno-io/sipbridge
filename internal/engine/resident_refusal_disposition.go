package engine

import (
	"crypto/sha256"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

const residentRefusalDispositionLimit = 4096

var errResidentRefusalDuplicate = errors.New("settled native refusal duplicate")

type residentRefusalDisposition struct {
	wireHash  [32]byte
	flow      *pb.FlowRef
	expires   time.Time
	bytes     int64
	tlsInvite bool
}

func (d residentRefusalDisposition) matches(wire []byte, flow *pb.FlowRef) bool {
	return d.wireHash == sha256.Sum256(wire) && (proto.Equal(d.flow, flow) ||
		d.tlsInvite && flow.GetTransport() == "tls" && validPolicyFlow(flow) && sameProcess(d.flow.Ingress, flow.Ingress))
}

// Called with the context lock after physical proof, receipt pins, and native
// transaction work have settled. Release the heavy context on the same pass.
func (e *ResidentMemory) compactResidentRefusalLocked(c *residentContext, now time.Time) bool {
	if (c.admissionClass != residentRefusal && c.admissionClass != residentNativeRefusal) ||
		c.subscription != nil || len(c.dialogs) != 0 || len(c.transactions) != 1 {
		return false
	}
	for _, tx := range c.transactions {
		if !tx.retired || !now.Before(tx.retireAt) || len(tx.selectors) != 1 || !strings.HasPrefix(tx.selectors[0], "stx:") {
			return false
		}
		// The original application decision can still be replayed up to its
		// own deadline. Use the existing indexed scheduler to reconsider.
		if c.command != nil && now.Before(tx.presentDeadline) {
			e.mu.Lock()
			e.deadlines.set(residentDeadlineKey{kind: residentRefusalCompactDeadline, contextID: c.snapshot.Context.Id}, 0, tx.presentDeadline)
			e.mu.Unlock()
			e.wakeDeadline()
			return false
		}
		selector := tx.selectors[0]
		bytes := int64(256+len(selector)) + residentFlowBytes(tx.flow)
		e.mu.Lock()
		defer e.mu.Unlock()
		if len(e.refusalDispositions) >= residentRefusalDispositionLimit || e.transactions[selector] != c ||
			e.bytes+e.reserved+bytes-c.bytes-c.reserved > e.maxBytes {
			return false
		}
		if e.refusalDispositions == nil {
			e.refusalDispositions = make(map[string]residentRefusalDisposition)
		}
		e.refusalDispositions[selector] = residentRefusalDisposition{wireHash: tx.wireHash, flow: tx.flow, expires: tx.retireAt, bytes: bytes,
			tlsInvite: c.requests["refusal"] == "" && c.snapshot.Kind == pb.ContextSnapshot_UAS && c.snapshot.Transport == "tls"}
		e.bytes += bytes
		e.deadlines.set(residentDeadlineKey{kind: residentRefusalDispositionDeadline, record: selector}, uint64(tx.retireAt.UnixMilli()), tx.retireAt)
		e.deadlines.remove(residentDeadlineKey{kind: residentTransactionRetirementDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key})
		e.wakeDeadline()
		return true
	}
	return false
}

func (e *ResidentMemory) residentSelectorOccupiedLocked(selectors []string) bool {
	for _, selector := range selectors {
		if strings.HasPrefix(selector, "stx:") && (e.transactions[selector] != nil || time.Now().Before(e.refusalDispositions[selector].expires)) {
			return true
		}
	}
	return false
}

func (e *ResidentMemory) observeResidentRefusalLocked(selector string, wire []byte, flow *pb.FlowRef, message sip.Message) error {
	disposition, ok := e.refusalDispositions[selector]
	if !ok || !time.Now().Before(disposition.expires) {
		return nil
	}
	if request, ok := message.(*sip.Request); ok && (request.IsAck() || flow != nil && disposition.matches(wire, flow)) {
		return errResidentRefusalDuplicate
	}
	return rejectIngest(481, "native refusal transaction retained")
}

func (e *ResidentMemory) fireResidentRefusalDeadline(item residentDeadline) bool {
	switch item.key.kind {
	case residentRefusalDispositionDeadline:
		e.mu.Lock()
		disposition := e.refusalDispositions[item.key.record]
		if uint64(disposition.expires.UnixMilli()) == item.generation && !time.Now().Before(disposition.expires) {
			delete(e.refusalDispositions, item.key.record)
			e.bytes -= disposition.bytes
		}
		e.mu.Unlock()
		return true
	case residentRefusalCompactDeadline:
		e.mu.Lock()
		c := e.contexts[item.key.contextID]
		e.mu.Unlock()
		if c != nil {
			c.mu.Lock()
			e.afterResidentOutputReleaseLocked(c)
			c.mu.Unlock()
		}
		return true
	}
	return false
}
