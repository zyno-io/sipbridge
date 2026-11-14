package engine

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

const stateRequestFlowProofLimit = stateTransactionEmissions
const stateRequestProofBytes = 1600

// This exact type is embedded in the retained transaction and measured alone
// for consumable completion headroom, with a conservative bound for enclosing
// binary length-prefix growth. No protocol aggregate is copied to size it.
type stateRequestDisposition struct {
	AdmittedFlowProofs []string `json:"admittedFlowProofs,omitempty"`
	DispositionUntilMS int64    `json:"dispositionUntilMs,omitempty"`
}

func (d stateRequestDisposition) validate() error {
	if len(d.AdmittedFlowProofs) > stateRequestFlowProofLimit || d.DispositionUntilMS < 0 || d.DispositionUntilMS > 1<<53-1 {
		return state.ErrCorrupt
	}
	seen := make(map[string]bool, len(d.AdmittedFlowProofs))
	for _, proof := range d.AdmittedFlowProofs {
		if len(proof) != 64 || seen[proof] {
			return state.ErrCorrupt
		}
		for _, c := range proof {
			if c < '0' || c > '9' && c < 'a' || c > 'f' {
				return state.ErrCorrupt
			}
		}
		seen[proof] = true
	}
	if len(d.AdmittedFlowProofs) > 0 && d.DispositionUntilMS == 0 {
		return state.ErrCorrupt
	}
	return nil
}

func (d stateRequestDisposition) encodedBytes() (int, error) {
	if err := d.validate(); err != nil {
		return 0, err
	}
	if len(d.AdmittedFlowProofs) == 0 && d.DispositionUntilMS == 0 {
		return 0, nil
	}
	// Reserve the typed field plus conservative length-prefix growth through
	// transaction, context/maps, group and opaque outer payload framing. The
	// maximum 64-MiB record bounds each growth to at most four bytes.
	actual := proto.Size(storageEncodeEngineStateRequestDisposition(d)) + 32

	if actual > stateRequestProofBytes {
		return 0, state.ErrCorrupt
	}
	return actual, nil
}

func (e *StateEngine) requestFlowProof(flow *pb.FlowRef, wire []byte) (string, error) {
	if flow == nil {
		return "", errors.New("request flow required")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(flow)
	if err != nil {
		return "", err
	}
	return e.id("request-disposition-v1", string(encoded), flow.Transport, string(wire)), nil
}

func stateExactServerRequest(tx *stateTransaction, m portable.Message) bool {
	return stateServerTransaction(tx) && portable.MatchRequest(tx.Native.Origin, m) && tx.Native.Origin.EqualWire(m)
}

type stateRequestFence struct {
	context, transaction, kind string
}

// Classify before either GC path can discard the opposite disposition of the
// current immutable input. Keep only bounded identities, never another snapshot.
func (r *stateReduction) protectRequestFence() error {
	var flow *pb.FlowRef
	var wire []byte
	switch r.in.Kind {
	case "reject":
		flow, wire = r.in.Flow, r.in.Message.GetWire()
	case "ingest", "native-ingest":
		flow, wire = r.in.Ingest.GetFlow(), r.in.Ingest.GetWire()
	default:
		return nil
	}
	if flow == nil || len(wire) == 0 {
		return nil
	}
	m := r.incomingMessage(wire, flow, r.in.Kind != "reject")
	facts, err := m.Facts()
	if err != nil || !facts.IsRequest || facts.Method == sip.ACK {
		return nil
	}
	proof, err := r.engine.requestFlowProof(flow, wire)
	if err != nil {
		return err
	}
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		if c.Rejection && proto.Equal(c.Flow, flow) && c.Origin.EqualWireBytes(wire) {
			fence := &stateRequestFence{context: id, kind: "refused"}
			for key, tx := range c.Transactions {
				if stateExactServerRequest(tx, m) {
					fence.transaction = key
					break
				}
			}
			r.requestFence = fence
			return nil
		}
		for key, tx := range c.Transactions {
			if !stateExactServerRequest(tx, m) {
				continue
			}
			if contains(tx.AdmittedFlowProofs, proof) {
				r.requestFence = &stateRequestFence{context: id, transaction: key, kind: "admitted"}
				return nil
			}
			if len(tx.AdmittedFlowProofs) == stateRequestFlowProofLimit {
				r.requestFence = &stateRequestFence{context: id, transaction: key, kind: "saturated"}
			}
		}
	}
	return nil
}

func (r *stateReduction) transactionRetirable(id, key string, tx *stateTransaction) bool {
	return (r.requestFence == nil || r.requestFence.context != id || r.requestFence.transaction != key) &&
		(tx.RetireMS > 0 && max(tx.RetireMS, tx.DispositionUntilMS) <= r.now || r.settledNotifyRetirable(id, tx))
}

func (r *stateReduction) dispositionFloor() (int64, error) {
	floor := r.deadline + 120000
	if r.deadline <= 0 || floor > 1<<53-1 || floor < r.deadline {
		return 0, state.ErrCorrupt
	}
	return floor, nil
}

func (r *stateReduction) admitRequestFlow(tx *stateTransaction, flow *pb.FlowRef, wire []byte) (bool, error) {
	if tx.RetireMS > 0 && tx.RetireMS <= r.now {
		return false, nil // Protecting old proof cannot reopen its retired send window.
	}
	proof, err := r.engine.requestFlowProof(flow, wire)
	if err != nil {
		return false, err
	}
	if contains(tx.AdmittedFlowProofs, proof) {
		return true, nil // A fresh RPC identity cannot slide an existing proof's floor.
	}
	if r.deadline > 0 && r.now >= r.deadline {
		message := r.incomingMessage(wire, flow, true)
		facts, err := message.Facts()
		if err != nil || !facts.IsRequest || facts.Method != sip.CANCEL && facts.Method != sip.BYE {
			return false, nil
		}
	}
	if len(tx.AdmittedFlowProofs) >= stateRequestFlowProofLimit {
		return false, nil
	}
	floor, err := r.dispositionFloor()
	if err != nil {
		return false, err
	}
	tx.AdmittedFlowProofs = append(tx.AdmittedFlowProofs, proof)
	tx.DispositionUntilMS = max(tx.DispositionUntilMS, floor)
	return true, nil
}

func (r *stateReduction) handledRequest() error {
	// Controller maps Error/SipStatus back to a fresh refusal. A committed handled
	// disposition intentionally carries neither, even when no new SIP work ran.
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	return nil
}

func (r *stateReduction) handleRefusedRequest() error {
	fence := r.requestFence
	if fence == nil || fence.kind != "refused" {
		return nil
	}
	c := r.group.Contexts[fence.context]
	if tx := c.Transactions[fence.transaction]; tx != nil && tx.Native.LastResponse != nil {
		if r.deadline > 0 && r.now >= r.deadline || tx.RetireMS > 0 && tx.RetireMS <= r.now {
			return r.handledRequest() // Protection preserves classification, not a fresh send window.
		}
		if err := r.send(c, tx, *tx.Native.LastResponse, true); err != nil {
			return err
		}
	}
	return r.handledRequest()
}

func addStateProofHeadroom(total, count, bytes int) (int, error) {
	limit := int(^uint(0) >> 1)
	if total < 0 || count < 0 || bytes < 0 || bytes > 0 && count > (limit-total)/bytes {
		return 0, state.ErrCapacity
	}
	return total + count*bytes, nil
}

// The marker distinguishes a proven non-effectful failed Load from errors
// after Runtime.Apply may have recovered/committed a prior native input.
type stateReadFailure struct{ error }

func (e *stateReadFailure) Unwrap() error { return e.error }
func stateReadUnavailable(err error) bool {
	var failure *stateReadFailure
	return errors.As(err, &failure)
}

func classifyStateReadFailure(err error) error {
	if err == nil || errors.Is(err, state.ErrUnknown) {
		return err
	}
	for _, problem := range []error{state.ErrAuthority, state.ErrVersion, state.ErrClock, state.ErrCorrupt} {
		if errors.Is(err, problem) {
			return &stateReadFailure{err}
		}
	}
	var network net.Error
	if errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.DeadlineExceeded) {
		return &stateReadFailure{err}
	}
	// Authentication/type/script data errors are explicit Redis rejections. A
	// local pool-capacity error or caller cancellation is not that evidence.
	var rejected redis.Error
	if errors.As(err, &rejected) {
		return &stateReadFailure{err}
	}
	return err
}
