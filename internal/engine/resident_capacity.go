package engine

import (
	"unsafe"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// An admitted protocol procedure reserves room for two bounded completion
// messages (for example ACK then BYE), plus their proofs and indexes. This is
// logical headroom, never a heap allocation or a per-Call-ID reservation.
const residentCompletionReserve = int64(2*65536 + 8192)

// accountResidentLocked changes only the touched context's accounting. c.mu is
// held; no code holds e.mu while waiting for c.mu. Releases cannot fail.
func (e *ResidentMemory) accountResidentLocked(c *residentContext, bytesDelta, reservedDelta int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c.bytes+bytesDelta < 0 || c.reserved+reservedDelta < 0 || e.bytes+bytesDelta < 0 || e.reserved+reservedDelta < 0 {
		return state.ErrCorrupt
	}
	if bytesDelta+reservedDelta > 0 && e.bytes+bytesDelta+e.reserved+reservedDelta > e.maxBytes {
		return state.ErrCapacity
	}
	c.bytes += bytesDelta
	c.reserved += reservedDelta
	e.bytes += bytesDelta
	e.reserved += reservedDelta
	return nil
}

func residentMessageBytes(message portable.Message) int64 {
	return int64(len(message.Wire) + len(message.Transport) + len(message.Source) + len(message.Destination))
}

func residentTransactionBytes(tx *residentTransaction) int64 {
	if tx == nil {
		return 0
	}
	size := int64(unsafe.Sizeof(*tx)) + residentMessageBytes(tx.native.Origin) + int64(len(tx.native.Key)+len(tx.native.ResponseTag)+len(tx.native.Error)+len(tx.requestID))
	if tx.native.LastResponse != nil {
		size += int64(unsafe.Sizeof(*tx.native.LastResponse)) + residentMessageBytes(*tx.native.LastResponse)
	}
	if tx.native.ACK != nil {
		size += int64(unsafe.Sizeof(*tx.native.ACK)) + residentMessageBytes(*tx.native.ACK)
	}
	if tx.policy != nil {
		size += int64(unsafe.Sizeof(*tx.policy)) + int64(len(tx.policy.DirectoryEpoch)) + residentFlowBytes(tx.policy.SelectedFlow)
		if delivery := tx.policy.ExpectedDelivery; delivery != nil {
			size += int64(unsafe.Sizeof(*delivery)) + int64(len(delivery.DirectoryEpoch)+len(delivery.RegistrationTicket)) + residentFlowBytes(delivery.Flow)
		}
	}
	if tx.credential != nil {
		size += int64(unsafe.Sizeof(*tx.credential)) + int64(len(tx.credential.Id)+len(tx.credential.Version))
	}
	for _, selector := range tx.selectors {
		size += int64(len(selector)) + 32
	}
	// Include map slots and the timer strings, not just timer payloads.
	size += int64(len(tx.native.Timers))*128 + 128
	return size + residentFlowBytes(tx.flow) + residentProcessBytes(tx.application)
}

func residentProcessBytes(process *pb.ProcessRef) int64 {
	if process == nil {
		return 0
	}
	return int64(unsafe.Sizeof(*process)) + int64(len(process.InstanceId)+len(process.Epoch))
}

func residentFlowBytes(flow *pb.FlowRef) int64 {
	if flow == nil {
		return 0
	}
	return int64(unsafe.Sizeof(*flow)) + int64(len(flow.ConnectionId)+len(flow.SourceIp)+len(flow.Listener)+len(flow.Transport)) + residentProcessBytes(flow.Ingress)
}

func residentProofBytes(write *pb.FlowWrite) int64 {
	// newResidentOutput shares repeated Flow and Effect pointers within this
	// immutable proof. Charge each owned object once, without codec traversal.
	permit, effect := write.Permit, write.Effect
	auth, ref := permit.Authorization, effect.Context
	size := int64(unsafe.Sizeof(*write) + unsafe.Sizeof(*permit) + unsafe.Sizeof(*effect) + unsafe.Sizeof(*auth) + unsafe.Sizeof(*ref))
	size += int64(len(permit.DirectoryEpoch) + len(permit.OrderingProof) + len(permit.CompletionScope) + len(effect.Id) + len(effect.AttemptId) + len(auth.CompletionScope) + len(auth.Fingerprint) + len(auth.ApplicationLeaseTerm) + len(ref.Id))
	size += residentProcessBytes(ref.Owner)
	if s := ref.State; s != nil {
		size += int64(unsafe.Sizeof(*s)) + int64(len(s.GroupId)+len(s.Incarnation)+len(s.CallId))
		if a := s.Authority; a != nil {
			size += int64(unsafe.Sizeof(*a)) + int64(len(a.Service)+len(a.Generation)+len(a.PrimaryId))
		}
	}
	return size
}

func residentContextBytes(c *residentContext) int64 {
	// Fixed map headers/initial buckets and derived index slots are charged even
	// for empty maps. Native records and output wire are charged on insertion.
	return int64(unsafe.Sizeof(*c)) + 768 + residentFlowBytes(c.flow) + residentSnapshotBytes(c.snapshot)
}

func residentSnapshotBytes(s *pb.ContextSnapshot) int64 {
	if s == nil {
		return 0
	}
	size := int64(unsafe.Sizeof(*s)) + int64(len(s.BindingId)+len(s.PrincipalId)+len(s.DialogId)+len(s.CallId)+len(s.LocalTag)+len(s.RemoteTag)+len(s.LocalUri)+len(s.RemoteUri)+len(s.RemoteTarget)+len(s.LocalSdp)+len(s.RemoteSdp)+len(s.Transport)+len(s.DirectoryEpoch)+len(s.ApplicationLeaseTerm))
	size += residentProcessBytes(s.Application) + residentProcessBytes(s.Executor)
	for _, selector := range s.Selectors {
		size += int64(len(selector)) + 16
	}
	if s.Context != nil {
		size += int64(unsafe.Sizeof(*s.Context)) + int64(len(s.Context.Id)) + residentProcessBytes(s.Context.Owner)
		if ref := s.Context.State; ref != nil {
			size += int64(unsafe.Sizeof(*ref)) + int64(len(ref.GroupId)+len(ref.Incarnation)+len(ref.CallId))
			if a := ref.Authority; a != nil {
				size += int64(unsafe.Sizeof(*a)) + int64(len(a.Service)+len(a.Generation)+len(a.PrimaryId))
			}
		}
	}
	if d := s.Delivery; d != nil {
		size += int64(unsafe.Sizeof(*d)) + int64(len(d.DirectoryEpoch)+len(d.RegistrationTicket)) + residentFlowBytes(d.Flow)
	}
	return size
}

// Count the owned DTO values once at publication, not via a codec or a walk of
// the context's native history. Fixed map-slot allowances are conservative.
func residentSipMessageBytes(message *pb.SipMessage) int64 {
	if message == nil {
		return 0
	}
	size := int64(unsafe.Sizeof(*message)) + int64(len(message.Wire)+len(message.Body)+len(message.Method)+len(message.Uri)+len(message.Reason))
	for _, h := range message.Headers {
		if h != nil {
			size += int64(unsafe.Sizeof(*h)) + int64(len(h.Name)+len(h.Value)) + 8
		}
	}
	for _, selector := range message.Selectors {
		size += int64(len(selector)) + 16
	}
	for key, list := range message.Addresses {
		size += int64(len(key)) + 64
		if list == nil {
			continue
		}
		size += int64(unsafe.Sizeof(*list))
		for _, address := range list.Values {
			if address == nil {
				continue
			}
			size += int64(unsafe.Sizeof(*address)) + int64(len(address.Name)+len(address.Uri)) + 8
			for key, value := range address.Params {
				size += int64(len(key)+len(value)) + 64
			}
		}
	}
	for _, via := range message.Vias {
		if via == nil {
			continue
		}
		size += int64(unsafe.Sizeof(*via)) + int64(len(via.Protocol)+len(via.Host)) + 8
		for key, value := range via.Params {
			size += int64(len(key)+len(value)) + 64
		}
	}
	if auth := message.Authorization; auth != nil {
		size += int64(unsafe.Sizeof(*auth)) + int64(len(auth.Username)+len(auth.Realm)+len(auth.Nonce)+len(auth.Uri)+len(auth.Response)+len(auth.Algorithm)+len(auth.Qop)+len(auth.Nc)+len(auth.Cnonce))
	}
	return size
}
