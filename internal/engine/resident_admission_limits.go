package engine

import (
	"net/netip"

	"github.com/emiago/sipgo/sip/portable"
)

const (
	residentPendingLimit       = 128
	residentSourceLimit        = 32
	residentRefusalLimit       = 1024
	residentNativeRefusalLimit = 1024
)

type residentAdmissionClass uint8

const (
	residentUncounted residentAdmissionClass = iota
	residentPending
	residentInbound
	residentOutbound
	residentRefusal
	residentNativeRefusal
)

func normalizedAdmissionSource(source string) string {
	if address, err := netip.ParseAddr(source); err == nil {
		return address.Unmap().String()
	}
	return ""
}

// Release the lookup reservation. Direct ingress transfers its pending count
// and source charge to the published context before releasing this index.
func (e *ResidentMemory) releaseResidentAdmissionLocked(id string, admission residentAdmission, transferred bool) {
	delete(e.admissions, id)
	if e.admissionByWire[admission.wireHash] == id {
		delete(e.admissionByWire, admission.wireHash)
	}
	e.admissionCounts[admission.callID]--
	if e.admissionCounts[admission.callID] == 0 {
		delete(e.admissionCounts, admission.callID)
	}
	e.deadlines.remove(residentDeadlineKey{kind: residentAdmissionDeadline, contextID: id})
	if !transferred {
		e.classCounts[residentPending]--
		e.releaseResidentSourceLocked(admission.source)
	}
}

func (e *ResidentMemory) releaseResidentSourceLocked(source string) {
	if source != "" {
		e.pendingSources[source]--
		if e.pendingSources[source] == 0 {
			delete(e.pendingSources, source)
		}
	}
}

func (e *ResidentMemory) changeResidentAdmissionLocked(c *residentContext, next residentAdmissionClass) {
	if c.admissionClass != residentUncounted {
		e.classCounts[c.admissionClass]--
	}
	if c.admissionClass == residentPending {
		e.releaseResidentSourceLocked(c.admissionSource)
		c.admissionSource = ""
	}
	c.admissionClass = next
	if next != residentUncounted {
		e.classCounts[next]++
	}
}

func residentDecisionAdmission(tx *residentTransaction) residentAdmissionClass {
	if tx.native.Phase == portable.Completed || tx.native.Phase == portable.Terminated {
		if tx.native.LastResponse != nil {
			final, err := tx.native.LastResponse.Facts()
			if err == nil && final.Status >= 300 {
				return residentRefusal
			}
		}
	}
	return residentInbound
}
