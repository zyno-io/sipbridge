package engine

import (
	"errors"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

const residentPhonePrefix = "phone-"

func localPhoneID(id string) bool { return strings.HasPrefix(id, residentPhonePrefix) }

// This identity selects a sole process-lifetime owner, not a backend fallback.
func (e *ResidentMemory) phoneIncarnation() string {
	return e.id("phone-incarnation", e.cfg.Process.InstanceId, e.cfg.Process.Epoch)[:48]
}

func (e *ResidentMemory) phoneID(callID, nonce string) string {
	return residentPhonePrefix + nonce + "-" + e.id("phone-context", e.cfg.Process.InstanceId, e.cfg.Process.Epoch, callID, nonce)[:48]
}

func (e *ResidentMemory) validatePhoneRef(ref *pb.ContextRef) error {
	if ref == nil || ref.State == nil || !sameProcess(ref.Owner, e.cfg.Process) || ref.State.Incarnation != e.phoneIncarnation() || !localPhoneID(ref.Id) {
		return errors.New("local phone authority mismatch")
	}
	parts := strings.SplitN(strings.TrimPrefix(ref.Id, residentPhonePrefix), "-", 2)
	if len(parts) != 2 || e.phoneID(ref.State.CallId, parts[0]) != ref.Id {
		return errors.New("local phone identity mismatch")
	}
	return nil
}

// The caller holds only its native context lock. No Peer or Server method may
// run inside publish; the session capability guards native indexes last.
func (e *ResidentMemory) guardResidentPhone(capability *control.ApplicationCapability, application *pb.ProcessRef, term string, publish func(time.Time) error) error {
	if e.phoneDomain {
		return capability.Guard(application, term, publish)
	}
	return publish(time.Time{})
}

func (e *ResidentMemory) releasedPhoneSubscriptionLocked(c *residentContext) bool {
	return c.subscription != nil && c.ownerLost && !c.snapshot.Ended && !c.subscription.Terminating &&
		time.Now().UnixMilli() < c.subscription.ExpiresMS && c.flow != nil && sameProcess(c.flow.Ingress, e.cfg.Process) &&
		(c.flow.Transport == "udp" || e.cfg.LiveFlow == nil || e.cfg.LiveFlow(c.flow))
}

func cloneResidentApplication(application *pb.ProcessRef) *pb.ProcessRef {
	return proto.Clone(application).(*pb.ProcessRef)
}
