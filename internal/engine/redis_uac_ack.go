package engine

import (
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// prepareRedisUACFailureACK creates a native physical attempt for the exact
// ACK already emitted by the INVITE transaction reducer.
func prepareRedisUACFailureACK(snapshot *pb.ContextSnapshot, flow *pb.FlowRef, key []byte, ack portable.Message, procedure, dialog, result, wireField, outputField, operation string, deadline time.Time, replay, cleanup bool) (rediscore.OutputIntent, []rediscore.Change, error) {
	var empty rediscore.OutputIntent
	if snapshot == nil || snapshot.Context == nil || snapshot.Context.State == nil || snapshot.Context.State.Authority == nil || !validPolicyFlow(flow) || len(key) < 32 || len(key) > 1024 || !time.Now().Before(deadline) {
		return empty, nil, errNativeRecord
	}
	facts, err := ack.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.ACK || ack.Transport() != flow.Transport {
		return empty, nil, errNativeRecord
	}
	authority := snapshot.Context.State.Authority
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, snapshot.Context.State.GroupId, snapshot.Context.State.Incarnation, snapshot.CallId, snapshot.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_NATIVE_COMPLETION, DeadlineMs: uint64(deadline.UnixMilli()), ApplicationLeaseTerm: snapshot.ApplicationLeaseTerm, CompletionScope: scope}
	fingerprint, err := SendFingerprint(snapshot, &pb.SipMessage{Method: "ACK", Wire: ack.WireBytes()}, auth, flow)
	if err != nil {
		return empty, nil, err
	}
	ref.Id, auth.Fingerprint = "e-"+fingerprint, fingerprint
	write := &pb.FlowWrite{Effect: ref, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: authority.PrimaryId, OrderingProof: nativeIdentity(key, "native-write-authority-v1", fingerprint), CompletionScope: scope}}
	drain := drainNativeMessageObservation(ack, facts)
	metadata := &nativepb.Output{Write: write, Procedure: procedure, Dialog: dialog, Result: result, Operation: operation, Method: "ACK", Replay: replay, CleanupAfterAck: cleanup, AckSequence: facts.CSeq, Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	if replay {
		return nativePrepareExistingPhysicalOutput(metadata, ack, wireField, outputField)
	}
	return nativePreparePhysicalOutput(metadata, ack, wireField, outputField, nil, nil)
}
