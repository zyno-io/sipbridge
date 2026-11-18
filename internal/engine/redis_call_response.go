package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// prepareRedisCallResponseOutput constructs a physical response without changing its selected native record.
func prepareRedisCallResponseOutput(method string, status uint32, snapshot *pb.ContextSnapshot, flow *pb.FlowRef, key []byte, selected portable.Message, procedureField, dialogField, responseField, physicalField, outputField string, until time.Time, replay bool) (rediscore.OutputIntent, []rediscore.Change, bool, error) {
	return prepareRedisCallResponseWithResult(method, status, snapshot, flow, key, selected, procedureField, dialogField, responseField, physicalField, outputField, "", "", nil, until, replay)
}

// A committed response has native response authority; an ordinary command
// also retains its own result and the policy epoch selected at admission.
func prepareRedisCallResponseWithResult(method string, status uint32, snapshot *pb.ContextSnapshot, flow *pb.FlowRef, key []byte, selected portable.Message, procedureField, dialogField, responseField, physicalField, outputField, resultField, operation string, policy *pb.CommandAdmission, until time.Time, replay bool) (rediscore.OutputIntent, []rediscore.Change, bool, error) {
	var empty rediscore.OutputIntent
	if snapshot == nil || snapshot.Context == nil || snapshot.Context.State == nil || snapshot.Context.State.Authority == nil || !validPolicyFlow(flow) || len(key) < 32 || len(key) > 1024 || !time.Now().Before(until) {
		return empty, nil, false, errNativeRecord
	}
	epoch := snapshot.Context.State.Authority.PrimaryId
	if resultField != "" {
		if policy == nil || operation == "" || replay || policy.ExpiresAtMs == 0 || uint64(until.UnixMilli()) > policy.ExpiresAtMs {
			return empty, nil, false, errNativeRecord
		}
		epoch = policy.DirectoryEpoch
	} else if policy != nil || resultField != "" || operation != "" {
		return empty, nil, false, errNativeRecord
	}
	physical, err := selected.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
	if err != nil {
		return empty, nil, false, err
	}
	shared := physical.EqualWire(selected)
	if shared {
		physicalField = responseField
	}
	facts, err := physical.Facts()
	if err != nil || facts.IsRequest || string(facts.CSeqMethod) != method || uint32(facts.Status) != status {
		return empty, nil, false, errors.New("invalid call response")
	}
	authority := snapshot.Context.State.Authority
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, snapshot.Context.State.GroupId, snapshot.Context.State.Incarnation, snapshot.CallId, snapshot.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_RESPONSE, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: snapshot.ApplicationLeaseTerm, CompletionScope: scope}
	fingerprint, err := SendFingerprint(snapshot, &pb.SipMessage{Method: string(facts.CSeqMethod), Status: uint32(facts.Status), Wire: physical.WireBytes()}, auth, flow)
	if err != nil {
		return empty, nil, false, err
	}
	ref.Id, auth.Fingerprint = "e-"+fingerprint, fingerprint
	write := &pb.FlowWrite{Effect: ref, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: epoch, OrderingProof: nativeIdentity(key, "native-write-authority-v1", fingerprint), CompletionScope: scope}}
	drain := drainNativeMessageObservation(physical, facts)
	output := &nativepb.Output{Write: write, Procedure: procedureField, Dialog: dialogField, Result: resultField, Operation: operation, Method: string(facts.CSeqMethod), Status: uint32(facts.Status), Replay: replay, Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	intent, changes, err := nativePreparePhysicalOutput(output, physical, physicalField, outputField, nil, nil)
	return intent, changes, shared, err
}
