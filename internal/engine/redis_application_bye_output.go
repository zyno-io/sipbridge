package engine

import (
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func validReadyApplicationBye(metadata *nativepb.Output, field string) bool {
	if metadata == nil || metadata.Write == nil || metadata.Write.Permit == nil || metadata.Write.Permit.Authorization == nil || !redisApplicationClientMethod(metadata.Method) {
		return false
	}
	prefix := redisApplicationClientPrefix(metadata.Method)
	if !strings.HasPrefix(field, "r:"+prefix+"output-") {
		return false
	}
	id := strings.TrimPrefix(field, "r:"+prefix+"output-")
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 || id != hex.EncodeToString(decoded) || metadata.Procedure != "r:"+prefix+id || metadata.Dialog == "" || metadata.Operation == "" || metadata.Status != 0 || metadata.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_APPLICATION || !strings.HasPrefix(metadata.Result, "r:result-") {
		return false
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(metadata.Result, "r:result-"), 10, 64)
	return err == nil && sequence > 0 && sequence <= 1<<53-1 && metadata.Result == "r:result-"+strconv.FormatUint(sequence, 10)
}

// prepareRedisApplicationByeOutput constructs one application-authorized BYE
// attempt. Admission, native publication and later proof settlement remain
// separate atomic boundaries.
func prepareRedisApplicationByeOutput(snapshot *pb.ContextSnapshot, flow *pb.FlowRef, message portable.Message, key []byte, policy *pb.CommandAdmission, operation, procedureField, dialogField, resultField, wireField, outputField string, lease rediscore.ApplicationLeaseRef, deadline time.Time) (rediscore.OutputIntent, []rediscore.Change, error) {
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || !redisApplicationClientMethod(string(facts.Method)) {
		return rediscore.OutputIntent{}, nil, errors.New("application dialog request required")
	}
	return prepareRedisApplicationDialogOutput(snapshot, flow, message, key, policy, operation, procedureField, dialogField, resultField, wireField, outputField, lease, deadline, string(facts.Method), 0)
}

// prepareRedisApplicationDialogOutput shares selected-wire construction; an
// admitted ACK uses native-completion authority, while new requests use application authority.
func prepareRedisApplicationDialogOutput(snapshot *pb.ContextSnapshot, flow *pb.FlowRef, message portable.Message, key []byte, policy *pb.CommandAdmission, operation, procedureField, dialogField, resultField, wireField, outputField string, lease rediscore.ApplicationLeaseRef, deadline time.Time, method string, ackSequence uint32) (rediscore.OutputIntent, []rediscore.Change, error) {
	var empty rediscore.OutputIntent
	if len(key) < 32 || len(key) > 1024 || snapshot == nil || snapshot.Context == nil || snapshot.Context.State == nil || snapshot.Context.State.Authority == nil || !validPolicyFlow(flow) || policy == nil || procedureField == "" || dialogField == "" || resultField == "" {
		return empty, nil, errNativeRecord
	}
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || string(facts.Method) != method || method == "ACK" && (ackSequence == 0 || facts.CSeq != ackSequence) || method != "ACK" && (!redisApplicationClientMethod(method) && method != "INVITE" || ackSequence != 0) {
		return empty, nil, errors.New("application dialog request required")
	}
	now := time.Now()
	until := minTime(minTime(deadline, now.Add(2*time.Second)), time.UnixMilli(int64(policy.ExpiresAtMs)))
	if !now.Before(until) {
		return empty, nil, errors.New("application dialog output deadline elapsed")
	}
	authority := snapshot.Context.State.Authority
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	completion := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, snapshot.Context.State.GroupId, snapshot.Context.State.Incarnation, snapshot.CallId, snapshot.Context.Id)
	class := pb.EffectAuthorization_APPLICATION
	if method == "ACK" {
		class = pb.EffectAuthorization_NATIVE_COMPLETION
	}
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: class, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: snapshot.ApplicationLeaseTerm, CompletionScope: completion}
	observed := &pb.SipMessage{Method: string(facts.CSeqMethod), Wire: message.WireBytes()}
	fingerprint, err := SendFingerprint(snapshot, observed, auth, flow)
	if err != nil {
		return empty, nil, err
	}
	ref.Id, auth.Fingerprint = "e-"+fingerprint, fingerprint
	write := &pb.FlowWrite{Effect: ref, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: policy.DirectoryEpoch, OrderingProof: nativeIdentity(key, "native-write-authority-v1", fingerprint), CompletionScope: completion}}
	drain := drainNativeMessageObservation(message, facts)
	metadata := &nativepb.Output{Write: write, Procedure: procedureField, Dialog: dialogField, Result: resultField, Operation: operation, Method: string(facts.Method), AckSequence: ackSequence, Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	if method == "ACK" {
		return nativePreparePhysicalOutput(metadata, message, wireField, outputField, nil, nil)
	}
	return nativePreparePhysicalOutput(metadata, message, wireField, outputField, &lease, snapshot.Delivery)
}
