package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// redisUACQualification is a private success-path fixture input. A caller
// must supply a currently live authenticated lease, resolved flow, original
// budget and an explicitly selected qualification headroom.
type redisUACQualification struct {
	Key                   []byte
	Context               *pb.ContextRef
	Authority             *pb.StateAuthority
	Application, Executor *pb.ProcessRef
	Lease                 rediscore.ApplicationLeaseRef
	Flow                  *pb.FlowRef
	Command               *pb.Command
	Operation             string
	Advertisement         nativeSIPAdvertisement
	ExecutorTimer         rediscore.TimerAssignment
	Credit                uint64
	Deadline              time.Time
}

// admitRedisUACQualification atomically stages a single initial INVITE success
// candidate. It is not wired to serving or to any physical-write loop.
func admitRedisUACQualification(ctx context.Context, store *rediscore.Store, in redisUACQualification) (*pb.OperationResult, error) {
	return admitRedisUACStart(ctx, store, in, false)
}

// The two OpenUAC forms share authority, wire construction and atomic admission.
func admitRedisUACStart(ctx context.Context, store *rediscore.Store, in redisUACQualification, standalone bool) (*pb.OperationResult, error) {
	now := time.Now()
	cmd := in.Command
	if store == nil || ctx.Err() != nil || !now.Before(in.Deadline) || in.Deadline.UnixMilli() <= 0 || in.Deadline.UnixMilli() > 1<<53-1 || len(in.Key) < 32 || len(in.Key) > 1024 || in.Context == nil || localPhoneID(in.Context.GetId()) || in.Context.State == nil || !validSendContext(in.Context) || in.Authority == nil || !proto.Equal(in.Context.State.Authority, in.Authority) || in.Context.State.GroupId != in.Context.Id || in.Context.State.Partition != 0 || !validSendProcess(in.Application) || !validSendProcess(in.Executor) || !sameProcess(in.Context.Owner, in.Executor) || !validPolicyFlow(in.Flow) || cmd == nil || cmd.Context == nil || cmd.Context.Id != in.Context.Id || cmd.Context.State != nil || cmd.Sequence != 1 || cmd.GetOpenUac() == nil || !validSendText(in.Operation, 160, false) || in.Credit == 0 || in.Credit > 1<<53-1 {
		return nil, errNativeRecord
	}
	open := cmd.GetOpenUac()
	worker, err := nativeWorkerReference(in.Executor)
	if err != nil || in.ExecutorTimer.Worker != worker || in.ExecutorTimer.Epoch != in.Executor.Epoch || in.ExecutorTimer.Term != in.Executor.Epoch {
		return nil, errNativeRecord
	}
	if standalone && !redisStandaloneMethod(open.Method) || !standalone && open.Method != "" && open.Method != "INVITE" {
		return nil, errors.New("initial INVITE required")
	}
	kind, method, prefix, timerPrefix := pb.ContextSnapshot_UAC, "INVITE", "uac-", "call-tx-"
	if standalone {
		kind, method, prefix, timerPrefix = pb.ContextSnapshot_TRANSACTION, open.Method, "transaction-", "transaction-"
	}
	app, err := nativeApplicationReference(in.Key, in.Application)
	if err != nil || app != in.Lease.Application || in.Lease.Term == "" {
		return nil, errors.New("application lease identity mismatch")
	}
	snapshot := &pb.ContextSnapshot{Context: proto.Clone(in.Context).(*pb.ContextRef), Application: proto.Clone(in.Application).(*pb.ProcessRef), ApplicationLeaseTerm: in.Lease.Term, Executor: proto.Clone(in.Executor).(*pb.ProcessRef), Kind: kind, CallId: in.Context.State.CallId, Transport: uacTransport(open)}
	if err = validateNativeCommandPolicy(snapshot, cmd, in.Operation, now, in.Deadline); err != nil {
		return nil, err
	}
	if cmd.Admission.SelectedFlow != nil && !proto.Equal(cmd.Admission.SelectedFlow, in.Flow) {
		return nil, errors.New("selected UAC flow changed")
	}
	contact, sentBy, err := in.Advertisement.resolve(in.Flow.Transport)
	if err != nil {
		return nil, err
	}
	tag := nativeIdentity(in.Key, "tag", in.Context.State.Incarnation, in.Context.Id)[:32]
	branch := sip.RFC3261BranchMagicCookie + nativeIdentity(in.Key, "branch", in.Context.State.Incarnation, in.Operation)[:32]
	tx, effects, err := nativePrepareClientStart(open, in.Context, in.Flow, tag, branch, contact, sentBy, now)
	if err != nil {
		return nil, err
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || string(facts.Method) != method || tx.Origin.WireLen() > 65536 {
		return nil, errNativeRecord
	}
	selectors, err := nativeSelectorIDs(tx.Origin, true, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorClientTransaction {
		return nil, errNativeRecord
	}
	var publicRequest *pb.SipMessage
	var publicSelectors []string
	if standalone {
		publicSelectors, err = nativeMessageSelectors(tx.Origin, true)
	} else {
		publicRequest, err = projectNativeMessage(tx.Origin, true)
		if err == nil {
			publicSelectors = publicRequest.Selectors
		}
	}
	if err != nil {
		return nil, err
	}
	id := nativeIdentity(in.Key, "uac-initial", in.Context.State.Authority.PrimaryId, in.Context.State.Incarnation, in.Context.Id, in.Operation)[:32]
	procedureField, originField, resultField, outputField := "r:"+prefix+"procedure-"+id, "r:"+prefix+"origin-"+id, "r:result-1", "r:"+prefix+"output-"+id
	if !nativeMessageField(procedureField) || !nativeMessageField(originField) || !nativeMessageField(outputField) {
		return nil, errNativeRecord
	}
	until := minTime(in.Deadline, time.UnixMilli(int64(cmd.Admission.ExpiresAtMs)))
	if !now.Before(until) {
		return nil, errNativeRecord
	}
	physicalUntil := minTime(until, now.Add(2*time.Second))
	snapshot.BindingId, snapshot.PrincipalId = open.BindingId, open.PrincipalId
	snapshot.DirectoryEpoch = cmd.Admission.DirectoryEpoch
	snapshot.LocalTag = facts.FromTag
	snapshot.LocalUri, snapshot.RemoteUri = facts.FromURI, facts.ToURI
	snapshot.LocalSdp = append([]byte(nil), open.Body...)
	snapshot.Selectors = append([]string(nil), publicSelectors...)
	snapshot.CommandSequence, snapshot.Revision = 1, 1
	record := &nativepb.Context{Context: proto.Clone(in.Context).(*pb.ContextRef), Application: proto.Clone(in.Application).(*pb.ProcessRef), ApplicationLeaseTerm: in.Lease.Term, Executor: proto.Clone(in.Executor).(*pb.ProcessRef), Flow: proto.Clone(in.Flow).(*pb.FlowRef), Kind: kind, CallId: facts.CallID, LocalTag: facts.FromTag, LocalUri: facts.FromURI, RemoteUri: facts.ToURI, LocalSdp: append([]byte(nil), open.Body...), BindingId: open.BindingId, PrincipalId: open.PrincipalId, DirectoryEpoch: cmd.Admission.DirectoryEpoch, Transport: in.Flow.Transport, CommandSequence: 1, Revision: 1, Selectors: append([]string(nil), publicSelectors...), InitialProcedures: []string{procedureField}}
	if standalone {
		snapshot.LocalSdp = nil
		record.LocalSdp = nil
	}
	contextData, err := encodeNativeContext(record)
	if err != nil {
		return nil, err
	}
	metadata := &nativepb.Procedure{Flow: proto.Clone(in.Flow).(*pb.FlowRef), Application: proto.Clone(in.Application).(*pb.ProcessRef), Policy: proto.Clone(cmd.Admission).(*pb.CommandAdmission), DeadlineMs: until.UnixMilli(), CommandResult: resultField, CreditContribution: in.Credit, Outputs: []string{outputField}, Emissions: 1}
	if !standalone && open.Network != nil && open.Network.Credential != nil {
		metadata.Credential = proto.Clone(open.Network.Credential).(*pb.CredentialRef)
	}
	procedureData, err := encodeNativeProcedure(tx, nativeProcedureRefs{Origin: originField}, metadata)
	if err != nil {
		return nil, err
	}
	encodedCommand, err := proto.MarshalOptions{Deterministic: true}.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(encodedCommand)
	result := &nativepb.Result{Certainty: pb.OperationResult_ACCEPTED, Request: originField, Output: outputField, CutoffMs: in.Deadline.UnixMilli(), Sequence: 1, OperationId: in.Operation, Fingerprint: fingerprint[:], DecidingLeaseTerm: in.Lease.Term}
	if standalone {
		result.Certainty, result.Pending, result.Request = pb.OperationResult_UNKNOWN, true, ""
	}
	resultData, err := encodeNativeResult(result)
	if err != nil {
		return nil, err
	}
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	authority := in.Context.State.Authority
	scope := nativeIdentity(in.Key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, in.Context.State.GroupId, in.Context.State.Incarnation, in.Context.State.CallId, in.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_APPLICATION, DeadlineMs: uint64(physicalUntil.UnixMilli()), ApplicationLeaseTerm: in.Lease.Term, CompletionScope: scope}
	hash, err := SendFingerprint(snapshot, &pb.SipMessage{Method: method, Wire: tx.Origin.WireBytes()}, auth, in.Flow)
	if err != nil {
		return nil, err
	}
	ref.Id, auth.Fingerprint = "e-"+hash, hash
	write := &pb.FlowWrite{Effect: ref, Flow: in.Flow, Permit: &pb.SendPermit{Authorization: auth, Flow: in.Flow, DirectoryEpoch: cmd.Admission.DirectoryEpoch, OrderingProof: nativeIdentity(in.Key, "native-write-authority-v1", hash), CompletionScope: scope}}
	drain := drainNativeMessageObservation(tx.Origin, facts)
	output := &nativepb.Output{Write: write, Procedure: procedureField, Result: resultField, Operation: in.Operation, Method: method, Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	intent, outputChanges, err := nativePreparePhysicalOutput(output, tx.Origin, originField, outputField, &in.Lease, nil)
	if err != nil {
		return nil, err
	}
	delivery, err := nativeDeliveryTagChange(nil)
	if err != nil {
		return nil, err
	}
	changes := []rediscore.Change{{Field: "r:context", Value: contextData}, {Field: procedureField, Value: procedureData}, {Field: resultField, Value: resultData}, delivery}
	changes = append(changes, outputChanges...)
	timers := []rediscore.TimerChange{{Member: "out-" + outputField[2:], Due: physicalUntil}}
	for _, effect := range effects {
		switch effect.Kind {
		case portable.Schedule:
			if effect.Timer != portable.TimerA && effect.Timer != portable.TimerE {
				timers = append(timers, rediscore.TimerChange{Member: timerPrefix + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.Send:
		default:
			return nil, errNativeRecord
		}
	}
	class := rediscore.AdmissionOutbound
	if standalone {
		class = rediscore.AdmissionStandalone
	}
	_, err = store.Commit(ctx, rediscore.Publication{Admission: redisInstanceAdmission(in.Key, in.Executor, class, ""), Identity: rediscore.Identity{Context: in.Context.Id, Incarnation: in.Context.State.Incarnation, Owner: in.Lease.Application, Term: in.Lease.Term}, Create: true, Deadline: until, CreditDelta: int64(in.Credit), Application: &in.Lease, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: procedureField}}, Timers: &rediscore.TimerMutation{Target: in.ExecutorTimer, Changes: timers}})
	if err != nil {
		return nil, fmt.Errorf("initial UAC publication: %w", err)
	}
	if ctx.Err() != nil || !time.Now().Before(in.Deadline) {
		return nil, rediscore.ErrUnknown
	}
	if standalone {
		return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Snapshot: snapshot}, nil
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: snapshot, Request: publicRequest}, nil
}
