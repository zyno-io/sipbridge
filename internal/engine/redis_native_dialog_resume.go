package engine

import (
	"context"
	"crypto/hmac"
	"errors"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
)

func (e *redisNative) dialogResumeID(store *rediscore.Store, id rediscore.Identity, result *nativepb.Result, method string) string {
	return method + "." + nativeIdentity(e.cfg.StateIdentityKey, "dialog-resume-v1", store.PrimaryRunID(), id.Context, id.Incarnation, id.Owner, id.Term,
		strconv.FormatUint(result.Sequence, 10), strconv.FormatInt(result.CutoffMs, 10), result.OperationId, string(result.Fingerprint), method)
}

func hasOnlyInitialDialogOutput(procedure *nativepb.Procedure, field string) bool {
	return procedure.GetEmissions() == 1 && !procedure.GetOutputConsumed() && len(procedure.GetOutputs()) == 1 && procedure.Outputs[0] == field
}

// A ready attempt has not yet reserved its socket write. If the exact reliable
// socket is gone and the original caller negotiated resume, take away that
// attempt's ability to reserve before returning any resumable evidence.
func (e *redisNative) parkClosedDialogOutput(ctx context.Context, store *rediscore.Store, metadata *nativepb.Output) (bool, error) {
	if metadata.Status != 0 || metadata.Result == "" || metadata.Method != "INVITE" && !redisApplicationClientMethod(metadata.Method) ||
		metadata.Write.GetFlow().GetTransport() == "udp" || e.cfg.LiveFlow == nil || e.cfg.LiveFlow(metadata.Write.GetFlow()) {
		return false, nil
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(metadata.Result, "r:result-"), 10, 64)
	if err != nil || sequence == 0 {
		return false, nil
	}
	ref, err := nativeOutputRef(metadata.Write)
	if err != nil || !ref.RequireApplication {
		return false, err
	}
	hint := rediscore.ResultHint{Context: ref.Context, Incarnation: ref.Incarnation, Sequence: sequence}
	e.mu.Lock()
	waiter := e.resultWaiters[hint]
	requested := waiter != nil && waiter.allowDialogResume
	var fingerprint [32]byte
	if requested {
		fingerprint = waiter.dialogResumeFingerprint
	}
	e.mu.Unlock()
	if !requested {
		return false, nil
	}
	selected, err := inspectRedisApplicationByeOutput(ctx, store, metadata.Write, nil)
	if err != nil {
		return false, err
	}
	result, err := decodeNativeResult(selected.selected.View.Records[metadata.Result])
	if err != nil {
		return false, err
	}
	current, err := decodeNativeContext(selected.selected.View.Records["r:context"])
	if err != nil {
		return false, err
	}
	if current.Ended || current.CommandSequence != sequence || !result.Pending || !hmac.Equal(fingerprint[:], result.Fingerprint) || selected.capture.proof.State != rediscore.OutputPending ||
		!hasOnlyInitialDialogOutput(selected.procedure, result.Output) {
		return false, nil
	}
	if err = store.ParkPendingApplicationOutput(ctx, selected.selected.Identity, selected.selected.View.Revision, ref); err != nil {
		if errors.Is(err, rediscore.ErrConflict) || errors.Is(err, rediscore.ErrExpired) {
			return false, nil
		}
		return false, err
	}
	e.mu.Lock()
	if e.resultWaiters[hint] == waiter && waiter.dialogResumeFingerprint == fingerprint {
		waiter.allowDialogResume = false
		waiter.dialogResumeID = e.dialogResumeID(store, selected.selected.Identity, result, metadata.Method)
		waiter.dialogResumeRef = ref
		close(waiter.changed)
		waiter.changed = make(chan struct{})
	}
	e.mu.Unlock()
	return true, nil
}

func (e *redisNative) parkedDialogResult(ctx context.Context, store *rediscore.Store, id rediscore.Identity, result *nativepb.Result, method string, waiter *nativeResultWaiter, snapshot *pb.ContextSnapshot) (*pb.OperationResult, error) {
	e.mu.Lock()
	opID, ref := waiter.dialogResumeID, waiter.dialogResumeRef
	e.mu.Unlock()
	if opID == "" || !hmac.Equal([]byte(opID), []byte(e.dialogResumeID(store, id, result, method))) {
		return nil, nil
	}
	proof, err := store.InspectOutput(ctx, ref)
	if errors.Is(err, rediscore.ErrAbsent) || errors.Is(err, rediscore.ErrConflict) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if proof.State != rediscore.OutputResumeParked || proof.MetadataField != result.Output || time.Now().UnixMilli() >= proof.DeadlineMS {
		return nil, nil
	}
	return &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Snapshot: snapshot,
		DialogRecovery: &pb.DialogRecovery{Disposition: pb.DialogRecovery_RESUMABLE, OperationId: opID}}, nil
}

func (e *redisNative) commandResumeDialog(parent context.Context, peer *control.Peer, command *pb.Command, deadline time.Time) (*pb.OperationResult, error) {
	ref, policy := command.GetContext(), command.GetAdmission()
	flow := policy.GetSelectedFlow()
	opID := command.GetResumeDialogRequest().GetOperationId()
	method, _, hasMethod := strings.Cut(opID, ".")
	if !peer.HasCapability(control.DialogSocketResumeCapability) || !hasMethod || method != "INVITE" && !redisApplicationClientMethod(method) ||
		ref == nil || ref.State == nil || !state.ValidReference(ref.Id) || !state.ValidReference(ref.State.Incarnation) || !proto.Equal(ref.State.Authority, e.StateAuthority()) ||
		command.Sequence == 0 || !validPolicyFlow(flow) || flow.Transport == "udp" || !sameProcess(flow.Ingress, e.cfg.Process) || e.cfg.LiveFlow == nil || !e.cfg.LiveFlow(flow) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "invalid dialog resume socket or scope"}, nil
	}
	lease, err := e.controllerWitness(peer)
	if err != nil {
		return nil, err
	}
	store, workerLease, until, err := e.driver.Current()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	stop := context.AfterFunc(workerLease, cancel)
	defer stop()
	sequence := command.Sequence
	resultField := "r:result-" + strconv.FormatUint(sequence, 10)
	hint := rediscore.ResultHint{Context: ref.Id, Incarnation: ref.State.Incarnation, Sequence: sequence}
	waiter, err := e.registerResultWaiter(hint)
	if err != nil {
		return nil, err
	}
	defer e.releaseResultWaiter(hint, waiter)
	for pass := 0; pass < 2; pass++ {
		selected, err := store.ObserveApplicationContext(ctx, ref.Id, ref.State.Incarnation, lease, "r:context", resultField)
		if err != nil {
			return nil, err
		}
		current, err := decodeNativeContext(selected.View.Records["r:context"])
		if err != nil {
			return nil, err
		}
		result, err := decodeNativeResult(selected.View.Records[resultField])
		if err != nil {
			return nil, err
		}
		if selected.Identity.Owner != lease.Application || selected.Identity.Term != lease.Term || current.Context == nil || !proto.Equal(current.Context.State, ref.State) ||
			!sameProcess(current.Application, peer.Application()) || current.ApplicationLeaseTerm != lease.Term ||
			current.CommandSequence != sequence || result.Sequence != sequence || result.DecidingLeaseTerm != lease.Term ||
			!hmac.Equal([]byte(opID), []byte(e.dialogResumeID(store, selected.Identity, result, method))) {
			return nil, rediscore.ErrAuthority
		}
		cutoff := minTime(deadline, nativeUDPRetryDeadline(until, result.CutoffMs))
		if !time.Now().Before(cutoff) {
			return nil, rediscore.ErrExpired
		}
		capture := redisNotifyWaitCapture{state: current.Context.State, sequence: result.Sequence, fingerprint: [32]byte(result.Fingerprint)}
		if !result.Pending {
			return e.awaitRedisApplicationByeResult(ctx, store, selected.Identity, lease, capture, result.OperationId, cutoff, until, waiter, method)
		}
		if current.Ended {
			return nil, rediscore.ErrConflict
		}
		outputView, err := store.Read(ctx, selected.Identity, result.Output)
		if err != nil {
			return nil, err
		}
		if outputView.Revision != selected.View.Revision {
			continue
		}
		metadata, err := decodeNativeOutput(outputView.Records[result.Output])
		if err != nil || metadata.Method != method {
			return nil, errNativeRecord
		}
		exact, err := inspectRedisApplicationByeOutput(ctx, store, metadata.Write, nil)
		if errors.Is(err, rediscore.ErrAbsent) || errors.Is(err, rediscore.ErrConflict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if exact.selected.View.Revision != selected.View.Revision {
			continue
		}
		if exact.capture.proof.State != rediscore.OutputResumeParked {
			if !proto.Equal(metadata.Write.Flow, flow) {
				return nil, rediscore.ErrConflict
			}
			return e.awaitRedisApplicationByeResult(ctx, store, selected.Identity, lease, capture, result.OperationId, cutoff, until, waiter, method)
		}
		if err = e.assignResumedDialog(ctx, store, selected, current, exact, command, lease, cutoff); err != nil {
			if !errors.Is(err, rediscore.ErrUnknown) && (errors.Is(err, rediscore.ErrAbsent) || errors.Is(err, rediscore.ErrConflict)) {
				continue
			}
			return nil, err
		}
		return e.awaitRedisApplicationByeResult(ctx, store, selected.Identity, lease, capture, result.OperationId, cutoff, until, waiter, method)
	}
	return nil, rediscore.ErrConflict
}

func (e *redisNative) assignResumedDialog(ctx context.Context, store *rediscore.Store, selected *rediscore.ObservedContext, current *nativepb.Context, old selectedCleanupByeOutput, command *pb.Command, lease rediscore.ApplicationLeaseRef, cutoff time.Time) error {
	metadata, proof := old.capture.metadata, old.capture.proof
	policy := command.Admission
	flow := policy.SelectedFlow
	if !hasOnlyInitialDialogOutput(old.procedure, proof.MetadataField) || metadata.Write.Flow.Transport != flow.Transport || proto.Equal(metadata.Write.Flow, flow) || policy.GetExpiresAtMs() < uint64(proof.DeadlineMS) ||
		cutoff.UnixMilli() < proof.DeadlineMS {
		return policyRefusal("resume would change the selected request transport or deadline")
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return err
	}
	if err = validateNativeCommandPolicy(snapshot, command, metadata.Operation, time.Now(), cutoff); err != nil {
		return err
	}
	acceptedFlow := current.Flow
	if current.Delivery != nil {
		acceptedFlow = current.Delivery.Flow
	}
	if !policy.RegistrationFallback && !proto.Equal(acceptedFlow, flow) {
		return policyRefusal("resume socket is neither accepted nor a registration fallback")
	}
	view, err := store.Read(ctx, selected.Identity, proof.MessageField)
	if err != nil {
		return err
	}
	if view.Revision != selected.View.Revision {
		return rediscore.ErrConflict
	}
	message, err := selectedmessage.DecodeOpaque(view.Records[proof.MessageField])
	if err != nil {
		return err
	}
	facts, err := message.Facts()
	if err != nil || message.WireHash() != proof.WireHash {
		return errNativeRecord
	}
	_, sentBy, err := (nativeSIPAdvertisement{contactURI: e.cfg.ContactURI, address: e.cfg.AdvertiseAddress, plainAddress: e.cfg.PlainAdvertiseAddress}).resolve(flow.Transport)
	if err != nil || facts.ViaSentBy != sentBy {
		return policyRefusal("resume would change the original Via sent-by")
	}
	procedure := proto.Clone(old.procedure).(*nativepb.Procedure)
	procedure.Flow = proto.Clone(flow).(*pb.FlowRef)
	procedure.Policy = proto.Clone(policy).(*pb.CommandAdmission)
	procedure.Policy.ExpiresAtMs = uint64(proof.DeadlineMS)
	procedureBytes, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		return err
	}
	intent, changes, err := prepareRedisApplicationDialogOutput(snapshot, flow, message, e.cfg.StateIdentityKey, procedure.Policy, metadata.Operation,
		metadata.Procedure, metadata.Dialog, metadata.Result, proof.MessageField, proof.MetadataField, lease, time.UnixMilli(proof.DeadlineMS), metadata.Method, 0)
	if err != nil {
		return err
	}
	for _, change := range changes {
		if change.Field == proof.MetadataField {
			_, err := store.ResumeParkedApplicationOutput(ctx, store.PrimaryRunID(), selected.Identity, selected.View.Revision, old.capture.ref, intent,
				rediscore.Change{Field: metadata.Procedure, Value: procedureBytes}, change.Value)
			return err
		}
	}
	return errNativeRecord
}
