package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/state"
)

// commandOpenUAC is the private application-command boundary. The serving
// selector remains separate until the complete native surface is qualified.
func (e *redisNative) commandOpenUAC(parent context.Context, peer *control.Peer, cmd *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	refuse := func(message string) (*pb.OperationResult, error) {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: message}, nil
	}
	if e == nil || cmd == nil || cmd.Context == nil || cmd.Context.State != nil || !state.ValidReference(cmd.Context.Id) || (strings.HasPrefix(cmd.Context.Id, "in-") || localPhoneID(cmd.Context.Id)) || cmd.Sequence != 1 || cmd.GetOpenUac() == nil || !validSendText(operation, 160, false) || !time.Now().Before(deadline) {
		return refuse("invalid initial INVITE command")
	}
	open := proto.Clone(cmd.GetOpenUac()).(*pb.OpenUac)
	if open.Method != "" && open.Method != "INVITE" && !redisStandaloneMethod(open.Method) {
		return refuse("unsupported initial method")
	}
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return refuse("controller attachment unavailable")
	}
	application := peer.Application()
	incarnation := uuid.NewString()
	authority := e.StateAuthority()
	callID := nativeIdentity(e.cfg.StateIdentityKey, "outbound-call", application.InstanceId, application.Epoch, cmd.Context.Id, incarnation) + "@sipbridge"
	ref := &pb.ContextRef{Owner: proto.Clone(e.cfg.Process).(*pb.ProcessRef), Id: cmd.Context.Id, State: &pb.StateRef{Authority: authority, GroupId: cmd.Context.Id, Incarnation: incarnation, CallId: callID}}
	snapshot := &pb.ContextSnapshot{Context: ref, Application: application, ApplicationLeaseTerm: witness.Term, Executor: e.cfg.Process, Kind: pb.ContextSnapshot_UAC, CallId: callID, Transport: uacTransport(open)}
	if err = validateNativeCommandPolicy(snapshot, cmd, operation, time.Now(), deadline); err != nil {
		return refuse(err.Error())
	}
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return nil, fmt.Errorf("initial UAC worker binding: %w", err)
	}
	defer cancel()
	stopPeer := context.AfterFunc(peer.Context(), cancel)
	defer stopPeer()
	var flow *pb.FlowRef
	if cmd.Admission.SelectedFlow != nil {
		flow = proto.Clone(cmd.Admission.SelectedFlow).(*pb.FlowRef)
	} else {
		// Network opening is an external operation: check the exact live term
		// before starting it, and fence it again in the final publication.
		current, observeErr := store.ObserveApplication(ctx, witness.Application)
		if observeErr != nil {
			return nil, observeErr
		}
		if current.Term != witness.Term || current.Until.IsZero() || !time.Now().Before(current.Until) {
			return refuse("controller attachment unavailable")
		}
		if e.cfg.OpenNetwork == nil {
			return refuse("network target unavailable")
		}
		network := open.Network
		if network == nil {
			network = &pb.NetworkTarget{Transport: open.Transport}
		}
		until := minTime(deadline, time.UnixMilli(int64(cmd.Admission.ExpiresAtMs)))
		until = minTime(until, current.Until)
		if ctx.Err() != nil || !time.Now().Before(until) {
			return refuse("outbound admission expired")
		}
		openCtx, stop := context.WithDeadline(ctx, until)
		flow, err = e.cfg.OpenNetwork(openCtx, open.Uri, network)
		stop()
		if err != nil {
			return refuse(err.Error())
		}
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, rediscore.ErrExpired
	}
	if !validPolicyFlow(flow) || flow.Transport != uacTransport(open) {
		return refuse("network flow unavailable")
	}
	input := redisUACQualification{Key: e.cfg.StateIdentityKey, Context: ref, Authority: authority, Application: application, Executor: e.cfg.Process, Lease: witness, Flow: flow, Command: cmd, Operation: operation, Advertisement: nativeSIPAdvertisement{contactURI: e.cfg.ContactURI, address: e.cfg.AdvertiseAddress, plainAddress: e.cfg.PlainAdvertiseAddress}, ExecutorTimer: rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, Credit: uint64(redisInviteCompletionCredit(flow.Transport)), Deadline: deadline}
	var result *pb.OperationResult
	if redisStandaloneMethod(open.Method) {
		input.Credit = uint64(residentSmallBudget)
		result, err = e.startStandaloneTransaction(ctx, store, input)
	} else {
		result, err = admitRedisUACQualification(ctx, store, input)
	}
	if errors.Is(err, rediscore.ErrCapacity) || errors.Is(err, errStatePolicyAdmission) {
		return refuse(err.Error())
	}
	if errors.Is(err, rediscore.ErrConflict) {
		return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "outbound context unavailable"}, nil
	}
	return result, err
}
