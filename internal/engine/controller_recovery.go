package engine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

const controllerReattachTimeoutMS = int64(10000)

func controllerProofHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func controllerHolder(peer *control.Peer) state.ApplicationLeaseHolder {
	return state.ApplicationLeaseHolder{PrincipalHash: controllerProofHash(peer.Principal()), HolderID: peer.Process().GetInstanceId(), HolderEpoch: peer.Process().GetEpoch()}
}

// Recovery tokens are deterministic per successful claim so a lost helloAck can
// be recovered without keeping plaintext secrets in the authoritative store.
func (e *StateEngine) recoveryToken(application *pb.ProcessRef, term string, generation uint64, attempt string) string {
	mac := hmac.New(sha256.New, e.cfg.StateIdentityKey)
	for _, value := range []string{"sipbridge-controller-recovery-v1", e.applicationLeaseID(application), term, strconv.FormatUint(generation, 10), attempt} {
		_, _ = mac.Write([]byte(value))
		_, _ = mac.Write([]byte{0})
	}
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (e *StateEngine) controllerLease(application *pb.ProcessRef, lease state.ApplicationLease, token string) *pb.ApplicationLease {
	return &pb.ApplicationLease{Application: proto.Clone(application).(*pb.ProcessRef), Authority: e.StateAuthority(), Term: lease.Term,
		CheckedAtMs: uint64(lease.CheckedAtMS), ExpiresAtMs: uint64(lease.ExpiresAtMS),
		Holder: &pb.ProcessRef{InstanceId: lease.HolderID, Epoch: lease.HolderEpoch}, AttachmentGeneration: lease.Generation,
		RecoveryToken: token, ReattachUntilMs: uint64(lease.AttachmentUntilMS), LeaseLifetimeMs: 5000, ReattachTimeoutMs: uint32(lease.ReattachTimeoutMS)}
}

func (e *StateEngine) ControllerSession(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
	if !peer.HasCapability(control.ControllerReattachCapability) {
		return e.ApplicationLease(ctx, peer.Process(), "")
	}
	if peer.Principal() == "" || !validSendProcess(peer.Process()) {
		return nil, errors.New("authenticated controller identity required")
	}
	holder := controllerHolder(peer)
	if hello.GetRecovery() == nil {
		application, term := peer.Process(), uuid.NewString()
		token := e.recoveryToken(application, term, 1, "")
		lease, err := e.store.AcquireRecoverableApplicationLease(ctx, e.applicationLeaseID(application), term,
			state.ApplicationLeaseOwner{ApplicationLeaseHolder: holder, TokenHash: controllerProofHash(token), ReattachTimeoutMS: controllerReattachTimeoutMS})
		if err != nil {
			return nil, err
		}
		return e.controllerLease(application, lease, token), nil
	}
	request := hello.Recovery
	decoded, err := base64.RawURLEncoding.DecodeString(request.Token)
	attempt, attemptErr := uuid.Parse(request.AttemptId)
	if !validSendProcess(request.Application) || request.Term == "" || request.AttachmentGeneration == 0 || request.AttachmentGeneration >= 1<<53-1 ||
		err != nil || len(decoded) != 32 || attemptErr != nil || attempt == uuid.Nil || attempt.String() != request.AttemptId {
		return nil, errors.New("invalid controller recovery proof")
	}
	next := e.recoveryToken(request.Application, request.Term, request.AttachmentGeneration+1, request.AttemptId)
	lease, err := e.store.AttachApplicationLease(ctx, e.applicationLeaseID(request.Application), request.Term, request.AttachmentGeneration,
		state.ApplicationLeaseAttach{ApplicationLeaseHolder: holder, TokenHash: controllerProofHash(request.Token), NextTokenHash: controllerProofHash(next), AttemptID: request.AttemptId})
	if err != nil {
		return nil, err
	}
	if lease.Generation == request.AttachmentGeneration {
		next = request.Token
	}
	return e.controllerLease(request.Application, lease, next), nil
}

func (e *StateEngine) ControllerRenew(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
	if !peer.HasCapability(control.ControllerReattachCapability) {
		return e.ApplicationLease(ctx, peer.Process(), peer.ApplicationLease().GetTerm())
	}
	previous := peer.ApplicationLease()
	if previous == nil {
		return nil, control.ErrApplicationLeaseFenced
	}
	lease, err := e.store.RenewApplicationAttachment(ctx, state.ApplicationLeaseRef{ID: e.applicationLeaseID(previous.Application), Term: previous.Term, Generation: previous.AttachmentGeneration}, controllerHolder(peer))
	if err != nil {
		if errors.Is(err, state.ErrExpired) || errors.Is(err, state.ErrStale) || errors.Is(err, state.ErrConflict) {
			return nil, errors.Join(control.ErrApplicationLeaseFenced, err)
		}
		return nil, err
	}
	return e.controllerLease(previous.Application, lease, previous.RecoveryToken), nil
}

// Ready status ends restoration only after the caller's other status fields have
// been validated. Heartbeats alone cannot extend an unfinished recovery window.
func (e *StateEngine) ControllerReady(parent context.Context, peer *control.Peer) error {
	if !peer.HasCapability(control.ControllerReattachCapability) {
		return nil
	}
	lease := peer.ApplicationLease()
	if lease == nil {
		return control.ErrApplicationLeaseFenced
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	_, err := e.store.CompleteApplicationAttachment(ctx, state.ApplicationLeaseRef{ID: e.applicationLeaseID(lease.Application), Term: lease.Term, Generation: lease.AttachmentGeneration}, controllerHolder(peer))
	return err
}

func (e *StateEngine) validateControllerAttachment(ctx context.Context, peer *control.Peer) error {
	if !peer.HasCapability(control.ControllerReattachCapability) {
		return nil
	}
	current := peer.ApplicationLease()
	if current == nil {
		return control.ErrApplicationLeaseFenced
	}
	lease, err := e.store.ReadApplicationLease(ctx, e.applicationLeaseID(current.Application))
	if err != nil {
		return err
	}
	holder := controllerHolder(peer)
	if !lease.Recoverable || lease.Sealed || lease.Term != current.Term || lease.Generation != current.AttachmentGeneration ||
		lease.ExpiresAtMS <= lease.CheckedAtMS || lease.PrincipalHash != holder.PrincipalHash || lease.HolderID != holder.HolderID || lease.HolderEpoch != holder.HolderEpoch {
		return control.ErrApplicationLeaseFenced
	}
	return nil
}

func (e *StateEngine) controllerContexts(ctx context.Context, peer *control.Peer, request *pb.ControllerContexts) (*pb.ControllerContextsResult, error) {
	if !peer.HasCapability(control.ControllerReattachCapability) || request == nil {
		return nil, errors.New("controller recovery capability required")
	}
	if err := e.validateControllerAttachment(ctx, peer); err != nil {
		return nil, err
	}
	lease := peer.ApplicationLease()
	limit := int(request.Limit)
	if limit == 0 {
		limit = 32
	}
	page, err := e.store.ListApplicationContexts(ctx, state.ApplicationLeaseRef{ID: e.applicationLeaseID(lease.Application), Term: lease.Term, Generation: lease.AttachmentGeneration}, request.Cursor, limit)
	if err != nil {
		return nil, err
	}
	result := &pb.ControllerContextsResult{Cursor: page.NextCursor}
	for _, item := range page.Entries {
		result.Contexts = append(result.Contexts, e.contextRef(item.Ref, item.ContextID))
	}
	return result, nil
}
