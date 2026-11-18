package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"net"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

var sendFingerprintMethod = regexp.MustCompile(`^[A-Z][A-Z0-9_-]{0,31}$`)

// SendFingerprint binds one immutable send intent to its logical context,
// authority class, deadline, emission and proposed flow. Physical context-owner
// metadata is deliberately excluded so execution can move between replicas.
//
// A minting caller may leave Effect.Id and Fingerprint empty. Revalidation rejects
// a supplied value that differs from the computed identity. This is a content
// binding, not permission to send; authority and time admission are separate.
func SendFingerprint(snapshot *pb.ContextSnapshot, message *pb.SipMessage, auth *pb.EffectAuthorization, proposed *pb.FlowRef) (string, error) {
	if snapshot == nil || message == nil || auth == nil || !validSendContext(snapshot.Context) ||
		!validSendProcess(snapshot.Application) || auth.Effect == nil || !validSendContext(auth.Effect.Context) ||
		!sameSendContext(snapshot.Context, auth.Effect.Context) || !state.ValidReference(auth.Effect.AttemptId) ||
		!validSendText(snapshot.PrincipalId, 256, true) || !validSendText(snapshot.BindingId, 256, true) ||
		!validSendTransport(snapshot.Transport) || !sendFingerprintMethod.MatchString(message.Method) ||
		(message.Status != 0 && (message.Status < 100 || message.Status > 699)) ||
		len(message.Wire) == 0 || len(message.Wire) > 65536 || auth.DeadlineMs == 0 ||
		(auth.CompletionScope != "" && !state.ValidReference(auth.CompletionScope)) ||
		auth.ApplicationLeaseTerm != snapshot.ApplicationLeaseTerm ||
		(auth.ApplicationLeaseTerm != "" && !state.ValidReference(auth.ApplicationLeaseTerm)) ||
		(auth.Effect.Id != "" && len(auth.Effect.Id) != 66) || (auth.Fingerprint != "" && len(auth.Fingerprint) != 64) {
		return "", errors.New("invalid SIP send fingerprint input")
	}
	switch auth.AuthorityClass {
	case pb.EffectAuthorization_APPLICATION, pb.EffectAuthorization_NATIVE_COMPLETION, pb.EffectAuthorization_TERMINAL_CLEANUP, pb.EffectAuthorization_RESPONSE:
	default:
		return "", errors.New("invalid SIP send authority class")
	}
	retainedState := snapshot.Context.State
	authority := retainedState.Authority
	wireDigest := sha256.Sum256(message.Wire)
	parts := []string{
		"sipbridge-send-v1", authority.Service, authority.Generation, authority.PrimaryId,
		retainedState.GroupId, retainedState.Incarnation, retainedState.CallId, strconv.FormatUint(uint64(retainedState.Partition), 10),
		snapshot.Context.Id, snapshot.Application.InstanceId, snapshot.Application.Epoch, auth.ApplicationLeaseTerm,
		snapshot.PrincipalId, snapshot.BindingId, snapshot.Transport,
		strconv.FormatInt(int64(auth.AuthorityClass), 10), auth.CompletionScope,
		strconv.FormatUint(auth.DeadlineMs, 10), auth.Effect.AttemptId,
		message.Method, strconv.FormatUint(uint64(message.Status), 10), hex.EncodeToString(wireDigest[:]),
	}
	if proposed == nil {
		parts = append(parts, "", "", "", "", "", "", "")
	} else {
		if !validSendProcess(proposed.Ingress) || !state.ValidReference(proposed.ConnectionId) ||
			net.ParseIP(proposed.SourceIp) == nil || proposed.SourcePort < 1 || proposed.SourcePort > 65535 ||
			!validSendText(proposed.Listener, 1024, false) || !validSendTransport(proposed.Transport) {
			return "", errors.New("invalid SIP send fingerprint flow")
		}
		parts = append(parts, proposed.Ingress.InstanceId, proposed.Ingress.Epoch, proposed.ConnectionId,
			proposed.SourceIp, strconv.FormatUint(uint64(proposed.SourcePort), 10), proposed.Listener, proposed.Transport)
	}
	h := sha256.New()
	for _, part := range parts {
		writeSendFingerprintPart(h, part)
	}
	fingerprint := hex.EncodeToString(h.Sum(nil))
	if (auth.Fingerprint != "" && auth.Fingerprint != fingerprint) || (auth.Effect.Id != "" && auth.Effect.Id != "e-"+fingerprint) {
		return "", errors.New("SIP send fingerprint identity mismatch")
	}
	return fingerprint, nil
}

func writeSendFingerprintPart(h hash.Hash, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	var prefix [9]byte
	hex.Encode(prefix[:8], length[:])
	prefix[8] = ':'
	_, _ = h.Write(prefix[:])
	_, _ = h.Write([]byte(value))
}

func validSendContext(ref *pb.ContextRef) bool {
	if ref == nil || !state.ValidReference(ref.Id) || ref.State == nil {
		return false
	}
	s := ref.State
	a := s.Authority
	return a != nil && state.ValidReference(a.Service) && state.ValidReference(a.Generation) &&
		state.ValidReference(a.PrimaryId) && a.SchemaVersion > 0 &&
		state.ValidReference(s.GroupId) && state.ValidReference(s.Incarnation) &&
		validSendText(s.CallId, 1024, false) && s.Partition <= 4095
}

func sameSendContext(a, b *pb.ContextRef) bool {
	x, y := a.State, b.State
	p, q := x.Authority, y.Authority
	return a.Id == b.Id && x.GroupId == y.GroupId && x.Incarnation == y.Incarnation && x.CallId == y.CallId && x.Partition == y.Partition &&
		p.Service == q.Service && p.Generation == q.Generation && p.PrimaryId == q.PrimaryId
}

func validSendProcess(ref *pb.ProcessRef) bool {
	return ref != nil && state.ValidReference(ref.InstanceId) && state.ValidReference(ref.Epoch)
}

func validSendTransport(value string) bool {
	return value == "udp" || value == "tcp" || value == "tls"
}

func validSendText(value string, maximum int, empty bool) bool {
	return (empty || value != "") && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsAny(value, "\r\n\x00")
}
