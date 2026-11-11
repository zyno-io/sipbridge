package state

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// metadata holds immutable configuration and pure validation only.
type metadata struct {
	cfg       Config
	authority Authority
	versions  string
	profile   string
	root      string
}

func newMetadata(cfg Config) metadata {
	s := metadata{cfg: cfg, root: cfg.Prefix + ":" + cfg.Namespace + ":"}
	encoded, _ := json.Marshal(cfg.Versions)
	s.versions = string(encoded)
	mac := hmac.New(sha256.New, cfg.NamespaceKey)
	fmt.Fprintf(mac, "sip-state-profile-v1:%s:%d:%s", s.root, cfg.Partitions, s.versions)
	limits, _ := json.Marshal(cfg.Limits)
	mac.Write(limits)
	s.profile = hex.EncodeToString(mac.Sum(nil))
	identity := hmac.New(sha256.New, cfg.NamespaceKey)
	fmt.Fprintf(identity, "sip-state-logical-authority-v1:%s:%s:%s", s.root, cfg.Generation, s.profile)
	s.authority = Authority{Generation: cfg.Generation, RunID: hex.EncodeToString(identity.Sum(nil))[:40]}
	return s
}

func (s *metadata) Authority() Authority { return s.authority }

// Limits returns the immutable per-group budget used for reducer admission.
func (s *metadata) Limits() Limits { return s.cfg.Limits }

func (s *metadata) Namespace() string { return s.cfg.Namespace }

func (s *metadata) Versions() Versions { return s.cfg.Versions }

func (s *metadata) PartitionCount() uint16 { return s.cfg.Partitions }

func (s *metadata) GroupForCallID(callID string) (Ref, error) {
	if callID == "" || len(callID) > 1024 || strings.ContainsAny(callID, "\r\n\x00") {
		return Ref{}, errors.New("invalid SIP Call-ID")
	}
	h := hmac.New(sha256.New, s.cfg.NamespaceKey)
	for _, part := range []string{s.cfg.Namespace, callID} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		h.Write(size[:])
		h.Write([]byte(part))
	}
	digest := h.Sum(nil)
	return Ref{ID: base64.RawURLEncoding.EncodeToString(digest), Partition: uint16(binary.BigEndian.Uint32(digest[:4]) % uint32(s.cfg.Partitions)), CallID: callID}, nil
}

func (s *metadata) validateRef(ref Ref) error {
	decoded, err := base64.RawURLEncoding.DecodeString(ref.ID)
	if err != nil || len(decoded) != sha256.Size || ref.Partition >= s.cfg.Partitions ||
		uint16(binary.BigEndian.Uint32(decoded[:4])%uint32(s.cfg.Partitions)) != ref.Partition ||
		(ref.Incarnation != "" && !referencePattern.MatchString(ref.Incarnation)) {
		return ErrConflict
	}
	if ref.CallID != "" {
		expected, err := s.GroupForCallID(ref.CallID)
		if err != nil || expected.ID != ref.ID {
			return ErrConflict
		}
	}
	return nil
}

func (s *metadata) validateInput(input Input) error {
	if err := validateApplicationLeaseIDs(input.ApplicationLeaseIDs); err != nil {
		return err
	}
	if !referencePattern.MatchString(input.ID) || !referencePattern.MatchString(input.Kind) || !validText(input.Fingerprint, 256) ||
		input.DeadlineMS <= 0 || input.DeadlineMS > 1<<53-1 || input.ReceiptUntilMS < input.DeadlineMS ||
		input.ReceiptUntilMS > 1<<53-1 || input.ReceiptUntilMS-input.DeadlineMS > int64(10*time.Minute/time.Millisecond) {
		return errors.New("invalid SIP input identity/deadline")
	}
	_, data, err := digestInput(input)
	if err != nil {
		return err
	}
	if len(data) > s.cfg.Limits.InputBytes {
		return ErrCapacity
	}
	return nil
}

func (s *metadata) decodeRecord(ref Ref, inc, revision, data, receipts, pending, callID, proof string) (Record, error) {
	ref.Incarnation = inc
	ref.CallID = callID
	if err := s.validateRef(ref); err != nil {
		return Record{}, ErrCorrupt
	}
	if len(data) > s.cfg.Limits.GroupBytes-4096 {
		return Record{}, ErrCorrupt
	}
	record := Record{Ref: ref, Authority: s.authority, Versions: s.cfg.Versions, Revision: revision}
	// The custom decoder already validates the complete JSON before assignment.
	// An outer json.Unmarshal would scan the entire retained aggregate twice.
	if record.Payload.UnmarshalJSON([]byte(data)) != nil || json.Unmarshal([]byte(receipts), &record.Receipts) != nil {
		return Record{}, ErrCorrupt
	}
	if proof != "" {
		record.Proof = &Receipt{}
		if json.Unmarshal([]byte(proof), record.Proof) != nil {
			return Record{}, ErrCorrupt
		}
	}
	if pending != "" {
		record.Pending = &Input{}
		if json.Unmarshal([]byte(pending), record.Pending) != nil {
			return Record{}, ErrCorrupt
		}
	}
	if err := s.validatePayloadStructure(record.Payload); err != nil {
		return Record{}, ErrCorrupt
	}
	if err := s.validatePayloadSize(record.Payload, len(data)); err != nil {
		return Record{}, ErrCorrupt
	}
	return record, nil
}

func (s *metadata) validatePayloadStructure(payload Payload) error {
	if err := validateApplicationContexts(payload.ApplicationContexts, payload.ApplicationLeaseIDs); err != nil {
		return err
	}
	if err := validateApplicationLeaseIDs(payload.ApplicationLeaseIDs); err != nil {
		return err
	}
	if len(payload.CommitLeaseGuards) > applicationLeaseLimit {
		return ErrCapacity
	}
	for _, guard := range payload.CommitLeaseGuards {
		if !validApplicationLeaseRef(guard) {
			return ErrCorrupt
		}
	}
	leaseIDs := make(map[string]bool, len(payload.ApplicationLeaseIDs))
	for _, id := range payload.ApplicationLeaseIDs {
		leaseIDs[id] = true
	}
	l := s.cfg.Limits
	if payload.ActiveTransactions < 0 || payload.ActiveTransactions > l.Transactions || payload.ReservedCompletionBytes < 0 ||
		len(payload.Effects) > l.Effects || len(payload.Timers) > l.Timers || payload.Retention.UntilMS < 0 || payload.Retention.UntilMS > 1<<53-1 {
		return ErrCapacity
	}
	seen := make(map[string]bool, len(payload.Effects))
	for _, effect := range payload.Effects {
		if effect.Worker != "" && effect.Worker != AnyDispatchWorker && !dispatchWorkerPattern.MatchString(effect.Worker) {
			return ErrCorrupt
		}
		if effect.ApplicationLease != nil && (!validApplicationLeaseRef(*effect.ApplicationLease) || !leaseIDs[effect.ApplicationLease.ID]) {
			return ErrCorrupt
		}
		if !referencePattern.MatchString(effect.ID) || !referencePattern.MatchString(effect.Kind) || seen[effect.ID] || effect.DeadlineMS <= 0 || effect.DeadlineMS > 1<<53-1 {
			return fmt.Errorf("%w: invalid or repeated effect identity %q", ErrCorrupt, effect.ID)
		}
		seen[effect.ID] = true
		switch effect.Status {
		case EffectPending, EffectAuthorized, EffectCancelled:
		case EffectDispatching, EffectWritten, EffectNotWritten, EffectUnknown:
			if !referencePattern.MatchString(effect.AttemptID) {
				return fmt.Errorf("%w: effect %q has invalid attempt", ErrCorrupt, effect.ID)
			}
		default:
			return fmt.Errorf("%w: effect %q has invalid status", ErrCorrupt, effect.ID)
		}
	}
	seen = make(map[string]bool, len(payload.Timers))
	for _, timer := range payload.Timers {
		if !referencePattern.MatchString(timer.ID) || seen[timer.ID] || !decimal(timer.Generation) || timer.DueMS <= 0 || timer.DueMS > 1<<53-1 || timer.DeadlineMS < timer.DueMS || timer.DeadlineMS > 1<<53-1 {
			return fmt.Errorf("%w: invalid or repeated timer %q", ErrCorrupt, timer.ID)
		}
		seen[timer.ID] = true
	}
	return nil
}

func (s *metadata) validatePayloadSize(payload Payload, encodedBytes int) error {
	available := s.cfg.Limits.GroupBytes - 4096
	if len(payload.ApplicationLeaseIDs) > 0 {
		projection, err := json.Marshal(payload.ApplicationLeaseIDs)
		if err != nil {
			return err
		}
		encodedBytes += len(projection)
	}
	if payload.ReservedCompletionBytes > available || encodedBytes > available-payload.ReservedCompletionBytes {
		return ErrCapacity
	}
	return nil
}
