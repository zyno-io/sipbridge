package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// Wire is the independently owned exact physical variant selected by the
// proof. The caller transfers this buffer to the bounded local manager.
type nativeInviteRetryProof struct {
	Key      nativeUDPRetryKey
	Attempt  rediscore.OutputRef
	Proof    rediscore.OutputInspection
	Captured time.Time
	Flow     *pb.FlowRef
	Wire     []byte
}

type nativeInviteServerRetrySeed struct {
	nativeInviteRetryProof
	Transaction portable.Transaction
}

type nativeInviteDialogRetrySeed struct {
	nativeInviteRetryProof
	Dialog portable.Dialog
}

func nativeInviteRetryEntryBytes(entry *nativeUDPRetryEntry) int64 {
	var retained int64
	if entry.dialog != nil {
		retained = residentDialogBytes(*entry.dialog)
	} else {
		retained = residentTransactionBytes(&residentTransaction{native: entry.tx})
	}
	return retained + int64(proto.Size(entry.flow)+len(entry.wire)+len(entry.key.Context)+len(entry.key.Incarnation)+len(entry.key.Procedure))
}

// checkedInviteRetryProof associates the SDK's immutable logical final with
// the exact written physical variant, including any observed Via rewrite.
func checkedInviteRetryProof(seed nativeInviteRetryProof, logical portable.Message, dialog bool) error {
	now := time.Now()
	if !validNativeUDPRetryKey(seed.Key) || seed.Attempt.Context != seed.Key.Context || seed.Attempt.Incarnation != seed.Key.Incarnation || seed.Proof.State != rediscore.OutputWritten || !seed.Proof.Settled || seed.Proof.MetadataField == "" || !nativeMessageField(seed.Proof.MetadataField) || seed.Proof.MessageField == "" || !nativeMessageField(seed.Proof.MessageField) || seed.Proof.WireHash == ([32]byte{}) || seed.Captured.IsZero() || seed.Captured == seed.Captured.Round(0) || now.Before(seed.Captured) || now.Sub(seed.Captured) > nativeUDPHintWindow || !validPolicyFlow(seed.Flow) || seed.Flow.ConnectionId != seed.Attempt.Connection || logical.IsZero() || len(seed.Wire) == 0 || len(seed.Wire) > stateWireLimit {
		return errors.New("invalid INVITE retry proof")
	}
	metadata, err := decodeNativeOutput(seed.Proof.Metadata)
	if err != nil || metadata.Method != "INVITE" || !proto.Equal(metadata.Write.Flow, seed.Flow) || metadata.Status < 200 || metadata.Procedure != seed.Key.Procedure || dialog && (metadata.Status >= 300 || metadata.Dialog == "") || !dialog && metadata.Status < 300 {
		return errors.New("INVITE retry metadata mismatch")
	}
	facts, err := logical.Facts()
	if err != nil || facts.IsRequest || string(facts.CSeqMethod) != "INVITE" || uint32(facts.Status) != metadata.Status || logical.Transport() != seed.Flow.Transport {
		return errors.New("INVITE retry selected response changed")
	}
	ref, err := nativeOutputRef(metadata.Write)
	if err != nil || ref != seed.Attempt {
		return errors.New("INVITE retry attempt mismatch")
	}
	worker, err := nativeWorkerReference(seed.Flow.Ingress)
	if err != nil || worker != seed.Attempt.Worker || seed.Flow.Ingress.Epoch != seed.Attempt.Epoch || sha256.Sum256(seed.Wire) != seed.Proof.WireHash {
		return errors.New("INVITE retry physical evidence mismatch")
	}
	physical, err := logical.WithObservedResponseFlow(seed.Flow.SourceIp, int(seed.Flow.SourcePort))
	if err != nil || physical.WireHash() != seed.Proof.WireHash {
		return errors.New("INVITE retry logical response mismatch")
	}
	return nil
}

func (m *nativeUDPRetry) inviteEntry(seed nativeInviteRetryProof, tick, terminal portable.Timer, due, cutoff int64, size int64) (*nativeUDPRetryEntry, context.Context, time.Time, error) {
	leaseCtx, until, err := m.lease()
	now := time.Now()
	if err != nil || leaseCtx == nil || leaseCtx.Err() != nil || until == until.Round(0) || !now.Before(until) || due <= 0 || cutoff <= 0 {
		return nil, nil, time.Time{}, errors.New("INVITE retry lease unavailable")
	}
	anchorMS := until.UnixMilli()
	const maxOffset = int64((1<<63 - 1) / time.Millisecond)
	if anchorMS <= 0 || due > anchorMS && due-anchorMS > maxOffset || due < anchorMS && anchorMS-due > maxOffset || cutoff > anchorMS && cutoff-anchorMS > maxOffset || cutoff < anchorMS && anchorMS-cutoff > maxOffset {
		return nil, nil, time.Time{}, errors.New("INVITE retry deadline out of range")
	}
	key := nativeUDPRetryKey{Context: strings.Clone(seed.Key.Context), Incarnation: strings.Clone(seed.Key.Incarnation), Procedure: strings.Clone(seed.Key.Procedure)}
	entryCtx, cancel := context.WithCancel(leaseCtx)
	entry := &nativeUDPRetryEntry{key: key, tick: tick, terminal: terminal, flow: proto.Clone(seed.Flow).(*pb.FlowRef), wire: seed.Wire, anchor: until.Add(-time.Duration(until.Nanosecond() % int(time.Millisecond))), anchorMS: anchorMS, cutoff: nativeUDPRetryDeadline(until, cutoff), ctx: entryCtx, cancel: cancel, bytes: size + int64(proto.Size(seed.Flow)+len(seed.Wire)+len(key.Context)+len(key.Incarnation)+len(key.Procedure)), index: -1}
	return entry, leaseCtx, until, nil
}

func (m *nativeUDPRetry) ArmInviteServer(seed nativeInviteServerRetrySeed) error {
	tx := seed.Transaction
	g, h := tx.Timers[portable.TimerG], tx.Timers[portable.TimerH]
	if tx.Family != portable.InviteServer || tx.Reliable || tx.Phase != portable.Completed || tx.LastResponse == nil || tx.Origin.Transport() != "udp" || seed.Flow == nil || seed.Flow.Transport != "udp" || !g.Active || !h.Active || g.Generation == 0 || h.Generation == 0 || g.Interval <= 0 {
		return errors.New("invalid INVITE G seed")
	}
	if err := checkedInviteRetryProof(seed.nativeInviteRetryProof, *tx.LastResponse, false); err != nil {
		return err
	}
	slim := tx.Clone()
	slim.Timers = map[portable.Timer]portable.Deadline{portable.TimerG: g, portable.TimerH: h}
	entry, leaseCtx, until, err := m.inviteEntry(seed.nativeInviteRetryProof, portable.TimerG, portable.TimerH, g.At, h.At, residentTransactionBytes(&residentTransaction{native: slim}))
	if err != nil {
		return err
	}
	entry.tx = slim
	return m.install(entry, seed.Captured, leaseCtx, until)
}

func (m *nativeUDPRetry) ArmInviteDialog(seed nativeInviteDialogRetrySeed) error {
	dialog := seed.Dialog
	repeat, ack := dialog.Timers[portable.Timer2xx], dialog.Timers[portable.TimerACK]
	if dialog.Role != portable.UAS && dialog.Role != portable.UAC || dialog.RemoteInvite == nil || dialog.RemoteInvite.State != portable.InviteAwaitingACK || dialog.RemoteInvite.Final == nil || !repeat.Active || !ack.Active || repeat.Generation == 0 || ack.Generation == 0 || repeat.Interval <= 0 || seed.Flow == nil || dialog.Transport != seed.Flow.Transport {
		return errors.New("invalid INVITE 2xx seed")
	}
	if err := checkedInviteRetryProof(seed.nativeInviteRetryProof, *dialog.RemoteInvite.Final, true); err != nil {
		return err
	}
	// The local retry reducer consumes only this exact remote INVITE. Other
	// dialog procedures remain authoritative in Redis and need no wire cache.
	slim := dialog
	slim.LocalInvite, slim.LocalBye = nil, nil
	clone := slim.Clone()
	entry, leaseCtx, until, err := m.inviteEntry(seed.nativeInviteRetryProof, portable.Timer2xx, portable.TimerACK, repeat.At, ack.At, residentDialogBytes(clone))
	if err != nil {
		return err
	}
	entry.dialog = &clone
	return m.install(entry, seed.Captured, leaseCtx, until)
}
