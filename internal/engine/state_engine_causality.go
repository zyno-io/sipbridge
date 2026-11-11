package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// One latest successful ACK proof per retained directional dialog is sufficient:
// native retirement prevents another INVITE after allocating cleanup BYE.
type stateACKProof struct {
	Sequence uint32 `json:"sequence"`
	WireHash string `json:"wireHash"`
}

func stateHistoricalDelivery(effect state.Effect) bool {
	return effect.Kind == "deliver" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown)
}

func stateACKHash(wire []byte) string {
	sum := sha256.Sum256(wire)
	return hex.EncodeToString(sum[:])
}

func stateAnnotateACKDependency(c *stateContext, tx *stateTransaction, message portable.Message, body *stateEngineEffect) error {
	parsed, err := message.Parse()
	if err != nil {
		return err
	}
	request, ok := parsed.(*sip.Request)
	if !ok || !request.IsAck() && request.Method != sip.BYE {
		return nil
	}
	key := tag(request.From().Params) + "/" + tag(request.To().Params)
	dialog, exists := c.Dialogs[key]
	if !exists || dialog.LocalInvite == nil || dialog.LocalInvite.ACK == nil {
		return nil // UAS initial ACK timeout has no local ACK obligation.
	}
	ack := dialog.LocalInvite.ACK
	if request.IsAck() {
		if !bytes.Equal(ack.Wire, message.Wire) {
			return nil // A non-2xx transaction ACK is not successful-dialog proof.
		}
		body.Dialog = key
	} else if tx.Cleanup {
		body.Dialog = key
		body.AfterACK = stateACKHash(ack.Wire)
	}
	return nil
}

func stateACKDependencyReady(c *stateContext, body stateEngineEffect) bool {
	if body.AfterACK == "" {
		return true
	}
	if c == nil || body.Dialog == "" {
		return false
	}
	return c.ACKWritten[body.Dialog].WireHash == body.AfterACK
}

func stateRecordWrittenACK(c *stateContext, body stateEngineEffect) error {
	if body.Dialog == "" || body.AfterACK != "" {
		return nil
	}
	sequence, hash, method := body.WireSequence, body.WireHash, body.WireMethod
	if len(body.Wire) > 0 {
		parsed, err := sip.ParseMessage(body.Wire)
		if err != nil {
			return err
		}
		sequence = parsed.CSeq().SeqNo
		hash = stateACKHash(body.Wire)
		method = string(parsed.CSeq().MethodName)
	}
	if c == nil || method != "ACK" || len(hash) != 64 {
		return errors.New("native ACK completion identity invalid")
	}
	if _, exists := c.Dialogs[body.Dialog]; !exists {
		return errors.New("native ACK dialog unavailable")
	}
	if c.ACKWritten == nil {
		c.ACKWritten = make(map[string]stateACKProof)
	}
	if sequence >= c.ACKWritten[body.Dialog].Sequence {
		c.ACKWritten[body.Dialog] = stateACKProof{Sequence: sequence, WireHash: hash}
	}
	if body.Dialog == c.Dialog && c.Snapshot.Kind == pb.ContextSnapshot_UAC {
		c.Snapshot.InitialAck = true
	}
	return nil
}

// The initial application procedure ends at its exact winning ACK write. The
// client transaction's Timer M owns late forks independently of that result.
func (r *stateReduction) completeInitialUAC(c *stateContext, body stateEngineEffect) {
	if c == nil || c.Snapshot.Kind != pb.ContextSnapshot_UAC || !c.Snapshot.Answered || !c.Snapshot.InitialAck ||
		c.Snapshot.Ended || c.Cancelled || c.OwnerLost || body.Dialog == "" || body.Dialog != c.Dialog || body.AfterACK != "" ||
		body.WireMethod != "ACK" || body.Command == "" || c.Operation != body.Command ||
		c.Result == nil || c.Result.Certainty != pb.OperationResult_ACCEPTED || c.Result.Error != "" {
		return
	}
	tx := c.Transactions[body.Transaction]
	dialog, exists := c.Dialogs[body.Dialog]
	procedure := dialog.LocalInvite
	if tx == nil || tx.Native.Family != portable.InviteClient || tx.Command != body.Command || !exists ||
		dialog.Role != portable.UAC || dialog.Phase != portable.DialogConfirmed || dialog.Retiring || procedure == nil ||
		procedure.State != portable.InviteAcked || procedure.CancelRequested || procedure.Final == nil || procedure.ACK == nil ||
		!bytes.Equal(tx.Native.Origin.Wire, procedure.Request.Wire) || procedure.Sequence != body.WireSequence ||
		stateACKHash(procedure.ACK.Wire) != body.WireHash {
		return
	}
	origin, err := tx.Native.Origin.Parse()
	if err != nil || tag(origin.To().Params) != "" {
		return // A re-INVITE is a different application operation.
	}
	final, err := procedure.Final.Parse()
	response, ok := final.(*sip.Response)
	if err != nil || !ok || !response.IsSuccess() || !portable.MatchResponse(tx.Native.Origin, *procedure.Final) {
		return
	}
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Request: mustObserve(tx.Native.Origin.Wire, true),
		Response: mustObserve(procedure.Final.Wire, false), Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
}

// The request is a write receipt for this current public BYE, not a peer reply.
// The transaction and its native retransmissions continue independently.
func (r *stateReduction) recordWrittenPublicBye(c *stateContext, body stateEngineEffect) {
	if c == nil || !body.Native || body.Command == "" || body.Command != c.Operation || body.WireMethod != "BYE" || body.WireStatus != 0 ||
		body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_APPLICATION || c.Result == nil ||
		c.Result.Certainty != pb.OperationResult_ACCEPTED || c.Result.Error != "" {
		return
	}
	tx := c.Transactions[body.Transaction]
	if !currentPublicBye(c, tx) || body.WireHash != stateACKHash(tx.Native.Origin.Wire) {
		return
	}
	tx.Written = true
}

func currentPublicBye(c *stateContext, tx *stateTransaction) bool {
	if c == nil || tx == nil || tx.Cleanup || tx.Native.Family != portable.NonInviteClient || tx.Policy == nil ||
		c.Operation == "" || tx.Command != c.Operation || tx.Policy.Command != c.Operation {
		return false
	}
	dialog, exists := c.Dialogs[c.Dialog]
	return exists && dialog.LocalBye != nil && bytes.Equal(dialog.LocalBye.Wire, tx.Native.Origin.Wire)
}

// Initial OpenUac requests are retained before writing. Only this exact current
// in-dialog public BYE gets a request after its physical WRITTEN proof.
func writtenPublicBye(c *stateContext) *stateTransaction {
	if c == nil || c.Result == nil || c.Result.Certainty != pb.OperationResult_ACCEPTED || c.Result.Error != "" {
		return nil
	}
	for _, tx := range c.Transactions {
		if tx.Written && currentPublicBye(c, tx) {
			return tx
		}
	}
	return nil
}

func projectWrittenPublicBye(c *stateContext, result *pb.OperationResult) {
	if tx := writtenPublicBye(c); tx != nil {
		result.Request = mustObserve(tx.Native.Origin.Wire, true)
	}
}

// A slow ACK proof may outlive the first cleanup intent's finite grant budget.
// A new native emission may then send the same cached BYE before its original
// transaction timeout, but cannot adopt a started or uncertain physical attempt.
func (r *stateReduction) releaseACKCleanup(c *stateContext, ack stateEngineEffect) error {
	if c == nil || ack.Dialog == "" || ack.AfterACK != "" {
		return nil
	}
	hash := ack.WireHash
	if hash == "" {
		hash = stateACKHash(ack.Wire)
	}
	if c.ACKWritten[ack.Dialog].WireHash != hash {
		return nil
	}
	byTransaction := make(map[string][]int)
	for index, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.unmarshalMetadata(effect.Payload) != nil {
			return state.ErrCorrupt
		}
		if body.Context == c.Snapshot.Context.Id && body.Dialog == ack.Dialog && body.AfterACK == hash {
			byTransaction[body.Transaction] = append(byTransaction[body.Transaction], index)
		}
	}
	for transaction, indices := range byTransaction {
		tx := c.Transactions[transaction]
		if tx == nil || !tx.Cleanup || tx.Written || tx.Native.Family != portable.NonInviteClient {
			continue
		}
		deadline := tx.Native.Timers[portable.TimerF]
		if !deadline.Active || deadline.At <= r.now {
			continue
		}
		parsed, err := tx.Native.Origin.Parse()
		request, ok := parsed.(*sip.Request)
		if err != nil || !ok || request.Method != sip.BYE {
			return errors.New("retained cleanup transaction invalid")
		}
		blocked := false
		for _, index := range indices {
			effect := r.payload.Effects[index]
			if effect.Kind == "deliver" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown || effect.Status == state.EffectWritten) ||
				effect.DeadlineMS > r.now && (effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown) {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		for _, index := range indices {
			effect := r.payload.Effects[index]
			var body stateEngineEffect
			if body.unmarshalMetadata(effect.Payload) != nil {
				return state.ErrCorrupt
			}
			switch {
			case effect.Kind == "wire" && effect.Status == state.EffectPending:
				body.Cancelled, effect.Status = true, state.EffectCancelled
			case effect.Kind == "wire" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown):
				// A late authorization receipt can only settle this old intent.
				body.Cancelled, effect.Status = true, state.EffectUnknown
			case effect.Kind == "deliver" && effect.Status == state.EffectAuthorized:
				body.Cancelled, effect.Status = true, state.EffectNotWritten
				effect.AttemptID = body.Authorization.GetEffect().GetAttemptId()
			}
			r.setEffect(index, effect, body)
		}
		r.touch(c)
		if err := r.send(c, tx, tx.Native.Origin, true, deadline.At); err != nil {
			return err
		}
	}
	return nil
}

// ACK may complete before its cleanup emission expires. The later positive
// no-start result must also release cleanup, without requiring another final
// response or another ACK syscall to repeat the already committed proof.
func (r *stateReduction) releaseExpiredACKCleanup(c *stateContext, body stateEngineEffect, deadline int64) error {
	if deadline > r.now {
		return nil
	}
	return r.releaseNoStartACKCleanup(c, body)
}

func (r *stateReduction) releaseNoStartACKCleanup(c *stateContext, body stateEngineEffect) error {
	if c == nil || body.AfterACK == "" || body.Dialog == "" || c.ACKWritten[body.Dialog].WireHash != body.AfterACK {
		return nil
	}
	return r.releaseACKCleanup(c, stateEngineEffect{Dialog: body.Dialog, WireHash: body.AfterACK})
}

func (e *StateEngine) ackDependencyReady(ref state.Ref, body stateEngineEffect) bool {
	ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
	defer cancel()
	view, _, err := e.store.ReadState(ctx, ref)
	if err != nil {
		return false
	}
	record := stateViewProtocolRecord(view)
	group, err := decodeStateGroup(record)
	return err == nil && stateACKDependencyReady(group.Contexts[body.Context], body)
}

func (r *stateReduction) contextRetirementProven(id string, c *stateContext) bool {
	if r.requestFence != nil && r.requestFence.context == id {
		return false
	}
	// finish has already removed transactions whose fixed replay retention ended.
	if len(c.Transactions) != 0 {
		return false
	}
	for _, dialog := range c.Dialogs {
		for _, timer := range dialog.Timers {
			if timer.Active {
				return false
			}
		}
	}
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.unmarshalMetadata(effect.Payload) != nil {
			return false
		}
		if body.Context == id && effect.Kind == "deliver" && !body.Settled {
			return false
		}
		if body.Context == id && (effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown) {
			return false
		}
	}
	return true
}

// No continuation or current immutable input may be hidden by cold-history
// scheduling. The unresolved evidence remains independently authoritative.
func (r *stateReduction) coldHistoryContext(id string, c *stateContext) bool {
	if !r.engine.autonomousCompletion() || !c.Snapshot.Ended || c.RetireMS <= 0 || c.RetireMS > r.now || c.ReplayUntilMS > r.now ||
		len(c.Transactions) != 0 || c.OwnerCleanupAtMS != 0 ||
		r.requestFence != nil && r.requestFence.context == id {
		return false
	}
	for _, dialog := range c.Dialogs {
		for _, timer := range dialog.Timers {
			if timer.Active {
				return false
			}
		}
	}
	return true
}

func (r *stateReduction) coldTrustedHistory(id string, c *stateContext) bool {
	if !r.coldHistoryContext(id, c) {
		return false
	}
	// The common case has no obsolete UNKNOWN at all. Do not materialize
	// every physical body merely to establish that it is not cold history.
	candidate := false
	for _, effect := range r.payload.Effects {
		if effect.Status != state.EffectUnknown || effect.DeadlineMS+120000 > r.now || !stateTrustedRelayEffect(effect) {
			continue
		}
		var identity stateEngineEffect
		if identity.unmarshalMetadata(effect.Payload) != nil {
			return false
		}
		if identity.Context == id {
			candidate = true
			break
		}
	}
	if !candidate {
		return false
	}
	found := false
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.unmarshalMetadata(effect.Payload) != nil {
			return false
		}
		if body.Context != id {
			continue
		}
		if effect.Status == state.EffectUnknown && stateTrustedRelayEffect(effect) && effect.DeadlineMS+120000 <= r.now {
			found = true
			continue
		}
		if effect.Kind == "deliver" && !body.Settled || effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized ||
			effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown {
			return false
		}
	}
	return found
}

// A cold native attempt remains uncertain and charged. Without a live protocol
// continuation, periodic lifecycle rewrites cannot establish physical evidence;
// exact ingress dispatch and a late physical completion remain independent.
func (r *stateReduction) coldNativeHistory(id string, c *stateContext) bool {
	if !c.OwnerLost || !r.coldHistoryContext(id, c) {
		return false
	}
	found := false
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.unmarshalMetadata(effect.Payload) != nil {
			return false
		}
		if body.Context != id {
			continue
		}
		if stateHistoricalDelivery(effect) && body.Native && !body.Settled && effect.DeadlineMS+120000 <= r.now {
			found = true
			continue
		}
		if effect.Kind == "deliver" && !body.Settled || effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized ||
			effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown {
			return false
		}
	}
	return found
}
