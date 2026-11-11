package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Default E/G/2xx schedules allow eleven emissions through 64*T1. A retained
// transaction budgets two such wire classes (request/final plus ACK/provisional).
const (
	stateProfileTransmissions = 11
	stateTransactionEmissions = 2 * stateProfileTransmissions
	stateForkDialogs          = 16
	stateProfileTransactions  = 32
	stateNativeProcedures     = 128
	stateProfileContexts      = 32
	stateWireLimit            = 65536
	stateProofMetadataBytes   = 8192
)

type stateACKEmission struct {
	Sequence uint32 `json:"sequence"`
	Count    uint32 `json:"count"`
}
type stateCompletionBudget struct {
	Effects, Timers, Transactions, Regular, Dialogs, Bytes int
	ProofBytes                                             int
	ContextSlots                                           map[string]int
}

func stateResponseEmissionAvailable(tx *stateTransaction, status uint32) bool {
	if tx.Emissions >= stateTransactionEmissions || tx.ProvisionalEmissions > tx.Emissions {
		return false
	}
	if status < 200 {
		return tx.ProvisionalEmissions < stateProfileTransmissions
	}
	return tx.Emissions-tx.ProvisionalEmissions < stateProfileTransmissions
}

func (r *stateReduction) reserveEmission(c *stateContext, tx *stateTransaction, message portable.Message, flow *pb.FlowRef, replay bool) (bool, error) {
	parsed, err := message.Parse()
	if err != nil {
		return false, err
	}
	request, requestMessage := parsed.(*sip.Request)
	response, responseMessage := parsed.(*sip.Response)
	serverResponse := stateServerTransaction(tx) && responseMessage
	ackKey := ""
	if requestMessage && request.IsAck() {
		key := tag(request.From().Params) + "/" + tag(request.To().Params)
		if d, ok := c.Dialogs[key]; ok && d.LocalInvite != nil && d.LocalInvite.ACK != nil && bytes.Equal(d.LocalInvite.ACK.Wire, message.Wire) {
			ackKey = key
		}
	}
	// Multiple packets can wait on one not-yet-started immutable intent. A
	// reserved/unknown physical attempt is never made into retry permission.
	if replay {
		for _, effect := range r.payload.Effects {
			if effect.DeadlineMS <= r.now {
				continue
			}
			if effect.Status != state.EffectPending && effect.Status != state.EffectAuthorized && !(effect.Kind == "wire" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown)) {
				continue
			}
			var body stateEngineEffect
			if body.unmarshalMetadata(effect.Payload) == nil && body.Context == c.Snapshot.Context.Id && body.Transaction == tx.Native.Key && bytes.Equal(body.Wire, message.Wire) && proto.Equal(body.Flow, flow) &&
				(!serverResponse || body.Command == tx.Command || uint32(response.StatusCode) != tx.RespondStatus && body.Command == "") {
				return false, nil
			}
		}
	}
	if ackKey != "" {
		if c.ACKEmissions == nil {
			c.ACKEmissions = make(map[string]stateACKEmission)
		}
		counter := c.ACKEmissions[ackKey]
		if counter.Sequence != request.CSeq().SeqNo {
			counter = stateACKEmission{Sequence: request.CSeq().SeqNo}
		}
		if counter.Count >= stateProfileTransmissions {
			return false, nil
		}
		counter.Count++
		c.ACKEmissions[ackKey] = counter
	} else {
		if tx.Emissions >= stateTransactionEmissions {
			return false, nil
		}
		if serverResponse && !stateResponseEmissionAvailable(tx, uint32(response.StatusCode)) {
			return false, nil
		}
		if r.in.Kind == "command" {
			pending := 0
			for _, effect := range r.payload.Effects {
				if effect.Kind != "wire" && effect.Kind != "deliver" {
					continue
				}
				var body stateEngineEffect
				if body.unmarshalMetadata(effect.Payload) == nil && body.Context == c.Snapshot.Context.Id && body.Transaction == tx.Native.Key && (effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized || effect.Kind == "wire" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown)) {
					final, opaque := stateResponseClass(body)
					if !serverResponse || opaque || final == (response.StatusCode >= 200) {
						pending++
					}
				}
			}
			if pending >= 3 {
				return false, state.ErrCapacity
			}
		}
		tx.Emissions++
		if serverResponse && response.StatusCode < 200 {
			tx.ProvisionalEmissions++
		}
	}
	return true, nil
}

func (r *stateReduction) reclaimEffects() error {
	retained := r.payload.Effects[:0]
	for _, effect := range r.payload.Effects {
		if !stateSupportedEffectKind(effect.Kind) {
			return state.ErrCorrupt
		}
		terminal := effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled
		if !terminal {
			retained = append(retained, effect)
			continue
		}
		var body stateEngineEffect
		if err := body.unmarshalMetadata(effect.Payload); err != nil {
			return state.ErrCorrupt
		}
		if effect.Kind == "deliver" && !body.Settled {
			retained = append(retained, effect)
			continue
		}
		if effect.Kind == "deliver" && effect.Status == state.EffectWritten {
			if c := r.group.Contexts[body.Context]; c != nil {
				if tx := c.Transactions[body.Transaction]; tx != nil {
					tx.Written = true
					stateRecordWrittenFinal(tx, body)
				}
			}
		}
		// Native procedure/request ids, ACK hashes and transaction Written are
		// the compact proof after receiver settlement; absent old effects never
		// return bytes from Reserve or permission for another physical attempt.
	}
	r.payload.Effects = retained
	return nil
}

func (r *stateReduction) completionBudget() (stateCompletionBudget, error) {
	budget := stateCompletionBudget{ContextSlots: make(map[string]int)}
	proofHeadroom := 0
	futureTX, futureRegular, futureDialogs, sends, progress, progressBytes, otherRPC, wireProcedures, snapshotBytes, endedACKWire := 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	policyPresent := false
	for id, c := range r.group.Contexts {
		beforeFuture, beforeDialogs, beforePotential := futureTX, budget.Dialogs, futureDialogs
		initial, setup, opening := false, false, false
		cleanup, cancels, maxDigest := 0, 0, uint32(0)
		cancelKeys := make(map[string]bool)
		serverInvites := make(map[string]bool)
		for _, tx := range c.Transactions {
			policyPresent = policyPresent || tx.Policy != nil
			if stateServerTransaction(tx) {
				actual, err := tx.stateRequestDisposition.encodedBytes()
				if err != nil {
					return budget, err
				}
				proofHeadroom, err = addStateProofHeadroom(proofHeadroom, 1, stateRequestProofBytes-actual)
				if err != nil {
					return budget, err
				}
			}
			parsed, err := tx.Native.Origin.Parse()
			if err != nil {
				return budget, err
			}
			if tx.Native.Phase != portable.Terminated {
				budget.Transactions++
				if parsed.CSeq().MethodName != sip.BYE && parsed.CSeq().MethodName != sip.CANCEL {
					budget.Regular++
				}
				if tx.Native.Family == portable.InviteClient || tx.Native.Family == portable.NonInviteClient {
					setup = true
				}
			}
			if tx.Native.Phase != portable.Terminated || (tx.Native.Family == portable.InviteServer || tx.Native.Family == portable.NonInviteServer) && tx.Native.LastResponse != nil || tx.Native.Family == portable.InviteClient && tx.Native.ACK != nil {
				sends += max(0, stateTransactionEmissions-int(tx.Emissions))
				wireProcedures++
			}
			if tx.Native.Family == portable.InviteClient && tag(parsed.To().Params) == "" {
				initial = true
			}
			if tx.Cleanup && parsed.CSeq().MethodName == sip.BYE {
				cleanup++
			}
			branch := parsed.Via().Params.GetOr("branch", "") + "/" + fmt.Sprint(parsed.CSeq().SeqNo) + "/" + parsed.Via().SentBy()
			if parsed.CSeq().MethodName == sip.CANCEL {
				cancels++
				cancelKeys[branch] = true
			}
			if tx.Native.Family == portable.InviteServer && !tx.Native.Cancelled {
				serverInvites[branch] = true
			}
			maxDigest = max(maxDigest, tx.DigestAttempts)
		}
		for branch := range serverInvites {
			if !cancelKeys[branch] {
				futureTX++
			}
		}
		for _, effect := range r.payload.Effects {
			if effect.Kind != "open" && effect.Kind != "credential" {
				continue
			}
			var body stateEngineEffect
			if body.unmarshalMetadata(effect.Payload) != nil {
				return budget, state.ErrCorrupt
			}
			if body.Context == c.Snapshot.Context.Id && (effect.Status == state.EffectPending || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown) {
				policyPresent = policyPresent || body.Policy != nil
				setup = true
				if effect.Kind == "open" {
					opening = true
				}
				if origin, err := c.Origin.Parse(); err == nil && origin.CSeq().MethodName == sip.INVITE {
					initial = true
				}
			}
		}
		if opening {
			futureTX++
			futureRegular++
		}
		if setup && c.Credential != nil && !c.Snapshot.Answered && !c.Snapshot.Ended {
			retries := max(0, stateDigestRetryLimit-int(maxDigest))
			futureTX += retries
			futureRegular += retries
		}
		origin, err := c.Origin.Parse()
		if err != nil {
			return budget, err
		}
		if initial {
			futureTX += max(0, stateForkDialogs-cleanup) + max(0, 1-cancels)
			missing := max(0, stateForkDialogs-len(c.Dialogs))
			futureDialogs += missing
			sends += missing * stateProfileTransmissions
			if !c.Snapshot.Answered {
				remaining := stateProfileTransmissions * (stateForkDialogs + 1)
				for _, used := range c.ProgressEmissions {
					remaining -= int(used)
				}
				remaining = max(0, remaining)
				progress += remaining
				progressBytes += remaining * (base64.StdEncoding.EncodedLen(stateWireLimit+len(c.Snapshot.LocalSdp)) + stateProofMetadataBytes)
			}
		} else if origin.CSeq().MethodName == sip.INVITE && c.Snapshot.Kind == pb.ContextSnapshot_UAS && c.Dialog == "" && !c.Snapshot.Ended {
			futureDialogs++
			futureTX++
		}
		for key, dialog := range c.Dialogs {
			if dialog.Phase != portable.DialogEnded {
				budget.Dialogs++
			}
			if !initial && dialog.Phase != portable.DialogEnded && dialog.LocalBye == nil {
				futureTX++
			}
			if dialog.LocalInvite != nil {
				owesACK := initial
				for _, tx := range c.Transactions {
					if tx.Native.Family == portable.InviteClient {
						parsed, _ := tx.Native.Origin.Parse()
						if parsed.CSeq().SeqNo == dialog.LocalInvite.Sequence && (tag(parsed.To().Params) == "" || tag(parsed.To().Params) == dialog.RemoteTag) {
							owesACK = true
							break
						}
					}
				}
				if owesACK {
					used := uint32(0)
					if counter := c.ACKEmissions[key]; counter.Sequence == dialog.LocalInvite.Sequence {
						used = counter.Count
					}
					sends += max(0, stateProfileTransmissions-int(used))
					if dialog.Phase == portable.DialogEnded && used < stateProfileTransmissions {
						endedACKWire += 2 // Immutable body and authorization envelope.
					}
				}
			}
		}
		otherRPC += 8
		local, remote := len(c.Snapshot.LocalSdp), len(c.Snapshot.RemoteSdp)
		if origin.CSeq().MethodName == sip.INVITE {
			remote = max(remote, stateWireLimit)
			if c.Snapshot.Kind == pb.ContextSnapshot_UAS {
				local = max(local, stateWireLimit)
			}
		}
		snapshotBytes += 8 * base64.StdEncoding.EncodedLen(local+remote)
		budget.ContextSlots[id] = len(c.Transactions) + futureTX - beforeFuture + budget.Dialogs - beforeDialogs + futureDialogs - beforePotential
	}
	// Each admitted directional branch can receive a simultaneous remote BYE
	// even while its own local terminal procedure is active.
	futurePeer := budget.Dialogs + futureDialogs
	futureTX += futurePeer
	var proofErr error
	proofHeadroom, proofErr = addStateProofHeadroom(proofHeadroom, futureTX, stateRequestProofBytes)
	if proofErr != nil {
		return budget, proofErr
	}
	budget.ProofBytes = proofHeadroom
	sends += futureTX * stateTransactionEmissions
	budget.Transactions += futureTX
	budget.Regular += futureRegular
	budget.Dialogs += futureDialogs
	budget.Effects = sends + progress + otherRPC
	budget.Timers = 4*budget.Transactions + 2*budget.Dialogs + 5*len(r.group.Contexts) + 1
	pendingWire := 3*(wireProcedures+futureTX) + 2*budget.Dialogs + endedACKWire
	nativeWire := 2*futureTX + 4*futureDialogs
	// Future digest/setup/terminal procedures can inherit a current policy
	// selection. Reserve its actual maximum JSON bound, never another ledger.
	budget.Bytes = (sends+otherRPC)*stateProofMetadataBytes + progressBytes + snapshotBytes + base64.StdEncoding.EncodedLen((pendingWire+nativeWire)*stateWireLimit) + proofHeadroom
	if policyPresent {
		budget.Bytes += base64.StdEncoding.EncodedLen(futureTX * statePolicyAdmissionBytes)
	}
	return budget, nil
}

func (r *stateReduction) capacityFits(budget stateCompletionBudget) bool {
	limits := r.engine.store.Limits()
	for _, occupied := range budget.ContextSlots {
		if occupied > 64 {
			return false
		}
	}
	return budget.Regular <= stateProfileTransactions && budget.Transactions <= min(stateNativeProcedures, limits.Transactions) && budget.Dialogs <= stateForkDialogs && len(r.payload.Effects)+budget.Effects <= limits.Effects && budget.Timers <= limits.Timers
}

func stateProgressKey(message *pb.SipMessage) string {
	if message == nil {
		return "untagged"
	}
	parsed, err := sip.ParseMessage(message.Wire)
	if err != nil {
		return "untagged"
	}
	if tag(parsed.To().Params) == "" {
		return "untagged"
	}
	return tag(parsed.From().Params) + "/" + tag(parsed.To().Params)
}

var errStateCompletionCapacity = errors.New("native completion capacity reserved")

func (e *StateEngine) statePayloadFits(record state.Record, payload state.Payload) bool {
	limits := e.store.Limits()
	if payload.ActiveTransactions > limits.Transactions || len(payload.Effects) > limits.Effects || len(payload.Timers) > limits.Timers {
		return false
	}
	encoded, err := payload.MarshalBinary()
	if err != nil {
		return false
	}
	history, err := json.Marshal(record.Receipts)
	if err != nil {
		return false
	}
	marker := base64.StdEncoding.EncodedLen(limits.OutcomeBytes) + 1024
	return len(encoded)+len(history)+4096+payload.ReservedCompletionBytes+2*marker <= limits.GroupBytes
}
