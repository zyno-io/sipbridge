package engine

import (
	"errors"
	"strconv"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// The native transaction owns the only cached response wire. This compact anchor
// identifies its exact rendered response without retaining another wire copy.
type stateServerFinal struct {
	Status       uint32      `json:"status"`
	Command      string      `json:"command,omitempty"`
	DeadlineMS   int64       `json:"deadlineMs,omitempty"`
	UntilMS      int64       `json:"untilMs,omitempty"`
	WireHash     string      `json:"wireHash"`
	Flow         *pb.FlowRef `json:"flow"`
	Written      bool        `json:"written,omitempty"`
	AnyWritten   bool        `json:"anyWritten,omitempty"`
	FinalWritten bool        `json:"finalWritten,omitempty"`
	NoStart      bool        `json:"noStart,omitempty"`
	RetryAtMS    int64       `json:"retryAtMs,omitempty"`
}

func stateRenderResponse(message portable.Message, flow *pb.FlowRef) (portable.Message, error) {
	parsed, err := message.Parse()
	if err != nil {
		return message, err
	}
	if response, ok := parsed.(*sip.Response); ok && flow != nil {
		response.Via().Params.Add("received", flow.SourceIp)
		if response.Via().Params.Has("rport") {
			response.Via().Params.Add("rport", strconv.Itoa(int(flow.SourcePort)))
		}
		message.Wire = []byte(response.String())
	}
	return message, nil
}

func stateServerTransaction(tx *stateTransaction) bool {
	return tx != nil && (tx.Native.Family == portable.InviteServer || tx.Native.Family == portable.NonInviteServer)
}

func stateFinalEffect(tx *stateTransaction, body stateEngineEffect) bool {
	if !stateServerTransaction(tx) || tx.Native.LastResponse == nil || body.Flow == nil {
		return false
	}
	message, err := stateRenderResponse(*tx.Native.LastResponse, body.Flow)
	if err != nil {
		return false
	}
	parsed, err := message.Parse()
	response, ok := parsed.(*sip.Response)
	return err == nil && ok && response.StatusCode >= 200 && body.WireHash == stateACKHash(message.Wire)
}

func stateRecordWrittenFinal(tx *stateTransaction, body stateEngineEffect) {
	if tx == nil || tx.FinalResponse == nil {
		return
	}
	if stateFinalEffect(tx, body) {
		tx.FinalResponse.FinalWritten = true
	}
	if stateRelatedResponse(tx, body) {
		tx.FinalResponse.AnyWritten = true
	}
	if stateSelectedResponseEffect(tx, body) {
		tx.FinalResponse.Written = true
		tx.FinalResponse.RetryAtMS = 0
	}
}

func stateSelectedResponseEffect(tx *stateTransaction, body stateEngineEffect) bool {
	if !stateServerTransaction(tx) || tx.FinalResponse == nil || tx.Native.LastResponse == nil || body.Flow == nil {
		return false
	}
	selected := tx.FinalResponse
	return selected.Status == body.WireStatus && selected.Command == body.Command && selected.WireHash == body.WireHash && proto.Equal(selected.Flow, body.Flow)
}

func stateResponseUntil(tx *stateTransaction, status uint32, command string) int64 {
	var until int64
	if command != "" {
		until = tx.RespondDeadlineMS
	}
	if status < 200 && (command == "" || until == 0) {
		return 0
	}
	if tx.RetireMS > 0 && (until == 0 || tx.RetireMS < until) {
		until = tx.RetireMS
	}
	if status >= 200 {
		for _, name := range []portable.Timer{portable.TimerJ, portable.TimerH, portable.TimerL} {
			if timer := tx.Native.Timers[name]; timer.Active && (until == 0 || timer.At < until) {
				until = timer.At
			}
		}
	}
	return until
}

func stateResponseHelper(tx *stateTransaction) bool {
	if !stateServerTransaction(tx) || tx.FinalResponse == nil {
		return false
	}
	if tx.FinalResponse.Status < 200 || tx.Native.Family == portable.NonInviteServer {
		return true
	}
	// Reliable non-2xx INVITE finals have Timer H but no periodic Timer G.
	// ACK/termination ends this obligation; successful finals retain TU retry.
	return tx.FinalResponse.Status >= 300 && tx.Native.Reliable && tx.Native.Phase == portable.Completed &&
		!tx.Native.Timers[portable.TimerG].Active && tx.Native.Timers[portable.TimerH].Active
}

// Provisional identities include the command and rendered response on any
// admitted flow. Final uncertainty remains transaction-wide, as before.
func stateRelatedResponse(tx *stateTransaction, body stateEngineEffect) bool {
	selected := tx.FinalResponse
	if selected == nil {
		return false
	}
	final, unclassified := stateResponseClass(body)
	if selected.Status >= 200 {
		return final || unclassified
	}
	if body.Command != selected.Command {
		return false
	}
	if unclassified {
		return true
	}
	if body.WireStatus != selected.Status || body.Flow == nil || tx.Native.LastResponse == nil {
		return false
	}
	rendered, err := stateRenderResponse(*tx.Native.LastResponse, body.Flow)
	return err == nil && stateACKHash(rendered.Wire) == body.WireHash
}

func (r *stateReduction) expireUnstartedResponse(c *stateContext, tx *stateTransaction) error {
	for index, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.UnmarshalBinary(effect.Payload) != nil {
			return state.ErrCorrupt
		}
		if body.Context != c.Snapshot.Context.Id || body.Transaction != tx.Native.Key || !stateRelatedResponse(tx, body) || effect.DeadlineMS > r.now {
			continue
		}
		switch {
		case effect.Kind == "wire" && effect.Status == state.EffectPending:
			body.Cancelled, effect.Status = true, state.EffectCancelled
		case effect.Kind == "wire" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown):
			body.Cancelled, effect.Status = true, state.EffectUnknown
		case effect.Kind == "deliver" && effect.Status == state.EffectAuthorized:
			body.Cancelled, effect.Status = true, state.EffectNotWritten
			effect.AttemptID = body.Authorization.GetEffect().GetAttemptId()
		default:
			continue
		}
		if stateSelectedResponseEffect(tx, body) {
			tx.FinalResponse.NoStart = true
		}
		r.setEffect(index, effect, body)
	}
	return nil
}

func stateResponseNoStart(effect state.Effect) bool {
	return effect.Kind == "deliver" && effect.Status == state.EffectNotWritten ||
		effect.Kind == "wire" && effect.Status == state.EffectCancelled
}

func (r *stateReduction) releaseNoStartServerFinal(c *stateContext, effect state.Effect, body stateEngineEffect) {
	if c == nil || !stateResponseNoStart(effect) {
		return
	}
	tx := c.Transactions[body.Transaction]
	if !stateSelectedResponseEffect(tx, body) {
		return
	}
	selected := tx.FinalResponse
	selected.NoStart = true
	if !stateResponseHelper(tx) || selected.Written || selected.AnyWritten || selected.Status >= 200 && selected.FinalWritten ||
		selected.UntilMS <= r.now || !stateResponseEmissionAvailable(tx, selected.Status) {
		return
	}
	if selected.Status < 200 && (selected.Command == "" || c.Operation != selected.Command || c.Result == nil || c.Result.Certainty != pb.OperationResult_ACCEPTED) {
		return
	}
	if selected.RetryAtMS == 0 {
		selected.RetryAtMS = min(r.now+tx.Native.Profile.T1, selected.UntilMS)
	}
}

// Every retry is a fresh finite emission of the exact selected native response,
// never another start of a reserved attempt or another logical SIP procedure.
func (r *stateReduction) retryServerFinal(c *stateContext, tx *stateTransaction) error {
	selected := tx.FinalResponse
	if selected == nil || selected.RetryAtMS == 0 || selected.RetryAtMS > r.now {
		return nil
	}
	if !stateResponseHelper(tx) || selected.Written || selected.AnyWritten || selected.Status >= 200 && selected.FinalWritten ||
		tx.Native.LastResponse == nil || selected.UntilMS <= r.now || !stateResponseEmissionAvailable(tx, selected.Status) ||
		selected.Status < 200 && (selected.Command == "" || c.Operation != selected.Command || c.Result == nil || c.Result.Certainty != pb.OperationResult_ACCEPTED) {
		selected.RetryAtMS = 0
		return nil
	}
	selected.RetryAtMS = min(r.now+tx.Native.Profile.T1, selected.UntilMS)
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.UnmarshalBinary(effect.Payload) != nil {
			return state.ErrCorrupt
		}
		if body.Context != c.Snapshot.Context.Id || body.Transaction != tx.Native.Key || !stateRelatedResponse(tx, body) {
			continue
		}
		if effect.Kind == "deliver" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown || effect.Status == state.EffectWritten) {
			return nil
		}
		if effect.DeadlineMS > r.now && (effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized ||
			effect.Kind == "wire" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown)) {
			return nil
		}
	}
	if err := r.expireUnstartedResponse(c, tx); err != nil {
		return err
	}
	message, err := stateRenderResponse(*tx.Native.LastResponse, selected.Flow)
	if err != nil || stateACKHash(message.Wire) != selected.WireHash {
		return errors.New("retained selected response rendering mismatch")
	}
	selected.RetryAtMS = 0
	return r.sendOnFlow(c, tx, message, true, selected.Flow, selected.UntilMS)
}

// Typed status survives physical reservation; old full-wire effects can be
// classified by parsing. An opaque historical physical effect blocks recovery.
func stateResponseClass(body stateEngineEffect) (final, unclassified bool) {
	if body.WireStatus > 0 {
		return body.WireStatus >= 200, false
	}
	if len(body.Wire) > 0 {
		parsed, err := sip.ParseMessage(body.Wire)
		if err == nil {
			response, ok := parsed.(*sip.Response)
			return ok && response.StatusCode >= 200, false
		}
	}
	return false, true
}

func (r *stateReduction) serverFinalRetryLive(c *stateContext, tx *stateTransaction) bool {
	if !stateServerTransaction(tx) || tx.FinalResponse == nil {
		return false
	}
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.UnmarshalBinary(effect.Payload) != nil || body.Context != c.Snapshot.Context.Id || body.Transaction != tx.Native.Key ||
			!stateSelectedResponseEffect(tx, body) || effect.DeadlineMS <= r.now {
			continue
		}
		if effect.Kind == "deliver" && effect.Status == state.EffectAuthorized || effect.Kind == "wire" &&
			(effect.Status == state.EffectPending || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown) {
			return true
		}
	}
	selected := tx.FinalResponse
	if !stateResponseEmissionAvailable(tx, selected.Status) {
		return false
	}
	if stateResponseHelper(tx) {
		return !selected.Written && !selected.AnyWritten && selected.UntilMS > r.now && selected.RetryAtMS > 0
	}
	if g, h := tx.Native.Timers[portable.TimerG], tx.Native.Timers[portable.TimerH]; g.Active && h.Active && h.At > r.now {
		return true
	}
	for _, dialog := range c.Dialogs {
		procedure := dialog.RemoteInvite
		if procedure != nil && procedure.State == portable.InviteAwaitingACK && procedure.Final != nil &&
			portable.MatchResponse(tx.Native.Origin, *procedure.Final) && dialog.Timers[portable.Timer2xx].Active &&
			dialog.Timers[portable.TimerACK].Active && dialog.Timers[portable.TimerACK].At > r.now {
			return true
		}
	}
	return false
}

// A failed provisional delivery can release the application lane only with
// positive selected-response absence and no other live/uncertain physical work.
func (r *stateReduction) finishProvisionalNoStart(c *stateContext, tx *stateTransaction) bool {
	selected := tx.FinalResponse
	if selected == nil || selected.Status >= 200 || !selected.NoStart || selected.Written || selected.AnyWritten {
		return false
	}
	if err := r.expireUnstartedResponse(c, tx); err != nil {
		return false
	}
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if body.UnmarshalBinary(effect.Payload) != nil {
			return false
		}
		if body.Context != c.Snapshot.Context.Id || body.Transaction != tx.Native.Key || !stateRelatedResponse(tx, body) {
			continue
		}
		if effect.Kind == "deliver" && (effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown || effect.Status == state.EffectWritten || effect.Status == state.EffectAuthorized) {
			return false
		}
		if effect.Kind == "wire" && !body.Cancelled && (effect.Status == state.EffectPending || effect.Status == state.EffectAuthorized || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown) {
			return false
		}
	}
	c.Result.Certainty = pb.OperationResult_COMPLETED
	if c.Result.Error == "" {
		c.Result.Error = "SIP provisional response not written within delivery budget"
	}
	selected.RetryAtMS = 0
	return true
}

func (r *stateReduction) responseCommandNoStart(c *stateContext, effect state.Effect, body stateEngineEffect) bool {
	if !stateResponseNoStart(effect) || c == nil || body.Command == "" || c.Operation != body.Command || c.Result == nil || c.Result.Certainty != pb.OperationResult_ACCEPTED {
		return false
	}
	tx := c.Transactions[body.Transaction]
	if !stateServerTransaction(tx) {
		return false
	}
	if !stateSelectedResponseEffect(tx, body) {
		return true
	}
	if tx.RespondDeadlineMS > r.now && r.serverFinalRetryLive(c, tx) {
		return true
	}
	if r.finishProvisionalNoStart(c, tx) {
		return true
	}
	c.Result.Certainty = pb.OperationResult_UNKNOWN
	if c.Result.Error == "" {
		c.Result.Error = "SIP response delivery unavailable or deadline elapsed"
	}
	return true
}

// Retry and command deadlines share the existing one-per-group recovery timer.
func (r *stateReduction) serverResponseDue() int64 {
	var due int64
	for _, c := range r.group.Contexts {
		for _, tx := range c.Transactions {
			if tx.FinalResponse != nil && tx.FinalResponse.RetryAtMS > 0 && (due == 0 || tx.FinalResponse.RetryAtMS < due) {
				due = tx.FinalResponse.RetryAtMS
			}
			if tx.RespondDeadlineMS > 0 && c.Operation == tx.Command && c.Result != nil && c.Result.Certainty == pb.OperationResult_ACCEPTED &&
				(due == 0 || tx.RespondDeadlineMS < due) {
				due = tx.RespondDeadlineMS
			}
		}
	}
	return due
}

func (r *stateReduction) recoverServerResponses() error {
	for _, c := range r.group.Contexts {
		for _, tx := range c.Transactions {
			if tx.RespondDeadlineMS > 0 && c.Operation == tx.Command && c.Result != nil && c.Result.Certainty == pb.OperationResult_ACCEPTED {
				selected := tx.FinalResponse
				if selected != nil && selected.Command == c.Operation && selected.NoStart &&
					(selected.UntilMS <= r.now || !stateResponseEmissionAvailable(tx, selected.Status)) {
					if err := r.expireUnstartedResponse(c, tx); err != nil {
						return err
					}
					r.finishProvisionalNoStart(c, tx)
				}
				if tx.RespondDeadlineMS <= r.now && c.Result.Certainty == pb.OperationResult_ACCEPTED {
					if err := r.expireUnstartedResponse(c, tx); err != nil {
						return err
					}
					if !r.finishProvisionalNoStart(c, tx) {
						c.Result.Certainty = pb.OperationResult_UNKNOWN
						if c.Result.Error == "" {
							c.Result.Error = "SIP response delivery deadline elapsed"
						}
					}
				}
			}
			if err := r.retryServerFinal(c, tx); err != nil {
				return err
			}
		}
	}
	return nil
}
