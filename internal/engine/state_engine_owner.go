package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

const (
	stateOwnerProbeInterval = int64(5000)
	stateOwnerProbeBudget   = int64(2000)
	stateOwnerProofAge      = int64(5000)
	stateOwnerClockSkew     = int64(250)
)

func stateOwnerContextIDs(group stateGroup) []string {
	ids := make([]string, 0, len(group.Contexts))
	for id := range group.Contexts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Controller selection is a pure preflight, separate from the original command
// or SIP admission. A crash after installing the directory-approved controller
// cannot consume the original command sequence or repeat its application work.
func (e *StateEngine) preflightSubscriptionController(ctx context.Context, ref state.Ref, in stateEngineInput, deadline time.Time, prepared ...portable.Message) (bool, error) {
	if in.Application == nil || in.Context == nil || in.Kind != "command" && in.Kind != "ingest" {
		return false, nil
	}
	if in.Kind == "ingest" && in.Ingest != nil {
		message := statePreflightMessage(in, prepared)
		facts, err := message.Facts()
		if err == nil && !facts.IsRequest {
			return false, nil // an exact native response completes a transaction, not business ownership
		}
	}
	view, _, err := e.store.ReadState(ctx, ref)
	if err != nil {
		return false, err
	}
	record := stateViewProtocolRecord(view)
	group, err := decodeStateGroup(record)
	if err != nil {
		return false, err
	}
	c := group.Contexts[in.Context.Id]
	if c == nil || c.Subscription == nil || sameProcess(c.Snapshot.Application, in.Application) {
		return false, nil
	}
	peers := e.control.ReadyPeers()
	if len(peers) == 0 {
		return false, fmt.Errorf("%w: subscription controller proof unavailable", errStateApplicationOwner)
	}
	probe := &pb.ApplicationProbe{Context: e.contextRef(record.Ref, in.Context.Id), Application: proto.Clone(in.Application).(*pb.ProcessRef)}
	view, record, group, c = state.StateView{}, state.Record{}, stateGroup{}, nil
	reply, err := peers[0].Call(ctx, &pb.Envelope{Payload: &pb.Envelope_ProbeApplication{ProbeApplication: probe}})
	if err != nil || reply.GetApplicationStatus() == nil {
		return false, fmt.Errorf("%w: subscription controller proof unavailable", errStateApplicationOwner)
	}
	proof := proto.Clone(reply.GetApplicationStatus()).(*pb.ApplicationStatus)
	if proof.Context != nil {
		proof.Context.Owner = nil
	}
	input := stateEngineInput{Kind: "owner-control", Context: probe.Context, Application: probe.Application,
		ApplicationLeaseTerm: in.ApplicationLeaseTerm, ApplicationGeneration: in.ApplicationGeneration,
		Reply: &pb.Envelope{Payload: &pb.Envelope_ApplicationStatus{ApplicationStatus: proof}}}
	id := e.id("subscription-controller", in.Context.Id, in.Application.InstanceId, in.Application.Epoch, proof.DirectoryEpoch, strconv.FormatUint(proof.CheckedAtMs, 10), in.ApplicationLeaseTerm, strconv.FormatUint(in.ApplicationGeneration, 10))
	result, err := e.apply(ctx, ref, id, input, deadline, false)
	if err != nil {
		return false, err
	}
	if result.Certainty != pb.OperationResult_COMPLETED || result.Error != "" {
		return false, fmt.Errorf("%w: subscription controller proof refused", errStateApplicationOwner)
	}
	return true, nil
}

func (r *stateReduction) ownerControlResult() error {
	if r.in.Context == nil || r.in.Application == nil {
		return fmt.Errorf("%w: subscription controller identity required", errStateApplicationOwner)
	}
	c := r.group.Contexts[r.in.Context.Id]
	status := r.in.Reply.GetApplicationStatus()
	if c == nil || c.Subscription == nil || status == nil || !status.Live {
		return fmt.Errorf("%w: subscription controller proof unavailable", errStateApplicationOwner)
	}
	if err := r.validateOwnerStatus(c, status, r.in.Context, r.in.Application); err != nil {
		return fmt.Errorf("%w: %v", errStateApplicationOwner, err)
	}
	return r.installSubscriptionController(c, r.in.Application, status.DirectoryEpoch)
}

func (r *stateReduction) installSubscriptionController(c *stateContext, application *pb.ProcessRef, epoch string) error {
	if c.Subscription == nil || c.Snapshot.Ended {
		return fmt.Errorf("%w: active subscription required", errStateApplicationOwner)
	}
	old := c.Snapshot.Application
	if err := r.ownerAdopted(c, old, application, epoch); err != nil {
		return err
	}
	if !sameProcess(old, application) {
		c.Snapshot.Application = proto.Clone(application).(*pb.ProcessRef)
		r.touch(c)
	}
	return nil
}

func (r *stateReduction) validateOwnerStatus(c *stateContext, status *pb.ApplicationStatus, context *pb.ContextRef, application *pb.ProcessRef) error {
	checked := int64(status.CheckedAtMs)
	if !stateSameContext(status.Context, context) || !sameProcess(status.Application, application) || status.DirectoryEpoch == "" || status.CheckedAtMs > 1<<53-1 ||
		checked <= 0 || checked > r.now+stateOwnerClockSkew || r.now-checked > stateOwnerProofAge {
		return errors.New("application owner proof identity/time mismatch")
	}
	if c.OwnerDirectoryEpoch != "" && c.OwnerDirectoryEpoch != status.DirectoryEpoch {
		return errors.New("application owner directory epoch changed")
	}
	return nil
}

func (r *stateReduction) ownerProbe(c *stateContext) (int, state.Effect, stateEngineEffect, bool) {
	if c.OwnerProbeEffect == "" {
		return 0, state.Effect{}, stateEngineEffect{}, false
	}
	index, effect, body, err := r.findEffect(c.OwnerProbeEffect)
	return index, effect, body, err == nil && effect.Kind == "owner"
}

func (r *stateReduction) removeOwnerProbe(c *stateContext) {
	if index, _, _, found := r.ownerProbe(c); found {
		r.payload.Effects = append(r.payload.Effects[:index], r.payload.Effects[index+1:]...)
	}
	c.OwnerProbeEffect = ""
}

func (r *stateReduction) ensureOwnerProbe(c *stateContext, application *pb.ProcessRef) error {
	if application == nil || application.InstanceId == "" || application.Epoch == "" {
		return errors.New("application owner proof identity required")
	}
	if _, _, body, found := r.ownerProbe(c); found {
		if sameProcess(body.Envelope.GetProbeApplication().GetApplication(), application) {
			return nil
		}
		// A read-only old query can be replaced by an approved pending processor's
		// query without changing the current application owner.
		r.removeOwnerProbe(c)
	}
	probe := &pb.ApplicationProbe{Context: stateSnapshot(c, r.engine.cfg.Process).Context, Application: proto.Clone(application).(*pb.ProcessRef)}
	r.addEffect("owner", c, stateEngineEffect{Application: proto.Clone(application).(*pb.ProcessRef), Envelope: &pb.Envelope{Payload: &pb.Envelope_ProbeApplication{ProbeApplication: probe}}}, r.now+stateOwnerProbeBudget)
	c.OwnerProbeEffect = r.payload.Effects[len(r.payload.Effects)-1].ID
	c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
	return nil
}

func (r *stateReduction) deferForOwnerProof(c *stateContext, tx *stateTransaction) (bool, error) {
	if r.engine.autonomousCompletion() {
		if _, err := r.requireNativeOwner(c, false); err != nil {
			return true, r.ownerRefuseRequest(c, tx)
		}
		tx.OwnerValidated, tx.OwnerDeferred = true, false
		return false, nil
	}
	application := tx.Application
	if application == nil {
		application = c.Snapshot.Application
		tx.Application = proto.Clone(application).(*pb.ProcessRef)
	}
	if c.OwnerLost && sameProcess(application, c.Snapshot.Application) {
		return true, r.ownerRefuseRequest(c, tx)
	}
	if tx.OwnerValidated || c.Snapshot.Kind != pb.ContextSnapshot_TRANSACTION && c.OwnerValidated && !c.OwnerLost && sameProcess(application, c.Snapshot.Application) {
		return false, nil
	}
	tx.OwnerDeferred = true
	if err := r.ensureOwnerProbe(c, application); err != nil {
		return true, err
	}
	return true, nil
}

func (r *stateReduction) ownerRefuseRequest(c *stateContext, tx *stateTransaction) error {
	tx.OwnerDeferred, tx.DeferredPresent, tx.PendingResponse = false, false, nil
	tx.PresentDeadlineMS = 0
	if c.Subscription != nil && c.Subscription.Pending == tx.Native.Key {
		c.Subscription.Pending = ""
	}
	if tx.Native.Family != portable.InviteServer && tx.Native.Family != portable.NonInviteServer {
		return nil
	}
	if tx.Native.LastResponse != nil {
		facts, err := tx.Native.LastResponse.Facts()
		if err == nil && !facts.IsRequest && facts.Status >= 200 {
			return nil
		}
	}
	response, err := r.buildResponse(c, tx.Native.Origin, 503, "Application owner unavailable", nil, nil)
	if err != nil {
		return err
	}
	if c.OwnerLost {
		if _, err := tx.Native.Origin.ClientTxKey(); err != nil {
			// Earlier admission accepted branch forms the native response matcher
			// cannot prove. Preserve the owner fence and request-disposition proof,
			// and end the unsendable transaction without inventing physical output.
			return r.stepTransaction(c, tx, portable.Input{Kind: portable.Terminate, Now: r.now, Error: "unsupported request response identity"})
		}
	}
	if facts, err := tx.Native.Origin.Facts(); err == nil && facts.CSeqMethod == sip.INVITE {
		for key, dialog := range c.Dialogs {
			if dialog.RemoteInvite != nil && portable.MatchRequest(dialog.RemoteInvite.Request, tx.Native.Origin) {
				next, _, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogSendResponse, Now: r.now, Message: response})
				if err != nil {
					return err
				}
				c.Dialogs[key] = next
			}
		}
	}
	return r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: response})
}

func (r *stateReduction) failDeferredOwner(c *stateContext, application *pb.ProcessRef) error {
	failed := false
	for _, tx := range c.Transactions {
		if tx.OwnerDeferred && sameProcess(tx.Application, application) {
			failed = true
			if err := r.ownerRefuseRequest(c, tx); err != nil {
				return err
			}
		}
	}
	if failed && c.Subscription == nil && !c.Snapshot.Answered && c.Dialog == "" {
		c.Snapshot.Ended = true
		if c.RetireMS == 0 {
			c.RetireMS = r.now + 120000
		}
		c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, SipStatus: 503, Error: "application owner proof unavailable", Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
		r.touch(c)
	}
	return nil
}

func (r *stateReduction) ownerDue(c *stateContext) int64 {
	due := c.OwnerProbeAtMS
	if c.OwnerLost && c.Subscription == nil {
		return c.OwnerCleanupAtMS
	}
	if _, effect, body, found := r.ownerProbe(c); found {
		for _, tx := range c.Transactions {
			if tx.OwnerDeferred && sameProcess(tx.Application, body.Envelope.GetProbeApplication().GetApplication()) && (due == 0 || effect.DeadlineMS < due) {
				due = effect.DeadlineMS
			}
		}
	}
	return due
}

func (r *stateReduction) ownerPendingWork() (map[string]bool, error) {
	pending := make(map[string]bool)
	for id, c := range r.group.Contexts {
		for _, tx := range c.Transactions {
			if tx.OwnerDeferred {
				pending[id] = true
				break
			}
		}
	}
	for _, effect := range r.payload.Effects {
		if effect.Kind != "event" && effect.Kind != "adopt" && effect.Kind != "open" && effect.Kind != "credential" {
			continue
		}
		if effect.Status != state.EffectPending && effect.Status != state.EffectDispatching && effect.Status != state.EffectUnknown {
			continue
		}
		var body stateEngineEffect
		if r.unmarshalEffectMetadata(&body, effect) != nil {
			return nil, state.ErrCorrupt
		}
		if c := r.group.Contexts[body.Context]; c != nil && !c.OwnerLost && sameProcess(body.Application, c.Snapshot.Application) {
			pending[body.Context] = true
		}
	}
	return pending, nil
}

func stateFinalResponseServerContext(c *stateContext) bool {
	if c.Snapshot.Kind != pb.ContextSnapshot_TRANSACTION || c.Snapshot.Answered || c.Subscription != nil || c.Dialog != "" || len(c.Dialogs) != 0 || len(c.Transactions) == 0 {
		return false
	}
	for _, tx := range c.Transactions {
		if tx.Native.Family != portable.NonInviteServer || tx.Native.LastResponse == nil || tx.OwnerDeferred || tx.DeferredPresent || tx.PendingResponse != nil ||
			tx.Native.Phase != portable.Completed && tx.Native.Phase != portable.Terminated {
			return false
		}
		facts, err := tx.Native.LastResponse.Facts()
		if err != nil || facts.IsRequest || facts.Status < 200 {
			return false
		}
	}
	return true
}

func stateOwnerProbeNeeded(c *stateContext, pending map[string]bool) bool {
	if pending[c.Snapshot.Context.Id] {
		return true
	}
	return !c.OwnerLost && !c.Snapshot.Ended && !stateFinalResponseServerContext(c)
}

func (r *stateReduction) ownerTimer() (bool, error) {
	if r.engine.autonomousCompletion() {
		return r.nativeOwnerTimer()
	}
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		if r.in.Timer != r.timerID(id, "owner", "probe", "") {
			continue
		}
		due := r.ownerDue(c)
		if due == 0 || r.in.Generation != strconv.FormatInt(due, 10) || r.now < due {
			return true, nil
		}
		if c.OwnerLost && c.Subscription == nil {
			return true, r.finishOwnerCleanup(c)
		}
		pending, err := r.ownerPendingWork()
		if err != nil {
			return true, err
		}
		if !stateOwnerProbeNeeded(c, pending) {
			r.removeOwnerProbe(c)
			return true, nil
		}
		if index, effect, body, found := r.ownerProbe(c); found {
			application := body.Envelope.GetProbeApplication().GetApplication()
			if effect.DeadlineMS <= r.now {
				deferred := false
				for _, tx := range c.Transactions {
					deferred = deferred || tx.OwnerDeferred && sameProcess(tx.Application, application)
				}
				if deferred {
					if err := r.failDeferredOwner(c, application); err != nil {
						return true, err
					}
					r.removeOwnerProbe(c)
					c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
					return true, nil
				}
			}
			// Retrying a pure read is a new bounded RPC attempt, never permission to
			// repeat an application operation or a physical SIP send.
			effect.Status, effect.AttemptID, effect.DeadlineMS = state.EffectPending, "", r.now+stateOwnerProbeBudget
			body.Executor = nil
			r.setEffect(index, effect, body)
			c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
			return true, nil
		}
		if !c.OwnerLost {
			return true, r.ensureOwnerProbe(c, c.Snapshot.Application)
		}
		c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
		return true, nil
	}
	return false, nil
}

func (r *stateReduction) appendOwnerTimers() error {
	if r.engine.autonomousCompletion() {
		return r.appendNativeOwnerTimers()
	}
	if err := r.expireOwnerComputations(); err != nil {
		return err
	}
	pending, err := r.ownerPendingWork()
	if err != nil {
		return err
	}
	for _, id := range stateOwnerContextIDs(r.group) {
		c := r.group.Contexts[id]
		if c.OwnerLost {
			if err := r.continueOwnerLoss(c); err != nil {
				return err
			}
		}
		if !stateOwnerProbeNeeded(c, pending) && c.OwnerCleanupAtMS == 0 {
			r.removeOwnerProbe(c)
			continue
		}
		if c.OwnerProbeAtMS == 0 {
			c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
		}
		due := r.ownerDue(c)
		if due <= r.now {
			due = r.now + 1
		}
		r.payload.Timers = append(r.payload.Timers, state.Timer{ID: r.timerID(id, "owner", "probe", ""), Generation: strconv.FormatInt(r.ownerDue(c), 10), DueMS: due, DeadlineMS: due})
	}
	return nil
}

func (e *StateEngine) dispatchOwnerProbe(ctx context.Context, ref state.Ref, effect state.Effect, body stateEngineEffect, release func()) {
	peers := e.control.ReadyPeers()
	if len(peers) == 0 || body.Envelope.GetProbeApplication() == nil {
		return
	}
	request := proto.Clone(body.Envelope).(*pb.Envelope)
	request.GetProbeApplication().Context.Owner = proto.Clone(e.cfg.Process).(*pb.ProcessRef)
	reply, err := peers[0].Call(ctx, request)
	in := stateEngineInput{Kind: "owner-result", Effect: effect.ID}
	if err != nil {
		in.Failure = "application owner probe unavailable"
	} else {
		in.Reply = stateCanonicalReply(reply)
		if status := in.Reply.GetApplicationStatus(); status != nil && status.Context != nil {
			status.Context.Owner = nil // executor hints do not change pure proof identity
		}
	}
	if _, committed, err := e.applyCommitted(e.ctx, ref, stateEffectResultID(e, "owner-proof", effect.ID, in), in, time.Now().Add(2*time.Second), false); err == nil {
		release()
		e.dispatchCommitted(ref, committed, effect.ID, effect.Kind)
	}
}

func (r *stateReduction) ownerProbeResult() error {
	if r.engine.autonomousCompletion() {
		return errors.New("directory probe is not native SIP owner authority")
	}
	index, effect, body, err := r.findEffect(r.in.Effect)
	if err != nil {
		return nil // a completed/replaced pure query keeps no receipt history
	}
	if effect.Kind != "owner" || body.Envelope.GetProbeApplication() == nil {
		return errors.New("application owner query required")
	}
	c := r.group.Contexts[body.Context]
	probe := body.Envelope.GetProbeApplication()
	if c == nil || c.OwnerProbeEffect != effect.ID {
		return nil
	}
	status := r.in.Reply.GetApplicationStatus()
	if r.in.Failure != "" || status == nil {
		effect.Status = state.EffectUnknown
		if effect.AttemptID == "" {
			effect.AttemptID = "probe-" + r.engine.id(effect.ID)[:48]
		}
		r.setEffect(index, effect, body)
		c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
		return nil
	}
	if err := r.validateOwnerStatus(c, status, probe.Context, probe.Application); err != nil {
		return err
	}
	if !status.Live && !c.OwnerValidated && (c.Snapshot.Answered || c.Subscription != nil) {
		return errors.New("application owner epoch anchor unavailable")
	}
	r.removeOwnerProbe(c)
	c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
	if status.Live {
		if c.Subscription != nil && !sameProcess(probe.Application, c.Snapshot.Application) {
			if err := r.installSubscriptionController(c, probe.Application, status.DirectoryEpoch); err != nil {
				return err
			}
		}
		if sameProcess(probe.Application, c.Snapshot.Application) && !c.OwnerLost {
			c.OwnerValidated, c.OwnerDirectoryEpoch = true, status.DirectoryEpoch
		}
		for _, tx := range c.Transactions {
			if tx.OwnerDeferred && sameProcess(tx.Application, probe.Application) {
				tx.OwnerValidated, tx.OwnerDeferred = true, false
				if err := r.presentNow(c, tx); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if sameProcess(probe.Application, c.Snapshot.Application) {
		if c.OwnerDirectoryEpoch == "" {
			c.OwnerDirectoryEpoch = status.DirectoryEpoch // no business was presented yet
		}
		return r.loseApplicationOwner(c)
	}
	return r.failDeferredOwner(c, probe.Application)
}

func (r *stateReduction) ownerAdmitPermit(c *stateContext, body stateEngineEffect, permit *pb.SendPermit) error {
	if c.OwnerLost || body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_APPLICATION || !sameProcess(body.Application, c.Snapshot.Application) {
		return nil
	}
	if permit.DirectoryEpoch == "" || c.OwnerDirectoryEpoch != "" && c.OwnerDirectoryEpoch != permit.DirectoryEpoch {
		return errors.New("application permit owner epoch mismatch")
	}
	c.OwnerValidated, c.OwnerDirectoryEpoch = true, permit.DirectoryEpoch
	return nil
}

func (r *stateReduction) ownerGuardCommand(c *stateContext, command *pb.Command) error {
	if c.Subscription != nil && !sameProcess(c.Snapshot.Application, r.in.Application) {
		return fmt.Errorf("%w: subscription controller changed", errStateApplicationOwner)
	}
	if r.engine.autonomousCompletion() {
		_, err := r.requireNativeOwner(c, true)
		return err
	}
	if !c.OwnerLost {
		return nil
	}
	if c.Subscription != nil && command.GetRespond() != nil {
		tx := c.Transactions[c.Requests[command.GetRespond().RequestId]]
		if tx != nil && tx.OwnerValidated && !sameProcess(tx.Application, c.Snapshot.Application) && sameProcess(tx.Application, r.in.Application) {
			return nil
		}
	}
	return fmt.Errorf("%w: application owner epoch fenced", errStateApplicationOwner)
}

func (r *stateReduction) ownerGuardRequest(c *stateContext, method sip.RequestMethod, in *pb.OwnerIngest) error {
	if r.engine.autonomousCompletion() && method != sip.ACK && method != sip.CANCEL && method != sip.BYE {
		if _, err := r.requireNativeOwner(c, true); err != nil {
			return &portable.ProtocolError{Status: 481, Reason: "native application owner term fenced"}
		}
	}
	if !c.OwnerLost || method == sip.ACK || method == sip.CANCEL || method == sip.BYE {
		return nil
	}
	if c.Subscription != nil && method == sip.SUBSCRIBE && !sameProcess(in.Application, c.Snapshot.Application) {
		return nil
	}
	return &portable.ProtocolError{Status: 481, Reason: "application owner epoch fenced"}
}

func (r *stateReduction) ownerAcceptedACK(c *stateContext, dialog string, flow *pb.FlowRef) error {
	if flow == nil {
		return errors.New("ACK completion flow required")
	}
	if c.DialogCompletionFlows == nil {
		c.DialogCompletionFlows = make(map[string]*pb.FlowRef)
	}
	c.DialogCompletionFlows[dialog] = proto.Clone(flow).(*pb.FlowRef)
	if dialog == c.Dialog {
		c.Snapshot.InitialAck = true
	}
	r.touch(c)
	return nil
}

func (r *stateReduction) ownerAdopted(c *stateContext, oldApplication, application *pb.ProcessRef, epoch string) error {
	if epoch == "" || c.OwnerDirectoryEpoch != "" && c.OwnerDirectoryEpoch != epoch {
		return errors.New("subscription owner proof epoch mismatch")
	}
	if c.OwnerLost && sameProcess(oldApplication, application) {
		return fmt.Errorf("%w: fenced subscription processor cannot resume", errStateApplicationOwner)
	}
	if r.engine.autonomousCompletion() {
		term := c.Snapshot.ApplicationLeaseTerm
		generation := c.Snapshot.AttachmentGeneration
		if r.in.Kind == "owner-control" {
			generation = r.in.ApplicationGeneration
		}
		if !sameProcess(oldApplication, application) {
			if r.in.Kind != "owner-control" || !sameProcess(application, r.in.Application) {
				return fmt.Errorf("%w: native subscription adoption requires directory-approved controller", errStateApplicationOwner)
			}
			term = r.in.ApplicationLeaseTerm
		}
		if r.in.Kind == "owner-control" && (!sameProcess(application, r.in.Application) || term != r.in.ApplicationLeaseTerm) {
			return fmt.Errorf("%w: stale subscription controller session", errStateApplicationOwner)
		}
		if _, err := r.guardNativeLease(application, term, generation); err != nil {
			return err
		}
		c.Snapshot.ApplicationLeaseTerm = term
		c.Snapshot.AttachmentGeneration = generation
	}
	if !sameProcess(oldApplication, application) {
		if err := r.abandonOwnerWork(c, oldApplication); err != nil {
			return err
		}
	}
	r.removeOwnerProbe(c)
	c.OwnerLost, c.Cancelled, c.OwnerValidated, c.OwnerCleanupAtMS = false, false, true, 0
	c.OwnerDirectoryEpoch, c.OwnerProbeAtMS = epoch, r.now+stateOwnerProbeInterval
	return nil
}

func (r *stateReduction) abandonOwnerWork(c *stateContext, application *pb.ProcessRef) error {
	for index, effect := range r.payload.Effects {
		var body stateEngineEffect
		if r.unmarshalEffectMetadata(&body, effect) != nil {
			return state.ErrCorrupt
		}
		if body.Context != c.Snapshot.Context.Id || !sameProcess(body.Application, application) || effect.Kind == "owner" {
			continue
		}
		if effect.Kind == "wire" || effect.Kind == "deliver" {
			if body.Authorization.GetAuthorityClass() != pb.EffectAuthorization_APPLICATION {
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
			}
		} else if effect.Status == state.EffectPending || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown {
			// The permanently fenced epoch cannot execute a future command. This
			// abandons policy/presentation outcomes, never a physical SIP attempt.
			body.Cancelled, effect.Status = true, state.EffectCancelled
		}
		r.setEffect(index, effect, body)
	}
	return nil
}

func (r *stateReduction) loseApplicationOwner(c *stateContext) error {
	if c.OwnerLost {
		return nil
	}
	c.OwnerLost = true
	if c.Subscription == nil {
		c.Cancelled = true
		c.OwnerCleanupAtMS = r.now + 2*64*portable.DefaultTimers().T1
	}
	if err := r.abandonOwnerWork(c, c.Snapshot.Application); err != nil {
		return err
	}
	for _, tx := range c.Transactions {
		owned := tx.Application == nil || sameProcess(tx.Application, c.Snapshot.Application)
		if owned && (tx.OwnerDeferred || tx.DeferredPresent || tx.PendingResponse != nil || tx.Native.Family == portable.InviteServer || tx.Native.Family == portable.NonInviteServer) {
			if err := r.ownerRefuseRequest(c, tx); err != nil {
				return err
			}
		}
	}
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: "application owner epoch lost", Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
	r.touch(c)
	return r.continueOwnerLoss(c)
}

func (r *stateReduction) continueOwnerLoss(c *stateContext) error {
	if !c.OwnerLost || c.Subscription != nil {
		return nil
	}
	for _, tx := range c.Transactions {
		if tx.Native.Family == portable.InviteClient && !tx.Native.Cancelled && (tx.Native.Phase == portable.Calling || tx.Native.Phase == portable.Proceeding) {
			if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.RequestCancel, Now: r.now}); err != nil {
				return err
			}
		}
	}
	keys := make([]string, 0, len(c.Dialogs))
	for key := range c.Dialogs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		dialog := c.Dialogs[key]
		if dialog.Phase == portable.DialogEnded || dialog.LocalBye != nil || dialog.Phase == portable.DialogEarly {
			continue
		}
		if local := dialog.LocalInvite; local != nil && (local.State == portable.InvitePending || local.State == portable.InviteAwaitingACK) {
			continue // transaction cancellation/late-success ACK completes first
		}
		if remote := dialog.RemoteInvite; remote != nil && remote.State == portable.InviteAwaitingACK {
			continue // a UAS cannot BYE its successful initial before ACK/timeout
		}
		next, effects, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now, Method: sip.BYE, Branch: sip.RFC3261BranchMagicCookie + r.engine.id("owner-loss-bye", r.record.Ref.Incarnation, key)[:32]})
		if err != nil {
			// A native obligation may be unsendable (for example CSeq exhaustion).
			// Commit the owner fence and retain it until the fixed cleanup deadline.
			if c.Result == nil {
				c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			}
			c.Result.Error = "native owner-loss cleanup unavailable"
			continue
		}
		c.Dialogs[key] = next
		if err := r.dialogEffectsForClass(c, key, effects, true); err != nil {
			return err
		}
	}
	if c.Dialog == "" && !c.Snapshot.Ended {
		c.Snapshot.Ended = true
		if c.RetireMS == 0 {
			c.RetireMS = r.now + 120000
		}
		r.touch(c)
	}
	return nil
}

func (r *stateReduction) finishOwnerCleanup(c *stateContext) error {
	for key, dialog := range c.Dialogs {
		if dialog.Phase == portable.DialogEnded {
			continue
		}
		next, effects, err := portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogTerminate, Now: r.now})
		if err != nil {
			return err
		}
		c.Dialogs[key] = next
		if err := r.dialogEffectsFor(c, key, effects); err != nil {
			return err
		}
	}
	c.OwnerCleanupAtMS, c.Snapshot.Ended = 0, true
	if c.RetireMS == 0 {
		c.RetireMS = r.now + 120000
	}
	r.touch(c)
	return nil
}

func (r *stateReduction) expireOwnerComputations() error {
	for index, effect := range r.payload.Effects {
		if effect.Kind != "credential" && effect.Kind != "open" || effect.DeadlineMS+120000 > r.now || effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled {
			continue
		}
		var body stateEngineEffect
		if r.unmarshalEffectMetadata(&body, effect) != nil {
			return state.ErrCorrupt
		}
		effect.Status = state.EffectCancelled
		r.setEffect(index, effect, body)
		if c := r.group.Contexts[body.Context]; c != nil && !c.Snapshot.Ended {
			c.Snapshot.Ended, c.RetireMS = true, r.now+120000
			message := "SIP setup credential deadline elapsed"
			if effect.Kind == "open" {
				message = "SIP setup transport deadline elapsed"
			}
			c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: message, Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
			r.touch(c)
			r.event(c, pb.DialogEvent_FAILED, nil, c.Operation)
		}
	}
	return r.expireTerminalSetupNotifications()
}

func (r *stateReduction) terminalPreWireSetup(c *stateContext) (bool, error) {
	if !c.Snapshot.Ended || c.Snapshot.Answered || c.Dialog != "" || len(c.Dialogs) != 0 || c.Subscription != nil ||
		(c.OwnerValidated || c.OwnerDirectoryEpoch != "") && !r.engine.autonomousCompletion() || c.CompletionScope != "" {
		return false, nil
	}
	for _, tx := range c.Transactions {
		if tx.Written || tx.CompletionScope != "" {
			return false, nil
		}
	}
	for _, effect := range r.payload.Effects {
		if effect.Kind != "wire" && effect.Kind != "deliver" {
			continue
		}
		var body stateEngineEffect
		if r.unmarshalEffectMetadata(&body, effect) != nil {
			return false, state.ErrCorrupt
		}
		if body.Context != c.Snapshot.Context.Id {
			continue
		}
		// A retained grant, physical attempt or unresolved authorization cannot
		// be inferred away from a failed setup bit or reporting deadline.
		if effect.Kind == "deliver" || body.Permit != nil || effect.Status == state.EffectDispatching || effect.Status == state.EffectUnknown || effect.Status == state.EffectWritten {
			return false, nil
		}
	}
	return true, nil
}

func (r *stateReduction) expireTerminalSetupNotifications() error {
	for index, effect := range r.payload.Effects {
		if effect.Kind != "event" || effect.DeadlineMS+120000 > r.now || effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten || effect.Status == state.EffectCancelled {
			continue
		}
		var body stateEngineEffect
		if r.unmarshalEffectMetadata(&body, effect) != nil {
			return state.ErrCorrupt
		}
		if body.Envelope.GetDialogEvent().GetKind() != pb.DialogEvent_FAILED {
			continue
		}
		c := r.group.Contexts[body.Context]
		if c == nil {
			return state.ErrCorrupt
		}
		terminal, err := r.terminalPreWireSetup(c)
		if err != nil {
			return err
		}
		if terminal {
			body.Cancelled, effect.Status = true, state.EffectCancelled
			r.setEffect(index, effect, body)
		}
	}
	return nil
}
