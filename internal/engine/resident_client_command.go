package engine

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func (e *ResidentMemory) commandResidentClient(ctx context.Context, peer *control.Peer, cmd *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	if cmd == nil || cmd.Context == nil || peer == nil {
		return nil, errors.New("client command authority required")
	}
	if cmd.GetOpenUac() == nil {
		return e.commandResidentExistingClient(ctx, peer, cmd, operation, deadline)
	}
	ref := cmd.Context
	if !state.ValidReference(ref.Id) || strings.HasPrefix(ref.Id, "in-") {
		return nil, errors.New("invalid outbound context")
	}
	e.mu.Lock()
	c := e.contexts[ref.Id]
	e.mu.Unlock()
	created := false
	if c == nil {
		if ref.State != nil || peer.ApplicationLease() == nil {
			return nil, errors.New("outbound context unavailable")
		}
		app := peer.Application()
		incarnation := uuid.NewString()
		callID := e.id("outbound-call", app.InstanceId, app.Epoch, ref.Id, incarnation) + "@sipbridge"
		fullRef := e.contextRef(callID, ref.Id)
		fullRef.State.Incarnation = incarnation
		c = &residentContext{snapshot: &pb.ContextSnapshot{Context: fullRef, Application: proto.Clone(app).(*pb.ProcessRef), ApplicationLeaseTerm: peer.ApplicationLease().Term,
			Executor: proto.Clone(e.cfg.Process).(*pb.ProcessRef), CallId: callID, Kind: pb.ContextSnapshot_UAC, Transport: uacTransport(cmd.GetOpenUac())},
			transactions: make(map[string]*residentTransaction), requests: make(map[string]string), dialogs: make(map[string]*residentDialog), outputs: make(map[string]*residentOutput)}
		c.mu.Lock()
		if err := e.accountResidentLocked(c, residentContextBytes(c), 0); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		limit := residentSmallBudget
		if cmd.GetOpenUac().Method == "" || cmd.GetOpenUac().Method == "INVITE" {
			limit = residentInviteBudget
		} else if cmd.GetOpenUac().GetNetwork().GetCredential() != nil {
			limit = residentAuthenticatedBudget
		}
		var budgetErr error
		c.budget, budgetErr = e.openResidentBudgetLocked(c, limit)
		if budgetErr != nil {
			_ = e.accountResidentLocked(c, -c.bytes, 0)
			c.mu.Unlock()
			return nil, budgetErr
		}
		e.mu.Lock()
		lease := e.leases[residentControllerKey(app)]
		if e.closed || lease == nil || lease.term != c.snapshot.ApplicationLeaseTerm || !time.Now().Before(lease.expires) || e.contexts[ref.Id] != nil || len(e.contexts)+len(e.admissions) >= e.maxContexts {
			e.mu.Unlock()
			_ = e.accountResidentLocked(c, -c.bytes, -c.reserved)
			c.mu.Unlock()
			return nil, errors.New("outbound context capacity or identity conflict")
		}
		e.contexts[ref.Id] = c
		e.indexResidentOwnerUnderEngineLock(c)
		e.mu.Unlock()
		created = true
	} else {
		c.mu.Lock()
	}
	e.mu.Lock()
	current := e.contexts[ref.Id] == c
	e.mu.Unlock()
	if !current {
		c.mu.Unlock()
		return nil, errors.New("outbound context changed")
	}
	receipt, policy, replay, err := e.admitResidentCommandLocked(c, peer, cmd, operation, deadline)
	if err != nil {
		if created {
			e.mu.Lock()
			delete(e.contexts, ref.Id)
			if owners := e.owners[c.ownerKey]; owners != nil {
				delete(owners, c)
				if len(owners) == 0 {
					delete(e.owners, c.ownerKey)
				}
			}
			e.mu.Unlock()
			_ = e.accountResidentLocked(c, -c.bytes, -c.reserved)
		}
		c.mu.Unlock()
		return nil, err
	}
	if replay {
		c.mu.Unlock()
		return awaitResidentReplay(ctx, c, receipt)
	}
	refuse := func(err error) (*pb.OperationResult, error) {
		if created && len(c.transactions) == 0 {
			e.closeResidentBudgetLocked(c, c.budget)
			c.snapshot.Ended = true
			e.scheduleResidentContextRetirementLocked(c)
		}
		finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()})
		result := residentCommandResultLocked(c, receipt)
		if receipt.originalPending && receipt.registered {
			c.receiptPins--
		}
		receipt.originalPending = false
		releaseResidentCommandWireLocked(c, receipt)
		releaseResidentCommandLocked(c, receipt)
		if c.retireReady {
			e.afterResidentOutputReleaseLocked(c)
		}
		c.mu.Unlock()
		return result, nil
	}
	if len(c.transactions) != 0 || cmd.Sequence != 1 {
		return refuse(errors.New("outbound context already used"))
	}
	open := proto.Clone(cmd.GetOpenUac()).(*pb.OpenUac)
	fullRef := proto.Clone(c.snapshot.Context).(*pb.ContextRef)
	app := proto.Clone(c.snapshot.Application).(*pb.ProcessRef)
	var flow *pb.FlowRef
	if policy.SelectedFlow != nil {
		flow = proto.Clone(policy.SelectedFlow).(*pb.FlowRef)
	}
	c.mu.Unlock()
	if flow == nil {
		if e.cfg.OpenNetwork == nil {
			err = errors.New("network target unavailable")
		} else {
			network := open.Network
			if network == nil {
				network = &pb.NetworkTarget{Transport: open.Transport}
			}
			flow, err = e.cfg.OpenNetwork(ctx, open.Uri, network)
		}
	}
	c.mu.Lock()
	if err != nil {
		return refuse(err)
	}
	if flow == nil {
		return refuse(errors.New("network flow unavailable"))
	}
	e.mu.Lock()
	lease := e.leases[residentControllerKey(app)]
	live := !e.closed && e.contexts[ref.Id] == c && lease != nil && lease.term == c.snapshot.ApplicationLeaseTerm && time.Now().Before(lease.expires)
	e.mu.Unlock()
	if c.command != receipt || receipt.finished || !live || !time.Now().Before(deadline) || time.Now().UnixMilli() >= int64(policy.ExpiresAtMs) {
		return refuse(errors.New("outbound admission expired or changed"))
	}
	tx, effects, err := e.prepareResidentClientStart(open, fullRef, app, flow, operation, time.Now())
	if err != nil {
		return refuse(err)
	}
	observed, err := Observe(tx.native.Origin.Wire, true)
	if err != nil {
		return refuse(err)
	}
	parsed, err := tx.native.Origin.Parse()
	if err != nil {
		return refuse(err)
	}
	tx.command = receipt
	tx.policy = proto.Clone(policy).(*pb.CommandAdmission)
	tx.budget = c.budget
	tx.deadline = time.UnixMilli(int64(policy.ExpiresAtMs))
	if open.Network != nil && open.Network.Credential != nil {
		tx.credential = proto.Clone(open.Network.Credential).(*pb.CredentialRef)
	}
	tx.selectors = append([]string(nil), observed.Selectors...)
	tx.charged = residentTransactionBytes(tx)
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	newFlow := proto.Clone(flow).(*pb.FlowRef)
	snapshot.BindingId, snapshot.PrincipalId = open.BindingId, open.PrincipalId
	snapshot.LocalTag = tag(parsed.From().Params)
	snapshot.LocalUri, snapshot.RemoteUri = parsed.From().Address.String(), parsed.To().Address.String()
	snapshot.LocalSdp = append([]byte(nil), open.Body...)
	snapshot.Selectors = append([]string(nil), observed.Selectors...)
	if open.Method != "" && open.Method != "INVITE" {
		snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	}
	contextDelta := residentContextBytes(&residentContext{snapshot: snapshot, flow: newFlow}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
	contextCharge := max(int64(0), contextDelta)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		return refuse(err)
	}
	contextBorrowed, err := e.chargeResidentBudgetLocked(c, c.budget, contextCharge)
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return refuse(err)
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, policy, operation, deadline)
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, c.budget, contextCharge, contextBorrowed)
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return refuse(err)
	}
	retainResidentCommandLocked(tx.command)
	c.transactions[tx.native.Key] = tx
	c.flow, c.snapshot = newFlow, snapshot
	c.borrowed += contextBorrowed
	if contextDelta < 0 {
		released := -contextDelta
		borrowed := min(c.borrowed, released)
		_ = e.refundResidentBudgetLocked(c, c.budget, released, borrowed)
		c.borrowed -= borrowed
	}
	e.mu.Lock()
	for _, selector := range observed.Selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	if c.snapshot.Kind == pb.ContextSnapshot_UAC {
		finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Request: observed})
	}
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	return awaitResidentCommand(ctx, c, receipt)
}

func (e *ResidentMemory) commandResidentExistingClient(ctx context.Context, peer *control.Peer, cmd *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	c, err := e.findContext(cmd.Context)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if !e.residentContextCurrentLocked(c, cmd.Context) {
		c.mu.Unlock()
		return nil, state.ErrStale
	}
	receipt, policy, replay, err := e.admitResidentCommandLocked(c, peer, cmd, operation, deadline)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if replay {
		c.mu.Unlock()
		return awaitResidentReplay(ctx, c, receipt)
	}
	refuse := func(err error) (*pb.OperationResult, error) {
		finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: err.Error()})
		result := residentCommandResultLocked(c, receipt)
		if receipt.originalPending && receipt.registered {
			c.receiptPins--
		}
		receipt.originalPending = false
		releaseResidentCommandWireLocked(c, receipt)
		releaseResidentCommandLocked(c, receipt)
		if c.retireReady {
			e.afterResidentOutputReleaseLocked(c)
		}
		c.mu.Unlock()
		return result, nil
	}
	if cmd.GetCancel() != nil {
		selectedDialog := c.dialogs[c.selectedDialog]
		if selectedDialog != nil && (selectedDialog.native.LocalInvite == nil || selectedDialog.native.LocalInvite.State != portable.InvitePending) {
			dialog := selectedDialog
			if dialog.native.LocalInvite == nil {
				return refuse(errors.New("cancellable INVITE unavailable"))
			}
			var ack *residentOutput
			for _, output := range c.outputs {
				if output.dialog == c.selectedDialog && output.method == "ACK" && output.ackSequence == dialog.native.LocalInvite.Sequence && (output.state == residentOutputAuthorized || output.state == residentOutputStarted || output.state == residentOutputUnknown || output.state == residentOutputWritten) {
					ack = output
					if output.state == residentOutputWritten {
						break
					}
				}
			}
			if !dialog.ackWritten && ack == nil && dialog.native.Offer != portable.RemoteOfferInACK {
				return refuse(errors.New("ACK evidence unavailable"))
			}
			next, effects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogCancel, Now: time.Now().UnixMilli()})
			if err != nil {
				return refuse(err)
			}
			delta := residentDialogBytes(next) - residentDialogBytes(dialog.native)
			growth, err := e.prepareResidentClientGrowth(c, dialog.budget, delta)
			if err != nil {
				return refuse(err)
			}
			prior := dialog.native
			dialog.native = next
			var writes []*pb.FlowWrite
			if dialog.ackWritten {
				if dialog.completionFlow == nil {
					dialog.native = prior
					e.rollbackResidentClientGrowth(c, growth)
					return refuse(errors.New("written ACK flow unavailable"))
				}
				flow := dialog.completionFlow
				proof := &residentOutput{method: "ACK", dialog: c.selectedDialog, ackSequence: dialog.native.LocalInvite.Sequence, cleanupAfterACK: true, write: &pb.FlowWrite{Flow: flow}}
				writes, _, err = e.residentClientOutputCompletedLocked(c, proof, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN})
				if err != nil {
					dialog.native = prior
					e.rollbackResidentClientGrowth(c, growth)
					return refuse(err)
				}
			} else {
				for _, pending := range c.outputs {
					if pending.dialog == c.selectedDialog && pending.method == "ACK" && pending.ackSequence == dialog.native.LocalInvite.Sequence {
						pending.cleanupAfterACK = true
					}
				}
			}
			e.commitResidentClientGrowth(c, growth, &dialog.borrowed)
			c.cancelled = true
			_ = e.scheduleResidentDialogLocked(c, c.selectedDialog, effects)
			finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED})
			c.mu.Unlock()
			e.dispatchResident(ctx, writes)
			return awaitResidentCommand(ctx, c, receipt)
		}
		var invite *residentTransaction
		for _, candidate := range c.transactions {
			if candidate.native.Family == portable.InviteClient && (candidate.native.Phase == portable.Calling || candidate.native.Phase == portable.Proceeding) {
				if invite != nil {
					return refuse(errors.New("multiple active INVITE transactions"))
				}
				invite = candidate
			}
		}
		if invite == nil {
			return refuse(errors.New("cancellable INVITE unavailable"))
		}
		writes, err := e.residentCancelInviteLocked(c, invite, deadline)
		if err != nil {
			return refuse(err)
		}
		c.cancelled = true
		finishResidentCommandLocked(c, receipt, &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED})
		c.mu.Unlock()
		e.dispatchResident(ctx, writes)
		return awaitResidentCommand(ctx, c, receipt)
	}
	if cmd.GetDialogRequest() == nil {
		return refuse(errors.New("resident client command unavailable"))
	}
	if cmd.GetDialogRequest().Method == "ACK" {
		writes, err := e.residentAnswerACKLocked(c, cmd.GetDialogRequest(), receipt, policy, operation, deadline)
		if err != nil {
			return refuse(err)
		}
		c.mu.Unlock()
		e.dispatchResident(ctx, writes)
		return awaitResidentCommand(ctx, c, receipt)
	}
	var sub *stateSubscription
	var previousLocalTransaction string
	var nextDialog portable.Dialog
	var tx *residentTransaction
	var effects []portable.Effect
	if c.subscription != nil {
		sub, tx, effects, err = e.prepareResidentNotify(c, cmd.GetDialogRequest(), operation, time.Now())
	} else {
		dialog := c.dialogs[c.selectedDialog]
		if dialog == nil {
			return refuse(errors.New("dialog unavailable"))
		}
		request := cmd.GetDialogRequest()
		if request.Method == "INVITE" {
			previousLocalTransaction = dialog.localTransaction
		}
		var dialogEffects []portable.Effect
		nextDialog, dialogEffects, err = portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: time.Now().UnixMilli(), Method: sip.RequestMethod(request.Method), Body: request.Body,
			Branch: sip.RFC3261BranchMagicCookie + e.id("branch", c.snapshot.Context.State.Incarnation, operation)[:32], Headers: stateHeaders(request.Headers), Offer: request.Method == "INVITE" && len(request.Body) > 0})
		if err == nil {
			var origin *portable.Message
			for _, effect := range dialogEffects {
				if effect.Kind != portable.StartTransaction || effect.Message == nil || origin != nil {
					err = errors.New("unsupported dialog effects")
					break
				}
				origin = effect.Message
			}
			if err == nil && origin == nil {
				err = errors.New("dialog transaction unavailable")
			}
			if err == nil {
				family := portable.NonInviteClient
				if request.Method == "INVITE" {
					family = portable.InviteClient
				}
				var native portable.Transaction
				native, err = portable.NewTransaction(family, *origin, portable.DefaultTimers(), "")
				if err == nil {
					native, effects, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: time.Now().UnixMilli()})
					tx = &residentTransaction{native: native, flow: proto.Clone(c.flow).(*pb.FlowRef), application: proto.Clone(c.snapshot.Application).(*pb.ProcessRef)}
				}
			}
		}
	}
	if err != nil {
		return refuse(err)
	}
	if policy != nil && policy.SelectedFlow != nil {
		tx.flow = proto.Clone(policy.SelectedFlow).(*pb.FlowRef)
	}
	observed, err := Observe(tx.native.Origin.Wire, true)
	if err != nil {
		return refuse(err)
	}
	tx.command = receipt
	if policy != nil {
		tx.policy = proto.Clone(policy).(*pb.CommandAdmission)
	}
	tx.selectors = append([]string(nil), observed.Selectors...)
	required := c.subscription != nil && c.requiredNotifyBudget != nil
	if required {
		tx.budget = c.requiredNotifyBudget
	} else {
		tx.budget, err = e.openResidentBudgetLocked(c, residentSmallBudget)
		if err != nil {
			return refuse(err)
		}
	}
	receipt.budget = tx.budget
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		if !required {
			e.closeResidentBudgetLocked(c, tx.budget)
		}
		return refuse(err)
	}
	var growth residentClientGrowth
	var growthOwned *int64
	if sub != nil {
		growth, err = e.prepareResidentClientGrowth(c, c.subscriptionBudget, residentSubscriptionBytes(sub)-residentSubscriptionBytes(c.subscription))
		growthOwned = &c.subscriptionBorrowed
	} else {
		dialog := c.dialogs[c.selectedDialog]
		delta := residentDialogBytes(nextDialog) - residentDialogBytes(dialog.native)
		if cmd.GetDialogRequest().Method == "INVITE" {
			delta += int64(len(tx.native.Key) - len(dialog.localTransaction))
		}
		growth, err = e.prepareResidentClientGrowth(c, dialog.budget, delta)
		growthOwned = &dialog.borrowed
	}
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		if !required {
			e.closeResidentBudgetLocked(c, tx.budget)
		}
		return refuse(err)
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, policy, operation, deadline)
	if err != nil {
		e.rollbackResidentClientGrowth(c, growth)
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		if !required {
			e.closeResidentBudgetLocked(c, tx.budget)
		}
		return refuse(err)
	}
	if required {
		c.requiredNotifyBudget = nil
	}
	e.commitResidentClientGrowth(c, growth, growthOwned)
	if sub != nil {
		c.subscription = sub
	} else {
		c.dialogs[c.selectedDialog].native = nextDialog
		if cmd.GetDialogRequest().Method == "INVITE" {
			c.dialogs[c.selectedDialog].ackWritten = false
			c.dialogs[c.selectedDialog].localTransaction = tx.native.Key
		}
	}
	retainResidentCommandLocked(tx.command)
	c.transactions[tx.native.Key] = tx
	if previousLocalTransaction != "" && previousLocalTransaction != tx.native.Key {
		e.retireResidentTransactionLocked(c, c.transactions[previousLocalTransaction])
	}
	e.mu.Lock()
	for _, selector := range observed.Selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	c.mu.Unlock()
	e.dispatchResident(ctx, writes)
	return awaitResidentCommand(ctx, c, receipt)
}
