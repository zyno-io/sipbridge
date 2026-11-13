package engine

import (
	"context"
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func residentOwnerIndexKey(app *pb.ProcessRef, term string) string {
	return residentControllerKey(app) + "\x00" + term
}

// Context lock held; e.mu is acquired only after the context is selected.
func (e *ResidentMemory) indexResidentOwnerLocked(c *residentContext) {
	e.mu.Lock()
	e.indexResidentOwnerUnderEngineLock(c)
	e.mu.Unlock()
}

// Both c.mu and e.mu are held for initial publication, so expiry cannot pass
// between lease validation and insertion into its owner index.
func (e *ResidentMemory) indexResidentOwnerUnderEngineLock(c *residentContext) {
	key := residentOwnerIndexKey(c.snapshot.Application, c.snapshot.ApplicationLeaseTerm)
	if c.ownerKey == key {
		return
	}
	if previous := e.owners[c.ownerKey]; previous != nil {
		delete(previous, c)
		if len(previous) == 0 {
			delete(e.owners, c.ownerKey)
		}
	}
	if e.owners == nil {
		e.owners = make(map[string]map[*residentContext]struct{})
	}
	if e.owners[key] == nil {
		e.owners[key] = make(map[*residentContext]struct{})
	}
	e.owners[key][c] = struct{}{}
	c.ownerKey = key
}

func (e *ResidentMemory) expireResidentOwner(app *pb.ProcessRef, term string) {
	key := residentOwnerIndexKey(app, term)
	e.mu.Lock()
	owned := e.owners[key]
	contexts := make([]*residentContext, 0, len(owned))
	for c := range owned {
		contexts = append(contexts, c)
	}
	delete(e.owners, key)
	e.mu.Unlock()
	for _, c := range contexts {
		e.loseResidentOwner(c, key)
	}
}

func (e *ResidentMemory) loseResidentOwner(c *residentContext, key string) {
	c.mu.Lock()
	if c.ownerKey != key || c.snapshot == nil || c.ownerLost {
		c.mu.Unlock()
		return
	}
	e.mu.Lock()
	if owners := e.owners[key]; owners != nil {
		delete(owners, c)
		if len(owners) == 0 {
			delete(e.owners, key)
		}
	}
	e.mu.Unlock()
	c.ownerKey = ""
	c.ownerLost = true
	c.revision++
	c.snapshot.Revision++
	// Subscriptions retain their native expiry and may be adopted by a new processor.
	if c.subscription != nil {
		c.mu.Unlock()
		return
	}
	c.cancelled = true
	c.ownerCleanupAt = time.Now().Add(2 * 64 * time.Duration(portable.DefaultTimers().T1) * time.Millisecond)
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentOwnerCleanupDeadline, contextID: c.snapshot.Context.Id, record: c.snapshot.Context.GetState().GetIncarnation()}, uint64(c.ownerCleanupAt.UnixMilli()), c.ownerCleanupAt)
	e.mu.Unlock()
	e.wakeDeadline()
	var writes []*pb.FlowWrite
	for _, tx := range c.transactions {
		if tx.native.Family == portable.InviteClient && !tx.native.Cancelled && (tx.native.Phase == portable.Calling || tx.native.Phase == portable.Proceeding) {
			staged, err := e.residentCancelInviteLocked(c, tx, time.Now().Add(2*time.Second))
			if err == nil {
				writes = append(writes, staged...)
			}
		}
	}
	for dialogKey, dialog := range c.dialogs {
		if dialog.native.LocalInvite != nil && !dialog.ackWritten {
			for _, output := range c.outputs {
				if output.method == "ACK" && output.dialog == dialogKey {
					output.cleanupAfterACK = true
				}
			}
		}
		staged, err := e.residentOwnerByeLocked(c, dialogKey, dialog, false)
		if err == nil {
			writes = append(writes, staged...)
		}
	}
	var requests []string
	for _, tx := range c.transactions {
		if tx.requestID != "" && (tx.native.Family == portable.InviteServer || tx.native.Family == portable.NonInviteServer) {
			requests = append(requests, tx.requestID)
		}
	}
	c.mu.Unlock()
	e.dispatchResidentTimerOutputs(writes)
	for _, request := range requests {
		_ = e.failResidentPresentation(c, request)
	}
}

// At the fixed owner cleanup bound, retain independent uncertain physical and
// active native timer obligations. Their own completion re-enters this helper.
func (e *ResidentMemory) finishResidentOwnerCleanupLocked(c *residentContext) {
	if !c.ownerLost || c.subscription != nil || c.ownerCleanupAt.IsZero() || time.Now().Before(c.ownerCleanupAt) {
		return
	}
	for _, output := range c.outputs {
		if output.state == residentOutputStarted || output.state == residentOutputUnknown {
			return
		}
	}
	for _, tx := range c.transactions {
		if !tx.retired {
			for _, timer := range tx.native.Timers {
				if timer.Active {
					return
				}
			}
		}
	}
	for _, dialog := range c.dialogs {
		for _, timer := range dialog.native.Timers {
			if timer.Active {
				return
			}
		}
	}
	for _, tx := range c.transactions {
		if tx.retired || tx.native.Phase == portable.Terminated {
			continue
		}
		next, effects, err := portable.StepTransaction(tx.native, portable.Input{Kind: portable.Terminate, Now: time.Now().UnixMilli(), Error: "application owner expired"})
		if err != nil {
			return
		}
		delta, borrowed, err := e.stageResidentTransactionBytesLocked(c, tx, next)
		if err != nil {
			return
		}
		for _, effect := range effects {
			if effect.Kind == portable.Send || effect.Kind == portable.Respond {
				e.abortResidentTransactionBytesLocked(c, tx, delta, borrowed)
				return
			}
		}
		tx.native = next
		e.commitResidentTransactionBytesLocked(c, tx, delta, borrowed)
		_ = e.scheduleResidentTransactionLocked(c, tx, effects)
	}
	for key, dialog := range c.dialogs {
		if dialog.native.Phase == portable.DialogEnded {
			continue
		}
		next, effects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogTerminate, Now: time.Now().UnixMilli()})
		if err != nil {
			return
		}
		dialog.native = next
		_ = e.scheduleResidentDialogLocked(c, key, effects)
	}
	c.snapshot.Ended = true
	c.revision++
	c.snapshot.Revision++
	c.ownerCleanupAt = time.Time{}
	e.scheduleResidentContextRetirementLocked(c)
}

func (e *ResidentMemory) fireResidentOwnerCleanup(item residentDeadline) bool {
	if item.key.kind != residentOwnerCleanupDeadline {
		return false
	}
	e.mu.Lock()
	c := e.contexts[item.key.contextID]
	e.mu.Unlock()
	if c == nil {
		return true
	}
	c.mu.Lock()
	if c.snapshot.GetContext().GetState().GetIncarnation() == item.key.record && e.residentContextCurrentLocked(c, c.snapshot.Context) && !c.ownerCleanupAt.IsZero() && uint64(c.ownerCleanupAt.UnixMilli()) == item.generation {
		e.finishResidentOwnerCleanupLocked(c)
	}
	c.mu.Unlock()
	return true
}

// A cleanup BYE is native continuation funded by the original dialog budget.
// UAC cleanup requires positive local ACK-write evidence first.
func (e *ResidentMemory) residentOwnerByeLocked(c *residentContext, key string, dialog *residentDialog, timeout bool) ([]*pb.FlowWrite, error) {
	if (dialog.native.Phase != portable.DialogConfirmed && !(timeout && dialog.native.Phase == portable.DialogTerminating)) || dialog.native.LocalBye != nil {
		return nil, nil
	}
	if dialog.native.LocalInvite != nil && !dialog.ackWritten {
		return nil, nil
	}
	if remote := dialog.native.RemoteInvite; !timeout && remote != nil && remote.State == portable.InviteAwaitingACK {
		return nil, nil
	}
	now := time.Now()
	next, effects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogLocalRequest, Now: now.UnixMilli(), Method: sip.BYE, Branch: sip.RFC3261BranchMagicCookie + e.id("owner-loss-bye", c.snapshot.Context.State.Incarnation, key)[:32]})
	if err != nil {
		return nil, err
	}
	var origin *portable.Message
	for _, effect := range effects {
		if effect.Kind == portable.StartTransaction {
			origin = effect.Message
			break
		}
	}
	if origin == nil {
		return nil, errors.New("owner cleanup BYE unavailable")
	}
	native, err := portable.NewTransaction(portable.NonInviteClient, *origin, portable.DefaultTimers(), "")
	if err != nil {
		return nil, err
	}
	started, txEffects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return nil, err
	}
	observed, err := Observe(origin.Wire, true)
	if err != nil {
		return nil, err
	}
	cleanupFlow := c.flow
	if dialog.completionFlow != nil {
		cleanupFlow = dialog.completionFlow
	}
	tx := &residentTransaction{native: started, flow: proto.Clone(cleanupFlow).(*pb.FlowRef), application: proto.Clone(c.snapshot.Application).(*pb.ProcessRef), cleanup: true, budget: dialog.budget, selectors: append([]string(nil), observed.Selectors...)}
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		return nil, err
	}
	growth, err := e.prepareResidentClientGrowth(c, dialog.budget, residentDialogBytes(next)-residentDialogBytes(dialog.native))
	if err != nil {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return nil, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, txEffects, nil, "", now.Add(2*time.Second))
	if err != nil {
		e.rollbackResidentClientGrowth(c, growth)
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		return nil, err
	}
	dialog.native = next
	e.commitResidentClientGrowth(c, growth, &dialog.borrowed)
	c.transactions[tx.native.Key] = tx
	e.mu.Lock()
	for _, selector := range tx.selectors {
		e.transactions[selector] = c
	}
	e.mu.Unlock()
	_ = e.scheduleResidentTransactionLocked(c, tx, txEffects)
	return writes, nil
}

// replaceResidentCompletionFlowLocked records physical native completion
// evidence without publishing a new controller-approved application route.
func (e *ResidentMemory) replaceResidentCompletionFlowLocked(c *residentContext, dialog *residentDialog, flow *pb.FlowRef) error {
	if !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
		return errors.New("invalid completion flow")
	}
	delta := residentFlowBytes(flow) - residentFlowBytes(dialog.completionFlow)
	var borrowed int64
	var err error
	if delta > 0 {
		borrowed, err = e.chargeResidentBudgetLocked(c, dialog.budget, delta)
		if err != nil {
			return err
		}
	}
	if delta < 0 {
		returned := min(dialog.completionBorrowed, -delta)
		if err = e.refundResidentBudgetLocked(c, dialog.budget, -delta, returned); err != nil {
			return err
		}
		dialog.completionBorrowed -= returned
	}
	dialog.completionBorrowed += borrowed
	dialog.completionFlow = proto.Clone(flow).(*pb.FlowRef)
	return nil
}

// Successful native ACK continuity is immediate. Route publication is a
// separate controller decision and may not overwrite any newer context state.
func (e *ResidentMemory) publishResidentAckFlow(c *residentContext, key string, flow *pb.FlowRef) {
	c.mu.Lock()
	dialog := c.dialogs[key]
	if dialog == nil || c.ownerLost || proto.Equal(c.flow, flow) || !proto.Equal(dialog.completionFlow, flow) {
		c.mu.Unlock()
		return
	}
	revision := c.revision
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	application := proto.Clone(c.snapshot.Application).(*pb.ProcessRef)
	proposed := proto.Clone(flow).(*pb.FlowRef)
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
	delivery, err := e.adoptResidentFlow(ctx, snapshot, proposed, application, false)
	cancel()
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ownerLost || c.revision != revision || c.dialogs[key] != dialog || !proto.Equal(dialog.completionFlow, proposed) || !sameProcess(c.snapshot.Application, application) {
		return
	}
	e.mu.Lock()
	lease := e.leases[residentControllerKey(application)]
	live := lease != nil && lease.term == c.snapshot.ApplicationLeaseTerm && time.Now().Before(lease.expires)
	e.mu.Unlock()
	if !live {
		return
	}
	candidate := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	candidate.Delivery = delivery
	delta := residentContextBytes(&residentContext{snapshot: candidate, flow: proposed}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
	var borrowed int64
	if delta > 0 {
		borrowed, err = e.chargeResidentBudgetLocked(c, dialog.budget, delta)
		if err != nil {
			return
		}
	}
	if delta < 0 {
		returned := min(c.borrowed, -delta)
		if err = e.refundResidentBudgetLocked(c, dialog.budget, -delta, returned); err != nil {
			return
		}
		c.borrowed -= returned
	}
	c.borrowed += borrowed
	c.snapshot = candidate
	c.flow = proposed
	c.revision++
	c.snapshot.Revision++
}

func (e *ResidentMemory) scheduleResidentAckFlowPublication(c *residentContext, key string, flow *pb.FlowRef) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	owned := proto.Clone(flow).(*pb.FlowRef)
	e.wg.Add(1)
	e.mu.Unlock()
	go func() { defer e.wg.Done(); e.publishResidentAckFlow(c, key, owned) }()
}
