package engine

import (
	"crypto/sha256"
	"sort"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// retireResidentTransactionLocked drops terminal REGISTER and private-refusal
// native wire while retaining independent compact output proofs and receipts.
func (e *ResidentMemory) retireResidentTransactionLocked(c *residentContext, tx *residentTransaction) {
	if tx == nil || tx.retired || tx.native.Phase != portable.Terminated {
		return
	}
	parsed, err := tx.native.Origin.Parse()
	if err != nil {
		return
	}
	request, ok := parsed.(*sip.Request)
	if !ok {
		return
	}
	// Initial client INVITEs anchor late successful forks; tagged in-dialog
	// procedures can retire once their dialog association is superseded.
	if tx.native.Family == portable.InviteClient && (request.To() == nil || tag(request.To().Params) == "") {
		return
	}
	// Dialog timers and successful INVITE obligations can outlive their
	// transaction phase. Keep any record still referenced by a live dialog.
	for _, dialog := range c.dialogs {
		if dialog.transaction == tx.native.Key || dialog.localTransaction == tx.native.Key {
			return
		}
	}
	if c.transactions[tx.native.Key] != tx {
		return
	}
	// Keep only an exact request disposition until the output proof cutoff;
	// dropping the selector here would admit the same request a second time.
	command := tx.command
	tx.command = nil
	releaseResidentCommandLocked(c, command)
	tx.wireHash = sha256.Sum256(tx.native.Origin.Wire)
	tx.native = portable.Transaction{Key: tx.native.Key}
	tx.policy, tx.application, tx.credential = nil, nil, nil
	tx.retired = true
	tx.retireAt = time.Now().Add(2 * time.Minute)
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentTransactionRetirementDeadline, contextID: c.snapshot.Context.Id, record: tx.native.Key}, uint64(tx.retireAt.UnixMilli()), tx.retireAt)
	e.mu.Unlock()
	e.wakeDeadline()
	// No native continuation can use this procedure's reservation now.
	// Retained exact output proofs carry their own charged bytes.
	shared := c.subscription != nil && tx.budget == c.subscriptionBudget
	for _, other := range c.transactions {
		if other != tx && !other.retired && other.budget == tx.budget {
			shared = true
			break
		}
	}
	for _, dialog := range c.dialogs {
		if dialog.budget == tx.budget && dialog.native.Phase != portable.DialogEnded {
			shared = true
			break
		}
	}
	if !shared {
		e.closeResidentBudgetLocked(c, tx.budget)
	}
	const dispositionCharge int64 = 1024
	if tx.charged > dispositionCharge {
		released := tx.charged - dispositionCharge
		borrowed := min(tx.borrowed, released)
		_ = e.refundResidentBudgetLocked(c, tx.budget, released, borrowed)
		tx.borrowed -= borrowed
		tx.charged = dispositionCharge
	}
	e.afterResidentOutputReleaseLocked(c)
}

// Called with c.mu after a compact proof is released or terminal transaction is
// retired. Unknown outcomes remain present and fence retirement.
func (e *ResidentMemory) afterResidentOutputReleaseLocked(c *residentContext) {
	if c.snapshot == nil || c.receiptPins > 0 || len(c.outputs) != 0 || len(c.events) != 0 || c.requiredNotifyBudget != nil {
		return
	}
	if (c.subscription != nil || len(c.dialogs) != 0) && (!c.retireReady || !c.snapshot.Ended) {
		return
	}
	// Early losing branches retain native identity through the late-final window.
	// Once that cutoff and every transaction deadline have passed, end those
	// branches without inventing an application lifecycle event.
	if c.retireReady && c.snapshot.Ended {
		terminal := true
		for _, tx := range c.transactions {
			if !tx.retired && tx.native.Phase != portable.Terminated {
				terminal = false
				break
			}
		}
		if terminal {
			for key, dialog := range c.dialogs {
				if dialog.native.Phase != portable.DialogEarly {
					continue
				}
				next, effects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogTerminate, Now: time.Now().UnixMilli()})
				if err != nil {
					return
				}
				growth, err := e.prepareResidentClientGrowth(c, dialog.budget, residentDialogBytes(next)-residentDialogBytes(dialog.native))
				if err != nil {
					return
				}
				dialog.native = next
				e.commitResidentClientGrowth(c, growth, &dialog.borrowed)
				_ = e.scheduleResidentDialogLocked(c, key, effects)
			}
		}
	}
	for _, dialog := range c.dialogs {
		if dialog.native.Phase != portable.DialogEnded {
			return
		}
		for _, deadline := range dialog.native.Timers {
			if deadline.Active {
				return
			}
		}
	}
	for _, tx := range c.transactions {
		if tx.retired && time.Now().Before(tx.retireAt) {
			return
		}
		if !tx.retired && ((!c.retireReady || !c.snapshot.Ended) || tx.native.Phase != portable.Terminated) {
			return
		}
	}
	for _, tx := range c.transactions {
		command := tx.command
		tx.command = nil
		releaseResidentCommandLocked(c, command)
	}
	latest := c.command
	c.command = nil
	releaseResidentCommandLocked(c, latest)
	e.closeResidentBudgetLocked(c, c.subscriptionBudget)
	e.closeResidentBudgetLocked(c, c.budget)
	e.mu.Lock()
	if e.contexts[c.snapshot.Context.Id] == c {
		for _, dialog := range c.dialogs {
			tags := []string{dialog.native.LocalTag, dialog.native.RemoteTag}
			sort.Strings(tags)
			selector := opaque("dlg:", dialog.native.CallID, tags[0], tags[1])
			if e.dialogs[selector] == c {
				delete(e.dialogs, selector)
			}
		}
		if c.subscription != nil {
			tags := []string{c.snapshot.LocalTag, c.snapshot.RemoteTag}
			sort.Strings(tags)
			selector := opaque("dlg:", c.snapshot.CallId, tags[0], tags[1])
			if e.dialogs[selector] == c {
				delete(e.dialogs, selector)
			}
		}
		for _, tx := range c.transactions {
			for _, selector := range tx.selectors {
				if e.transactions[selector] == c {
					delete(e.transactions, selector)
				}
			}
		}
		if owners := e.owners[c.ownerKey]; owners != nil {
			delete(owners, c)
			if len(owners) == 0 {
				delete(e.owners, c.ownerKey)
			}
		}
		delete(e.contexts, c.snapshot.Context.Id)
		e.bytes -= c.bytes
		e.reserved -= c.reserved
		c.bytes = 0
		c.reserved = 0
		c.snapshot.Ended = true
	}
	e.mu.Unlock()
}

// Terminal contexts retain their bounded disposition for the public replay window.
func (e *ResidentMemory) scheduleResidentContextRetirementLocked(c *residentContext) {
	at := time.Now().Add(2 * time.Minute)
	c.retireGeneration = uint64(at.UnixMilli())
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentContextRetirementDeadline, contextID: c.snapshot.Context.Id, record: c.snapshot.Context.GetState().GetIncarnation()}, c.retireGeneration, at)
	e.mu.Unlock()
	e.wakeDeadline()
}

// Retire one compact disposition independently of an otherwise live context.
// Exact unresolved physical evidence keeps its owning disposition fenced.
func (e *ResidentMemory) fireResidentTransactionRetirement(item residentDeadline) bool {
	if item.key.kind != residentTransactionRetirementDeadline {
		return false
	}
	e.mu.Lock()
	c := e.contexts[item.key.contextID]
	e.mu.Unlock()
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tx := c.transactions[item.key.record]
	if tx == nil || !tx.retired || uint64(tx.retireAt.UnixMilli()) != item.generation || time.Now().Before(tx.retireAt) {
		return true
	}
	for _, output := range c.outputs {
		if output.transaction == item.key.record {
			return true
		}
	}
	e.releaseResidentDispositionLocked(c, tx)
	e.afterResidentOutputReleaseLocked(c)
	return true
}

func (e *ResidentMemory) releaseResidentDispositionLocked(c *residentContext, tx *residentTransaction) {
	if c.transactions[tx.native.Key] != tx {
		return
	}
	key := tx.native.Key
	command := tx.command
	tx.command = nil
	releaseResidentCommandLocked(c, command)
	delete(c.transactions, key)
	for id, requestKey := range c.requests {
		if requestKey == key {
			delete(c.requests, id)
		}
	}
	e.mu.Lock()
	for _, selector := range tx.selectors {
		used := false
		for _, other := range c.transactions {
			for _, otherSelector := range other.selectors {
				if selector == otherSelector {
					used = true
					break
				}
			}
			if used {
				break
			}
		}
		if !used && e.transactions[selector] == c {
			delete(e.transactions, selector)
		}
	}
	e.deadlines.remove(residentDeadlineKey{kind: residentTransactionRetirementDeadline, contextID: c.snapshot.Context.Id, record: key})
	e.mu.Unlock()
	if tx.charged > 0 {
		_ = e.refundResidentBudgetLocked(c, tx.budget, tx.charged, tx.borrowed)
		tx.charged, tx.borrowed = 0, 0
	}
}

// Only the released proof's procedure needs reconsideration; other live
// transaction records are not scanned on ordinary output completion.
func (e *ResidentMemory) afterResidentTransactionOutputReleaseLocked(c *residentContext, key string) {
	tx := c.transactions[key]
	if tx == nil || !tx.retired || time.Now().Before(tx.retireAt) {
		return
	}
	for _, output := range c.outputs {
		if output.transaction == key {
			return
		}
	}
	e.releaseResidentDispositionLocked(c, tx)
}
