package engine

import (
	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/state"
)

// RetainedInspection contains only bounded classes/counts, never SIP identities
// or payloads. It is for offline diagnosis and grants no execution authority.
type RetainedInspection struct {
	Contexts, Transactions, Dialogs, Allocations, Retired                   int
	Answered, Ended, OwnerLost                                              int
	TransactionPhases, NativeTimers                                         map[string]int
	TransactionFamilies, TransactionMethods                                 map[string]int
	EarliestContextRetireMS, LatestDispositionUntilMS                       int64
	LatestReplayUntilMS, LatestTransactionRetireMS, LatestRespondDeadlineMS int64
	ContextRetirementGuards, TransactionRetirementGuards                    map[string]int
}

func InspectRetainedProtocol(data []byte) (RetainedInspection, error) {
	group, err := decodeStateGroup(state.Record{Payload: state.Payload{ProtocolState: data}, Versions: NativeStateVersions()})
	if err != nil {
		return RetainedInspection{}, err
	}
	result := RetainedInspection{Contexts: len(group.Contexts), Allocations: len(group.Allocations), Retired: len(group.Retired),
		TransactionPhases: make(map[string]int), NativeTimers: make(map[string]int),
		TransactionFamilies: make(map[string]int), TransactionMethods: make(map[string]int),
		ContextRetirementGuards: make(map[string]int), TransactionRetirementGuards: make(map[string]int)}
	for _, context := range group.Contexts {
		if context.Snapshot == nil {
			return RetainedInspection{}, state.ErrCorrupt
		}
		result.LatestReplayUntilMS = max(result.LatestReplayUntilMS, context.ReplayUntilMS)
		for name, present := range map[string]bool{
			"subscription": context.Subscription != nil, "completionScope": context.CompletionScope != "",
			"dialog": context.Dialog != "", "ownerCleanup": context.OwnerCleanupAtMS != 0,
			"ownerProbeEffect": context.OwnerProbeEffect != "",
		} {
			if present {
				result.ContextRetirementGuards[name]++
			}
		}
		result.Transactions += len(context.Transactions)
		result.Dialogs += len(context.Dialogs)
		if context.RetireMS > 0 && (result.EarliestContextRetireMS == 0 || context.RetireMS < result.EarliestContextRetireMS) {
			result.EarliestContextRetireMS = context.RetireMS
		}
		for _, tx := range context.Transactions {
			result.LatestTransactionRetireMS = max(result.LatestTransactionRetireMS, tx.RetireMS)
			result.LatestRespondDeadlineMS = max(result.LatestRespondDeadlineMS, tx.RespondDeadlineMS)
			for name, present := range map[string]bool{
				"lastResponse": tx.Native.LastResponse != nil, "written": tx.Written,
				"completionScope": tx.CompletionScope != "", "ownerDeferred": tx.OwnerDeferred,
				"deferredPresent": tx.DeferredPresent, "pendingResponse": tx.PendingResponse != nil,
				"finalResponse": tx.FinalResponse != nil, "digestEffect": tx.DigestEffect != "",
				"digestAttempts": tx.DigestAttempts != 0, "respondStatus": tx.RespondStatus != 0,
				"unprovenDisposition": tx.DispositionUntilMS <= 0,
			} {
				if present {
					result.TransactionRetirementGuards[name]++
				}
			}
			family := "unsupported"
			switch tx.Native.Family {
			case portable.InviteClient, portable.InviteServer, portable.NonInviteClient, portable.NonInviteServer:
				family = string(tx.Native.Family)
			}
			result.TransactionFamilies[family]++
			facts, err := tx.Native.Origin.Facts()
			if err != nil {
				return RetainedInspection{}, state.ErrCorrupt
			}
			method := "other"
			switch name := facts.CSeqMethod; name {
			case sip.INVITE, sip.ACK, sip.BYE, sip.CANCEL, sip.REGISTER, sip.SUBSCRIBE, sip.NOTIFY, sip.OPTIONS, sip.INFO, sip.REFER, sip.UPDATE, sip.PRACK, sip.MESSAGE, sip.PUBLISH:
				method = string(name)
			}
			result.TransactionMethods[method]++
			phase := "unsupported"
			switch tx.Native.Phase {
			case portable.Idle, portable.Calling, portable.Trying, portable.Proceeding, portable.Completed, portable.Accepted, portable.Confirmed, portable.Terminated:
				phase = string(tx.Native.Phase)
			}
			result.TransactionPhases[phase]++
			result.LatestDispositionUntilMS = max(result.LatestDispositionUntilMS, tx.DispositionUntilMS)
			for name, timer := range tx.Native.Timers {
				if !timer.Active {
					continue
				}
				class := "unsupported"
				switch name {
				case portable.TimerA, portable.TimerB, portable.TimerD, portable.TimerE, portable.TimerF, portable.TimerG, portable.TimerH,
					portable.TimerI, portable.TimerJ, portable.TimerK, portable.TimerL, portable.TimerM, portable.TimerTrying, portable.Timer2xx, portable.TimerACK, portable.TimerCancel:
					class = string(name)
				}
				result.NativeTimers[class]++
			}
		}
		if context.Snapshot.Answered {
			result.Answered++
		}
		if context.Snapshot.Ended {
			result.Ended++
		}
		if context.OwnerLost {
			result.OwnerLost++
		}
	}
	return result, nil
}
