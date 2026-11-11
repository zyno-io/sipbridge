package engine

import (
	"sort"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// transactionEnded supplies TU lifecycle transitions which are not transaction
// retransmission duties. The native dialog still owns each directional state.
func (r *stateReduction) transactionEnded(c *stateContext, tx *stateTransaction, failure string) error {
	serverResponse := tx.Native.Family == portable.InviteServer || tx.Native.Family == portable.NonInviteServer
	if !tx.Cleanup && (!serverResponse || failure != "") && tx.Command != "" && c.Operation == tx.Command && c.Result != nil && c.Result.Certainty == pb.OperationResult_ACCEPTED {
		c.Result.Certainty = pb.OperationResult_COMPLETED
		if serverResponse {
			c.Result.Certainty = pb.OperationResult_UNKNOWN
		}
		if failure != "" {
			c.Result.Error = failure
		}
	}
	origin, err := tx.Native.Origin.Parse()
	if err != nil {
		return err
	}
	if tx.Native.Family == portable.InviteClient && tag(origin.To().Params) == "" && !c.Snapshot.Answered && !c.Snapshot.Ended && c.Dialog == "" && tx.DigestEffect == "" {
		// Early branches cannot become application dialogs after the initial
		// procedure ends. Keep their exact origins for late 2xx ACK/BYE duties.
		c.Snapshot.Ended = true
		c.RetireMS = r.now + 120000
		r.touch(c)
		r.event(c, pb.DialogEvent_FAILED, nil, tx.Command, failure)
	}
	if tx.Native.Family != portable.NonInviteClient || origin.CSeq().MethodName != sip.BYE {
		return nil
	}
	key := tag(origin.From().Params) + "/" + tag(origin.To().Params)
	d, exists := c.Dialogs[key]
	if !exists || d.LocalBye == nil || !portable.MatchRequest(*d.LocalBye, tx.Native.Origin) {
		return nil
	}
	next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogTerminate, Now: r.now})
	if err != nil {
		return err
	}
	c.Dialogs[key] = next
	return r.dialogEffectsFor(c, key, effects)
}

// dueTimers gives hard protocol deadlines precedence over retransmission and
// generic recovery work when multiple generations are overdue after an outage.
func (e *StateEngine) dueTimers(record state.Record, now int64) []state.Timer {
	var due []state.Timer
	for _, timer := range record.Payload.Timers {
		if timer.DueMS <= now {
			due = append(due, timer)
		}
	}
	if len(due) == 0 {
		return nil
	}
	g, err := decodeStateGroup(record)
	if err != nil {
		e.scheduler.fail(schedulerDecodeFailure, err)
		return nil
	}
	priority := make(map[string]bool)
	r := stateReduction{engine: e}
	for id, c := range g.Contexts {
		for key, tx := range c.Transactions {
			for name := range tx.Native.Timers {
				if name != portable.TimerA && name != portable.TimerE && name != portable.TimerG && name != portable.TimerTrying {
					priority[r.timerID(id, "tx", key, name)] = true
				}
			}
		}
		for key := range c.Dialogs {
			priority[r.timerID(id, "dialog", key, portable.TimerACK)] = true
		}
	}
	sort.SliceStable(due, func(i, j int) bool {
		if priority[due[i].ID] != priority[due[j].ID] {
			return priority[due[i].ID]
		}
		return due[i].DueMS < due[j].DueMS
	})
	return due
}
