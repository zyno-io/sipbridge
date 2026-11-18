package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// forkResponse retains each native branch independently. A provisional response
// may create early state, but only the first successful initial INVITE can select
// the application dialog. Later successful branches keep their own ACK/BYE work.
func (r *stateReduction) forkResponse(c *stateContext, tx *stateTransaction, m portable.Message) (bool, error) {
	if tx.Native.Family != portable.InviteClient {
		return false, nil
	}
	response, err := m.Facts()
	if err != nil {
		return true, err
	}
	if response.IsRequest {
		return true, errors.New("INVITE response required")
	}
	origin, err := tx.Native.Origin.Facts()
	if err != nil {
		return true, err
	}
	initial := origin.ToTag == ""
	key := response.FromTag + "/" + response.ToTag
	if !initial {
		d, exists := c.Dialogs[key]
		if !exists {
			return true, errors.New("response dialog unavailable")
		}
		next, effects, err := portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: r.now, Message: m})
		if err != nil {
			return true, err
		}
		c.Dialogs[key] = next
		if err = r.dialogEffectsFor(c, key, effects); err != nil {
			return true, err
		}
		if !(response.Status >= 100 && response.Status < 200) {
			if (response.Status >= 200 && response.Status < 300) && key == c.Dialog {
				c.Snapshot.LocalSdp = stateMessageBody(tx.Native.Origin)
				c.Snapshot.RemoteSdp = stateMessageBody(m)
				c.Snapshot.RemoteTarget = next.Target
				r.touch(c)
			}
			c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Request: stateProjectMessage(tx.Native.Origin, true), Response: stateProjectMessage(m, false), Snapshot: stateSnapshot(c, r.engine.cfg.Process)}
		}
		return true, nil
	}
	if response.Status >= 300 {
		if c.Dialog == "" && !c.Snapshot.Ended {
			c.Snapshot.Ended = true
			c.RetireMS = r.now + 120000
			r.touch(c)
			r.eventNative(c, pb.DialogEvent_FAILED, m, tx.Command, response)
		}
		return true, nil
	}
	if response.Status <= 100 || response.ToTag == "" || !response.HasContact {
		if response.Status >= 200 && response.Status < 300 {
			return true, errors.New("successful INVITE requires remote tag and Contact")
		}
		r.eventNative(c, pb.DialogEvent_PROVISIONAL, m, tx.Command, response)
		return true, nil
	}
	d, exists := c.Dialogs[key]
	if c.DialogScopes == nil {
		c.DialogScopes = make(map[string]string)
	}
	if c.DialogScopes[key] == "" && tx.CompletionScope != "" {
		c.DialogScopes[key] = tx.CompletionScope
	}
	if !exists {
		if len(c.Dialogs) >= 16 {
			return true, errors.New("native dialog branch capacity")
		}
		d, err = portable.NewDialog(portable.UAC, tx.Native.Origin, m, portable.DefaultTimers())
		if err != nil {
			return true, err
		}
	}
	if !(response.Status >= 200 && response.Status < 300) {
		c.Dialogs[key] = d
		r.eventNative(c, pb.DialogEvent_PROVISIONAL, m, tx.Command, response)
		return true, nil
	}
	winning := c.Dialog == key
	if c.Dialog == "" && !c.Cancelled && !c.Snapshot.Ended {
		c.Dialog = key
		winning = true
	}
	var effects []portable.Effect
	if exists {
		d, effects, err = portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogReceiveResponse, Now: r.now, Message: m})
	} else {
		d, effects, err = portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogStart, Now: r.now})
	}
	if err != nil {
		return true, err
	}
	c.Dialogs[key] = d
	if !winning || c.Cancelled || c.Snapshot.Ended {
		// Cancellation marks the native branch as retiring before ACK. The
		// resulting NeedBYE is emitted once after ACK, with its own local CSeq.
		if d.LocalInvite != nil && !d.LocalInvite.CancelRequested && d.Phase != portable.DialogEnded {
			d, effects, err = portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogCancel, Now: r.now})
			if err != nil {
				return true, err
			}
		}
		c.Dialogs[key] = d
		return true, r.dialogEffectsFor(c, key, effects)
	}
	firstAnswer := !c.Snapshot.Answered
	c.Snapshot.Answered = true
	c.Snapshot.DialogId = sip.DialogIDMake(d.CallID, d.LocalTag, d.RemoteTag)
	c.Snapshot.LocalTag = d.LocalTag
	c.Snapshot.RemoteTag = d.RemoteTag
	c.Snapshot.RemoteTarget = d.Target
	c.Snapshot.RemoteSdp = stateMessageBody(m)
	r.index(c, m, false)
	r.touch(c)
	if err = r.dialogEffectsFor(c, key, effects); err != nil {
		return true, err
	}
	if firstAnswer {
		r.eventNative(c, pb.DialogEvent_ANSWERED, m, tx.Command, response)
	}
	return true, nil
}
