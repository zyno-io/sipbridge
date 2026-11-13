package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// residentAnswerACKLocked applies the controller's answer to a remote offer
// carried by an INVITE 2xx. The ACK remains attached to the original native
// INVITE; its application receipt is independently settled by its exact write.
func (e *ResidentMemory) residentAnswerACKLocked(c *residentContext, request *pb.DialogRequest, receipt *residentCommand, policy *pb.CommandAdmission, operation string, deadline time.Time) ([]*pb.FlowWrite, error) {
	dialog := c.dialogs[c.selectedDialog]
	if dialog == nil || dialog.native.LocalInvite == nil || dialog.native.Offer != portable.RemoteOfferInACK {
		return nil, errors.New("dialog has no pending ACK answer")
	}
	tx := c.transactions[dialog.localTransaction]
	if tx == nil || tx.native.Family != portable.InviteClient {
		return nil, errors.New("ACK INVITE unavailable")
	}
	receipt.budget = tx.budget
	next, effects, err := portable.StepDialog(dialog.native, portable.DialogInput{Kind: portable.DialogLocalACK, Now: time.Now().UnixMilli(), Body: request.Body,
		Headers: stateHeaders(request.Headers), Branch: sip.RFC3261BranchMagicCookie + e.id("answer-ack", c.snapshot.Context.State.Incarnation, c.selectedDialog, operation)[:32], Answer: true})
	if err != nil {
		return nil, err
	}
	snapshot := proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	snapshot.LocalSdp = append([]byte(nil), request.Body...)
	dialogDelta := residentDialogBytes(next) - residentDialogBytes(dialog.native)
	contextDelta := residentContextBytes(&residentContext{snapshot: snapshot, flow: c.flow}) - residentContextBytes(&residentContext{snapshot: c.snapshot, flow: c.flow})
	dialogGrowth, err := e.prepareResidentClientGrowth(c, dialog.budget, dialogDelta)
	if err != nil {
		return nil, err
	}
	contextGrowth, err := e.prepareResidentClientGrowth(c, c.budget, contextDelta)
	if err != nil {
		e.rollbackResidentClientGrowth(c, dialogGrowth)
		return nil, err
	}
	writes, err := e.stageResidentOutputsLocked(c, tx, effects, policy, operation, deadline)
	if err != nil || len(writes) == 0 {
		e.rollbackResidentClientGrowth(c, contextGrowth)
		e.rollbackResidentClientGrowth(c, dialogGrowth)
		if err == nil {
			err = errors.New("ACK produced no output")
		}
		return nil, err
	}
	dialog.native, c.snapshot = next, snapshot
	e.commitResidentClientGrowth(c, dialogGrowth, &dialog.borrowed)
	e.commitResidentClientGrowth(c, contextGrowth, &c.borrowed)
	for _, write := range writes {
		if output := c.outputs[write.Effect.Id]; output != nil && output.method == "ACK" {
			previous := output.command
			output.dialog, output.command = c.selectedDialog, receipt
			retainResidentCommandLocked(receipt)
			releaseResidentCommandLocked(c, previous)
			output.cleanupAfterACK = c.cancelled || c.ownerLost
		}
	}
	_ = e.scheduleResidentDialogLocked(c, c.selectedDialog, effects)
	return writes, nil
}
