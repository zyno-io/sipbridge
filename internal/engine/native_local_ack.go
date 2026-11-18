package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// nativePrepareLocalAnswerACK applies the pinned dialog reducer to the
// controller's answer for a remote offer. It performs no publication or I/O.
func nativePrepareLocalAnswerACK(dialog portable.Dialog, request *pb.DialogRequest, branch string, nowMS int64) (portable.Dialog, []portable.Effect, error) {
	if request == nil || request.Method != "ACK" || dialog.LocalInvite == nil || dialog.Offer != portable.RemoteOfferInACK || branch == "" || nowMS <= 0 {
		return portable.Dialog{}, nil, errors.New("dialog has no pending ACK answer")
	}
	return portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogLocalACK, Now: nowMS, Body: request.Body, Headers: stateHeaders(request.Headers), Branch: branch, Answer: true})
}
