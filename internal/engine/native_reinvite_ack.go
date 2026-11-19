package engine

import (
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

// Native ACK completion never depends on a controller being available. Only
// optional application delivery requires its existing live session capability.
func supportsReinviteACK(server *control.Server, application *pb.ProcessRef, term string) bool {
	if server == nil {
		return false
	}
	peer, err := server.Peer(application)
	return err == nil && acceptsReinviteACK(peer, term)
}

func acceptsReinviteACK(peer *control.Peer, term string) bool {
	return peer != nil && term != "" && peer.ApplicationLease().GetTerm() == term && peer.HasCapability(control.ReinviteAckCapability)
}

func projectReinviteACK(event *pb.DialogEvent, message portable.Message) error {
	facts, err := message.Facts()
	if err != nil {
		return err
	}
	event.InviteCseq = facts.CSeq
	return nil
}

// Initial ACKNOWLEDGED keeps its original snapshot-only shape.
func isReinviteACKEvent(event *pb.DialogEvent) bool {
	return event != nil && event.Kind == pb.DialogEvent_ACKNOWLEDGED && event.Message != nil
}
