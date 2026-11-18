package engine

import (
	"errors"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// nativePrepareClientStart constructs one unpublished native client
// transaction. The caller owns authority, accounting, publication and I/O.
func nativePrepareClientStart(open *pb.OpenUac, ref *pb.ContextRef, flow *pb.FlowRef, tag, branch, contact, sentBy string, now time.Time) (portable.Transaction, []portable.Effect, error) {
	if open == nil || ref == nil || ref.State == nil || flow == nil {
		return portable.Transaction{}, nil, errors.New("client transaction authority required")
	}
	transport := uacTransport(open)
	if transport != "udp" && transport != "tcp" && transport != "tls" || flow.Transport != transport {
		return portable.Transaction{}, nil, errors.New("unsupported or mismatched transport")
	}
	method := sip.RequestMethod(open.Method)
	if method == "" {
		method = sip.INVITE
	}
	message, err := portable.BuildInitialRequest(portable.InitialRequest{Method: method, URI: open.Uri, CallID: ref.State.CallId, LocalTag: tag, Branch: branch, CSeq: 1, Advertise: sentBy, Contact: contact, Transport: transport, Headers: stateHeaders(open.Headers), Body: open.Body})
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	family := portable.NonInviteClient
	if method == sip.INVITE {
		family = portable.InviteClient
	}
	native, err := portable.NewTransaction(family, message, portable.DefaultTimers(), "")
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	return portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
}
