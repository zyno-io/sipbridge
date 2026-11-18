package sipgo

import (
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/siptest"
	"github.com/stretchr/testify/require"
)

func identityDialog(t *testing.T) (*DialogServerSession, *DialogClientSession) {
	t.Helper()
	client := testClient(t, func(req *sip.Request) *sip.Response {
		return sip.NewResponseFromRequest(req, 200, "OK", nil)
	})
	invite, _, _ := createTestInvite(t, "sip:callee@example.com", "udp", "127.0.0.1:5090")
	invite.AppendHeader(&sip.ContactHeader{Address: sip.Uri{Host: "caller.example.com"}})
	ua := &DialogUA{Client: client}
	server, err := ua.ReadInvite(invite, siptest.NewServerTxRecorder(invite))
	require.NoError(t, err)
	server.InviteResponse = sip.NewResponseFromRequest(server.InviteRequest, 200, "OK", nil)
	caller := &DialogClientSession{Dialog: Dialog{
		InviteRequest: invite, InviteResponse: server.InviteResponse,
	}, UA: ua}
	caller.Init()
	return server, caller
}

func TestDialogDirectionalRequestIdentity(t *testing.T) {
	for _, role := range []string{"UAS", "UAC"} {
		for _, mutation := range []string{"call-id", "local-tag", "remote-tag", "swapped-tags", "missing-from", "missing-to", "empty-tag"} {
			t.Run(role+"/"+mutation, func(t *testing.T) {
				server, caller := identityDialog(t)
				req := newByeRequestUAC(server.InviteRequest, server.InviteResponse, nil)
				read := server.ReadRequest
				if role == "UAC" {
					from, to := req.To().AsFrom(), req.From().AsTo()
					req.ReplaceHeader(&from)
					req.ReplaceHeader(&to)
					read = caller.ReadRequest
				}
				req.CSeq().SeqNo++
				valid := req.Clone()
				switch mutation {
				case "call-id":
					id := sip.CallIDHeader("another-call")
					req.ReplaceHeader(&id)
				case "local-tag":
					req.To().Params.Add("tag", "wrong-local")
				case "remote-tag":
					req.From().Params.Add("tag", "wrong-remote")
				case "swapped-tags":
					local, _ := req.To().Params.Get("tag")
					remote, _ := req.From().Params.Get("tag")
					req.To().Params.Add("tag", remote)
					req.From().Params.Add("tag", local)
				case "missing-from":
					req.RemoveHeader("From")
				case "missing-to":
					req.RemoveHeader("To")
				case "empty-tag":
					req.From().Params.Add("tag", "")
				}
				require.ErrorIs(t, read(req, nil), ErrDialogOutsideDialog)
				require.NoError(t, read(valid, nil), "invalid identity must not consume CSeq")
			})
		}
	}
}

func TestDialogByeAndAckValidateIdentity(t *testing.T) {
	for _, method := range []sip.RequestMethod{sip.BYE, sip.ACK} {
		t.Run(string(method), func(t *testing.T) {
			server, caller := identityDialog(t)
			req := newByeRequestUAC(server.InviteRequest, server.InviteResponse, nil)
			req.Method = method
			req.CSeq().MethodName = method
			if method == sip.ACK {
				req.CSeq().SeqNo = server.InviteRequest.CSeq().SeqNo
			}
			req.From().Params.Add("tag", "wrong-remote")
			if method == sip.ACK {
				require.ErrorIs(t, server.ReadAck(req, nil), ErrDialogOutsideDialog)
			} else {
				require.ErrorIs(t, server.ReadBye(req, nil), ErrDialogOutsideDialog)
				from, to := req.To().AsFrom(), req.From().AsTo()
				req.ReplaceHeader(&from)
				req.ReplaceHeader(&to)
				require.ErrorIs(t, caller.ReadBye(req, nil), ErrDialogOutsideDialog)
			}
			require.Equal(t, sip.DialogState(0), server.LoadState())
			require.Equal(t, sip.DialogState(0), caller.LoadState())
		})
	}
}
