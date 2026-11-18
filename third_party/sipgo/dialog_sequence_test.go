package sipgo

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/siptest"
	"github.com/stretchr/testify/require"
)

func sequenceDialog(t *testing.T, initial uint32) (*DialogClientSession, *DialogServerSession) {
	t.Helper()
	client := testClient(t, func(req *sip.Request) *sip.Response {
		return sip.NewResponseFromRequest(req, 200, "OK", nil)
	})
	ua := &DialogUA{Client: client, ContactHDR: sip.ContactHeader{Address: sip.Uri{Host: "local.example.com"}}}
	invite := sip.NewRequest(sip.INVITE, sip.Uri{Host: "remote.example.com"})
	invite.AppendHeader(&sip.CSeqHeader{SeqNo: initial, MethodName: sip.INVITE})
	caller, err := ua.WriteInvite(context.Background(), invite)
	require.NoError(t, err)
	t.Cleanup(func() { caller.inviteTx.Terminate() })
	require.NoError(t, caller.WaitAnswer(context.Background(), AnswerOptions{}))
	server, err := ua.ReadInvite(invite, siptest.NewServerTxRecorder(invite))
	require.NoError(t, err)
	server.InviteResponse = sip.NewResponseFromRequest(server.InviteRequest, 200, "OK", nil)
	return caller, server
}

func sequenceRequest(method sip.RequestMethod, seq uint32) *sip.Request {
	req := sip.NewRequest(method, sip.Uri{Host: "local.example.com"})
	req.AppendHeader(&sip.CSeqHeader{SeqNo: seq, MethodName: method})
	return req
}

func TestDialogIndependentRemoteSequence(t *testing.T) {
	caller, server := sequenceDialog(t, 121787510)
	// The phone may start its independent in-dialog sequence at 2 even when
	// the INVITE sent by this UAC had a much larger CSeq.
	require.NoError(t, caller.Dialog.ReadRequest(sequenceRequest(sip.INVITE, 2), nil))
	require.NoError(t, caller.Dialog.ReadRequest(sequenceRequest(sip.INFO, 7), nil))
	require.ErrorIs(t, caller.Dialog.ReadRequest(sequenceRequest(sip.INFO, 7), nil), ErrDialogInvalidCseq)
	require.ErrorIs(t, caller.Dialog.ReadRequest(sequenceRequest(sip.BYE, 6), nil), ErrDialogInvalidCseq)
	require.NoError(t, caller.Dialog.ReadRequest(sequenceRequest(sip.ACK, 2), nil), "ACK follows its INVITE, not the latest non-INVITE")
	require.ErrorIs(t, caller.Dialog.ReadRequest(sequenceRequest(sip.ACK, 7), nil), ErrDialogInvalidCseq)
	require.NoError(t, caller.Dialog.ReadRequest(sequenceRequest(sip.CANCEL, 2), nil))
	require.NoError(t, caller.Dialog.ReadRequest(sequenceRequest(sip.BYE, 8), nil))
	require.Equal(t, uint32(121787510), caller.CSEQ(), "remote requests must not change local sequence")

	request := sip.NewRequest(sip.INFO, sip.Uri{Host: "remote.example.com"})
	server.buildReq(request)
	require.Equal(t, uint32(1), request.CSeq().SeqNo, "UAS chooses its own first local sequence")
	require.ErrorIs(t, server.Dialog.ReadRequest(sequenceRequest(sip.BYE, 121787510), nil), ErrDialogInvalidCseq)
	require.NoError(t, server.Dialog.ReadRequest(sequenceRequest(sip.BYE, 121787511), nil))
}

func TestDialogFirstRemoteZeroAndMalformedCSeq(t *testing.T) {
	caller, _ := sequenceDialog(t, 100)
	require.NoError(t, caller.Dialog.ReadRequest(sequenceRequest(sip.INFO, 0), nil))
	require.ErrorIs(t, caller.Dialog.ReadRequest(sequenceRequest(sip.INFO, 0), nil), ErrDialogInvalidCseq)
	req := sequenceRequest(sip.INFO, 1)
	req.CSeq().MethodName = sip.BYE
	require.ErrorIs(t, caller.Dialog.ReadRequest(req, nil), ErrDialogInvalidCseq)
	req.RemoveHeader("CSeq")
	require.ErrorIs(t, caller.Dialog.ReadRequest(req, nil), ErrDialogInvalidCseq)
}

func TestDialogLocalSequenceAndAck(t *testing.T) {
	caller, _ := sequenceDialog(t, 100)
	req := sip.NewRequest(sip.INFO, sip.Uri{Host: "remote.example.com"})
	caller.buildReq(req)
	require.Equal(t, uint32(101), req.CSeq().SeqNo)
	ack := newAckRequestUAC(caller.InviteRequest, caller.InviteResponse, nil)
	caller.buildReq(ack)
	require.Equal(t, uint32(100), ack.CSeq().SeqNo, "initial ACK must retain INVITE CSeq after INFO")
	require.Equal(t, uint32(101), caller.CSEQ())

	const count = 32
	sequences := make(chan uint32, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := sip.NewRequest(sip.INFO, sip.Uri{Host: "remote.example.com"})
			caller.buildReq(req)
			sequences <- req.CSeq().SeqNo
		}()
	}
	wg.Wait()
	close(sequences)
	seen := make(map[uint32]bool)
	for seq := range sequences {
		require.False(t, seen[seq], "concurrent request must allocate unique CSeq")
		seen[seq] = true
	}
	require.Equal(t, uint32(101+count), caller.CSEQ())
}

func TestDialogServerAckAfterRemoteInfo(t *testing.T) {
	_, server := sequenceDialog(t, 100)
	require.NoError(t, server.Dialog.ReadRequest(sequenceRequest(sip.INFO, 101), nil))
	ack := newAckRequestUAC(server.InviteRequest, server.InviteResponse, nil)
	require.NoError(t, server.ReadAck(ack, nil))
}

func TestDialogByeRemoteSequence(t *testing.T) {
	for _, role := range []string{"UAC", "UAS"} {
		t.Run(role, func(t *testing.T) {
			caller, server := sequenceDialog(t, 100)
			dialog := &caller.Dialog
			readBye := caller.ReadBye
			if role == "UAS" {
				dialog, readBye = &server.Dialog, server.ReadBye
			}
			require.NoError(t, dialog.ReadRequest(sequenceRequest(sip.INFO, 101), nil))
			bye := newByeRequestUAC(server.InviteRequest, server.InviteResponse, nil)
			if role == "UAC" {
				from, to := caller.InviteResponse.To().AsFrom(), caller.InviteRequest.From().AsTo()
				bye.ReplaceHeader(&from)
				bye.ReplaceHeader(&to)
			}
			bye.CSeq().SeqNo = 101
			require.ErrorIs(t, readBye(bye, nil), ErrDialogInvalidCseq)
			require.NotEqual(t, sip.DialogStateEnded, dialog.LoadState())
			bye.CSeq().SeqNo = 102
			bye.AppendHeader(sip.HeaderClone(caller.InviteRequest.Via()))
			tx := siptest.NewServerTxRecorder(bye)
			defer tx.Terminate()
			require.NoError(t, readBye(bye, tx))
			require.Equal(t, sip.DialogStateEnded, dialog.LoadState())
			require.Len(t, tx.Result(), 1)
		})
	}
}

func TestDialogLocalByeUsesIndependentSequence(t *testing.T) {
	for _, role := range []string{"UAC", "UAS"} {
		t.Run(role, func(t *testing.T) {
			caller, server := sequenceDialog(t, 100)
			dialog, bye, want := &caller.Dialog, caller.Bye, uint32(101)
			if role == "UAS" {
				dialog, bye, want = &server.Dialog, server.Bye, 1
			}
			dialog.setState(sip.DialogStateConfirmed)
			require.NoError(t, dialog.ReadRequest(sequenceRequest(sip.INFO, 200), nil))
			var sent *sip.Request
			caller.UA.Client.TxRequester = &siptest.ClientTxRequester{OnRequest: func(req *sip.Request) *sip.Response {
				sent = req.Clone()
				return sip.NewResponseFromRequest(req, 200, "OK", nil)
			}}
			require.NoError(t, bye(context.Background()))
			require.NotNil(t, sent)
			require.Equal(t, sip.BYE, sent.CSeq().MethodName)
			require.Equal(t, want, sent.CSeq().SeqNo)
		})
	}
}

func TestDialogRemoteCSeqRetransmissionStaysNative(t *testing.T) {
	ua, err := NewUA()
	require.NoError(t, err)
	t.Cleanup(func() { ua.Close() })
	server, err := NewServer(ua)
	require.NoError(t, err)
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	peer, err := net.Dial("udp", listener.LocalAddr().String())
	require.NoError(t, err)
	t.Cleanup(func() { peer.Close() })
	invite, _, _ := createTestInvite(t, "sip:callee@example.com", "udp", peer.LocalAddr().String())
	invite.CSeq().SeqNo = 1
	dialog := &Dialog{InviteRequest: invite}
	dialog.Init()
	var handled atomic.Int32
	server.OnInfo(func(req *sip.Request, tx sip.ServerTransaction) {
		handled.Add(1)
		code := 200
		if err := dialog.ReadRequest(req, tx); err != nil {
			code = 500
		}
		if err := tx.Respond(sip.NewResponseFromRequest(req, code, "Sequence result", nil)); err != nil {
			t.Error(err)
		}
	})
	go server.ServeUDP(listener)
	req := invite.Clone()
	req.Method, req.CSeq().MethodName = sip.INFO, sip.INFO
	req.CSeq().SeqNo = 2
	req.SetBody(nil)
	exchange := func(want int) {
		t.Helper()
		require.NoError(t, peer.SetDeadline(time.Now().Add(2*time.Second)))
		_, err := peer.Write([]byte(req.String()))
		require.NoError(t, err)
		buf := make([]byte, 4096)
		n, err := peer.Read(buf)
		require.NoError(t, err)
		message, err := sip.ParseMessage(buf[:n])
		require.NoError(t, err)
		require.Equal(t, want, message.(*sip.Response).StatusCode)
	}
	exchange(200)
	exchange(200) // The same branch returns the transaction's cached response.
	require.Equal(t, int32(1), handled.Load())
	req.Via().Params.Add("branch", sip.GenerateBranch())
	exchange(500) // A new transaction cannot reuse the accepted CSeq.
	req.Via().Params.Add("branch", sip.GenerateBranch())
	req.CSeq().SeqNo++
	exchange(200)
	require.Equal(t, int32(3), handled.Load())
}
