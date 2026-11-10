package sip

import (
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

type responseMatchConnection struct{}

func (*responseMatchConnection) LocalAddr() net.Addr    { return &net.TCPAddr{} }
func (*responseMatchConnection) Ref(int) int            { return 1 }
func (*responseMatchConnection) TryClose() (int, error) { return 1, nil }
func (*responseMatchConnection) Close() error           { return nil }
func (*responseMatchConnection) WriteMsg(Message) error { return nil }

func TestTransactionLayerMatchClientResponse(t *testing.T) {
	transport := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	layer := NewTransactionLayer(transport)
	t.Cleanup(func() { layer.Close(); transport.Close() })
	request, _, _ := testCreateInvite(t, "sip:bob@example.test", "TLS", "127.0.0.1:5061")
	key, err := ClientTxKeyMake(request)
	require.NoError(t, err)
	tx := NewClientTx(key, request, &responseMatchConnection{}, slog.Default())
	layer.clientTransactions.items[key] = tx
	tx.OnTerminate(layer.clientTxTerminate)
	response := NewResponseFromRequest(request, 200, "OK", nil)
	response.SetSource("192.0.2.2:51999")
	response.Via().Params.Add("received", "192.0.2.2")
	response.Via().Params.Add("rport", "51999")
	require.True(t, layer.MatchClientResponse(response), "physical response source may change")
	for name, mutate := range map[string]func(*Response){
		"call-id":         func(r *Response) { id := CallIDHeader("borrowed-call"); r.ReplaceHeader(&id) },
		"from-tag":        func(r *Response) { r.From().Params.Add("tag", "borrowed-from") },
		"sequence":        func(r *Response) { r.CSeq().SeqNo++ },
		"method":          func(r *Response) { r.CSeq().MethodName = ACK },
		"branch":          func(r *Response) { r.Via().Params.Add("branch", GenerateBranch()) },
		"sent-by-host":    func(r *Response) { r.Via().Host = "borrowed.example.test" },
		"sent-by-port":    func(r *Response) { r.Via().Port++ },
		"via-transport":   func(r *Response) { r.Via().Transport = "UDP" },
		"missing-from":    func(r *Response) { r.RemoveHeader("From") },
		"missing-call-id": func(r *Response) { r.RemoveHeader("Call-ID") },
		"missing-cseq":    func(r *Response) { r.RemoveHeader("CSeq") },
		"missing-via":     func(r *Response) { r.RemoveHeader("Via") },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := response.Clone()
			mutate(invalid)
			require.False(t, layer.MatchClientResponse(invalid))
		})
	}
	request.To().Params.Add("tag", "established-remote")
	response.To().Params.Add("tag", "different-remote")
	require.False(t, layer.MatchClientResponse(response), "in-dialog response must retain remote tag")
	response.To().Params.Add("tag", "established-remote")
	require.True(t, layer.MatchClientResponse(response))
	tx.Terminate()
	require.False(t, layer.MatchClientResponse(response), "retired transactions grant no new ingress")
	require.False(t, layer.MatchClientResponse(nil))
}
