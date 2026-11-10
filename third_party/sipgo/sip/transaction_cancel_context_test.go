package sip

import (
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelContextConnection struct {
	mu        sync.Mutex
	responses []*Response
}

func (c *cancelContextConnection) LocalAddr() net.Addr    { return &net.TCPAddr{} }
func (c *cancelContextConnection) Ref(int) int            { return 1 }
func (c *cancelContextConnection) TryClose() (int, error) { return 1, nil }
func (c *cancelContextConnection) Close() error           { return nil }
func (c *cancelContextConnection) WriteMsg(message Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses = append(c.responses, message.(*Response).Clone())
	return nil
}
func (c *cancelContextConnection) snapshot() []*Response {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Response(nil), c.responses...)
}

func TestServerTxCancelContextIdentity(t *testing.T) {
	request, _, _ := testCreateInvite(t, "sip:bob@example.test", "TLS", "127.0.0.1:5061")
	tx := NewServerTx("key", request, nil, slog.Default())
	valid := request.Clone()
	valid.Method = CANCEL
	valid.CSeq().MethodName = CANCEL
	valid.SetSource("192.0.2.2:5077")
	valid.Via().Params.Add("received", "192.0.2.2")
	valid.Via().Params.Add("rport", "5077")
	require.True(t, tx.matchesCancelIdentity(valid), "reconnect may change source/rport")
	for _, mutate := range []func(*Request){
		func(r *Request) { id := CallIDHeader("different-call"); r.ReplaceHeader(&id) },
		func(r *Request) { r.CSeq().SeqNo++ },
		func(r *Request) { r.From().Params.Add("tag", "different-from") },
		func(r *Request) { r.To().Params.Add("tag", "unexpected-to") },
	} {
		other := valid.Clone()
		mutate(other)
		require.False(t, tx.matchesCancelIdentity(other))
	}
}

func TestTransactionLayerCancelBorrowedBranch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	transport := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	layer := NewTransactionLayer(transport, WithTransactionLayerLogger(logger))
	t.Cleanup(layer.Close)
	alice, _, _ := testCreateInvite(t, "sip:callee@example.test", "TLS", "127.0.0.1:5061")
	alice.SetSource("127.0.0.1:5061")
	aliceConnection := &cancelContextConnection{}
	key, err := ServerTxKeyMake(alice)
	require.NoError(t, err)
	original := NewServerTx(key, alice, aliceConnection, logger)
	require.NoError(t, original.Init())
	layer.serverTransactions.put(key, original)
	original.OnTerminate(func(key string, err error) { layer.serverTransactions.drop(key) })

	bobConnection := &cancelContextConnection{}
	transport.Pool("tls").Add("127.0.0.1:5077", bobConnection)
	callbackCalled := false
	layer.OnRequest(func(request *Request, tx *ServerTx) {
		defer tx.Terminate()
		callbackCalled = true
		require.Equal(t, CANCEL, request.Method)
		require.NoError(t, tx.Respond(NewResponseFromRequest(request, 481, "No matching request", nil)))
	})
	borrowed := alice.Clone()
	borrowed.Method = CANCEL
	borrowed.CSeq().MethodName = CANCEL
	id := CallIDHeader("bobs-own-call")
	borrowed.ReplaceHeader(&id)
	borrowed.From().Params.Add("tag", "bobs-own-tag")
	borrowed.SetSource("127.0.0.1:5077")
	require.NoError(t, layer.handleRequest(borrowed))
	require.True(t, callbackCalled, "identity mismatch follows unmatched CANCEL path")
	require.Empty(t, aliceConnection.snapshot(), "borrowed branch cannot cancel Alice's INVITE")
	rejected := bobConnection.snapshot()
	require.Len(t, rejected, 1)
	require.Equal(t, 481, rejected[0].StatusCode)
	require.NoError(t, original.Err())

	// The real identity still matches even over the replacement source flow.
	legitimate := alice.Clone()
	legitimate.Method = CANCEL
	legitimate.CSeq().MethodName = CANCEL
	legitimate.SetSource("127.0.0.1:5077")
	require.NoError(t, layer.handleRequest(legitimate))
	require.Len(t, aliceConnection.snapshot(), 2, "valid cancellation still generates 200 and 487")
}
