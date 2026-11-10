package sip

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelIdentityConnection struct {
	mu        sync.Mutex
	responses []*Response
}

func (c *cancelIdentityConnection) LocalAddr() net.Addr    { return &net.TCPAddr{} }
func (c *cancelIdentityConnection) Ref(n int) int          { return 1 }
func (c *cancelIdentityConnection) TryClose() (int, error) { return 1, nil }
func (c *cancelIdentityConnection) Close() error           { return nil }
func (c *cancelIdentityConnection) WriteMsg(message Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses = append(c.responses, message.(*Response).Clone())
	return nil
}
func (c *cancelIdentityConnection) snapshot() []*Response {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Response(nil), c.responses...)
}

func cancelIdentityFixture(t *testing.T) (*TransactionLayer, *ServerTx, *Request, *cancelIdentityConnection) {
	t.Helper()
	request, _, _ := testCreateInvite(t, "sip:bob@example.test", "TLS", "127.0.0.1:5061")
	connection := &cancelIdentityConnection{}
	key, err := ServerTxKeyMake(request)
	require.NoError(t, err)
	tx := NewServerTx(key, request, connection, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, tx.Init())
	t.Cleanup(tx.Terminate)
	layer := &TransactionLayer{serverTransactions: newTransactionStore[*ServerTx]()}
	layer.serverTransactions.put(key, tx)
	return layer, tx, request, connection
}

func matchingCancel(request *Request) *Request {
	cancel := request.Clone()
	cancel.Method = CANCEL
	cancel.CSeq().MethodName = CANCEL
	// The CANCEL retains its own received/source metadata after reconnect.
	cancel.Via().Params.Add("received", "192.0.2.2")
	cancel.Via().Params.Add("rport", "5077")
	return cancel
}

func responseTag(response *Response) string {
	tag, _ := response.To().Params.Get("tag")
	return tag
}

func taggedResponse(request *Request, status int, tag string) *Response {
	response := NewResponseFromRequest(request, status, "Test", nil)
	response.To().Params.Add("tag", tag)
	return response
}

func TestServerTxCancelResponseIdentity(t *testing.T) {
	layer, tx, request, connection := cancelIdentityFixture(t)
	provisional := taggedResponse(request, 183, "established-local-tag")
	require.NoError(t, tx.Respond(provisional))
	cancel := matchingCancel(request)
	require.NoError(t, layer.handleRequest(cancel))
	require.NoError(t, layer.handleRequest(cancel))
	responses := connection.snapshot()
	require.Len(t, responses, 4)
	for _, response := range responses {
		require.Equal(t, "established-local-tag", responseTag(response))
	}
	require.Equal(t, CANCEL, responses[1].CSeq().MethodName)
	require.Equal(t, INVITE, responses[2].CSeq().MethodName)
	require.Equal(t, 487, responses[2].StatusCode)
	received, _ := responses[1].Via().Params.Get("received")
	require.Equal(t, "192.0.2.2", received)
	require.False(t, request.To().Params.Has("tag"), "original request is immutable")
	require.Equal(t, "established-local-tag", responseTag(provisional))
	require.ErrorIs(t, tx.Respond(taggedResponse(request, 200, "late-answer-tag")), ErrTransactionCanceled)
	require.NoError(t, tx.Receive(request))
	responses = connection.snapshot()
	require.Equal(t, 487, responses[len(responses)-1].StatusCode, "late answer cannot replace cached 487")
	require.Equal(t, "established-local-tag", responseTag(responses[len(responses)-1]))
}

func TestServerTxCancelBeforeTaggedResponse(t *testing.T) {
	layer, tx, request, connection := cancelIdentityFixture(t)
	require.NoError(t, tx.Respond(NewResponseFromRequest(request, 100, "Trying", nil)))
	require.NoError(t, layer.handleRequest(matchingCancel(request)))
	require.NoError(t, layer.handleRequest(matchingCancel(request)))
	responses := connection.snapshot()
	require.Empty(t, responseTag(responses[0]))
	tag := responseTag(responses[1])
	require.NotEmpty(t, tag)
	for _, response := range responses[1:] {
		require.Equal(t, tag, responseTag(response))
	}
}

func TestServerTxCancelAfterFinal(t *testing.T) {
	for _, status := range []int{200, 486} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			layer, tx, request, connection := cancelIdentityFixture(t)
			require.NoError(t, tx.Respond(taggedResponse(request, 183, "dialog-tag")))
			require.NoError(t, tx.Respond(taggedResponse(request, status, "dialog-tag")))
			require.NoError(t, layer.handleRequest(matchingCancel(request)))
			require.NoError(t, layer.handleRequest(matchingCancel(request)))
			responses := connection.snapshot()
			require.Len(t, responses, 4)
			for _, response := range responses {
				require.Equal(t, "dialog-tag", responseTag(response))
				require.NotEqual(t, 487, response.StatusCode)
			}
			require.Equal(t, status, tx.fsmResp.StatusCode)
			require.Equal(t, CANCEL, responses[2].CSeq().MethodName)
		})
	}
}

func TestServerTxCancelConcurrentFinal(t *testing.T) {
	for range 100 {
		layer, tx, request, connection := cancelIdentityFixture(t)
		require.NoError(t, tx.Respond(taggedResponse(request, 183, "dialog-tag")))
		begin := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-begin; results <- layer.handleRequest(matchingCancel(request)) }()
		go func() { <-begin; results <- tx.Respond(taggedResponse(request, 200, "dialog-tag")) }()
		close(begin)
		for range 2 {
			err := <-results
			require.True(t, err == nil || errors.Is(err, ErrTransactionCanceled), "%v", err)
		}
		responses := connection.snapshot()
		finals := 0
		for _, response := range responses {
			require.Equal(t, "dialog-tag", responseTag(response))
			if response.CSeq().MethodName == INVITE && response.StatusCode >= 200 {
				finals++
			}
		}
		require.Equal(t, 1, finals, "only cancellation or application final can win")
		tx.Terminate()
	}
}

func TestServerTxCancelDoesNotRewriteForwardedTags(t *testing.T) {
	layer, tx, request, connection := cancelIdentityFixture(t)
	first := taggedResponse(request, 200, "fork-a")
	second := taggedResponse(request, 200, "fork-b")
	require.NoError(t, tx.Respond(first))
	require.NoError(t, tx.Respond(second))
	require.NoError(t, layer.handleRequest(matchingCancel(request)))
	responses := connection.snapshot()
	require.Equal(t, "fork-a", responseTag(responses[0]))
	require.Equal(t, "fork-b", responseTag(responses[1]))
	require.Equal(t, "fork-b", responseTag(responses[2]))
	require.Equal(t, "fork-a", responseTag(first))
	require.Equal(t, "fork-b", responseTag(second))
}
