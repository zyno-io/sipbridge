package sip

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionIngressUsesBoundConnectionAndTransportTimers(t *testing.T) {
	for _, network := range []string{"UDP", "TCP", "TLS"} {
		t.Run(network, func(t *testing.T) {
			transport := NewTransportLayer(net.DefaultResolver, NewParser(), nil, WithTransportLayerConnectionCreation(false))
			layer := NewTransactionLayer(transport)
			t.Cleanup(func() { layer.Close(); transport.Close() })
			connection := &ingressConnection{}
			source := "127.254.0.1:15060"
			transport.Pool(strings.ToLower(network)).Add(source, connection)
			wire := "OPTIONS sip:service@example.test SIP/2.0\r\nVia: SIP/2.0/" + network + " peer.invalid:5060;branch=z9hG4bK-ingress\r\nFrom: <sip:a@example.test>;tag=remote\r\nTo: <sip:service@example.test>\r\nCall-ID: ingress-test\r\nCSeq: 1 OPTIONS\r\nContent-Length: 0\r\n\r\n"
			message, err := ParseMessage([]byte(wire))
			require.NoError(t, err)
			message.SetSource(source)
			message.SetTransport(network)
			handled := make(chan *ServerTx, 1)
			layer.OnRequest(func(request *Request, tx *ServerTx) {
				if err := tx.Respond(NewResponseFromRequest(request, 200, "OK", nil)); err != nil {
					t.Error(err)
				}
				handled <- tx
			})
			layer.HandleMessage(message)
			select {
			case tx := <-handled:
				require.Equal(t, network != "UDP", tx.reliable)
				require.Same(t, connection, tx.conn)
				responses := connection.snapshot()
				require.Len(t, responses, 1)
				require.Equal(t, 200, responses[0].StatusCode)
				require.Equal(t, network, responses[0].Transport())
			case <-time.After(time.Second):
				t.Fatal("native transaction did not receive injected ingress")
			}
		})
	}
}

// Independent of other patch tests so this API proposal can be cherry-picked.
type ingressConnection struct {
	mu        sync.Mutex
	responses []*Response
}

func (*ingressConnection) LocalAddr() net.Addr    { return &net.UDPAddr{} }
func (*ingressConnection) Ref(int) int            { return 1 }
func (*ingressConnection) TryClose() (int, error) { return 1, nil }
func (*ingressConnection) Close() error           { return nil }
func (c *ingressConnection) WriteMsg(message Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses = append(c.responses, message.(*Response).Clone())
	return nil
}
func (c *ingressConnection) snapshot() []*Response {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Response(nil), c.responses...)
}

func TestTransactionIngressRefusesUnboundConnection(t *testing.T) {
	for _, network := range []string{"UDP", "TCP", "TLS"} {
		t.Run(network, func(t *testing.T) {
			transport := NewTransportLayer(net.DefaultResolver, NewParser(), nil, WithTransportLayerConnectionCreation(false))
			defer transport.Close()
			request := NewRequest(OPTIONS, Uri{Scheme: "sip", Host: "127.0.0.1", Port: 9})
			request.AppendHeader(&ViaHeader{ProtocolName: "SIP", ProtocolVersion: "2.0", Transport: network, Host: "127.0.0.1", Port: 9, Params: NewParams()})
			request.SetTransport(network)
			request.SetDestination("127.0.0.1:9")
			request.SetSource("127.0.0.1:9")
			_, err := transport.ClientRequestConnection(context.Background(), request)
			require.ErrorIs(t, err, ErrConnectionCreationDisabled)
			_, err = transport.serverRequestConnection(context.Background(), request)
			require.ErrorIs(t, err, ErrConnectionCreationDisabled)
		})
	}
}
