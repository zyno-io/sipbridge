package sip

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type TransactionRequestHandler func(req *Request, tx *ServerTx)
type UnhandledResponseHandler func(req *Response)
type ErrorHandler func(err error)

func defaultRequestHandler(r *Request, tx *ServerTx) {
	DefaultLogger().Info("Unhandled sip request. OnRequest handler not added", "caller", "transactionLayer", "msg", r.Short())
}

func defaultUnhandledRespHandler(r *Response) {
	DefaultLogger().Info("TransactionLayer: Unhandled sip response. Possible retransmissions. Set UnhandledResponseHandler", "caller", "transactionLayer", "msg", r.Short())
}

type TransactionLayer struct {
	tpl           *TransportLayer
	reqHandler    TransactionRequestHandler
	unRespHandler UnhandledResponseHandler

	clientTransactions *transactionStore[*ClientTx]
	serverTransactions *transactionStore[*ServerTx]

	terminateOnConnClose bool

	log *slog.Logger
}

type TransactionLayerOption func(tpl *TransactionLayer)

func WithTransactionLayerLogger(l *slog.Logger) TransactionLayerOption {
	return func(txl *TransactionLayer) {
		if l != nil {
			txl.log = l.With("caller", "TransactionLayer")
		}
	}
}

func WithTransactionLayerUnhandledResponseHandler(f func(r *Response)) TransactionLayerOption {
	return func(txl *TransactionLayer) {
		txl.unRespHandler = f
	}
}

// WithTransactionLayerTerminateOnConnClose enables termination of pending
// client and server transactions when the underlying connection closes,
// instead of waiting for timers to expire.
//
// Experimental
func WithTransactionLayerTerminateOnConnClose() TransactionLayerOption {
	return func(txl *TransactionLayer) {
		txl.terminateOnConnClose = true
	}
}

func NewTransactionLayer(tpl *TransportLayer, options ...TransactionLayerOption) *TransactionLayer {
	txl := &TransactionLayer{
		tpl:                tpl,
		clientTransactions: newTransactionStore[*ClientTx](),
		serverTransactions: newTransactionStore[*ServerTx](),

		reqHandler:    defaultRequestHandler,
		unRespHandler: defaultUnhandledRespHandler,
	}
	txl.log = DefaultLogger().With("caller", "TransactionLayer")

	for _, o := range options {
		o(txl)
	}

	//Send all transport messages to our transaction layer
	tpl.OnMessage(txl.handleMessage)

	notify := func(conn Connection) { txl.OnConnectionClose(conn) }
	if tpl.tcp != nil {
		tpl.tcp.onConnClose = notify
	}
	if tpl.tls != nil {
		tpl.tls.onConnClose = notify
	}
	if tpl.ws != nil {
		tpl.ws.onConnClose = notify
	}
	if tpl.wss != nil {
		tpl.wss.onConnClose = notify
	}

	return txl
}

func (txl *TransactionLayer) OnRequest(h TransactionRequestHandler) {
	txl.reqHandler = h
}

// OnConnectionClose is called when a reliable transport connection (TCP, TLS,
// WS, WSS) is closed by the remote side or due to a read error.
func (txl *TransactionLayer) OnConnectionClose(conn Connection) {
	if txl.terminateOnConnClose {
		txl.terminateClientTransactions(conn)
		txl.terminateServerTransactions(conn)
	}
}

func (txl *TransactionLayer) terminateClientTransactions(conn Connection) {
	txl.clientTransactions.mu.RLock()
	for _, tx := range txl.clientTransactions.items {
		if tx.conn == conn {
			go tx.spinFsmWithError(client_input_transport_err,
				fmt.Errorf("connection closed: %w", ErrTransactionTransport))
		}
	}
	txl.clientTransactions.mu.RUnlock()
}

func (txl *TransactionLayer) terminateServerTransactions(conn Connection) {
	txl.serverTransactions.mu.RLock()
	for _, tx := range txl.serverTransactions.items {
		if tx.conn == conn {
			go tx.spinFsmWithError(server_input_transport_err,
				fmt.Errorf("connection closed: %w", ErrTransactionTransport))
		}
	}
	txl.serverTransactions.mu.RUnlock()
}

// HandleMessage admits an already parsed message from an application-owned
// transport. The caller must set the observed transport and source and install
// its Connection in the corresponding transport pool before calling this method.
// Requests and responses retain the normal native transaction matching and timers.
func (txl *TransactionLayer) HandleMessage(msg Message) {
	txl.handleMessage(msg)
}

// handleMessage is entry for handling requests and responses from transport
func (txl *TransactionLayer) handleMessage(msg Message) {
	// Having concurency here we increased throghput but also solving deadlock
	// Current client transactions are blocking on passUp and this may block when calling tx.Receive
	// forking here can remove this

	switch msg := msg.(type) {
	case *Request:
		go txl.handleRequestBackground(msg)
	case *Response:
		go txl.handleResponseBackground(msg)
	default:
		txl.log.Error("unsupported message, skip it")
	}
}

func (txl *TransactionLayer) handleRequestBackground(req *Request) {
	if err := txl.handleRequest(req); err != nil {
		txl.log.Error("Server tx failed to handle request", "error", err, "req", req.StartLine())
	}
}

func (txl *TransactionLayer) handleRequest(req *Request) error {
	if req.IsCancel() {
		// Match transaction https://datatracker.ietf.org/doc/html/rfc3261#section-9.2
		// 	The CANCEL method requests that the TU at the server side cancel a
		//    pending transaction.  The TU determines the transaction to be
		//    cancelled by taking the CANCEL request, and then assuming that the
		//    request method is anything but CANCEL or AC

		// For now we only match INVITE
		key, err := makeServerTxKey(req, INVITE)
		if err != nil {
			txl.rejectMalformedRequest(req, err)
			return fmt.Errorf("make key failed: %w", err)
		}

		tx, exists := txl.getServerTx(key)
		if exists && !tx.matchesCancelIdentity(req) {
			exists = false
		}
		if exists {
			// The matched transaction serializes CANCEL 200 and any generated
			// INVITE 487 with its response identity and application finals.
			if err := tx.receiveCancel(req); err != nil {
				return fmt.Errorf("failed to receive req: %w", err)
			}
			return nil
		}
		// Now proceed as normal transaction, and let developer decide what todo with this CANCEL
	}

	key, err := makeServerTxKey(req, "")
	if err != nil {
		txl.rejectMalformedRequest(req, err)
		return fmt.Errorf("make key failed: %w", err)
	}

	return txl.serverTxRequest(req, key)
}

// rejectMalformedRequest sends a stateless 400 Bad Request response when a
// request is too malformed to create a transaction (e.g. missing CSeq or Via).
//
// Per RFC 3261 Section 8.2:
//
//	If a UAS does not understand a header field in a request (that is,
//	the header field is not defined in this specification or in any
//	installed extensions), the server MUST ignore that header field and
//	continue processing the message.
//
// And Section 16.3 (Request Validation):
//
//	If the request contains a malformed header field that the proxy
//	requires, the proxy SHOULD return a 400 (Bad Request) response.
//
// Without this, the sender retransmits indefinitely because it never
// receives any response.
func (txl *TransactionLayer) rejectMalformedRequest(req *Request, reason error) {
	// Build a minimal 400 response from whatever headers the request has.
	// NewResponseFromRequest safely skips nil CSeq, From, To, Call-ID.
	res := NewResponseFromRequest(req, StatusBadRequest, "Bad Request", nil)

	if err := txl.tpl.WriteMsg(res); err != nil {
		txl.log.Error("Failed to send stateless 400 for malformed request",
			"error", err,
			"reason", reason.Error(),
			"req", req.StartLine(),
		)
	}
}

func (txl *TransactionLayer) serverTxRequest(req *Request, key string) error {
	txl.serverTransactions.lock()
	tx, exists := txl.serverTransactions.items[key]
	if exists {
		txl.serverTransactions.unlock()
		if err := tx.Receive(req); err != nil {
			return fmt.Errorf("failed to receive req: %w", err)
		}
		return nil
	}
	txl.serverTransactions.unlock()

	// Connections are singleflight operations
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := txl.tpl.serverRequestConnection(ctx, req)
	if err != nil {
		return fmt.Errorf("server tx get connection failed: %w", err)
	}
	if conn == nil {
		return fmt.Errorf("transport returned nil connection")
	}

	txl.serverTransactions.lock()
	tx, exists = txl.serverTransactions.items[key]
	if exists {
		txl.serverTransactions.unlock()
		conn.TryClose()

		if err := tx.Receive(req); err != nil {
			return fmt.Errorf("failed to receive req: %w", err)
		}
		return nil
	}

	tx = NewServerTx(key, req, conn, txl.log)
	if err := tx.Init(); err != nil {
		txl.serverTransactions.unlock()
		// Init failed: this tx never reaches delete(), so release the connection
		// reference serverRequestConnection took here (mirrors the conn.TryClose
		// the client path does when it discards a transaction).
		conn.TryClose()
		return err
	}

	// put tx to store
	txl.serverTransactions.items[key] = tx
	tx.OnTerminate(txl.serverTxTerminate)
	txl.serverTransactions.unlock()

	// pass request and transaction to handler
	txl.reqHandler(req, tx)
	return nil
}

func (txl *TransactionLayer) serverTxCreate(req *Request, key string) (*ServerTx, error) {
	// Connection must exist by transport layer or it will be created
	// What if connection setup can not be made fast enough?
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := txl.tpl.serverRequestConnection(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("server tx get connection failed: %w", err)
	}

	tx := NewServerTx(key, req, conn, txl.log)
	if err := tx.Init(); err != nil {
		// Init failed: this tx never reaches delete(), so release the connection
		// reference serverRequestConnection took here (mirrors the conn.TryClose
		// the client path does when it discards a transaction).
		if conn != nil {
			conn.TryClose()
		}
		return tx, err
	}
	return tx, nil
}

func (txl *TransactionLayer) handleResponseBackground(res *Response) {
	if err := txl.handleResponse(res); err != nil {
		txl.log.Error("Client tx failed to handle response", "error", err)
	}
}

func (txl *TransactionLayer) handleResponse(res *Response) error {
	key, err := ClientTxKeyMake(res)
	if err != nil {
		return fmt.Errorf("make key failed: %w", err)
	}

	tx, exists := txl.getClientTx(key)
	if !exists {
		// RFC 3261 - 17.1.1.2.
		// Not matched responses should be passed directly to the UA
		txl.unRespHandler(res)
		return nil
	}

	tx.Receive(res)
	return nil
}

// MatchClientResponse reports whether a response matches a live native client
// transaction and its complete request identity. It does not deliver the response
// or authorize any transport. Source/rport/received may differ after reconnect.
// Callers retaining a dialog must additionally validate its established tags.
func (txl *TransactionLayer) MatchClientResponse(res *Response) bool {
	if res == nil || res.CallID() == nil || res.From() == nil || res.To() == nil || res.CSeq() == nil || res.Via() == nil {
		return false
	}
	key, err := ClientTxKeyMake(res)
	if err != nil {
		return false
	}
	tx, exists := txl.getClientTx(key)
	if !exists {
		return false
	}
	select {
	case <-tx.Done():
		return false
	default:
	}
	origin := tx.Origin()
	if origin.CallID() == nil || origin.From() == nil || origin.To() == nil || origin.CSeq() == nil || origin.Via() == nil {
		return false
	}
	from, _ := origin.From().Params.Get("tag")
	responseFrom, _ := res.From().Params.Get("tag")
	to, _ := origin.To().Params.Get("tag")
	responseTo, _ := res.To().Params.Get("tag")
	via, responseVia := origin.Via(), res.Via()
	port, responsePort := via.Port, responseVia.Port
	if port <= 0 {
		port = int(DefaultPort(via.Transport))
	}
	if responsePort <= 0 {
		responsePort = int(DefaultPort(responseVia.Transport))
	}
	return *origin.CallID() == *res.CallID() && from == responseFrom &&
		(to == "" || to == responseTo) && origin.CSeq().SeqNo == res.CSeq().SeqNo && origin.CSeq().MethodName == res.CSeq().MethodName &&
		via.Host == responseVia.Host && port == responsePort && via.Transport == responseVia.Transport &&
		via.ProtocolName == responseVia.ProtocolName && via.ProtocolVersion == responseVia.ProtocolVersion
}

func (txl *TransactionLayer) Request(ctx context.Context, req *Request) (*ClientTx, error) {
	tx, err := txl.NewClientTransaction(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := tx.Init(); err != nil {
		tx.Terminate()
		return nil, err
	}
	return tx, nil
}

func (txl *TransactionLayer) NewClientTransaction(ctx context.Context, req *Request) (*ClientTx, error) {
	if req.IsAck() {
		return nil, fmt.Errorf("ACK request must be sent directly through transport")
	}

	key, err := ClientTxKeyMake(req)
	if err != nil {
		return nil, err
	}

	tx, err := txl.clientTxRequest(ctx, req, key)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (txl *TransactionLayer) clientTxRequest(ctx context.Context, req *Request, key string) (*ClientTx, error) {
	conn, err := txl.tpl.ClientRequestConnection(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("client transcation failed to request connection: %w", err)
	}

	txl.clientTransactions.lock()
	tx, exists := txl.clientTransactions.items[key]
	if exists {
		txl.clientTransactions.unlock()
		conn.TryClose()
		return nil, fmt.Errorf("client transaction %q already exists", key)
	}
	tx = NewClientTx(key, req, conn, txl.log)

	txl.clientTransactions.items[key] = tx
	tx.OnTerminate(txl.clientTxTerminate)
	txl.clientTransactions.unlock()
	return tx, nil
}

func (txl *TransactionLayer) Respond(res *Response) (*ServerTx, error) {
	key, err := ServerTxKeyMake(res)
	if err != nil {
		return nil, err
	}

	tx, exists := txl.getServerTx(key)
	if !exists {
		return nil, fmt.Errorf("transaction does not exists")
	}

	err = tx.Respond(res)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (txl *TransactionLayer) clientTxTerminate(key string, err error) {
	if !txl.clientTransactions.drop(key) {
		txl.log.Info("Non existing client tx was removed", "tx", key)
	}
}

func (txl *TransactionLayer) serverTxTerminate(key string, err error) {
	if !txl.serverTransactions.drop(key) {
		txl.log.Info("Non existing server tx was removed", "tx", key)
	}
}

// RFC 17.1.3.
func (txl *TransactionLayer) getClientTx(key string) (*ClientTx, bool) {
	return txl.clientTransactions.get(key)
	// tx, ok := txl.clientTransactions.get(key)
	// if !ok {
	// 	return nil, false
	// }
	// return tx.(*ClientTx), true
}

// RFC 17.2.3.
func (txl *TransactionLayer) getServerTx(key string) (*ServerTx, bool) {
	return txl.serverTransactions.get(key)
	// tx, ok := txl.serverTransactions.get(key)
	// if !ok {
	// 	return nil, false
	// }
	// return tx.(*ServerTx), true
}

func (txl *TransactionLayer) Close() {
	txl.clientTransactions.terminateAll()
	txl.serverTransactions.terminateAll()
	txl.log.Debug("transaction layer closed")
}

func (txl *TransactionLayer) Transport() *TransportLayer {
	return txl.tpl
}
