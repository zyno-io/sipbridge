package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
)

const publicAddress = "203.0.113.10:5061"
const messageLimit = 65536

type Flow struct {
	Backend string `json:"backend"`
	Epoch   string `json:"epoch"`
	ID      uint64 `json:"id"`
	Remote  string `json:"remote"`
}

func (f Flow) key() string { return fmt.Sprintf("%s:%s:%d:%s", f.Backend, f.Epoch, f.ID, f.Remote) }

type Input struct {
	Op                string `json:"op"`
	Flow              Flow   `json:"flow"`
	Wire              string `json:"wire"`
	ID                string `json:"id"`
	CallID            string `json:"call_id"`
	Action            string `json:"action"`
	SubscriptionEvent string `json:"subscription_event"`
	DeliveryID        string `json:"delivery_id"`
	Error             string `json:"error"`
}

type Record struct {
	Event             string          `json:"event"`
	Flow              *Flow           `json:"flow,omitempty"`
	Wire              string          `json:"wire,omitempty"`
	ID                string          `json:"id,omitempty"`
	DeliveryID        string          `json:"delivery_id,omitempty"`
	Error             string          `json:"error,omitempty"`
	Reason            string          `json:"reason,omitempty"`
	Result            string          `json:"result,omitempty"`
	State             string          `json:"state,omitempty"`
	ReportedTransport string          `json:"reported_transport,omitempty"`
	NativeSource      string          `json:"native_source,omitempty"`
	Reliable          bool            `json:"reliable,omitempty"`
	TransportError    bool            `json:"transport_error,omitempty"`
	Running           int             `json:"running"`
	Finished          int             `json:"finished"`
	WaitingAck        int             `json:"waiting_ack"`
	Dialogs           int             `json:"dialogs"`
	Subscriptions     int             `json:"subscriptions"`
	Bindings          map[string]Flow `json:"bindings,omitempty"`
}

type callContext struct{ Device, RemoteTag string }

type runtime struct {
	mu                  sync.Mutex
	output              chan Record
	sequence            atomic.Uint64
	pending             map[string]chan string
	flows               map[string]Flow
	bindings            map[string]Flow
	authenticated       map[string]string
	closed              map[string]bool
	connections         map[string]*logicalConnection
	inputs              map[string]*virtualConn
	contexts            map[string]callContext
	dialogs             map[string]*sipgo.DialogServerSession
	targets             map[string]sip.Uri
	transactions        map[string]bool
	registerCommits     map[string]time.Time
	tp                  *sip.TransportLayer
	dialogUA            *sipgo.DialogUA
	client              *sipgo.Client
	subscriptions       map[string]*subscription
	subscriptionInitial map[string]*subscription
	enableSubscriptions bool
	listener            *virtualListener
	pinned              bool
	trackTarget         bool
}

func (r *runtime) emit(record Record) error {
	select {
	case r.output <- record:
		return nil
	default:
		return errors.New("fixture output queue is full")
	}
}

type logicalConnection struct {
	owner    *runtime
	physical Flow
	refs     atomic.Int64
}

func (c *logicalConnection) LocalAddr() net.Addr    { return address(publicAddress) }
func (c *logicalConnection) Ref(n int) int          { return int(c.refs.Add(int64(n))) }
func (c *logicalConnection) TryClose() (int, error) { return c.Ref(-1), nil }

// Logical connection lifetime belongs to the owner, not any physical TLS flow.
func (c *logicalConnection) Close() error { return nil }

func (c *logicalConnection) WriteMsg(message sip.Message) error {
	r := c.owner
	r.mu.Lock()
	flow := c.physical
	if message.CSeq().MethodName != sip.REGISTER && !r.pinned {
		device := r.authenticated[c.physical.key()]
		current, ok := r.bindings[device]
		if !ok {
			r.mu.Unlock()
			return errors.New("no authorized current flow")
		}
		flow = current
	}
	if r.closed[flow.key()] {
		r.mu.Unlock()
		return errors.New("current TLS flow is closed")
	}
	if len(r.pending) >= 128 {
		r.mu.Unlock()
		return errors.New("delivery acknowledgment capacity reached")
	}
	id := strconv.FormatUint(r.sequence.Add(1), 10)
	result := make(chan string, 1)
	r.pending[id] = result
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.pending, id); r.mu.Unlock() }()
	if err := r.emit(Record{Event: "egress", Flow: &flow, Wire: message.String(), DeliveryID: id}); err != nil {
		return err
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case failure := <-result:
		if failure != "" {
			return fmt.Errorf("TLS ingress write: %s", failure)
		}
		return nil
	case <-timer.C:
		return errors.New("TLS delivery acknowledgment deadline")
	}
}

type address string

func (a address) Network() string { return "tls" }
func (a address) String() string  { return string(a) }

type virtualConn struct {
	flow      Flow
	source    string
	chunks    chan []byte
	closed    chan struct{}
	once      sync.Once
	remainder []byte
}

func (c *virtualConn) Read(dst []byte) (int, error) {
	if len(c.remainder) == 0 {
		select {
		case <-c.closed:
			return 0, io.EOF
		case c.remainder = <-c.chunks:
		}
	}
	n := copy(dst, c.remainder)
	c.remainder = c.remainder[n:]
	return n, nil
}
func (c *virtualConn) Write(data []byte) (int, error) {
	return 0, errors.New("native connection bypassed logical egress")
}
func (c *virtualConn) Close() error                     { c.once.Do(func() { close(c.closed) }); return nil }
func (c *virtualConn) LocalAddr() net.Addr              { return address(publicAddress) }
func (c *virtualConn) RemoteAddr() net.Addr             { return address(c.source) }
func (c *virtualConn) SetDeadline(time.Time) error      { return nil }
func (c *virtualConn) SetReadDeadline(time.Time) error  { return nil }
func (c *virtualConn) SetWriteDeadline(time.Time) error { return nil }

type virtualListener struct {
	accepts chan net.Conn
	closed  chan struct{}
}

func (l *virtualListener) Addr() net.Addr { return address(publicAddress) }
func (l *virtualListener) Close() error   { close(l.closed); return nil }
func (l *virtualListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.accepts:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func header(message sip.Message, name string) string {
	headers := message.GetHeaders(name)
	if len(headers) == 0 {
		return ""
	}
	return headers[0].Value()
}

func (r *runtime) ingest(flow Flow, wire string) error {
	if len(wire) > messageLimit {
		return errors.New("SIP message too large")
	}
	message, err := sip.ParseMessage([]byte(wire))
	if err != nil {
		return err
	}
	if req, ok := message.(*sip.Request); ok {
		host, port, err := net.SplitHostPort(flow.Remote)
		if err != nil {
			return err
		}
		via := req.Via()
		if via == nil {
			return errors.New("missing Via")
		}
		if via.Params == nil {
			via.Params = sip.NewParams()
		}
		via.Params.Add("received", host)
		if via.Params.Has("rport") {
			via.Params.Add("rport", port)
		}
		wire = req.String()
	}
	r.mu.Lock()
	if r.closed[flow.key()] {
		r.mu.Unlock()
		return errors.New("closed flow cannot be reused")
	}
	device := r.authenticated[flow.key()]
	if req, ok := message.(*sip.Request); ok && req.Method != sip.REGISTER {
		if device == "" {
			r.mu.Unlock()
			return r.emit(Record{Event: "denied", Flow: &flow, Reason: "unbound flow"})
		}
		remoteTag, _ := req.From().Params.Get("tag")
		id := req.CallID().Value()
		ctx, exists := r.contexts[id]
		if exists && (ctx.Device != device || ctx.RemoteTag != remoteTag) {
			r.mu.Unlock()
			return r.emit(Record{Event: "denied", Flow: &flow, Reason: "context identity"})
		}
		if !exists && (req.Method == sip.INVITE || req.Method == sip.SUBSCRIBE) {
			r.contexts[id] = callContext{device, remoteTag}
		}
	} else if response, ok := message.(*sip.Response); ok {
		ctx, exists := r.contexts[response.CallID().Value()]
		if !exists || device == "" || ctx.Device != device {
			r.mu.Unlock()
			return errors.New("response from unrelated flow")
		}
	}
	conn := r.inputs[flow.key()]
	if conn == nil {
		if len(r.inputs) >= 64 {
			r.mu.Unlock()
			return errors.New("virtual flow capacity reached")
		}
		source := fmt.Sprintf("127.0.0.254:%d", 20000+len(r.inputs))
		conn = &virtualConn{flow: flow, source: source, chunks: make(chan []byte, 32), closed: make(chan struct{})}
		logical := &logicalConnection{owner: r, physical: flow}
		logical.refs.Store(1)
		r.inputs[flow.key()], r.connections[source], r.flows[source] = conn, logical, flow
		r.mu.Unlock()
		r.emit(Record{Event: "transport", Flow: &flow, NativeSource: source, ReportedTransport: "TLS", Reliable: true})
		select {
		case r.listener.accepts <- conn:
		default:
			return errors.New("virtual listener capacity reached")
		}
	} else {
		r.mu.Unlock()
	}
	select {
	case conn.chunks <- []byte(wire):
		return nil
	default:
		return errors.New("virtual ingress capacity reached")
	}
}

func (r *runtime) request(req *sip.Request, tx *sip.ServerTx) {
	if req.Method != sip.INVITE {
		defer tx.TerminateGracefully()
	}
	key := tx.Key()
	r.mu.Lock()
	r.transactions[key] = true
	r.mu.Unlock()
	tx.OnTerminate(func(key string, err error) { r.mu.Lock(); delete(r.transactions, key); r.mu.Unlock() })
	respond := func(status int, reason string) {
		if err := tx.Respond(sip.NewResponseFromRequest(req, status, reason, nil)); err != nil {
			r.emit(Record{Event: "handler_error", Error: err.Error()})
		}
	}
	if req.Method == sip.REGISTER {
		device, token := header(req, "X-Proof-Device"), header(req, "X-Proof-Token")
		accepted := (device == "alice" && token == "fixture-alice") || (device == "bob" && token == "fixture-bob")
		r.mu.Lock()
		flow := r.flows[req.Source()]
		old := r.authenticated[flow.key()]
		accepted = accepted && !r.closed[flow.key()] && (old == "" || old == device)
		if accepted {
			now := time.Now()
			for operation, expires := range r.registerCommits {
				if !expires.After(now) {
					delete(r.registerCommits, operation)
				}
			}
			operation := fmt.Sprintf("%s:%s:%s:%d", key, device, req.CallID().Value(), req.CSeq().SeqNo)
			if _, replay := r.registerCommits[operation]; !replay {
				if len(r.registerCommits) >= 1024 {
					accepted = false
				} else {
					r.authenticated[flow.key()] = device
					r.bindings[device] = flow
					r.registerCommits[operation] = now.Add(time.Minute)
				}
			}
		}
		r.mu.Unlock()
		if accepted {
			respond(200, "OK")
		} else {
			respond(403, "Forbidden")
		}
		return
	}
	if req.Method == sip.CANCEL {
		respond(481, "Call/Transaction Does Not Exist")
		return
	}
	if req.Method == sip.SUBSCRIBE && r.enableSubscriptions {
		if err := r.handleSubscribe(req, tx); err != nil {
			r.emit(Record{Event: "handler_error", Error: err.Error()})
		}
		return
	}
	id := req.CallID().Value()
	r.mu.Lock()
	dialog := r.dialogs[id]
	r.mu.Unlock()
	toTag, _ := req.To().Params.Get("tag")
	if toTag != "" {
		dialogID, err := sip.DialogIDFromRequestUAS(req)
		if dialog == nil || err != nil || dialogID != dialog.ID {
			respond(481, "Call/Transaction Does Not Exist")
			return
		}
		switch req.Method {
		case sip.ACK:
			err = dialog.ReadAck(req, tx)
		case sip.BYE:
			err = dialog.ReadBye(req, tx)
		default:
			respond(200, "OK")
		}
		if err != nil {
			r.emit(Record{Event: "handler_error", Error: err.Error()})
		}
		return
	}
	if req.Method != sip.INVITE {
		respond(405, "Method Not Allowed")
		return
	}
	dialog, err := r.dialogUA.ReadInvite(req, tx)
	if err != nil {
		r.emit(Record{Event: "handler_error", Error: err.Error()})
		return
	}
	r.mu.Lock()
	r.dialogs[id] = dialog
	r.targets[id] = *req.Contact().Address.Clone()
	r.mu.Unlock()
	dialog.OnState(func(state sip.DialogState) {
		label := state.String()
		switch state {
		case sip.DialogStateConfirmed:
			label = "Confirmed"
		case sip.DialogStateEnded:
			label = "Terminated"
		}
		if state == sip.DialogStateEnded {
			r.mu.Lock()
			delete(r.dialogs, id)
			delete(r.targets, id)
			r.mu.Unlock()
		}
		r.emit(Record{Event: "dialog_state", State: label + "(" + id + ")"})
	})
	if header(req, "X-Proof-Answer") == "yes" {
		err = dialog.Respond(200, "OK", []byte("v=0\r\n"))
	} else {
		err = dialog.Respond(183, "Session Progress", []byte("v=0\r\n"))
	}
	if err != nil {
		r.emit(Record{Event: "handler_error", Error: err.Error()})
	}
	// Consume native non-2xx ACK delivery; protocol matching/timers stay native.
	select {
	case <-tx.Acks():
	case <-tx.Done():
	}
}

func (r *runtime) command(input Input) {
	if input.Action == "notify" && r.enableSubscriptions {
		r.subscriptionCommand(input)
		return
	}
	failure := func(err error) {
		r.emit(Record{Event: "command_error", ID: input.ID, Error: err.Error(), TransportError: errors.Is(err, sip.ErrTransactionTransport)})
	}
	r.mu.Lock()
	dialog := r.dialogs[input.CallID]
	target := r.targets[input.CallID]
	r.mu.Unlock()
	if dialog == nil {
		failure(errors.New("dialog not found"))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	result := "Ok(Some(200))"
	if input.Action == "bye" {
		bye := sip.NewRequest(sip.BYE, target)
		err := error(nil)
		if r.trackTarget {
			err = dialog.WriteBye(ctx, bye)
		} else {
			err = dialog.Bye(ctx)
		}
		if err != nil {
			failure(err)
			return
		}
	} else {
		method := sip.INFO
		if input.Action == "reinvite" {
			method = sip.INVITE
		}
		recipient := dialog.InviteRequest.Contact().Address
		if r.trackTarget {
			recipient = *target.Clone()
		}
		req := sip.NewRequest(method, recipient)
		req.SetBody([]byte("probe"))
		response, err := dialog.Do(ctx, req)
		if err != nil {
			failure(err)
			return
		}
		// No output rewriting or automatic target-commit workaround in this comparison.
		if method == sip.INVITE && response.IsSuccess() {
			if response.Contact() == nil {
				failure(errors.New("successful target refresh missing Contact"))
				return
			}
			if r.trackTarget {
				r.mu.Lock()
				r.targets[input.CallID] = *response.Contact().Address.Clone()
				r.mu.Unlock()
			}
			ack := sip.NewRequest(sip.ACK, response.Contact().Address)
			// Generic Do returns the final response. The public dialog request
			// helper constructs ACK identity/CSeq/route headers from this dialog.
			if err := dialog.WriteRequest(ack); err != nil {
				failure(err)
				return
			}
		}
		result = fmt.Sprintf("Ok(Some(%d))", response.StatusCode)
	}
	r.emit(Record{Event: "command_done", ID: input.ID, Result: result})
}

func main() {
	if os.Getenv("SIPBRIDGE_FIXTURE_STDIO") != "1" {
		fmt.Fprintln(os.Stderr, "sipbridge: this executable is the isolated protocol fixture; run cmd/sipbridge for the service")
		os.Exit(2)
	}
	sip.SetTimers(100*time.Millisecond, 400*time.Millisecond, 300*time.Millisecond)
	r := &runtime{output: make(chan Record, 128), pending: make(map[string]chan string), flows: make(map[string]Flow), bindings: make(map[string]Flow), authenticated: make(map[string]string), closed: make(map[string]bool), connections: make(map[string]*logicalConnection), inputs: make(map[string]*virtualConn), contexts: make(map[string]callContext), dialogs: make(map[string]*sipgo.DialogServerSession), targets: make(map[string]sip.Uri), transactions: make(map[string]bool), registerCommits: make(map[string]time.Time), listener: &virtualListener{accepts: make(chan net.Conn, 32), closed: make(chan struct{})}, pinned: os.Getenv("PROOF_PIN_INITIAL_FLOW") != "", trackTarget: os.Getenv("PROOF_TRACK_TARGET") == "1"}
	go func() {
		encoder := json.NewEncoder(os.Stdout)
		for record := range r.output {
			if err := encoder.Encode(record); err != nil {
				os.Exit(1)
			}
		}
	}()
	ua, err := sipgo.NewUA(sipgo.WithUserAgent("reconnect-proof"), sipgo.WithUserAgentTransportLayerOptions(sip.WithTransportLayerReadFilter(func(info sip.TransportReadProps, data []byte) ([]byte, error) {
		r.mu.Lock()
		conn := r.connections[info.RemoteAddr.String()]
		r.mu.Unlock()
		if conn == nil {
			return nil, errors.New("unknown trusted ingress source")
		}
		r.tp.Pool("tls").Add(info.RemoteAddr.String(), conn)
		return data, nil
	})))
	if err != nil {
		panic(err)
	}
	defer ua.Close()
	r.tp = ua.TransportLayer()
	client, err := sipgo.NewClient(ua, sipgo.WithClientAddr(publicAddress))
	if err != nil {
		panic(err)
	}
	contactMessage, err := sip.ParseMessage([]byte("OPTIONS sip:test@example.test SIP/2.0\r\nContact: <sip:runtime@203.0.113.10:5061;transport=tls>\r\nContent-Length: 0\r\n\r\n"))
	if err != nil {
		panic(err)
	}
	contactRequest := contactMessage.(*sip.Request)
	r.dialogUA = &sipgo.DialogUA{Client: client, ContactHDR: *contactRequest.Contact(), RewriteContact: true}
	r.client = client
	r.subscriptions, r.subscriptionInitial = make(map[string]*subscription), make(map[string]*subscription)
	r.enableSubscriptions = os.Getenv("PROOF_SUBSCRIPTIONS") == "1"
	if r.enableSubscriptions {
		go r.expireSubscriptions()
	}
	ua.TransactionLayer().OnRequest(r.request)
	go func() {
		if err := r.tp.ServeTLS(r.listener); err != nil && !errors.Is(err, net.ErrClosed) {
			slog.Error("virtual transport", "error", err)
		}
	}()
	r.emit(Record{Event: "ready", ReportedTransport: "TLS", Reliable: true})
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), messageLimit)
	for scanner.Scan() {
		input := Input{}
		if err := json.Unmarshal(scanner.Bytes(), &input); err != nil {
			r.emit(Record{Event: "input_error", Error: err.Error()})
			continue
		}
		switch input.Op {
		case "ingress":
			if err := r.ingest(input.Flow, input.Wire); err != nil {
				r.emit(Record{Event: "input_error", Error: err.Error()})
			}
		case "closed":
			r.mu.Lock()
			r.closed[input.Flow.key()] = true
			r.mu.Unlock()
		case "delivery_ack":
			r.mu.Lock()
			pending := r.pending[input.DeliveryID]
			r.mu.Unlock()
			if pending != nil {
				select {
				case pending <- input.Error:
				default:
				}
			}
		case "command":
			go r.command(input)
		case "stats":
			r.mu.Lock()
			bindings := make(map[string]Flow)
			for device, flow := range r.bindings {
				bindings[device] = flow
			}
			record := Record{Event: "stats", ID: input.ID, Running: len(r.transactions), Dialogs: len(r.dialogs), Subscriptions: len(r.subscriptions), Bindings: bindings}
			for _, dialog := range r.dialogs {
				if dialog.LoadState() == sip.DialogStateEstablished {
					record.WaitingAck++
				}
			}
			r.mu.Unlock()
			r.emit(record)
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Error("fixture IPC", "error", err)
	}
}
