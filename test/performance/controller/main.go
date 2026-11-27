// A deliberately small benchmark application. It uses the canonical control
// codec and does no directory/database/media work. Its CPU is measured separately.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	pb "sipbridge/internal/controlpb"
)

type application struct {
	ctx       context.Context
	conn      *websocket.Conn
	session   string
	owner     *pb.ProcessRef
	mu        sync.Mutex
	writes    sync.Mutex
	pending   map[string]chan *pb.Envelope
	lanes     map[string]*sync.Mutex
	register  atomic.Uint64
	subscribe atomic.Uint64
	notify    atomic.Uint64
	failures  atomic.Uint64
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ca, err := os.ReadFile("/certs/ca.crt")
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid fixture CA")
	}
	cert, err := tls.LoadX509KeyPair("/certs/client.crt", "/certs/client.key")
	if err != nil {
		return err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}}
	conn, _, err := websocket.Dial(ctx, "wss://core:8443/v1/control", &websocket.DialOptions{HTTPClient: client, Subprotocols: []string{"sipbridge.control.json.v1"}})
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	a := &application{ctx: ctx, conn: conn, owner: &pb.ProcessRef{InstanceId: "perf-app", Epoch: uuid.NewString()}, pending: make(map[string]chan *pb.Envelope), lanes: make(map[string]*sync.Mutex)}
	if err = a.send(&pb.Envelope{CorrelationId: "hello", Payload: &pb.Envelope_Hello{Hello: &pb.SessionHello{Controller: a.owner, ConnectionAttempt: 1, Major: 1, MinMinor: 1, MaxMinor: 1, Bounds: &pb.Bounds{MaxEnvelopeBytes: 1 << 20}}}}); err != nil {
		return err
	}
	_, wire, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	var hello pb.Envelope
	if err = protojson.Unmarshal(wire, &hello); err != nil {
		return err
	}
	ack := hello.GetHelloAck()
	if ack == nil || ack.GetMajor() != 1 || ack.GetMinor() != 1 || ack.GetStateBackend() != pb.SessionHelloAck_MEMORY || ack.ApplicationLease == nil {
		return errors.New("benchmark requires the memory serving contract and its application lease")
	}
	a.session = ack.SessionId
	if err = a.send(&pb.Envelope{CorrelationId: uuid.NewString(), Payload: &pb.Envelope_Status{Status: &pb.SessionStatus{RetainedReady: true, BindingsReady: true, AdmitSubscriptions: true, AdmitCalls: true}}}); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]uint64{"register": a.register.Load(), "subscribe": a.subscribe.Load(), "notify": a.notify.Load(), "failures": a.failures.Load()})
	})
	server := &http.Server{Addr: ":8801", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = server.ListenAndServe() }()
	defer server.Close()
	fmt.Println("fixture-application-ready")
	for ctx.Err() == nil {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var e pb.Envelope
		if err := protojson.Unmarshal(data, &e); err != nil {
			return err
		}
		a.mu.Lock()
		waiter := a.pending[e.CorrelationId]
		a.mu.Unlock()
		if waiter != nil {
			select {
			case waiter <- &e:
			default:
			}
			continue
		}
		// Never serialize the WebSocket read loop behind a nested command RPC.
		go func(e *pb.Envelope) {
			if err := a.handle(e); err != nil {
				a.failures.Add(1)
				fmt.Fprintln(os.Stderr, "fixture application:", err)
			}
		}(&e)
	}
	return nil
}

func (a *application) send(e *pb.Envelope) error {
	e.SessionId = a.session
	data, err := protojson.Marshal(e)
	if err != nil {
		return err
	}
	a.writes.Lock()
	defer a.writes.Unlock()
	ctx, cancel := context.WithTimeout(a.ctx, 4*time.Second)
	defer cancel()
	return a.conn.Write(ctx, websocket.MessageText, data)
}
func (a *application) reply(e, reply *pb.Envelope) error {
	reply.CorrelationId = e.CorrelationId
	return a.send(reply)
}
func (a *application) accepted(e *pb.Envelope) error {
	return a.reply(e, &pb.Envelope{Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
}
func (a *application) call(e *pb.Envelope) (*pb.Envelope, error) {
	e.CorrelationId = uuid.NewString()
	e.RemainingBudgetMs = 2000
	if e.GetCommand() != nil {
		e.RemainingBudgetMs = 4000
	}
	ch := make(chan *pb.Envelope, 1)
	a.mu.Lock()
	if len(a.pending) >= 512 {
		a.mu.Unlock()
		return nil, errors.New("fixture RPC capacity")
	}
	a.pending[e.CorrelationId] = ch
	a.mu.Unlock()
	defer func() { a.mu.Lock(); delete(a.pending, e.CorrelationId); a.mu.Unlock() }()
	if err := a.send(e); err != nil {
		return nil, err
	}
	select {
	case reply := <-ch:
		return reply, nil
	case <-a.ctx.Done():
		return nil, a.ctx.Err()
	case <-time.After(5 * time.Second):
		return nil, errors.New("fixture RPC timeout")
	}
}
func header(wire []byte, name string) string {
	for _, line := range strings.Split(string(wire), "\r\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(k, name) {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
func (a *application) handle(e *pb.Envelope) error {
	switch body := e.Payload.(type) {
	case *pb.Envelope_Ping:
		return a.reply(e, &pb.Envelope{Payload: &pb.Envelope_Pong{Pong: body.Ping}})
	case *pb.Envelope_FlowAdoption:
		p := body.FlowAdoption
		return a.reply(e, &pb.Envelope{Payload: &pb.Envelope_FlowAdoptionResult{FlowAdoptionResult: &pb.FlowAdoptionResult{Receipt: &pb.ContextDelivery{Flow: p.Flow, NativeRevision: p.NativeRevision, DirectoryEpoch: "perf-directory", RegistrationTicket: "perf-ticket"}}}})
	case *pb.Envelope_DialogEvent:
		return a.accepted(e)
	case *pb.Envelope_Request:
		p := body.Request
		dialogID := p.GetSnapshot().GetContext().GetOpaqueDialogId()
		if dialogID == "" {
			return errors.New("request has no serving dialog ID")
		}
		a.mu.Lock()
		lane := a.lanes[dialogID]
		if lane == nil {
			if len(a.lanes) >= 4096 {
				a.mu.Unlock()
				return errors.New("fixture context capacity")
			}
			lane = &sync.Mutex{}
			a.lanes[dialogID] = lane
		}
		a.mu.Unlock()
		lane.Lock()
		defer lane.Unlock()
		method := p.Message.Method
		status := uint32(200)
		if method == "SUBSCRIBE" {
			status = 202
		} else if method != "REGISTER" {
			return errors.New("unexpected fixture request method")
		}
		headers := []*pb.Header{{Name: "Expires", Value: header(p.Message.Wire, "Expires")}}
		if method == "REGISTER" {
			headers = append(headers, &pb.Header{Name: "Contact", Value: header(p.Message.Wire, "Contact")})
		}
		return a.decide(p, status, headers)
	case *pb.Envelope_FlowClosed:
		return nil
	default:
		return fmt.Errorf("unexpected control payload %T", body)
	}
}

// The fixture prepares policy and content only. The native committed result is
// not physical delivery proof; the independent TLS load client verifies every SIP result.
func (a *application) decide(p *pb.RequestPresented, status uint32, headers []*pb.Header) error {
	decision := &pb.RequestDecision{Context: p.Snapshot.Context, RequestId: p.RequestId, Sequence: p.Snapshot.CommandSequence + 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "perf-directory", ExpiresAtMs: uint64(time.Now().Add(2 * time.Second).UnixMilli()), ExpectedDelivery: p.Snapshot.Delivery},
		Basis:     pb.OwnerIngest_AUTHENTICATED_BINDING, PrincipalId: "perf-phone", BindingId: "perf-binding", Status: status, Headers: headers}
	if proposal := p.FlowAdoption; proposal != nil {
		decision.Delivery = &pb.ContextDelivery{Flow: proposal.Flow, NativeRevision: proposal.NativeRevision, DirectoryEpoch: "perf-directory", RegistrationTicket: "perf-ticket"}
	}
	if p.Message.Method == "SUBSCRIBE" {
		event := header(p.Message.Wire, "Event")
		content := "application/simple-message-summary"
		payload := "Messages-Waiting: no\r\nMessage-Account: sip:1000@bench.invalid\r\nVoice-Message: 0/0 (0/0)\r\n"
		if event == "dialog" {
			content = "application/dialog-info+xml"
			payload = fmt.Sprintf("<?xml version=\"1.0\"?><dialog-info xmlns=\"urn:ietf:params:xml:ns:dialog-info\" version=\"%s\" state=\"full\" entity=\"sip:1000@bench.invalid\"></dialog-info>", strings.Fields(header(p.Message.Wire, "CSeq"))[0])
		}
		state := "active;expires=" + header(p.Message.Wire, "Expires")
		if header(p.Message.Wire, "Expires") == "0" {
			state = "terminated;reason=timeout"
		}
		decision.Notify = &pb.DialogRequest{Method: "NOTIFY", Headers: []*pb.Header{{Name: "Event", Value: event}, {Name: "Subscription-State", Value: state}, {Name: "Content-Type", Value: content}}, Body: []byte(payload)}
		decision.Admission.SelectedFlow = p.Flow
	}
	reply, err := a.call(&pb.Envelope{Payload: &pb.Envelope_RequestDecision{RequestDecision: decision}})
	if err != nil {
		return err
	}
	result := reply.GetResult()
	if result == nil || result.Certainty != pb.OperationResult_ACCEPTED || result.Error != "" || result.Snapshot == nil {
		return fmt.Errorf("fixture decision refused: %v", result)
	}
	if p.Message.Method == "REGISTER" {
		a.register.Add(1)
	} else {
		a.subscribe.Add(1)
		a.notify.Add(1)
	}
	if p.Message.Method == "REGISTER" || header(p.Message.Wire, "Expires") == "0" {
		a.mu.Lock()
		delete(a.lanes, p.GetSnapshot().GetContext().GetOpaqueDialogId())
		a.mu.Unlock()
	}
	return nil
}
