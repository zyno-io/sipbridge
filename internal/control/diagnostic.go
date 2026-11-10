package control

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// Diagnostic contains only local categories and bounded, validated references.
// No error text, peer close reason, frame or SIP value enters this projection.
type Diagnostic struct {
	Event          string
	Stage          string
	IOClass        string         `json:",omitempty"`
	Session        string         `json:",omitempty"`
	Peer           *pb.ProcessRef `json:",omitempty"`
	Correlation    string         `json:",omitempty"`
	Kind           string         `json:",omitempty"`
	CloseCode      int            `json:",omitempty"`
	BudgetMS       uint32         `json:",omitempty"`
	Deadline       bool
	Canceled       bool
	CallerExpired  bool
	SessionExpired bool
}

func (p *session) diagnostic(stage string, err error, envelope *pb.Envelope, callerExpired bool) {
	observer := p.server.cfg.OnDiagnostic
	if observer == nil {
		return
	}
	d := Diagnostic{Event: "sipbridge-control-failure", Stage: stage, Deadline: errors.Is(err, context.DeadlineExceeded),
		Canceled: errors.Is(err, context.Canceled), CallerExpired: callerExpired, SessionExpired: p.ctx.Err() != nil}
	if errors.Is(err, net.ErrClosed) {
		d.IOClass = "closed"
	} else if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		d.IOClass = "eof"
	}
	if reference.MatchString(p.id) {
		d.Session = p.id
	}
	if validProcess(p.process) {
		d.Peer = proto.Clone(p.process).(*pb.ProcessRef)
	}
	if code := int(websocket.CloseStatus(err)); code >= 1000 && code <= 4999 {
		d.CloseCode = code
	}
	if envelope != nil {
		d.BudgetMS = envelope.RemainingBudgetMs
		if reference.MatchString(envelope.CorrelationId) {
			d.Correlation = envelope.CorrelationId
		}
		if envelope.Payload != nil {
			// Generated oneof type names are a fixed local vocabulary, not data.
			d.Kind = strings.TrimPrefix(reflect.TypeOf(envelope.Payload).Elem().Name(), "Envelope_")
		}
	}
	defer func() { _ = recover() }() // An observer must not alter cleanup.
	observer(d)
}
