package portable

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/emiago/sipgo/sip"
)

// Message is an owned immutable SIP value. Its zero value is an absent message.
// Copies share only private immutable evidence; no parsed SIP graph is retained.
// Restored wire is validated lazily when consumed, not while unrelated state is read.
var errMissingIdentity = errors.New("missing SIP identity")

type Message struct{ data *messageData }

type messageData struct {
	wire                           []byte
	transport, source, destination string
	once                           sync.Once
	frozen                         *frozenMessage
	err                            error
	retained                       atomic.Int64
}

// MessageFacts contains only scalar, immutable protocol and projection facts.
// Transaction keys retain their separate native validation through the key methods.
type MessageFacts struct {
	IsRequest                                                            bool
	Method                                                               sip.RequestMethod
	Status                                                               int
	Reason, URI, CallID, FromTag, ToTag                                  string
	FromURI, ToURI, ContactURI, ContactHostPort                          string
	HasContact                                                           bool
	CSeq                                                                 uint32
	CSeqMethod                                                           sip.RequestMethod
	ViaHost, ViaTransport, ViaProtocol, ViaVersion, ViaBranch, ViaSentBy string
	ViaPort                                                              int
	BodyLength                                                           int
	SDP                                                                  bool
}

// frozenMessage retains compact construction fields, not a sip.Request/Response.
// Params and URIs are privately owned and only copied into mutable builders.
// Extension and repeated headers remain ordered strings for exact projection.
// orderedConstruction is allocated only for repeated singleton/Contact fields.
// The common one-header case uses the selected facts already held below.
type orderedConstruction struct {
	from    []sip.FromHeader
	to      []sip.ToHeader
	callID  []sip.CallIDHeader
	cseq    []sip.CSeqHeader
	contact []sip.ContactHeader
}

type frozenMessage struct {
	facts                           MessageFacts
	nativeErr                       error
	ordered                         *orderedConstruction
	canonical                       bool
	version                         string
	recipient                       sip.Uri
	from                            sip.FromHeader
	to                              sip.ToHeader
	via                             sip.ViaHeader
	vias                            []sip.ViaHeader
	contact                         *sip.ContactHeader
	headers                         []Header
	body                            []byte
	clientKey, serverKey, cancelKey string
	clientErr, serverErr, cancelErr error
}

func ownParams(in sip.HeaderParams) sip.HeaderParams {
	if in == nil {
		return nil
	}
	out := make(sip.HeaderParams, len(in))
	for i, v := range in {
		out[i] = sip.HeaderKV{K: strings.Clone(v.K), V: strings.Clone(v.V)}
	}
	return out
}
func ownURI(in sip.Uri) sip.Uri { return in.CloneOwned() }
func ownFrom(in *sip.FromHeader) sip.FromHeader {
	return sip.FromHeader{DisplayName: strings.Clone(in.DisplayName), Address: ownURI(in.Address), Params: ownParams(in.Params)}
}
func ownTo(in *sip.ToHeader) sip.ToHeader {
	return sip.ToHeader{DisplayName: strings.Clone(in.DisplayName), Address: ownURI(in.Address), Params: ownParams(in.Params)}
}
func ownVia(in *sip.ViaHeader) sip.ViaHeader {
	return sip.ViaHeader{ProtocolName: strings.Clone(in.ProtocolName), ProtocolVersion: strings.Clone(in.ProtocolVersion), Transport: strings.Clone(in.Transport), Host: strings.Clone(in.Host), Port: in.Port, Params: ownParams(in.Params)}
}
func ownContact(in *sip.ContactHeader) *sip.ContactHeader {
	if in == nil {
		return nil
	}
	return &sip.ContactHeader{DisplayName: strings.Clone(in.DisplayName), Address: ownURI(in.Address), Params: ownParams(in.Params)}
}

// ParseMessage is the raw-wire trust boundary. Each value parses at most once.
func ParseMessage(wire []byte, transport, source, destination string) (Message, error) {
	m := RestoreMessage(wire, transport, source, destination)
	_, err := m.prepare()
	return m, err
}

// FreezeMessage accepts the original parser result for these exact wire bytes.
// The caller must own the same observed frame and must not pair independently
// supplied wire/metadata with a parsed object. Untrusted wire uses ParseMessage.
// All aliases to the parser, frame and metadata are severed before returning.
func FreezeMessage(wire []byte, parsed sip.Message, transport, source, destination string) (Message, error) {
	m := RestoreMessage(wire, transport, source, destination)
	m.data.once.Do(func() { m.data.frozen, m.data.err = freeze(parsed, transport, false); m.data.addFrozenBytes() })
	_, err := m.prepare()
	return m, err
}

// RestoreMessage maps the existing serialized wire shape without eagerly parsing
// unused messages. The opaque holder belongs to this operation; no parsed cache is
// serialized or shared through a process-global lookup.
func RestoreMessage(wire []byte, transport, source, destination string) Message {
	d := &messageData{wire: bytes.Clone(wire), transport: strings.Clone(transport), source: strings.Clone(source), destination: strings.Clone(destination)}
	d.retained.Store(int64(unsafe.Sizeof(*d) + uintptr(cap(d.wire)+len(transport)+len(source)+len(destination))))
	return Message{data: d}
}
func (m Message) prepareFacts() (*frozenMessage, error) {
	if m.data == nil {
		return nil, errors.New("empty SIP message")
	}
	m.data.once.Do(func() {
		parsed, err := sip.ParseMessage(m.data.wire)
		if err != nil {
			m.data.err = err
			return
		}
		m.data.frozen, m.data.err = freeze(parsed, m.data.transport, false)
		m.data.addFrozenBytes()
	})
	return m.data.frozen, m.data.err
}
func (m Message) prepare() (*frozenMessage, error) {
	f, err := m.prepareFacts()
	if err != nil {
		return nil, err
	}
	if f.nativeErr != nil {
		return nil, f.nativeErr
	}
	return f, nil
}
func (m Message) prepareObserved() (*frozenMessage, error) {
	f, err := m.prepareFacts()
	if err != nil {
		if errors.Is(err, errMissingIdentity) {
			return nil, errors.New("missing mandatory SIP identity")
		}
		return nil, err
	}
	if f.facts.IsRequest && f.clientErr != nil {
		return nil, errors.New("RFC 3261 Via branch required")
	}
	return f, nil
}

func constructionHeaders(in []sip.Header) ([]sip.Header, error) {
	var out []sip.Header
	for _, h := range in {
		switch h.(type) {
		case *sip.ViaHeader, *sip.FromHeader, *sip.ToHeader, *sip.ContactHeader, *sip.CSeqHeader, *sip.MaxForwardsHeader, *sip.ContentLengthHeader, *sip.ContentTypeHeader, *sip.RouteHeader, *sip.RecordRouteHeader, *sip.ReferToHeader, *sip.ReferredByHeader:
			out = append(out, h)
		case *sip.CallIDHeader:
			value := strings.TrimSpace(h.Value())
			if value == "" {
				return nil, errors.New("empty Call-ID body")
			}
			if value != h.Value() {
				owned := sip.CallIDHeader(value)
				h = &owned
			}
			out = append(out, h)
		default:
			parser := sip.HeadersParser(sip.DefaultHeadersParser())
			if _, known := parser[strings.ToLower(h.Name())]; !known {
				if trimmed := strings.TrimSpace(h.Value()); trimmed != h.Value() {
					h = sip.NewHeader(h.Name(), trimmed)
				}
				out = append(out, h)
				continue
			}
			var err error
			out, err = parser.ParseHeader(out, []byte(h.Name()+": "+h.Value()))
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func structuredHeaderView(v sip.Message, headers []sip.Header, transport string) sip.Message {
	var out sip.Message
	switch input := v.(type) {
	case *sip.Request:
		request := sip.NewRequest(sip.RequestMethod(strings.ToUpper(string(input.Method))), input.Recipient)
		request.SipVersion = input.SipVersion
		out = request
	case *sip.Response:
		response := sip.NewResponse(input.StatusCode, input.Reason)
		response.SipVersion = input.SipVersion
		out = response
	}
	for _, h := range headers {
		out.AppendHeader(h)
	}
	out.SetBody(v.Body())
	out.SetTransport(strings.ToUpper(transport))
	out.SetSource(v.Source())
	out.SetDestination(v.Destination())
	return out
}

func freeze(v sip.Message, transport string, structured bool) (*frozenMessage, error) {
	if v == nil {
		return nil, errMissingIdentity
	}
	originalHeaders := v.(interface{ Headers() []sip.Header }).Headers()
	headers := originalHeaders
	if structured {
		var err error
		headers, err = constructionHeaders(headers)
		if err != nil {
			return nil, err
		}
		v = structuredHeaderView(v, headers, transport)
	}
	if v.CallID() == nil || v.From() == nil || v.To() == nil || v.CSeq() == nil || v.Via() == nil {
		return nil, errMissingIdentity
	}
	var nativeErr error
	if v.CSeq().SeqNo > math.MaxInt32 || v.CallID().Value() == "" {
		nativeErr = errors.New("invalid CSeq or Call-ID")
	} else if transport != "udp" && transport != "tcp" && transport != "tls" {
		nativeErr = errors.New("observed transport required")
	}
	r, isRequest := v.(*sip.Request)
	if nativeErr == nil && isRequest && v.CSeq().MethodName != r.Method {
		nativeErr = errors.New("CSeq method mismatch")
	}
	view := v.(interface {
		Headers() []sip.Header
		Contact() *sip.ContactHeader
	})
	f := &frozenMessage{nativeErr: nativeErr, from: ownFrom(v.From()), to: ownTo(v.To()), via: ownVia(v.Via()), contact: ownContact(view.Contact()), body: bytes.Clone(v.Body())}
	f.facts = MessageFacts{IsRequest: isRequest, Method: v.CSeq().MethodName, CallID: strings.Clone(v.CallID().Value()), FromTag: tag(f.from.Params), ToTag: tag(f.to.Params), FromURI: f.from.Address.String(), ToURI: f.to.Address.String(), CSeq: v.CSeq().SeqNo, CSeqMethod: sip.RequestMethod(strings.Clone(string(v.CSeq().MethodName))), ViaHost: f.via.Host, ViaPort: f.via.Port, ViaTransport: f.via.Transport, ViaProtocol: f.via.ProtocolName, ViaVersion: f.via.ProtocolVersion, ViaBranch: f.via.Params.GetOr("branch", ""), ViaSentBy: f.via.SentBy(), BodyLength: len(f.body)}
	f.facts.Method = f.facts.CSeqMethod
	if isRequest {
		f.version = strings.Clone(r.SipVersion)
		f.recipient = ownURI(r.Recipient)
		if r.Method != f.facts.CSeqMethod {
			f.facts.Method = sip.RequestMethod(strings.Clone(string(r.Method)))
		}
		f.facts.URI = f.recipient.String()
	} else if response, ok := v.(*sip.Response); ok {
		if structured && strings.ContainsAny(response.Reason, "\r\n") {
			return nil, errors.New("invalid reason phrase")
		}
		f.version = strings.Clone(response.SipVersion)
		f.facts.Status = response.StatusCode
		f.facts.Reason = strings.Clone(response.Reason)
	} else {
		return nil, errors.New("request or response required")
	}
	if f.contact != nil {
		f.facts.HasContact = true
		f.facts.ContactURI = f.contact.Address.String()
		f.facts.ContactHostPort = f.contact.Address.HostPort()
	}
	f.canonical = structured && len(headers) == len(originalHeaders)
	if f.canonical {
		for i, h := range headers {
			original := originalHeaders[i]
			if h.Name() != original.Name() || h.Value() != original.Value() {
				f.canonical = false
				break
			}
		}
	}
	var fromCount, toCount, idCount, cseqCount, contactCount int
	for _, h := range headers {
		switch h.(type) {
		case *sip.FromHeader:
			fromCount++
		case *sip.ToHeader:
			toCount++
		case *sip.CallIDHeader:
			idCount++
		case *sip.CSeqHeader:
			cseqCount++
		case *sip.ContactHeader:
			contactCount++
		}
	}
	if fromCount > 1 || toCount > 1 || idCount > 1 || cseqCount > 1 || contactCount > 1 {
		f.ordered = &orderedConstruction{}
		if fromCount > 1 {
			f.ordered.from = make([]sip.FromHeader, 0, fromCount)
		}
		if toCount > 1 {
			f.ordered.to = make([]sip.ToHeader, 0, toCount)
		}
		if idCount > 1 {
			f.ordered.callID = make([]sip.CallIDHeader, 0, idCount)
		}
		if cseqCount > 1 {
			f.ordered.cseq = make([]sip.CSeqHeader, 0, cseqCount)
		}
		if contactCount > 1 {
			f.ordered.contact = make([]sip.ContactHeader, 0, contactCount)
		}
	}
	f.headers = make([]Header, 0, len(headers))
	contentTypes := 0
	contentType := ""
	for _, h := range headers {
		if ordered := f.ordered; ordered != nil {
			switch value := h.(type) {
			case *sip.FromHeader:
				if fromCount > 1 {
					ordered.from = append(ordered.from, ownFrom(value))
				}
			case *sip.ToHeader:
				if toCount > 1 {
					ordered.to = append(ordered.to, ownTo(value))
				}
			case *sip.CallIDHeader:
				if idCount > 1 {
					ordered.callID = append(ordered.callID, sip.CallIDHeader(strings.Clone(value.Value())))
				}
			case *sip.CSeqHeader:
				if cseqCount > 1 {
					ordered.cseq = append(ordered.cseq, sip.CSeqHeader{SeqNo: value.SeqNo, MethodName: sip.RequestMethod(strings.Clone(string(value.MethodName)))})
				}
			case *sip.ContactHeader:
				if contactCount > 1 {
					ordered.contact = append(ordered.contact, *ownContact(value))
				}
			}
		}
		if via, ok := h.(*sip.ViaHeader); ok {
			f.vias = append(f.vias, ownVia(via))
		}
		name, value := strings.Clone(h.Name()), strings.Clone(h.Value())
		f.headers = append(f.headers, Header{Name: name, Value: value})
		if strings.EqualFold(name, "Content-Type") {
			contentTypes++
			contentType = value
		}
	}
	f.facts.SDP = len(f.body) > 0 && contentTypes == 1 && strings.EqualFold(strings.TrimSpace(strings.Split(contentType, ";")[0]), "application/sdp")
	f.clientKey, f.clientErr = sip.ClientTxKeyMake(v)
	f.serverKey, f.serverErr = sip.ServerTxKeyMake(v)
	f.cancelKey, f.cancelErr = f.serverKey, f.serverErr
	if v.CSeq().MethodName == sip.CANCEL {
		if isRequest {
			copy := r.Clone()
			copy.CSeq().MethodName = sip.INVITE
			f.cancelKey, f.cancelErr = sip.ServerTxKeyMake(copy)
		} else {
			copy := v.(*sip.Response).Clone()
			copy.CSeq().MethodName = sip.INVITE
			f.cancelKey, f.cancelErr = sip.ServerTxKeyMake(copy)
		}
	}
	return f, nil
}
func (m Message) Clone() Message { return m }
func (m Message) IsZero() bool   { return m.data == nil }
func (m Message) WireBytes() []byte {
	if m.data == nil {
		return nil
	}
	return bytes.Clone(m.data.wire)
}
func (m Message) WireLen() int {
	if m.data == nil {
		return 0
	}
	return len(m.data.wire)
}
func (m Message) EqualWire(other Message) bool { return bytes.Equal(m.rawWire(), other.rawWire()) }
func (m Message) rawWire() []byte {
	if m.data == nil {
		return nil
	}
	return m.data.wire
}
func (m Message) Transport() string {
	if m.data == nil {
		return ""
	}
	return m.data.transport
}
func (m Message) Source() string {
	if m.data == nil {
		return ""
	}
	return m.data.source
}
func (m Message) Destination() string {
	if m.data == nil {
		return ""
	}
	return m.data.destination
}
func (m Message) Facts() (MessageFacts, error) {
	f, err := m.prepare()
	if err != nil {
		return MessageFacts{}, err
	}
	return f.facts, nil
}
func (m Message) Body() ([]byte, error) {
	f, err := m.prepare()
	if err != nil {
		return nil, err
	}
	return bytes.Clone(f.body), nil
}
func (m Message) Headers() ([]Header, error) {
	f, err := m.prepare()
	if err != nil {
		return nil, err
	}
	return append([]Header(nil), f.headers...), nil
}
func (m Message) HeaderValues(name string) ([]string, error) {
	f, err := m.prepare()
	if err != nil {
		return nil, err
	}
	var values []string
	for _, h := range f.headers {
		if strings.EqualFold(name, h.Name) {
			values = append(values, h.Value)
		}
	}
	return values, nil
}
func (m Message) ClientTxKey() (string, error) {
	f, err := m.prepare()
	if err != nil {
		return "", err
	}
	return f.clientKey, f.clientErr
}
func (m Message) ServerTxKey() (string, error) {
	f, err := m.prepare()
	if err != nil {
		return "", err
	}
	return f.serverKey, f.serverErr
}
func (m Message) CancelServerTxKey() (string, error) {
	f, err := m.prepare()
	if err != nil {
		return "", err
	}
	return f.cancelKey, f.cancelErr
}

// WithObservedResponseFlow creates physical emission evidence without changing
// the selected native final. A replacement TLS flow can finalize a new variant.
func (m Message) WithObservedResponseFlow(ip string, port int) (Message, error) {
	f, err := response(m)
	if err != nil {
		return Message{}, err
	}
	if f.canonical && f.via.Params.GetOr("received", "") == ip && (!f.via.Params.Has("rport") || f.via.Params.GetOr("rport", "") == strconv.Itoa(port)) {
		return m, nil
	}
	r := f.mutableResponse()
	r.Via().Params.Add("received", ip)
	if r.Via().Params.Has("rport") {
		r.Via().Params.Add("rport", strconv.Itoa(port))
	}
	return wire(r, m.Transport(), m.Source(), m.Destination()), nil
}

// wire freezes structured native output directly, without reparsing its render.
func wire(v sip.Message, transport, source, destination string) Message {
	m := Message{data: &messageData{wire: []byte(v.String()), transport: strings.Clone(transport), source: strings.Clone(source), destination: strings.Clone(destination)}}
	m.data.retained.Store(int64(unsafe.Sizeof(*m.data) + uintptr(cap(m.data.wire)+len(transport)+len(source)+len(destination))))
	m.data.once.Do(func() { m.data.frozen, m.data.err = freeze(v, transport, true); m.data.addFrozenBytes() })
	return m
}
func request(m Message) (*frozenMessage, error) {
	f, err := m.prepare()
	if err != nil {
		return nil, err
	}
	if !f.facts.IsRequest {
		return nil, errors.New("request required")
	}
	return f, nil
}
func response(m Message) (*frozenMessage, error) {
	f, err := m.prepare()
	if err != nil {
		return nil, err
	}
	if f.facts.IsRequest {
		return nil, errors.New("response required")
	}
	return f, nil
}

func (f *frozenMessage) appendHeaders(to interface{ AppendHeader(sip.Header) }) {
	f.appendHeadersNamed(to, "")
}
func (f *frozenMessage) appendHeadersNamed(to interface{ AppendHeader(sip.Header) }, name string) {
	var viaIndex, fromIndex, toIndex, idIndex, cseqIndex, contactIndex int
	for _, h := range f.headers {
		if name != "" && !strings.EqualFold(name, h.Name) {
			continue
		}
		switch {
		case strings.EqualFold(h.Name, "Via") && viaIndex < len(f.vias):
			v := ownVia(&f.vias[viaIndex])
			viaIndex++
			to.AppendHeader(&v)
			continue
		case strings.EqualFold(h.Name, "From"):
			selected := &f.from
			if f.ordered != nil && fromIndex < len(f.ordered.from) {
				selected = &f.ordered.from[fromIndex]
			}
			fromIndex++
			v := ownFrom(selected)
			to.AppendHeader(&v)
			continue
		case strings.EqualFold(h.Name, "To"):
			selected := &f.to
			if f.ordered != nil && toIndex < len(f.ordered.to) {
				selected = &f.ordered.to[toIndex]
			}
			toIndex++
			v := ownTo(selected)
			to.AppendHeader(&v)
			continue
		case strings.EqualFold(h.Name, "Call-ID"):
			value := f.facts.CallID
			if f.ordered != nil && idIndex < len(f.ordered.callID) {
				value = f.ordered.callID[idIndex].Value()
			}
			idIndex++
			v := sip.CallIDHeader(value)
			to.AppendHeader(&v)
			continue
		case strings.EqualFold(h.Name, "CSeq"):
			v := sip.CSeqHeader{SeqNo: f.facts.CSeq, MethodName: f.facts.CSeqMethod}
			if f.ordered != nil && cseqIndex < len(f.ordered.cseq) {
				v = f.ordered.cseq[cseqIndex]
			}
			cseqIndex++
			to.AppendHeader(&v)
			continue
		case strings.EqualFold(h.Name, "Contact") && f.contact != nil:
			selected := f.contact
			if f.ordered != nil && contactIndex < len(f.ordered.contact) {
				selected = &f.ordered.contact[contactIndex]
			}
			contactIndex++
			to.AppendHeader(ownContact(selected))
			continue
		}
		to.AppendHeader(sip.NewHeader(h.Name, h.Value))
	}
}
func (f *frozenMessage) mutableRequest() *sip.Request {
	r := sip.NewRequest(f.facts.Method, ownURI(f.recipient))
	r.SipVersion = f.version
	f.appendHeaders(r)
	r.SetBody(bytes.Clone(f.body))
	return r
}
func (f *frozenMessage) mutableResponse() *sip.Response {
	r := sip.NewResponse(f.facts.Status, f.facts.Reason)
	r.SipVersion = f.version
	f.appendHeaders(r)
	r.SetBody(bytes.Clone(f.body))
	return r
}

// MarshalJSON/UnmarshalJSON preserve the existing byte-oriented portable shape.
// Storage codecs must map this shape explicitly; private facts are never stored.
func (m Message) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Wire        []byte `json:"wire"`
		Transport   string `json:"transport"`
		Source      string `json:"source,omitempty"`
		Destination string `json:"destination,omitempty"`
	}{m.rawWire(), m.Transport(), m.Source(), m.Destination()})
}
func (m *Message) UnmarshalJSON(data []byte) error {
	var v struct {
		Wire        []byte `json:"wire"`
		Transport   string `json:"transport"`
		Source      string `json:"source,omitempty"`
		Destination string `json:"destination,omitempty"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*m = RestoreMessage(v.Wire, v.Transport, v.Source, v.Destination)
	return nil
}

// WireHash and AppendWire expose evidence without a repeated copying read.
func (m Message) WireHash() [32]byte                 { return sha256.Sum256(m.rawWire()) }
func (m Message) AppendWire(dst []byte) []byte       { return append(dst, m.rawWire()...) }
func (m Message) WriteWire(w io.Writer) (int, error) { return w.Write(m.WireBytes()) }
func (f *frozenMessage) headerValues(name string) []string {
	var values []string
	for _, h := range f.headers {
		if strings.EqualFold(name, h.Name) {
			values = append(values, h.Value)
		}
	}
	return values
}

// Equal compares serialized value identity, independently of lazy preparation.
// Nil and present-empty wire remain distinct as in the existing storage shape.
func (m Message) Equal(other Message) bool {
	return m.Transport() == other.Transport() && m.Source() == other.Source() && m.Destination() == other.Destination() && (m.rawWire() == nil) == (other.rawWire() == nil) && m.EqualWire(other)
}

// ViaFacts is a copied public projection of one ordered Via value.
// Params preserves repeated/ordered parameters and is owned by the recipient.
type ViaFacts struct {
	ProtocolName, ProtocolVersion, Transport, Host string
	Port                                           int
	Params                                         []sip.HeaderKV
}

func (m Message) Vias() ([]ViaFacts, error) {
	f, err := m.prepare()
	if err != nil {
		return nil, err
	}
	return projectVias(f), nil
}
func projectVias(f *frozenMessage) []ViaFacts {
	out := make([]ViaFacts, len(f.vias))
	for i, v := range f.vias {
		out[i] = ViaFacts{ProtocolName: v.ProtocolName, ProtocolVersion: v.ProtocolVersion, Transport: v.Transport, Host: v.Host, Port: v.Port, Params: append([]sip.HeaderKV(nil), v.Params...)}
	}
	return out
}

func (m Message) EqualWireBytes(wire []byte) bool { return bytes.Equal(m.rawWire(), wire) }

// RetainedBytes charges private holder, wire, facts, catalog, body and selected
// builder data. It never forces lazy restoration. Shared strings may be charged
// twice conservatively; immutable sharing between records may also be charged
// by each retaining record. Preparation publishes its additional charge once.
func (m Message) RetainedBytes() int64 {
	if m.data == nil {
		return 0
	}
	return m.data.retained.Load()
}
func paramsBytes(p sip.HeaderParams) int64 {
	n := int64(cap(p)) * int64(unsafe.Sizeof(sip.HeaderKV{}))
	for _, v := range p {
		n += int64(len(v.K) + len(v.V))
	}
	return n
}
func uriBytes(u sip.Uri) int64 {
	return int64(len(u.Scheme)+len(u.Raw())+len(u.User)+len(u.Password)+len(u.Host)) + paramsBytes(u.UriParams) + paramsBytes(u.Headers)
}
func viaBytes(v sip.ViaHeader) int64 {
	return int64(len(v.ProtocolName)+len(v.ProtocolVersion)+len(v.Transport)+len(v.Host)) + paramsBytes(v.Params)
}
func (d *messageData) addFrozenBytes() {
	f := d.frozen
	if f == nil {
		return
	}
	n := int64(unsafe.Sizeof(*f)) + int64(cap(f.headers))*int64(unsafe.Sizeof(Header{})) + int64(cap(f.vias))*int64(unsafe.Sizeof(sip.ViaHeader{})) + int64(cap(f.body))
	n += int64(len(f.version) + len(f.facts.CallID) + len(f.facts.CSeqMethod) + len(f.facts.Reason) + len(f.facts.URI) + len(f.facts.FromURI) + len(f.facts.ToURI) + len(f.facts.ContactURI) + len(f.facts.ContactHostPort) + len(f.facts.ViaSentBy) + len(f.clientKey) + len(f.serverKey) + len(f.cancelKey))
	n += uriBytes(f.recipient) + uriBytes(f.from.Address) + paramsBytes(f.from.Params) + int64(len(f.from.DisplayName)) + uriBytes(f.to.Address) + paramsBytes(f.to.Params) + int64(len(f.to.DisplayName)) + viaBytes(f.via)
	for _, v := range f.vias {
		n += viaBytes(v)
	}
	if f.contact != nil {
		n += int64(unsafe.Sizeof(*f.contact)) + int64(len(f.contact.DisplayName)) + uriBytes(f.contact.Address) + paramsBytes(f.contact.Params)
	}
	for _, h := range f.headers {
		n += int64(len(h.Name) + len(h.Value))
	}
	for _, err := range []error{f.clientErr, f.serverErr, f.cancelErr} {
		if err != nil {
			n += int64(len(err.Error()) + 32)
		}
	}
	if o := f.ordered; o != nil {
		n += int64(unsafe.Sizeof(*o)) + int64(cap(o.from))*int64(unsafe.Sizeof(sip.FromHeader{})) + int64(cap(o.to))*int64(unsafe.Sizeof(sip.ToHeader{})) + int64(cap(o.callID))*int64(unsafe.Sizeof(sip.CallIDHeader(""))) + int64(cap(o.cseq))*int64(unsafe.Sizeof(sip.CSeqHeader{})) + int64(cap(o.contact))*int64(unsafe.Sizeof(sip.ContactHeader{}))
		for _, v := range o.from {
			n += int64(len(v.DisplayName)) + uriBytes(v.Address) + paramsBytes(v.Params)
		}
		for _, v := range o.to {
			n += int64(len(v.DisplayName)) + uriBytes(v.Address) + paramsBytes(v.Params)
		}
		for _, v := range o.callID {
			n += int64(len(v))
		}
		for _, v := range o.cseq {
			n += int64(len(v.MethodName))
		}
		for _, v := range o.contact {
			n += int64(len(v.DisplayName)) + uriBytes(v.Address) + paramsBytes(v.Params)
		}
	}
	if f.facts.Method != f.facts.CSeqMethod {
		n += int64(len(f.facts.Method))
	}
	if f.nativeErr != nil {
		n += int64(len(f.nativeErr.Error()) + 32)
	}
	d.retained.Add(n)
}

// ObservedFacts and its projection companions preserve the ingress observation
// contract: mandatory identity plus request RFC3261 branch admission, without
// portable transport/CSeq restrictions. Reducers never use these weaker reads.
func (m Message) ObservedFacts() (MessageFacts, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return MessageFacts{}, e
	}
	return f.facts, nil
}
func (m Message) ObservedHeaders() ([]Header, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return nil, e
	}
	return append([]Header(nil), f.headers...), nil
}
func (m Message) ObservedBody() ([]byte, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return nil, e
	}
	return bytes.Clone(f.body), nil
}
func (m Message) ObservedVias() ([]ViaFacts, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return nil, e
	}
	return projectVias(f), nil
}
func (m Message) ObservedClientTxKey() (string, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return "", e
	}
	return f.clientKey, f.clientErr
}
func (m Message) ObservedServerTxKey() (string, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return "", e
	}
	return f.serverKey, f.serverErr
}
func (m Message) ObservedCancelServerTxKey() (string, error) {
	f, e := m.prepareObserved()
	if e != nil {
		return "", e
	}
	return f.cancelKey, f.cancelErr
}
