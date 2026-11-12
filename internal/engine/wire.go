package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
	pb "sipbridge/internal/controlpb"
)

func opaque(prefix string, parts ...string) string {
	data, _ := json.Marshal(parts)
	sum := sha256.Sum256(data)
	return prefix + hex.EncodeToString(sum[:])
}
func header(m sip.Message, name string) string {
	values := m.GetHeaders(name)
	var h sip.Header
	if len(values) > 0 {
		h = values[0]
	}
	if h == nil {
		return ""
	}
	return h.Value()
}
func tag(params sip.HeaderParams) string { value, _ := params.Get("tag"); return value }

func parseObservedMessage(wire []byte) (sip.Message, error) {
	m, err := sip.ParseMessage(wire)
	if err != nil {
		return nil, err
	}
	if err := validateObservedMessage(m); err != nil {
		return nil, err
	}
	return m, nil
}

func validateObservedMessage(m sip.Message) error {
	if m == nil || m.CallID() == nil || m.CSeq() == nil || m.From() == nil || m.To() == nil || m.Via() == nil {
		return fmt.Errorf("missing mandatory SIP identity")
	}
	if _, request := m.(*sip.Request); request {
		if _, err := sip.ClientTxKeyMake(m); err != nil {
			return fmt.Errorf("RFC 3261 Via branch required")
		}
	}
	return nil
}

// Observe parses an immutable serialization. Native objects never cross control.
func Observe(wire []byte, outbound bool) (*pb.SipMessage, error) {
	m, err := parseObservedMessage(wire)
	if err != nil {
		return nil, err
	}
	return projectObservedMessage(wire, m, outbound), nil
}

// The stream parser and original bytes belong to the same framing operation.
// Reuse that parsed value only here, never across a control/store trust boundary.
func observeFramedMessage(wire []byte, m sip.Message) (*pb.SipMessage, error) {
	if err := validateObservedMessage(m); err != nil {
		return nil, err
	}
	return projectObservedMessage(wire, m, false), nil
}

func projectObservedMessage(wire []byte, m sip.Message, outbound bool) *pb.SipMessage {
	w := &pb.SipMessage{Wire: wire, Body: m.Body(), Addresses: make(map[string]*pb.AddressList)}
	var all []sip.Header
	if request, ok := m.(*sip.Request); ok {
		all = request.Headers()
	} else {
		all = m.(*sip.Response).Headers()
	}
	for _, h := range all {
		name := strings.ToLower(h.Name())
		w.Headers = append(w.Headers, &pb.Header{Name: name, Value: h.Value()})
		if name == "from" || name == "to" || name == "contact" || name == "refer-to" || name == "referred-by" || name == "p-asserted-identity" || name == "remote-party-id" {
			var uri sip.Uri
			params := sip.NewParams()
			display, err := sip.ParseAddressValue(h.Value(), &uri, &params)
			if err == nil {
				if w.Addresses[name] == nil {
					w.Addresses[name] = &pb.AddressList{}
				}
				w.Addresses[name].Values = append(w.Addresses[name].Values, &pb.Address{Name: display, Uri: uri.String(), Params: params.Items()})
			}
		}
		if via, ok := h.(*sip.ViaHeader); ok {
			w.Vias = append(w.Vias, &pb.Via{Protocol: via.Transport, Host: via.Host, Port: uint32(via.Port), Params: via.Params.Items()})
		}
	}
	request, isRequest := m.(*sip.Request)
	if isRequest {
		w.Method = string(request.Method)
		w.Uri = request.Recipient.String()
	} else {
		r := m.(*sip.Response)
		w.Method = string(r.CSeq().MethodName)
		w.Status = uint32(r.StatusCode)
		w.Reason = r.Reason
	}
	w.Selectors = observedMessageSelectors(m, outbound)
	if auth, err := digest.ParseCredentials(header(m, "Authorization")); err == nil {
		w.Authorization = &pb.DigestCredentials{Username: auth.Username, Realm: auth.Realm, Nonce: auth.Nonce, Uri: auth.URI, Response: auth.Response, Algorithm: auth.Algorithm, Qop: auth.QOP, Nc: fmt.Sprintf("%08x", auth.Nc), Cnonce: auth.Cnonce}
	}
	return w
}

// observeSelectors preserves wire validation and native key rules without
// allocating controller header/address/digest projections.
func observeSelectors(wire []byte, outbound bool) ([]string, error) {
	m, err := parseObservedMessage(wire)
	if err != nil {
		return nil, err
	}
	return observedMessageSelectors(m, outbound), nil
}

// Only immutable scalars survive the parse. Portable validation and observed
// selector validation intentionally have different acceptance rules.
type wireIdentity struct {
	Method            sip.RequestMethod
	Sequence          uint32
	ToTag             string
	ViaBranch, SentBy string
}

type wireProjection struct {
	identity         wireIdentity
	portableEligible bool
	observedError    error
	request          bool
	clientKey        string
	serverKey        string
	dialogSelector   string
	selectors        [2][]string
	projected        [2]bool
}

func projectWire(wire []byte) *wireProjection {
	p := new(wireProjection)
	m, err := sip.ParseMessage(wire)
	if err != nil {
		p.observedError = err
		return p
	}
	p.observedError = validateObservedMessage(m)
	request, isRequest := m.(*sip.Request)
	p.request = isRequest
	if m.CallID() != nil && m.From() != nil && m.To() != nil && m.CSeq() != nil && m.Via() != nil {
		p.identity = projectedWireIdentity(m)
		// These are the wire-only success conditions of pinned portable.Parse.
		// Transport is checked on each use; failures go through that parser so
		// its validation order and exact errors remain authoritative.
		p.portableEligible = m.CSeq().SeqNo <= math.MaxInt32 && m.CallID().Value() != "" && (!isRequest || request.Method == m.CSeq().MethodName)
	}
	if p.observedError == nil {
		p.clientKey, _ = sip.ClientTxKeyMake(m)
		p.serverKey, _ = observedServerTxKey(m)
		p.dialogSelector = observedDialogSelector(m)
	}
	return p
}

func projectedWireIdentity(m sip.Message) wireIdentity {
	return wireIdentity{Method: m.CSeq().MethodName, Sequence: m.CSeq().SeqNo, ToTag: tag(m.To().Params), ViaBranch: m.Via().Params.GetOr("branch", ""), SentBy: m.Via().SentBy()}
}

func (p *wireProjection) directionSelectors(outbound bool) ([]string, error) {
	if p.observedError != nil {
		return nil, p.observedError
	}
	index := 0
	if outbound {
		index = 1
	}
	if !p.projected[index] {
		key, prefix := p.clientKey, "ctx:"
		if p.request != outbound {
			key, prefix = p.serverKey, "stx:"
		}
		if key != "" {
			p.selectors[index] = append(p.selectors[index], opaque(prefix, key))
		}
		if p.dialogSelector != "" {
			p.selectors[index] = append(p.selectors[index], p.dialogSelector)
		}
		p.projected[index] = true
	}
	return p.selectors[index], nil
}

func observedServerTxKey(m sip.Message) (string, error) {
	var keyMessage sip.Message = m
	if m.CSeq().MethodName == sip.CANCEL {
		if request, ok := m.(*sip.Request); ok {
			c := request.Clone()
			c.CSeq().MethodName = sip.INVITE
			keyMessage = c
		} else {
			c := m.(*sip.Response).Clone()
			c.CSeq().MethodName = sip.INVITE
			keyMessage = c
		}
	}
	return sip.ServerTxKeyMake(keyMessage)
}

func observedDialogSelector(m sip.Message) string {
	from, to := tag(m.From().Params), tag(m.To().Params)
	if from == "" || to == "" {
		return ""
	}
	tags := []string{from, to}
	sort.Strings(tags)
	return opaque("dlg:", m.CallID().Value(), tags[0], tags[1])
}

func observedMessageSelectors(m sip.Message, outbound bool) []string {
	var selectors []string
	_, isRequest := m.(*sip.Request)

	if isRequest != outbound {
		if key, err := observedServerTxKey(m); err == nil {
			selectors = append(selectors, opaque("stx:", key))
		}
	} else if key, err := sip.ClientTxKeyMake(m); err == nil {
		selectors = append(selectors, opaque("ctx:", key))
	}
	if dialog := observedDialogSelector(m); dialog != "" {
		selectors = append(selectors, dialog)
	}
	return selectors
}

func headers(values []*pb.Header) ([]sip.Header, error) {
	out := make([]sip.Header, 0, len(values))
	for _, h := range values {
		name := strings.ToLower(h.Name)
		if strings.ContainsAny(h.Name+h.Value, "\r\n") || name == "via" || name == "cseq" || name == "to" || name == "contact" || name == "call-id" || name == "content-length" || name == "record-route" || name == "route" {
			return nil, fmt.Errorf("header is native-owned or invalid")
		}
		if name == "from" {
			var uri sip.Uri
			params := sip.NewParams()
			display, err := sip.ParseAddressValue(h.Value, &uri, &params)
			if err != nil {
				return nil, err
			}
			params.Remove("tag")
			out = append(out, &sip.FromHeader{DisplayName: display, Address: uri, Params: params})
			continue
		}
		out = append(out, sip.NewHeader(h.Name, h.Value))
	}
	return out, nil
}
