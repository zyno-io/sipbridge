package portable

import (
	"errors"
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
)

// Header preserves application header order and repeated values. Protocol-owned
// identity, routing and framing headers cannot be overridden through this list.
type Header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// InitialRequest supplies every random/native identity explicitly. The driver
// must commit the resulting message before interpreting a send effect.
type InitialRequest struct {
	Method                        sip.RequestMethod
	URI, From, To                 string
	CallID, LocalTag, Branch      string
	CSeq                          uint32
	Advertise, Contact, Transport string
	Headers                       []Header
	Body                          []byte
}

func BuildInitialRequest(in InitialRequest) (Message, error) {
	if !validToken(string(in.Method)) || in.Method == sip.ACK || in.Method == sip.CANCEL || in.CSeq > math.MaxInt32 ||
		in.CallID == "" || !validToken(in.LocalTag) || !validBranch(in.Branch) || strings.ContainsAny(in.CallID, " \t\r\n\x00") || strings.ContainsAny(in.URI, "\r\n\x00") {
		return Message{}, errors.New("explicit initial method, Call-ID, tag, branch and CSeq required")
	}
	if in.Transport != "udp" && in.Transport != "tcp" && in.Transport != "tls" {
		return Message{}, errors.New("observed transport required")
	}
	var target sip.Uri
	if err := sip.ParseUri(in.URI, &target); err != nil || target.Host == "" {
		return Message{}, errors.New("invalid initial Request-URI")
	}
	if target.IsEncrypted() && in.Transport != "tls" {
		return Message{}, errors.New("SIPS requires TLS")
	}
	fromText, toText := in.From, in.To
	remaining := make([]Header, 0, len(in.Headers))
	for _, h := range in.Headers {
		switch strings.ToLower(h.Name) {
		case "from", "f":
			if fromText != "" {
				return Message{}, errors.New("duplicate initial From")
			}
			fromText = h.Value
		case "to", "t":
			if toText != "" {
				return Message{}, errors.New("duplicate initial To")
			}
			toText = h.Value
		default:
			remaining = append(remaining, h)
		}
	}
	if fromText == "" {
		fromText = "<sip:sipbridge@" + in.Advertise + ">"
	}
	if toText == "" {
		toText = "<" + target.String() + ">"
	}
	from, err := parseAddress(fromText)
	if err != nil {
		return Message{}, err
	}
	if existing := tag(from.Params); existing != "" && existing != in.LocalTag {
		return Message{}, errors.New("application From conflicts with native tag")
	}
	from.Params.Add("tag", in.LocalTag)
	to, err := parseAddress(toText)
	if err != nil {
		return Message{}, err
	}
	if tag(to.Params) != "" {
		return Message{}, errors.New("initial To must not claim a dialog")
	}
	r := sip.NewRequest(in.Method, target)
	r.AppendHeader(&sip.FromHeader{DisplayName: from.DisplayName, Address: from.Address, Params: from.Params})
	r.AppendHeader(&sip.ToHeader{DisplayName: to.DisplayName, Address: to.Address, Params: to.Params})
	id := sip.CallIDHeader(in.CallID)
	r.AppendHeader(&id)
	r.AppendHeader(&sip.CSeqHeader{SeqNo: in.CSeq, MethodName: in.Method})
	fwd := sip.MaxForwardsHeader(70)
	r.AppendHeader(&fwd)
	via, err := buildVia(in.Advertise, in.Transport, in.Branch)
	if err != nil {
		return Message{}, err
	}
	r.PrependHeader(via)
	if in.Contact != "" {
		contact, err := parseNativeContact(in.Contact)
		if err != nil {
			return Message{}, err
		}
		r.AppendHeader(&sip.ContactHeader{DisplayName: contact.DisplayName, Address: contact.Address, Params: contact.Params})
	}
	r.SetBody(in.Body)
	if len(in.Body) > 0 {
		r.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	}
	if err := applyHeaders(r, remaining); err != nil {
		return Message{}, err
	}
	return wire(r, in.Transport, "", ""), nil
}

func BuildResponseWithHeaders(origin Message, status int, reason string, body []byte, localTag, contact string, headers []Header) (Message, error) {
	if strings.ContainsAny(reason, "\r\n\x00") {
		return Message{}, errors.New("invalid reason phrase")
	}
	r, err := responseFor(origin, status, reason, body, localTag)
	if err != nil {
		return Message{}, err
	}
	o, err := request(origin)
	if err != nil {
		return Message{}, err
	}
	registration := o.facts.Method == sip.REGISTER
	if contact != "" && status != 100 && !registration {
		value, err := parseNativeContact(contact)
		if err != nil {
			return Message{}, err
		}
		r.AppendHeader(&sip.ContactHeader{DisplayName: value.DisplayName, Address: value.Address, Params: value.Params})
	}
	if len(body) > 0 {
		r.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	}
	contentHeaders := headers
	if registration && status >= 200 && status < 300 {
		contentHeaders = make([]Header, 0, len(headers))
		for _, h := range headers {
			if strings.EqualFold(h.Name, "Contact") || strings.EqualFold(h.Name, "m") {
				if strings.TrimSpace(h.Value) == "*" {
					return Message{}, errors.New("REGISTER response requires concrete binding Contacts")
				}
				value, err := parseAddress(h.Value)
				if err != nil {
					return Message{}, err
				}
				r.AppendHeader(&sip.ContactHeader{DisplayName: value.DisplayName, Address: value.Address, Params: value.Params})
			} else {
				contentHeaders = append(contentHeaders, h)
			}
		}
	}
	if err := applyHeaders(r, contentHeaders); err != nil {
		return Message{}, err
	}
	return wire(r, origin.Transport(), "", origin.Source()), nil
}

func BuildDialogRequestWithHeaders(s Dialog, method sip.RequestMethod, sequence uint32, body []byte, branch string, headers []Header) (Message, error) {
	r, destination, err := buildDialogRequest(s, method, sequence, body, branch)
	if err != nil {
		return Message{}, err
	}
	// The former wire wrapper parsed the intermediate request before adding
	// application headers. Preserve its field normalization without a packet
	// render/parse roundtrip, including comma/padded Route values.
	normalized, err := constructionHeaders(r.Headers())
	if err != nil {
		return Message{}, err
	}
	r = structuredHeaderView(r, normalized, s.Transport).(*sip.Request)
	if r.CSeq().MethodName != r.Method {
		return Message{}, errors.New("CSeq method mismatch")
	}
	if err := applyHeaders(r, headers); err != nil {
		return Message{}, err
	}
	return wire(r, s.Transport, "", destination), nil
}

func validToken(v string) bool {
	if v == "" {
		return false
	}
	for _, c := range v {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-.!%*_+`'~", c) {
			continue
		}
		return false
	}
	return true
}

func buildVia(advertise, transport, branch string) (*sip.ViaHeader, error) {
	host, portText, err := net.SplitHostPort(advertise)
	if err != nil || host == "" || strings.ContainsAny(host, "\r\n\x00") {
		return nil, errors.New("local Via host:port required")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return nil, errors.New("invalid Via port")
	}
	params := sip.NewParams()
	params.Add("branch", branch)
	params.Add("rport", "")
	return &sip.ViaHeader{ProtocolName: "SIP", ProtocolVersion: "2.0", Transport: strings.ToUpper(transport), Host: host, Port: port, Params: params}, nil
}

func parseAddress(value string) (*sip.ContactHeader, error) {
	if strings.ContainsAny(value, "\r\n\x00") {
		return nil, errors.New("invalid address value")
	}
	var uri sip.Uri
	params := sip.NewParams()
	name, err := sip.ParseAddressValue(value, &uri, &params)
	if err != nil || uri.Host == "" {
		return nil, errors.New("invalid address value")
	}
	return &sip.ContactHeader{DisplayName: name, Address: uri, Params: params}, nil
}

func parseNativeContact(value string) (*sip.ContactHeader, error) {
	if strings.Contains(value, "<") {
		return parseAddress(value)
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return nil, errors.New("invalid native Contact")
	}
	var uri sip.Uri
	if err := sip.ParseUri(value, &uri); err != nil || uri.Host == "" {
		return nil, errors.New("invalid native Contact URI")
	}
	return &sip.ContactHeader{Address: uri, Params: sip.NewParams()}, nil
}

type headerMessage interface {
	sip.Message
	RemoveHeader(string) bool
}

func applyHeaders(m headerMessage, headers []Header) error {
	for _, h := range headers {
		if !validToken(h.Name) || strings.ContainsAny(h.Value, "\r\n\x00") {
			return errors.New("invalid application header")
		}
		switch strings.ToLower(h.Name) {
		case "via", "v", "from", "f", "to", "t", "call-id", "i", "cseq", "route", "record-route", "max-forwards", "contact", "m", "content-length", "l":
			return errors.New("application header conflicts with native identity/routing/framing")
		case "content-type", "c":
			m.RemoveHeader("Content-Type")
		}
		m.AppendHeader(sip.NewHeader(h.Name, h.Value))
	}
	return nil
}
