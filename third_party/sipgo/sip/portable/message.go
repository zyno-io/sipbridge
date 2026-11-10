// Package portable implements side-effect-free, serializable native SIP state.
package portable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
)

const Version = 1

type Message struct {
	Wire        []byte `json:"wire"`
	Transport   string `json:"transport"`
	Source      string `json:"source,omitempty"`
	Destination string `json:"destination,omitempty"`
}

func (m Message) Clone() Message { m.Wire = bytes.Clone(m.Wire); return m }
func (m Message) Parse() (sip.Message, error) {
	v, err := sip.ParseMessage(m.Wire)
	if err != nil {
		return nil, err
	}
	if v.CallID() == nil || v.From() == nil || v.To() == nil || v.CSeq() == nil || v.Via() == nil {
		return nil, errors.New("missing SIP identity")
	}
	if v.CSeq().SeqNo > math.MaxInt32 || v.CallID().Value() == "" {
		return nil, errors.New("invalid CSeq or Call-ID")
	}
	if m.Transport != "udp" && m.Transport != "tcp" && m.Transport != "tls" {
		return nil, errors.New("observed transport required")
	}
	v.SetTransport(strings.ToUpper(m.Transport))
	v.SetSource(m.Source)
	v.SetDestination(m.Destination)
	if r, ok := v.(*sip.Request); ok && r.CSeq().MethodName != r.Method {
		return nil, errors.New("CSeq method mismatch")
	}
	return v, nil
}

func wire(v sip.Message, transport, source, destination string) Message {
	return Message{Wire: []byte(v.String()), Transport: transport, Source: source, Destination: destination}
}
func tag(v sip.HeaderParams) string { return v.GetOr("tag", "") }
func request(m Message) (*sip.Request, error) {
	v, e := m.Parse()
	if e != nil {
		return nil, e
	}
	r, ok := v.(*sip.Request)
	if !ok {
		return nil, errors.New("request required")
	}
	return r, nil
}
func response(m Message) (*sip.Response, error) {
	v, e := m.Parse()
	if e != nil {
		return nil, e
	}
	r, ok := v.(*sip.Response)
	if !ok {
		return nil, errors.New("response required")
	}
	return r, nil
}

// MatchResponse includes the full native origin proof, beyond branch lookup.
func MatchResponse(origin, incoming Message) bool {
	o, e := request(origin)
	if e != nil {
		return false
	}
	r, e := response(incoming)
	if e != nil {
		return false
	}
	a, e := sip.ClientTxKeyMake(o)
	if e != nil {
		return false
	}
	b, e := sip.ClientTxKeyMake(r)
	if e != nil || a != b {
		return false
	}
	v, w := o.Via(), r.Via()
	p, q := v.Port, w.Port
	if p <= 0 {
		p = int(sip.DefaultPort(v.Transport))
	}
	if q <= 0 {
		q = int(sip.DefaultPort(w.Transport))
	}
	return origin.Transport == incoming.Transport && *o.CallID() == *r.CallID() && tag(o.From().Params) == tag(r.From().Params) && (tag(o.To().Params) == "" || tag(o.To().Params) == tag(r.To().Params)) && o.CSeq().SeqNo == r.CSeq().SeqNo && o.CSeq().MethodName == r.CSeq().MethodName && v.Host == w.Host && p == q && v.Transport == w.Transport && v.ProtocolName == w.ProtocolName && v.ProtocolVersion == w.ProtocolVersion
}

// MatchRequest preserves original identity for retransmissions, ACK and CANCEL.
func MatchRequest(origin, incoming Message) bool {
	o, e := request(origin)
	if e != nil {
		return false
	}
	r, e := request(incoming)
	if e != nil {
		return false
	}
	if origin.Transport != incoming.Transport || *o.CallID() != *r.CallID() || tag(o.From().Params) != tag(r.From().Params) || o.CSeq().SeqNo != r.CSeq().SeqNo || o.Recipient.String() != r.Recipient.String() {
		return false
	}
	if (r.IsCancel() || r.IsAck()) && r.Method != o.Method {
		if !o.IsInvite() {
			return false
		}
	} else if o.Method != r.Method {
		return false
	}
	if !r.IsAck() && tag(o.To().Params) != tag(r.To().Params) {
		return false
	}
	copy := r.Clone()
	if copy.IsCancel() && o.IsInvite() {
		copy.CSeq().MethodName = sip.INVITE
	}
	a, e := sip.ServerTxKeyMake(o)
	if e != nil {
		return false
	}
	b, e := sip.ServerTxKeyMake(copy)
	return e == nil && a == b
}

// ResponseFor requires committed entropy; native response construction cannot
// generate a fresh tag here, including for automatic CANCEL/487 responses.
func ResponseFor(origin Message, status int, reason string, body []byte, localTag string) (Message, error) {
	if localTag != "" && !validToken(localTag) {
		return Message{}, errors.New("invalid reserved response tag")
	}
	r, e := request(origin)
	if e != nil {
		return Message{}, e
	}
	if status < 100 || status > 699 {
		return Message{}, errors.New("invalid status")
	}
	if status != 100 && tag(r.To().Params) == "" {
		if localTag == "" {
			return Message{}, errors.New("response tag required")
		}
		r.To().Params.Add("tag", localTag)
	}
	v := sip.NewResponseFromRequest(r, status, reason, bytes.Clone(body))
	return wire(v, origin.Transport, "", origin.Source), nil
}

func BuildNon2xxACK(origin, final Message) (Message, error) {
	if !MatchResponse(origin, final) {
		return Message{}, errors.New("ACK response identity mismatch")
	}
	o, _ := request(origin)
	r, _ := response(final)
	if !o.IsInvite() || r.StatusCode < 300 {
		return Message{}, errors.New("non-2xx INVITE final required")
	}
	a := sip.NewRequest(sip.ACK, o.Recipient)
	for _, name := range []string{"Via", "From", "Call-ID", "Route", "Max-Forwards"} {
		sip.CopyHeaders(name, o, a)
	}
	a.AppendHeader(sip.HeaderClone(r.To()))
	c := *o.CSeq()
	c.MethodName = sip.ACK
	a.AppendHeader(&c)
	a.SetBody(nil)
	return wire(a, origin.Transport, origin.Source, origin.Destination), nil
}

func BuildCancel(origin Message) (Message, error) {
	o, e := request(origin)
	if e != nil {
		return Message{}, e
	}
	if !o.IsInvite() {
		return Message{}, errors.New("INVITE required")
	}
	r := sip.NewRequest(sip.CANCEL, o.Recipient)
	for _, name := range []string{"Via", "From", "To", "Call-ID", "Route", "Max-Forwards"} {
		sip.CopyHeaders(name, o, r)
	}
	c := *o.CSeq()
	c.MethodName = sip.CANCEL
	r.AppendHeader(&c)
	r.SetBody(nil)
	return wire(r, origin.Transport, origin.Source, origin.Destination), nil
}

type DigestCredentials struct {
	Username string
	Password string `json:"-"`
	Cnonce   string
	Count    int
}

// BuildDigestRetry is deterministic: the credential executor supplies cnonce.
// Credentials are input only and never become part of portable protocol state.
func BuildDigestRetry(origin, challenge Message, credentials DigestCredentials, branch string) (Message, error) {
	if !MatchResponse(origin, challenge) {
		return Message{}, errors.New("challenge identity mismatch")
	}
	o, _ := request(origin)
	r, _ := response(challenge)
	name, auth := "WWW-Authenticate", "Authorization"
	if r.StatusCode == 407 {
		name, auth = "Proxy-Authenticate", "Proxy-Authorization"
	} else if r.StatusCode != 401 {
		return Message{}, errors.New("401/407 required")
	}
	h := r.GetHeader(name)
	if h == nil || credentials.Cnonce == "" || credentials.Count < 1 || !validBranch(branch) {
		return Message{}, errors.New("challenge, cnonce, count and branch required")
	}
	if o.CSeq().SeqNo >= math.MaxInt32 {
		return Message{}, errors.New("CSeq exhausted")
	}
	c, e := digest.ParseChallenge(h.Value())
	if e != nil {
		return Message{}, e
	}
	c.Algorithm = strings.ToUpper(c.Algorithm)
	opts := digest.Options{Method: string(o.Method), URI: o.Recipient.String(), Username: credentials.Username, Password: credentials.Password, Cnonce: credentials.Cnonce, Count: credentials.Count}
	if len(o.Body()) > 0 {
		body := bytes.Clone(o.Body())
		opts.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}
	d, e := digest.Digest(c, opts)
	if e != nil {
		return Message{}, e
	}
	o.RemoveHeader(auth)
	o.AppendHeader(sip.NewHeader(auth, d.String()))
	o.CSeq().SeqNo++
	o.Via().Params.Add("branch", branch)
	return wire(o, origin.Transport, origin.Source, origin.Destination), nil
}

func validBranch(branch string) bool {
	return strings.HasPrefix(branch, sip.RFC3261BranchMagicCookie) && len(branch) > len(sip.RFC3261BranchMagicCookie) && validToken(branch)
}

type ProtocolError struct {
	Status int
	Reason string
}

func (e *ProtocolError) Error() string       { return fmt.Sprintf("SIP %d: %s", e.Status, e.Reason) }
func reject(status int, reason string) error { return &ProtocolError{Status: status, Reason: reason} }
