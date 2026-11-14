// Package portable implements side-effect-free, serializable native SIP state.
package portable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
)

const Version = 1

func tag(v sip.HeaderParams) string { return v.GetOr("tag", "") }

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
	a, e := origin.ClientTxKey()
	if e != nil {
		return false
	}
	b, e := incoming.ClientTxKey()
	if e != nil || a != b {
		return false
	}
	v, w := o.facts, r.facts
	p, q := v.ViaPort, w.ViaPort
	if p <= 0 {
		p = int(sip.DefaultPort(v.ViaTransport))
	}
	if q <= 0 {
		q = int(sip.DefaultPort(w.ViaTransport))
	}
	return origin.Transport() == incoming.Transport() && v.CallID == w.CallID && v.FromTag == w.FromTag && (v.ToTag == "" || v.ToTag == w.ToTag) && v.CSeq == w.CSeq && v.CSeqMethod == w.CSeqMethod && v.ViaHost == w.ViaHost && p == q && v.ViaTransport == w.ViaTransport && v.ViaProtocol == w.ViaProtocol && v.ViaVersion == w.ViaVersion
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
	v, w := o.facts, r.facts
	if origin.Transport() != incoming.Transport() || v.CallID != w.CallID || v.FromTag != w.FromTag || v.CSeq != w.CSeq || v.URI != w.URI {
		return false
	}
	if (w.Method == sip.CANCEL || w.Method == sip.ACK) && w.Method != v.Method {
		if v.Method != sip.INVITE {
			return false
		}
	} else if v.Method != w.Method {
		return false
	}
	if w.Method != sip.ACK && v.ToTag != w.ToTag {
		return false
	}
	a, e := origin.ServerTxKey()
	if e != nil {
		return false
	}
	b, e := incoming.ServerTxKey()
	if w.Method == sip.CANCEL && v.Method == sip.INVITE {
		b, e = incoming.CancelServerTxKey()
	}
	return e == nil && a == b
}

// ResponseFor requires committed entropy; native response construction cannot
// generate a fresh tag here, including for automatic CANCEL/487 responses.
func responseFor(origin Message, status int, reason string, body []byte, localTag string) (*sip.Response, error) {
	if localTag != "" && !validToken(localTag) {
		return nil, errors.New("invalid reserved response tag")
	}
	f, e := request(origin)
	if e != nil {
		return nil, e
	}
	if status < 100 || status > 699 {
		return nil, errors.New("invalid status")
	}
	if status != 100 && f.facts.ToTag == "" && localTag == "" {
		return nil, errors.New("response tag required")
	}
	r := sip.NewResponse(status, reason)
	r.SipVersion = f.version
	for _, name := range []string{"Record-Route", "Via"} {
		f.appendHeadersNamed(r, name)
	}
	from, to := ownFrom(&f.from), ownTo(&f.to)
	id := sip.CallIDHeader(f.facts.CallID)
	sequence := sip.CSeqHeader{SeqNo: f.facts.CSeq, MethodName: f.facts.CSeqMethod}
	r.AppendHeader(&from)
	r.AppendHeader(&to)
	r.AppendHeader(&id)
	r.AppendHeader(&sequence)
	if status != 100 && f.facts.ToTag == "" {
		r.To().Params.Add("tag", localTag)
	}
	if val, exists := r.Via().Params.Get("rport"); exists && val == "" {
		source := origin.Source()
		if source == "" {
			// Reuse the native Request.Source fallback exactly, including
			// observed transport defaults and received/rport behavior.
			request := sip.NewRequest(f.facts.Method, sip.Uri{})
			via := ownVia(&f.via)
			request.AppendHeader(&via)
			request.SetTransport(strings.ToUpper(origin.Transport()))
			source = request.Source()
		}
		host, port, _ := net.SplitHostPort(source)
		r.Via().Params.Add("rport", port)
		r.Via().Params.Add("received", host)
	}
	if status == 100 {
		f.appendHeadersNamed(r, "Timestamp")
	}
	r.SetBody(body)
	return r, nil
}

func ResponseFor(origin Message, status int, reason string, body []byte, localTag string) (Message, error) {
	v, err := responseFor(origin, status, reason, body, localTag)
	if err != nil {
		return Message{}, err
	}
	return wire(v, origin.Transport(), "", origin.Source()), nil
}

func BuildNon2xxACK(origin, final Message) (Message, error) {
	if !MatchResponse(origin, final) {
		return Message{}, errors.New("ACK response identity mismatch")
	}
	o, _ := request(origin)
	r, _ := response(final)
	if o.facts.Method != sip.INVITE || r.facts.Status < 300 {
		return Message{}, errors.New("non-2xx INVITE final required")
	}
	a := sip.NewRequest(sip.ACK, ownURI(o.recipient))
	for _, name := range []string{"Via", "From", "Call-ID", "Route", "Max-Forwards"} {
		o.appendHeadersNamed(a, name)
	}
	to := ownTo(&r.to)
	a.AppendHeader(&to)
	a.AppendHeader(&sip.CSeqHeader{SeqNo: o.facts.CSeq, MethodName: sip.ACK})
	a.SetBody(nil)
	return wire(a, origin.Transport(), origin.Source(), origin.Destination()), nil
}

func BuildCancel(origin Message) (Message, error) {
	o, e := request(origin)
	if e != nil {
		return Message{}, e
	}
	if o.facts.Method != sip.INVITE {
		return Message{}, errors.New("INVITE required")
	}
	r := sip.NewRequest(sip.CANCEL, ownURI(o.recipient))
	for _, name := range []string{"Via", "From", "To", "Call-ID", "Route", "Max-Forwards"} {
		o.appendHeadersNamed(r, name)
	}
	r.AppendHeader(&sip.CSeqHeader{SeqNo: o.facts.CSeq, MethodName: sip.CANCEL})
	r.SetBody(nil)
	return wire(r, origin.Transport(), origin.Source(), origin.Destination()), nil
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
	of, _ := request(origin)
	rf, _ := response(challenge)
	name, auth := "WWW-Authenticate", "Authorization"
	if rf.facts.Status == 407 {
		name, auth = "Proxy-Authenticate", "Proxy-Authorization"
	} else if rf.facts.Status != 401 {
		return Message{}, errors.New("401/407 required")
	}
	values := rf.headerValues(name)
	if len(values) == 0 || credentials.Cnonce == "" || credentials.Count < 1 || !validBranch(branch) {
		return Message{}, errors.New("challenge, cnonce, count and branch required")
	}
	if of.facts.CSeq >= math.MaxInt32 {
		return Message{}, errors.New("CSeq exhausted")
	}
	c, e := digest.ParseChallenge(values[0])
	if e != nil {
		return Message{}, e
	}
	c.Algorithm = strings.ToUpper(c.Algorithm)
	opts := digest.Options{Method: string(of.facts.Method), URI: of.facts.URI, Username: credentials.Username, Password: credentials.Password, Cnonce: credentials.Cnonce, Count: credentials.Count}
	if len(of.body) > 0 {
		body := of.body
		opts.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}
	d, e := digest.Digest(c, opts)
	if e != nil {
		return Message{}, e
	}
	o := of.mutableRequest()
	o.RemoveHeader(auth)
	o.AppendHeader(sip.NewHeader(auth, d.String()))
	o.CSeq().SeqNo++
	o.Via().Params.Add("branch", branch)
	return wire(o, origin.Transport(), origin.Source(), origin.Destination()), nil
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

// ValidateDigestRetry checks untrusted computed output against the original
// immutable request and committed challenge inputs, without re-parsing either.
// This verifies derivation scope; password lookup and digest computation remain
// separate transient executor work.
func ValidateDigestRetry(origin, challenge, retry Message, cnonce string, count int, branch string) error {
	if retry.WireLen() == 0 || retry.WireLen() > 65536 {
		return errors.New("derived digest request size invalid")
	}
	o, err := origin.prepare()
	if err != nil {
		return err
	}
	if !o.facts.IsRequest {
		return errors.New("digest origin request required")
	}
	q, err := retry.prepare()
	if err != nil {
		return err
	}
	if !q.facts.IsRequest {
		return errors.New("derived digest request required")
	}
	r, err := challenge.prepare()
	if err != nil {
		return err
	}
	if r.facts.IsRequest || !MatchResponse(origin, challenge) {
		return errors.New("digest challenge mismatch")
	}
	auth, header := "Authorization", "WWW-Authenticate"
	if r.facts.Status == 407 {
		auth, header = "Proxy-Authorization", "Proxy-Authenticate"
	} else if r.facts.Status != 401 {
		return errors.New("digest challenge status required")
	}
	values := q.headerValues(auth)
	if len(values) != 1 || retry.Transport() != origin.Transport() {
		return errors.New("derived digest identity invalid")
	}
	credentials, err := digest.ParseCredentials(values[0])
	if err != nil || credentials.URI != o.facts.URI || credentials.Response == "" {
		return errors.New("derived digest inputs invalid")
	}
	values = r.headerValues(header)
	if len(values) == 0 {
		return errors.New("digest challenge header required")
	}
	parameters, err := digest.ParseChallenge(values[0])
	if err != nil || credentials.Nonce != parameters.Nonce || credentials.Realm != parameters.Realm || credentials.Opaque != parameters.Opaque {
		return errors.New("derived digest challenge inputs invalid")
	}
	if (credentials.QOP != "" || credentials.Cnonce != "") && credentials.Cnonce != cnonce {
		return errors.New("derived digest computation identity invalid")
	}
	if credentials.QOP != "" && credentials.Nc != count {
		return errors.New("derived digest computation identity invalid")
	}
	a, b := o.mutableRequest(), q.mutableRequest()
	a.RemoveHeader(auth)
	b.RemoveHeader(auth)
	a.CSeq().SeqNo++
	a.Via().Params.Add("branch", branch)
	if a.String() != b.String() {
		return errors.New("derived digest changes native request")
	}
	return nil
}
