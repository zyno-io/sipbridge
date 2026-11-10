package portable

import (
	"bytes"
	"errors"
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
)

type Role string

const (
	UAC Role = "uac"
	UAS Role = "uas"
)

type DialogPhase string

const (
	DialogEarly       DialogPhase = "early"
	DialogAwaitingACK DialogPhase = "awaiting_ack"
	DialogConfirmed   DialogPhase = "confirmed"
	DialogTerminating DialogPhase = "terminating"
	DialogEnded       DialogPhase = "ended"
)

type OfferState string

const (
	NoOffer          OfferState = "none"
	LocalOffer       OfferState = "local"
	RemoteOffer      OfferState = "remote"
	LocalOfferInACK  OfferState = "local_ack"
	RemoteOfferInACK OfferState = "remote_ack"
)

type InviteState string

const (
	InvitePending     InviteState = "pending"
	InviteAwaitingACK InviteState = "awaiting_ack"
	InviteAcked       InviteState = "acked"
	InviteRejected    InviteState = "rejected"
)

type InviteProcedure struct {
	State           InviteState `json:"state"`
	Sequence        uint32      `json:"sequence"`
	Initial         bool        `json:"initial"`
	Request         Message     `json:"request"`
	Final           *Message    `json:"final,omitempty"`
	ACK             *Message    `json:"ack,omitempty"`
	CancelRequested bool        `json:"cancel_requested,omitempty"`
}
type Dialog struct {
	Version      int                `json:"version"`
	Role         Role               `json:"role"`
	Phase        DialogPhase        `json:"phase"`
	CallID       string             `json:"call_id"`
	LocalTag     string             `json:"local_tag"`
	RemoteTag    string             `json:"remote_tag"`
	LocalURI     string             `json:"local_uri"`
	RemoteURI    string             `json:"remote_uri"`
	Contact      string             `json:"contact"`
	SentBy       string             `json:"sent_by"`
	Target       string             `json:"target"`
	Routes       []string           `json:"routes"`
	Transport    string             `json:"transport"`
	LocalCSeq    uint32             `json:"local_cseq"`
	LocalSet     bool               `json:"local_set"`
	RemoteCSeq   uint32             `json:"remote_cseq"`
	RemoteSet    bool               `json:"remote_set"`
	LocalInvite  *InviteProcedure   `json:"local_invite,omitempty"`
	RemoteInvite *InviteProcedure   `json:"remote_invite,omitempty"`
	LocalBye     *Message           `json:"local_bye,omitempty"`
	Retiring     bool               `json:"retiring"`
	Offer        OfferState         `json:"offer"`
	Timers       map[Timer]Deadline `json:"timers"`
	Profile      TimerProfile       `json:"profile"`
}
type DialogInputKind string

const (
	DialogStart           DialogInputKind = "start"
	DialogReceiveRequest  DialogInputKind = "receive_request"
	DialogReceiveResponse DialogInputKind = "receive_response"
	DialogSendResponse    DialogInputKind = "send_response"
	DialogLocalRequest    DialogInputKind = "local_request"
	DialogLocalACK        DialogInputKind = "local_ack"
	DialogTimer           DialogInputKind = "timer"
	DialogTerminate       DialogInputKind = "terminate"
	DialogCancel          DialogInputKind = "cancel"
)

type DialogInput struct {
	Kind       DialogInputKind
	Now        int64
	Message    Message
	Method     sip.RequestMethod
	Body       []byte
	Branch     string
	Timer      Timer
	Generation uint64
	Offer      bool
	Answer     bool
	Headers    []Header
}

// NewDialog constructs one directional branch. For UAS, submit the chosen
// response through DialogSendResponse to arm TU timers and its response effect.
// For UAC, DialogStart reports the pending ACK after a successful response.
func NewDialog(role Role, origin, reply Message, profile TimerProfile) (Dialog, error) {
	if role != UAC && role != UAS || !profile.valid() || !MatchResponse(origin, reply) {
		return Dialog{}, errors.New("invalid dialog origin")
	}
	o, _ := request(origin)
	r, _ := response(reply)
	if !o.IsInvite() || r.StatusCode <= 100 || r.StatusCode >= 300 || tag(o.From().Params) == "" || tag(r.To().Params) == "" {
		return Dialog{}, errors.New("tagged INVITE provisional/success required")
	}
	s := Dialog{Version: Version, Role: role, Phase: DialogEarly, CallID: o.CallID().Value(), Transport: origin.Transport, Profile: profile, Timers: make(map[Timer]Deadline), Offer: NoOffer}
	p := &InviteProcedure{State: InvitePending, Sequence: o.CSeq().SeqNo, Initial: true, Request: origin.Clone()}
	var routeHeaders []sip.Header
	if role == UAC {
		s.LocalTag, s.RemoteTag = tag(o.From().Params), tag(r.To().Params)
		s.LocalURI, s.RemoteURI = o.From().Address.String(), r.To().Address.String()
		s.LocalCSeq, s.LocalSet = o.CSeq().SeqNo, true
		s.LocalInvite = p
		if r.Contact() == nil {
			return Dialog{}, errors.New("remote Contact required")
		}
		s.Target = r.Contact().Address.String()
		routeHeaders = r.GetHeaders("Record-Route")
		for i := len(routeHeaders) - 1; i >= 0; i-- {
			s.Routes = append(s.Routes, routeHeaders[i].Value())
		}
		if o.Contact() != nil {
			s.Contact = o.Contact().Address.String()
		}
		s.SentBy = o.Via().SentBy()
		if r.IsSuccess() {
			p.State = InviteAwaitingACK
			p.Final = cloneMessage(&reply)
			if !s.Retiring {
				s.Phase = DialogAwaitingACK
			}
		}
		if isSDP(o) && !isSDP(r) {
			s.Offer = LocalOffer
		}
		if !isSDP(o) && isSDP(r) {
			s.Offer = RemoteOfferInACK
		}
	} else {
		s.LocalTag, s.RemoteTag = tag(r.To().Params), tag(o.From().Params)
		s.LocalURI, s.RemoteURI = r.To().Address.String(), o.From().Address.String()
		s.RemoteCSeq, s.RemoteSet = o.CSeq().SeqNo, true
		s.RemoteInvite = p
		if o.Contact() == nil {
			return Dialog{}, errors.New("remote Contact required")
		}
		s.Target = o.Contact().Address.String()
		routeHeaders = o.GetHeaders("Record-Route")
		for _, h := range routeHeaders {
			s.Routes = append(s.Routes, h.Value())
		}
		if r.Contact() != nil {
			s.Contact = r.Contact().Address.String()
			s.SentBy = r.Contact().Address.HostPort()
		}
		if isSDP(o) {
			s.Offer = RemoteOffer
		}
	}
	if err := s.Validate(); err != nil {
		return Dialog{}, err
	}
	return s, nil
}

func isSDP(m sip.Message) bool {
	headers := m.GetHeaders("Content-Type")
	return len(m.Body()) > 0 && len(headers) == 1 && strings.EqualFold(strings.TrimSpace(strings.Split(headers[0].Value(), ";")[0]), "application/sdp")
}
func cloneProcedure(p *InviteProcedure) *InviteProcedure {
	if p == nil {
		return nil
	}
	v := *p
	v.Request = p.Request.Clone()
	v.Final = cloneMessage(p.Final)
	v.ACK = cloneMessage(p.ACK)
	return &v
}
func (s Dialog) Clone() Dialog {
	s.Routes = append([]string(nil), s.Routes...)
	s.LocalInvite = cloneProcedure(s.LocalInvite)
	s.RemoteInvite = cloneProcedure(s.RemoteInvite)
	s.LocalBye = cloneMessage(s.LocalBye)
	m := make(map[Timer]Deadline, len(s.Timers))
	for k, v := range s.Timers {
		m[k] = v
	}
	s.Timers = m
	return s
}
func MatchDialogRequest(s Dialog, m Message) bool {
	r, e := request(m)
	return e == nil && m.Transport == s.Transport && r.CallID().Value() == s.CallID && tag(r.To().Params) == s.LocalTag && tag(r.From().Params) == s.RemoteTag && s.LocalTag != "" && s.RemoteTag != ""
}
func MatchDialogResponse(s Dialog, m Message) bool {
	r, e := response(m)
	return e == nil && m.Transport == s.Transport && r.CallID().Value() == s.CallID && tag(r.From().Params) == s.LocalTag && tag(r.To().Params) == s.RemoteTag && s.LocalTag != "" && s.RemoteTag != ""
}

// BuildDialogRequest freezes role-correct loose/strict routes and supplied Via
// branch without entropy or CSeq allocation. StepDialog owns sequence allocation.
func BuildDialogRequest(s Dialog, method sip.RequestMethod, sequence uint32, body []byte, branch string) (Message, error) {
	if !validToken(string(method)) || !validToken(s.LocalTag) || !validToken(s.RemoteTag) || sequence > math.MaxInt32 ||
		(s.Transport != "udp" && s.Transport != "tcp" && s.Transport != "tls") || !validBranch(branch) ||
		strings.ContainsAny(s.Contact+s.SentBy+s.Target+s.LocalURI+s.RemoteURI+s.CallID, "\r\n\x00") {
		return Message{}, errors.New("invalid dialog build identity")
	}
	var target, local, remote sip.Uri
	for _, v := range []struct {
		text string
		uri  *sip.Uri
	}{{s.Target, &target}, {s.LocalURI, &local}, {s.RemoteURI, &remote}} {
		if e := sip.ParseUri(v.text, v.uri); e != nil {
			return Message{}, e
		}
	}
	if target.IsEncrypted() && s.Transport != "tls" {
		return Message{}, errors.New("SIPS requires TLS")
	}
	routes := append([]string(nil), s.Routes...)
	destination := ""
	if len(routes) > 0 {
		var first sip.Uri
		params := sip.NewParams()
		if _, e := sip.ParseAddressValue(routes[0], &first, &params); e != nil {
			return Message{}, e
		}
		destination = first.HostPort()
		if !first.UriParams.Has("lr") {
			routes = append(routes[1:], "<"+target.String()+">")
			target = first
		}
	}
	r := sip.NewRequest(method, target)
	from, to := sip.NewParams(), sip.NewParams()
	from.Add("tag", s.LocalTag)
	to.Add("tag", s.RemoteTag)
	r.AppendHeader(&sip.FromHeader{Address: local, Params: from})
	r.AppendHeader(&sip.ToHeader{Address: remote, Params: to})
	id := sip.CallIDHeader(s.CallID)
	r.AppendHeader(&id)
	r.AppendHeader(&sip.CSeqHeader{SeqNo: sequence, MethodName: method})
	fwd := sip.MaxForwardsHeader(70)
	r.AppendHeader(&fwd)
	host, portText, e := net.SplitHostPort(s.SentBy)
	if e != nil {
		return Message{}, errors.New("local Via sent-by host:port required")
	}
	port, e := strconv.Atoi(portText)
	if e != nil || port < 1 || port > 65535 {
		return Message{}, errors.New("invalid Via port")
	}
	vp := sip.NewParams()
	vp.Add("branch", branch)
	vp.Add("rport", "")
	r.PrependHeader(&sip.ViaHeader{ProtocolName: "SIP", ProtocolVersion: "2.0", Transport: strings.ToUpper(s.Transport), Host: host, Port: port, Params: vp})
	for _, route := range routes {
		if strings.ContainsAny(route, "\r\n") {
			return Message{}, errors.New("invalid route")
		}
		r.AppendHeader(sip.NewHeader("Route", route))
	}
	if s.Contact != "" && method != sip.ACK {
		var contact sip.Uri
		if e := sip.ParseUri(s.Contact, &contact); e != nil {
			return Message{}, e
		}
		r.AppendHeader(&sip.ContactHeader{Address: contact})
	}
	r.SetBody(bytes.Clone(body))
	if len(body) > 0 {
		r.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	}
	return wire(r, s.Transport, "", destination), nil
}

type dialogTransition struct {
	state   Dialog
	effects []Effect
	now     int64
	err     error
}

func (d *dialogTransition) emit(k EffectKind, m *Message, replay bool) {
	d.effects = append(d.effects, Effect{Kind: k, Message: cloneMessage(m), Replay: replay})
}
func (d *dialogTransition) schedule(k Timer, after, interval int64) {
	v := d.state.Timers[k]
	if v.Generation == math.MaxUint64 || after < 0 || d.now > math.MaxInt64-after {
		d.err = errors.New("deadline exhausted")
		return
	}
	v = Deadline{Generation: v.Generation + 1, At: d.now + after, Interval: interval, Active: true}
	d.state.Timers[k] = v
	d.effects = append(d.effects, Effect{Kind: Schedule, Timer: k, Deadline: v})
}
func (d *dialogTransition) stop(k Timer) {
	v := d.state.Timers[k]
	if v.Active {
		v.Active = false
		d.state.Timers[k] = v
		d.effects = append(d.effects, Effect{Kind: CancelTimer, Timer: k, Deadline: v})
	}
}
func (d *dialogTransition) end() {
	d.state.Retiring = true
	if d.state.Phase == DialogEnded {
		return
	}
	d.state.Phase = DialogEnded
	d.stop(Timer2xx)
	d.stop(TimerACK)
	d.emit(Ended, nil, false)
}
func pending(p *InviteProcedure) bool {
	return p != nil && (p.State == InvitePending || p.State == InviteAwaitingACK)
}

func StepDialog(s Dialog, in DialogInput) (Dialog, []Effect, error) {
	if s.Validate() != nil || in.Now < 0 {
		return s, nil, errors.New("invalid dialog state/environment")
	}
	d := dialogTransition{state: s.Clone(), now: in.Now}
	if d.state.Timers == nil {
		d.state.Timers = make(map[Timer]Deadline)
	}
	if in.Kind == DialogTerminate {
		d.end()
		return d.state, d.effects, nil
	}
	if s.Phase == DialogEnded && in.Kind != DialogReceiveResponse && in.Kind != DialogLocalACK {
		return s, nil, reject(481, "dialog ended")
	}
	switch in.Kind {
	case DialogStart:
		if s.LocalInvite != nil && s.LocalInvite.State == InviteAwaitingACK {
			d.emit(NeedACK, s.LocalInvite.Final, false)
		}
	case DialogReceiveRequest:
		if !MatchDialogRequest(s, in.Message) {
			return s, nil, reject(481, "directional dialog identity mismatch")
		}
		r, _ := request(in.Message)
		if r.IsAck() {
			p := d.state.RemoteInvite
			if p == nil || p.Sequence != r.CSeq().SeqNo || p.State != InviteAwaitingACK && p.State != InviteAcked {
				return s, nil, reject(500, "ACK INVITE sequence mismatch")
			}
			if p.State == InviteAcked {
				return d.state, nil, nil
			}
			if s.Offer == LocalOfferInACK && !isSDP(r) {
				return s, nil, reject(488, "ACK answer required")
			}
			p.State = InviteAcked
			p.ACK = cloneMessage(&in.Message)
			d.state.Offer = NoOffer
			if !d.state.Retiring {
				d.state.Phase = DialogConfirmed
			}
			d.stop(Timer2xx)
			d.stop(TimerACK)
			d.emit(Acknowledged, &in.Message, false)
			break
		}
		if r.IsCancel() {
			return s, nil, reject(481, "CANCEL requires exact transaction association")
		}
		if (s.Retiring || s.Phase == DialogTerminating) && r.Method != sip.BYE {
			return s, nil, reject(481, "dialog terminating")
		}
		if s.RemoteSet && r.CSeq().SeqNo <= s.RemoteCSeq {
			return s, nil, reject(500, "remote CSeq must increase")
		}
		if r.IsInvite() && (pending(s.LocalInvite) || pending(s.RemoteInvite) || s.Offer != NoOffer) {
			return s, nil, reject(491, "INVITE procedure pending")
		}
		d.state.RemoteCSeq, d.state.RemoteSet = r.CSeq().SeqNo, true
		if r.IsInvite() {
			d.state.RemoteInvite = &InviteProcedure{State: InvitePending, Sequence: r.CSeq().SeqNo, Request: in.Message.Clone()}
			if in.Offer || isSDP(r) {
				d.state.Offer = RemoteOffer
			}
		}
		if r.Method == sip.BYE {
			m, e := ResponseFor(in.Message, 200, "OK", nil, s.LocalTag)
			if e != nil {
				return s, nil, e
			}
			d.emit(Respond, &m, false)
			d.end()
		} else {
			d.emit(PresentRequest, &in.Message, false)
		}
	case DialogLocalRequest:
		if (s.Retiring || s.Phase == DialogTerminating) && (in.Method != sip.BYE || s.LocalBye != nil || s.LocalInvite != nil && s.LocalInvite.State != InviteAcked) {
			return s, nil, reject(481, "dialog terminating")
		}
		if in.Method == sip.ACK || in.Method == sip.CANCEL {
			return s, nil, errors.New("use ACK procedure or transaction CANCEL")
		}
		if in.Method == sip.INVITE && (pending(s.LocalInvite) || pending(s.RemoteInvite) || s.Offer != NoOffer) {
			return s, nil, reject(491, "INVITE procedure pending")
		}
		if s.LocalCSeq >= math.MaxInt32 {
			return s, nil, errors.New("CSeq exhausted")
		}
		seq := s.LocalCSeq + 1
		m, e := BuildDialogRequestWithHeaders(s, in.Method, seq, in.Body, in.Branch, in.Headers)
		if e != nil {
			return s, nil, e
		}
		d.state.LocalCSeq, d.state.LocalSet = seq, true
		if in.Method == sip.INVITE {
			d.state.LocalInvite = &InviteProcedure{State: InvitePending, Sequence: seq, Request: m.Clone()}
			if in.Offer || len(in.Body) > 0 {
				d.state.Offer = LocalOffer
			}
		}
		if in.Method == sip.BYE {
			d.state.Retiring = true
			d.state.Phase = DialogTerminating
			d.state.LocalBye = cloneMessage(&m)
		}
		d.emit(StartTransaction, &m, false)
	case DialogLocalACK:
		p := d.state.LocalInvite
		if p == nil || p.State != InviteAwaitingACK && p.State != InviteAcked {
			return s, nil, errors.New("no successful INVITE ACK obligation")
		}
		if p.ACK != nil {
			d.emit(Send, p.ACK, true)
			break
		}
		if s.Offer == RemoteOfferInACK && len(in.Body) == 0 {
			return s, nil, reject(488, "ACK answer required")
		}
		m, e := BuildDialogRequestWithHeaders(s, sip.ACK, p.Sequence, in.Body, in.Branch, in.Headers)
		if e != nil {
			return s, nil, e
		}
		p.ACK = &m
		p.State = InviteAcked
		d.state.Offer = NoOffer
		if !d.state.Retiring {
			d.state.Phase = DialogConfirmed
		}
		d.emit(Send, &m, false)
		if p.CancelRequested && d.state.LocalBye == nil && d.state.Phase != DialogEnded {
			d.state.Retiring = true
			d.state.Phase = DialogTerminating
			d.state.Retiring = true
			d.emit(NeedBYE, p.Final, false)
		}
	case DialogCancel:
		p := d.state.LocalInvite
		if p == nil {
			return s, nil, errors.New("no local INVITE to cancel")
		}
		p.CancelRequested = true
		d.state.Retiring = true
		d.state.Phase = DialogTerminating
		if p.State == InviteAwaitingACK {
			d.emit(NeedACK, p.Final, false)
		} else if p.State == InviteAcked && s.LocalBye == nil {
			d.state.Phase = DialogTerminating
			d.state.Retiring = true
			d.emit(NeedBYE, p.Final, false)
		}
	case DialogSendResponse:
		d.reply(in.Message, in.Answer, in.Offer)
	case DialogReceiveResponse:
		d.receive(in.Message)
	case DialogTimer:
		v := s.Timers[in.Timer]
		if !v.Active || v.Generation != in.Generation || in.Now < v.At {
			return d.state, nil, nil
		}
		v.Active = false
		d.state.Timers[in.Timer] = v
		if in.Timer == TimerACK {
			d.stop(Timer2xx)
			d.state.Phase = DialogTerminating
			d.state.Retiring = true
			d.emit(NeedBYE, nil, false)
		} else if in.Timer == Timer2xx && s.RemoteInvite != nil && s.RemoteInvite.State == InviteAwaitingACK {
			if a := s.Timers[TimerACK]; a.Active && in.Now >= a.At {
				d.stop(Timer2xx)
				d.stop(TimerACK)
				d.state.Phase = DialogTerminating
				d.state.Retiring = true
				d.emit(NeedBYE, nil, false)
			} else {
				d.emit(Respond, s.RemoteInvite.Final, true)
				interval := min(min(v.Interval, math.MaxInt64/2)*2, s.Profile.T2)
				d.schedule(Timer2xx, interval, interval)
			}
		}
	default:
		return s, nil, errors.New("unknown dialog input")
	}
	if d.err != nil {
		return s, nil, d.err
	}
	return d.state, d.effects, nil
}

func (s Dialog) Validate() error {
	if s.Version != Version || !s.Profile.valid() || s.Role != UAC && s.Role != UAS || !validToken(s.LocalTag) || !validToken(s.RemoteTag) || s.CallID == "" || s.LocalCSeq > math.MaxInt32 || s.RemoteCSeq > math.MaxInt32 || s.Transport != "udp" && s.Transport != "tcp" && s.Transport != "tls" {
		return errors.New("invalid dialog state")
	}
	switch s.Phase {
	case DialogEarly, DialogAwaitingACK, DialogConfirmed, DialogTerminating, DialogEnded:
	default:
		return errors.New("unknown dialog phase")
	}
	if (s.Phase == DialogTerminating || s.Phase == DialogEnded) != s.Retiring {
		return errors.New("inconsistent retirement state")
	}
	if s.LocalBye != nil && !s.Retiring {
		return errors.New("BYE without retirement")
	}
	switch s.Offer {
	case NoOffer, LocalOffer, RemoteOffer, LocalOfferInACK, RemoteOfferInACK:
	default:
		return errors.New("unknown offer phase")
	}
	for _, p := range []*InviteProcedure{s.LocalInvite, s.RemoteInvite} {
		if p == nil {
			continue
		}
		switch p.State {
		case InvitePending, InviteAwaitingACK, InviteAcked, InviteRejected:
		default:
			return errors.New("unknown INVITE procedure")
		}
		if p.Sequence > math.MaxInt32 {
			return errors.New("invalid INVITE sequence")
		}
		r, e := request(p.Request)
		if e != nil || !r.IsInvite() || r.CSeq().SeqNo != p.Sequence || r.CallID().Value() != s.CallID {
			return errors.New("retained INVITE identity mismatch")
		}
	}
	for k, v := range s.Timers {
		if k != Timer2xx && k != TimerACK || v.At < 0 || v.Interval < 0 || v.Active && v.Generation == 0 {
			return errors.New("invalid dialog timer")
		}
	}
	return nil
}

func (d *dialogTransition) reply(m Message, answer, offer bool) {
	s := &d.state
	p := s.RemoteInvite
	if p == nil || !MatchResponse(p.Request, m) {
		d.err = reject(481, "response does not match remote INVITE")
		return
	}
	r, _ := response(m)
	if tag(r.To().Params) != s.LocalTag {
		d.err = reject(481, "response local tag mismatch")
		return
	}
	if p.State == InviteRejected || p.State == InviteAcked {
		if p.Final == nil || !bytes.Equal(p.Final.Wire, m.Wire) {
			d.err = errors.New("conflicting INVITE final")
		}
		return
	}
	if p.State == InviteAwaitingACK {
		if r.IsSuccess() && p.Final != nil && bytes.Equal(p.Final.Wire, m.Wire) {
			d.emit(Respond, p.Final, true)
		} else {
			d.err = errors.New("conflicting INVITE final")
		}
		return
	}
	if r.IsProvisional() {
		if s.Offer == RemoteOffer && isSDP(r) {
			s.Offer = NoOffer
		}
		d.emit(Respond, &m, false)
		return
	}
	p.Final = cloneMessage(&m)
	if r.IsSuccess() {
		p.State = InviteAwaitingACK
		if r.Contact() != nil {
			s.Contact = r.Contact().Address.String()
			if s.SentBy == "" {
				s.SentBy = r.Contact().Address.HostPort()
			}
		}
		if !s.Retiring {
			s.Phase = DialogAwaitingACK
		}
		o, _ := request(p.Request)
		if o.Contact() != nil {
			s.Target = o.Contact().Address.String()
		}
		if s.Offer == RemoteOffer && !isSDP(r) {
			d.err = reject(488, "INVITE answer required")
			return
		}
		if !isSDP(o) && isSDP(r) {
			s.Offer = LocalOfferInACK
		} else {
			s.Offer = NoOffer
		}
		d.schedule(Timer2xx, s.Profile.T1, s.Profile.T1)
		d.schedule(TimerACK, 64*s.Profile.T1, 0)
	} else {
		p.State = InviteRejected
		s.Offer = NoOffer
		if p.Initial {
			d.end()
		}
	}
	d.emit(Respond, &m, false)
}

func (d *dialogTransition) receive(m Message) {
	s := &d.state
	if s.LocalBye != nil && MatchResponse(*s.LocalBye, m) && MatchDialogResponse(*s, m) {
		r, _ := response(m)
		d.emit(ReportResponse, &m, false)
		if !r.IsProvisional() {
			d.end()
		}
		return
	}
	p := s.LocalInvite
	if p == nil || !MatchResponse(p.Request, m) || !MatchDialogResponse(*s, m) {
		d.err = reject(481, "response does not match local INVITE branch")
		return
	}
	r, _ := response(m)
	if r.IsProvisional() {
		if p.State == InvitePending && s.Offer == LocalOffer && isSDP(r) {
			s.Offer = NoOffer
		}
		if p.State == InvitePending {
			d.emit(ReportResponse, &m, false)
		}
		return
	}
	if p.State == InviteRejected {
		if p.Final == nil || !bytes.Equal(p.Final.Wire, m.Wire) {
			d.err = reject(481, "conflicting INVITE final")
			return
		}
		d.emit(ReportResponse, p.Final, true)
		return
	}
	if p.State == InviteAcked || p.State == InviteAwaitingACK {
		if !r.IsSuccess() || p.Final == nil || !bytes.Equal(p.Final.Wire, m.Wire) {
			d.err = reject(481, "conflicting INVITE final")
			return
		}
		if p.ACK != nil {
			d.emit(Send, p.ACK, true)
		} else {
			d.emit(NeedACK, &m, true)
		}
		return
	}
	p.Final = cloneMessage(&m)
	if r.IsSuccess() {
		if r.Contact() == nil {
			d.err = errors.New("successful INVITE Contact required")
			return
		}
		s.Target = r.Contact().Address.String()
		if p.Initial {
			s.Routes = nil
			headers := r.GetHeaders("Record-Route")
			for i := len(headers) - 1; i >= 0; i-- {
				s.Routes = append(s.Routes, headers[i].Value())
			}
		}
		p.State = InviteAwaitingACK
		if !s.Retiring {
			s.Phase = DialogAwaitingACK
		}
		o, _ := request(p.Request)
		if s.Offer == LocalOffer && !isSDP(r) {
			d.err = reject(488, "INVITE answer required")
			return
		}
		if !isSDP(o) && isSDP(r) {
			s.Offer = RemoteOfferInACK
		} else {
			s.Offer = NoOffer
		}
		d.emit(NeedACK, &m, false)
	} else {
		p.State = InviteRejected
		s.Offer = NoOffer
		if p.Initial {
			d.end()
		}
	}
	d.emit(ReportResponse, &m, false)
}
