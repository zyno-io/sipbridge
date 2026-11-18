package portable

import (
	"errors"
	"math"

	"github.com/emiago/sipgo/sip"
)

type Family string

const (
	InviteClient    Family = "invite_client"
	NonInviteClient Family = "noninvite_client"
	InviteServer    Family = "invite_server"
	NonInviteServer Family = "noninvite_server"
)

type Phase string

const (
	Idle       Phase = "idle"
	Calling    Phase = "calling"
	Trying     Phase = "trying"
	Proceeding Phase = "proceeding"
	Completed  Phase = "completed"
	Accepted   Phase = "accepted"
	Confirmed  Phase = "confirmed"
	Terminated Phase = "terminated"
)

type Timer string

const (
	TimerA      Timer = "A"
	TimerB      Timer = "B"
	TimerD      Timer = "D"
	TimerE      Timer = "E"
	TimerF      Timer = "F"
	TimerG      Timer = "G"
	TimerH      Timer = "H"
	TimerI      Timer = "I"
	TimerJ      Timer = "J"
	TimerK      Timer = "K"
	TimerL      Timer = "L"
	TimerM      Timer = "M"
	TimerTrying Timer = "trying"
	Timer2xx    Timer = "2xx"
	TimerACK    Timer = "ACK"
	TimerCancel Timer = "cancel"
)

type TimerProfile struct {
	T1              int64 `json:"t1"`
	T2              int64 `json:"t2"`
	T4              int64 `json:"t4"`
	Trying          int64 `json:"trying"`
	InviteCompleted int64 `json:"invite_completed"`
}

func DefaultTimers() TimerProfile {
	return TimerProfile{T1: 500, T2: 4000, T4: 5000, Trying: 200, InviteCompleted: 32000}
}
func (p TimerProfile) valid() bool {
	return p.T1 > 0 && p.T1 <= math.MaxInt64/64 && p.T2 >= p.T1 && p.T4 > 0 && p.Trying >= 0 && p.InviteCompleted >= 0
}

type Deadline struct {
	Generation uint64 `json:"generation"`
	At         int64  `json:"at"`
	Interval   int64  `json:"interval"`
	Active     bool   `json:"active"`
}
type InputKind string

const (
	Start            InputKind = "start"
	ReceiveRequest   InputKind = "receive_request"
	ReceiveResponse  InputKind = "receive_response"
	SendResponse     InputKind = "send_response"
	TimerFired       InputKind = "timer"
	TransportFailure InputKind = "transport_failure"
	Terminate        InputKind = "terminate"
	RequestCancel    InputKind = "request_cancel"
)

type Input struct {
	Kind       InputKind
	Now        int64
	Message    Message
	Timer      Timer
	Generation uint64
	Error      string
}
type EffectKind string

const (
	Send             EffectKind = "send"
	PresentRequest   EffectKind = "present_request"
	ReportResponse   EffectKind = "report_response"
	Acknowledged     EffectKind = "acknowledged"
	Cancelled        EffectKind = "cancelled"
	Ended            EffectKind = "ended"
	Schedule         EffectKind = "schedule"
	CancelTimer      EffectKind = "cancel_timer"
	Diagnostic       EffectKind = "diagnostic"
	StartTransaction EffectKind = "start_transaction"
	Respond          EffectKind = "respond"
	NeedACK          EffectKind = "need_ack"
	NeedBYE          EffectKind = "need_bye"
	NeedCANCEL       EffectKind = "need_cancel"
)

type Effect struct {
	Kind     EffectKind `json:"kind"`
	Message  *Message   `json:"message,omitempty"`
	Timer    Timer      `json:"timer,omitempty"`
	Deadline Deadline   `json:"deadline,omitempty"`
	Replay   bool       `json:"replay,omitempty"`
	Error    string     `json:"error,omitempty"`
}
type Transaction struct {
	Version      int                `json:"version"`
	Family       Family             `json:"family"`
	Phase        Phase              `json:"phase"`
	Key          string             `json:"key"`
	Origin       Message            `json:"origin"`
	LastResponse *Message           `json:"last_response,omitempty"`
	ACK          *Message           `json:"ack,omitempty"`
	ResponseTag  string             `json:"response_tag,omitempty"`
	Reliable     bool               `json:"reliable"`
	Timers       map[Timer]Deadline `json:"timers"`
	Profile      TimerProfile       `json:"profile"`
	Cancelled    bool               `json:"cancelled"`
	CancelSent   bool               `json:"cancel_sent"`
	Error        string             `json:"error,omitempty"`
}

func NewTransaction(f Family, origin Message, profile TimerProfile, localTag string) (Transaction, error) {
	o, e := request(origin)
	if e != nil {
		return Transaction{}, e
	}
	if !profile.valid() {
		return Transaction{}, errors.New("invalid timer profile")
	}
	invite := f == InviteClient || f == InviteServer
	if f != InviteClient && f != InviteServer && f != NonInviteClient && f != NonInviteServer {
		return Transaction{}, errors.New("unknown transaction family")
	}
	if (o.facts.Method == sip.ACK) || (o.facts.Method == sip.INVITE) != invite {
		return Transaction{}, errors.New("transaction method/family mismatch")
	}
	var key string
	if f == InviteClient || f == NonInviteClient {
		key, e = origin.ClientTxKey()
	} else {
		key, e = origin.ServerTxKey()
	}
	if e != nil {
		return Transaction{}, e
	}
	if o.facts.ToTag != "" {
		localTag = o.facts.ToTag
	}
	if (f == InviteServer || f == NonInviteServer) && localTag == "" {
		return Transaction{}, errors.New("reserved response tag required")
	}
	s := Transaction{Version: Version, Family: f, Phase: Idle, Key: key, Origin: origin.Clone(), ResponseTag: localTag, Reliable: origin.Transport() != "udp", Timers: make(map[Timer]Deadline), Profile: profile}
	if err := s.Validate(); err != nil {
		return Transaction{}, err
	}
	return s, nil
}
func cloneMessage(m *Message) *Message {
	if m == nil {
		return nil
	}
	v := m.Clone()
	return &v
}
func (s Transaction) Clone() Transaction {
	s.Origin = s.Origin.Clone()
	s.LastResponse = cloneMessage(s.LastResponse)
	s.ACK = cloneMessage(s.ACK)
	timers := make(map[Timer]Deadline, len(s.Timers))
	for k, v := range s.Timers {
		timers[k] = v
	}
	s.Timers = timers
	return s
}

type transition struct {
	state   Transaction
	effects []Effect
	err     error
	now     int64
}

func (t *transition) emit(k EffectKind, m *Message, replay bool) {
	t.effects = append(t.effects, Effect{Kind: k, Message: cloneMessage(m), Replay: replay})
}
func (t *transition) schedule(k Timer, after, interval int64) {
	d := t.state.Timers[k]
	if after < 0 || t.now > math.MaxInt64-after || d.Generation == math.MaxUint64 {
		t.err = errors.New("deadline exhausted")
		return
	}
	d = Deadline{Generation: d.Generation + 1, At: t.now + after, Interval: interval, Active: true}
	t.state.Timers[k] = d
	t.effects = append(t.effects, Effect{Kind: Schedule, Timer: k, Deadline: d})
}
func (t *transition) stop(k Timer) {
	d := t.state.Timers[k]
	if d.Active {
		d.Active = false
		t.state.Timers[k] = d
		t.effects = append(t.effects, Effect{Kind: CancelTimer, Timer: k, Deadline: d})
	}
}
func (t *transition) finish(cause string) {
	if t.state.Phase == Terminated {
		return
	}
	t.state.Phase = Terminated
	t.state.Error = cause
	for _, k := range []Timer{TimerA, TimerB, TimerD, TimerE, TimerF, TimerG, TimerH, TimerI, TimerJ, TimerK, TimerL, TimerM, TimerTrying, TimerCancel} {
		t.stop(k)
	}
	t.effects = append(t.effects, Effect{Kind: Ended, Error: cause})
}

// StepTransaction never mutates s or in. Driver commits state/effects before I/O.
func StepTransaction(s Transaction, in Input) (Transaction, []Effect, error) {
	if s.Validate() != nil || in.Now < 0 {
		return s, nil, errors.New("invalid transaction state/environment")
	}
	t := transition{state: s.Clone(), now: in.Now}
	if t.state.Timers == nil {
		t.state.Timers = make(map[Timer]Deadline)
	}
	client := s.Family == InviteClient || s.Family == NonInviteClient
	if s.Phase == Terminated {
		return t.state, nil, nil
	}
	if in.Kind == TimerFired {
		d := s.Timers[in.Timer]
		if !d.Active || d.Generation != in.Generation || in.Now < d.At {
			return t.state, nil, nil
		}
		d.Active = false
		t.state.Timers[in.Timer] = d
	}
	switch in.Kind {
	case Start:
		if s.Phase != Idle {
			return t.state, nil, nil
		}
		if client {
			if s.Family == InviteClient {
				t.state.Phase = Calling
				if !s.Reliable {
					t.schedule(TimerA, s.Profile.T1, s.Profile.T1)
				}
				t.schedule(TimerB, 64*s.Profile.T1, 0)
			} else {
				t.state.Phase = Trying
				if !s.Reliable {
					t.schedule(TimerE, s.Profile.T1, s.Profile.T1)
				}
				t.schedule(TimerF, 64*s.Profile.T1, 0)
			}
			t.emit(Send, &s.Origin, false)
		} else {
			if s.Family == InviteServer {
				t.state.Phase = Proceeding
				t.schedule(TimerTrying, s.Profile.Trying, 0)
			} else {
				t.state.Phase = Trying
			}
			t.emit(PresentRequest, &s.Origin, false)
		}
	case ReceiveResponse:
		if !client || s.Phase == Idle || !MatchResponse(s.Origin, in.Message) {
			return s, nil, reject(481, "response origin mismatch")
		}
		t.clientResponse(in.Message)
	case ReceiveRequest:
		if client || s.Phase == Idle || !MatchRequest(s.Origin, in.Message) {
			return s, nil, reject(481, "request origin mismatch")
		}
		t.serverRequest(in.Message)
	case SendResponse:
		if client || s.Phase == Idle || !MatchResponse(s.Origin, in.Message) {
			return s, nil, reject(481, "response origin mismatch")
		}
		t.serverResponse(in.Message)
	case TimerFired:
		t.timer(in.Timer)
	case TransportFailure:
		if s.Phase == Accepted {
			t.effects = append(t.effects, Effect{Kind: Diagnostic, Error: in.Error})
		} else {
			t.finish("transport: " + in.Error)
		}
	case RequestCancel:
		if s.Family != InviteClient {
			return s, nil, errors.New("INVITE client cancellation required")
		}
		t.state.Cancelled = true
		t.cancelIfReady()
	case Terminate:
		t.finish(in.Error)
	default:
		return s, nil, errors.New("unknown transaction input")
	}
	if t.err != nil {
		return s, nil, t.err
	}
	return t.state, t.effects, nil
}

// Validate rejects incompatible or inconsistent restored values before a driver
// can interpret any effect. It never repairs corrupt state by restarting it.
func (s Transaction) Validate() error {
	if s.Version != Version || !s.Profile.valid() {
		return errors.New("unsupported transaction state")
	}
	o, err := request(s.Origin)
	if err != nil {
		return err
	}
	client := s.Family == InviteClient || s.Family == NonInviteClient
	invite := s.Family == InviteClient || s.Family == InviteServer
	if s.Family != InviteClient && s.Family != NonInviteClient && s.Family != InviteServer && s.Family != NonInviteServer || (o.facts.Method == sip.INVITE) != invite || (o.facts.Method == sip.ACK) || s.Reliable != (s.Origin.Transport() != "udp") {
		return errors.New("transaction family/transport mismatch")
	}
	var key string
	if client {
		key, err = s.Origin.ClientTxKey()
	} else {
		key, err = s.Origin.ServerTxKey()
	}
	if err != nil || key != s.Key || !client && !validToken(s.ResponseTag) {
		return errors.New("transaction identity mismatch")
	}
	validPhase := s.Phase == Idle || s.Phase == Proceeding || s.Phase == Completed || s.Phase == Terminated
	if s.Family == InviteClient {
		validPhase = validPhase || s.Phase == Calling || s.Phase == Accepted
	}
	if s.Family == NonInviteClient || s.Family == NonInviteServer {
		validPhase = validPhase || s.Phase == Trying
	}
	if s.Family == InviteServer {
		validPhase = validPhase || s.Phase == Accepted || s.Phase == Confirmed
	}
	if !validPhase {
		return errors.New("unknown transaction phase")
	}
	if s.LastResponse != nil && !MatchResponse(s.Origin, *s.LastResponse) {
		return errors.New("retained response origin mismatch")
	}
	if s.ACK != nil {
		a, e := request(*s.ACK)
		if e != nil || !(a.facts.Method == sip.ACK) || !MatchRequest(s.Origin, *s.ACK) {
			return errors.New("retained ACK origin mismatch")
		}
	}
	for k, v := range s.Timers {
		allowed := false
		switch s.Family {
		case InviteClient:
			allowed = k == TimerA || k == TimerB || k == TimerD || k == TimerM || k == TimerCancel
		case NonInviteClient:
			allowed = k == TimerE || k == TimerF || k == TimerK
		case InviteServer:
			allowed = k == TimerTrying || k == TimerG || k == TimerH || k == TimerI || k == TimerL
		case NonInviteServer:
			allowed = k == TimerJ
		}
		if !allowed || v.At < 0 || v.Interval < 0 || v.Active && v.Generation == 0 {
			return errors.New("invalid transaction timer")
		}
	}
	return nil
}

func (t *transition) clientResponse(m Message) {
	r, _ := response(m)
	s := &t.state
	if s.Family == InviteClient && s.Phase == Accepted {
		if r.facts.Status >= 200 && r.facts.Status < 300 {
			t.emit(ReportResponse, &m, true)
		}
		return
	}
	if s.Phase == Completed {
		if s.Family == InviteClient && r.facts.Status >= 300 {
			t.emit(Send, s.ACK, true)
		}
		return
	}
	if r.facts.Status < 200 {
		s.LastResponse = cloneMessage(&m)
		s.Phase = Proceeding
		t.emit(ReportResponse, &m, false)
		if s.Family == InviteClient {
			t.stop(TimerA)
			t.stop(TimerB)
			t.cancelIfReady()
		} else {
			d := s.Timers[TimerE]
			d.Interval = s.Profile.T2
			s.Timers[TimerE] = d
		}
		return
	}
	s.LastResponse = cloneMessage(&m)
	t.stop(TimerA)
	t.stop(TimerB)
	t.stop(TimerE)
	t.stop(TimerF)
	t.stop(TimerCancel)
	if s.Family == InviteClient && (r.facts.Status >= 200 && r.facts.Status < 300) {
		s.Phase = Accepted
		t.schedule(TimerM, 64*s.Profile.T1, 0)
		t.emit(ReportResponse, &m, false)
		return
	}
	s.Phase = Completed
	if s.Family == InviteClient {
		a, e := BuildNon2xxACK(s.Origin, m)
		if e != nil {
			t.err = e
			return
		}
		s.ACK = &a
		t.emit(Send, &a, false)
	}
	t.emit(ReportResponse, &m, false)
	if s.Reliable {
		t.finish("")
	} else if s.Family == InviteClient {
		t.schedule(TimerD, s.Profile.InviteCompleted, 0)
	} else {
		t.schedule(TimerK, s.Profile.T4, 0)
	}
}

func (t *transition) cancelIfReady() {
	s := &t.state
	if !s.Cancelled || s.CancelSent || s.Phase != Proceeding {
		return
	}
	m, err := BuildCancel(s.Origin)
	if err != nil {
		t.err = err
		return
	}
	s.CancelSent = true
	t.schedule(TimerCancel, 64*s.Profile.T1, 0)
	t.emit(StartTransaction, &m, false)
}

// StepServerCancel serializes CANCEL's own non-INVITE server transaction with
// the INVITE final-selection race. Persist BOTH returned values in one group
// commit. cancel may be nil for a first request; retain its value through J even
// if the related INVITE has retired. PresentRequest is never emitted for CANCEL.
func StepServerCancel(invite Transaction, cancel *Transaction, in Input) (Transaction, Transaction, []Effect, error) {
	if invite.Validate() != nil || invite.Family != InviteServer || in.Kind != ReceiveRequest {
		return invite, Transaction{}, nil, errors.New("INVITE server CANCEL input required")
	}
	r, err := request(in.Message)
	if err != nil || !(r.facts.Method == sip.CANCEL) || !MatchRequest(invite.Origin, in.Message) {
		return invite, Transaction{}, nil, reject(481, "CANCEL original identity mismatch")
	}
	if cancel != nil {
		if cancel.Family != NonInviteServer || !MatchRequest(cancel.Origin, in.Message) {
			return invite, *cancel, nil, reject(481, "CANCEL transaction mismatch")
		}
		next, effects, err := StepTransaction(*cancel, in)
		return invite.Clone(), next, effects, err
	}
	c, err := NewTransaction(NonInviteServer, in.Message, invite.Profile, invite.ResponseTag)
	if err != nil {
		return invite, Transaction{}, nil, err
	}
	c, _, err = StepTransaction(c, Input{Kind: Start, Now: in.Now})
	if err != nil {
		return invite, c, nil, err
	}
	status, reason := 200, "OK"
	if invite.Phase == Idle || invite.Phase == Terminated {
		status, reason = 481, "Call/Transaction Does Not Exist"
	}
	response, err := ResponseFor(in.Message, status, reason, nil, invite.ResponseTag)
	if err != nil {
		return invite, c, nil, err
	}
	c, effects, err := StepTransaction(c, Input{Kind: SendResponse, Now: in.Now, Message: response})
	if err != nil {
		return invite, c, nil, err
	}
	t := transition{state: invite.Clone(), now: in.Now}
	if invite.Phase == Proceeding {
		final, err := ResponseFor(invite.Origin, 487, "Request Terminated", nil, invite.ResponseTag)
		if err != nil {
			return invite, c, nil, err
		}
		t.state.Cancelled = true
		t.emit(Cancelled, &in.Message, false)
		t.serverResponse(final)
	}
	if t.err != nil {
		return invite, c, nil, t.err
	}
	return t.state, c, append(effects, t.effects...), nil
}

func (t *transition) serverRequest(m Message) {
	r, _ := request(m)
	s := &t.state
	if (r.facts.Method == sip.CANCEL) && s.Family == InviteServer {
		c, e := ResponseFor(m, 200, "OK", nil, s.ResponseTag)
		if e != nil {
			t.err = e
			return
		}
		t.emit(Send, &c, false)
		if s.Family == InviteServer && s.Phase == Proceeding {
			f, e := ResponseFor(s.Origin, 487, "Request Terminated", nil, s.ResponseTag)
			if e != nil {
				t.err = e
				return
			}
			s.Cancelled = true
			t.emit(Cancelled, &m, false)
			t.serverResponse(f)
		}
		return
	}
	if r.facts.Method == sip.ACK {
		if s.LastResponse == nil {
			return
		}
		res, _ := response(*s.LastResponse)
		if r.facts.ToTag != res.facts.ToTag {
			t.err = reject(481, "ACK response tag mismatch")
			return
		}
		if s.Phase == Completed {
			s.Phase = Confirmed
			t.stop(TimerG)
			t.stop(TimerH)
			t.emit(Acknowledged, &m, false)
			if s.Reliable {
				t.finish("")
			} else {
				t.schedule(TimerI, s.Profile.T4, 0)
			}
		} else if s.Phase == Accepted {
			t.emit(Acknowledged, &m, true)
		}
		return
	}
	if !m.EqualWire(s.Origin) {
		t.err = reject(481, "transaction replay payload mismatch")
		return
	}
	if s.Phase == Trying || s.Phase == Proceeding || s.Phase == Completed {
		if s.LastResponse != nil {
			t.emit(Send, s.LastResponse, true)
		}
	}
}

func (t *transition) serverResponse(m Message) {
	s := &t.state
	r, _ := response(m)
	if r.facts.Status != 100 && r.facts.ToTag != s.ResponseTag {
		t.err = reject(481, "reserved response tag mismatch")
		return
	}
	if s.Phase == Confirmed || s.Phase == Completed {
		if s.LastResponse != nil && !s.LastResponse.EqualWire(m) {
			t.err = errors.New("conflicting transaction final")
		}
		return
	}
	if s.Phase == Accepted {
		if (r.facts.Status >= 200 && r.facts.Status < 300) && s.LastResponse != nil && s.LastResponse.EqualWire(m) {
			t.emit(Send, s.LastResponse, true)
		} else {
			t.err = errors.New("conflicting transaction final")
		}
		return
	}
	t.stop(TimerTrying)
	s.LastResponse = cloneMessage(&m)
	t.emit(Send, &m, false)
	if r.facts.Status < 200 {
		s.Phase = Proceeding
		return
	}
	if s.Family == InviteServer {
		if r.facts.Status >= 200 && r.facts.Status < 300 {
			s.Phase = Accepted
			t.schedule(TimerL, 64*s.Profile.T1, 0)
		} else {
			s.Phase = Completed
			if !s.Reliable {
				t.schedule(TimerG, s.Profile.T1, s.Profile.T1)
			}
			t.schedule(TimerH, 64*s.Profile.T1, 0)
		}
	} else {
		s.Phase = Completed
		if s.Reliable {
			t.finish("")
		} else {
			t.schedule(TimerJ, 64*s.Profile.T1, 0)
		}
	}
}

func (t *transition) timer(k Timer) {
	s := &t.state
	d := s.Timers[k]
	var terminal Timer
	switch k {
	case TimerA:
		terminal = TimerB
	case TimerE:
		terminal = TimerF
	case TimerG:
		terminal = TimerH
	}
	if deadline := s.Timers[terminal]; terminal != "" && deadline.Active && deadline.At <= t.now {
		t.timer(terminal)
		return
	}
	switch k {
	case TimerA:
		if s.Phase == Calling {
			t.emit(Send, &s.Origin, true)
			t.schedule(k, min(d.Interval, math.MaxInt64/2)*2, min(d.Interval, math.MaxInt64/2)*2)
		}
	case TimerE:
		if s.Phase == Trying || s.Phase == Proceeding {
			t.emit(Send, &s.Origin, true)
			interval := s.Profile.T2
			if s.Phase == Trying {
				interval = min(min(d.Interval, math.MaxInt64/2)*2, s.Profile.T2)
			}
			t.schedule(k, interval, interval)
		}
	case TimerG:
		if s.Phase == Completed {
			t.emit(Send, s.LastResponse, true)
			interval := min(min(d.Interval, math.MaxInt64/2)*2, s.Profile.T2)
			t.schedule(k, interval, interval)
		}
	case TimerB, TimerF:
		t.finish("timeout")
	case TimerCancel:
		if s.Family == InviteClient && s.Cancelled && s.CancelSent && s.Phase == Proceeding {
			t.finish("cancellation timeout")
		}
	case TimerD, TimerI, TimerJ, TimerK, TimerL, TimerM:
		t.finish("")
	case TimerH:
		t.finish("ACK timeout")
	case TimerTrying:
		if s.Family == InviteServer && s.Phase == Proceeding && s.LastResponse == nil {
			m, e := ResponseFor(s.Origin, 100, "Trying", nil, s.ResponseTag)
			if e != nil {
				t.err = e
				return
			}
			t.serverResponse(m)
		}
	}
}
