package sipgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/emiago/sipgo/sip"
)

var (
	ErrDialogOutsideDialog   = errors.New("Call/Transaction Outside Dialog")
	ErrDialogDoesNotExists   = errors.New("Call/Transaction Does Not Exist")
	ErrDialogInviteNoContact = errors.New("no Contact header")
	ErrDialogInvalidCseq     = errors.New("invalid CSEQ number")
)

type ErrDialogResponse struct {
	Res *sip.Response
}

func (e ErrDialogResponse) Error() string {
	return fmt.Sprintf("Invite failed with response: %s", e.Res.StartLine())
}

type DialogStateFn func(s sip.DialogState)
type Dialog struct {
	ID string

	// InviteRequest is set when dialog is created. It is not thread safe!
	// Use it only as read only and use methods to change headers
	InviteRequest *sip.Request

	// Local and remote request sequences are independent. ACK and CANCEL
	// reuse their INVITE sequence and never advance either sequence space.
	lastCSeqNo        atomic.Uint32
	localInviteCSeqNo atomic.Uint32
	remoteCSeqMu      sync.Mutex
	remoteCSeqNo      uint32
	remoteCSeqSet     bool
	remoteInviteCSeq  uint32
	remoteInviteSet   bool

	// InviteResponse is last response received or sent. It is not thread safe!
	// Use it only as read only and do not change values
	InviteResponse *sip.Response

	state atomic.Int32

	ctx    context.Context
	cancel context.CancelCauseFunc

	onStatePointer atomic.Pointer[DialogStateFn]
}

// Init sets up a dialog created by a received request (UAS role).
func (d *Dialog) Init() {
	d.ctx, d.cancel = context.WithCancelCause(context.Background())
	d.state = atomic.Int32{}
	d.lastCSeqNo = atomic.Uint32{}
	d.localInviteCSeqNo = atomic.Uint32{}
	d.remoteCSeqNo, d.remoteInviteCSeq = 0, 0
	d.remoteCSeqSet, d.remoteInviteSet = false, false

	// We may have sequence number initialized
	if cseq := d.InviteRequest.CSeq(); cseq != nil {
		d.remoteCSeqNo, d.remoteCSeqSet = cseq.SeqNo, true
		if d.InviteRequest.IsInvite() {
			d.remoteInviteCSeq, d.remoteInviteSet = cseq.SeqNo, true
		}
	}
	d.onStatePointer = atomic.Pointer[DialogStateFn]{}
}

func (d *Dialog) OnState(f DialogStateFn) {
	for {
		current := d.onStatePointer.Load()
		newCB := f
		if current != nil {
			cb := *current
			newCB = func(s sip.DialogState) {
				f(s)
				cb(s)
			}
		}
		if d.onStatePointer.CompareAndSwap(current, &newCB) {
			return
		}
	}

}

func (d *Dialog) OnStateReplay(f DialogStateFn) sip.DialogState {
	d.OnState(f)
	state := d.LoadState()
	f(state)
	return state
}

func (d *Dialog) InitWithState(s sip.DialogState) {
	d.Init()
	d.state.Store(int32(s))
}

func (d *Dialog) setState(s sip.DialogState) {
	old := d.state.Swap(int32(s))
	if old == int32(s) {
		// Safety
		return
	}

	if s == sip.DialogStateEnded {
		d.cancel(nil)
	}

	if f := d.onStatePointer.Load(); f != nil {
		cb := *f
		cb(s)
	}
}

// endWithCause sets dialog state ended and place context cause error
// Experimental
func (d *Dialog) endWithCause(err error) {
	s := sip.DialogStateEnded
	old := d.state.Swap(int32(s))
	if old == int32(s) {
		// Safety
		return
	}
	d.cancel(err)

	if f := d.onStatePointer.Load(); f != nil {
		cb := *f
		cb(s)
	}
}

// Err returns error that caused dialog termination
func (d *Dialog) err() error {
	return context.Cause(d.Context())
}

func (d *Dialog) LoadState() sip.DialogState {
	return sip.DialogState(d.state.Load())
}

func (d *Dialog) StateRead() <-chan sip.DialogState {
	ch := make(chan sip.DialogState, 5)
	d.OnState(func(s sip.DialogState) {
		select {
		case ch <- s:
		default:
		}
	})

	return ch
}

func (d *Dialog) CSEQ() uint32 {
	return d.lastCSeqNo.Load()
}

func (d *Dialog) Context() context.Context {
	return d.ctx
}

// validateRequestIdentity checks all three directional dialog identifiers.
// Transaction lookup alone is insufficient to establish dialog membership.
func (d *Dialog) validateRequestIdentity(req *sip.Request, localTag, remoteTag string) error {
	if req.CallID() == nil || d.InviteRequest.CallID() == nil ||
		*req.CallID() != *d.InviteRequest.CallID() || req.To() == nil || req.From() == nil ||
		localTag == "" || remoteTag == "" {
		return ErrDialogOutsideDialog
	}
	toTag, _ := req.To().Params.Get("tag")
	fromTag, _ := req.From().Params.Get("tag")
	if toTag != localTag || fromTag != remoteTag {
		return ErrDialogOutsideDialog
	}
	return nil
}

func (d *Dialog) ReadRequest(req *sip.Request, tx sip.ServerTransaction) error {
	cseq := req.CSeq()
	if cseq == nil || cseq.MethodName != req.Method {
		return ErrDialogInvalidCseq
	}
	d.remoteCSeqMu.Lock()
	defer d.remoteCSeqMu.Unlock()
	if req.IsAck() || req.IsCancel() {
		if !d.remoteInviteSet || cseq.SeqNo != d.remoteInviteCSeq {
			return ErrDialogInvalidCseq
		}
		return nil
	}
	// A retransmission is handled by its native server transaction before
	// reaching the dialog. A new transaction must advance remote CSeq.
	if d.remoteCSeqSet && cseq.SeqNo <= d.remoteCSeqNo {
		return ErrDialogInvalidCseq
	}
	d.remoteCSeqNo, d.remoteCSeqSet = cseq.SeqNo, true
	if req.IsInvite() {
		d.remoteInviteCSeq, d.remoteInviteSet = cseq.SeqNo, true
	}
	return nil
}

func (d *Dialog) buildLocalCSeq(req *sip.Request, cseq *sip.CSeqHeader, provided bool) {
	cseq.MethodName = req.Method
	if req.IsAck() || req.IsCancel() {
		if !provided {
			cseq.SeqNo = d.localInviteCSeqNo.Load()
		}
		return
	}
	cseq.SeqNo = d.lastCSeqNo.Add(1)
	if req.IsInvite() {
		d.localInviteCSeqNo.Store(cseq.SeqNo)
	}
}
