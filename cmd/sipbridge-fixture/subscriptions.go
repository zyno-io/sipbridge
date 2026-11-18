package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"mime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
)

type subscription struct {
	mu                                           sync.Mutex
	notifyMu                                     sync.Mutex
	native                                       sipgo.Dialog
	key, initialKey, callID, eventKey, eventWire string
	target                                       sip.Uri
	source                                       string
	routes                                       []*sip.RouteHeader
	expires                                      time.Time
	localSeq, remoteSeq                          uint32
	lastRequestKey                               string
	lastStatus, lastExpires                      int
	terminating, closed                          bool
}

func subscriptionEvent(req *sip.Request) (string, string, error) {
	wire := header(req, "Event")
	event, params, err := mime.ParseMediaType("application/" + wire)
	if err != nil {
		return "", "", err
	}
	event = strings.TrimPrefix(event, "application/")
	if event != "message-summary" && event != "dialog" {
		return "", "", errors.New("unsupported event package")
	}
	for key := range params {
		if key != "id" {
			return "", "", errors.New("unsupported fixture Event parameter")
		}
	}
	return event + ";id=" + params["id"], wire, nil
}

func subscriptionExpires(req *sip.Request) (int, error) {
	raw := header(req, "Expires")
	if raw == "" {
		return 60, nil
	}
	expires, err := strconv.Atoi(raw)
	if err != nil || expires < 0 {
		return 0, errors.New("invalid Expires")
	}
	return min(expires, 300), nil
}

func (r *runtime) subscriptionResponse(req *sip.Request, status int, tag string, expires int) *sip.Response {
	response := sip.NewResponseFromRequest(req, status, "Subscription", nil)
	if tag != "" {
		response.To().Params.Add("tag", tag)
	}
	if status == 200 {
		response.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expires)))
		response.AppendHeader(sip.HeaderClone(&r.dialogUA.ContactHDR))
	}
	return response
}

func (r *runtime) removeSubscription(sub *subscription) {
	sub.mu.Lock()
	sub.closed = true
	sub.mu.Unlock()
	r.mu.Lock()
	if r.subscriptions[sub.key] == sub {
		delete(r.subscriptions, sub.key)
	}
	if r.subscriptionInitial[sub.initialKey] == sub {
		delete(r.subscriptionInitial, sub.initialKey)
	}
	r.mu.Unlock()
}

func (r *runtime) handleSubscribe(req *sip.Request, tx *sip.ServerTx) error {
	event, eventWire, err := subscriptionEvent(req)
	if err != nil {
		return tx.Respond(r.subscriptionResponse(req, 489, "", 0))
	}
	expires, err := subscriptionExpires(req)
	if err != nil || req.Contact() == nil {
		return tx.Respond(r.subscriptionResponse(req, 400, "", 0))
	}
	requestKey, err := sip.ServerTxKeyMake(req)
	if err != nil {
		return err
	}
	remoteTag, _ := req.From().Params.Get("tag")
	if remoteTag == "" {
		return tx.Respond(r.subscriptionResponse(req, 400, "", 0))
	}
	localTag, _ := req.To().Params.Get("tag")
	initialKey := fmt.Sprintf("%s:%s:%s", req.CallID().Value(), remoteTag, event)
	r.mu.Lock()
	sub := r.subscriptionInitial[initialKey]
	if localTag != "" {
		id, idErr := sip.DialogIDFromRequestUAS(req)
		if idErr != nil {
			r.mu.Unlock()
			return idErr
		}
		sub = r.subscriptions[id+":"+event]
		if sub == nil {
			r.mu.Unlock()
			return tx.Respond(r.subscriptionResponse(req, 481, localTag, 0))
		}
	}
	fresh := sub == nil
	if fresh {
		if len(r.subscriptions) >= 128 {
			r.mu.Unlock()
			return tx.Respond(r.subscriptionResponse(req, 503, "", 0))
		}
		if header(req, "X-Proof-Reject") == "yes" {
			r.mu.Unlock()
			return tx.Respond(r.subscriptionResponse(req, 403, "", 0))
		}
		response := r.subscriptionResponse(req, 200, "", expires)
		taggedRequest := req.Clone()
		localTag, _ = response.To().Params.Get("tag")
		taggedRequest.To().Params.Add("tag", localTag)
		id, idErr := sip.DialogIDFromRequestUAS(taggedRequest)
		if idErr != nil {
			r.mu.Unlock()
			return idErr
		}
		sub = &subscription{key: id + ":" + event, initialKey: initialKey, callID: req.CallID().Value(), eventKey: event, eventWire: eventWire,
			native: sipgo.Dialog{ID: id, InviteRequest: taggedRequest, InviteResponse: response}}
		sub.native.Init()
		for _, record := range req.GetHeaders("Record-Route") {
			route, ok := record.(*sip.RecordRouteHeader)
			if !ok || !route.Address.UriParams.Has("lr") {
				r.mu.Unlock()
				return tx.Respond(r.subscriptionResponse(req, 400, "", 0))
			}
			sub.routes = append(sub.routes, &sip.RouteHeader{Address: *route.Address.Clone()})
		}
		// Publish while locked, so another refresh cannot pass initial commit.
		sub.mu.Lock()
		r.subscriptions[sub.key], r.subscriptionInitial[initialKey] = sub, sub
		r.mu.Unlock()
	} else {
		r.mu.Unlock()
		sub.mu.Lock()
		localTag, _ = sub.native.InviteResponse.To().Params.Get("tag")
	}
	if sub.closed || sub.terminating || (!fresh && !sub.expires.After(time.Now())) {
		sub.mu.Unlock()
		return tx.Respond(r.subscriptionResponse(req, 481, localTag, 0))
	}
	seq := req.CSeq().SeqNo
	if !fresh && seq == sub.remoteSeq && requestKey == sub.lastRequestKey {
		response := r.subscriptionResponse(req, sub.lastStatus, localTag, sub.lastExpires)
		sub.mu.Unlock()
		return tx.Respond(response)
	}
	if !fresh && (seq <= sub.remoteSeq || req.To().Params.GetOr("tag", "") == "") {
		sub.mu.Unlock()
		return tx.Respond(r.subscriptionResponse(req, 500, localTag, 0))
	}
	if !fresh {
		if err := sub.native.ReadRequest(req, tx); err != nil {
			sub.mu.Unlock()
			return tx.Respond(r.subscriptionResponse(req, 500, localTag, 0))
		}
	}
	status := 200
	if header(req, "X-Proof-Reject") == "yes" {
		status = 403
	}
	sub.remoteSeq, sub.lastRequestKey, sub.lastStatus, sub.lastExpires = seq, requestKey, status, expires
	if status == 200 {
		sub.target = *req.Contact().Address.Clone()
		sub.source = req.Source()
		sub.expires = time.Now().Add(time.Duration(expires) * time.Second)
		sub.terminating = expires == 0
	}
	response := r.subscriptionResponse(req, status, localTag, expires)
	// Policy and accepted metadata commit once even if wire delivery is uncertain.
	sendErr := tx.Respond(response)
	sub.mu.Unlock()
	if status == 200 {
		reason := ""
		if expires == 0 {
			reason = "deactivated"
		}
		go r.backgroundNotify(sub, reason)
	}
	return sendErr
}

func (r *runtime) sendNotify(ctx context.Context, sub *subscription, reason string) (*sip.Response, error) {
	sub.notifyMu.Lock()
	defer sub.notifyMu.Unlock()
	sub.mu.Lock()
	if sub.closed || (sub.terminating && reason == "") {
		sub.mu.Unlock()
		return nil, errors.New("subscription is terminating")
	}
	if reason == "" && !sub.expires.After(time.Now()) {
		sub.mu.Unlock()
		return nil, errors.New("subscription has expired")
	}
	if sub.localSeq >= 2147483647 {
		sub.mu.Unlock()
		return nil, errors.New("NOTIFY CSeq exhausted")
	}
	sub.localSeq++
	request := sip.NewRequest(sip.NOTIFY, *sub.target.Clone())
	from := sub.native.InviteResponse.To().AsFrom()
	to := sub.native.InviteRequest.From().AsTo()
	request.AppendHeader(&from)
	request.AppendHeader(&to)
	request.AppendHeader(sip.HeaderClone(sub.native.InviteRequest.CallID()))
	request.AppendHeader(&sip.CSeqHeader{SeqNo: sub.localSeq, MethodName: sip.NOTIFY})
	request.AppendHeader(sip.HeaderClone(&r.dialogUA.ContactHDR))
	request.AppendHeader(sip.NewHeader("Event", sub.eventWire))
	state := fmt.Sprintf("active;expires=%d", max(0, int(math.Ceil(time.Until(sub.expires).Seconds()))))
	if reason != "" {
		state = "terminated;reason=" + reason
	}
	request.AppendHeader(sip.NewHeader("Subscription-State", state))
	for _, route := range sub.routes {
		request.AppendHeader(sip.HeaderClone(route))
	}
	request.SetTransport("TLS")
	request.SetDestination(sub.source)
	if strings.HasPrefix(sub.eventKey, "dialog;") {
		request.AppendHeader(sip.NewHeader("Content-Type", "application/dialog-info+xml"))
		request.SetBody([]byte("<dialog-info xmlns=\"urn:ietf:params:xml:ns:dialog-info\" version=\"1\" state=\"full\" entity=\"sip:fixture@example.test\"/>"))
	} else {
		request.AppendHeader(sip.NewHeader("Content-Type", "application/simple-message-summary"))
		request.SetBody([]byte("Messages-Waiting: no\r\n"))
	}
	sub.mu.Unlock()
	response, err := r.client.Do(ctx, request)
	if reason != "" || (response != nil && response.StatusCode == 481) {
		r.removeSubscription(sub)
	}
	if err != nil {
		return nil, err
	}
	if !response.IsSuccess() {
		return response, fmt.Errorf("NOTIFY rejected with %d", response.StatusCode)
	}
	return response, nil
}

func (r *runtime) backgroundNotify(sub *subscription, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	_, err := r.sendNotify(ctx, sub, reason)
	if err != nil {
		r.emit(Record{Event: "notify_failure", ID: sub.key, Error: err.Error()})
	}
}

func (r *runtime) subscriptionCommand(input Input) {
	r.mu.Lock()
	candidates := make([]*subscription, 0, 1)
	for _, sub := range r.subscriptions {
		if sub.callID == input.CallID && (input.SubscriptionEvent == "" || sub.eventKey == input.SubscriptionEvent) {
			candidates = append(candidates, sub)
		}
	}
	r.mu.Unlock()
	if len(candidates) != 1 {
		r.emit(Record{Event: "command_error", ID: input.ID, Error: "subscription absent or ambiguous"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	response, err := r.sendNotify(ctx, candidates[0], "")
	if err != nil {
		r.emit(Record{Event: "command_error", ID: input.ID, Error: err.Error(), TransportError: errors.Is(err, sip.ErrTransactionTransport)})
		return
	}
	r.emit(Record{Event: "command_done", ID: input.ID, Result: fmt.Sprintf("Ok(Some(%d))", response.StatusCode)})
}

func (r *runtime) expireSubscriptions() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for now := range ticker.C {
		r.mu.Lock()
		candidates := make([]*subscription, 0, len(r.subscriptions))
		for _, sub := range r.subscriptions {
			candidates = append(candidates, sub)
		}
		r.mu.Unlock()
		for _, sub := range candidates {
			sub.mu.Lock()
			expired := !sub.closed && !sub.terminating && !sub.expires.After(now)
			if expired {
				sub.terminating = true
			}
			sub.mu.Unlock()
			if expired {
				go r.backgroundNotify(sub, "timeout")
			}
		}
	}
}
