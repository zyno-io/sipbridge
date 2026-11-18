package engine

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

type nativeSIPAdvertisement struct {
	contactURI   string
	address      string
	plainAddress string
}

func (a nativeSIPAdvertisement) resolve(transport string) (string, string, error) {
	return advertisement(Config{ContactURI: a.contactURI, AdvertiseAddress: a.address, PlainAdvertiseAddress: a.plainAddress}, transport)
}

// These candidate procedures do not publish state or perform I/O. The caller
// must commit the response, subscription and required output atomically.
func nativePrepareSubscription(current *stateSubscription, snapshotBefore *pb.ContextSnapshot, transaction portable.Transaction, response *pb.Respond, advertise nativeSIPAdvertisement, now time.Time) (*stateSubscription, *pb.ContextSnapshot, portable.Message, error) {
	request, err := transaction.Origin.Facts()
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	if !request.IsRequest {
		return nil, nil, portable.Message{}, errors.New("SUBSCRIBE request required")
	}
	if request.Method != sip.SUBSCRIBE || !request.HasContact || response == nil || response.Status < 200 || response.Status >= 300 {
		return nil, nil, portable.Message{}, errors.New("successful SUBSCRIBE with Contact required")
	}
	events, err := transaction.Origin.HeaderValues("Event")
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	rawEvent := ""
	if len(events) > 0 {
		rawEvent = events[0]
	}
	event, err := eventKeyValue(rawEvent)
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	expires := 3600
	for _, h := range response.Headers {
		if strings.EqualFold(h.Name, "Expires") {
			expires, err = strconv.Atoi(h.Value)
			if err != nil || expires < 0 || expires > 10800 {
				return nil, nil, portable.Message{}, errors.New("invalid granted subscription expiry")
			}
		}
	}
	sub := &stateSubscription{Event: event}
	if current != nil {
		if current.Event != event {
			return nil, nil, portable.Message{}, errors.New("subscription event mismatch")
		}
		*sub = *current
		sub.Routes = append([]string(nil), current.Routes...)
	} else {
		routes, routeErr := transaction.Origin.HeaderValues("Record-Route")
		if routeErr != nil {
			return nil, nil, portable.Message{}, routeErr
		}
		sub.Routes = append(sub.Routes, routes...)
	}
	sub.Target = request.ContactURI
	sub.RemoteSequence = request.CSeq
	sub.Pending = ""
	sub.ExpiresMS = now.Add(time.Duration(expires) * time.Second).UnixMilli()
	sub.Terminating = expires == 0
	sub.TerminalMS = 0
	if sub.Terminating {
		sub.TerminalMS = now.Add(5 * time.Second).UnixMilli()
	}
	snapshot := proto.Clone(snapshotBefore).(*pb.ContextSnapshot)
	snapshot.Kind = pb.ContextSnapshot_SUBSCRIPTION
	snapshot.RemoteTag = request.FromTag
	snapshot.DialogId = sip.DialogIDMake(snapshot.CallId, snapshot.LocalTag, snapshot.RemoteTag)
	snapshot.RemoteTarget = sub.Target
	contact, _, err := advertise.resolve(transaction.Origin.Transport())
	if err != nil {
		return nil, nil, portable.Message{}, err
	}
	message, err := portable.BuildResponseWithHeaders(transaction.Origin, int(response.Status), reason(response.Status), response.Body, snapshot.LocalTag, contact, stateHeaders(response.Headers))
	return sub, snapshot, message, err
}

func nativePrepareNotify(current *stateSubscription, snapshot *pb.ContextSnapshot, command *pb.DialogRequest, advertise nativeSIPAdvertisement, branch string, now time.Time) (*stateSubscription, portable.Transaction, []portable.Effect, error) {
	if current == nil || command == nil || command.Method != "NOTIFY" {
		return nil, portable.Transaction{}, nil, errors.New("subscription only supports NOTIFY")
	}
	sub := *current
	stateValue := "active"
	for _, h := range command.Headers {
		if strings.EqualFold(h.Name, "Subscription-State") {
			stateValue = h.Value
		}
	}
	final := strings.HasPrefix(stateValue, "terminated")
	if snapshot.Ended || sub.Final || sub.Terminating && !final {
		return nil, portable.Transaction{}, nil, errSubscriptionTerminating
	}
	if !final {
		remaining := (sub.ExpiresMS - now.UnixMilli() + 999) / 1000
		if remaining < 1 {
			return nil, portable.Transaction{}, nil, errors.New("subscription expired")
		}
		stateValue = fmt.Sprintf("active;expires=%d", remaining)
	}
	if sub.LocalSequence >= math.MaxInt32 {
		return nil, portable.Transaction{}, nil, errors.New("subscription CSeq exhausted")
	}
	sub.LocalSequence++
	headers := []portable.Header{{Name: "Event", Value: sub.Event}, {Name: "Subscription-State", Value: stateValue}}
	for _, h := range command.Headers {
		if strings.EqualFold(h.Name, "Content-Type") {
			headers = append(headers, portable.Header{Name: h.Name, Value: h.Value})
		}
	}
	contact, sentBy, err := advertise.resolve(snapshot.Transport)
	if err != nil {
		return nil, portable.Transaction{}, nil, err
	}
	dialog := portable.Dialog{Version: portable.Version, Role: portable.UAS, Phase: portable.DialogConfirmed, CallID: snapshot.CallId,
		LocalTag: snapshot.LocalTag, RemoteTag: snapshot.RemoteTag, LocalURI: snapshot.LocalUri, RemoteURI: snapshot.RemoteUri,
		Target: sub.Target, Routes: sub.Routes, Contact: contact, SentBy: sentBy, Transport: snapshot.Transport}
	message, err := portable.BuildDialogRequestWithHeaders(dialog, sip.NOTIFY, sub.LocalSequence, command.Body,
		branch, headers)
	if err != nil {
		return nil, portable.Transaction{}, nil, err
	}
	native, err := portable.NewTransaction(portable.NonInviteClient, message, portable.DefaultTimers(), "")
	if err != nil {
		return nil, portable.Transaction{}, nil, err
	}
	next, effects, err := portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: now.UnixMilli()})
	if err != nil {
		return nil, portable.Transaction{}, nil, err
	}
	if final {
		sub.Terminating, sub.Final, sub.TerminalMS = true, true, now.Add(5*time.Second).UnixMilli()
	}
	return &sub, next, effects, nil
}
