package engine

import (
	"errors"
	"net"
	"strconv"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

// nativeServerStart computes the initial server transaction and its candidate
// effects. The caller validates authority and publishes the result atomically.
func nativeServerStart(message portable.Message, localTag string, nowMS int64) (portable.Transaction, []portable.Effect, error) {
	if localTag == "" {
		return portable.Transaction{}, nil, errors.New("server transaction identity required")
	}
	facts, err := message.Facts()
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	if !facts.IsRequest || facts.Method == sip.ACK || facts.Method == sip.CANCEL {
		return portable.Transaction{}, nil, errors.New("new server transaction requires a request")
	}
	family := portable.NonInviteServer
	if facts.Method == sip.INVITE {
		family = portable.InviteServer
	}
	initial, err := portable.NewTransaction(family, message, portable.DefaultTimers(), localTag)
	if err != nil {
		return portable.Transaction{}, nil, err
	}
	return portable.StepTransaction(initial, portable.Input{Kind: portable.Start, Now: nowMS})
}

// nativeServerSelectResponse stages the selected response without publishing
// the transaction, scheduling timers or starting a physical write.
func nativeServerSelectResponse(transaction portable.Transaction, response portable.Message, nowMS int64) (portable.Transaction, []portable.Effect, error) {
	return portable.StepTransaction(transaction, portable.Input{Kind: portable.SendResponse, Now: nowMS, Message: response})
}

// The unbound direct INVITE may select 100 at TimerTrying, but its physical
// emission waits for the accepted decision's application binding.
func nativeHoldUnboundTrying(direct, pending, unbound bool, timer portable.Timer) bool {
	return direct && pending && unbound && timer == portable.TimerTrying
}

// nativeServerResponseCandidate contains only an unpublished response and
// transaction transition; its caller retains authority and performs the commit.
type nativeServerResponseCandidate struct {
	Message     portable.Message
	Transaction portable.Transaction
	Effects     []portable.Effect
}

func nativeServerBuildResponse(transaction portable.Transaction, status int, reason string, body []byte, localTag, contact string, headers []portable.Header, nowMS int64) (nativeServerResponseCandidate, error) {
	return nativeServerBuildResponseForFlow(transaction, status, reason, body, localTag, contact, headers, nil, nowMS)
}

// nativeServerBuildObservedResponse selects the actual first response on a
// REGISTER/SUBSCRIBE procedure's fixed admitted flow. Later replacement-flow
// emission remains a separate physical variant of the already selected final.
func nativeServerBuildObservedResponse(transaction portable.Transaction, status int, reason string, body []byte, localTag, contact string, headers []portable.Header, flow *pb.FlowRef, nowMS int64) (nativeServerResponseCandidate, error) {
	if !validPolicyFlow(flow) || transaction.Origin.Transport() != flow.Transport || transaction.Origin.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) {
		return nativeServerResponseCandidate{}, errors.New("response requires its admitted observed flow")
	}
	facts, err := transaction.Origin.Facts()
	if err != nil || facts.Method != sip.REGISTER && facts.Method != sip.SUBSCRIBE {
		return nativeServerResponseCandidate{}, errors.New("fixed-flow REGISTER or SUBSCRIBE response required")
	}
	return nativeServerBuildResponseForFlow(transaction, status, reason, body, localTag, contact, headers, flow, nowMS)
}

func nativeServerBuildResponseForFlow(transaction portable.Transaction, status int, reason string, body []byte, localTag, contact string, headers []portable.Header, flow *pb.FlowRef, nowMS int64) (nativeServerResponseCandidate, error) {
	message, err := portable.BuildResponseWithHeaders(transaction.Origin, status, reason, body, localTag, contact, headers)
	if err != nil {
		return nativeServerResponseCandidate{}, err
	}
	if flow != nil {
		message, err = message.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
		if err != nil {
			return nativeServerResponseCandidate{}, err
		}
		if message.WireLen() > stateWireLimit {
			return nativeServerResponseCandidate{}, errors.New("observed response exceeds packet bound")
		}
	}
	next, effects, err := nativeServerSelectResponse(transaction, message, nowMS)
	if err != nil {
		return nativeServerResponseCandidate{}, err
	}
	return nativeServerResponseCandidate{Message: message, Transaction: next, Effects: effects}, nil
}

// A positive result only permits the transaction's narrow retirement path.
// Independent waiters, outputs, events and dialog duties still fence context removal.
type nativeReliableRegisterFinal struct {
	Method                 string
	Status                 uint32
	Transport              string
	Written                bool
	Settled                bool
	DispatcherAcknowledged bool
	HasSubscription        bool
	HasDialog              bool
}

func nativeReliableRegisterMayRetire(final nativeReliableRegisterFinal) bool {
	return final.Method == "REGISTER" && final.Status >= 200 &&
		(final.Transport == "tcp" || final.Transport == "tls") &&
		final.Written && final.Settled && final.DispatcherAcknowledged &&
		!final.HasSubscription && !final.HasDialog
}
