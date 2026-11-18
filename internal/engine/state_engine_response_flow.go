package engine

import (
	"errors"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func (r *stateReduction) recordResponseFlow(c *stateContext, tx *stateTransaction, message portable.Message, in *pb.OwnerIngest) error {
	if in == nil || in.Flow == nil || !portable.MatchResponse(tx.Native.Origin, message) || in.Flow.Transport != c.Snapshot.Transport {
		return errors.New("response completion flow identity mismatch")
	}
	if c.Snapshot.PrincipalId != "" && in.PrincipalId != "" && c.Snapshot.PrincipalId != in.PrincipalId ||
		c.Snapshot.BindingId != "" && in.BindingId != "" && c.Snapshot.BindingId != in.BindingId {
		return &portable.ProtocolError{Status: 481, Reason: "response completion binding mismatch"}
	}
	known := proto.Equal(in.Flow, tx.Flow) || proto.Equal(in.Flow, tx.ResponseFlow) || proto.Equal(in.Flow, c.Flow)
	if !known {
		binding := in.Admission == pb.OwnerIngest_AUTHENTICATED_BINDING && c.Snapshot.PrincipalId != "" && c.Snapshot.BindingId != "" &&
			in.PrincipalId == c.Snapshot.PrincipalId && in.BindingId == c.Snapshot.BindingId
		tls := in.Flow.Transport == "tls" && in.Admission == pb.OwnerIngest_TLS_TRANSACTION_CONTINUITY
		if !binding && !tls {
			return &portable.ProtocolError{Status: 481, Reason: "replacement response flow admission required"}
		}
	}
	tx.ResponseFlow = proto.Clone(in.Flow).(*pb.FlowRef)
	if tx.Native.Family != portable.InviteClient {
		return nil
	}
	facts, err := message.Facts()
	if err != nil {
		return err
	}
	if facts.IsRequest || facts.Status <= 100 || facts.Status >= 300 || facts.ToTag == "" {
		return nil
	}
	key := facts.FromTag + "/" + facts.ToTag
	if c.DialogCompletionFlows == nil {
		c.DialogCompletionFlows = make(map[string]*pb.FlowRef)
	}
	if _, exists := c.DialogCompletionFlows[key]; !exists && len(c.DialogCompletionFlows) >= 16 {
		return &portable.ProtocolError{Status: 503, Reason: "native branch completion flow capacity"}
	}
	c.DialogCompletionFlows[key] = proto.Clone(in.Flow).(*pb.FlowRef)
	return nil
}

func stateCompletionFlow(c *stateContext, tx *stateTransaction, message portable.Message) *pb.FlowRef {
	facts, err := message.Facts()
	if err != nil {
		return nil
	}
	if !facts.IsRequest {
		return nil
	}
	if facts.Method == sip.ACK && tx.Native.ACK != nil && tx.Native.ACK.EqualWire(message) {
		// A non-2xx ACK completes this exact transaction, rather than the
		// earlier provisional dialog which can still name an older socket.
		if tx.ResponseFlow != nil {
			return tx.ResponseFlow
		}
		return tx.Flow
	}
	if facts.Method == sip.ACK || tx.Cleanup {
		key := facts.FromTag + "/" + facts.ToTag
		if flow := c.DialogCompletionFlows[key]; flow != nil {
			return flow
		}
	}
	if tx.ResponseFlow != nil {
		return tx.ResponseFlow
	}
	if facts.Method == sip.CANCEL {
		for _, parent := range c.Transactions {
			if parent.Native.Family == portable.InviteClient && parent.ResponseFlow != nil && portable.MatchRequest(parent.Native.Origin, message) {
				return parent.ResponseFlow
			}
		}
	}
	return tx.Flow
}
