package engine

import pb "sipbridge/internal/controlpb"

// MethodPolicy is immutable for the ingress process lifetime. Zero enables both
// registration methods, preserving existing consumers and retained transactions.
type MethodPolicy struct {
	DisableRegister  bool
	DisableSubscribe bool
}

func (p MethodPolicy) disabled(message *pb.SipMessage) bool {
	return message != nil && message.Status == 0 &&
		(message.Method == "REGISTER" && p.DisableRegister || message.Method == "SUBSCRIBE" && p.DisableSubscribe)
}

func (p MethodPolicy) allow() string {
	const base = "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, UPDATE, PRACK, REFER, NOTIFY"
	switch {
	case p.DisableRegister && p.DisableSubscribe:
		return base
	case p.DisableRegister:
		return base + ", SUBSCRIBE"
	case p.DisableSubscribe:
		return base + ", REGISTER"
	default:
		return base + ", REGISTER, SUBSCRIBE"
	}
}
