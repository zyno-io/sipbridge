package engine

import (
	"strings"

	pb "sipbridge/internal/controlpb"
)

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
	methods := []string{"INVITE", "ACK", "CANCEL", "BYE", "OPTIONS", "INFO", "UPDATE", "PRACK", "REFER", "NOTIFY"}
	if !p.DisableRegister {
		methods = append(methods, "REGISTER")
	}
	if !p.DisableSubscribe {
		methods = append(methods, "SUBSCRIBE")
	}
	return strings.Join(methods, ", ")
}
