package main

import (
	"context"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/engine"
)

// nativeRuntime is the common direct transport/control boundary. Each selected
// runtime owns its own storage and scheduling implementation.
type nativeRuntime interface {
	engine.StateWriter
	engine.StateInspector
	Close()
	Ready() error
	Handle(*control.Peer, *pb.Envelope) error
	ApplicationLease(context.Context, *pb.ProcessRef, string) (*pb.ApplicationLease, error)
	ControllerSession(context.Context, *control.Peer, *pb.SessionHello) (*pb.ApplicationLease, error)
	ControllerRenew(context.Context, *control.Peer) (*pb.ApplicationLease, error)
	ControllerReady(context.Context, *control.Peer) error
	LocateRoute(context.Context, *pb.SipMessage) (*pb.ContextSnapshot, error)
	Reject(context.Context, *pb.FlowRef, *pb.SipMessage, uint32) error
	TryNativeCompletion(context.Context, *pb.FlowRef, *pb.SipMessage) (bool, error)
	RejectParsed(context.Context, *pb.FlowRef, *pb.SipMessage, uint32, sip.Message, portable.Message) error
	TryNativeCompletionParsed(context.Context, *pb.FlowRef, *pb.SipMessage, sip.Message, ...portable.Message) (bool, error)
	ReceiveRequestParsed(context.Context, *control.Peer, *pb.FlowRef, *pb.SipMessage, sip.Message, ...portable.Message) error
	DisconnectApplication(context.Context, *pb.ProcessRef, string) error
	FlowClosed(*pb.FlowRef)
}

var _ nativeRuntime = (*engine.RedisNative)(nil)

var _ nativeRuntime = (*engine.ResidentMemory)(nil)
