package main

import (
	"context"

	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/engine"
	"sipbridge/internal/execution"
)

// nativeRuntime is the existing transport/control boundary. Storage is not part
// of this interface: the resident Memory runtime owns its typed records directly.
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
	SchedulerStatistics() engine.SchedulerStatistics
	ExecutionStatistics() []execution.Statistic
	OperationObservationStatistics() (uint64, uint64)
	CallbackStatistics() ([]engine.CallbackStatistic, []engine.PhysicalLocalityStatistic)
}

var _ nativeRuntime = (*engine.StateEngine)(nil)

var _ nativeRuntime = (*engine.ResidentMemory)(nil)
