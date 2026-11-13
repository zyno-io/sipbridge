package engine

import (
	"errors"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type residentOutputState uint8

const (
	residentOutputAuthorized residentOutputState = iota
	residentOutputStarted
	residentOutputUnknown
	residentOutputWritten
	residentOutputNotWritten
)

// residentOutput is guarded by its owning context lock. Its wire and proof are
// independently owned, and the exact physical attempt remains valid even when an
// unrelated transition changes the context revision.
type residentOutput struct {
	settled          bool
	wireBytes        int64
	budget           *residentBudget
	borrowed         int64
	expectedDelivery *pb.ContextDelivery
	command          *residentCommand
	transaction      string
	dialog           string
	cleanupAfterACK  bool
	replay           bool
	ackSequence      uint32
	operation        string
	method           string
	status           uint32
	drain            state.LocalDrainObservation
	bytes            int64
	write            *pb.FlowWrite
	wire             []byte
	deadline         time.Time
	state            residentOutputState
}

func newResidentOutput(write *pb.FlowWrite, wire []byte, deadline time.Time) (*residentOutput, error) {
	if write == nil || write.Effect == nil || write.Effect.Context == nil || write.Effect.Id == "" || write.Effect.AttemptId == "" || write.Permit == nil || write.Permit.Authorization == nil || write.Flow == nil || len(wire) == 0 || deadline.IsZero() ||
		!proto.Equal(write.Effect, write.Permit.Authorization.Effect) || !proto.Equal(write.Flow, write.Permit.Flow) {
		return nil, errors.New("invalid physical output authority")
	}
	permit := proto.Clone(write.Permit).(*pb.SendPermit)
	owned := &pb.FlowWrite{Permit: permit, Flow: permit.Flow, Effect: permit.Authorization.Effect}
	if unknown := write.ProtoReflect().GetUnknown(); len(unknown) > 0 {
		owned.ProtoReflect().SetUnknown(append([]byte(nil), unknown...))
	}
	return &residentOutput{wireBytes: int64(len(wire)), write: owned, wire: append([]byte(nil), wire...), deadline: deadline}, nil
}

func (o *residentOutput) matches(write *pb.FlowWrite) bool {
	return write != nil && proto.Equal(o.write, write)
}

func (o *residentOutput) reserve(write *pb.FlowWrite, now time.Time) (*CommittedWrite, error) {
	if !o.matches(write) {
		return nil, errors.New("effect authority mismatch")
	}
	switch o.state {
	case residentOutputWritten:
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_WRITTEN}, nil
	case residentOutputNotWritten:
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_NOT_WRITTEN}, nil
	case residentOutputStarted, residentOutputUnknown:
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
	}
	if !now.Before(o.deadline) || o.write.Permit.Authorization.DeadlineMs == 0 || uint64(now.UnixMilli()) >= o.write.Permit.Authorization.DeadlineMs {
		return nil, errors.New("physical output deadline elapsed")
	}
	o.state = residentOutputStarted
	wire := o.wire
	o.wire = nil
	return &CommittedWrite{Wire: wire}, nil
}

func (o *residentOutput) complete(write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if !o.matches(write) || result == nil || !proto.Equal(result.Effect, write.Effect) {
		return errors.New("delivery proof mismatch")
	}
	if o.state == residentOutputAuthorized {
		if result.Outcome != pb.DeliveryResult_NOT_WRITTEN || !proto.Equal(result.Effect, write.Effect) {
			return errors.New("unreserved delivery absence proof mismatch")
		}
	}
	next := residentOutputUnknown
	switch result.Outcome {
	case pb.DeliveryResult_WRITTEN:
		next = residentOutputWritten
	case pb.DeliveryResult_NOT_WRITTEN:
		next = residentOutputNotWritten
	}
	if o.state == residentOutputWritten || o.state == residentOutputNotWritten {
		if o.state != next {
			return errors.New("conflicting delivery outcome")
		}
		return nil
	}
	o.state = next
	o.wire = nil
	return nil
}
