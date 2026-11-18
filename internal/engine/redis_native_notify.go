package engine

import (
	"context"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"time"
)

// Phone NOTIFY belongs to the resident owner; shared NOTIFY is a call request.
func (e *redisNative) commandNotify(ctx context.Context, peer *control.Peer, command *pb.Command, operation string, deadline time.Time) (*pb.OperationResult, error) {
	return e.commandApplicationBye(ctx, peer, command, operation, deadline)
}
