package engine

import (
	"context"
	"slices"

	"github.com/google/uuid"
	"sipbridge/internal/capture"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
)

func validCaptureUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func notifyCapture(command *pb.DialogRequest) (capture.Attribution, bool) {
	if command == nil {
		return capture.Attribution{}, false
	}
	if command.BlfCapture == nil {
		return capture.Attribution{}, true
	}
	id, calls := command.BlfCapture.DeliveryId, command.BlfCapture.NativeCallIds
	if command.Method != "NOTIFY" || !validCaptureUUID(id) || len(calls) > 20 {
		return capture.Attribution{}, false
	}
	for i, call := range calls {
		if !validCaptureUUID(call) || slices.Contains(calls[:i], call) {
			return capture.Attribution{}, false
		}
	}
	return capture.Attribution{BLFDeliveryID: id, BLFNativeCallIDs: slices.Clone(calls)}, true
}

func captureContextKey(ref *pb.ContextRef) string {
	return ref.Id + ":" + ref.State.Incarnation
}

func attributeNativeCall(ctx context.Context, current *nativepb.Context) {
	capture.Attribute(ctx, capture.Attribution{NativeCallID: current.NativeCallId})
}
