package engine

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func assertRedisNativeJSONInspection(t *testing.T, ctx context.Context, ws *websocket.Conn, session string, expected *pb.ContextSnapshot, executor *pb.ProcessRef) {
	t.Helper()
	writeControllerEnvelope(t, ctx, ws, &pb.Envelope{SessionId: session, CorrelationId: "inspect-current", RemainingBudgetMs: 1000, Payload: &pb.Envelope_Inspect{Inspect: &pb.ContextInspect{Context: expected.Context}}})
	frame := readControllerEnvelope(t, ctx, ws)
	result := frame.GetResult()
	if frame.CorrelationId != "inspect-current" || result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.Error != "" || result.Snapshot == nil ||
		!proto.Equal(result.Snapshot.Context, expected.Context) || !proto.Equal(result.Snapshot.Executor, executor) || result.Snapshot.CommandSequence != expected.CommandSequence ||
		result.Snapshot.ApplicationLeaseTerm != expected.ApplicationLeaseTerm || !proto.Equal(result.Snapshot.Application, expected.Application) ||
		result.Request != nil || result.Response != nil || result.SipStatus != 0 || result.RetirementProof != nil {
		t.Fatalf("inspection did not return only current metadata: %+v", result)
	}
}

func assertRedisNativeRetirementInspection(t *testing.T, ctx context.Context, native *redisNative, peer *control.Peer, ref *pb.ContextRef) {
	t.Helper()
	absent := proto.Clone(ref).(*pb.ContextRef)
	absent.Id = uuid.NewString()
	absent.State.GroupId = absent.Id
	absent.State.Incarnation = uuid.NewString()
	for _, name := range []string{"absent", "wrong-authority", "placeholder"} {
		candidate := proto.Clone(absent).(*pb.ContextRef)
		if name == "wrong-authority" {
			candidate.State.Authority.PrimaryId = "different-native-authority"
		}
		if name == "placeholder" {
			candidate.State = nil
		}
		result, err := native.inspectNativeContext(ctx, peer, candidate, time.Now().Add(time.Second))
		if err != nil || result == nil || result.Snapshot != nil || result.Request != nil || result.Response != nil {
			t.Fatal("inspection disclosed unrelated context/history", name, result, err)
		}
		if name == "absent" {
			if result.Certainty != pb.OperationResult_COMPLETED || result.RetirementProof == nil ||
				!proto.Equal(result.RetirementProof.Context, candidate) || result.RetirementProof.Authority != nil ||
				result.RetirementProof.Reason != pb.ContextRetirementProof_CONTEXT_ABSENT {
				t.Fatal("exact absence lacked positive proof", result)
			}
		} else if result.Certainty != pb.OperationResult_NOT_ACCEPTED || result.RetirementProof != nil {
			t.Fatal("unverified reference manufactured retirement evidence", name, result)
		}
	}
}
