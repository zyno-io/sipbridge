package engine

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/storagecodec"
)

// Freeze the former full-copy projection as a byte-equivalence oracle. It is
// intentionally expensive and confined to tests.
func clonedEffectStorageProjection(value stateEngineEffect) stateEngineEffect {
	if value.Envelope == nil {
		return value
	}
	envelope := proto.Clone(value.Envelope).(*pb.Envelope)
	var message *pb.SipMessage
	if request := envelope.GetRequest(); request != nil {
		message, request.Message = request.Message, nil
	}
	if egress := envelope.GetEgress(); egress != nil {
		message, egress.Message = egress.Message, nil
	}
	if event := envelope.GetDialogEvent(); event != nil {
		message, event.Message = event.Message, nil
	}
	if message != nil && len(value.Wire) == 0 {
		value.Wire = message.Wire
	}
	value.Envelope = envelope
	return value
}

func storageProjectionFixture(t testing.TB, kind string) stateEngineEffect {
	t.Helper()
	value := metadataFixtureEffect(t, kind, false)
	value.Wire = nil // The storage projection must select the original exact wire.
	value.Envelope.SessionId, value.Envelope.RemainingBudgetMs = "session", 1777
	snapshot := &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context"}, Revision: 42, CallId: "fixture", Ended: true}
	flow := &pb.FlowRef{ConnectionId: "connection", Transport: "tls"}
	switch kind {
	case "request":
		value.Envelope.GetRequest().Snapshot = snapshot
		value.Envelope.GetRequest().RequestId = "request"
		value.Envelope.GetRequest().Flow = flow
	case "egress":
		value.Envelope.GetEgress().Snapshot = snapshot
		value.Envelope.GetEgress().ReturnFlow = flow
		value.Envelope.GetEgress().Authorization = &pb.EffectAuthorization{Effect: &pb.StateEffectRef{Context: snapshot.Context}, DeadlineMs: 12345}
	case "event":
		value.Envelope.GetDialogEvent().Snapshot = snapshot
		value.Envelope.GetDialogEvent().Kind = pb.DialogEvent_FAILED
		value.Envelope.GetDialogEvent().OperationId = "operation"
		value.Envelope.GetDialogEvent().Error = "fixture failure"
	}
	return value
}

func TestEffectStorageProjectionMatchesFullCloneWithoutMutatingInput(t *testing.T) {
	for _, kind := range []string{"request", "egress", "event"} {
		t.Run(kind, func(t *testing.T) {
			value := storageProjectionFixture(t, kind)
			original := proto.Clone(value.Envelope).(*pb.Envelope)
			for _, explicitWire := range []bool{false, true} {
				if explicitWire {
					value.Wire = []byte("already selected exact wire")
				}
				want := clonedEffectStorageProjection(value)
				got := stateEffectForStorage(value)
				if !proto.Equal(want.Envelope, got.Envelope) || !bytes.Equal(want.Wire, got.Wire) {
					t.Fatal("storage metadata or exact wire changed")
				}
				wantBytes, err := storagecodec.Marshal(storagecodec.Effect, storageEncodeEngineStateEngineEffect(want))
				if err != nil {
					t.Fatal(err)
				}
				gotBytes, err := value.MarshalBinary()
				if err != nil || !bytes.Equal(wantBytes, gotBytes) {
					t.Fatal("retained binary bytes changed", err)
				}
				if !proto.Equal(original, value.Envelope) {
					t.Fatal("projection mutated caller's message")
				}
			}
		})
	}
}

func TestEffectStorageProjectionCoversCanonicalMetadataFields(t *testing.T) {
	// A new public metadata field requires reviewing the storage projection;
	// silently dropping it would change authority even if SIP wire stayed exact.
	for _, sample := range []struct {
		value  proto.Message
		fields string
	}{
		{&pb.RequestPresented{}, "snapshot,request_id,message,flow"},
		{&pb.EgressRequest{}, "snapshot,message,return_flow,authorization"},
		{&pb.DialogEvent{}, "kind,snapshot,message,operation_id,error"},
	} {
		fields := sample.value.ProtoReflect().Descriptor().Fields()
		var actual string
		for i := 0; i < fields.Len(); i++ {
			if i > 0 {
				actual += ","
			}
			actual += string(fields.Get(i).Name())
		}
		if actual != sample.fields {
			t.Fatalf("review retained metadata projection for %T: %s", sample.value, actual)
		}
	}
	fields := (&pb.Envelope{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.ContainingOneof() != nil || field.Name() == protoreflect.Name("correlation_id") || field.Name() == protoreflect.Name("session_id") || field.Name() == protoreflect.Name("remaining_budget_ms") {
			continue
		}
		t.Fatalf("review retained envelope metadata projection for new field %s", field.Name())
	}
}

func BenchmarkEffectStorageProjection(b *testing.B) {
	for _, kind := range []string{"request", "egress", "event"} {
		value := storageProjectionFixture(b, kind)
		for _, fullClone := range []bool{true, false} {
			name := "metadata"
			if fullClone {
				name = "full-clone"
			}
			b.Run(kind+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var projected stateEngineEffect
					if fullClone {
						projected = clonedEffectStorageProjection(value)
					} else {
						projected = stateEffectForStorage(value)
					}
					if _, err := storagecodec.Marshal(storagecodec.Effect, storageEncodeEngineStateEngineEffect(projected)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
