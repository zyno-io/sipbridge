package engine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func residentOwnedOutputFixture(unknown bool) *pb.FlowWrite {
	process := &pb.ProcessRef{InstanceId: "native", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	effect := &pb.StateEffectRef{Context: &pb.ContextRef{Owner: proto.Clone(process).(*pb.ProcessRef), Id: "context", State: &pb.StateRef{
		Authority: NewResidentMemoryAuthority("test", "one"), GroupId: "group", Incarnation: "incarnation", CallId: "call@example.test"}}, Id: "effect", AttemptId: "attempt"}
	addUnknown := func(message proto.Message) {
		if unknown {
			wire := protowire.AppendTag(nil, 200, protowire.BytesType)
			message.ProtoReflect().SetUnknown(protowire.AppendString(wire, "opaque metadata"))
		}
	}
	for _, message := range []proto.Message{process, flow, effect, effect.Context, effect.Context.Owner, effect.Context.State, effect.Context.State.Authority} {
		addUnknown(message)
	}
	permit := &pb.SendPermit{Flow: proto.Clone(flow).(*pb.FlowRef), Authorization: &pb.EffectAuthorization{
		Effect: proto.Clone(effect).(*pb.StateEffectRef), DeadlineMs: 1001000, AuthorityClass: pb.EffectAuthorization_APPLICATION,
		ApplicationLeaseTerm: "lease", Fingerprint: "fingerprint", CompletionScope: "completion"},
		DirectoryEpoch: "directory", OrderingProof: "ordering", CompletionScope: "completion"}
	write := &pb.FlowWrite{Flow: flow, Effect: effect, Permit: permit}
	for _, message := range []proto.Message{permit.Authorization, permit, write} {
		addUnknown(message)
	}
	return write
}

func TestResidentOutputOwnsCanonicalProofAndUnknownFields(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "equal-distinct", true: "shared-input"}[shared], func(t *testing.T) {
			write := residentOwnedOutputFixture(true)
			if shared {
				write.Permit.Flow, write.Permit.Authorization.Effect = write.Flow, write.Effect
			}
			want := proto.Clone(write).(*pb.FlowWrite)
			wire := []byte("independent SIP wire")
			output, err := newResidentOutput(write, wire, time.Unix(1001, 0))
			if err != nil {
				t.Fatal(err)
			}
			if output.write.Flow != output.write.Permit.Flow || output.write.Effect != output.write.Permit.Authorization.Effect {
				t.Fatal("owned proof did not share its canonical flow and effect")
			}
			wantBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			gotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(output.write)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) {
				t.Fatal("owned proof changed fields or unknown wire metadata", err)
			}
			// Clear every input object and mutate its backing unknown bytes. The
			// output must own all descendants, including outer unknown metadata.
			inputs := []proto.Message{write, write.Flow, write.Flow.Ingress, write.Effect, write.Effect.Context, write.Effect.Context.Owner,
				write.Effect.Context.State, write.Effect.Context.State.Authority, write.Permit, write.Permit.Flow, write.Permit.Flow.Ingress,
				write.Permit.Authorization, write.Permit.Authorization.Effect, write.Permit.Authorization.Effect.Context,
				write.Permit.Authorization.Effect.Context.Owner, write.Permit.Authorization.Effect.Context.State,
				write.Permit.Authorization.Effect.Context.State.Authority}
			for _, input := range inputs {
				if raw := input.ProtoReflect().GetUnknown(); len(raw) > 0 {
					raw[len(raw)-1] ^= 1
				}
				proto.Reset(input)
			}
			wire[0] = 'x'
			if !proto.Equal(output.write, want) || string(output.wire) != "independent SIP wire" {
				t.Fatal("mutating input changed the retained proof or wire")
			}
		})
	}
}

func TestResidentOutputProjectionCoversCanonicalFields(t *testing.T) {
	fields := (&pb.FlowWrite{}).ProtoReflect().Descriptor().Fields()
	var names []string
	for index := 0; index < fields.Len(); index++ {
		names = append(names, string(fields.Get(index).Name()))
	}
	if strings.Join(names, ",") != "flow,effect,permit" {
		t.Fatalf("review owned FlowWrite projection for canonical fields %v", names)
	}
}

func TestResidentOutputRetainsDeliveryWitnessOnlyForApplication(t *testing.T) {
	for _, class := range []pb.EffectAuthorization_Class{pb.EffectAuthorization_APPLICATION, pb.EffectAuthorization_RESPONSE, pb.EffectAuthorization_NATIVE_COMPLETION} {
		t.Run(class.String(), func(t *testing.T) {
			e, c, tx := residentSubscriptionFixture(t)
			e.maxBytes, e.deadlineWake = 64<<20, make(chan struct{}, 1)
			c.outputs = make(map[string]*residentOutput)
			c.snapshot.ApplicationLeaseTerm = "lease"
			e.leases = map[string]*residentLease{residentControllerKey(c.snapshot.Application): {term: "lease", expires: time.Now().Add(time.Minute)}}
			sub, snapshot, response, err := e.prepareResidentSubscription(c, tx, &pb.Respond{Status: 202}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			c.subscription, c.snapshot = sub, snapshot
			witness := &pb.ContextDelivery{Flow: proto.Clone(c.flow).(*pb.FlowRef), NativeRevision: 1, DirectoryEpoch: "directory", RegistrationTicket: "ticket"}
			c.snapshot.Delivery = proto.Clone(witness).(*pb.ContextDelivery)
			e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
			c.budget, err = e.openResidentBudgetLocked(c, residentSubscriptionBudget)
			if err != nil {
				t.Fatal(err)
			}
			tx.budget = c.budget
			effects := []portable.Effect{{Kind: portable.Send, Message: &response}}
			if class != pb.EffectAuthorization_RESPONSE {
				_, tx, effects, err = e.prepareResidentNotify(c, &pb.DialogRequest{Method: "NOTIFY"}, "witness-test", time.Now())
				if err != nil {
					t.Fatal(err)
				}
				tx.budget = c.budget
			}
			policy := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpectedDelivery: witness, ExpiresAtMs: uint64(time.Now().Add(time.Second).UnixMilli())}
			writes, err := e.stageResidentOutputGroupsLocked(c, []residentOutputGroup{{tx: tx, effects: effects, policy: policy, nativeCompletion: class == pb.EffectAuthorization_NATIVE_COMPLETION}}, "witness-test", time.Now().Add(time.Second))
			if err != nil || len(writes) != 1 {
				t.Fatal("failed to stage exact output", err)
			}
			output := c.outputs[writes[0].Effect.Id]
			if output.write.Permit.Authorization.AuthorityClass != class {
				t.Fatal("wrong staged authority class")
			}
			if class == pb.EffectAuthorization_APPLICATION {
				if output.expectedDelivery == witness || !proto.Equal(output.expectedDelivery, witness) {
					t.Fatal("application output did not own its original delivery witness")
				}
			} else if output.expectedDelivery != nil {
				t.Fatal("native output retained an unused delivery witness")
			}
			witness.RegistrationTicket = "mutated input"
			c.snapshot.Delivery.RegistrationTicket = "new accepted delivery"
			_, err = e.Reserve(context.Background(), writes[0])
			if (err != nil) != (class == pb.EffectAuthorization_APPLICATION) {
				t.Fatal("delivery change did not preserve authority-class fencing", err)
			}
		})
	}
}

var residentOutputConstructionSink *residentOutput

func BenchmarkResidentOutputConstruction(b *testing.B) {
	write := residentOwnedOutputFixture(false)
	write.Permit.Flow, write.Permit.Authorization.Effect = write.Flow, write.Effect
	wire, until := []byte("independent SIP wire"), time.Unix(1001, 0)
	// Keep the former constructor only as an allocation comparison. Both cases
	// validate the same input and retain the same owned wire and canonical proof.
	fullClone := func(write *pb.FlowWrite, wire []byte, deadline time.Time) (*residentOutput, error) {
		if write == nil || write.Effect == nil || write.Effect.Context == nil || write.Effect.Id == "" || write.Effect.AttemptId == "" || write.Permit == nil || write.Permit.Authorization == nil || write.Flow == nil || len(wire) == 0 || deadline.IsZero() ||
			!proto.Equal(write.Effect, write.Permit.Authorization.Effect) || !proto.Equal(write.Flow, write.Permit.Flow) {
			return nil, errors.New("invalid physical output authority")
		}
		owned := proto.Clone(write).(*pb.FlowWrite)
		owned.Permit.Flow, owned.Permit.Authorization.Effect = owned.Flow, owned.Effect
		return &residentOutput{wireBytes: int64(len(wire)), write: owned, wire: append([]byte(nil), wire...), deadline: deadline}, nil
	}
	for _, sample := range []struct {
		name string
		make func(*pb.FlowWrite, []byte, time.Time) (*residentOutput, error)
	}{{"FullCloneBaseline", fullClone}, {"OwnedPermit", newResidentOutput}} {
		b.Run(sample.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				output, err := sample.make(write, wire, until)
				if err != nil {
					b.Fatal(err)
				}
				residentOutputConstructionSink = output
			}
		})
	}
}
