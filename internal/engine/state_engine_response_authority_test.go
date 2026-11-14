package engine

import (
	"bytes"
	"context"
	"github.com/emiago/sipgo/sip"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func nativeResponseRequest(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef, cseq uint32) string {
	t.Helper()
	_, g := stateTestLoad(t, f.engine, ref)
	c := g.Contexts[ref.Id]
	for id, key := range c.Requests {
		parsed, err := sip.ParseMessage(c.Transactions[key].Native.Origin.WireBytes())
		if err == nil && parsed.CSeq().SeqNo == cseq {
			return id
		}
	}
	t.Fatal("native response fixture request missing")
	return ""
}

func nativeCommandAdmission(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef) *pb.CommandAdmission {
	t.Helper()
	_, g := stateTestLoad(t, f.engine, ref)
	c := g.Contexts[ref.Id]
	admission := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli())}
	if c != nil {
		if c.OwnerDirectoryEpoch != "" {
			admission.DirectoryEpoch = c.OwnerDirectoryEpoch
		}
		if c.Delivery != nil {
			admission.ExpectedDelivery = proto.Clone(c.Delivery).(*pb.ContextDelivery)
		}
	}
	return admission
}

func nativeSelectResponse(t *testing.T, f nativeOwnerFixture, ref *pb.ContextRef, id string, seq uint64, request string, status uint32, admission *pb.CommandAdmission) *pb.OperationResult {
	t.Helper()
	command := &pb.Command{Context: ref, Sequence: seq, Admission: admission,
		Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: status, Body: []byte("v=0\r\n")}}}
	return f.apply(t, ref, id, stateEngineInput{Kind: "command", Application: f.app, ApplicationLeaseTerm: f.term, Command: command})
}

func TestNativeCommandAdmissionMissingOrMalformedCannotAcceptCall(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		admission *pb.CommandAdmission
	}{
		{name: "missing"},
		{name: "epoch", admission: &pb.CommandAdmission{DirectoryEpoch: "directory\n", ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli())}},
		{name: "expiry", admission: &pb.CommandAdmission{DirectoryEpoch: "directory"}},
		{name: "delivery", admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(10 * time.Second).UnixMilli()), ExpectedDelivery: &pb.ContextDelivery{DirectoryEpoch: "directory"}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newNativeOwnerFixture(t)
			ref := f.admit(t)
			request := nativeResponseRequest(t, f, ref, 1)
			command := &pb.Command{Context: ref, Sequence: 1, Admission: scenario.admission,
				Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: request, Status: 200, Body: []byte("v=0\r\n")}}}
			group, err := f.engine.group(ref)
			if err != nil {
				t.Fatal(err)
			}
			before, err := f.engine.store.Load(context.Background(), group)
			if err != nil {
				t.Fatal(err)
			}
			beforeGroup, err := decodeStateGroup(before)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.engine.apply(context.Background(), group, "invalid-admission", stateEngineInput{Kind: "command", Context: ref, Application: f.app,
				ApplicationLeaseTerm: f.term, Operation: "invalid-admission", Command: command}, time.Now().Add(2*time.Second), false)
			if err != nil || result == nil || result.Certainty != pb.OperationResult_NOT_ACCEPTED {
				t.Fatalf("invalid command admission was not refused: result=%+v err=%v", result, err)
			}
			after, err := f.engine.store.Load(context.Background(), group)
			if err != nil {
				t.Fatalf("failed to read retained call after refusal: %v", err)
			}
			afterGroup, err := decodeStateGroup(after)
			if err != nil {
				t.Fatal(err)
			}
			beforeContext, afterContext := beforeGroup.Contexts[ref.Id], afterGroup.Contexts[ref.Id]
			if beforeContext == nil || afterContext == nil || afterContext.Snapshot.Answered || afterContext.Dialog != "" ||
				afterContext.CompletionScope != beforeContext.CompletionScope || afterContext.OwnerDirectoryEpoch != beforeContext.OwnerDirectoryEpoch ||
				afterContext.Snapshot.CommandSequence != beforeContext.Snapshot.CommandSequence ||
				!reflect.DeepEqual(afterContext.Transactions, beforeContext.Transactions) || len(after.Payload.Effects) != len(before.Payload.Effects) {
				t.Fatalf("invalid command admission changed retained call state: %v", err)
			}
		})
	}
}

func TestNativeCommandAdmissionInitialAnchorIsStableAndIndependent(t *testing.T) {
	f := newNativeOwnerFixture(t)
	ref := f.admit(t)
	request := nativeResponseRequest(t, f, ref, 1)
	admission := nativeCommandAdmission(t, f, ref)
	if result := nativeSelectResponse(t, f, ref, "provisional", 1, request, 183, admission); result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, f.engine, ref)
	before := group.Contexts[ref.Id]
	if before.CompletionScope != f.engine.nativeCompletionScope(ref) || before.ScopeOrigin == nil || before.OwnerDirectoryEpoch != admission.DirectoryEpoch {
		t.Fatal("provisional response did not retain the native initial anchor")
	}

	changed := proto.Clone(admission).(*pb.CommandAdmission)
	changed.DirectoryEpoch = "another-directory"
	if result := nativeSelectResponse(t, f, ref, "wrong-epoch", 2, request, 200, changed); result.Certainty != pb.OperationResult_NOT_ACCEPTED {
		t.Fatal("a changed command admission replaced the initial anchor")
	}
	if result := nativeSelectResponse(t, f, ref, "accept", 2, request, 200, admission); result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group := stateTestLoad(t, f.engine, ref)
	c := group.Contexts[ref.Id]
	if !c.Snapshot.Answered || c.CompletionScope != f.engine.nativeCompletionScope(ref) || c.ScopeOrigin == nil ||
		c.OwnerDirectoryEpoch != admission.DirectoryEpoch || c.DialogScopes[c.Dialog] != c.CompletionScope {
		t.Fatal("accepted initial response changed its native anchor")
	}
	tx := c.Transactions[c.Requests[request]]
	if tx.CompletionScope != c.CompletionScope || !bytes.Equal(c.ScopeOrigin.WireBytes(), tx.Native.Origin.WireBytes()) {
		t.Fatal("native response anchor did not bind the original retained INVITE")
	}
	var native bool
	for _, effect := range record.Payload.Effects {
		var body stateEngineEffect
		if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if body.Authorization != nil {
			native = true
			if body.Authorization.CompletionScope != f.engine.nativeCompletionScope(ref) {
				t.Fatal("selected response omitted its independent native completion capability")
			}
		}
	}
	if !native {
		t.Fatal("selected response omitted its native completion effect")
	}
}
