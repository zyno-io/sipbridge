package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

func TestCompactedWaitDeadlinePreservesLastObservedSnapshotAndMessage(t *testing.T) {
	engine := &StateEngine{cfg: Config{Process: &pb.ProcessRef{InstanceId: "executor", Epoch: "epoch"}}}
	request := stateTestRequest("OPTIONS", 1, "", "wait-expiry")
	retained := &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED, Request: mustObserve(request.Wire, true)}
	result, err := engine.awaitOperationPolling(context.Background(), state.Ref{}, "context", "operation", &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}, true, stateOperationPolling{
		load: func(context.Context, state.Ref) (state.Record, error) {
			return operationWaitRecord(t, "operation", retained), nil
		},
		wait: func(context.Context, time.Duration) bool { return false },
		now:  time.Now,
	})
	if err != nil || result.GetCertainty() != pb.OperationResult_UNKNOWN || result.GetSnapshot().GetRevision() != 17 || result.GetRequest().GetMethod() != "OPTIONS" || !bytes.Equal(result.GetRequest().GetWire(), request.Wire) {
		t.Fatal("wait deadline lost the last observed public result", err)
	}
}

func TestCompactedCommandReplayProjectsCurrentSnapshot(t *testing.T) {
	r, c, tx := advertiseFixture(t, "tls", sip.INVITE, "")
	r.fingerprint = "same-command-payload"
	advertiseCommand(t, r, c, &pb.Command{Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: 200, Body: []byte("v=0\r\n")}}})
	c.Snapshot.Revision = 17
	c.Delivery = &pb.ContextDelivery{Flow: c.Flow, NativeRevision: 9, DirectoryEpoch: "accepted-directory"}
	c.OwnerDirectoryEpoch = "accepted-directory"
	encoded, err := r.group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.group.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	c = r.group.Contexts[c.Snapshot.Context.Id]
	if c.Result.Snapshot != nil {
		t.Fatal("fixture did not reload a compact result")
	}
	before := proto.Clone(c.Result).(*pb.OperationResult)
	r.result = nil
	if err := r.command(); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(r.result.Snapshot, stateSnapshot(c, r.engine.cfg.Process)) || !proto.Equal(c.Result, before) {
		t.Fatal("command replay lost current delivery/revision or mutated cached evidence")
	}
	r.result.Snapshot = nil
	if !proto.Equal(r.result, before) {
		t.Fatal("command replay changed cached outcome")
	}
}

func TestRetainedResultOmitsOnlyDerivedSnapshot(t *testing.T) {
	group := benchmarkBinaryGroup(1)
	c := group.Contexts["fixture"]
	c.Snapshot.LocalSdp = bytes.Repeat([]byte("local-session\r\n"), 64)
	c.Snapshot.RemoteSdp = bytes.Repeat([]byte("remote-session\r\n"), 64)
	c.Result = &pb.OperationResult{
		Certainty: pb.OperationResult_UNKNOWN, Error: "physical outcome unknown", SipStatus: 200,
		Snapshot:        proto.Clone(c.Snapshot).(*pb.ContextSnapshot),
		Request:         stateTestRequest("INVITE", 1, "", "compaction"),
		RetirementProof: &pb.ContextRetirementProof{Context: c.Snapshot.Context, Reason: pb.ContextRetirementProof_CONTEXT_ABSENT},
	}
	c.Result.Request = mustObserve(c.Result.Request.Wire, true)
	request, err := sip.ParseMessage(c.Result.Request.Wire)
	if err != nil {
		t.Fatal(err)
	}
	response := sip.NewResponseFromRequest(request.(*sip.Request), 200, "OK", []byte("v=0\r\n"))
	c.Result.Response, err = Observe([]byte(response.String()), false)
	if err != nil {
		t.Fatal(err)
	}
	before := proto.Clone(c.Result).(*pb.OperationResult)
	legacy, err := storagecodec.Marshal(storagecodec.Group, storageEncodeEngineStateGroup(group))
	if err != nil {
		t.Fatal(err)
	}
	compact, err := group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(c.Result, before) {
		t.Fatal("encoding mutated the operation-local result")
	}
	if len(compact) >= len(legacy) || len(legacy)-len(compact) < len(c.Snapshot.LocalSdp)+len(c.Snapshot.RemoteSdp) {
		t.Fatalf("duplicate session description retained: previous=%d compact=%d", len(legacy), len(compact))
	}
	var retained storagepb.EngineStateGroup
	if err := storagecodec.Unmarshal(compact, storagecodec.Group, &retained); err != nil {
		t.Fatal(err)
	}
	if retained.Contexts.Values["fixture"].Value.Result.Snapshot != nil {
		t.Fatal("encoded cached result contains a snapshot")
	}
	for _, data := range [][]byte{legacy, compact} {
		var decoded stateGroup
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatal(err)
		}
		got := decoded.Contexts["fixture"]
		if got.Result.Snapshot != nil || !proto.Equal(got.Snapshot, c.Snapshot) {
			t.Fatal("decoded result retained a duplicate or changed the authoritative snapshot")
		}
		if !proto.Equal(got.Result.Request, &pb.SipMessage{Wire: before.Request.Wire}) || !proto.Equal(got.Result.Response, &pb.SipMessage{Wire: before.Response.Wire}) {
			t.Fatal("decoded cached result contains a parsed message graph")
		}
		got.Result.Snapshot = proto.Clone(before.Snapshot).(*pb.ContextSnapshot)
		if err := stateMaterializeResultMessages(got.Result); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(got.Result, before) {
			t.Fatal("compaction changed cached outcome or SIP evidence")
		}
		request, response := got.Result.Request, got.Result.Response
		if err := stateMaterializeResultMessages(got.Result); err != nil || got.Result.Request != request || got.Result.Response != response {
			t.Fatal("already validated public messages were reparsed", err)
		}
	}
}

func TestCachedResultCannotTrustRetainedParsedMessageFields(t *testing.T) {
	group := benchmarkBinaryGroup(1)
	group.Contexts["fixture"].Result = &pb.OperationResult{Request: &pb.SipMessage{Method: "INVITE", Wire: []byte("invalid SIP")}}
	legacy, err := storagecodec.Marshal(storagecodec.Group, storageEncodeEngineStateGroup(group))
	if err != nil {
		t.Fatal(err)
	}
	var decoded stateGroup
	if err := decoded.UnmarshalBinary(legacy); err != nil {
		t.Fatal(err)
	}
	result := decoded.Contexts["fixture"].Result
	if result.Request.Method != "" || !errors.Is(stateMaterializeResultMessages(result), state.ErrCorrupt) {
		t.Fatal("retained parsed fields bypassed fresh wire validation")
	}
}

func TestCachedResultMessageProjectionRejectsInvalidWireAtomically(t *testing.T) {
	valid := stateTestRequest("INVITE", 1, "", "compaction-invalid")
	request, err := sip.ParseMessage(valid.Wire)
	if err != nil {
		t.Fatal(err)
	}
	response := sip.NewResponseFromRequest(request.(*sip.Request), 200, "OK", nil)
	responseWire := []byte(response.String())
	for name, result := range map[string]*pb.OperationResult{
		"empty request":              {Request: &pb.SipMessage{}},
		"malformed request":          {Request: &pb.SipMessage{Wire: []byte("invalid SIP")}},
		"malformed response":         {Request: &pb.SipMessage{Wire: valid.Wire}, Response: &pb.SipMessage{Wire: []byte("invalid SIP")}},
		"response is request":        {Response: &pb.SipMessage{Wire: valid.Wire}},
		"request is response":        {Request: &pb.SipMessage{Wire: responseWire}},
		"parsed request is response": {Request: mustObserve(responseWire, true)},
	} {
		t.Run(name, func(t *testing.T) {
			before := proto.Clone(result).(*pb.OperationResult)
			if err := stateMaterializeResultMessages(result); !errors.Is(err, state.ErrCorrupt) {
				t.Fatal("invalid retained message accepted", err)
			}
			if !proto.Equal(result, before) {
				t.Fatal("failed projection published partial fields")
			}
		})
	}
}

// Both representations use the same completed native subscription fixture.
// Record bytes are serialized protocol-state bytes, not Redis MEMORY USAGE or
// retained Go heap. Decode B/op measures transient allocations, not RSS.
func BenchmarkRetainedResultSnapshotCompaction(b *testing.B) {
	for _, transactions := range []int{2, 6, 16} {
		fixture := idleSubscriptionOwnerFixture(b, transactions)
		var payload state.Payload
		if err := payload.UnmarshalBinary(fixture.Payload); err != nil {
			b.Fatal(err)
		}
		var group stateGroup
		if err := group.UnmarshalBinary(payload.ProtocolState); err != nil {
			b.Fatal(err)
		}
		for _, c := range group.Contexts {
			if c.Result != nil {
				if err := stateMaterializeResultMessages(c.Result); err != nil {
					b.Fatal(err)
				}
				c.Result.Snapshot = stateSnapshot(c, fixture.Process)
			}
		}
		for _, compact := range []bool{false, true} {
			name := "previous"
			data, err := storagecodec.Marshal(storagecodec.Group, storageEncodeEngineStateGroup(group))
			if compact {
				name = "compact"
				data, err = group.MarshalBinary()
			}
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("transactions-%d/%s", transactions, name), func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(data)), "protocol-bytes")
				for n := 0; n < b.N; n++ {
					var decoded stateGroup
					if err := decoded.UnmarshalBinary(data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
