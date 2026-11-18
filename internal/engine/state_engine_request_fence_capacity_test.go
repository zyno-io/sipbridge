package engine

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func requestFenceProofs(count int) []string {
	proofs := make([]string, count)
	for index := range proofs {
		proofs[index] = fmt.Sprintf("%064x", index+1)
	}
	return proofs
}

func requestFenceCapacityStore(t *testing.T, limit int) *state.Store {
	t.Helper()
	store, err := state.OpenDeferred(state.Config{Redis: state.RedisOptions{Address: "127.0.0.1:1"}, Prefix: "sipstate:proof-capacity",
		Namespace: "test-" + uuid.NewString(), NamespaceKey: bytes.Repeat([]byte{11}, 32), Partitions: 1, Generation: "generation",
		Versions: state.Versions{Schema: "1", Reducer: "7", Timers: "1"}, Limits: state.Limits{GroupBytes: limit}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func requestFenceEncodePayload(t *testing.T, r *stateReduction) []byte {
	t.Helper()
	encoded, err := marshalRetainedFixture(r.group)
	if err != nil {
		t.Fatal(err)
	}
	payload := r.payload
	payload.ProtocolState = encoded
	encoded, err = marshalRetainedFixture(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestStateRequestFenceProofEncodingMatchesActualTransactionAndPayload(t *testing.T) {
	for _, disposition := range []stateRequestDisposition{
		{},
		{DispositionUntilMS: 1<<53 - 1},
		{AdmittedFlowProofs: requestFenceProofs(1), DispositionUntilMS: 1},
		{AdmittedFlowProofs: requestFenceProofs(stateRequestFlowProofLimit), DispositionUntilMS: 1<<53 - 1},
	} {
		r, _, tx := serverFinalFixture(t, sip.OPTIONS)
		r.group.Version = 1
		tx.stateRequestDisposition = stateRequestDisposition{} // Measure against the explicit metadata-free encoding.
		beforeTX, err := marshalRetainedFixture(tx)
		if err != nil {
			t.Fatal(err)
		}
		beforePayload := requestFenceEncodePayload(t, r)
		tx.stateRequestDisposition = disposition
		actual, err := disposition.encodedBytes()
		if err != nil {
			t.Fatal(err)
		}
		afterTX, err := marshalRetainedFixture(tx)
		if err != nil {
			t.Fatal(err)
		}
		afterPayload := requestFenceEncodePayload(t, r)
		if actual < len(afterTX)-len(beforeTX) || actual < len(afterPayload)-len(beforePayload) || actual > stateRequestProofBytes {
			t.Fatalf("tiny encoder omitted actual tags/separators/Payload encoding: actual=%d tx=%d payload=%d bound=%d", actual, len(afterTX)-len(beforeTX), len(afterPayload)-len(beforePayload), stateRequestProofBytes)
		}
		if len(disposition.AdmittedFlowProofs) == stateRequestFlowProofLimit && actual <= 1475 {
			t.Fatal("maximum proof array omitted field/floor overhead")
		}
	}
}

func TestStateRequestFenceProofValidationRejectsMalformedRetainedMetadata(t *testing.T) {
	proof := requestFenceProofs(1)[0]
	for name, disposition := range map[string]stateRequestDisposition{
		"duplicate":     {AdmittedFlowProofs: []string{proof, proof}, DispositionUntilMS: 1},
		"too-many":      {AdmittedFlowProofs: requestFenceProofs(stateRequestFlowProofLimit + 1), DispositionUntilMS: 1},
		"short":         {AdmittedFlowProofs: []string{proof[:63]}, DispositionUntilMS: 1},
		"uppercase":     {AdmittedFlowProofs: []string{strings.Repeat("A", 64)}, DispositionUntilMS: 1},
		"non-hex":       {AdmittedFlowProofs: []string{strings.Repeat("g", 64)}, DispositionUntilMS: 1},
		"missing-floor": {AdmittedFlowProofs: []string{proof}},
		"negative":      {DispositionUntilMS: -1},
		"overflow":      {DispositionUntilMS: 1 << 53},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := disposition.encodedBytes(); !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("tiny encoder accepted invalid metadata: %v", err)
			}
			r, _, tx := serverFinalFixture(t, sip.OPTIONS)
			r.group.Version = 1
			tx.stateRequestDisposition = disposition
			encoded, err := marshalRetainedFixture(r.group)
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodeStateGroup(state.Record{Payload: state.Payload{ProtocolState: encoded}})
			if !errors.Is(err, state.ErrCorrupt) {
				t.Fatalf("retained decoder accepted invalid metadata: %v", err)
			}
		})
	}
	r, _, tx := serverFinalFixture(t, sip.OPTIONS)
	r.group.Version = 1
	tx.stateRequestDisposition = stateRequestDisposition{AdmittedFlowProofs: []string{proof}, DispositionUntilMS: 1}
	tx.Native.Family = portable.NonInviteClient
	encoded, err := marshalRetainedFixture(r.group)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeStateGroup(state.Record{Payload: state.Payload{ProtocolState: encoded}}); !errors.Is(err, state.ErrCorrupt) {
		t.Fatal("client transaction acquired a server-request admission proof")
	}
}

func TestStateRequestFenceRetainedTerminatedServersKeepConsumableReserve(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	tx.Native.Phase, tx.Native.LastResponse, tx.Native.Timers = portable.Terminated, nil, nil
	tx.RetireMS = r.now + 120000
	for _, disposition := range []stateRequestDisposition{
		{},
		{AdmittedFlowProofs: requestFenceProofs(1), DispositionUntilMS: 1},
		{AdmittedFlowProofs: requestFenceProofs(stateRequestFlowProofLimit), DispositionUntilMS: 1<<53 - 1},
	} {
		tx.stateRequestDisposition = disposition
		with, err := r.completionBudget()
		if err != nil {
			t.Fatal(err)
		}
		delete(c.Transactions, tx.Native.Key)
		without, err := r.completionBudget()
		c.Transactions[tx.Native.Key] = tx
		if err != nil {
			t.Fatal(err)
		}
		actual, err := disposition.encodedBytes()
		if err != nil {
			t.Fatal(err)
		}
		if with.Transactions != 0 || with.Effects != without.Effects || with.ProofBytes-without.ProofBytes != stateRequestProofBytes-actual || with.Bytes-without.Bytes != stateRequestProofBytes-actual {
			t.Fatalf("terminated server without LastResponse/effects lost consumable proof headroom: with=%+v without=%+v actual=%d", with, without, actual)
		}
	}
}

func TestStateRequestFenceProofReserveTransfersWithoutGrowingAccountedBytes(t *testing.T) {
	r, c, invite := serverFinalFixture(t, sip.INVITE)
	// Isolate one already-promised paired server CANCEL from dialog/fork work.
	// Native INVITE state still owns exactly that outstanding CANCEL procedure.
	c.Snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	before, err := r.completionBudget()
	if err != nil {
		t.Fatal(err)
	}
	if before.ContextSlots[c.Snapshot.Context.Id] != 2 {
		t.Fatal("fixture did not reserve exactly one paired CANCEL")
	}
	cancel, err := portable.BuildCancel(invite.Native.Origin)
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.NewTransaction(portable.NonInviteServer, cancel, portable.DefaultTimers(), c.Tag)
	if err != nil {
		t.Fatal(err)
	}
	native.Phase = portable.Terminated
	materialized := &stateTransaction{Native: native, RetireMS: r.now + 120000,
		stateRequestDisposition: stateRequestDisposition{AdmittedFlowProofs: requestFenceProofs(1), DispositionUntilMS: 1}}
	c.Transactions[native.Key] = materialized
	actual, err := materialized.stateRequestDisposition.encodedBytes()
	if err != nil {
		t.Fatal(err)
	}
	after, err := r.completionBudget()
	if err != nil {
		t.Fatal(err)
	}
	if after.ProofBytes+actual != before.ProofBytes || after.ContextSlots[c.Snapshot.Context.Id] != before.ContextSlots[c.Snapshot.Context.Id] {
		t.Fatalf("promised server materialization charged metadata twice: before=%+v after=%+v actual=%d", before, after, actual)
	}
	accounted := after.ProofBytes + actual
	for count := 2; count <= stateRequestFlowProofLimit; count++ {
		materialized.AdmittedFlowProofs = requestFenceProofs(count)
		materialized.DispositionUntilMS = 1<<53 - 1
		budget, err := r.completionBudget()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := materialized.stateRequestDisposition.encodedBytes()
		if err != nil || budget.ProofBytes+actual != accounted {
			t.Fatalf("proof/floor growth consumed new capacity: count=%d total=%d want=%d err=%v", count, budget.ProofBytes+actual, accounted, err)
		}
	}
}

func requestFenceRetainedOptions(t *testing.T, r *stateReduction, c *stateContext, index int) string {
	t.Helper()
	message, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.OPTIONS, URI: "sip:retained@example.test", From: "<sip:peer@example.test>",
		CallID: r.record.Ref.CallID, LocalTag: "retained", Branch: fmt.Sprintf("z9hG4bK-retained-%d", index), CSeq: uint32(index + 1), Advertise: "192.0.2.19:5061", Transport: "tls"})
	if err != nil {
		t.Fatal(err)
	}
	native, err := portable.NewTransaction(portable.NonInviteServer, message, portable.DefaultTimers(), c.Tag)
	if err != nil {
		t.Fatal(err)
	}
	native.Phase, native.Timers = portable.Terminated, nil
	c.Transactions[native.Key] = &stateTransaction{Native: native, Flow: c.Flow, RetireMS: r.now + 120000}
	return native.Key
}

func TestStateRequestFenceContextSlotsProtectPairedCancelAndForkCleanup(t *testing.T) {
	t.Run("paired-cancel", func(t *testing.T) {
		r, c, invite := serverFinalFixture(t, sip.INVITE)
		r.engine.store = requestFenceCapacityStore(t, 64<<20)
		r.deadline = r.now + 2000
		budget, err := r.completionBudget()
		if err != nil || budget.ContextSlots[c.Snapshot.Context.Id] != 4 {
			t.Fatalf("server INVITE did not reserve CANCEL and both directional BYEs: %+v %v", budget, err)
		}
		for index := 0; index < 64-budget.ContextSlots[c.Snapshot.Context.Id]; index++ {
			requestFenceRetainedOptions(t, r, c, index)
		}
		if !stateTestCapacityFits(r) {
			t.Fatal("reserved critical slots did not fit at their exact boundary")
		}
		extra := requestFenceRetainedOptions(t, r, c, 1000)
		if stateTestCapacityFits(r) {
			t.Fatal("ordinary transaction consumed paired CANCEL/BYE slots")
		}
		delete(c.Transactions, extra)
		cancel, err := portable.BuildCancel(invite.Native.Origin)
		if err != nil {
			t.Fatal(err)
		}
		r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application,
			Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: cancel.WireBytes()}}
		r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
		if err = r.ingest(); err != nil || !stateTestCapacityFits(r) {
			t.Fatalf("native CANCEL could not consume its reserved context slot: %v", err)
		}
	})
	t.Run("fork-cleanup", func(t *testing.T) {
		r, c, tx := forkFixture(t)
		r.engine.store = requestFenceCapacityStore(t, 64<<20)
		if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "winner")); err != nil {
			t.Fatal(err)
		}
		budget, err := r.completionBudget()
		if err != nil || budget.ContextSlots[c.Snapshot.Context.Id] != 34 {
			t.Fatalf("initial fork profile lost local cleanup/CANCEL/peer-BYE slots: %+v %v", budget, err)
		}
		for index := 0; index < 64-budget.ContextSlots[c.Snapshot.Context.Id]; index++ {
			requestFenceRetainedOptions(t, r, c, index)
		}
		if !stateTestCapacityFits(r) {
			t.Fatal("fork completion reserve did not fit at the context boundary")
		}
		extra := requestFenceRetainedOptions(t, r, c, 1000)
		if stateTestCapacityFits(r) {
			t.Fatal("ordinary transaction consumed promised fork cleanup slots")
		}
		delete(c.Transactions, extra)
		if _, err = r.forkResponse(c, tx, forkReply(t, tx, 200, "late-loser")); err != nil || !stateTestCapacityFits(r) {
			t.Fatalf("late fork ACK/BYE could not consume its reserved context slot: %v", err)
		}
	})
}

func TestStateRequestFenceNativeCancelFitsPretransitionGroupByteBoundary(t *testing.T) {
	r, c, invite := serverFinalFixture(t, sip.INVITE)
	r.engine.store = requestFenceCapacityStore(t, 64<<20)
	r.group.Version = 1
	r.group.Retired = make(map[string]int64)
	r.deadline = r.now + 2000
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	before, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalRetainedFixture(before)
	if err != nil {
		t.Fatal(err)
	}
	history, err := marshalRetainedFixture(r.record.Receipts)
	if err != nil {
		t.Fatal(err)
	}
	limits := r.engine.store.Limits()
	marker := base64.StdEncoding.EncodedLen(limits.OutcomeBytes) + 1024
	boundary := len(encoded) + len(history) + 4096 + before.ReservedCompletionBytes + 2*marker
	r.engine.store = requestFenceCapacityStore(t, boundary)
	if !r.engine.statePayloadFits(r.record, before) {
		t.Fatal("pre-transition payload does not fit its own exact used+reserved boundary")
	}
	cancel, err := portable.BuildCancel(invite.Native.Origin)
	if err != nil {
		t.Fatal(err)
	}
	r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application,
		Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: cancel.WireBytes()}}
	if err = r.ingest(); err != nil {
		t.Fatal(err)
	}
	after, err := r.finish()
	if err != nil || !r.engine.statePayloadFits(r.record, after) || !stateTestCapacityFits(r) {
		t.Fatalf("native CANCEL/proof materialization exceeded its pre-transition reserved byte boundary: %v", err)
	}
	selected := map[uint32]bool{}
	for _, tx := range c.Transactions {
		if tx.Native.LastResponse == nil {
			continue
		}
		message, err := sip.ParseMessage(tx.Native.LastResponse.WireBytes())
		if err != nil {
			t.Fatal(err)
		}
		response, ok := message.(*sip.Response)
		if ok {
			selected[uint32(response.StatusCode)] = true
		}
	}
	if !selected[200] || !selected[487] {
		t.Fatal("boundary completion did not retain its separate CANCEL200 and INVITE487 finals")
	}
}
