package state

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func rawReceiptResults(t *testing.T, backend Backend, ref Ref) (string, string) {
	t.Helper()
	switch s := backend.(type) {
	case *Store:
		values, err := s.client.HMGet(context.Background(), s.keys(ref)[0], "receipts", "proof").Result()
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(values[0]), fmt.Sprint(values[1])
	case *MemoryStore:
		if _, err := s.lock(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer s.unlock()
		g, err := s.group(ref)
		if err != nil {
			t.Fatal(err)
		}
		receipts, _ := json.Marshal(g.receipts)
		proof := ""
		if g.proof != nil {
			wire, _ := json.Marshal(g.proof)
			proof = string(wire)
		}
		return string(receipts), proof
	default:
		t.Fatal("unknown test backend")
		return "", ""
	}
}

func requireDiscardedReceipt(t *testing.T, receipt *Receipt, original Receipt) {
	t.Helper()
	if receipt == nil || !receipt.OutcomeDiscarded || len(receipt.Outcome) != 0 {
		t.Fatalf("missing discarded-result marker: %+v", receipt)
	}
	expected := original
	expected.Outcome = nil
	expected.OutcomeDiscarded = true
	if !reflect.DeepEqual(*receipt, expected) {
		t.Fatalf("result discard changed commit evidence: got %+v want %+v", *receipt, expected)
	}
}

func TestReceiptResultExpiryKeepsPositiveEvidenceAndReplayIdentity(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		for _, proof := range []bool{false, true} {
			t.Run(fmt.Sprint("proof-", proof), func(t *testing.T) {
				ctx := context.Background()
				in := contractInput(fmt.Sprint("refused-", proof), true)
				in.Proof = proof
				in.ResultUntilMS = time.Now().Add(-time.Second).UnixMilli()
				update := contractRead(t, s, contractRef(t, s, in.ID), in)
				original, err := s.CompareAndSwap(ctx, update, Payload{}, []byte(`{"certainty":"NOT_ACCEPTED","error":"refused"}`))
				if err != nil || original.OutcomeDiscarded || len(original.Outcome) == 0 {
					t.Fatal("fresh definite commit lost its own result", err)
				}
				record, now, err := s.LoadAt(ctx, update.Record.Ref)
				if err != nil || now < in.ResultUntilMS {
					t.Fatal("same-read clock unavailable", err)
				}
				retained := record.Proof
				if !proof {
					retained = &record.Receipts[0]
				}
				requireDiscardedReceipt(t, retained, original)
				// Relative control budgets may regenerate deadlines on exact replay.
				// They cannot renew the original result cutoff or commit marker.
				retry := in
				retry.DeadlineMS += 1000
				retry.ReceiptUntilMS += 1000
				retry.ResultUntilMS = retry.DeadlineMS
				replay, err := s.ReadUpdate(ctx, update.Record.Ref, retry)
				if err != nil || replay.Update != nil {
					t.Fatal("exact replay became fresh work", err)
				}
				requireDiscardedReceipt(t, replay.Receipt, original)
				inspection, err := s.InspectOperation(ctx, update.Record.Ref, in.ID, in.Fingerprint)
				if err != nil || inspection.Pending {
					t.Fatal("positive commit proof became missing/pending", err)
				}
				requireDiscardedReceipt(t, inspection.Receipt, original)
				retry.Fingerprint = "different"
				if _, err := s.ReadUpdate(ctx, update.Record.Ref, retry); !errors.Is(err, ErrConflict) {
					t.Fatal("discarded refusal allowed conflicting replay", err)
				}
			})
		}
	})
}

func TestReceiptResultReadSuppressionAndFailedCASDoNotCompactStorage(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		ref := contractRef(t, s, "lazy-results")
		cutoff := time.Now().Add(time.Second).UnixMilli()
		var originals []Receipt
		for n := 0; n < 5; n++ {
			in := contractInput(fmt.Sprint("result-", n), n == 0)
			in.ResultUntilMS = cutoff
			in.Proof = n == 4
			update := contractRead(t, s, ref, in)
			receipt, err := s.CompareAndSwap(ctx, update, Payload{}, bytes.Repeat([]byte{'x'}, 64))
			if err != nil {
				t.Fatal(err)
			}
			ref = update.Record.Ref
			originals = append(originals, receipt)
		}
		before, proofBefore := rawReceiptResults(t, s, ref)
		if bytes.Contains([]byte(before), []byte("outcomeDiscarded")) {
			t.Fatal("fixture cutoff elapsed before records were created")
		}
		time.Sleep(max(0, time.Until(time.UnixMilli(cutoff).Add(10*time.Millisecond))))
		record, err := s.Load(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		for n := range record.Receipts {
			requireDiscardedReceipt(t, &record.Receipts[n], originals[n])
		}
		requireDiscardedReceipt(t, record.Proof, originals[4])
		inspection, err := s.InspectOperation(ctx, ref, originals[0].OperationID, originals[0].Fingerprint)
		if err != nil {
			t.Fatal(err)
		}
		requireDiscardedReceipt(t, inspection.Receipt, originals[0])
		update := contractRead(t, s, ref, contractInput("quota-refusal", false))
		if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); !errors.Is(err, ErrCapacity) {
			t.Fatal("discarded markers stopped consuming receipt slots", err)
		}
		after, proofAfter := rawReceiptResults(t, s, ref)
		if before != after || proofBefore != proofAfter {
			t.Fatal("read or failed CAS mutated retained receipt/proof bytes")
		}
		// An internal transition can compact history without consuming another
		// ordinary marker or replacing the external cleanup proof.
		input := contractInput("compact", false)
		input.Kind = "timer"
		input.Proof = true
		update = contractRead(t, s, ref, input)
		if _, err := s.CompareAndSwap(ctx, update, Payload{}, nil); err != nil {
			t.Fatal(err)
		}
		after, proofAfter = rawReceiptResults(t, s, ref)
		if len(after) >= len(before) || len(proofAfter) >= len(proofBefore) {
			t.Fatal("successful commit did not reclaim result bytes")
		}
		record, err = s.Load(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		for n := range record.Receipts {
			requireDiscardedReceipt(t, &record.Receipts[n], originals[n])
		}
		requireDiscardedReceipt(t, record.Proof, originals[4])
		t.Logf("five 64-byte outcomes: receipt/proof JSON %d -> %d bytes; identities and slots unchanged", len(before)+len(proofBefore), len(after)+len(proofAfter))
	})
}

func TestReceiptResultCompactionPrecedesByteCapacity(t *testing.T) {
	backendContract(t, func(t *testing.T, s Backend) {
		ctx := context.Background()
		ref := contractRef(t, s, "result-capacity")
		in := contractInput("old", true)
		in.ResultUntilMS = time.Now().Add(300 * time.Millisecond).UnixMilli()
		update := contractRead(t, s, ref, in)
		old, err := s.CompareAndSwap(ctx, update, Payload{}, bytes.Repeat([]byte{'x'}, 64))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(max(0, time.Until(time.UnixMilli(in.ResultUntilMS).Add(10*time.Millisecond))))
		input := contractInput("new", false)
		update = contractRead(t, s, update.Record.Ref, input)
		digest, _, _ := digestInput(input)
		newReceipt := Receipt{OperationID: input.ID, Fingerprint: input.Fingerprint, InputDigest: digest, Revision: "2", ExpiresMS: input.ReceiptUntilMS}
		compact := receiptResultAt(old, in.ResultUntilMS)
		encoded, _ := json.Marshal([]Receipt{compact, newReceipt})
		limit := s.Limits().GroupBytes - 4096 - base64.StdEncoding.EncodedLen(s.Limits().OutcomeBytes) - 1024 - len(encoded)
		next := Payload{ProtocolState: bytes.Repeat([]byte{'x'}, limit)}
		for {
			data, err := next.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if len(data) <= limit {
				break
			}
			next.ProtocolState = next.ProtocolState[:len(next.ProtocolState)-1]
		}
		data, _ := next.MarshalBinary()
		full, _ := json.Marshal([]Receipt{old, newReceipt})
		if len(data)+len(full) <= limit+len(encoded) {
			t.Fatal("fixture does not require reclaimed receipt capacity")
		}
		if _, err := s.CompareAndSwap(ctx, update, next, nil); err != nil {
			t.Fatal("expired outcome prevented otherwise fitting commit", err)
		}
	})
}

func TestReceiptResultBoundaryEmptyAndDefaultPolicy(t *testing.T) {
	base := time.Now()
	now := base
	s, err := newMemory(contractConfig(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	in := contractInput("empty", true)
	in.ResultUntilMS = base.UnixMilli() + 100
	update := contractRead(t, s, contractRef(t, s, "empty-result"), in)
	original, err := s.CompareAndSwap(context.Background(), update, Payload{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	now = base.Add(99 * time.Millisecond)
	record, _, err := s.LoadAt(context.Background(), update.Record.Ref)
	if err != nil || record.Receipts[0].OutcomeDiscarded || len(record.Receipts[0].Outcome) != 0 {
		t.Fatal("legitimate empty result treated as discarded", err)
	}
	now = base.Add(100 * time.Millisecond)
	record, _, err = s.LoadAt(context.Background(), update.Record.Ref)
	if err != nil {
		t.Fatal(err)
	}
	requireDiscardedReceipt(t, &record.Receipts[0], original)
	in = contractInput("default", false)
	update = contractRead(t, s, update.Record.Ref, in)
	if _, err := s.CompareAndSwap(context.Background(), update, Payload{}, []byte("default result")); err != nil {
		t.Fatal(err)
	}
	now = base.Add(time.Second)
	replay, err := s.ReadUpdate(context.Background(), update.Record.Ref, in)
	if err != nil || replay.Receipt == nil || replay.Receipt.OutcomeDiscarded || string(replay.Receipt.Outcome) != "default result" {
		t.Fatal("omitted cutoff changed generic result retention", err)
	}
}

func TestReceiptResultPolicyRejectsMalformedBounds(t *testing.T) {
	in := contractInput("invalid-cutoff", true)
	for _, cutoff := range []int64{-1, in.ReceiptUntilMS + 1, 1 << 53} {
		in.ResultUntilMS = cutoff
		if validateInputFields(in) == nil {
			t.Fatal("invalid input result bound accepted", cutoff)
		}
	}
	s, ref, fields := retainedDecodeFixture(t)
	for _, receipt := range []Receipt{{ResultUntilMS: -1}, {ResultUntilMS: 100, ExpiresMS: 99}, {ResultUntilMS: 1 << 53, ExpiresMS: 1 << 53}, {OutcomeDiscarded: true}, {ResultUntilMS: 100, ExpiresMS: 200, OutcomeDiscarded: true, Outcome: []byte("not discarded")}} {
		wire, _ := json.Marshal([]Receipt{receipt})
		fields.receipts = string(wire)
		if _, err := decodeRetainedRecord(s, ref, fields); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("malformed result policy accepted: %+v %v", receipt, err)
		}
	}
}

func TestReceiptResultFreshRedisAcknowledgementAfterCutoff(t *testing.T) {
	s := testStore(t, func(c *Config) { c.Redis.IOTimeout = time.Second })
	readyStore(t, s)
	ctx := context.Background()
	// Warm CAS before the delay hook, which must delay an actual commit reply.
	warm := contractRead(t, s, contractRef(t, s, "warm"), contractInput("warm", true))
	if _, err := s.CompareAndSwap(ctx, warm, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	in := contractInput("delayed", true)
	in.ResultUntilMS = time.Now().Add(100 * time.Millisecond).UnixMilli()
	update := contractRead(t, s, contractRef(t, s, "delayed-result"), in)
	s.client.AddHook(&delayedClockClient{command: "script", after: 200 * time.Millisecond})
	receipt, err := s.CompareAndSwap(ctx, update, Payload{}, []byte("fresh result"))
	if err != nil || time.Now().UnixMilli() < in.ResultUntilMS || receipt.OutcomeDiscarded || string(receipt.Outcome) != "fresh result" {
		t.Fatal("fresh proven reply was discarded", err)
	}
	replay, err := s.ReadUpdate(ctx, update.Record.Ref, in)
	if err != nil {
		t.Fatal(err)
	}
	requireDiscardedReceipt(t, replay.Receipt, receipt)
}

func TestReceiptResultClaimedFixtureReplay(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	for _, proof := range []bool{false, true} {
		ref := group(t, s, fmt.Sprint("claimed-result-", proof))
		input := contractInput(fmt.Sprint("claimed-", proof), true)
		input.Proof = proof
		input.ResultUntilMS = time.Now().Add(-time.Second).UnixMilli()
		begun, err := s.Begin(context.Background(), ref, "fixture", input)
		if err != nil || begun.Step == nil {
			t.Fatal("claimed fixture admission failed", err)
		}
		receipt, err := s.Commit(context.Background(), *begun.Step, Payload{}, []byte("committed refusal"))
		if err != nil || receipt.OutcomeDiscarded {
			t.Fatal("fresh claimed result unavailable", err)
		}
		before, proofBefore := rawReceiptResults(t, s, begun.Step.Record.Ref)
		replay, err := s.Begin(context.Background(), begun.Step.Record.Ref, "fixture", input)
		if err != nil || replay.Step != nil {
			t.Fatal("claimed replay lost marker", err)
		}
		requireDiscardedReceipt(t, replay.Receipt, receipt)
		after, proofAfter := rawReceiptResults(t, s, begun.Step.Record.Ref)
		if before != after || proofBefore != proofAfter {
			t.Fatal("claimed receipt replay mutated storage")
		}
	}
}
