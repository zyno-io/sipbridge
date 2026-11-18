package rediscore

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestCallForkStructuralShape measures the real Lua projections for 16
// simultaneously retained fork-shaped records and two additional overlapping
// operation-shaped groups. Payloads are synthetic; this is not a producer
// size proof or a complete call-credit bound.
func TestCallForkStructuralShape(t *testing.T) {
	s, c, id, assignment := structuralBoundFixture(t)
	ctx := context.Background()
	deadline := time.UnixMilli(maxExactInteger)
	if _, err := s.Commit(ctx, Publication{Identity: id, Create: true, Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	before := structuralBytes(t, c, s)
	field := func(kind byte, n int) string {
		suffix := fmt.Sprintf("%c%02d", kind, n)
		return "r:" + strings.Repeat("x", 192-len(suffix)) + suffix
	}
	var all []string
	var payloadBytes int64
	for n := 0; n < 18; n++ {
		dialog, procedure, response, ack, bye, ackMeta, byeMeta := field('d', n), field('p', n), field('r', n), field('a', n), field('b', n), field('m', n), field('n', n)
		fields := []string{dialog, procedure, response, ack, bye, ackMeta, byeMeta}
		all = append(all, fields...)
		changes := []Change{{Field: dialog, Value: bytes.Repeat([]byte{1}, 2048)}, {Field: procedure, Value: bytes.Repeat([]byte{2}, 2048)}, {Field: response, Value: bytes.Repeat([]byte{3}, 65606)}, {Field: ack, Value: bytes.Repeat([]byte{4}, 65606)}, {Field: bye, Value: bytes.Repeat([]byte{5}, 65606)}, {Field: ackMeta, Value: bytes.Repeat([]byte{6}, 8139)}, {Field: byeMeta, Value: bytes.Repeat([]byte{7}, 8139)}}
		for _, change := range changes {
			payloadBytes += int64(len(change.Field) + len(change.Value))
		}
		out := func(which string, message, meta string) OutputIntent {
			return OutputIntent{ID: fmt.Sprintf("%064d%s", n, which), Attempt: strings.Repeat("a", 96), Worker: assignment.Worker, Epoch: assignment.Epoch, Connection: strings.Repeat("c", 96), MessageField: message, MetadataField: meta, Dependencies: []string{dialog, procedure}, WireHash: [32]byte{1}, ProofHash: [32]byte{2}, Deadline: deadline}
		}
		selectors := []SelectorChange{{Selector: Selector{Kind: SelectorDialog, Digest: [32]byte{1, byte(n + 1)}}, Record: dialog}, {Selector: Selector{Kind: SelectorClientTransaction, Digest: [32]byte{2, byte(n + 1)}}, Record: procedure}}
		timers := make([]TimerChange, 4)
		for j := range timers {
			suffix := fmt.Sprintf("%02d%d", n, j)
			timers[j] = TimerChange{Member: strings.Repeat("t", 192-len(suffix)) + suffix, Due: deadline}
		}
		_, err := s.Commit(ctx, Publication{Identity: id, Revision: uint64(n + 1), Deadline: deadline, Changes: changes, NewOutputs: []OutputIntent{out("a", ack, ackMeta), out("b", bye, byeMeta)}, Selectors: selectors, Timers: &TimerMutation{Target: assignment, Expected: func() TimerAssignment {
			if n == 0 {
				return TimerAssignment{}
			}
			return assignment
		}(), Changes: timers}})
		if err != nil {
			t.Fatalf("group %d: %v", n, err)
		}
	}
	after := structuralBytes(t, c, s)
	hlen, err := c.HLen(ctx, s.contextKey(id.Context)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if hlen > 4096 || len(all) > maxFields {
		t.Fatalf("fixture exceeds field limits: retained=%d selected=%d", hlen, len(all))
	}
	view, err := s.Read(ctx, id, all...)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Records) != len(all) {
		t.Fatalf("bounded selected read returned %d fields, want %d", len(view.Records), len(all))
	}
	t.Logf("groups=18 (16 fork-shaped + 2 overlap-shaped) output_intents=36 selected_fields=%d retained_hash_fields=%d logical_delta=%d selected_payload_and_names=%d structural_delta=%d", len(all), hlen, after-before, payloadBytes, after-before-payloadBytes)
}
