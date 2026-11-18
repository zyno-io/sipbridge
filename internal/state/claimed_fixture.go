package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/google/uuid"
)

// The claimed writer is retained for historical compatibility fixtures. Production
// CAS execution uses ReadUpdate/CompareAndSwap and refuses retained Pending work.
func (s *Store) Begin(ctx context.Context, ref Ref, worker string, input Input) (BeginResult, error) {
	if err := s.validateInput(input); err != nil {
		return BeginResult{}, err
	}
	return s.begin(ctx, ref, worker, &input)
}

// Resume claims an existing immutable input. It never admits synthetic work.
func (s *Store) Resume(ctx context.Context, ref Ref, worker string) (BeginResult, error) {
	return s.begin(ctx, ref, worker, nil)
}

func (s *Store) begin(ctx context.Context, ref Ref, worker string, input *Input) (BeginResult, error) {
	if err := s.validateRef(ref); err != nil {
		return BeginResult{}, err
	}
	if !workerPattern.MatchString(worker) {
		return BeginResult{}, errors.New("invalid worker identity")
	}
	observation, err := s.ensurePrimary(ctx)
	if err != nil {
		return BeginResult{}, err
	}
	var digest, encoded, id string
	var deadline int64
	if input != nil {
		var data []byte
		var err error
		digest, data, err = digestInput(*input)
		if err != nil {
			return BeginResult{}, err
		}
		encoded = string(data)
		id = input.ID
		deadline = input.DeadlineMS
	}
	empty, _ := marshalCommitPayload(Payload{})
	claimID := uuid.NewString()
	values, err := s.run(ctx, beginScript, ref, []interface{}{uuid.NewString(), claimID, worker, s.cfg.ClaimTTL.Milliseconds(), string(empty), encoded, digest, id, deadline, 0, s.cfg.Limits.GroupBytes, s.cfg.Limits.InputBytes, s.cfg.Limits.Receipts, base64.StdEncoding.EncodedLen(s.cfg.Limits.OutcomeBytes) + 1024}, true, observation)
	if err != nil {
		return BeginResult{}, err
	}
	if values[0] == "RECEIPT" {
		if len(values) != 2 {
			return BeginResult{}, ErrCorrupt
		}
		var receipt Receipt
		data, err := textAt(values, 1)
		if err != nil || json.Unmarshal([]byte(data), &receipt) != nil {
			return BeginResult{}, ErrCorrupt
		}
		if err := validateReceiptResult(receipt); err != nil {
			return BeginResult{}, err
		}
		return BeginResult{Receipt: &receipt}, nil
	}
	if len(values) != 13 || values[0] != "STEP" {
		return BeginResult{}, ErrCorrupt
	}
	now, err := millisAt(values, 1)
	if err != nil {
		return BeginResult{}, err
	}
	field := make([]string, 10)
	for i := range field {
		field[i], err = textAt(values, i+2)
		if err != nil {
			return BeginResult{}, err
		}
	}
	record, err := s.decodeRecord(ref, field[0], field[1], field[2], field[3], field[4], field[8], field[9])
	if err != nil {
		return BeginResult{}, err
	}
	projectReceiptResults(&record, now)
	if record.Pending == nil {
		return BeginResult{}, ErrCorrupt
	}
	if err := validateApplicationLeaseIDs(record.Pending.ApplicationLeaseIDs); err != nil {
		return BeginResult{}, ErrCorrupt
	}
	leases, err := textAt(values, 12)
	if err != nil {
		return BeginResult{}, err
	}
	if err := decodeApplicationLeases(&record, leases, now, record.Pending.ApplicationLeaseIDs); err != nil {
		return BeginResult{}, err
	}
	pendingDigest, _, err := digestInput(*record.Pending)
	if err != nil || pendingDigest != field[5] {
		return BeginResult{}, ErrCorrupt
	}
	until, err := strconv.ParseInt(field[7], 10, 64)
	if err != nil {
		return BeginResult{}, ErrCorrupt
	}
	claim := Claim{ID: claimID, Token: field[6], Worker: worker, BaseRevision: record.Revision, UntilMS: until, InputDigest: pendingDigest, Ref: record.Ref, Authority: s.authority}
	claim.transport = observation
	claim.applicationEffects = snapshotApplicationEffects(record.Payload.Effects)
	return BeginResult{Step: &Step{Record: record, Input: *record.Pending, Claim: claim, NowMS: now, Recovered: input == nil || record.Pending.ID != input.ID}}, nil
}

func (s *Store) Commit(ctx context.Context, step Step, next Payload, outcome []byte) (Receipt, error) {
	return s.commit(ctx, step, next, outcome, commitScript, nil)
}

// RefreshClaim reloads only after recovery changed owner observations. A
// concurrent winner or transport switch cannot transplant this computation.
func (s *Store) RefreshClaim(ctx context.Context, step Step) (Step, error) {
	if step.Claim.transport != s.transport.Load() {
		return Step{}, ErrStale
	}
	record, now, err := s.read(ctx, step.Record.Ref)
	if err != nil {
		return Step{}, err
	}
	if record.Authority != step.Record.Authority || record.Revision != step.Record.Revision || record.Pending == nil {
		return Step{}, ErrStale
	}
	digest, _, err := digestInput(*record.Pending)
	if err != nil || digest != step.Claim.InputDigest || step.Claim.transport != s.transport.Load() {
		return Step{}, ErrStale
	}
	step.Record, step.NowMS = record, now
	return step, nil
}
