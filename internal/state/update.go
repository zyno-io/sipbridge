package state

import (
	"context"
	"strconv"

	"github.com/google/uuid"
)

// Update is transient computation input, never a retained claim or pending input.
// Its private observations bind a subsequent CAS to this exact validated read.
type Update struct {
	Record Record
	Input  Input
	NowMS  int64

	// token is opaque to the engine and owned by the issuing adapter.
	token any
}

// redisUpdate binds CAS to the exact validated Redis read/connection.
type redisUpdate struct {
	counter      string
	input        *preparedInput
	transport    *primaryObservation
	priorEffects map[string]applicationLeaseEffect
}

type UpdateResult struct {
	Update  *Update
	Receipt *Receipt
	// Ref pins an observed existing incarnation, including a busy legacy input.
	// An absent read's uncommitted creation proposal does not set it.
	Ref *Ref
}

// ReadUpdate never allocates a group. A retained legacy pending input is refused;
// CAS cannot overwrite it or steal its claim. Only historical fixtures resume it.
func (s *Store) ReadUpdate(ctx context.Context, ref Ref, input Input) (UpdateResult, error) {
	if err := s.validateRef(ref); err != nil {
		return UpdateResult{}, err
	}
	prepared, err := s.prepareInput(input, true)
	if err != nil {
		return UpdateResult{}, err
	}
	input = ownedInput(input)
	observation, err := s.ensurePrimary(ctx)
	if err != nil {
		return UpdateResult{}, err
	}
	values, err := s.run(ctx, readUpdateScript, ref, []interface{}{prepared.encoded}, false, observation)
	if err != nil {
		return UpdateResult{}, err
	}
	now, err := millisAt(values, 1)
	if err != nil {
		return UpdateResult{}, err
	}
	record := Record{Ref: ref, Authority: s.authority, Versions: s.cfg.Versions, Revision: "0"}
	counter := "0"
	var leases string
	existing := false
	switch values[0] {
	case "ABSENT":
		if len(values) != 3 {
			return UpdateResult{}, ErrCorrupt
		}
		if !input.Create || ref.Incarnation != "" || ref.CallID == "" {
			return UpdateResult{}, ErrNotFound
		}
		record.Ref.Incarnation = uuid.NewString()
		leases, err = textAt(values, 2)
	case "RECORD":
		existing = true
		if len(values) != 11 {
			return UpdateResult{}, ErrCorrupt
		}
		fields := make([]string, 9)
		for n := range fields {
			fields[n], err = textAt(values, n+2)
			if err != nil {
				return UpdateResult{}, err
			}
		}
		record, err = s.decodeRecord(ref, fields[0], fields[1], fields[2], fields[3], fields[8], fields[4], fields[5])
		counter, leases = fields[6], fields[7]
	default:
		return UpdateResult{}, ErrCorrupt
	}
	if err != nil || !decimal(counter) {
		return UpdateResult{}, ErrCorrupt
	}
	sequence, sequenceErr := strconv.ParseUint(counter, 10, 63)
	revision, revisionErr := strconv.ParseUint(record.Revision, 10, 63)
	if sequenceErr != nil || revisionErr != nil || sequence < revision {
		return UpdateResult{}, ErrCorrupt
	}
	if err := decodeApplicationLeases(&record, leases, now, input.ApplicationLeaseIDs); err != nil {
		return UpdateResult{}, err
	}
	for _, receipt := range record.Receipts {
		if receipt.OperationID == input.ID && receipt.ExpiresMS > now {
			if receipt.Fingerprint != input.Fingerprint {
				return UpdateResult{}, ErrConflict
			}
			return UpdateResult{Receipt: &receipt}, nil
		}
	}
	if proof := record.Proof; proof != nil && proof.OperationID == input.ID && proof.ExpiresMS > now {
		if proof.Fingerprint != input.Fingerprint {
			return UpdateResult{}, ErrConflict
		}
		return UpdateResult{Receipt: proof}, nil
	}
	if input.DeadlineMS <= now {
		return UpdateResult{}, ErrExpired
	}
	if record.Pending != nil {
		observed := record.Ref
		return UpdateResult{Ref: &observed}, ErrBusy
	}
	update := &Update{Record: record, Input: input, NowMS: now, token: &redisUpdate{counter: counter, input: prepared,
		transport: observation, priorEffects: snapshotApplicationEffects(record.Payload.Effects)}}
	result := UpdateResult{Update: update}
	if existing {
		observed := record.Ref
		result.Ref = &observed
	}
	return result, nil
}

func (s *Store) CompareAndSwap(ctx context.Context, update Update, next Payload, outcome []byte) (Receipt, error) {
	token, ok := update.token.(*redisUpdate)
	if !ok || token.transport == nil || token.transport != s.transport.Load() {
		return Receipt{}, ErrStale // No mutation has been attempted.
	}
	counter, err := strconv.ParseUint(token.counter, 10, 63)
	if err != nil || counter == 1<<63-1 {
		return Receipt{}, ErrCapacity
	}
	if !token.input.matches(update.Input) {
		return Receipt{}, ErrConflict
	}
	if update.Record.Pending != nil {
		return Receipt{}, ErrBusy
	}
	step := Step{Record: update.Record, Input: update.Input, NowMS: update.NowMS, Claim: Claim{
		Authority: s.authority, Ref: update.Record.Ref, BaseRevision: update.Record.Revision,
		Token: strconv.FormatUint(counter+1, 10), InputDigest: token.input.digest, transport: token.transport, preparedInput: token.input,
		applicationEffects: token.priorEffects,
	}}
	if step.Record.Authority != s.authority {
		return Receipt{}, ErrAuthority
	}
	return s.commit(ctx, step, next, outcome, compareAndSwapScript, []interface{}{token.input.encoded, token.counter})
}
