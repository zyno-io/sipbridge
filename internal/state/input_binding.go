package state

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"time"
)

// preparedInput is private to one admitted CAS read. The JSON string is the
// exact existing canonical envelope; Memory has no need to retain that string.
// No payload or protocol aggregate is held by the integrity binding.
type preparedInput struct {
	encoded string
	digest  string
	header  inputHeader
	leases  []string
	payload [sha256.Size]byte
}

type inputHeader struct {
	proof, create                             bool
	id, kind, fingerprint                     string
	deadlineMS, receiptUntilMS, resultUntilMS int64
}

func inputHeaders(input Input) inputHeader {
	return inputHeader{input.Proof, input.Create, input.ID, input.Kind, input.Fingerprint, input.DeadlineMS, input.ReceiptUntilMS, input.ResultUntilMS}
}

func ownedInput(input Input) Input {
	input.Payload = bytes.Clone(input.Payload)
	input.ApplicationLeaseIDs = slices.Clone(input.ApplicationLeaseIDs)
	return input
}

func validateInputFields(input Input) error {
	if err := validateApplicationLeaseIDs(input.ApplicationLeaseIDs); err != nil {
		return err
	}
	if !ValidReference(input.ID) || !ValidReference(input.Kind) || !validText(input.Fingerprint, 256) ||
		input.DeadlineMS <= 0 || input.DeadlineMS > 1<<53-1 || input.ReceiptUntilMS < input.DeadlineMS ||
		input.ReceiptUntilMS > 1<<53-1 || input.ReceiptUntilMS-input.DeadlineMS > int64(10*time.Minute/time.Millisecond) ||
		input.ResultUntilMS < 0 || input.ResultUntilMS > input.ReceiptUntilMS {
		return errors.New("invalid SIP input identity/deadline")
	}
	return nil
}

func (s *metadata) prepareInput(input Input, retainEnvelope bool) (*preparedInput, error) {
	if err := validateInputFields(input); err != nil {
		return nil, err
	}
	digest, data, err := digestInput(input)
	if err != nil {
		return nil, err
	}
	if len(data) > s.cfg.Limits.InputBytes {
		return nil, ErrCapacity
	}
	prepared := &preparedInput{digest: digest, header: inputHeaders(input), leases: slices.Clone(input.ApplicationLeaseIDs), payload: sha256.Sum256(input.Payload)}
	if retainEnvelope {
		prepared.encoded = string(data)
	}
	return prepared, nil
}

func (p *preparedInput) matches(input Input) bool {
	return p != nil && p.header == inputHeaders(input) && slices.Equal(p.leases, input.ApplicationLeaseIDs) && p.payload == sha256.Sum256(input.Payload)
}
