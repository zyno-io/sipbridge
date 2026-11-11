package state

import (
	"bytes"
	"encoding/json"
)

// Structured reducer values remain JSON in the enclosing atomic value. Encoding
// an already-JSON snapshot as base64 would multiply every immutable SIP wire a
// second time. Opaque/non-JSON values preserve the original base64 representation.
func structured(data []byte) bool {
	return canonicalStructuredValue(data) != nil
}

// The returned canonical bytes are independently owned by json.Marshal, not data.
func canonicalStructuredValue(data []byte) json.RawMessage {
	if len(data) == 0 || data[0] != '{' && data[0] != '[' {
		return nil
	}
	// Reject large malformed candidates before compaction can allocate an
	// HTML-escaped copy. Small candidates retain the single validation scan.
	if len(data) > 64<<10 && !json.Valid(data) {
		return nil
	}
	// Marshaling RawMessage validates syntax and depth; equality additionally
	// requires canonical bytes, including whitespace and HTML escaping.
	canonical, err := json.Marshal(json.RawMessage(data))
	if err != nil || !bytes.Equal(canonical, data) {
		return nil
	}
	return canonical
}

func encodeValue(data []byte) json.RawMessage {
	if canonical := canonicalStructuredValue(data); canonical != nil {
		return canonical
	}
	encoded, _ := json.Marshal(data)
	return encoded
}

// data must belong to a fresh local decoder. Transfer that ownership for
// structured values; callers must not reuse a prior receiver's buffers here.
func decodeValue(data json.RawMessage) ([]byte, error) {
	if len(data) == 0 || string(data) == "null" {
		return nil, nil
	}
	if data[0] == '{' || data[0] == '[' {
		// A duplicate JSON field may replace a large value using the same local
		// RawMessage allocation. Do not retain disproportionate superseded bytes.
		if cap(data)-len(data) > max(4096, len(data)/4) {
			return append([]byte(nil), data...), nil
		}
		return data, nil
	}
	var value []byte
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}
func (value Effect) MarshalJSON() ([]byte, error) {
	type plain Effect
	return json.Marshal(struct {
		plain
		Payload json.RawMessage `json:"payload,omitempty"`
	}{plain: plain(value), Payload: encodeValue(value.Payload)})
}
func (value *Effect) UnmarshalJSON(data []byte) error {
	type plain Effect
	var decoded struct {
		plain
		Payload json.RawMessage `json:"payload,omitempty"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	payload, err := decodeValue(decoded.Payload)
	if err != nil {
		return err
	}
	*value = Effect(decoded.plain)
	value.Payload = payload
	return nil
}
func (value Payload) MarshalJSON() ([]byte, error) {
	type plain Payload
	return json.Marshal(struct {
		plain
		ProtocolState json.RawMessage `json:"protocolState,omitempty"`
	}{plain: plain(value), ProtocolState: encodeValue(value.ProtocolState)})
}

// Retained records use only the binary codec; JSON methods above are diagnostic.
func marshalCommitPayload(value Payload) ([]byte, error) {
	return value.MarshalBinary()
}

// UnmarshalJSON validates the complete input before replacing the receiver.
// Retained-record reads invoke it directly without an outer JSON unmarshal.
func (value *Payload) UnmarshalJSON(data []byte) error {
	type plain Payload
	var decoded struct {
		plain
		ProtocolState json.RawMessage `json:"protocolState,omitempty"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	protocol, err := decodeValue(decoded.ProtocolState)
	if err != nil {
		return err
	}
	*value = Payload(decoded.plain)
	value.ProtocolState = protocol
	return nil
}
