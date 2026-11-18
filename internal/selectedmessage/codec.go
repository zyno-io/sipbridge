// Package selectedmessage provides the operation-local selected wire record.
// Only an already admitted message can be persisted by a trusted writer.
package selectedmessage

import (
	"errors"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

const maxFieldBytes = storagecodec.MaxSelectedMessageFieldBytes
const maxRecordBytes = storagecodec.MaxSelectedMessageBytes

var ErrRecord = errors.New("invalid selected message")

// Marshal retains exact wire and transport metadata. It never parses SIP.
func Marshal(m portable.Message) ([]byte, error) {
	if m.WireLen() == 0 || m.WireLen() > maxFieldBytes || len(m.Transport()) > maxFieldBytes || len(m.Source()) > maxFieldBytes || len(m.Destination()) > maxFieldBytes {
		return nil, ErrRecord
	}
	p := &storagepb.PortableMessage{Wire: m.WireBytes(), Transport: m.Transport(), Source: m.Source(), Destination: m.Destination()}
	return storagecodec.Marshal(storagecodec.SelectedMessage, p)
}

// DecodeOpaque validates the private record and restores an owned lazy holder.
// Validation of SIP semantics occurs only when the holder is consumed.
func DecodeOpaque(data []byte) (portable.Message, error) {
	p, err := decodeRecord(data)
	if err != nil {
		return portable.Message{}, err
	}
	return portable.RestoreMessage(p.Wire, p.Transport, p.Source, p.Destination), nil
}

// DecodeWire returns the protobuf decoder's independently owned wire directly.
// A physical writer needs no immutable SIP holder or second full-packet copy.
// It applies exactly the same private framing and metadata checks as native
// restoration, without parsing SIP.
func DecodeWire(data []byte) ([]byte, error) {
	p, err := decodeRecord(data)
	if err != nil {
		return nil, err
	}
	return p.Wire, nil
}

func decodeRecord(data []byte) (*storagepb.PortableMessage, error) {
	if len(data) > maxRecordBytes {
		return nil, ErrRecord
	}
	p := new(storagepb.PortableMessage)
	if err := storagecodec.Unmarshal(data, storagecodec.SelectedMessage, p); err != nil {
		return nil, err
	}
	if len(p.Wire) == 0 || len(p.Wire) > maxFieldBytes || len(p.Transport) > maxFieldBytes || len(p.Source) > maxFieldBytes || len(p.Destination) > maxFieldBytes {
		return nil, ErrRecord
	}
	return p, nil
}

// ExactWire returns independently owned bytes for a physical writer, without
// triggering native or observed SIP parsing.
func ExactWire(m portable.Message) []byte { return m.WireBytes() }

// Provenance identifies the caller's already checked validation authority.
// Different provenance never shares a holder even when every byte is equal.
type Provenance string

type entry struct {
	message    portable.Message
	provenance Provenance
}

// Memo is local to one operation and must not be shared between operations.
type Memo struct {
	limit   int
	entries []entry
}

func NewMemo(limit int) (*Memo, error) {
	if limit < 1 || limit > 4096 {
		return nil, ErrRecord
	}
	return &Memo{limit: limit}, nil
}

// Adopt reuses an already owned immutable holder when identity and provenance
// both match. It does not parse or copy a fresh admitted holder.
func (m *Memo) Adopt(message portable.Message, provenance Provenance) (portable.Message, error) {
	if m == nil || provenance == "" || len(provenance) > maxFieldBytes || message.WireLen() == 0 || message.WireLen() > maxFieldBytes {
		return portable.Message{}, ErrRecord
	}
	for _, e := range m.entries {
		if e.provenance == provenance && e.message.Transport() == message.Transport() && e.message.Source() == message.Source() && e.message.Destination() == message.Destination() && e.message.EqualWire(message) {
			return e.message, nil
		}
	}
	if len(m.entries) >= m.limit {
		return portable.Message{}, ErrRecord
	}
	m.entries = append(m.entries, entry{message: message, provenance: provenance})
	return message, nil
}
func (m *Memo) Decode(data []byte, provenance Provenance) (portable.Message, error) {
	message, err := DecodeOpaque(data)
	if err != nil {
		return portable.Message{}, err
	}
	return m.Adopt(message, provenance)
}
