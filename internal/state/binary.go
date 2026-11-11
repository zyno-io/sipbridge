package state

import (
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

func (value Payload) MarshalBinary() ([]byte, error) {
	return storagecodec.Marshal(storagecodec.Payload, storageEncodeStatePayload(value))
}

// UnmarshalBinary publishes an independently owned payload only after the entire
// typed record passes wire validation. Store-specific bounds follow at its caller.
func (value *Payload) UnmarshalBinary(data []byte) error {
	decoded := new(storagepb.StatePayload)
	if err := storagecodec.Unmarshal(data, storagecodec.Payload, decoded); err != nil {
		return ErrCorrupt
	}
	next := storageDecodeStatePayload(decoded)
	*value = next
	return nil
}
