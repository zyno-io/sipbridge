package storagecodec

import (
	"bytes"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// DecodeValidatedFrame runs the canonical complete preflight before invoking a
// typed reader. The callback borrows the input body only for this call; published
// results must own their bytes. It supplies no protocol or execution authority.
// This is the qualification boundary for generated direct readers, not a second
// validation profile or a way to skip descriptor validation.
func DecodeValidatedFrame(data []byte, kind byte, descriptor protoreflect.MessageDescriptor, decode func([]byte) error) (err error) {
	started := time.Now()
	defer func() { observe(kind, false, started, len(data), err) }()
	if descriptor == nil || decode == nil || len(data) > MaxBytes || len(data) < len(magic)+1 || !bytes.Equal(data[:len(magic)], magic) || data[len(magic)] != kind {
		return ErrRecord
	}
	body := data[len(magic)+1:]
	remaining := MaxFields
	if err := preflight(body, descriptor, 0, &remaining); err != nil {
		return err
	}
	return decode(body)
}
