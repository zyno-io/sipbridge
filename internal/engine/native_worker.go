package engine

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
)

// nativeWorkerReference is unique to the complete public process lifetime;
// a stable pod instance ID alone must never name the private worker index.
func nativeWorkerReference(process *pb.ProcessRef) (string, error) {
	if !validSendProcess(process) {
		return "", errNativeRecord
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sipbridge.native.worker.v1\x00"))
	var length [binary.MaxVarintLen64]byte
	for _, part := range []string{process.InstanceId, process.Epoch} {
		n := binary.PutUvarint(length[:], uint64(len(part)))
		_, _ = h.Write(length[:n])
		_, _ = h.Write([]byte(part))
	}
	var raw [32]byte
	h.Sum(raw[:0])
	var id uuid.UUID
	copy(id[:], raw[:16])
	id[6] = (id[6] & 0x0f) | 0x80
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String(), nil
}
