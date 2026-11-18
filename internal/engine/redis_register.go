package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"time"
)

// nativeApplicationReference gives every authenticated process one stable private lease key.
func nativeApplicationReference(key []byte, process *pb.ProcessRef) (string, error) {
	if len(key) < 32 || len(key) > 1024 || !validSendProcess(process) {
		return "", errors.New("invalid application identity")
	}
	tuple := []byte("sipbridge.native.application.v1")
	for _, value := range []string{process.InstanceId, process.Epoch} {
		tuple = binary.AppendUvarint(tuple, uint64(len(value)))
		tuple = append(tuple, value...)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(tuple)
	sum := mac.Sum(nil)
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = id[6]&0x0f | 0x80
	id[8] = id[8]&0x3f | 0x80
	return id.String(), nil
}

// redisRegisterReceive supplies values already authenticated by the ingress and
// controller boundaries. Context and RequestID are fresh, non-reused identities;
// the application witness is checked again atomically by the store.
type redisRegisterReceive struct {
	IdentityKey           []byte
	Context               *pb.ContextRef
	Authority             *pb.StateAuthority
	Application, Executor *pb.ProcessRef
	Lease                 rediscore.ApplicationLeaseRef
	Flow                  *pb.FlowRef
	Message               portable.Message
	RequestID, LocalTag   string
	Timer                 rediscore.TimerAssignment
	Deadline              time.Time
}
