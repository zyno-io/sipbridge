package engine

import (
	"errors"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// redisUACDigestCandidate is operation-local. It carries no credentials or
// secret material and is never stored as a queue or a replacement authority.
type redisUACDigestCandidate struct {
	failure nativeInitialInviteFailure
	work    stateDigestChallenge
}

// prepareRedisUACDigest derives the exact native non-2xx transition and the
// immutable inputs for a bounded credential lookup; it performs no I/O.
func prepareRedisUACDigest(key []byte, identity rediscore.Identity, tx portable.Transaction, metadata *nativepb.Procedure, challenge portable.Message, now time.Time) (*redisUACDigestCandidate, bool, error) {
	facts, err := challenge.Facts()
	if err != nil {
		return nil, false, err
	}
	if facts.IsRequest || facts.Status != 401 && facts.Status != 407 {
		return nil, false, nil
	}
	if challenge.WireLen() == 0 || challenge.WireLen() > stateWireLimit {
		return nil, true, errNativeRecord
	}
	if len(key) < 32 || len(key) > 1024 || identity.Context == "" || identity.Incarnation == "" || metadata == nil || metadata.Credential == nil || metadata.Credential.Id == "" || metadata.Credential.Version == "" || metadata.DigestAttempts >= stateDigestRetryLimit || metadata.DeadlineMs <= now.UnixMilli() {
		return nil, false, nil
	}
	if tx.Version != portable.Version || tx.Family != portable.InviteClient || !portable.MatchResponse(tx.Origin, challenge) {
		return nil, true, errors.New("digest challenge transaction mismatch")
	}
	if metadata.DigestHandled {
		return nil, true, nil
	}
	if tx.Phase != portable.Calling && tx.Phase != portable.Proceeding {
		return nil, true, errors.New("digest challenge transaction mismatch")
	}
	header := "WWW-Authenticate"
	if facts.Status == 407 {
		header = "Proxy-Authenticate"
	}
	values, err := challenge.HeaderValues(header)
	if err != nil {
		return nil, true, err
	}
	if len(values) == 0 {
		return nil, false, nil
	}
	failure, err := nativeReceiveInitialInviteFailure(tx, challenge, now.UnixMilli())
	if err != nil {
		return nil, true, err
	}
	if failure.ACK == nil || failure.Replay {
		return nil, true, errNativeRecord
	}
	attempt := metadata.DigestAttempts + 1
	id := nativeIdentity(key, "digest", identity.Incarnation, identity.Context, tx.Key, strconv.FormatUint(uint64(attempt), 10), string(challenge.WireBytes()))
	work := stateDigestChallenge{Origin: tx.Origin, Challenge: challenge, Credential: proto.Clone(metadata.Credential).(*pb.CredentialRef), Cnonce: id[:32], Count: int(attempt), Branch: sip.RFC3261BranchMagicCookie + id[32:], Attempt: attempt}
	return &redisUACDigestCandidate{failure: failure, work: work}, true, nil
}
