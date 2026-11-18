package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func TestRedisUACDigestCandidateKeepsSecretsTransient(t *testing.T) {
	origin, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:phone@example.test", From: "<sip:caller@example.test>", CallID: "digest-candidate", LocalTag: "local", Branch: "z9hG4bK-original", CSeq: 1, Advertise: "192.0.2.1:5060", Contact: "<sip:caller@192.0.2.1:5060;transport=udp>", Transport: "udp"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := portable.NewTransaction(portable.InviteClient, origin, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1000})
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{7}, 32)
	identity := rediscore.Identity{Context: "context", Incarnation: "incarnation"}
	metadata := &nativepb.Procedure{Credential: &pb.CredentialRef{Id: "carrier", Version: "v1"}, DeadlineMs: 5000}
	for _, status := range []int{401, 407} {
		challenge := digestReply(t, origin, status)
		candidate, handled, e := prepareRedisUACDigest(key, identity, tx, metadata, challenge, time.UnixMilli(1100))
		if e != nil || !handled || candidate == nil || candidate.failure.ACK == nil || candidate.failure.Transaction.Phase != portable.Completed || candidate.work.Attempt != 1 {
			t.Fatal("challenge candidate", status, e)
		}
		credentials := &pb.CredentialResult{Username: "fixture-user", Password: "fixture-password", Version: "v1"}
		retry, e := stateBuildDigestNative(candidate.work, credentials)
		if e != nil || retry.IsZero() || credentials.Username != "" || credentials.Password != "" || credentials.Version != "" {
			t.Fatal("credential was not consumed transiently", status, e)
		}
		facts, e := retry.Facts()
		if e != nil || facts.CSeq != 2 || facts.Method != sip.INVITE {
			t.Fatal("digest retry identity", status, e)
		}
	}
	if tx.Phase != portable.Calling || tx.ACK != nil || metadata.DigestAttempts != 0 || metadata.DigestHandled {
		t.Fatal("candidate mutated retained state")
	}
	missing := proto.Clone(metadata).(*nativepb.Procedure)
	missing.Credential = nil
	if candidate, handled, e := prepareRedisUACDigest(key, identity, tx, missing, digestReply(t, origin, 401), time.UnixMilli(1100)); e != nil || handled || candidate != nil {
		t.Fatal("missing credential started a lookup", e)
	}
}
