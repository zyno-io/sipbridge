package engine

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentCredentialReferenceLookupAndDigestRetry(t *testing.T) {
	for _, status := range []int{401, 407} {
		t.Run(reason(uint32(status)), func(t *testing.T) {
			e, peer, connection, ctx, session, writes, count := residentControlFixture(t)
			flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "carrier", SourceIp: "192.0.2.50", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
			credential := &pb.CredentialRef{Id: "carrier-credential", Version: "one"}
			command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "digest-options"}, Sequence: 1,
				Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
				Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:carrier@example.test", Method: "OPTIONS", Transport: "tls", Network: &pb.NetworkTarget{Transport: "tls", Credential: credential}, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
			done := make(chan *pb.OperationResult, 1)
			errors := make(chan error, 2)
			go func() {
				result, err := e.commandResidentClient(ctx, peer, command, "digest-command", time.Now().Add(5*time.Second))
				errors <- err
				done <- result
			}()
			origin := portable.Message{Wire: <-writes, Transport: "tls"}
			headerName := "WWW-Authenticate"
			if status == 407 {
				headerName = "Proxy-Authenticate"
			}
			challenge, err := portable.BuildResponseWithHeaders(origin, status, "Authentication required", nil, "carrier", "", []portable.Header{{Name: headerName, Value: `Digest realm="carrier",nonce="nonce-one",algorithm=MD5,qop="auth"`}})
			if err != nil {
				t.Fatal(err)
			}
			observed, err := Observe(challenge.Wire, false)
			if err != nil {
				t.Fatal(err)
			}
			go func() { _, err := e.completeResidentClient(ctx, flow, observed); errors <- err }()
			lookup := readControllerEnvelope(t, ctx, connection)
			if lookup.GetCredentialLookup().GetCredential().GetId() != credential.Id || lookup.GetCredentialLookup().GetCredential().GetVersion() != credential.Version {
				t.Fatal("credential lookup did not preserve reference")
			}
			const secret = "synthetic-carrier-secret"
			writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: session, CorrelationId: lookup.CorrelationId, Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Username: "phone", Password: secret, Version: credential.Version}}})
			retry := portable.Message{Wire: <-writes, Transport: "tls"}
			parsed, err := retry.Parse()
			if err != nil {
				t.Fatal(err)
			}
			authName := "Authorization"
			if status == 407 {
				authName = "Proxy-Authorization"
			}
			authorization := header(parsed, authName)
			if parsed.CSeq().SeqNo != 2 || !strings.Contains(authorization, `username="phone"`) || bytes.Contains(retry.Wire, []byte(secret)) {
				t.Fatal("invalid or secret-bearing digest retry")
			}
			final, err := portable.BuildResponseWithHeaders(retry, 200, "OK", nil, "carrier", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			finalObserved, err := Observe(final.Wire, false)
			if err != nil {
				t.Fatal(err)
			}
			if handled, err := e.completeResidentClient(ctx, flow, finalObserved); err != nil || !handled {
				t.Fatal("authenticated final not handled", err)
			}
			for range 2 {
				if err := <-errors; err != nil {
					t.Fatal(err)
				}
			}
			result := <-done
			if result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || count.Load() != 2 {
				t.Fatal("digest procedure did not complete exactly once", result)
			}
			if parsed.CSeq().MethodName != sip.OPTIONS {
				t.Fatal("digest changed request method")
			}
		})
	}
}
