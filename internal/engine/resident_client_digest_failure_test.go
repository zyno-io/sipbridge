package engine

import (
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestResidentDigestRejectsInvalidOrStaleLookup(t *testing.T) {
	for _, scenario := range []string{"wrong-version", "timeout", "policy-changed"} {
		t.Run(scenario, func(t *testing.T) {
			e, peer, connection, ctx, session, writes, count := residentControlFixture(t)
			flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "carrier", SourceIp: "192.0.2.50", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
			credential := &pb.CredentialRef{Id: "carrier", Version: "one"}
			command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "digest-failure"}, Sequence: 1,
				Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
				Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:carrier@example.test", Method: "OPTIONS", Transport: "tls", Network: &pb.NetworkTarget{Transport: "tls", Credential: credential}, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
			results := make(chan *pb.OperationResult, 1)
			go func() {
				result, _ := e.commandResidentClient(ctx, peer, command, "digest-failure", time.Now().Add(5*time.Second))
				results <- result
			}()
			origin := portable.Message{Wire: <-writes, Transport: "tls"}
			challenge, err := portable.BuildResponseWithHeaders(origin, 401, "Unauthorized", nil, "carrier", "", []portable.Header{{Name: "WWW-Authenticate", Value: `Digest realm="carrier",nonce="nonce",algorithm=MD5,qop="auth"`}})
			if err != nil {
				t.Fatal(err)
			}
			observed, err := Observe(challenge.Wire, false)
			if err != nil {
				t.Fatal(err)
			}
			lookupCtx := ctx
			cancel := func() {}
			if scenario == "timeout" {
				lookupCtx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
			}
			defer cancel()
			completion := make(chan error, 1)
			go func() { _, err := e.completeResidentClient(lookupCtx, flow, observed); completion <- err }()
			lookup := readControllerEnvelope(t, ctx, connection)
			if lookup.GetCredentialLookup() == nil {
				t.Fatal("credential lookup missing")
			}
			if scenario == "wrong-version" {
				duplicateCtx, duplicateCancel := context.WithTimeout(ctx, 200*time.Millisecond)
				handled, duplicateErr := e.completeResidentClient(duplicateCtx, flow, observed)
				duplicateCancel()
				if duplicateErr != nil || !handled {
					t.Fatal("duplicate challenge started another lookup", duplicateErr)
				}
			}
			if scenario == "policy-changed" {
				e.mu.Lock()
				c := e.contexts[command.Context.Id]
				e.mu.Unlock()
				c.mu.Lock()
				c.snapshot.DirectoryEpoch = "changed"
				c.mu.Unlock()
			}
			if scenario != "timeout" {
				version := "one"
				if scenario == "wrong-version" {
					version = "other"
				}
				writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: session, CorrelationId: lookup.CorrelationId, Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Username: "phone", Password: "synthetic-test-secret", Version: version}}})
			}
			err = <-completion
			if scenario == "policy-changed" && err == nil {
				t.Fatal("stale policy accepted")
			}
			if scenario != "policy-changed" && err != nil {
				t.Fatal("lookup failure not recorded", err)
			}
			result := <-results
			certainty := pb.OperationResult_COMPLETED
			if scenario == "policy-changed" {
				certainty = pb.OperationResult_UNKNOWN
			}
			if result == nil || result.Certainty != certainty || count.Load() != 1 {
				t.Fatal("invalid lookup caused retry or wrong outcome", result)
			}
		})
	}
}

func TestResidentInviteDigestACKPrecedesAuthenticatedRetry(t *testing.T) {
	e, peer, connection, ctx, session, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "carrier", SourceIp: "192.0.2.50", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "digest-invite"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:carrier@example.test", Transport: "tls", Network: &pb.NetworkTarget{Transport: "tls", Credential: &pb.CredentialRef{Id: "carrier", Version: "one"}}, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	result, err := e.commandResidentClient(ctx, peer, command, "digest-invite", time.Now().Add(5*time.Second))
	if err != nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE not accepted", err)
	}
	origin := portable.Message{Wire: <-writes, Transport: "tls"}
	challenge, err := portable.BuildResponseWithHeaders(origin, 401, "Unauthorized", nil, "carrier", "", []portable.Header{{Name: "WWW-Authenticate", Value: `Digest realm="carrier",nonce="nonce",algorithm=MD5,qop="auth"`}})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(challenge.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	completion := make(chan error, 1)
	go func() { _, err := e.completeResidentClient(ctx, flow, observed); completion <- err }()
	ack := portable.Message{Wire: <-writes, Transport: "tls"}
	parsed, err := ack.Parse()
	if err != nil || parsed.CSeq().MethodName != sip.ACK {
		t.Fatal("challenge ACK missing", err)
	}
	lookup := readControllerEnvelope(t, ctx, connection)
	if lookup.GetCredentialLookup() == nil {
		t.Fatal("credential lookup missing")
	}
	writeControllerEnvelope(t, ctx, connection, &pb.Envelope{SessionId: session, CorrelationId: lookup.CorrelationId, Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Username: "phone", Password: "synthetic-test-secret", Version: "one"}}})
	retry := portable.Message{Wire: <-writes, Transport: "tls"}
	parsed, err = retry.Parse()
	if err != nil || parsed.CSeq().MethodName != sip.INVITE || parsed.CSeq().SeqNo != 2 || header(parsed, "Authorization") == "" {
		t.Fatal("authenticated INVITE missing", err)
	}
	if err = <-completion; err != nil {
		t.Fatal(err)
	}
}
