package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/icholy/digest"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
	"sipbridge/internal/state"
)

func digestFixture(t *testing.T, status int, methods ...sip.RequestMethod) (*stateReduction, *stateContext, *stateTransaction, state.Effect, stateDigestChallenge) {
	t.Helper()
	r, c, tx := forkFixture(t)
	r.group = stateGroup{Version: 1, Contexts: map[string]*stateContext{c.Snapshot.Context.Id: c}, Selectors: make(map[string]string)}
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	r.in.Operation = "challenge"
	c.Credential = &pb.CredentialRef{Id: "credential-ref", Version: "version-1"}
	c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
	// Full URI parameters and query headers must survive digest computation.
	parsed, _ := tx.Native.Origin.Parse()
	request := parsed.(*sip.Request)
	request.Recipient.UriParams.Add("user", "phone")
	request.Recipient.Headers.Add("Subject", "test")
	if len(methods) != 0 {
		request.Method = methods[0]
		request.CSeq().MethodName = methods[0]
		c.Snapshot.Kind = pb.ContextSnapshot_TRANSACTION
	}
	tx.Native.Origin.Wire = []byte(request.String())
	c.Origin = tx.Native.Origin
	if len(methods) != 0 {
		old := tx.Native.Key
		var err error
		tx.Native, err = portable.NewTransaction(portable.NonInviteClient, c.Origin, portable.DefaultTimers(), "")
		if err != nil {
			t.Fatal(err)
		}
		delete(c.Transactions, old)
		c.Transactions[tx.Native.Key] = tx
	}
	challenge := digestReply(t, tx.Native.Origin, status)
	last := challenge
	tx.Native.LastResponse = &last
	handled, err := r.digestChallenge(c, tx, challenge)
	if err != nil || !handled {
		t.Fatalf("challenge admission: %v, %v", handled, err)
	}
	for _, effect := range r.payload.Effects {
		if effect.Kind == "credential" {
			var body stateEngineEffect
			var saved stateDigestChallenge
			if json.Unmarshal(effect.Payload, &body) != nil || json.Unmarshal(body.Wire, &saved) != nil {
				t.Fatal("challenge metadata not retained")
			}
			return r, c, tx, effect, saved
		}
	}
	t.Fatal("credential intent missing")
	return nil, nil, nil, state.Effect{}, stateDigestChallenge{}
}

func digestReply(t *testing.T, origin portable.Message, status int) portable.Message {
	t.Helper()
	name := "WWW-Authenticate"
	if status == 407 {
		name = "Proxy-Authenticate"
	}
	message, err := portable.BuildResponseWithHeaders(origin, status, "Authentication required", nil, "challenge-tag", "", []portable.Header{{Name: name, Value: `Digest realm="carrier.fixture", nonce="fixed-nonce", algorithm=MD5, qop="auth"`}})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func digestDerived(t *testing.T, challenge stateDigestChallenge) *pb.SipMessage {
	t.Helper()
	credentials := &pb.CredentialResult{Username: "carrier-user", Password: "fixture-only-secret", Version: challenge.Credential.Version}
	message, err := stateBuildDigestResult(challenge, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.Username != "" || credentials.Password != "" || credentials.Version != "" || credentials.Error != "" {
		t.Fatal("transient credential result retained")
	}
	return message
}

func TestStateDigestRetryRetainsOnlyDerivedNativeIdentity(t *testing.T) {
	for _, status := range []int{401, 407} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			r, c, original, effect, challenge := digestFixture(t, status)
			before := len(r.payload.Effects)
			if handled, err := r.digestChallenge(c, original, challenge.Challenge); err != nil || !handled || len(r.payload.Effects) != before {
				t.Fatal("duplicate challenge allocated another effect")
			}
			derived := digestDerived(t, challenge)
			if repeated := digestDerived(t, challenge); !bytes.Equal(derived.Wire, repeated.Wire) {
				t.Fatal("pure credential replay changed derived wire")
			}
			parsed, _ := sip.ParseMessage(derived.Wire)
			request := parsed.(*sip.Request)
			auth := "Authorization"
			if status == 407 {
				auth = "Proxy-Authorization"
			}
			credentials, err := digest.ParseCredentials(request.GetHeader(auth).Value())
			if err != nil || credentials.URI != "sip:phone@example.test;user=phone?Subject=test" || credentials.Cnonce != challenge.Cnonce || credentials.Nc != challenge.Count {
				t.Fatalf("full digest target/identity lost: %+v, %v", credentials, err)
			}
			parameters, err := digest.ParseChallenge(`Digest realm="carrier.fixture", nonce="fixed-nonce", algorithm=MD5, qop="auth"`)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := digest.Digest(parameters, digest.Options{Method: "INVITE", URI: credentials.URI, Username: "carrier-user", Password: "fixture-only-secret", Cnonce: challenge.Cnonce, Count: challenge.Count})
			if err != nil || credentials.Response != expected.Response {
				t.Fatal("carrier cannot verify derived digest")
			}
			r.in = stateEngineInput{Kind: "digest-result", Effect: effect.ID, Executor: &pb.ProcessRef{InstanceId: "sip-takeover", Epoch: "new-epoch"}, Message: derived, Operation: "derived"}
			if err := r.digestResult(); err != nil {
				t.Fatal(err)
			}
			if len(c.Transactions) != 2 || c.Snapshot.RemoteTag != "" || c.Snapshot.Ended || c.Snapshot.Answered {
				t.Fatal("retry changed dialog lifecycle")
			}
			var retry *stateTransaction
			for _, transaction := range c.Transactions {
				if transaction != original {
					retry = transaction
				}
			}
			if retry == nil || retry.DigestAttempts != 1 || retry.Native.Phase != portable.Calling || retry.CompletionScope != "" {
				t.Fatal("retry did not start independently")
			}
			if err := r.digestResult(); err != nil || len(c.Transactions) != 2 {
				t.Fatalf("derived completion replay reran native start: %v", err)
			}
			encoded, err := json.Marshal(r.group)
			if err != nil || bytes.Contains(encoded, []byte("fixture-only-secret")) {
				t.Fatal("plaintext credential entered native state")
			}
			pending, _ := json.Marshal(r.in)
			if bytes.Contains(pending, []byte("fixture-only-secret")) || bytes.Contains(pending, []byte("credentialResult")) {
				t.Fatal("plaintext credential entered pending input")
			}
			// The final accepted tag may differ from the challenge's tag.
			accepted := forkReply(t, retry, 200, "accepted-tag")
			if err := r.receivedResponse(c, retry, accepted); err != nil || c.Snapshot.RemoteTag != "accepted-tag" || !c.Snapshot.Answered {
				t.Fatalf("authenticated retry final identity: %v", err)
			}
		})
	}
}

func TestStateDigestStandaloneTransactionRetry(t *testing.T) {
	for _, status := range []int{401, 407} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			r, c, _, effect, challenge := digestFixture(t, status, sip.OPTIONS)
			r.in = stateEngineInput{Effect: effect.ID, Executor: r.engine.cfg.Process, Message: digestDerived(t, challenge)}
			if err := r.digestResult(); err != nil {
				t.Fatal(err)
			}
			var retry *stateTransaction
			for _, tx := range c.Transactions {
				if tx.DigestAttempts == 1 {
					retry = tx
				}
			}
			if retry == nil || retry.Native.Family != portable.NonInviteClient || retry.Native.Phase != portable.Trying || c.Snapshot.Kind != pb.ContextSnapshot_TRANSACTION {
				t.Fatal("standalone digest retry created a dialog")
			}
		})
	}
}

func TestStateDigestRejectsChangedCredentialAndRetryIdentity(t *testing.T) {
	for _, field := range []string{"version", "scope", "branch", "cseq", "uri", "body", "auth", "cnonce", "nonce", "conflict"} {
		t.Run(field, func(t *testing.T) {
			r, c, _, effect, challenge := digestFixture(t, 407)
			if field == "version" {
				credentials := &pb.CredentialResult{Username: "user", Password: "fixture-only-secret", Version: "new-version"}
				if _, err := stateBuildDigestResult(challenge, credentials); err == nil || credentials.Password != "" {
					t.Fatal("changed credential version accepted or retained")
				}
				return
			}
			derived := digestDerived(t, challenge)
			parsed, _ := sip.ParseMessage(derived.Wire)
			request := parsed.(*sip.Request)
			switch field {
			case "scope":
				c.Credential.Version = "foreign-version"
			case "branch":
				request.Via().Params.Add("branch", "z9hG4bK-other")
			case "cseq":
				request.CSeq().SeqNo++
			case "uri":
				request.Recipient.Host = "other.example.test"
			case "body":
				request.SetBody([]byte("changed"))
			case "auth":
				request.RemoveHeader("Proxy-Authorization")
			case "cnonce", "nonce":
				credentials, err := digest.ParseCredentials(request.GetHeader("Proxy-Authorization").Value())
				if err != nil {
					t.Fatal(err)
				}
				if field == "cnonce" {
					credentials.Cnonce = "uncommitted"
				} else {
					credentials.Nonce = "other-challenge"
				}
				request.RemoveHeader("Proxy-Authorization")
				request.AppendHeader(sip.NewHeader("Proxy-Authorization", credentials.String()))
			}
			if field != "scope" && field != "conflict" {
				derived.Wire = []byte(request.String())
			}
			r.in = stateEngineInput{Effect: effect.ID, Executor: r.engine.cfg.Process, Message: derived}
			if field == "conflict" {
				if err := r.digestResult(); err != nil {
					t.Fatal(err)
				}
				r.in.Message = proto.Clone(derived).(*pb.SipMessage)
				r.in.Message.Wire = append(r.in.Message.Wire, '\r', '\n')
			}
			if err := r.digestResult(); err == nil {
				t.Fatal("conflicting digest completion accepted")
			}
		})
	}
}

func TestStateDigestBoundedFailureAndCancellation(t *testing.T) {
	for _, outcome := range []string{"expired", "credential-missing", "cancelled", "retry-limit"} {
		t.Run(outcome, func(t *testing.T) {
			r, c, tx, effect, challenge := digestFixture(t, 401)
			r.in = stateEngineInput{Effect: effect.ID, Executor: r.engine.cfg.Process, Message: digestDerived(t, challenge)}
			switch outcome {
			case "expired":
				r.now = effect.DeadlineMS
			case "credential-missing":
				r.in.Message = nil
				r.in.Failure = "credential executor error including fixture-only-secret"
			case "cancelled":
				tx.Native.Cancelled = true
			case "retry-limit":
				tx.DigestEffect = ""
				tx.DigestAttempts = stateDigestRetryLimit
				if handled, err := r.digestChallenge(c, tx, challenge.Challenge); err != nil || handled {
					t.Fatal("unbounded setup retries")
				}
				return
			}
			before := len(r.payload.Effects)
			if err := r.digestResult(); err != nil || len(c.Transactions) != 1 {
				t.Fatalf("failed/cancelled retry advanced native transaction: %v", err)
			}
			for _, next := range r.payload.Effects[before:] {
				if next.Kind == "wire" || bytes.Contains(next.Payload, []byte("fixture-only-secret")) {
					t.Fatal("failed digest created output or retained executor error")
				}
			}
			if outcome != "cancelled" && (!c.Snapshot.Ended || c.Result == nil || strings.Contains(c.Result.Error, "fixture-only-secret")) {
				t.Fatal("failure did not end setup with sanitized result")
			}
		})
	}
}

func TestStateDigestCrossReplicaRedisCommitAndReplay(t *testing.T) {
	testStateDigestCrossReplicaRedisCommitAndReplay(t, false)
}

func TestStateCASDigestCrossReplicaRedisCommitAndReplay(t *testing.T) {
	testStateDigestCrossReplicaRedisCommitAndReplay(t, true)
}

func testStateDigestCrossReplicaRedisCommitAndReplay(t *testing.T, cas bool) {
	t.Helper()
	a, b := historicalStateTestEngine(t)
	if cas {
		for _, worker := range []*StateEngine{a, b} {
			var err error
			worker.runtime, err = execution.NewCAS(worker.store, execution.ReducerFunc(worker.reduce), 8)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	ctx := context.Background()
	ref, err := a.Locate(ctx, stateTestRequest("INVITE", 1, "", "digest-group"))
	if err != nil {
		t.Fatal(err)
	}
	application := &pb.ProcessRef{InstanceId: "controller", Epoch: "controller-epoch"}
	open := &pb.OpenUac{Uri: "sip:called@carrier.example.test;user=phone?Subject=test", Network: &pb.NetworkTarget{Transport: "tls", Credential: &pb.CredentialRef{Id: "credential-ref", Version: "version-1"}}}
	result := stateTestApply(t, a, ref, "open-digest", stateEngineInput{Kind: "command", Application: application, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_OpenUac{OpenUac: open}}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, _ := stateTestLoad(t, b, ref)
	var openEffect state.Effect
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "open" {
			openEffect = effect
		}
	}
	if openEffect.ID == "" {
		t.Fatal("network-open effect missing")
	}
	stateTestApply(t, a, ref, "start-open", stateEngineInput{Kind: "effect-start", Effect: openEffect.ID})
	flow := &pb.FlowRef{Ingress: a.cfg.Process, ConnectionId: "carrier-flow", SourceIp: "192.0.2.20", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	result = stateTestApply(t, b, ref, "opened", stateEngineInput{Kind: "effect-result", Effect: openEffect.ID, Executor: a.cfg.Process, Flow: flow})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	_, group := stateTestLoad(t, b, ref)
	c := group.Contexts[ref.Id]
	challenge := digestReply(t, c.Origin, 407)
	result = stateTestApply(t, b, ref, "challenge", stateEngineInput{Kind: "ingest", Application: application, Ingest: &pb.OwnerIngest{Context: ref, Application: application, Flow: flow, Wire: challenge.Wire, Admission: pb.OwnerIngest_EXISTING_FLOW}})
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	record, group = stateTestLoad(t, a, ref)
	var selected state.Effect
	var saved stateDigestChallenge
	for _, effect := range record.Payload.Effects {
		if effect.Kind == "credential" {
			selected = effect
			var body stateEngineEffect
			if json.Unmarshal(effect.Payload, &body) != nil || json.Unmarshal(body.Wire, &saved) != nil {
				t.Fatal("retained challenge corrupt")
			}
		}
	}
	if selected.ID == "" {
		t.Fatal("digest challenge did not produce credential effect")
	}
	if !cas {
		stateTestApply(t, a, ref, "start-lookup", stateEngineInput{Kind: "effect-start", Effect: selected.ID})
	} else if selected.Status != state.EffectPending {
		t.Fatal("CAS pure lookup unexpectedly required a start record")
	}
	derived := digestDerived(t, saved)
	input := stateEngineInput{Kind: "digest-result", Context: ref, Operation: "derived", Effect: selected.ID, Message: derived}
	logical, err := a.group(ref)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	computeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resultDeadline := time.UnixMilli(selected.DeadlineMS).Add(2 * time.Minute)
	for _, worker := range []*StateEngine{a, b} {
		go func(engine *StateEngine) {
			outcome, err := engine.apply(computeCtx, logical, "derived", input, resultDeadline, false)
			// CAS retains native procedure identity, not generic internal receipts.
			// A collected completed effect may refuse a late duplicate; it must
			// never allocate a second authenticated transaction.
			collected := cas && outcome != nil && outcome.Certainty == pb.OperationResult_NOT_ACCEPTED && outcome.Error == "effect not found"
			if err == nil && outcome.Error != "" && !collected {
				err = fmt.Errorf("digest commit: %s", outcome.Error)
			}
			results <- err
		}(worker)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	after, afterGroup := stateTestLoad(t, a, ref)
	if len(afterGroup.Contexts[ref.Id].Transactions) != len(group.Contexts[ref.Id].Transactions)+1 {
		t.Fatal("takeover did not commit one retry")
	}
	result = stateTestApply(t, a, ref, "derived", input)
	collected := cas && result.Certainty == pb.OperationResult_NOT_ACCEPTED && result.Error == "effect not found"
	if result.Error != "" && !collected {
		t.Fatal(result.Error)
	}
	replayed, replayedGroup := stateTestLoad(t, b, ref)
	if (!cas && replayed.Revision != after.Revision) || len(replayed.Payload.Effects) != len(after.Payload.Effects) ||
		len(replayedGroup.Contexts[ref.Id].Transactions) != len(afterGroup.Contexts[ref.Id].Transactions) {
		t.Fatal("lost reply recovery reran digest transaction")
	}
	if cas && (replayed.Pending != nil || replayed.Proof != nil) {
		t.Fatal("CAS retained a generic pure computation record")
	}
	encoded, _ := json.Marshal(after)
	if bytes.Contains(encoded, []byte("fixture-only-secret")) || bytes.Contains(encoded, []byte("credentialResult")) {
		t.Fatal("plaintext credential retained in Redis")
	}
	// Both workers must see the same immutable retry identity after a new load.
	_, finalGroup := stateTestLoad(t, b, ref)
	if !bytes.Equal(finalGroup.Contexts[ref.Id].Origin.Wire, derived.Wire) {
		t.Fatal("retry wire not stable")
	}
}
