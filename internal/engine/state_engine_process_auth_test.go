package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"github.com/icholy/digest"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type processCredentialRead struct {
	Worker string
	Effect string
	Lookup *pb.CredentialLookup
}

func processCredentialEffect(t *testing.T, record state.Record) (state.Effect, stateEngineEffect, stateDigestChallenge) {
	t.Helper()
	for _, effect := range record.Payload.Effects {
		if effect.Kind != "credential" {
			continue
		}
		var body stateEngineEffect
		var challenge stateDigestChallenge
		if json.Unmarshal(effect.Payload, &body) != nil || json.Unmarshal(body.Wire, &challenge) != nil {
			t.Fatal("credential effect metadata corrupt")
		}
		return effect, body, challenge
	}
	t.Fatal("committed credential effect missing")
	return state.Effect{}, stateEngineEffect{}, stateDigestChallenge{}
}

// Redis byte fields and generated control wire use nested base64 encodings.
// Check their decoded values as well as the outer JSON; do not print a leak.
func processAssertNoPlaintextCredential(t *testing.T, data []byte, secret string) {
	t.Helper()
	var contains func([]byte, int) bool
	contains = func(value []byte, depth int) bool {
		if bytes.Contains(value, []byte(secret)) {
			return true
		}
		if depth > 16 {
			t.Fatal("credential evidence nesting exceeds the qualification scan")
		}
		decoder := json.NewDecoder(bytes.NewReader(value))
		for {
			token, err := decoder.Token()
			if err != nil {
				return false
			}
			text, ok := token.(string)
			if !ok {
				continue
			}
			if strings.Contains(text, secret) {
				return true
			}
			decoded, err := base64.StdEncoding.DecodeString(text)
			if err == nil && len(decoded) > 0 && contains(decoded, depth+1) {
				return true
			}
		}
	}
	if contains(data, 0) {
		t.Fatal("plaintext fixture credential entered retained state or process evidence")
	}
}

func processAssertCredentialRecord(t *testing.T, record state.Record, secret string) {
	t.Helper()
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	processAssertNoPlaintextCredential(t, encoded, secret)
}

func TestStateNativeProcessCredentialLookupSurvivesExecutorDeath(t *testing.T) {
	for _, status := range []int{401, 407} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			// This generated secret never enters a subprocess environment or an
			// artifact. Only the surviving worker receives its transient RPC reply.
			secret := "process-credential-" + uuid.NewString()
			evidenceDirectory := os.Getenv("SIPBRIDGE_PROCESS_EVIDENCE_DIR")
			if evidenceDirectory == "" {
				evidenceDirectory = t.TempDir()
				t.Setenv("SIPBRIDGE_PROCESS_EVIDENCE_DIR", evidenceDirectory)
			}
			// Register before the harness so this audit runs after its normal
			// capture, including after a failure; capture itself never sees a reply.
			t.Cleanup(func() {
				path := filepath.Join(evidenceDirectory, strings.ReplaceAll(t.Name(), "/", "_")+".json")
				data, err := os.ReadFile(path)
				if err != nil {
					if !t.Skipped() {
						t.Errorf("credential process evidence unavailable: %v", err)
					}
					return
				}
				processAssertNoPlaintextCredential(t, data, secret)
			})
			h := newNativeProcessHarness(t)
			a, b := h.start("sip-a"), h.start("sip-b")
			credential := &pb.CredentialRef{Id: "fixture-credential", Version: "fixed-version"}
			readA := make(chan processCredentialRead, 1)
			lookupError := make(chan string, 2)
			executorDead, releaseRecovery := make(chan struct{}), make(chan struct{})
			h.mu.Lock()
			h.credentialLookup = func(worker *processWorker, effect string, lookup *pb.CredentialLookup) *pb.CredentialResult {
				if lookup == nil || lookup.Context == nil || !proto.Equal(lookup.Credential, credential) {
					select {
					case lookupError <- "credential lookup changed its context or fixed version":
					default:
					}
					return &pb.CredentialResult{Error: "fixture credential unavailable"}
				}
				if sameProcess(worker.process, a.process) {
					select {
					case readA <- processCredentialRead{Worker: worker.process.InstanceId, Effect: effect, Lookup: proto.Clone(lookup).(*pb.CredentialLookup)}:
					default:
					}
					select {
					case <-executorDead:
					case <-h.ctx.Done():
					}
					return nil // A dies while its real read-only RPC is outstanding.
				}
				if !sameProcess(worker.process, b.process) {
					return &pb.CredentialResult{Error: "fixture credential executor unavailable"}
				}
				select {
				case <-releaseRecovery:
					return &pb.CredentialResult{Username: "fixture-carrier", Password: secret, Version: credential.Version}
				case <-h.ctx.Done():
					return nil
				}
			}
			h.mu.Unlock()

			// Keep the physical writer on B while A executes the credential read.
			phone := h.phone(b)
			seedCall := "credential-flow-" + uuid.NewString()
			phone.send(t, processRequest(t, sip.OPTIONS, seedCall, 1, ""))
			h.respond(b, h.request(seedCall, 1).Envelope.GetRequest(), 200)
			phone.receive(t, sip.OPTIONS, 200)
			const target = "sip:phone@example.test;user=phone?Subject=credential-death"
			opened := h.result(a, &pb.Command{Context: &pb.ContextRef{Owner: a.process, Id: "credential-outbound-" + uuid.NewString()}, Sequence: 1,
				Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: target, Transport: "tls", BindingId: "fixture-binding", PrincipalId: "fixture-phone",
					Network: &pb.NetworkTarget{Transport: "tls", Credential: credential}, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}})
			initial := phone.receive(t, sip.INVITE, 0).(*sip.Request)
			callID := initial.CallID().Value()
			before, group := h.record(callID)
			original := processOnlyContext(t, group)
			originalFlow := proto.Clone(original.Flow).(*pb.FlowRef)
			originalOperation := original.Operation
			if !sameProcess(originalFlow.Ingress, b.process) || initial.Recipient.String() != target {
				t.Fatal("credential fixture did not retain the full target on B's physical TLS flow")
			}
			challenge := digestReply(t, portable.Message{Wire: []byte(initial.String()), Transport: "tls"}, status)
			phone.send(t, challenge.Wire)
			var lookup processCredentialRead
			select {
			case lookup = <-readA:
			case failure := <-lookupError:
				t.Fatal(failure)
			case <-h.ctx.Done():
				t.Fatal("A never entered its actual credential lookup")
			}
			pending, pendingGroup := h.record(callID)
			effect, body, saved := processCredentialEffect(t, pending)
			pendingContext := processOnlyContext(t, pendingGroup)
			if effect.Status != state.EffectPending || body.Executor != nil || effect.ID != lookup.Effect || body.Envelope.CorrelationId != lookup.Effect ||
				!stateSameContext(lookup.Lookup.Context, opened.Snapshot.Context) || !sameProcess(body.Application, h.application) ||
				!proto.Equal(saved.Credential, credential) || !bytes.Equal(saved.Origin.Wire, []byte(initial.String())) ||
				!bytes.Equal(saved.Challenge.Wire, challenge.Wire) || saved.Attempt != 1 || saved.Count != 1 ||
				pendingContext.Origin.Wire == nil || len(pendingContext.Transactions) != 1 {
				t.Fatal("actual lookup did not use one committed immutable digest effect")
			}
			processAssertCredentialRecord(t, pending, secret)
			h.kill(a)
			close(executorDead) // Only the observed OS death opens this boundary.
			retained, retainedGroup := h.record(callID)
			exact, exactBody, exactChallenge := processCredentialEffect(t, retained)
			if exact.ID != effect.ID || exact.DeadlineMS != effect.DeadlineMS || !bytes.Equal(exactBody.Wire, body.Wire) ||
				!proto.Equal(exactBody.Envelope, body.Envelope) || exactChallenge.Branch != saved.Branch ||
				len(processOnlyContext(t, retainedGroup).Transactions) != 1 {
				t.Fatal("executor death changed the original credential inputs/deadline or admitted an early retry")
			}
			processAssertCredentialRecord(t, retained, secret)
			close(releaseRecovery)
			retry := phone.receive(t, sip.INVITE, 0).(*sip.Request)
			commit := h.workerEvent(b, "digest-commit", effect.ID)
			if commit.DeadlineMS != effect.DeadlineMS+120000 || commit.Group != before.Ref.ID || commit.Stage != "digest-result" {
				t.Fatal("recovered credential completion extended its original absolute input window")
			}
			readB := h.wait(func(observed processObservation) bool {
				return observed.Worker == b.process.InstanceId && observed.Envelope.GetCredentialLookup() != nil && observed.Envelope.CorrelationId == effect.ID
			}).Envelope.GetCredentialLookup()
			if !proto.Equal(readB.Credential, credential) || !stateSameContext(readB.Context, lookup.Lookup.Context) {
				t.Fatal("surviving replica looked up a different credential or logical context")
			}
			authorization := "Authorization"
			if status == 407 {
				authorization = "Proxy-Authorization"
			}
			if retry.GetHeader(authorization) == nil {
				t.Fatal("surviving replica did not emit an authenticated native retry")
			}
			credentials, err := digest.ParseCredentials(retry.GetHeader(authorization).Value())
			if err != nil || credentials.URI != target || credentials.Cnonce != saved.Cnonce || credentials.Nc != saved.Count ||
				retry.CSeq().SeqNo != initial.CSeq().SeqNo+1 || retry.Via().Params.GetOr("branch", "") != saved.Branch || tag(retry.To().Params) != "" {
				t.Fatal("recovered digest changed its full URI, fixed nonce/count, branch, CSeq or unestablished remote tag")
			}
			parameters, err := digest.ParseChallenge(`Digest realm="carrier.fixture", nonce="fixed-nonce", algorithm=MD5, qop="auth"`)
			if err != nil {
				t.Fatal("fixture digest challenge invalid")
			}
			expected, err := digest.Digest(parameters, digest.Options{Method: "INVITE", URI: target, Username: "fixture-carrier", Password: secret, Cnonce: saved.Cnonce, Count: saved.Count})
			if err != nil || credentials.Response != expected.Response {
				t.Fatal("carrier cannot verify the recovered authenticated retry")
			}
			// The challenge's independent transaction ACK may precede or follow
			// the authenticated retry on the same stream. Consume it before
			// checking the successful dialog's different CSeq ACK below.
			var originalACK sip.Message
			for _, message := range phone.received {
				if request, ok := message.(*sip.Request); ok && request.IsAck() && request.CSeq().SeqNo == initial.CSeq().SeqNo {
					originalACK = request
					break
				}
			}
			if originalACK == nil {
				originalACK = phone.receive(t, sip.ACK, 0)
				if originalACK.CSeq().SeqNo != initial.CSeq().SeqNo {
					t.Fatal("challenge transaction ACK lost its original CSeq")
				}
			}
			phone.send(t, []byte(processInviteFinal(retry, 200, "authenticated-phone").String()))
			ack := phone.receive(t, sip.ACK, 0)
			if ack.CSeq().SeqNo != retry.CSeq().SeqNo || tag(ack.To().Params) != "authenticated-phone" {
				t.Fatal("recovered authenticated dialog did not complete its own native ACK")
			}
			// An exact challenge replay retains the original non-2xx ACK. It
			// cannot allocate a second authenticated retry after establishment.
			phone.send(t, challenge.Wire)
			challengeACK := phone.receive(t, sip.ACK, 0)
			if challengeACK.CSeq().SeqNo != initial.CSeq().SeqNo || challengeACK.String() != originalACK.String() {
				t.Fatal("challenge replay restarted authentication instead of native ACK replay")
			}
			after, finalContext := processWaitRecord(h, callID, func(_ state.Record, c *stateContext) bool {
				return c.Snapshot.Answered && c.Snapshot.InitialAck
			})
			if before.Ref.Incarnation != after.Ref.Incarnation || !stateSameContext(opened.Snapshot.Context, finalContext.Snapshot.Context) ||
				!sameProcess(finalContext.Snapshot.Application, h.application) || !proto.Equal(finalContext.Flow, originalFlow) ||
				finalContext.Operation != originalOperation || finalContext.Snapshot.CommandSequence != 1 || finalContext.OwnerLost {
				t.Fatal("credential recovery recreated native/application authority or moved the surviving TLS flow")
			}
			retries := 0
			for _, tx := range finalContext.Transactions {
				if tx.DigestAttempts == 1 {
					retries++
					if !bytes.Equal(tx.Native.Origin.Wire, []byte(retry.String())) || tx.Emissions != 1 {
						t.Fatal("recovered native retry changed its retained wire or allocated another emission")
					}
				}
			}
			if retries != 1 || len(finalContext.Transactions) != 2 {
				t.Fatal("credential effect recovery allocated more than one authenticated transaction")
			}
			physicalRetries := 0
			for _, message := range phone.received {
				if request, ok := message.(*sip.Request); ok && request.Method == sip.INVITE && request.CSeq().SeqNo == retry.CSeq().SeqNo {
					physicalRetries++
				}
			}
			if physicalRetries != 1 {
				t.Fatal("pure lookup recovery duplicated the physical authenticated INVITE")
			}
			processAssertCredentialRecord(t, after, secret)
			select {
			case failure := <-lookupError:
				t.Fatal(failure)
			default:
			}
			t.Logf("credential effect=%s originalDeadline=%d resultDeadline=%d recoveredWorker=%s authenticatedCSeq=%d", effect.ID, effect.DeadlineMS, commit.DeadlineMS, b.process.InstanceId, retry.CSeq().SeqNo)
		})
	}
}
