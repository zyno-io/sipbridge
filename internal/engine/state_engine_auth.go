package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/icholy/digest"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

const stateDigestRetryLimit = 3

// Only immutable digest inputs and an opaque credential version are retained.
// A CredentialResult is deliberately not a field on any Redis input or value.
type stateDigestChallenge struct {
	Origin     portable.Message  `json:"origin"`
	Challenge  portable.Message  `json:"challenge"`
	Credential *pb.CredentialRef `json:"credential"`
	Cnonce     string            `json:"cnonce"`
	Count      int               `json:"count"`
	Branch     string            `json:"branch"`
	Attempt    uint32            `json:"attempt"`
	ResultHash string            `json:"resultHash,omitempty"`
}

func (r *stateReduction) digestChallenge(c *stateContext, tx *stateTransaction, message portable.Message) (bool, error) {
	parsed, err := message.Parse()
	if err != nil {
		return false, err
	}
	response, ok := parsed.(*sip.Response)
	if !ok || response.StatusCode != 401 && response.StatusCode != 407 || c.Credential == nil ||
		c.Credential.Id == "" || c.Credential.Version == "" || c.Snapshot.Answered || c.Snapshot.Ended || c.Cancelled || tx.Native.Cancelled ||
		(tx.Native.Family != portable.InviteClient && tx.Native.Family != portable.NonInviteClient) {
		return false, nil
	}
	if !portable.MatchResponse(tx.Native.Origin, message) {
		return false, errors.New("digest challenge transaction mismatch")
	}
	if tx.DigestEffect != "" {
		return true, nil
	}
	if tx.DigestAttempts >= stateDigestRetryLimit {
		return false, nil
	}
	name := "WWW-Authenticate"
	if response.StatusCode == 407 {
		name = "Proxy-Authenticate"
	}
	if response.GetHeader(name) == nil {
		return false, nil
	}
	attempt := tx.DigestAttempts + 1
	identity := r.engine.id("digest", r.record.Ref.Incarnation, c.Snapshot.Context.Id, tx.Native.Key, strconv.FormatUint(uint64(attempt), 10), string(message.Wire))
	challenge := stateDigestChallenge{
		Origin: tx.Native.Origin, Challenge: message, Credential: proto.Clone(c.Credential).(*pb.CredentialRef),
		Cnonce: identity[:32], Count: int(attempt), Branch: sip.RFC3261BranchMagicCookie + identity[32:], Attempt: attempt,
	}
	encoded, err := challenge.MarshalBinary()
	if err != nil {
		return false, err
	}
	lookup := &pb.CredentialLookup{Context: proto.Clone(c.Snapshot.Context).(*pb.ContextRef), Credential: proto.Clone(c.Credential).(*pb.CredentialRef)}
	r.addEffect("credential", c, stateEngineEffect{Wire: encoded, Transaction: tx.Native.Key, Command: tx.Command, Envelope: &pb.Envelope{Payload: &pb.Envelope_CredentialLookup{CredentialLookup: lookup}}}, r.now+2000)
	tx.DigestEffect = r.payload.Effects[len(r.payload.Effects)-1].ID
	r.touch(c)
	return true, nil
}

// dispatchCredential is read-only until the derived request enters apply. It can
// repeat on another worker without permitting a SIP write before native commit.
func (e *StateEngine) dispatchCredential(ctx context.Context, ref state.Ref, effect state.Effect, body stateEngineEffect, release func()) {
	var challenge stateDigestChallenge
	if challenge.UnmarshalBinary(body.Wire) != nil || body.Envelope.GetCredentialLookup() == nil || challenge.Credential == nil {
		return
	}
	// This pure lookup has no executor authority. Including an executor in its
	// result would make an identical concurrent computation conflict on replay.
	in := stateEngineInput{Kind: "digest-result", Effect: effect.ID}
	if time.Now().UnixMilli() >= effect.DeadlineMS {
		in.Failure = "SIP setup credential deadline elapsed"
	} else {
		peer, err := e.control.Peer(body.Application)
		if err != nil {
			return
		}
		reply, err := peer.Call(ctx, body.Envelope)
		if err != nil {
			// A transient read failure does not consume the fixed credential effect.
			return
		}
		in.Message, err = stateBuildDigestResult(challenge, reply.GetCredentialResult())
		if err != nil {
			in.Failure = "SIP setup credential unavailable"
		}
	}
	operation := e.id("digest-result", effect.ID)
	// This absolute input window is fixed by the committed effect. Concurrent
	// pure workers must not create different full input identities from now().
	deadline := time.UnixMilli(effect.DeadlineMS).Add(2 * time.Minute)
	if _, committed, err := e.applyCommitted(e.ctx, ref, operation, in, deadline, false); err == nil {
		release()
		e.dispatchCommitted(ref, committed, effect.ID, effect.Kind)
	}
}

func stateBuildDigestResult(challenge stateDigestChallenge, credentials *pb.CredentialResult) (*pb.SipMessage, error) {
	if credentials == nil {
		return nil, errors.New("SIP setup credential unavailable")
	}
	defer func() {
		credentials.Username = ""
		credentials.Password = ""
		credentials.Version = ""
		credentials.Error = ""
	}()
	if challenge.Credential == nil || credentials.Error != "" || credentials.Version != challenge.Credential.Version ||
		credentials.Username == "" || credentials.Password == "" || len(credentials.Username) > 1024 || len(credentials.Password) > 4096 {
		return nil, errors.New("SIP setup credential unavailable")
	}
	retry, err := portable.BuildDigestRetry(challenge.Origin, challenge.Challenge, portable.DigestCredentials{
		Username: credentials.Username, Password: credentials.Password, Cnonce: challenge.Cnonce, Count: challenge.Count,
	}, challenge.Branch)
	if err != nil || len(retry.Wire) > 65536 {
		return nil, errors.New("SIP setup digest rejected")
	}
	message, err := Observe(retry.Wire, true)
	if err != nil {
		return nil, errors.New("SIP setup digest rejected")
	}
	return message, nil
}

func stateDigestResultHash(message *pb.SipMessage, failure string) string {
	data := []byte("failure")
	if failure == "" && message != nil {
		data = message.Wire
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (r *stateReduction) digestResult() error {
	index, effect, body, err := r.findEffect(r.in.Effect)
	if err != nil {
		return err
	}
	if effect.Kind != "credential" {
		return errors.New("digest effect required")
	}
	c := r.group.Contexts[body.Context]
	var challenge stateDigestChallenge
	if c == nil || challenge.UnmarshalBinary(body.Wire) != nil || !proto.Equal(c.Credential, challenge.Credential) ||
		!proto.Equal(body.Envelope.GetCredentialLookup().GetCredential(), challenge.Credential) ||
		!stateSameContext(body.Envelope.GetCredentialLookup().GetContext(), c.Snapshot.Context) {
		return errors.New("digest credential scope mismatch")
	}
	tx := c.Transactions[body.Transaction]
	if tx == nil || tx.DigestEffect != effect.ID || !bytes.Equal(tx.Native.Origin.Wire, challenge.Origin.Wire) ||
		tx.Native.LastResponse == nil || !bytes.Equal(tx.Native.LastResponse.Wire, challenge.Challenge.Wire) ||
		challenge.Attempt != tx.DigestAttempts+1 || challenge.Attempt > stateDigestRetryLimit || challenge.Count != int(challenge.Attempt) {
		return errors.New("digest challenge state mismatch")
	}
	hash := stateDigestResultHash(r.in.Message, r.in.Failure)
	if effect.Status == state.EffectWritten || effect.Status == state.EffectNotWritten {
		if hash != challenge.ResultHash {
			return errors.New("conflicting digest completion")
		}
		return nil
	}
	if effect.Status != state.EffectPending && effect.Status != state.EffectDispatching {
		return errors.New("digest computation unavailable")
	}
	if effect.AttemptID == "" {
		effect.AttemptID = "compute-" + r.engine.id("digest-compute", effect.ID)[:48]
	}
	if c.Snapshot.Ended || c.Cancelled || tx.Native.Cancelled {
		effect.Status = state.EffectCancelled
		r.setEffect(index, effect, body)
		return nil
	}
	if r.in.Failure != "" || r.now >= effect.DeadlineMS {
		effect.Status = state.EffectNotWritten
		challenge.ResultHash = hash
		body.Wire, _ = challenge.MarshalBinary()
		r.setEffect(index, effect, body)
		c.Result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Error: "SIP setup authentication unavailable", Request: mustObserve(tx.Native.Origin.Wire, true), Response: mustObserve(challenge.Challenge.Wire, false)}
		c.Snapshot.Ended = true
		c.RetireMS = r.now + 120000
		r.touch(c)
		c.Result.Snapshot = stateSnapshot(c, r.engine.cfg.Process)
		r.result = proto.Clone(c.Result).(*pb.OperationResult)
		r.event(c, pb.DialogEvent_FAILED, nil, c.Operation)
		return nil
	}
	if r.in.Message == nil {
		return errors.New("derived digest request required")
	}
	retry := portable.Message{Wire: append([]byte(nil), r.in.Message.Wire...), Transport: challenge.Origin.Transport, Source: challenge.Origin.Source, Destination: challenge.Origin.Destination}
	if err = stateValidateDigestRetry(challenge, retry); err != nil {
		return err
	}
	native, err := portable.NewTransaction(tx.Native.Family, retry, tx.Native.Profile, "")
	if err != nil {
		return err
	}
	if _, exists := c.Transactions[native.Key]; exists {
		return errors.New("digest retry transaction already exists")
	}
	next := &stateTransaction{Native: native, Flow: tx.Flow, Command: tx.Command, Application: tx.Application, DigestAttempts: challenge.Attempt, Policy: tx.Policy}
	c.Transactions[native.Key] = next
	// A challenge's To-tag cannot become the next transaction's dialog identity.
	c.Origin = retry
	c.Snapshot.RemoteTag = ""
	if c.Result != nil {
		c.Result.Request = mustObserve(retry.Wire, true)
	}
	effect.Status = state.EffectWritten
	challenge.ResultHash = hash
	body.Wire, _ = challenge.MarshalBinary()
	r.setEffect(index, effect, body)
	r.touch(c)
	r.index(c, retry, true)
	return r.stepTransaction(c, next, portable.Input{Kind: portable.Start, Now: r.now})
}

func stateValidateDigestRetry(challenge stateDigestChallenge, retry portable.Message) error {
	if len(retry.Wire) == 0 || len(retry.Wire) > 65536 {
		return errors.New("derived digest request size invalid")
	}
	parsed, err := challenge.Origin.Parse()
	if err != nil {
		return err
	}
	origin, ok := parsed.(*sip.Request)
	if !ok {
		return errors.New("digest origin request required")
	}
	parsed, err = retry.Parse()
	if err != nil {
		return err
	}
	request, ok := parsed.(*sip.Request)
	if !ok {
		return errors.New("derived digest request required")
	}
	parsed, err = challenge.Challenge.Parse()
	if err != nil {
		return err
	}
	response, ok := parsed.(*sip.Response)
	if !ok || !portable.MatchResponse(challenge.Origin, challenge.Challenge) {
		return errors.New("digest challenge mismatch")
	}
	auth, header := "Authorization", "WWW-Authenticate"
	if response.StatusCode == 407 {
		auth, header = "Proxy-Authorization", "Proxy-Authenticate"
	} else if response.StatusCode != 401 {
		return errors.New("digest challenge status required")
	}
	if len(request.GetHeaders(auth)) != 1 || retry.Transport != challenge.Origin.Transport || request.CSeq() == nil || request.Via() == nil {
		return errors.New("derived digest identity invalid")
	}
	credentials, err := digest.ParseCredentials(request.GetHeader(auth).Value())
	if err != nil || credentials.URI != origin.Recipient.String() || credentials.Response == "" {
		return errors.New("derived digest inputs invalid")
	}
	if response.GetHeader(header) == nil {
		return errors.New("digest challenge header required")
	}
	parameters, err := digest.ParseChallenge(response.GetHeader(header).Value())
	if err != nil || credentials.Nonce != parameters.Nonce || credentials.Realm != parameters.Realm || credentials.Opaque != parameters.Opaque {
		return errors.New("derived digest challenge inputs invalid")
	}
	// RFC 2069 compatibility has no qop/cnonce/nc. The native builder decides the
	// supported algorithm and emits the fixed committed values when qop is used.
	if (credentials.QOP != "" || credentials.Cnonce != "") && credentials.Cnonce != challenge.Cnonce {
		return errors.New("derived digest computation identity invalid")
	}
	if credentials.QOP != "" && credentials.Nc != challenge.Count {
		return errors.New("derived digest computation identity invalid")
	}
	origin.RemoveHeader(auth)
	request.RemoveHeader(auth)
	origin.CSeq().SeqNo++
	origin.Via().Params.Add("branch", challenge.Branch)
	if origin.String() != request.String() {
		return errors.New("derived digest changes native request")
	}
	return nil
}
