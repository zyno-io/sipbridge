package engine

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

var challengeAuthority = regexp.MustCompile(`(?i)sips?:[^@;>\s]+@([^;>\s]+)`)
var challengePort = regexp.MustCompile(`:\d+$`)
var challengeTag = regexp.MustCompile(`^local-auth-[0-9a-f]{32}$`)

type challengeKey struct {
	key   []byte
	until time.Time
}

// Only issuance configuration and a finite ACK-proof key overlap live here.
// No account credential, accepted request, transaction or dialog is retained.
type localChallenges struct {
	mu      sync.Mutex
	policy  *pb.DigestChallengePolicy
	until   time.Time
	retired []challengeKey
}

func ParseRequiredAuthMethods(raw string, methods MethodPolicy) (*pb.InboundAuthPolicy, error) {
	if raw == "" {
		return nil, nil
	}
	policy := new(pb.InboundAuthPolicy)
	seen := make(map[string]bool)
	for _, value := range strings.Split(raw, ",") {
		method := strings.TrimSpace(value)
		if seen[method] || methods.disabled(&pb.SipMessage{Method: method}) {
			return nil, errors.New("required authentication conflicts with method policy")
		}
		seen[method] = true
		switch method {
		case "REGISTER":
			policy.RequireRegister = true
		case "INVITE":
			policy.RequireInvite = true
		case "SUBSCRIBE":
			policy.RequireSubscribe = true
		default:
			return nil, errors.New("required authentication permits only REGISTER, INVITE and SUBSCRIBE")
		}
	}
	return policy, nil
}

func validChallengePolicy(policy *pb.DigestChallengePolicy, now time.Time) bool {
	if policy == nil || len(policy.SigningKey) != sha256.Size || policy.Realm == "" || len(policy.Realm) > 253 ||
		strings.ContainsAny(policy.Realm, "\"\\\r\n") || policy.AuthenticationEpoch == "" || len(policy.AuthenticationEpoch) > 512 ||
		strings.ContainsAny(policy.AuthenticationEpoch, "\r\n\x00") || policy.ExpiresAtMs > uint64(now.Add(time.Minute).UnixMilli()) {
		return false
	}
	return policy.ExpiresAtMs > uint64(now.UnixMilli())
}

func (i *Ingress) UpdateChallengePolicy(policy *pb.DigestChallengePolicy) error {
	now := time.Now()
	if !validChallengePolicy(policy, now) {
		return errors.New("invalid local challenge policy")
	}
	c := &i.challenges
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := make([]challengeKey, 0, 4)
	for _, retired := range c.retired {
		if now.Before(retired.until) && !bytes.Equal(retired.key, policy.SigningKey) {
			kept = append(kept, retired)
		}
	}
	c.retired = kept
	if c.policy != nil && !bytes.Equal(c.policy.SigningKey, policy.SigningKey) {
		found := false
		for index := range c.retired {
			if bytes.Equal(c.retired[index].key, c.policy.SigningKey) {
				c.retired[index].until = now.Add(121 * time.Second)
				found = true
			}
		}
		if !found && len(c.retired) >= 4 {
			return errors.New("local challenge key overlap capacity")
		}
		if !found {
			c.retired = append(c.retired, challengeKey{key: bytes.Clone(c.policy.SigningKey), until: now.Add(121 * time.Second)})
		}
	}
	c.policy = proto.Clone(policy).(*pb.DigestChallengePolicy)
	// Keep expiry monotonic after acceptance; clock rollback cannot renew policy.
	c.until = now.Add(time.Duration(int64(policy.ExpiresAtMs)-now.UnixMilli()) * time.Millisecond)
	return nil
}

func (i *Ingress) challengePolicy() *pb.DigestChallengePolicy {
	i.challenges.mu.Lock()
	defer i.challenges.mu.Unlock()
	if i.challenges.policy == nil || !time.Now().Before(i.challenges.until) {
		return nil
	}
	return proto.Clone(i.challenges.policy).(*pb.DigestChallengePolicy)
}

func (i *Ingress) ChallengeReady() bool { return i.challengePolicy() != nil }

func localChallengeScope(message *pb.SipMessage) string {
	from := ""
	for _, header := range message.Headers {
		if strings.EqualFold(header.Name, "from") {
			from = header.Value
		}
	}
	match := challengeAuthority.FindStringSubmatch(from)
	if len(match) != 2 {
		return "invalid:" + strings.ToLower(from) + "\n"
	}
	return strings.ToLower(challengePort.ReplaceAllString(match[1], "")) + "\n"
}

func challengeMAC(key []byte, input string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(input))
	return mac.Sum(nil)
}

func localChallengeNonce(policy *pb.DigestChallengePolicy, message *pb.SipMessage, bucket int64) string {
	timestamp := strconv.FormatInt(bucket, 10)
	random := challengeMAC(policy.SigningKey, "sipbridge-local-random-v1\n"+policy.AuthenticationEpoch+"\n"+timestamp+"\n"+string(message.Wire))
	raw := "l1." + timestamp + "." + hex.EncodeToString(random[:16])
	signature := challengeMAC(policy.SigningKey, "sipbridge-digest-v1\n"+policy.AuthenticationEpoch+"\n"+localChallengeScope(message)+"\n"+raw)
	return base64.RawURLEncoding.EncodeToString([]byte(raw + "." + base64.RawURLEncoding.EncodeToString(signature)))
}

func localChallengeTag(key []byte, request *sip.Request, flow *pb.FlowRef) string {
	if request.CallID() == nil || request.From() == nil || request.Via() == nil || request.CSeq() == nil {
		return ""
	}
	identity, _ := json.Marshal([]string{flow.Ingress.GetInstanceId(), flow.Ingress.GetEpoch(), flow.ConnectionId, flow.Transport, flow.SourceIp,
		strconv.FormatUint(uint64(flow.SourcePort), 10), request.Recipient.String(), request.CallID().Value(), request.From().Params.GetOr("tag", ""),
		request.Via().Params.GetOr("branch", ""), strconv.FormatUint(uint64(request.CSeq().SeqNo), 10)})
	proof := challengeMAC(key, "sipbridge-local-challenge-ack-v1\n"+string(identity))
	return "local-auth-" + hex.EncodeToString(proof[:16])
}

func (i *Ingress) localChallengeACK(message *pb.SipMessage, flow *pb.FlowRef) bool {
	if i.authPolicy == nil || !i.authPolicy.RequireInvite || message == nil || flow == nil || message.Status != 0 || message.Method != "ACK" {
		return false
	}
	parsed, err := sip.ParseMessage(message.Wire)
	request, ok := parsed.(*sip.Request)
	if err != nil || !ok || request.To() == nil {
		return false
	}
	tag := request.To().Params.GetOr("tag", "")
	if !challengeTag.MatchString(tag) {
		return false
	}
	now := time.Now()
	i.challenges.mu.Lock()
	defer i.challenges.mu.Unlock()
	if policy := i.challenges.policy; policy != nil && now.Before(i.challenges.until.Add(121*time.Second)) &&
		hmac.Equal([]byte(localChallengeTag(policy.SigningKey, request, flow)), []byte(tag)) {
		return true
	}
	for _, retired := range i.challenges.retired {
		if now.Before(retired.until) && hmac.Equal([]byte(localChallengeTag(retired.key, request, flow)), []byte(tag)) {
			return true
		}
	}
	return false
}

func (i *Ingress) requiresLocalChallenge(message *pb.SipMessage) bool {
	if message == nil || i.authPolicy == nil || message.Status != 0 || message.Authorization != nil {
		return false
	}
	to := message.Addresses["to"]
	tagged := to != nil && len(to.Values) > 0 && to.Values[0].Params["tag"] != ""
	switch message.Method {
	case "REGISTER":
		return i.authPolicy.RequireRegister
	case "INVITE":
		return i.authPolicy.RequireInvite && !tagged
	case "SUBSCRIBE":
		return i.authPolicy.RequireSubscribe && !tagged
	}
	return false
}

func (i *Ingress) challengeLocally(ctx context.Context, flow *physicalFlow, message *pb.SipMessage) {
	policy := i.challengePolicy()
	if policy == nil {
		i.localResponseWithin(ctx, flow, message, 503, "auth-unavailable-", []*pb.Header{{Name: "Retry-After", Value: "1"}})
		return
	}
	bucket := time.Now().Unix() / 120 * 120
	nonce := localChallengeNonce(policy, message, bucket)
	challenge := fmt.Sprintf("Digest realm=%q, nonce=%q, algorithm=MD5, qop=\"auth\"", policy.Realm, nonce)
	i.localResponseWithin(ctx, flow, message, 401, "local-auth-", []*pb.Header{{Name: "WWW-Authenticate", Value: challenge}}, func(request *sip.Request) string {
		return localChallengeTag(policy.SigningKey, request, flow.ref)
	})
}
