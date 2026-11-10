package sipgo

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
	"github.com/stretchr/testify/require"
)

func TestDigestUsesCompleteRequestURI(t *testing.T) {
	for _, status := range []int{401, 407} {
		for _, dialog := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/dialog=%t", status, dialog), func(t *testing.T) {
				const uri = "sip:alice@example.test:5070;transport=tcp;user=phone?subject=hello"
				const challenge = `Digest realm="carrier", nonce="test-nonce", algorithm=MD5, qop="auth"`
				credentialHeader, challengeHeader := "Authorization", "WWW-Authenticate"
				if status == 407 {
					credentialHeader, challengeHeader = "Proxy-Authorization", "Proxy-Authenticate"
				}
				attempts := 0
				hash := func(value string) string { return fmt.Sprintf("%x", md5.Sum([]byte(value))) }
				client := testClient(t, func(req *sip.Request) *sip.Response {
					attempts++
					actualURI := strings.Fields(req.StartLine())[1]
					require.Equal(t, uri, actualURI)
					credentials := req.GetHeader(credentialHeader)
					if credentials == nil {
						response := sip.NewResponseFromRequest(req, status, "Authenticate", nil)
						response.AppendHeader(sip.NewHeader(challengeHeader, challenge))
						return response
					}
					parsed, err := digest.ParseCredentials(credentials.Value())
					require.NoError(t, err)
					require.Equal(t, actualURI, parsed.URI, "digest uri must match the complete wire request target")
					expected := hash(fmt.Sprintf("%s:test-nonce:%08x:%s:auth:%s", hash("alice:carrier:secret"), parsed.Nc, parsed.Cnonce, hash(string(req.Method)+":"+actualURI)))
					require.Equal(t, expected, parsed.Response, "digest must hash the complete wire request target")
					response := sip.NewResponseFromRequest(req, 200, "OK", nil)
					response.To().Params.Add("tag", "carrier")
					response.AppendHeader(sip.NewHeader("Contact", "<sip:carrier@example.test>"))
					return response
				})
				var target sip.Uri
				require.NoError(t, sip.ParseUri(uri, &target))
				if dialog {
					ua := &DialogUA{Client: client}
					session, err := ua.Invite(context.Background(), target, nil)
					require.NoError(t, err)
					require.NoError(t, session.WaitAnswer(context.Background(), AnswerOptions{Username: "alice", Password: "secret"}))
				} else {
					request := sip.NewRequest(sip.OPTIONS, target)
					response, err := client.Do(context.Background(), request)
					require.NoError(t, err)
					require.Equal(t, status, response.StatusCode)
					accepted, err := client.DoDigestAuth(context.Background(), request, response, DigestAuth{Username: "alice", Password: "secret"})
					require.NoError(t, err)
					require.Equal(t, 200, accepted.StatusCode)
				}
				require.Equal(t, 2, attempts)
			})
		}
	}
}
