package portable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/emiago/sipgo/sip"
	"os"
	"testing"
)

func TestImmutableCompatibilityCapture(t *testing.T) {
	out := map[string]interface{}{}
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, method := range []sip.RequestMethod{sip.REGISTER, sip.SUBSCRIBE, sip.INVITE} {
			key := transport + "/" + string(method)
			o := origin(t, method, transport)
			o = mutate(t, o, func(v sip.Message) {
				v.AppendHeader(sip.NewHeader("Record-Route", "<sip:edge1.example.test;lr>"))
				v.AppendHeader(sip.NewHeader("Record-Route", "<sip:edge2.example.test;lr>"))
				v.AppendHeader(sip.NewHeader("Timestamp", "1234"))
				v.AppendHeader(sip.NewHeader("X-Order", "one"))
				v.AppendHeader(sip.NewHeader("X-Order", "two"))
			})
			out[key+"/origin"] = o
			family := NonInviteServer
			if method == sip.INVITE {
				family = InviteServer
			}
			server := tx(t, family, o)
			server, effects := step(t, server, Input{Kind: Start, Now: 1000})
			out[key+"/start"] = []interface{}{server, effects}
			for _, status := range []int{100, 180, 200, 202, 401, 407, 486, 487} {
				headers := []Header{{Name: "X-Order", Value: "one"}, {Name: "X-Order", Value: "two"}}
				if method == sip.REGISTER && status >= 200 && status < 300 {
					headers = append(headers, Header{Name: "Contact", Value: "<sip:phone@192.0.2.2:6000>;expires=3600"})
				}
				m, err := BuildResponseWithHeaders(o, status, "fixed reason", []byte("exact body\r\n"), "remote-tag", "<sip:phone@192.0.2.2:5061>", headers)
				if err != nil {
					t.Fatal(err)
				}
				data, _ := json.Marshal(status)
				prefix := key + "/" + string(data)
				out[prefix+"/response"] = m
				next, fx, err := StepTransaction(server, Input{Kind: SendResponse, Now: 2000, Message: m})
				failure := ""
				if err != nil {
					failure = err.Error()
				}
				out[prefix+"/server"] = []interface{}{next, fx, failure}
				if method == sip.INVITE && status >= 300 {
					ack, err := BuildNon2xxACK(o, m)
					if err != nil {
						t.Fatal(err)
					}
					out[prefix+"/ack"] = ack
				}
				if method == sip.INVITE && status > 100 && status < 300 {
					for _, role := range []Role{UAC, UAS} {
						d, err := NewDialog(role, o, m, DefaultTimers())
						if err != nil {
							t.Fatal(err)
						}
						out[prefix+"/dialog/"+string(role)] = d
					}
				}
			}
			if method == sip.INVITE {
				cancel, err := BuildCancel(o)
				if err != nil {
					t.Fatal(err)
				}
				out[key+"/cancel"] = cancel
			}
		}
	}
	o := origin(t, sip.INVITE, "tls")
	o = mutate(t, o, func(v sip.Message) {
		v.SetBody([]byte("exact digest body\x00\r\n"))
		v.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	})
	for _, qop := range []string{"auth", "auth-int", ""} {
		for _, status := range []int{401, 407} {
			header := "WWW-Authenticate"
			if status == 407 {
				header = "Proxy-Authenticate"
			}
			text := `Digest realm="example.test", nonce="fixed-nonce", opaque="fixed-opaque", algorithm=MD5`
			if qop != "" {
				text += `, qop="` + qop + `"`
			}
			challenge, err := BuildResponseWithHeaders(o, status, "challenge", nil, "remote-tag", "", []Header{{Name: header, Value: text}})
			if err != nil {
				t.Fatal(err)
			}
			retry, err := BuildDigestRetry(o, challenge, DigestCredentials{Username: "fixture-user", Password: "synthetic-fixture-password", Cnonce: "fixed-cnonce", Count: 1}, "z9hG4bK-fixed-retry")
			if err != nil {
				t.Fatal(err)
			}
			code, _ := json.Marshal(status)
			out["digest/"+qop+"/"+string(code)] = retry
		}
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != "ce009b943bdcabc9a6e791edd5f6742a4308920dc5d1c197bee5aa0062fcb0f2" {
		t.Fatal("201-case pre-immutable native protocol/wire golden changed")
	}
	if target := os.Getenv("IMMUTABLE_COMPAT_CAPTURE"); target != "" {
		if err := os.WriteFile(target, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
