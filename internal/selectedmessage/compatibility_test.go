package selectedmessage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

// This mirrors the 201-case native compatibility corpus, exercising cold
// restoration at every origin and response used by a native procedure.
func TestSelectedColdRestore201NativeCases(t *testing.T) {
	capture := func(restore bool) map[string]interface{} {
		out := make(map[string]interface{})
		selected := func(m portable.Message) portable.Message {
			if restore {
				return cold(t, m)
			}
			return m
		}
		for _, transport := range []string{"udp", "tcp", "tls"} {
			for _, method := range []sip.RequestMethod{sip.REGISTER, sip.SUBSCRIBE, sip.INVITE} {
				key := transport + "/" + string(method)
				origin := selected(fixture(t, method, transport, 0))
				out[key+"/origin"] = origin
				family := portable.NonInviteServer
				if method == sip.INVITE {
					family = portable.InviteServer
				}
				tx, err := portable.NewTransaction(family, origin, portable.DefaultTimers(), "local")
				if err != nil {
					t.Fatal(err)
				}
				tx, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: 1000})
				if err != nil {
					t.Fatal(err)
				}
				out[key+"/start"] = []interface{}{tx, effects}
				for _, status := range []int{100, 180, 200, 202, 401, 407, 486, 487} {
					headers := []portable.Header{{Name: "X-Order", Value: "one"}, {Name: "X-Order", Value: "two"}}
					if method == sip.REGISTER && status >= 200 && status < 300 {
						headers = append(headers, portable.Header{Name: "Contact", Value: "<sip:phone@192.0.2.2:6000>;expires=3600"})
					}
					response, err := portable.BuildResponseWithHeaders(origin, status, "fixed reason", []byte("exact body\r\n"), "remote-tag", "<sip:phone@192.0.2.2:5061>", headers)
					if err != nil {
						t.Fatal(err)
					}
					response = selected(response)
					prefix := fmt.Sprintf("%s/%d", key, status)
					out[prefix+"/response"] = response
					next, fx, stepErr := portable.StepTransaction(tx, portable.Input{Kind: portable.SendResponse, Now: 2000, Message: response})
					failure := ""
					if stepErr != nil {
						failure = stepErr.Error()
					}
					out[prefix+"/server"] = []interface{}{next, fx, failure}
					if method == sip.INVITE && status >= 300 {
						ack, e := portable.BuildNon2xxACK(origin, response)
						if e != nil {
							t.Fatal(e)
						}
						out[prefix+"/ack"] = ack
					}
					if method == sip.INVITE && status > 100 && status < 300 {
						for _, role := range []portable.Role{portable.UAC, portable.UAS} {
							dialog, e := portable.NewDialog(role, origin, response, portable.DefaultTimers())
							if e != nil {
								t.Fatal(e)
							}
							out[prefix+"/dialog/"+string(role)] = dialog
						}
					}
				}
				if method == sip.INVITE {
					cancel, e := portable.BuildCancel(origin)
					if e != nil {
						t.Fatal(e)
					}
					out[key+"/cancel"] = cancel
				}
			}
		}
		origin := selected(fixture(t, sip.INVITE, "tls", 16))
		for _, qop := range []string{"auth", "auth-int", ""} {
			for _, status := range []int{401, 407} {
				header := "WWW-Authenticate"
				if status == 407 {
					header = "Proxy-Authenticate"
				}
				challenge := `Digest realm="example.test", nonce="fixed-nonce", opaque="fixed-opaque", algorithm=MD5`
				if qop != "" {
					challenge += `, qop="` + qop + `"`
				}
				response, err := portable.BuildResponseWithHeaders(origin, status, "challenge", nil, "remote-tag", "", []portable.Header{{Name: header, Value: challenge}})
				if err != nil {
					t.Fatal(err)
				}
				response = selected(response)
				retry, err := portable.BuildDigestRetry(origin, response, portable.DigestCredentials{Username: "fixture-user", Password: "synthetic-fixture-password", Cnonce: "fixed-cnonce", Count: 1}, "z9hG4bK-fixed-retry")
				if err != nil {
					t.Fatal(err)
				}
				out[fmt.Sprintf("digest/%s/%d", qop, status)] = retry
			}
		}
		return out
	}
	expected := capture(false)
	actual := capture(true)
	if len(expected) != 201 || len(actual) != 201 {
		t.Fatalf("unexpected corpus size %d/%d", len(expected), len(actual))
	}
	for key, want := range expected {
		a, e := json.Marshal(want)
		if e != nil {
			t.Fatal(e)
		}
		b, e := json.Marshal(actual[key])
		if e != nil || !bytes.Equal(a, b) {
			t.Fatalf("cold native case %s changed: %v", key, e)
		}
	}
}
