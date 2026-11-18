package engine

import (
	"fmt"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestInitialInviteSecureContactMemoryAndRedisCandidate(t *testing.T) {
	cases := []struct {
		name, target, contact, route, transport, want string
		fails                                         bool
	}{
		{"request-sips", "sips:edge@example.test", "sip:phone@example.test", "", "tls", "sips", false},
		{"route-sips", "sip:edge@example.test", "sip:phone@example.test", "sips:proxy@example.test", "tls", "sips", false},
		{"contact-sips", "sip:edge@example.test", "sips:phone@example.test", "", "tls", "sips", false},
		{"route-precedes-contact", "sip:edge@example.test", "sips:phone@example.test", "sip:proxy@example.test", "tls", "sip", false},
		{"sip-tls", "sip:edge@example.test", "sip:phone@example.test", "", "tls", "sip", false},
		{"sip-udp", "sip:edge@example.test", "sip:phone@example.test", "", "udp", "sip", false},
		{"sips-not-tls", "sips:edge@example.test", "sip:phone@example.test", "", "udp", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := sip.ParseMessage(stateTestRequest("INVITE", 1, "", "secure-contact").Wire)
			if err != nil {
				t.Fatal(err)
			}
			req := parsed.(*sip.Request)
			if err := sip.ParseUri(tc.target, &req.Recipient); err != nil {
				t.Fatal(err)
			}
			if req.Contact() == nil {
				t.Fatal("fixture missing Contact")
			}
			if err := sip.ParseUri(tc.contact, &req.Contact().Address); err != nil {
				t.Fatal(err)
			}
			if tc.route != "" {
				req.AppendHeader(sip.NewHeader("Record-Route", fmt.Sprintf("<%s;lr>", tc.route)))
				if tc.name == "route-precedes-contact" {
					req.AppendHeader(sip.NewHeader("Record-Route", "<sips:later.example.test;lr>"))
				}
			}
			origin, err := portable.ParseMessage([]byte(req.String()), tc.transport, "192.0.2.12:5061", "")
			if err != nil {
				t.Fatal(err)
			}
			transaction, _, err := nativeServerStart(origin, "local", 1000)
			if err != nil {
				t.Fatal(err)
			}
			r, c, _ := forkFixture(t)
			r.engine.cfg.AdvertiseAddress = "edge.example.test:5061"
			r.engine.cfg.PlainAdvertiseAddress = "edge.example.test:5060"
			r.engine.cfg.ContactURI = "sip:edge@edge.example.test:5061;transport=tls"
			c.Tag = "local"
			for _, status := range []int{180, 200} {
				contact, sentBy, err := r.engine.advertisement(tc.transport)
				if err != nil {
					t.Fatal(err)
				}
				candidate, nativeErr := nativePrepareInitialInviteResponse(transaction, &pb.Respond{Status: uint32(status)}, "local", contact, sentBy, 1001)
				memory, memoryErr := r.buildResponse(c, origin, status, "fixture", nil, nil)
				if tc.fails {
					if nativeErr == nil || memoryErr == nil {
						t.Fatal("secure origin accepted on non-TLS")
					}
					continue
				}
				if nativeErr != nil || memoryErr != nil {
					t.Fatalf("response errors native=%v memory=%v", nativeErr, memoryErr)
				}
				for name, message := range map[string]portable.Message{"native": candidate.Message, "memory": memory} {
					wire, err := sip.ParseMessage(message.WireBytes())
					if err != nil {
						t.Fatal(err)
					}
					actual := wire.(*sip.Response).Contact()
					if actual == nil || actual.Address.Scheme != tc.want || actual.Address.Host != "edge.example.test" {
						t.Fatalf("%s status %d wrong Contact", name, status)
					}
				}
				if status == 200 {
					if candidate.Dialog == nil {
						t.Fatal("dialog missing")
					}
					var retained sip.Uri
					if err := sip.ParseUri(candidate.Dialog.Contact, &retained); err != nil || retained.Scheme != tc.want {
						t.Fatal("dialog retained incorrect Contact")
					}
				}
			}
		})
	}
}
