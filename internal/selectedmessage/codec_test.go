package selectedmessage

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

func TestSelectedWriterDecodeOwnsExactWireWithoutSIPParse(t *testing.T) {
	// Private framing validation is shared; a physical writer does not invoke
	// a SIP parser even though native consumers will reject these SIP bytes.
	want := []byte("not a SIP message\x00\xff")
	data, err := storagecodec.Marshal(storagecodec.SelectedMessage, &storagepb.PortableMessage{Wire: want, Transport: "tls", Source: "192.0.2.1:5061"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeWire(data)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("writer parsed or changed exact wire", err)
	}
	got[0] = 'X'
	again, err := DecodeWire(data)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatal("returned wire aliases retained record", err)
	}
	for i := range data {
		data[i] = 0
	}
	if !bytes.Equal(again, want) {
		t.Fatal("decoder retained the input record buffer")
	}
	for _, p := range []*storagepb.PortableMessage{{Transport: "tls"}, {Wire: want, Source: strings.Repeat("a", maxFieldBytes+1)}} {
		encoded, e := storagecodec.Marshal(storagecodec.SelectedMessage, p)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = DecodeWire(encoded); e == nil {
			t.Fatal("writer bypassed shared private record validation")
		}
	}
}

var benchmarkWriterHash [32]byte

func fixture(t testing.TB, method sip.RequestMethod, transport string, body int) portable.Message {
	t.Helper()
	m, err := portable.BuildInitialRequest(portable.InitialRequest{Method: method, URI: "sip:phone@example.test;user=phone?Subject=test", From: "<sip:controller@example.test>", CallID: "benchmark-call", LocalTag: "benchmark-tag", Branch: "z9hG4bK-benchmark", CSeq: 42, Advertise: "192.0.2.1:5061", Contact: "<sip:controller@192.0.2.1:5061;transport=tls>", Transport: transport, Body: bytes.Repeat([]byte("a"), body)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func cold(t *testing.T, m portable.Message) portable.Message {
	t.Helper()
	data, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	result, err := DecodeOpaque(data)
	if err != nil {
		t.Fatal(err)
	}
	if !m.EqualWire(result) {
		t.Fatal("wire changed")
	}
	return result
}

func TestSelectedColdRestoreNativeProcedures(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		for _, method := range []sip.RequestMethod{sip.REGISTER, sip.SUBSCRIBE, sip.INVITE} {
			t.Run(transport+"/"+string(method), func(t *testing.T) {
				original := fixture(t, method, transport, 0)
				restored := cold(t, original)
				for _, status := range []int{100, 180, 200, 202, 401, 407, 486, 487} {
					headers := []portable.Header{{Name: "X-Order", Value: "one"}, {Name: "X-Order", Value: "two"}}
					before, err := portable.BuildResponseWithHeaders(original, status, "fixed reason", []byte("exact body\r\n"), "remote-tag", "<sip:phone@192.0.2.2:5061>", headers)
					if err != nil {
						t.Fatal(err)
					}
					after, err := portable.BuildResponseWithHeaders(restored, status, "fixed reason", []byte("exact body\r\n"), "remote-tag", "<sip:phone@192.0.2.2:5061>", headers)
					if err != nil || !before.EqualWire(after) {
						t.Fatalf("response %d changed: %v", status, err)
					}
					restoredResponse := cold(t, before)
					if _, err = restoredResponse.Facts(); err != nil {
						t.Fatal(err)
					}
					if method == sip.INVITE && status >= 300 {
						a, e := portable.BuildNon2xxACK(original, before)
						if e != nil {
							t.Fatal(e)
						}
						b, e := portable.BuildNon2xxACK(restored, restoredResponse)
						if e != nil || !a.EqualWire(b) {
							t.Fatal("ACK changed", e)
						}
					}
					if method == sip.INVITE && status > 100 && status < 300 {
						for _, role := range []portable.Role{portable.UAC, portable.UAS} {
							a, e := portable.NewDialog(role, original, before, portable.DefaultTimers())
							if e != nil {
								t.Fatal(e)
							}
							b, e := portable.NewDialog(role, restored, restoredResponse, portable.DefaultTimers())
							if e != nil {
								t.Fatal(e)
							}
							aj, ae := json.Marshal(a)
							bj, be := json.Marshal(b)
							if ae != nil || be != nil || !bytes.Equal(aj, bj) {
								t.Fatal("dialog changed", ae, be)
							}
						}
					}
				}
			})
		}
	}
}
func TestSelectedOpaqueAndMemoIdentity(t *testing.T) {
	m := fixture(t, sip.INVITE, "tls", 0)
	data, err := Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	lazy, err := DecodeOpaque(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ExactWire(lazy), m.WireBytes()) {
		t.Fatal("wire mismatch")
	}
	memo, err := NewMemo(8)
	if err != nil {
		t.Fatal(err)
	}
	a, err := memo.Adopt(m, "admitted")
	if err != nil {
		t.Fatal(err)
	}
	b, err := memo.Decode(data, "admitted")
	if err != nil || a != b {
		t.Fatal("same admitted identity not reused", err)
	}
	other, err := memo.Decode(data, "stored")
	if err != nil || other == a {
		t.Fatal("different provenance reused", err)
	}
	metadata := portable.RestoreMessage(m.WireBytes(), "tcp", m.Source(), m.Destination())
	c, err := memo.Adopt(metadata, "admitted")
	if err != nil || c == a {
		t.Fatal("different transport reused", err)
	}
	for _, variant := range []portable.Message{
		portable.RestoreMessage(m.WireBytes(), m.Transport(), "changed-source", m.Destination()),
		portable.RestoreMessage(m.WireBytes(), m.Transport(), m.Source(), "changed-destination"),
		portable.RestoreMessage(append(m.WireBytes(), 'x'), m.Transport(), m.Source(), m.Destination()),
	} {
		distinct, err := memo.Adopt(variant, "admitted")
		if err != nil || distinct == a {
			t.Fatal("different message identity reused", err)
		}
	}
	owned := ExactWire(lazy)
	owned[0] ^= 1
	if !lazy.EqualWire(m) {
		t.Fatal("writer bytes alias the immutable holder")
	}
	bad := portable.RestoreMessage([]byte("not SIP"), "tls", "", "")
	raw, err := Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	opaque, err := DecodeOpaque(raw)
	if err != nil || string(ExactWire(opaque)) != "not SIP" {
		t.Fatal("opaque decode parsed SIP", err)
	}
	if _, err = opaque.Facts(); err == nil {
		t.Fatal("invalid SIP consumed")
	}
	if _, err = DecodeOpaque(append(bytes.Clone(data), 0)); err == nil {
		t.Fatal("malformed protobuf accepted")
	}
	p := &storagepb.PortableMessage{Wire: bytes.Repeat([]byte{'a'}, maxFieldBytes+1)}
	tooLarge, err := storagecodec.Marshal(storagecodec.SelectedMessage, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeOpaque(tooLarge); err == nil {
		t.Fatal("oversize wire accepted")
	}
}
func TestSelectedPreservesNoncanonicalAndValidationModes(t *testing.T) {
	original := fixture(t, sip.INVITE, "tls", 0)
	wire := original.WireBytes()
	wire = bytes.Replace(wire, []byte("INVITE sip:"), []byte("BYE sip:"), 1)
	wire = bytes.Replace(wire, []byte("Content-Length:"), []byte("X-Order: one\r\nX-Order: two\r\nContent-Length:"), 1)
	m := portable.RestoreMessage(wire, "tls", "192.0.2.10:1000", "192.0.2.20:5061")
	r := cold(t, m)
	a, e := m.ObservedFacts()
	if e != nil {
		t.Fatal(e)
	}
	b, e := r.ObservedFacts()
	if e != nil || a != b {
		t.Fatal("observed facts changed", e)
	}
	if _, e = m.Facts(); e == nil {
		t.Fatal("fixture must fail native validation")
	}
	if _, e = r.Facts(); e == nil {
		t.Fatal("native rejection changed")
	}
	headers, e := r.ObservedHeaders()
	if e != nil {
		t.Fatal(e)
	}
	var order []string
	for _, h := range headers {
		if strings.EqualFold(h.Name, "X-Order") {
			order = append(order, h.Value)
		}
	}
	if !reflect.DeepEqual(order, []string{"one", "two"}) {
		t.Fatal("header order changed")
	}
	rawWire := bytes.Replace(wire, []byte("BYE sip:phone@example.test;user=phone?Subject=test"), []byte("BYE tel:+15551234567"), 1)
	raw := cold(t, portable.RestoreMessage(rawWire, "tls", "", ""))
	if !bytes.Equal(raw.WireBytes(), rawWire) {
		t.Fatal("opaque URI wire changed")
	}
	if _, e = raw.ObservedFacts(); e != nil {
		t.Fatal("opaque URI no longer observed", e)
	}
}

var benchmarkMessage portable.Message
var benchmarkBytes []byte

func BenchmarkSelectedWireRecord(b *testing.B) {
	for _, tc := range []struct {
		name   string
		method sip.RequestMethod
		body   int
	}{{"REGISTER", sip.REGISTER, 0}, {"INVITE-1200", sip.INVITE, 1200}, {"NOTIFY-2048", sip.NOTIFY, 2048}} {
		b.Run(tc.name, func(b *testing.B) {
			m := fixture(b, tc.method, "tls", tc.body)
			data, err := Marshal(m)
			if err != nil {
				b.Fatal(err)
			}
			b.Logf("wire=%d record=%d", m.WireLen(), len(data))
			b.Run("Encode", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, e := Marshal(m)
					if e != nil {
						b.Fatal(e)
					}
					benchmarkBytes = v
				}
			})
			b.Run("DecodeOpaque", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, e := DecodeOpaque(data)
					if e != nil {
						b.Fatal(e)
					}
					benchmarkMessage = v
				}
			})
			b.Run("DecodeAndConsume", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, e := DecodeOpaque(data)
					if e != nil {
						b.Fatal(e)
					}
					if _, e = v.Facts(); e != nil {
						b.Fatal(e)
					}
					benchmarkMessage = v
				}
			})
			b.Run("DecodeAndWrite", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, e := DecodeOpaque(data)
					if e != nil {
						b.Fatal(e)
					}
					benchmarkBytes = ExactWire(v)
				}
			})
			b.Run("HolderHashAndWrite", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, e := DecodeOpaque(data)
					if e != nil {
						b.Fatal(e)
					}
					benchmarkWriterHash = v.WireHash()
					benchmarkBytes = ExactWire(v)
				}
			})
			b.Run("OwnedWireHashAndWrite", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v, e := DecodeWire(data)
					if e != nil {
						b.Fatal(e)
					}
					benchmarkWriterHash = sha256.Sum256(v)
					benchmarkBytes = v
				}
			})
		})
	}
}
