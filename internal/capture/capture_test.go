package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEncodingPreservesTextAndCorrelations(t *testing.T) {
	wire := []byte("NOTIFY sip:test@example.test SIP/2.0\r\ni: CaseSensitive\r\nX-Zyno-Event-Id: delivery\r\nContent-Length: 19\r\n\r\nGrüße \"SIP\"\r\n\x00<&>")
	now := time.Unix(123, 456)
	encoded, err := encode([]queued{{Observation: Observation{Timestamp: now, Direction: "out", Transport: "tls", Source: "192.0.2.1:5061", Destination: "192.0.2.2:1000", Outcome: "written", Wire: wire}, Sequence: 1}}, "instance", "epoch")
	if err != nil {
		t.Fatal(err)
	}
	var p push
	if err := json.Unmarshal(encoded, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Streams) != 1 || p.Streams[0].Stream["sip_method"] != "NOTIFY" {
		t.Fatalf("unexpected stream: %+v", p.Streams)
	}
	if p.Streams[0].Stream["sip_bucket"] != "44" {
		t.Fatalf("unexpected version-1 hash bucket: %q", p.Streams[0].Stream["sip_bucket"])
	}
	entry := p.Streams[0].Values[0]
	if entry[0] != "123000000456" {
		t.Fatalf("timestamp: %v", entry[0])
	}
	var body line
	if err := json.Unmarshal([]byte(entry[1].(string)), &body); err != nil {
		t.Fatal(err)
	}
	if body.Wire != string(wire) || body.WireBytes != len(wire) {
		t.Fatal("wire text or original byte count changed")
	}
	metadata := entry[2].(map[string]interface{})
	if metadata["call_id"] != "CaseSensitive" || metadata["delivery_id"] != "delivery" || metadata["method"] != "NOTIFY" || metadata["src_ip"] != "192.0.2.1" || metadata["capture_id"] != "epoch:1" {
		t.Fatalf("metadata: %+v", metadata)
	}
}

func TestEncodingReplacesNonUTF8BodyWithOriginalByteCount(t *testing.T) {
	for _, body := range [][]byte{{0xff}, {'a', 0xff, 0}, bytes.Repeat([]byte{0xff}, 1234)} {
		head := fmt.Sprintf("MESSAGE sip:test SIP/2.0\r\nCall-ID: binary\r\nContent-Length: %d\r\n\r\n", len(body))
		wire := append([]byte(head), body...)
		original := bytes.Clone(wire)
		encoded, err := encode([]queued{{Observation: Observation{Wire: wire, Direction: "out", Outcome: "written", BytesWritten: len(wire)}}}, "instance", "epoch")
		if err != nil {
			t.Fatal(err)
		}
		var payload push
		if err := json.Unmarshal(encoded, &payload); err != nil {
			t.Fatal(err)
		}
		var record line
		if err := json.Unmarshal([]byte(payload.Streams[0].Values[0][1].(string)), &record); err != nil {
			t.Fatal(err)
		}
		want := head + fmt.Sprintf("[%d bytes]", len(body))
		if record.Wire != want || record.WireBytes != len(wire) || record.BytesWritten != len(wire) {
			t.Fatalf("unexpected binary-body capture: %+v", record)
		}
		if !bytes.Equal(wire, original) {
			t.Fatal("capture changed the signaling bytes")
		}
	}
}

func TestAmbiguousIdentityIsNotIndexed(t *testing.T) {
	wire := []byte("INVITE sip:x SIP/2.0\r\nCall-ID: one\r\ni: two\r\n\r\n")
	if id, _, _ := identity(wire); id != "" {
		t.Fatalf("accepted ambiguous call ID: %q", id)
	}
}

func TestCompactCallIDWithHeaderWhitespace(t *testing.T) {
	wire := []byte("OPTIONS sip:x SIP/2.0\r\ni : CaseSensitive\r\n\r\n")
	if id, _, _ := identity(wire); id != "CaseSensitive" {
		t.Fatalf("unexpected call ID %q", id)
	}
}

func TestFoldedCorrelationsMatchTheSIPParser(t *testing.T) {
	wire := []byte("NOTIFY sip:x SIP/2.0\r\nCall-ID:\r\n CaseSensitive\r\nX-Zyno-Event-Id:\r\n delivery\r\n\r\n")
	id, delivery, method := identity(wire)
	if id != "CaseSensitive" || delivery != "delivery" || method != "NOTIFY" {
		t.Fatalf("unexpected unfolded identities: %q %q %q", id, delivery, method)
	}
}

func TestRequestAndResponseMethodsUseTheSameBoundedLabel(t *testing.T) {
	for _, method := range []string{"INVITE", "ACK", "BYE", "CANCEL", "OPTIONS", "REGISTER", "PRACK", "SUBSCRIBE", "NOTIFY", "PUBLISH", "INFO", "REFER", "MESSAGE", "UPDATE"} {
		for _, wire := range []string{
			method + " sip:x SIP/2.0\r\nCall-ID: a\r\nCSeq: 1 " + method + "\r\n\r\n",
			"SIP/2.0 200 OK\r\nCall-ID: a\r\nCSeq: 1 " + method + "\r\n\r\n",
		} {
			_, _, got := identity([]byte(wire))
			if got != method || indexedMethod(got) != method {
				t.Fatalf("method %s produced %q / %q", method, got, indexedMethod(got))
			}
		}
	}
}

func TestEncodedMethodLabelGroupsRequestsAndResponses(t *testing.T) {
	request := []byte("INVITE sip:x SIP/2.0\r\nCall-ID: a\r\nCSeq: 1 INVITE\r\n\r\n")
	response := []byte("SIP/2.0 200 OK\r\nCall-ID: a\r\nCSeq: 1 INVITE\r\n\r\n")
	items := []queued{{Observation: Observation{Timestamp: time.Unix(1, 0), Wire: request}}, {Observation: Observation{Timestamp: time.Unix(1, 1), Wire: response}}}
	encoded, err := encode(items, "instance", "epoch")
	if err != nil {
		t.Fatal(err)
	}
	var result push
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Streams) != 1 || result.Streams[0].Stream["sip_method"] != "INVITE" || len(result.Streams[0].Values) != 2 {
		t.Fatalf("request and response were not grouped by method: %+v", result.Streams)
	}
}

func TestMethodExtractionRejectsAmbiguousResponseAndKeepsExtensions(t *testing.T) {
	cases := []struct{ wire, method, label string }{
		{"SIP/2.0 200 OK\r\nCSeq: 1\r\n INVITE\r\n\r\n", "INVITE", "INVITE"},
		{"SIP/2.0 200 OK\r\nCSeq: 1 INVITE\r\nCSeq: 1 BYE\r\n\r\n", "", "OTHER"},
		{"SIP/2.0 200 OK\r\nCSeq: invalid INVITE\r\n\r\n", "", "OTHER"},
		{"SIP/2.0 200 OK\r\nCSeq: +1 INVITE\r\n\r\n", "", "OTHER"},
		{"SIP/2.0 200 OK\r\n\r\nCSeq: 1 INVITE", "", "OTHER"},
		{"FOO sip:x SIP/2.0\r\nCSeq: 1 FOO\r\n\r\n", "FOO", "OTHER"},
		{"SIP/2.0 200 OK\r\nCSeq: 1 FOO\r\n\r\n", "FOO", "OTHER"},
		{"invite sip:x SIP/2.0\r\n\r\n", "invite", "OTHER"},
		{"INVITE sip:x SIP/2.0\r\nCSeq: 1 BYE\r\n\r\n", "INVITE", "INVITE"},
		{"INVITE/bad sip:x SIP/2.0\r\n\r\n", "", "OTHER"},
	}
	for _, c := range cases {
		_, _, got := identity([]byte(c.wire))
		if got != c.method || indexedMethod(got) != c.label {
			t.Errorf("%q: got %q / %q, want %q / %q", c.wire, got, indexedMethod(got), c.method, c.label)
		}
	}
}

func TestCollectorFailureDoesNotBlockSubmit(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	exporter, err := New(server.URL+"/loki/api/v1/push", "instance", "epoch")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { exporter.Run(ctx); close(done) }()
	msg := Observation{Timestamp: time.Now(), Wire: []byte("OPTIONS sip:x SIP/2.0\r\nCall-ID: a\r\n\r\n")}
	exporter.Submit(msg)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("collector not contacted")
	}
	for n := 0; n < queueSize+10; n++ {
		exporter.Submit(msg)
	}
	dropped, _ := exporter.Stats()
	if dropped == 0 {
		t.Fatal("expected bounded queue overflow")
	}
	close(release)
	cancel()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("exporter did not stop")
	}
}

func TestCollectorURLRejectsCredentials(t *testing.T) {
	_, err := New("http://user:password@example.test/loki/api/v1/push", "i", "e")
	if err == nil || strings.Contains(err.Error(), "password") {
		t.Fatal("expected safe URL rejection")
	}
}

func BenchmarkEncodeBatch(b *testing.B) {
	wire := []byte("INVITE sip:x SIP/2.0\r\nCall-ID: benchmark-dialog\r\nContent-Length: 1024\r\n\r\n" + strings.Repeat("a", 1024))
	items := make([]queued, batchSize)
	for index := range items {
		items[index] = queued{Observation: Observation{Timestamp: time.Unix(1, int64(index)), Direction: "out", Transport: "udp", Source: "192.0.2.1:5060", Destination: "192.0.2.2:5060", Outcome: "written", BytesWritten: len(wire), Wire: wire}, Sequence: uint64(index + 1)}
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire) * len(items)))
	for range b.N {
		if _, err := encode(items, "benchmark", "epoch"); err != nil {
			b.Fatal(err)
		}
	}
}
