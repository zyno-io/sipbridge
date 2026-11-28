package capture

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestCaptureAdmissionAttributesTheOriginalObservation(t *testing.T) {
	exporter, err := New("http://localhost/loki/api/v1/push", "ingress", "epoch")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	wire := []byte("INVITE sip:a SIP/2.0\r\nCall-ID: peer-controlled\r\n\r\n")
	parent, finish := exporter.Begin(context.Background(), Observation{Timestamp: now, Wire: wire})
	Hold(parent, "context:incarnation", now.Add(time.Hour), nil)
	finish()
	exporter.Bind("context:wrong-incarnation", "unexpected")
	select {
	case <-exporter.queue:
		t.Fatal("capture released by a different incarnation")
	default:
	}
	wire[0] = 'X'
	exporter.Bind("context:incarnation", "019ad09a-97da-7c55-8d89-cd38756c11d5")
	capture := <-exporter.queue
	finish()
	exporter.Bind("context:incarnation", "another")
	if capture.NativeCallID != "019ad09a-97da-7c55-8d89-cd38756c11d5" || !capture.Timestamp.Equal(now) || capture.Wire[0] != 'I' || len(exporter.queue) != 0 {
		t.Fatalf("wrong original capture: %+v", capture)
	}
}

func TestCaptureReadsRemoteAdmissionAfterHold(t *testing.T) {
	exporter, err := New("http://localhost/loki/api/v1/push", "ingress", "epoch")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	parent, finish := exporter.Begin(context.Background(), Observation{Timestamp: now, Wire: []byte("INVITE")})
	const id = "019ad09a-97da-7c55-8d89-cd38756c11d5"
	Hold(parent, "context:incarnation", now.Add(10*time.Millisecond), func(ctx context.Context) string {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("capture lookup has no deadline")
		}
		return id
	})
	finish()
	select {
	case observation := <-exporter.queue:
		if observation.NativeCallID != id || !observation.Timestamp.Equal(now) {
			t.Fatalf("wrong capture after remote admission: %+v", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("capture did not flush")
	}
}

func TestAuthenticatedCaptureIncludesAttributionWithoutCertificateOrSignature(t *testing.T) {
	observed := Observation{
		Timestamp: time.Unix(1785153631, 0),
		Wire:      []byte("NOTIFY sip:a SIP/2.0\r\nCall-ID: wire-id\r\nX-Zyno-Event-Id: injected\r\n\r\n"),
		Attribution: Attribution{
			BLFDeliveryID:    "019ad09a-97da-7c55-8d89-cd38756c11d5",
			BLFNativeCallIDs: []string{"019ad09a-97da-7c55-8d89-cd38756c11d6"},
		},
	}
	body, err := encode([]queued{{Observation: observed, Sequence: 2}}, "instance", "epoch", true)
	if err != nil {
		t.Fatal(err)
	}
	var p push
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	entry := p.Streams[0].Values[0]
	timestamp, record := entry[0].(string), entry[1].(string)
	metadata := entry[2].(map[string]interface{})
	if timestamp != "1785153631000000000" || p.Streams[0].Stream["capture_contract"] != "v2" ||
		metadata["capture_cert"] != nil || metadata["capture_signature"] != nil {
		t.Fatalf("unexpected capture envelope: %s %v", timestamp, metadata)
	}
	var decoded line
	if err := json.Unmarshal([]byte(record), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Version != 2 || decoded.BLFDeliveryID != observed.BLFDeliveryID {
		t.Fatalf("unexpected v2 capture body: %+v", decoded)
	}
	if metadata["blf_delivery_id"] != observed.BLFDeliveryID || metadata["delivery_id"] != nil || metadata["native_call_id"] != nil {
		t.Fatalf("wrong attributions: %v", metadata)
	}
}
