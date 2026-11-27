package capture

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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
	Hold(parent, "context:incarnation", now.Add(time.Hour))
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

func TestSignedCaptureBindsTimestampAndExactAttribution(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	observed := Observation{
		Timestamp: time.Unix(1785153631, 0),
		Wire:      []byte("NOTIFY sip:a SIP/2.0\r\nCall-ID: wire-id\r\nX-Zyno-Event-Id: injected\r\n\r\n"),
		Attribution: Attribution{
			BLFDeliveryID:    "019ad09a-97da-7c55-8d89-cd38756c11d5",
			BLFNativeCallIDs: []string{"019ad09a-97da-7c55-8d89-cd38756c11d6"},
		},
	}
	body, err := encodeSigned([]queued{{Observation: observed, Sequence: 2}}, "instance", "epoch", &captureSigning{leaf: "certificate", signer: key})
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
	sig, err := base64.StdEncoding.DecodeString(metadata["capture_signature"].(string))
	if err != nil {
		t.Fatal(err)
	}
	verify := func(ts, content string) bool {
		hash := sha256.Sum256([]byte("sipbridge-capture-v2\x00" + ts + "\x00" + content))
		return ecdsa.VerifyASN1(&key.PublicKey, hash[:], sig)
	}
	if !verify(timestamp, record) || verify(timestamp, record+" ") || verify(timestamp+"1", record) {
		t.Fatal("signature did not bind the original timestamp and message")
	}
	if metadata["blf_delivery_id"] != observed.BLFDeliveryID || metadata["delivery_id"] != nil || metadata["native_call_id"] != nil {
		t.Fatalf("wrong attributions: %v", metadata)
	}
}
