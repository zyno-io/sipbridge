package control

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestControlEnvelopeCarriesSupportedLargeSipPairAndDescriptions(t *testing.T) {
	diagnostics := strings.Repeat("m", 4096)
	head := "INVITE sip:peer@example.test SIP/2.0\r\nVia: SIP/2.0/TLS peer.example.test;branch=z9hG4bK-large\r\nFrom: <sip:a@example.test>;tag=a\r\nTo: <sip:b@example.test>;tag=b\r\nCall-ID: large-call\r\nCSeq: 2 INVITE\r\nContent-Type: application/sdp\r\nX-Diagnostics: " + diagnostics + "\r\n"
	body := bytes.Repeat([]byte("s"), 64*1024-len(head)-28)
	wire := append([]byte(head+fmt.Sprintf("Content-Length: %d\r\n\r\n", len(body))), body...)
	if len(wire) > 64*1024 {
		t.Fatal("fixture exceeds supported SIP framing")
	}
	message := &pb.SipMessage{Wire: wire, Body: body, Method: "INVITE", Headers: []*pb.Header{{Name: "x-diagnostics", Value: diagnostics}}}
	selectors := make([]string, 256)
	for i := range selectors {
		selectors[i] = "ctx:" + strings.Repeat("a", 64)
	}
	envelope := &pb.Envelope{CorrelationId: "large-native-result", Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED, Request: message, Response: message, Snapshot: &pb.ContextSnapshot{LocalSdp: body, RemoteSdp: body, Selectors: selectors}}}}
	encoded, err := proto.Marshal(envelope)
	if err != nil || len(encoded) > MaxEnvelope {
		t.Fatalf("supported SIP pair exceeds control framing: %d %v", len(encoded), err)
	}
	if len(encoded) <= 256*1024 {
		t.Fatal("fixture does not cover parsed metadata expansion")
	}
	decoded := &pb.Envelope{}
	err = proto.Unmarshal(encoded, decoded)
	if err != nil || !bytes.Equal(decoded.GetResult().GetSnapshot().GetRemoteSdp(), body) {
		t.Fatalf("large native result did not round trip: %v", err)
	}
}
