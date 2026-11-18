package engine

import (
	"context"
	"strings"
	"testing"

	pb "sipbridge/internal/controlpb"
)

func TestNativeCompletionSkipsBusinessMethodsBeforeWorkAdmission(t *testing.T) {
	f := newNativeOwnerFixture(t)
	f.engine.work = make(chan struct{}, 1)
	f.engine.work <- struct{}{}

	for _, method := range []string{"REGISTER", "SUBSCRIBE", "NOTIFY"} {
		t.Run(method, func(t *testing.T) {
			received := stateTestRequest(method, 2, "local-tag", "business-"+strings.ToLower(method))
			handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, received)
			if err != nil || handled {
				t.Fatalf("business request entered native completion: handled=%v err=%v", handled, err)
			}
			if len(f.engine.work) != 1 {
				t.Fatal("business request changed the completion work slot")
			}
		})
	}
}

func TestNativeCompletionFastNegativeUsesObservedWireAndPreservesResponsePath(t *testing.T) {
	f := newNativeOwnerFixture(t)
	f.engine.work = make(chan struct{}, 1)
	f.engine.work <- struct{}{}

	forged := stateTestRequest("BYE", 3, "local-tag", "forged-method")
	forged.Method = "SUBSCRIBE"
	if handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, forged); err == nil || handled || !strings.Contains(err.Error(), "native completion capacity") {
		t.Fatalf("forged protobuf method bypassed observed-wire completion: handled=%v err=%v", handled, err)
	}
	if len(f.engine.work) != 1 {
		t.Fatal("capacity refusal changed the occupied completion slot")
	}

	response, err := Observe([]byte("SIP/2.0 200 OK\r\nVia: SIP/2.0/TLS phone.example.test:5061;branch=z9hG4bKresponse;rport\r\nFrom: <sip:alice@example.test>;tag=remote-tag\r\nTo: <sip:service@example.test>;tag=local-tag\r\nCall-ID: redis-native-call@example.test\r\nCSeq: 2 REGISTER\r\nContent-Length: 0\r\n\r\n"), false)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, response); err == nil || handled || !strings.Contains(err.Error(), "native completion capacity") {
		t.Fatalf("response fast-negative skipped completion capacity: handled=%v err=%v", handled, err)
	}
	if len(f.engine.work) != 1 {
		t.Fatal("response capacity refusal changed the occupied completion slot")
	}

	invalid := &pb.SipMessage{Wire: []byte("not a SIP message")}
	if handled, err := f.engine.TryNativeCompletion(context.Background(), f.flow, invalid); err == nil || handled || strings.Contains(err.Error(), "native completion capacity") {
		t.Fatalf("invalid wire skipped native parser: handled=%v err=%v", handled, err)
	}
	if len(f.engine.work) != 1 {
		t.Fatal("invalid wire changed the occupied completion slot")
	}
}
