package engine

import (
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestNativeClientStartUsesProvidedIdentityAndNativeTimers(t *testing.T) {
	open := &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "udp", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}, Body: []byte("offer")}
	ref := &pb.ContextRef{Id: "context", State: &pb.StateRef{Incarnation: "incarnation", CallId: "client-call@example.test"}}
	flow := &pb.FlowRef{Transport: "udp"}
	tx, effects, err := nativePrepareClientStart(open, ref, flow, "fixed-tag", "z9hG4bK-fixed", "<sip:service@192.0.2.1:5060;transport=udp>", "192.0.2.1:5060", time.UnixMilli(1000))
	if err != nil {
		t.Fatal(err)
	}
	facts, err := tx.Origin.Facts()
	if err != nil {
		t.Fatal(err)
	}
	if tx.Family != portable.InviteClient || !facts.IsRequest || facts.Method != sip.INVITE || facts.CallID != "client-call@example.test" || facts.FromTag != "fixed-tag" || facts.ViaBranch != "z9hG4bK-fixed" || facts.CSeq != 1 || facts.BodyLength != 5 {
		t.Fatal("client request identity or body changed", facts)
	}
	if !tx.Timers[portable.TimerA].Active || !tx.Timers[portable.TimerB].Active {
		t.Fatal("UDP INVITE timers missing")
	}
	sends := 0
	for _, effect := range effects {
		if effect.Kind == portable.Send && effect.Message != nil && effect.Message.EqualWire(tx.Origin) {
			sends++
		}
	}
	if sends != 1 {
		t.Fatalf("native Start emitted %d exact sends", sends)
	}
	if _, _, err = nativePrepareClientStart(open, ref, &pb.FlowRef{Transport: "tls"}, "fixed-tag", "z9hG4bK-fixed", "<sip:service@192.0.2.1:5060;transport=udp>", "192.0.2.1:5060", time.UnixMilli(1000)); err == nil {
		t.Fatal("mismatched flow accepted")
	}
}
