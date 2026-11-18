package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

func TestUDPInviteRetryStopBeforeArmWithoutParsing(t *testing.T) {
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error {
		t.Error("stopped INVITE retransmitted")
		return nil
	})
	seed := udpClientRetrySeed(t, udpKey("invite-stop"), "INVITE", portable.InviteClient)
	before := seed.Transaction.Origin.RetainedBytes()
	m.Stop(seed.Key)
	if err := m.Arm(seed); err == nil {
		t.Fatal("response-before-arm did not suppress Timer A")
	}
	if seed.Transaction.Origin.RetainedBytes() != before {
		t.Fatal("response-before-arm parsed the retained origin")
	}
	unknown := udpClientRetrySeed(t, udpKey("invite-unknown"), "INVITE", portable.InviteClient)
	unknown.Proof.State = rediscore.OutputUnknown
	if err := m.Arm(unknown); err == nil {
		t.Fatal("UNKNOWN INVITE armed retransmission")
	}
}

func TestUDPInviteRetryUsesNativeBackoffAndOneWire(t *testing.T) {
	type write struct {
		at   time.Time
		wire []byte
		ptr  *byte
	}
	writes := make(chan write, 4)
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(_ context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		writes <- write{time.Now(), bytes.Clone(wire), &wire[0]}
		return nil
	})
	seed := udpClientRetrySeed(t, udpKey("invite-backoff"), "INVITE", portable.InviteClient)
	original := seed.Transaction.Origin.WireBytes()
	deadline := seed.Transaction.Timers[portable.TimerB]
	if err := m.Arm(seed); err != nil {
		t.Fatal(err)
	}
	var got [3]write
	for i := range got {
		select {
		case got[i] = <-writes:
		case <-time.After(time.Second):
			t.Fatal("Timer A retransmission missing", i)
		}
		if !bytes.Equal(got[i].wire, original) || i > 0 && got[i].ptr != got[0].ptr {
			t.Fatal("Timer A changed or repeatedly copied its exact wire")
		}
	}
	if got[1].at.Sub(got[0].at) < 40*time.Millisecond || got[2].at.Sub(got[1].at) < 80*time.Millisecond {
		t.Fatal("Timer A did not retain native exponential backoff")
	}
	m.mu.Lock()
	entry := m.entries[seed.Key]
	unchanged := entry != nil && entry.tick == portable.TimerA && entry.terminal == portable.TimerB && entry.tx.Timers[portable.TimerB] == deadline
	m.mu.Unlock()
	if !unchanged {
		t.Fatal("retry extended or replaced original Timer B")
	}
	m.Stop(seed.Key)
}

func TestUDPInviteRetryRejectsAlreadyProceeding(t *testing.T) {
	m, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error {
		t.Error("proceeding INVITE retransmitted")
		return nil
	})
	seed := udpClientRetrySeed(t, udpKey("invite-proceeding"), "INVITE", portable.InviteClient)
	response, err := portable.BuildResponseWithHeaders(seed.Transaction.Origin, 180, "Ringing", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	seed.Transaction, _, err = portable.StepTransaction(seed.Transaction, portable.Input{Kind: portable.ReceiveResponse, Now: time.Now().UnixMilli(), Message: response})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Arm(seed); err == nil {
		t.Fatal("proceeding INVITE restarted Timer A")
	}
}
