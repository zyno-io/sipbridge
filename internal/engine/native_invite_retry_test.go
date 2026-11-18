package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func nativeInviteRetryFixture(t *testing.T, key nativeUDPRetryKey, transport string, success bool) (nativeInviteRetryProof, portable.Transaction, portable.Dialog) {
	t.Helper()
	wire := strings.ReplaceAll(string(stateTestRequest("INVITE", 1, "", "retry").Wire), "SIP/2.0/TLS", "SIP/2.0/"+strings.ToUpper(transport))
	origin, err := portable.ParseMessage([]byte(wire), transport, "", "")
	if err != nil {
		t.Fatal(err)
	}
	profile := portable.DefaultTimers()
	profile.T1, profile.T2 = 25, 100
	tx, err := portable.NewTransaction(portable.InviteServer, origin, profile, "local")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.Start, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	status := 486
	if success {
		status = 200
	}
	final, err := portable.BuildResponseWithHeaders(origin, status, reason(uint32(status)), nil, "local", "sip:edge@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err = portable.StepTransaction(tx, portable.Input{Kind: portable.SendResponse, Now: now, Message: final})
	if err != nil {
		t.Fatal(err)
	}
	var dialog portable.Dialog
	if success {
		dialog, err = portable.NewDialog(portable.UAS, origin, final, profile)
		if err != nil {
			t.Fatal(err)
		}
		dialog, _, err = portable.StepDialog(dialog, portable.DialogInput{Kind: portable.DialogSendResponse, Now: now, Message: final})
		if err != nil {
			t.Fatal(err)
		}
	}
	process := &pb.ProcessRef{InstanceId: "worker", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: process, ConnectionId: "connection", SourceIp: "192.0.2.1", SourcePort: 5060, Listener: "sip.example.test:5060", Transport: transport}
	effect := &pb.StateEffectRef{Context: &pb.ContextRef{Id: key.Context, State: &pb.StateRef{Incarnation: key.Incarnation}}, Id: "effect", AttemptId: "attempt"}
	auth := &pb.EffectAuthorization{Effect: effect, AuthorityClass: pb.EffectAuthorization_RESPONSE, DeadlineMs: uint64(time.Now().Add(time.Second).UnixMilli())}
	write := &pb.FlowWrite{Flow: flow, Effect: effect, Permit: &pb.SendPermit{Flow: flow, Authorization: auth}}
	output := &nativepb.Output{Write: write, Procedure: key.Procedure, Method: "INVITE", Status: uint32(status)}
	if success {
		output.Dialog = "r:dialog"
	}
	metadata, err := encodeNativeOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := nativeOutputRef(write)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := final.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
	if err != nil {
		t.Fatal(err)
	}
	physicalWire := physical.WireBytes()
	if bytes.Equal(final.WireBytes(), physicalWire) {
		t.Fatal("fixture did not exercise logical-to-physical Via normalization")
	}
	proof := rediscore.OutputInspection{State: rediscore.OutputWritten, Settled: true, Metadata: metadata, MetadataField: "r:output", MessageField: "r:wire", WireHash: sha256.Sum256(physicalWire)}
	return nativeInviteRetryProof{Key: key, Attempt: ref, Proof: proof, Captured: time.Now(), Flow: flow, Wire: physicalWire}, tx, dialog
}

func TestNativeInviteRetryGAndDialogUseExactPhysicalWire(t *testing.T) {
	for _, test := range []struct {
		name, transport string
		success, uac    bool
	}{{"G", "udp", false, false}, {"2xx-udp", "udp", true, false}, {"2xx-tcp", "tcp", true, false}, {"2xx-tls", "tls", true, false}, {"2xx-uac-reinvite", "tls", true, true}} {
		t.Run(test.name, func(t *testing.T) {
			type write struct {
				wire    []byte
				address *byte
				until   time.Time
			}
			writes := make(chan write, 4)
			manager, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 2, Hints: 2, Writers: 2, Bytes: 1 << 20}, func(_ context.Context, _ *pb.FlowRef, wire []byte, until time.Time) error {
				writes <- write{bytes.Clone(wire), &wire[0], until}
				return nil
			})
			key := udpKey(test.name)
			proof, tx, dialog := nativeInviteRetryFixture(t, key, test.transport, test.success)
			if test.uac {
				dialog.Role = portable.UAC
				local := *dialog.RemoteInvite
				dialog.LocalInvite = &local
				old := key
				old.Procedure = "r:previous-invite"
				manager.Stop(old)
			}
			if test.success {
				err := manager.ArmInviteDialog(nativeInviteDialogRetrySeed{nativeInviteRetryProof: proof, Dialog: dialog})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				err := manager.ArmInviteServer(nativeInviteServerRetrySeed{nativeInviteRetryProof: proof, Transaction: tx})
				if err != nil {
					t.Fatal(err)
				}
			}
			var first, second write
			select {
			case first = <-writes:
			case <-time.After(time.Second):
				t.Fatal("first retry missing")
			}
			select {
			case second = <-writes:
			case <-time.After(time.Second):
				t.Fatal("second retry missing")
			}
			if !bytes.Equal(first.wire, proof.Wire) || !bytes.Equal(second.wire, proof.Wire) || first.address != second.address {
				t.Fatal("retry changed or recopied physical wire")
			}
			var original int64
			if test.success {
				original = dialog.Timers[portable.TimerACK].At
			} else {
				original = tx.Timers[portable.TimerH].At
			}
			if first.until.UnixMilli() > original || second.until.UnixMilli() > original {
				t.Fatal("retry exceeded original cutoff")
			}
			manager.Stop(key)
		})
	}
}

func TestNativeInviteRetryProofAndStopFences(t *testing.T) {
	manager, _ := udpRetryManager(t, nativeUDPRetryLimits{Entries: 4, Hints: 4, Writers: 2, Bytes: 1 << 20}, func(context.Context, *pb.FlowRef, []byte, time.Time) error { t.Error("fenced retry wrote"); return nil })
	key := udpKey("stopped")
	proof, tx, _ := nativeInviteRetryFixture(t, key, "udp", false)
	manager.Stop(key)
	if err := manager.ArmInviteServer(nativeInviteServerRetrySeed{nativeInviteRetryProof: proof, Transaction: tx}); err == nil {
		t.Fatal("ACK-before-arm did not stop G")
	}
	key = udpKey("stopped-dialog")
	proof, _, dialog := nativeInviteRetryFixture(t, key, "tls", true)
	manager.Stop(key)
	if err := manager.ArmInviteDialog(nativeInviteDialogRetrySeed{nativeInviteRetryProof: proof, Dialog: dialog}); err == nil {
		t.Fatal("ACK-before-arm did not stop reliable 2xx")
	}
	key = udpKey("unknown")
	proof, _, dialog = nativeInviteRetryFixture(t, key, "tls", true)
	proof.Proof.State = rediscore.OutputUnknown
	if err := manager.ArmInviteDialog(nativeInviteDialogRetrySeed{nativeInviteRetryProof: proof, Dialog: dialog}); err == nil {
		t.Fatal("uncertain proof armed 2xx")
	}
	key = udpKey("wire")
	proof, _, dialog = nativeInviteRetryFixture(t, key, "tls", true)
	proof.Wire[0] ^= 1
	if err := manager.ArmInviteDialog(nativeInviteDialogRetrySeed{nativeInviteRetryProof: proof, Dialog: dialog}); err == nil {
		t.Fatal("different physical wire armed 2xx")
	}
	key = udpKey("stale")
	proof, _, dialog = nativeInviteRetryFixture(t, key, "tls", true)
	proof.Captured = time.Now().Add(-3 * time.Second)
	if err := manager.ArmInviteDialog(nativeInviteDialogRetrySeed{nativeInviteRetryProof: proof, Dialog: dialog}); err == nil {
		t.Fatal("expired arm window accepted")
	}
}
