package engine

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func stateCountEvents(t *testing.T, r *stateReduction, kind pb.DialogEvent_Kind) int {
	t.Helper()
	n := 0
	for _, effect := range r.payload.Effects {
		var body stateEngineEffect
		if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if event := body.Envelope.GetDialogEvent(); event != nil && event.Kind == kind {
			n++
		}
	}
	return n
}

func TestStateInitialInviteTimeoutReportsFailureAndRetainsLateCleanup(t *testing.T) {
	for _, scenario := range []string{"timer-b", "cancel-no-final", "transport"} {
		t.Run(scenario, func(t *testing.T) {
			r, c, tx := forkFixture(t)
			c.Result = &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}
			if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
				t.Fatal(err)
			}
			name := portable.TimerB
			if scenario == "cancel-no-final" {
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.ReceiveResponse, Now: r.now, Message: forkReply(t, tx, 180, "early")}); err != nil {
					t.Fatal(err)
				}
				c.Cancelled = true
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.RequestCancel, Now: r.now}); err != nil {
					t.Fatal(err)
				}
				name = portable.TimerCancel
			}
			if scenario == "transport" {
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TransportFailure, Now: r.now, Error: "closed"}); err != nil {
					t.Fatal(err)
				}
			} else {
				deadline := tx.Native.Timers[name]
				if !deadline.Active {
					t.Fatal("native timeout not armed")
				}
				r.now = deadline.At
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: name, Generation: deadline.Generation}); err != nil {
					t.Fatal(err)
				}
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: name, Generation: deadline.Generation}); err != nil {
					t.Fatal(err)
				}
			}
			if !c.Snapshot.Ended || c.RetireMS <= r.now || c.Result.Certainty != pb.OperationResult_COMPLETED || stateCountEvents(t, r, pb.DialogEvent_FAILED) != 1 {
				t.Fatalf("setup did not fail exactly once: %+v", c)
			}
			start := len(r.payload.Effects)
			r.in = stateEngineInput{Kind: "ingest", Context: c.Snapshot.Context, Application: c.Snapshot.Application, Ingest: &pb.OwnerIngest{Context: c.Snapshot.Context, Application: c.Snapshot.Application, Flow: c.Flow, Wire: forkReply(t, tx, 200, "late").Wire}}
			if err := r.ingest(); err != nil {
				t.Fatal(err)
			}
			methods, _ := forkWireEffects(t, r.payload.Effects[start:])
			if !reflect.DeepEqual(methods, []string{"ACK", "BYE"}) || c.Dialog != "" || c.Snapshot.Answered || stateCountEvents(t, r, pb.DialogEvent_ANSWERED) != 0 {
				t.Fatalf("late success escaped cleanup: %v %+v", methods, c.Snapshot)
			}
			bye := r.payload.Effects[len(r.payload.Effects)-1]
			var body stateEngineEffect
			if err := unmarshalRetainedFixture(bye.Payload, &body); err != nil || body.AfterACK == "" {
				t.Fatal("late cleanup lost ACK-written prerequisite")
			}
		})
	}
}

func TestStateByeWithoutFinalTerminatesExactDialog(t *testing.T) {
	for _, scenario := range []string{"timer-f", "transport", "losing-fork"} {
		t.Run(scenario, func(t *testing.T) {
			r, c, initial := forkFixture(t)
			if _, err := r.forkResponse(c, initial, forkReply(t, initial, 200, "winner")); err != nil {
				t.Fatal(err)
			}
			key := c.Dialog
			if scenario == "losing-fork" {
				if _, err := r.forkResponse(c, initial, forkReply(t, initial, 200, "loser")); err != nil {
					t.Fatal(err)
				}
				key = "local/loser"
			} else {
				d, effects, err := portable.StepDialog(c.Dialogs[key], portable.DialogInput{Kind: portable.DialogLocalRequest, Now: r.now, Method: sip.BYE, Branch: "z9hG4bK-bye-timeout"})
				if err != nil {
					t.Fatal(err)
				}
				c.Dialogs[key] = d
				if err := r.dialogEffectsFor(c, key, effects); err != nil {
					t.Fatal(err)
				}
			}
			var tx *stateTransaction
			for _, candidate := range c.Transactions {
				if portable.MatchRequest(candidate.Native.Origin, *c.Dialogs[key].LocalBye) {
					tx = candidate
				}
			}
			if tx == nil {
				t.Fatal("BYE transaction missing")
			}
			if scenario == "transport" {
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TransportFailure, Now: r.now, Error: "closed"}); err != nil {
					t.Fatal(err)
				}
			} else {
				deadline := tx.Native.Timers[portable.TimerF]
				r.now = deadline.At
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: portable.TimerF, Generation: deadline.Generation}); err != nil {
					t.Fatal(err)
				}
			}
			if c.Dialogs[key].Phase != portable.DialogEnded {
				t.Fatal("BYE ended without terminating native dialog")
			}
			if scenario == "losing-fork" {
				if c.Snapshot.Ended || c.Dialogs[c.Dialog].Phase == portable.DialogEnded {
					t.Fatal("losing branch ended winner")
				}
			} else if !c.Snapshot.Ended || c.RetireMS <= r.now || stateCountEvents(t, r, pb.DialogEvent_ENDED) != 1 {
				t.Fatal("selected BYE did not end context")
			}
		})
	}
}

func TestStateRejectedRemoteReinviteReleasesOfferWithoutChangingAcceptedState(t *testing.T) {
	for _, status := range []uint32{488, 500} {
		t.Run(strconv.Itoa(int(status)), func(t *testing.T) {
			r, c, initial := forkFixture(t)
			if _, err := r.forkResponse(c, initial, forkReply(t, initial, 200, "winner")); err != nil {
				t.Fatal(err)
			}
			local, remote, target := append([]byte(nil), c.Snapshot.LocalSdp...), append([]byte(nil), c.Snapshot.RemoteSdp...), c.Snapshot.RemoteTarget
			request := func(seq uint32) portable.Message {
				m, err := portable.BuildInitialRequest(portable.InitialRequest{Method: sip.INVITE, URI: "sip:controller@example.test", From: "<sip:phone@example.test>", To: "<sip:controller@example.test>", CallID: "fork-call", LocalTag: "winner", CSeq: seq, Branch: fmt.Sprintf("z9hG4bK-remote-%d", seq), Advertise: "192.0.2.10:5061", Contact: "sip:phone@new.example.test:5061;transport=tls", Transport: "tls", Body: []byte("v=0\r\na=sendonly\r\n")})
				if err != nil {
					t.Fatal(err)
				}
				parsed, err := m.Parse()
				if err != nil {
					t.Fatal(err)
				}
				parsed.To().Params.Add("tag", "local")
				m.Wire = []byte(parsed.String())
				return m
			}
			m := request(20)
			d, _, err := portable.StepDialog(c.Dialogs[c.Dialog], portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: r.now, Message: m, Offer: true})
			if err != nil {
				t.Fatal(err)
			}
			c.Dialogs[c.Dialog] = d
			native, err := portable.NewTransaction(portable.InviteServer, m, portable.DefaultTimers(), c.Tag)
			if err != nil {
				t.Fatal(err)
			}
			native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
			if err != nil {
				t.Fatal(err)
			}
			tx := &stateTransaction{Native: native, Flow: c.Flow, RequestID: "remote-reinvite"}
			c.Transactions[native.Key], c.Requests[tx.RequestID] = tx, native.Key
			r.in = stateEngineInput{Kind: "command", Context: c.Snapshot.Context, Application: c.Snapshot.Application, Operation: "reject-remote", Command: &pb.Command{Context: c.Snapshot.Context, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: tx.RequestID, Status: status}}}}
			if err := r.command(); err != nil {
				t.Fatal(err)
			}
			d = c.Dialogs[c.Dialog]
			if d.RemoteInvite.State != portable.InviteRejected || d.Offer != portable.NoOffer || c.Snapshot.Ended || !bytes.Equal(c.Snapshot.LocalSdp, local) || !bytes.Equal(c.Snapshot.RemoteSdp, remote) || c.Snapshot.RemoteTarget != target {
				t.Fatal("rejected offer changed accepted dialog or remained pending")
			}
			if _, _, err = portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogReceiveRequest, Now: r.now, Message: request(21), Offer: true}); err != nil {
				t.Fatalf("next reINVITE still blocked: %v", err)
			}
		})
	}
}
