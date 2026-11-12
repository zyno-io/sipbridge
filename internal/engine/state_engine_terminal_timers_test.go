package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestTerminalTimersKeepCancellationEffectsAndACKEvidence(t *testing.T) {
	r, c, tx := forkFixture(t)
	r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
	started, _, err := portable.StepTransaction(tx.Native, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		t.Fatal(err)
	}
	tx.Native = started
	before := started.Clone()
	input := portable.Input{Kind: portable.ReceiveResponse, Now: r.now + 1, Message: forkReply(t, tx, 486, "busy")}
	next, effects, err := portable.StepTransaction(before, input)
	if err != nil || next.Phase != portable.Terminated || next.ACK == nil || next.LastResponse == nil || len(next.Timers) == 0 {
		t.Fatal("fixture needs a reliable final with stopped timers and exact ACK", err)
	}
	encodedEffects, err := json.Marshal(effects)
	if err != nil {
		t.Fatal(err)
	}
	cancellations := 0
	for _, effect := range effects {
		if effect.Kind != portable.CancelTimer {
			continue
		}
		original := before.Timers[effect.Timer]
		if !original.Active {
			t.Fatal("cancellation did not refer to a previously active generation")
		}
		original.Active = false
		if effect.Deadline != original {
			t.Fatal("cancellation changed its generation, deadline or interval")
		}
		cancellations++
	}
	if cancellations == 0 {
		t.Fatal("fixture did not cancel any timer")
	}
	if err := endTransactionTimers(before.Phase, &next); err != nil || next.Timers != nil {
		t.Fatal("terminal timer map not released", err)
	}
	afterEffects, err := json.Marshal(effects)
	if err != nil || !bytes.Equal(encodedEffects, afterEffects) || !reflect.DeepEqual(before, started) {
		t.Fatal("dropping the result map changed emitted cancellation evidence or input", err)
	}
	if err := next.Validate(); err != nil {
		t.Fatal("compact terminal transaction is not valid native state", err)
	}
	// Exercise the actual adapter hook, not only the helper. Its retained ACK
	// and final response still support exact late-final completion.
	r.now, r.in = input.Now, stateEngineInput{Kind: "ingest"}
	if err := r.stepTransaction(c, tx, input); err != nil {
		t.Fatal(err)
	}
	if tx.Native.Timers != nil || !reflect.DeepEqual(tx.Native, next) || tx.RetireMS != r.now+120000 {
		t.Fatal("adapter changed more than stopped timer storage or lost retention")
	}
	if !portable.MatchResponse(tx.Native.Origin, *tx.Native.LastResponse) || !bytes.Equal(tx.Native.ACK.Wire, next.ACK.Wire) {
		t.Fatal("exact final/ACK identity lost")
	}
	// A later read/replayed no-op must not normalize historical terminal maps.
	tx.Native.Timers = map[portable.Timer]portable.Deadline{portable.TimerB: {Generation: 7, At: 32000}}
	old := tx.Native.Clone()
	retire := tx.RetireMS
	if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now + 1}); err != nil || !reflect.DeepEqual(tx.Native, old) || tx.RetireMS != retire {
		t.Fatal("already terminated no-op rewrote retained state", err)
	}
}

func TestTerminalTimersRefuseActiveTerminalResultAndPreserveOtherPhases(t *testing.T) {
	for _, sample := range []struct {
		name          string
		before, after portable.Phase
		active        bool
		wantError     bool
	}{
		{"corrupt terminal result", portable.Trying, portable.Terminated, true, true},
		{"live result", portable.Trying, portable.Proceeding, true, false},
		{"live stopped generation", portable.Trying, portable.Proceeding, false, false},
		{"already terminal", portable.Terminated, portable.Terminated, false, false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			next := portable.Transaction{Phase: sample.after, Timers: map[portable.Timer]portable.Deadline{portable.TimerF: {Generation: 4, At: 19000, Interval: 500, Active: sample.active}}}
			original := maps.Clone(next.Timers)
			err := endTransactionTimers(sample.before, &next)
			if errors.Is(err, state.ErrCorrupt) != sample.wantError || !sample.wantError && err != nil || !reflect.DeepEqual(next.Timers, original) {
				t.Fatal("guard lost an active obligation or rewrote a nonterminal/no-op map", err)
			}
		})
	}
}

func TestTerminalTimersPairedCancelKeepsLiveInviteAndRetainedFinalProof(t *testing.T) {
	for _, transport := range []string{"udp", "tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			r, c, invite := advertiseFixture(t, transport, sip.INVITE, "")
			r.deadline = r.now + 2000
			cancel, err := portable.BuildCancel(invite.Native.Origin)
			if err != nil {
				t.Fatal(err)
			}
			expectedInvite, expectedCancel, _, err := portable.StepServerCancel(invite.Native, nil, portable.Input{Kind: portable.ReceiveRequest, Now: r.now, Message: cancel})
			if err != nil {
				t.Fatal(err)
			}
			ingestNativeCancelMessage(t, r, c, cancel)
			cancelTX := c.Transactions[expectedCancel.Key]
			if cancelTX == nil || invite.Native.Phase != portable.Completed || !reflect.DeepEqual(invite.Native.Timers, expectedInvite.Timers) || !invite.Native.Timers[portable.TimerH].Active {
				t.Fatal("paired CANCEL lost the still-live INVITE timer obligations")
			}
			if transport == "udp" {
				if cancelTX.Native.Phase != portable.Completed || !reflect.DeepEqual(cancelTX.Native.Timers, expectedCancel.Timers) || !cancelTX.Native.Timers[portable.TimerJ].Active {
					t.Fatal("UDP CANCEL lost its live Timer J")
				}
				return
			}
			if cancelTX.Native.Phase != portable.Terminated || cancelTX.Native.Timers != nil || cancelTX.Native.LastResponse == nil || cancelTX.FinalResponse == nil {
				t.Fatal("new immediately terminated CANCEL retained a timer map or lost its final")
			}
			cancelProof, inviteProof := cancelTX.stateRequestDisposition, invite.stateRequestDisposition
			cancelFinal := *cancelTX.FinalResponse
			finalWire := bytes.Clone(invite.Native.LastResponse.Wire)
			inviteFinal := *invite.FinalResponse
			ack, err := portable.BuildNon2xxACK(invite.Native.Origin, *invite.Native.LastResponse)
			if err != nil {
				t.Fatal(err)
			}
			ingestNativeCancelMessage(t, r, c, ack)
			if invite.Native.Phase != portable.Terminated || invite.Native.Timers != nil || !bytes.Equal(invite.Native.LastResponse.Wire, finalWire) || !reflect.DeepEqual(*invite.FinalResponse, inviteFinal) || !reflect.DeepEqual(invite.stateRequestDisposition, inviteProof) {
				t.Fatal("ACK terminal transition changed retained final or disposition proof")
			}
			// A historical already-terminated CANCEL replay stays untouched by
			// timer compaction; normal response replay accounting still applies.
			cancelTX.Native.Timers = map[portable.Timer]portable.Deadline{portable.TimerJ: {Generation: 9, At: r.now + 10}}
			oldTimers := maps.Clone(cancelTX.Native.Timers)
			invite.Native.Timers = map[portable.Timer]portable.Deadline{portable.TimerH: {Generation: 11, At: r.now + 20}}
			oldInviteTimers := maps.Clone(invite.Native.Timers)
			retire := cancelTX.RetireMS
			ingestNativeCancelMessage(t, r, c, cancel)
			if !reflect.DeepEqual(cancelTX.Native.Timers, oldTimers) || !reflect.DeepEqual(invite.Native.Timers, oldInviteTimers) || !reflect.DeepEqual(cancelTX.stateRequestDisposition, cancelProof) || !reflect.DeepEqual(*cancelTX.FinalResponse, cancelFinal) || cancelTX.RetireMS != retire {
				t.Fatal("paired replay rewrote old timer generations or final proof")
			}
		})
	}
}

func TestTerminalTimersSubscriptionEndsOnlyUnansweredRefresh(t *testing.T) {
	r, c, refresh := liveSubscriptionWithUnansweredRefresh(t)
	refresh.Native.Timers[portable.TimerJ] = portable.Deadline{Generation: 5, At: r.now - 1}
	proof := refresh.stateRequestDisposition
	origin := refresh.Native.Origin.Clone()
	notify := subscriptionNotifyFixture(t, r, c, refresh)
	notifyBefore := notify.Native.Clone()
	selected := make(map[string][]byte)
	for key, tx := range c.Transactions {
		if tx.Native.LastResponse != nil {
			encoded, err := json.Marshal(tx)
			if err != nil {
				t.Fatal(err)
			}
			selected[key] = encoded
		}
	}
	if len(selected) == 0 {
		t.Fatal("fixture needs an accepted subscription's retained response")
	}
	expireSubscriptionFixture(t, r, c)
	if refresh.Native.Phase != portable.Terminated || refresh.Native.Timers != nil || !reflect.DeepEqual(refresh.Native.Origin, origin) || !reflect.DeepEqual(refresh.stateRequestDisposition, proof) || refresh.RetireMS < proof.DispositionUntilMS {
		t.Fatal("subscription terminal transition lost immutable request proof")
	}
	if !reflect.DeepEqual(notify.Native, notifyBefore) || !notify.Native.Timers[portable.TimerF].Active {
		t.Fatal("subscription terminal transition changed an outstanding NOTIFY")
	}
	for key, before := range selected {
		after, err := json.Marshal(c.Transactions[key])
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("subscription terminal transition changed a retained selected response", err)
		}
	}
}
