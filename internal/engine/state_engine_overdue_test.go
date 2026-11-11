package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateOverdueNativeTimersResumeAfterLongRedisOutage(t *testing.T) {
	for _, kind := range []string{"transaction", "dialog", "owner"} {
		t.Run(kind, func(t *testing.T) {
			engine, _ := historicalStateTestEngine(t)
			r, c, tx := forkFixture(t)
			r.engine = engine
			r.group.Version = 1
			r.group.Retired = make(map[string]int64)
			r.result = &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}
			r.now = time.Now().Add(-3 * time.Minute).UnixMilli()
			ref, err := engine.store.GroupForCallID("fork-call")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			begun, err := engine.store.(*state.Store).Begin(context.Background(), ref, "seed", state.Input{ID: "seed", Kind: "locate", Fingerprint: "seed", Create: true, DeadlineMS: now.Add(2 * time.Second).UnixMilli(), ReceiptUntilMS: now.Add(2 * time.Minute).UnixMilli()})
			if err != nil {
				t.Fatal(err)
			}
			r.record = begun.Step.Record
			c.Snapshot.Context = engine.contextRef(r.record.Ref, c.Snapshot.Context.Id)
			c.OwnerValidated, c.OwnerDirectoryEpoch = true, "directory"
			switch kind {
			case "transaction":
				if err := r.stepTransaction(c, tx, portable.Input{Kind: portable.Start, Now: r.now}); err != nil {
					t.Fatal(err)
				}
			case "dialog":
				response := forkReply(t, tx, 200, "local-uas")
				d, err := portable.NewDialog(portable.UAS, tx.Native.Origin, response, portable.DefaultTimers())
				if err != nil {
					t.Fatal(err)
				}
				d, _, err = portable.StepDialog(d, portable.DialogInput{Kind: portable.DialogSendResponse, Now: r.now, Message: response, Answer: true})
				if err != nil {
					t.Fatal(err)
				}
				c.Tag, c.Dialog = d.LocalTag, d.LocalTag+"/"+d.RemoteTag
				c.Dialogs[c.Dialog] = d
				c.Snapshot.Kind, c.Snapshot.Answered = pb.ContextSnapshot_UAS, true
				delete(c.Transactions, tx.Native.Key)
			case "owner":
				if _, err := r.forkResponse(c, tx, forkReply(t, tx, 200, "owner")); err != nil {
					t.Fatal(err)
				}
				c.OwnerProbeAtMS = r.now + stateOwnerProbeInterval
			}
			payload, err := r.finish()
			if err != nil {
				t.Fatal(err)
			}
			var overdue bool
			for _, timer := range payload.Timers {
				overdue = overdue || timer.DeadlineMS+120000 < now.UnixMilli()
			}
			if !overdue {
				t.Fatal("fixture does not cross old admission cutoff")
			}
			if _, err = engine.store.(*state.Store).Commit(context.Background(), *begun.Step, payload, nil); err != nil {
				t.Fatal(err)
			}
			engine.tickGroup(context.Background(), r.record.Ref)
			record, err := engine.store.Load(context.Background(), r.record.Ref)
			if err != nil {
				t.Fatal(err)
			}
			g, err := decodeStateGroup(record)
			if err != nil {
				t.Fatal(err)
			}
			c = g.Contexts[c.Snapshot.Context.Id]
			if c == nil {
				t.Fatal("timer recovery lost context")
			}
			switch kind {
			case "transaction":
				if c.Transactions[tx.Native.Key].Native.Phase != portable.Terminated || !c.Snapshot.Ended {
					t.Fatal("overdue transaction deadline was not consumed")
				}
			case "dialog":
				if c.Dialogs[c.Dialog].LocalBye == nil || c.Dialogs[c.Dialog].Timers[portable.TimerACK].Active {
					t.Fatal("overdue dialog deadline did not start cleanup")
				}
			case "owner":
				if c.OwnerProbeEffect == "" || c.OwnerProbeAtMS <= now.UnixMilli() {
					t.Fatal("overdue owner query was not rearmed")
				}
			}
			for _, effect := range record.Payload.Effects {
				var body stateEngineEffect
				if err := unmarshalRetainedFixture(effect.Payload, &body); err != nil {
					t.Fatal(err)
				}
				if effect.Kind == "wire" && body.Authorization != nil && body.Authorization.DeadlineMs > uint64(now.UnixMilli()) {
					if kind == "transaction" || kind == "dialog" && bytes.HasPrefix(body.Wire, []byte("SIP/2.0 ")) {
						t.Fatal("catch-up emitted expired retransmission")
					}
				}
			}
		})
	}
}
