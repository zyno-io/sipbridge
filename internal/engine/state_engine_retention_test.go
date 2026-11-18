package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestStateRegisterCallIDReuseOutlivesThirtyTwoRetirements(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	ctx := context.Background()
	invite := stateTestRequest("INVITE", 1, "", "anchor")
	ref, err := engine.Locate(ctx, invite)
	if err != nil {
		t.Fatal(err)
	}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: engine.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.19", SourcePort: 50000, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, engine, ref, "anchor-ingest", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: invite.Wire}})
	record, group := stateTestLoad(t, engine, ref)
	anchor := group.Contexts[ref.Id]
	var requestID string
	for id := range anchor.Requests {
		requestID = id
	}
	answered := stateTestApply(t, engine, ref, "anchor-answer", stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	ack := stateTestRequest("ACK", 1, answered.Snapshot.LocalTag, "anchor-ack")
	stateTestApply(t, engine, ref, "anchor-ack", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: ack.Wire}})
	record, group = stateTestLoad(t, engine, ref)
	// The confirmed dialog keeps this exact Call-ID group live. Its completed
	// transaction replay retention is independently exercised by lifecycle tests.
	group.Contexts[ref.Id].Transactions = make(map[string]*stateTransaction)
	r := &stateReduction{engine: engine, record: record, group: group, now: time.Now().UnixMilli(), result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	for n := 0; n < 40; n++ {
		r.pruneRetired()
		id := fmt.Sprintf("registration-%d", n)
		registrationRef := engine.contextRef(record.Ref, id)
		wire := stateTestRequest("REGISTER", n+1, "", fmt.Sprintf("register-%d", n))
		r.deadline = r.now + 2000
		r.in = stateEngineInput{Kind: "ingest", Operation: id, Context: registrationRef, Application: app, Ingest: &pb.OwnerIngest{Context: registrationRef, Application: app, Flow: flow, Wire: wire.Wire}}
		if err = r.ingest(); err != nil {
			t.Fatalf("REGISTER context %d refused while group stayed live: %v", n, err)
		}
		c := r.group.Contexts[id]
		cutoff := r.now + 130000 // Original control deadline + reconciliation horizon.
		c.ReplayUntilMS = cutoff
		var tx *stateTransaction
		for _, candidate := range c.Transactions {
			tx = candidate
		}
		response, err := portable.BuildResponseWithHeaders(tx.Native.Origin, 200, "OK", nil, c.Tag, engine.cfg.ContactURI, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.stepTransaction(c, tx, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: response}); err != nil {
			t.Fatal(err)
		}
		for index, effect := range r.payload.Effects {
			effect.Status = state.EffectWritten
			effect.AttemptID = fmt.Sprintf("settled-%d-%d", n, index)
			r.payload.Effects[index] = effect
		}
		payload, err := r.finish()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			// A distinct control receipt carrying an exact SIP retransmission
			// cannot turn a fixed replay cutoff into a sliding retention lease.
			retry := r.in
			retry.Operation = "retransmission"
			encoded, err := marshalRetainedFixture(retry)
			if err != nil {
				t.Fatal(err)
			}
			replayRecord := record
			replayRecord.Payload = payload
			replayed, _, err := engine.reduce(replayRecord, state.Input{ID: "retransmission", Kind: "ingest", Payload: encoded, DeadlineMS: r.now + 15000, ReceiptUntilMS: cutoff + 100000}, r.now+5000)
			if err != nil {
				t.Fatal(err)
			}
			replayRecord.Payload = replayed
			replayGroup, err := decodeStateGroup(replayRecord)
			if err != nil {
				t.Fatal(err)
			}
			if replayGroup.Contexts[id].ReplayUntilMS != cutoff {
				t.Fatal("exact retransmission extended original retirement marker cutoff")
			}
		}
		for index, effect := range r.payload.Effects {
			effect.Status = state.EffectWritten
			effect.AttemptID = fmt.Sprintf("settled-%d-%d", n, index)
			r.payload.Effects[index] = effect
		}
		r.now = max(c.RetireMS, tx.DispositionUntilMS)

		if _, err = r.finish(); err != nil {
			t.Fatal(err)
		}
		if r.group.Contexts[id] != nil || r.group.Retired[id] != cutoff {
			t.Fatalf("REGISTER %d did not retain its fixed cutoff", n)
		}
		if r.group.Contexts[ref.Id] == nil {
			t.Fatal("anchor dialog no longer keeps group live")
		}
		r.now = cutoff + 1
		r.pruneRetired()
		if len(r.group.Retired) != 0 {
			t.Fatal("expired retirement markers accumulated across registration refreshes")
		}
	}
}

func TestStateLongCallPrunesRetiredTransactionSelectors(t *testing.T) {
	engine, _ := historicalStateTestEngine(t)
	ctx := context.Background()
	invite := stateTestRequest("INVITE", 1, "", "long-call")
	ref, err := engine.Locate(ctx, invite)
	if err != nil {
		t.Fatal(err)
	}
	app := &pb.ProcessRef{InstanceId: "controller", Epoch: "epoch"}
	flow := &pb.FlowRef{Ingress: engine.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.19", SourcePort: 50000, Listener: "sip.example.test:5061", Transport: "tls"}
	stateTestApply(t, engine, ref, "initial", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: invite.Wire}})
	_, group := stateTestLoad(t, engine, ref)
	var requestID string
	for id := range group.Contexts[ref.Id].Requests {
		requestID = id
	}
	answered := stateTestApply(t, engine, ref, "answer", stateEngineInput{Kind: "command", Application: app, Command: &pb.Command{Context: ref, Sequence: 1, Action: &pb.Command_Respond{Respond: &pb.Respond{RequestId: requestID, Status: 200}}}})
	ack := stateTestRequest("ACK", 1, answered.Snapshot.LocalTag, "ack")
	stateTestApply(t, engine, ref, "ack", stateEngineInput{Kind: "ingest", Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: ack.Wire}})
	record, group := stateTestLoad(t, engine, ref)
	c := group.Contexts[ref.Id]
	r := &stateReduction{engine: engine, record: record, group: group, now: time.Now().UnixMilli(), result: &pb.OperationResult{Certainty: pb.OperationResult_COMPLETED}}
	var first string
	var dialogSelector string
	for _, selector := range ack.Selectors {
		if strings.HasPrefix(selector, "dlg:") {
			dialogSelector = selector
		}
	}
	for n := 0; n < 100; n++ {
		r.now += 130000
		for _, tx := range c.Transactions {
			for name, timer := range tx.Native.Timers {
				if timer.Active && timer.At <= r.now {
					if err = r.stepTransaction(c, tx, portable.Input{Kind: portable.TimerFired, Now: r.now, Timer: name, Generation: timer.Generation}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		r.pruneTransactions()
		request := stateTestRequest("INFO", n+2, c.Snapshot.LocalTag, fmt.Sprintf("info-%d", n))
		r.deadline = r.now + 2000
		r.in = stateEngineInput{Kind: "ingest", Operation: fmt.Sprintf("info-%d", n), Context: ref, Application: app, Ingest: &pb.OwnerIngest{Context: ref, Application: app, Flow: flow, Wire: request.Wire}}
		if err = r.ingest(); err != nil {
			t.Fatal(err)
		}
		var latest *stateTransaction
		for _, tx := range c.Transactions {
			if bytes.Equal(tx.Native.Origin.WireBytes(), request.Wire) {
				latest = tx
			}
		}
		if latest == nil {
			t.Fatal("new INFO transaction not retained")
		}
		response, err := portable.BuildResponseWithHeaders(latest.Native.Origin, 200, "OK", nil, c.Tag, engine.cfg.ContactURI, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.stepTransaction(c, latest, portable.Input{Kind: portable.SendResponse, Now: r.now, Message: response}); err != nil {
			t.Fatal(err)
		}
		r.payload.Effects = nil // Receiver/physical acknowledgement is qualified separately.
		if _, err = r.finish(); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			for _, selector := range request.Selectors {
				if strings.HasPrefix(selector, "stx:") {
					first = selector
				}
			}
		}
		if len(c.Snapshot.Selectors) > 4 || len(r.group.Selectors) > 4 {
			t.Fatalf("retired transaction selector history grew at INFO%d: %d/%d", n, len(c.Snapshot.Selectors), len(r.group.Selectors))
		}
		if r.group.Selectors[dialogSelector] != ref.Id {
			t.Fatal("confirmed dialog selector disappeared during transaction pruning")
		}
		for _, selector := range request.Selectors {
			if r.group.Selectors[selector] != ref.Id {
				t.Fatal("retained current transaction selector lost lookup")
			}
		}
		if n > 1 && r.group.Selectors[first] != "" {
			t.Fatal("retired INFO selector remained indexed")
		}
		if c.Result != nil && c.Result.Snapshot != nil && len(c.Result.Snapshot.Selectors) > 4 {
			t.Fatal("last outcome retained unbounded selector history")
		}
	}
}
