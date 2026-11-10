package engine

import (
	"context"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestNativeRefusalACKAndLateCreatorNeedNoController(t *testing.T) {
	f := newNativeOwnerFixture(t)
	f.engine.work = make(chan struct{}, 64)
	ctx := context.Background()
	initial := stateTestRequest("INVITE", 1, "", "private-refusal")
	reserved, err := f.engine.Locate(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	// This fixture intentionally has no control.Server or ready Controller. Refusals
	// and their exact negative-final ACK remain purely native protocol work.
	if err = f.engine.Reject(ctx, f.flow, initial, 403); err != nil {
		t.Fatal(err)
	}
	_, group := stateTestLoad(t, f.engine, reserved)
	if len(group.Contexts) != 1 || len(group.Selectors) != 0 || len(group.Allocations) != 1 {
		t.Fatal("refusal published business state or deleted a flow-independent reservation")
	}
	var privateID, transaction string
	var tx *stateTransaction
	for id, c := range group.Contexts {
		for key, candidate := range c.Transactions {
			privateID, transaction, tx = id, key, candidate
		}
	}
	if tx == nil || tx.Native.LastResponse == nil {
		t.Fatal("private final response not retained")
	}
	ack, err := portable.BuildNon2xxACK(tx.Native.Origin, *tx.Native.LastResponse)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(ack.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	foreign := proto.Clone(f.flow).(*pb.FlowRef)
	foreign.ConnectionId = "foreign-refusal-flow"
	if err = f.engine.Reject(ctx, foreign, observed, 481); err != nil {
		t.Fatal(err)
	}
	_, group = stateTestLoad(t, f.engine, reserved)
	if group.Contexts[privateID].Transactions[transaction].Native.Phase != portable.Completed {
		t.Fatal("foreign ACK stopped private refusal")
	}
	if handled, err := f.engine.TryNativeCompletion(ctx, f.flow, observed); err != nil || !handled {
		t.Fatalf("exact native refusal ACK required business allocation: %v / %v", handled, err)
	}
	_, group = stateTestLoad(t, f.engine, reserved)
	tx = group.Contexts[privateID].Transactions[transaction]
	if tx.Native.Phase != portable.Terminated || tx.Native.Timers[portable.TimerG].Active || tx.Native.Timers[portable.TimerH].Active {
		t.Fatalf("exact TLS ACK did not terminate private refusal: phase=%s G=%v H=%v", tx.Native.Phase, tx.Native.Timers[portable.TimerG].Active, tx.Native.Timers[portable.TimerH].Active)
	}
	late := f.apply(t, reserved, "delayed-after-refusal", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
		Ingest: &pb.OwnerIngest{Context: reserved, Application: f.app, Flow: f.flow, Wire: initial.Wire}})
	if late.Certainty != pb.OperationResult_COMPLETED || late.Error != "" || late.SipStatus != 0 {
		t.Fatal("delayed policy reply did not stop at the committed private refusal")
	}
	_, group = stateTestLoad(t, f.engine, reserved)
	if len(group.Contexts) != 1 || !group.Contexts[privateID].Rejection || len(group.Selectors) != 0 {
		t.Fatal("delayed policy reply admitted business state after refusal")
	}
	unknown := stateTestRequest("INFO", 2, "unknown-dialog", "unknown-tagged")
	if err = f.engine.Reject(ctx, f.flow, unknown, 481); err != nil {
		t.Fatalf("unknown tagged request lost its isolated refusal: %v", err)
	}
}
