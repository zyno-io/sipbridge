package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

func TestResidentClientStartUsesNativeTransaction(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	for _, tc := range []struct {
		method string
		family portable.Family
	}{
		{"", portable.InviteClient},
		{"OPTIONS", portable.NonInviteClient},
	} {
		open := &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Method: tc.method, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}
		tx, effects, err := e.prepareResidentClientStart(open, c.snapshot.Context, c.snapshot.Application, c.flow, "operation", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		send := false
		for _, effect := range effects {
			if effect.Kind == portable.Send {
				send = true
			}
		}
		if tx.native.Family != tc.family || !send {
			t.Fatalf("unexpected native start: %s %#v", tx.native.Family, effects)
		}
		parsed, err := sip.ParseMessage(tx.native.Origin.WireBytes())
		if err != nil {
			t.Fatal(err)
		}
		request := parsed.(*sip.Request)
		if request.CallID().Value() != c.snapshot.Context.State.CallId || request.CSeq().SeqNo != 1 {
			t.Fatal("initial transaction identity mismatch")
		}
	}
}

func TestResidentClientStartRejectsFlowTransportMismatch(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	open := &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "udp"}
	if _, _, err := e.prepareResidentClientStart(open, c.snapshot.Context, c.snapshot.Application, c.flow, "operation", time.Now()); err == nil {
		t.Fatal("accepted a mismatched physical flow")
	}
}

func TestResidentClientNonInviteResponseCompletesExactReceipt(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	e.contexts = make(map[string]*residentContext)
	e.transactions = make(map[string]*residentContext)
	e.dialogs = make(map[string]*residentContext)
	e.deadlineWake = make(chan struct{}, 1)
	c.transactions = make(map[string]*residentTransaction)
	c.outputs = make(map[string]*residentOutput)
	open := &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Method: "OPTIONS", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}
	tx, _, err := e.prepareResidentClientStart(open, c.snapshot.Context, c.snapshot.Application, c.flow, "operation", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tx.command = newResidentCommand("operation", 1)
	receipt := tx.command
	c.budget, err = e.openResidentBudgetLocked(c, residentInviteBudget)
	if err != nil {
		t.Fatal(err)
	}
	tx.budget = c.budget
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		t.Fatal(err)
	}
	c.command = newResidentCommand("newer", 2)
	c.transactions[tx.native.Key] = tx
	// Compact dispositions and unrelated live procedures share the context.
	// Completion must select the exact native key without changing either.
	for index := range 256 {
		key := fmt.Sprintf("retired-%d", index)
		c.transactions[key] = &residentTransaction{retired: true, native: portable.Transaction{Key: key}}
	}
	var unrelated []*residentTransaction
	for index := range 8 {
		other, _, err := e.prepareResidentClientStart(open, c.snapshot.Context, c.snapshot.Application, c.flow, fmt.Sprintf("other-%d", index), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		c.transactions[other.native.Key] = other
		unrelated = append(unrelated, other)
	}
	e.contexts[c.snapshot.Context.Id] = c
	request, err := Observe(tx.native.Origin.WireBytes(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, selector := range request.Selectors {
		e.transactions[selector] = c
	}
	response, err := portable.BuildResponseWithHeaders(tx.native.Origin, 200, "OK", nil, "remote", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(response.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	// Caller projections are untrusted; classification and selector lookup must
	// come from the actual response wire, even when every supplied hint is false.
	observed.Method, observed.Status, observed.Selectors = "REGISTER", 0, []string{"forged"}
	wrongFlow := proto.Clone(c.flow).(*pb.FlowRef)
	wrongFlow.ConnectionId = "different-connection"
	if handled, err := e.tryNativeCompletion(context.Background(), wrongFlow, observed); err != nil || handled {
		t.Fatal("response bypassed exact transaction flow", handled, err)
	}
	handled, err := e.tryNativeCompletion(context.Background(), c.flow, observed)
	if err != nil || !handled {
		t.Fatalf("response not handled: %v", err)
	}
	if !receipt.finished || receipt.certainty != pb.OperationResult_COMPLETED || receipt.status != 200 || c.command.finished {
		t.Fatal("response did not settle its own receipt independently")
	}
	result := residentCommandResultLocked(c, receipt)
	if result.GetResponse().GetStatus() != 200 || result.GetResponse().GetMethod() != "OPTIONS" || !bytes.Equal(result.GetResponse().GetWire(), response.WireBytes()) {
		t.Fatal("public receipt used forged metadata")
	}
	for _, other := range unrelated {
		if other.native.Phase != portable.Trying {
			t.Fatal("response changed an unrelated live procedure")
		}
	}
}

func TestResidentLosingForkWaitsForWrittenACK(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.transactions = make(map[string]*residentContext)
	e.dialogs = make(map[string]*residentContext)
	e.deadlineWake = make(chan struct{}, 1)
	c.transactions, c.dialogs, c.outputs = make(map[string]*residentTransaction), make(map[string]*residentDialog), make(map[string]*residentOutput)
	open := &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}
	tx, _, err := e.prepareResidentClientStart(open, c.snapshot.Context, c.snapshot.Application, c.flow, "operation", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tx.command = newResidentCommand("operation", 1)
	c.budget, err = e.openResidentBudgetLocked(c, residentInviteBudget)
	if err != nil {
		t.Fatal(err)
	}
	tx.budget = c.budget
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		t.Fatal(err)
	}
	c.transactions[tx.native.Key] = tx
	for _, remoteTag := range []string{"winner", "loser"} {
		response, err := portable.BuildResponseWithHeaders(tx.native.Origin, 200, "OK", nil, remoteTag, "sip:phone@example.test", nil)
		if err != nil {
			t.Fatal(err)
		}
		observed, err := Observe(response.WireBytes(), false)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := sip.ParseMessage(response.WireBytes())
		if err != nil {
			t.Fatal(err)
		}
		writes, _, err := e.residentInviteResponseLocked(c, tx, response, observed, parsed.(*sip.Response))
		if err != nil {
			t.Fatal(err)
		}
		if len(writes) != 1 {
			t.Fatalf("%s ACK count = %d", remoteTag, len(writes))
		}
		if remoteTag == "winner" {
			output := c.outputs[writes[0].Effect.Id]
			dialog := c.dialogs[output.dialog]
			dialog.native.LocalInvite.Sequence++
			_, _, err = e.residentClientOutputCompletedLocked(c, output, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN})
			if err != nil || dialog.ackWritten {
				t.Fatal("stale ACK marked a later INVITE written", err)
			}
			dialog.native.LocalInvite.Sequence--
			continue
		}
		output := c.outputs[writes[0].Effect.Id]
		if !output.cleanupAfterACK {
			t.Fatal("losing branch lacks cleanup dependency")
		}
		follow, handled, err := e.residentClientOutputCompletedLocked(c, output, &pb.DeliveryResult{Outcome: pb.DeliveryResult_UNKNOWN})
		if err != nil || !handled || len(follow) != 0 {
			t.Fatal("unknown ACK authorized BYE", err)
		}
		dialog := c.dialogs[output.dialog]
		priorPhase := dialog.native.Phase
		dialog.native.Phase = portable.DialogEnded
		follow, handled, err = e.residentClientOutputCompletedLocked(c, output, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN})
		if err != nil || !handled || len(follow) != 0 || !dialog.ackWritten {
			t.Fatal("late written ACK restarted an ended branch", err)
		}
		dialog.native.Phase = priorPhase
		dialog.ackWritten = false
		replayed, _, err := e.residentInviteResponseLocked(c, tx, response, observed, parsed.(*sip.Response))
		if err != nil || len(replayed) != 1 || c.outputs[replayed[0].Effect.Id].method != "ACK" {
			t.Fatal("repeated final did not produce native re-ACK", err)
		}
		output = c.outputs[replayed[0].Effect.Id]
		follow, handled, err = e.residentClientOutputCompletedLocked(c, output, &pb.DeliveryResult{Outcome: pb.DeliveryResult_WRITTEN})
		if err != nil || !handled || len(follow) != 1 {
			t.Fatal("written ACK did not authorize BYE", err)
		}
		if c.outputs[follow[0].Effect.Id].method != "BYE" {
			t.Fatal("cleanup was not BYE")
		}
	}
}

func TestResidentOpenOptionsWritesAndCompletesOnResponse(t *testing.T) {
	e, peer, _, ctx, _, writes, count := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "outbound-options"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Method: "OPTIONS", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	resultCh := make(chan *pb.OperationResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := e.commandResidentClient(ctx, peer, command, "open-options", time.Now().Add(5*time.Second))
		resultCh <- result
		errCh <- err
	}()
	var wire []byte
	select {
	case wire = <-writes:
	case <-ctx.Done():
		t.Fatal("no request write")
	}
	request := portable.RestoreMessage(wire, "tls", "", "")
	response, err := portable.BuildResponseWithHeaders(request, 200, "OK", nil, "remote", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(response.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	handled, err := e.completeResidentClient(ctx, flow, observed)
	if err != nil || !handled {
		t.Fatal("response not handled", err)
	}
	result := <-resultCh
	if err = <-errCh; err != nil || result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.SipStatus != 200 || count.Load() != 1 {
		t.Fatalf("unexpected outbound result: %v, %v", result, err)
	}
}

func TestResidentOpenInviteProducesACKAndAnsweredEvent(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "outbound-invite"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	result, err := e.commandResidentClient(ctx, peer, command, "open-invite", time.Now().Add(5*time.Second))
	if err != nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE not accepted", result, err)
	}
	var wire []byte
	select {
	case wire = <-writes:
	case <-ctx.Done():
		t.Fatal("no INVITE write")
	}
	// Once the initial INVITE is admitted, the reserved completion budget must
	// fund its dialog, ACK and lifecycle event even at global capacity.
	e.mu.Lock()
	e.maxBytes = e.bytes + e.reserved
	e.mu.Unlock()
	response, err := portable.BuildResponseWithHeaders(portable.RestoreMessage(wire, "tls", "", ""), 200, "OK", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(response.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := e.completeResidentClient(ctx, flow, observed); done <- err }()
	select {
	case wire = <-writes:
	case err = <-done:
		t.Fatal("response failed before ACK", err)
	case <-ctx.Done():
		t.Fatal("no ACK write")
	}
	parsed, err := sip.ParseMessage(wire)
	if err != nil || parsed.CSeq().MethodName != sip.ACK {
		t.Fatal("expected native ACK", err)
	}
	envelope := readControllerEnvelope(t, ctx, conn)
	if envelope.GetDialogEvent().GetKind() != pb.DialogEvent_ANSWERED {
		t.Fatal("missing answered event", envelope)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	cancel := &pb.Command{Context: result.Snapshot.Context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	cancelResult, err := e.commandResidentClient(ctx, peer, cancel, "cancel-after-answer", time.Now().Add(5*time.Second))
	if err != nil || cancelResult.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("post-answer CANCEL failed", cancelResult, err)
	}
	select {
	case wire = <-writes:
	case <-ctx.Done():
		t.Fatal("no cleanup BYE")
	}
	parsed, err = sip.ParseMessage(wire)
	if err != nil || parsed.CSeq().MethodName != sip.BYE {
		t.Fatal("expected cleanup BYE after written ACK", err)
	}
}

func TestResidentOpenInviteProvenNoWriteReportsFailure(t *testing.T) {
	e, peer, conn, ctx, session, _, _ := residentControlFixture(t)
	e.cfg.WriteCommitted = func(_ context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN, Error: "fixture refused before write"}
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "unwritten-invite"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	result, err := e.commandResidentClient(ctx, peer, command, "unwritten", time.Now().Add(5*time.Second))
	if err != nil || result == nil || result.Certainty != pb.OperationResult_COMPLETED || result.Error == "" || !result.Snapshot.Ended {
		t.Fatal("proven no-write did not terminate setup", result, err)
	}
	envelope := readControllerEnvelope(t, ctx, conn)
	if envelope.GetDialogEvent().GetKind() != pb.DialogEvent_FAILED {
		t.Fatal("missing failed event", envelope)
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
}

func TestResidentCancelBeforeProvisionalStartsPairedTransaction(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "cancel-before-provisional"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	result, err := e.commandResidentClient(ctx, peer, command, "initial", time.Now().Add(5*time.Second))
	if err != nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE not accepted", err)
	}
	origin := portable.RestoreMessage(<-writes, "tls", "", "")
	cancel := &pb.Command{Context: result.Snapshot.Context, Sequence: 2, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	cancelResult, err := e.commandResidentClient(ctx, peer, cancel, "cancel", time.Now().Add(5*time.Second))
	if err != nil || cancelResult.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("CANCEL intent refused", err)
	}
	select {
	case <-writes:
		t.Fatal("CANCEL preceded provisional response")
	default:
	}
	provisional, err := portable.BuildResponseWithHeaders(origin, 180, "Ringing", nil, "remote", "sip:phone@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(provisional.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := e.completeResidentClient(ctx, flow, observed); done <- err }()
	var wire []byte
	select {
	case wire = <-writes:
	case err = <-done:
		t.Fatal("provisional failed before CANCEL", err)
	case <-ctx.Done():
		t.Fatal("CANCEL missing")
	}
	parsed, err := sip.ParseMessage(wire)
	if err != nil || parsed.CSeq().MethodName != sip.CANCEL {
		t.Fatal("wrong paired request", err)
	}
	envelope := readControllerEnvelope(t, ctx, conn)
	if envelope.GetDialogEvent().GetKind() != pb.DialogEvent_PROVISIONAL {
		t.Fatal("missing provisional event")
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: envelope.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResidentOpenNetworkRunsOutsideContextLock(t *testing.T) {
	e, peer, _, ctx, _, writes, _ := residentControlFixture(t)
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "carrier", SourceIp: "192.0.2.50", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
	e.cfg.OpenNetwork = func(_ context.Context, uri string, target *pb.NetworkTarget) (*pb.FlowRef, error) {
		e.mu.Lock()
		c := e.contexts["network-invite"]
		e.mu.Unlock()
		if c == nil {
			return nil, errors.New("open intent missing")
		}
		c.mu.Lock()
		c.mu.Unlock()
		if uri != "sip:carrier@example.test" || target.GetTransport() != "tls" {
			return nil, errors.New("network target changed")
		}
		return flow, nil
	}
	command := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "network-invite"}, Sequence: 1,
		Admission: &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli())},
		Action:    &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:carrier@example.test", Transport: "tls", Network: &pb.NetworkTarget{Transport: "tls"}, Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	done := make(chan error, 1)
	go func() {
		result, err := e.commandResidentClient(ctx, peer, command, "open-network", time.Now().Add(5*time.Second))
		if err == nil && result.Certainty != pb.OperationResult_ACCEPTED {
			err = errors.New("INVITE not accepted")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("network open blocked on context lock")
	}
	select {
	case <-writes:
	case <-ctx.Done():
		t.Fatal("network INVITE was not written")
	}
}

func TestResidentLateOfferWaitsForControllerACKAnswer(t *testing.T) {
	e, peer, conn, ctx, session, writes, _ := residentControlFixture(t)
	physicalFlows := make(chan *pb.FlowRef, 4)
	writeCommitted := e.cfg.WriteCommitted
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		physicalFlows <- write.Flow
		return writeCommitted(ctx, write)
	}
	flow := &pb.FlowRef{Ingress: e.cfg.Process, ConnectionId: "phone", SourceIp: "192.0.2.12", SourcePort: 50300, Listener: "sip.example.test:5061", Transport: "tls"}
	admission := &pb.CommandAdmission{DirectoryEpoch: "directory", ExpiresAtMs: uint64(time.Now().Add(5 * time.Second).UnixMilli()), SelectedFlow: flow}
	open := &pb.Command{Context: &pb.ContextRef{Owner: peer.Application(), Id: "late-offer"}, Sequence: 1, Admission: admission,
		Action: &pb.Command_OpenUac{OpenUac: &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", BindingId: "binding", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}}}
	result, err := e.commandResidentClient(ctx, peer, open, "open", time.Now().Add(5*time.Second))
	if err != nil || result.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("INVITE not accepted", err)
	}
	origin := portable.RestoreMessage(<-writes, "tls", "", "")
	if !proto.Equal(<-physicalFlows, flow) {
		t.Fatal("initial flow changed")
	}
	offer := []byte("v=0\r\no=remote 1 1 IN IP4 192.0.2.1\r\ns=-\r\nt=0 0\r\n")
	response, err := portable.BuildResponseWithHeaders(origin, 200, "OK", offer, "remote", "sip:phone@example.test", []portable.Header{{Name: "Content-Type", Value: "application/sdp"}})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := Observe(response.WireBytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := e.completeResidentClient(ctx, flow, observed); done <- err }()
	event := readControllerEnvelope(t, ctx, conn)
	if event.GetDialogEvent().GetKind() != pb.DialogEvent_ANSWERED {
		t.Fatal("missing late-offer event")
	}
	select {
	case <-writes:
		t.Fatal("ACK sent before controller answer")
	default:
	}
	writeControllerEnvelope(t, ctx, conn, &pb.Envelope{SessionId: session, CorrelationId: event.CorrelationId, Payload: &pb.Envelope_Result{Result: &pb.OperationResult{Certainty: pb.OperationResult_ACCEPTED}}})
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	answer := []byte("v=0\r\no=local 2 2 IN IP4 192.0.2.2\r\ns=-\r\nt=0 0\r\n")
	newFlow := proto.Clone(flow).(*pb.FlowRef)
	newFlow.ConnectionId = "replacement"
	newFlow.SourcePort++
	ackAdmission := proto.Clone(admission).(*pb.CommandAdmission)
	ackAdmission.SelectedFlow = newFlow
	ack := &pb.Command{Context: result.Snapshot.Context, Sequence: 2, Admission: ackAdmission, Action: &pb.Command_DialogRequest{DialogRequest: &pb.DialogRequest{Method: "ACK", Body: answer, Headers: []*pb.Header{{Name: "Content-Type", Value: "application/sdp"}}}}}
	ackResult, err := e.commandResidentClient(ctx, peer, ack, "answer-ack", time.Now().Add(5*time.Second))
	if err != nil || ackResult.Certainty != pb.OperationResult_COMPLETED {
		t.Fatal("ACK answer not completed", ackResult, err)
	}
	wire := <-writes
	parsed, err := sip.ParseMessage(wire)
	if err != nil || parsed.CSeq().MethodName != sip.ACK || !bytes.Equal(parsed.Body(), answer) {
		t.Fatal("ACK answer body missing", err)
	}
	if !ackResult.Snapshot.InitialAck {
		t.Fatal("positive ACK evidence not reflected in snapshot")
	}
	if !proto.Equal(<-physicalFlows, newFlow) {
		t.Fatal("ACK did not use selected replacement")
	}
	cancel := &pb.Command{Context: result.Snapshot.Context, Sequence: 3, Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}
	cancelResult, err := e.commandResidentClient(ctx, peer, cancel, "cancel-after-answer", time.Now().Add(5*time.Second))
	if err != nil || cancelResult.Certainty != pb.OperationResult_ACCEPTED {
		t.Fatal("post-answer CANCEL failed", err)
	}
	wire = <-writes
	parsed, err = sip.ParseMessage(wire)
	if err != nil || parsed.CSeq().MethodName != sip.BYE {
		t.Fatal("cleanup BYE missing", err)
	}
	if !proto.Equal(<-physicalFlows, newFlow) {
		t.Fatal("cleanup BYE fell back to older flow")
	}
}

func TestResidentLosingForkRemoteByeDoesNotEndWinner(t *testing.T) {
	e, c, _ := residentSubscriptionFixture(t)
	e.maxBytes = 64 << 20
	e.contexts = map[string]*residentContext{c.snapshot.Context.Id: c}
	e.transactions = make(map[string]*residentContext)
	e.dialogs = make(map[string]*residentContext)
	e.deadlineWake = make(chan struct{}, 1)
	c.transactions, c.dialogs, c.outputs = make(map[string]*residentTransaction), make(map[string]*residentDialog), make(map[string]*residentOutput)
	c.snapshot.Kind = pb.ContextSnapshot_UAC
	open := &pb.OpenUac{Uri: "sip:phone@example.test", Transport: "tls", Headers: []*pb.Header{{Name: "From", Value: "<sip:service@example.test>"}}}
	tx, _, err := e.prepareResidentClientStart(open, c.snapshot.Context, c.snapshot.Application, c.flow, "fork", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tx.command = newResidentCommand("fork", 1)
	c.budget, err = e.openResidentBudgetLocked(c, residentInviteBudget)
	if err != nil {
		t.Fatal(err)
	}
	tx.budget = c.budget
	tx.charged = residentTransactionBytes(tx)
	tx.borrowed, err = e.chargeResidentBudgetLocked(c, tx.budget, tx.charged)
	if err != nil {
		t.Fatal(err)
	}
	c.transactions[tx.native.Key] = tx
	var losingResponse portable.Message
	for _, remoteTag := range []string{"winner", "loser"} {
		response, err := portable.BuildResponseWithHeaders(tx.native.Origin, 200, "OK", nil, remoteTag, "sip:phone@example.test", nil)
		if err != nil {
			t.Fatal(err)
		}
		observed, err := Observe(response.WireBytes(), false)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := sip.ParseMessage(response.WireBytes())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = e.residentInviteResponseLocked(c, tx, response, observed, parsed.(*sip.Response)); err != nil {
			t.Fatal(err)
		}
		if remoteTag == "loser" {
			losingResponse = response
		}
	}
	remote, err := portable.NewDialog(portable.UAS, tx.native.Origin, losingResponse, portable.DefaultTimers())
	if err != nil {
		t.Fatal(err)
	}
	remote.SentBy = "phone.example.test:5061"
	bye, err := portable.BuildDialogRequest(remote, sip.BYE, 1, nil, "z9hG4bK-losing-remote-bye")
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan []byte, 1)
	e.cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		reserved, err := e.Reserve(ctx, write)
		if err != nil {
			return &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN, Error: err.Error()}
		}
		written <- reserved.Wire
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_WRITTEN}
		if err = e.Complete(ctx, write, result); err != nil {
			result.Error = err.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	priorEvents := len(c.events)
	handled, err := e.tryNativeCompletion(context.Background(), c.flow, &pb.SipMessage{Wire: bye.WireBytes()})
	if err != nil || !handled {
		t.Fatal("losing branch BYE was not handled", err)
	}
	parsed, err := sip.ParseMessage(<-written)
	if err != nil {
		t.Fatal(err)
	}
	response, ok := parsed.(*sip.Response)
	if !ok || response.StatusCode != 200 {
		t.Fatal("losing BYE did not receive 200")
	}
	if c.snapshot.Ended || len(c.events) != priorEvents {
		t.Fatal("losing BYE ended the selected call or emitted an event")
	}
}
