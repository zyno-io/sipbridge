package engine

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// prepareRedisCallByeOutput constructs the selected BYE 200 physical response.
func prepareRedisCallByeOutput(snapshot *pb.ContextSnapshot, flow *pb.FlowRef, key []byte, selected portable.Message, procedureField, dialogField, responseField, physicalField, outputField string, until time.Time, replay bool) (rediscore.OutputIntent, []rediscore.Change, bool, error) {
	return prepareRedisCallResponseOutput("BYE", 200, snapshot, flow, key, selected, procedureField, dialogField, responseField, physicalField, outputField, until, replay)
}

// matchRedisCallByeDuplicate must run before new dialog BYE selection. Existing
// physical uncertainty is never turned into a new attempt.
func matchRedisCallByeDuplicate(ctx context.Context, store *rediscore.Store, local *pb.ProcessRef, flow *pb.FlowRef, message portable.Message, key []byte, deadline time.Time) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if !facts.IsRequest || facts.Method != sip.BYE {
		return false, nil
	}
	if store == nil || !validSendProcess(local) || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, local) || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) || !time.Now().Before(deadline) {
		return true, errNativeRecord
	}
	ids, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorServerTransaction {
		return true, errNativeRecord
	}
	selected, err := store.LookupSelector(ctx, ids[0], "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	const prefix = "r:call-bye-"
	if !strings.HasPrefix(selected.Record, prefix) {
		return true, errNativeRecord
	}
	id := strings.TrimPrefix(selected.Record, prefix)
	if len(id) != 32 {
		return true, errNativeRecord
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return true, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return true, err
	}
	originField, responseField, outputField := "r:call-bye-origin-"+id, "r:call-bye-final-"+id, "r:call-bye-output-"+id
	if current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.CallId != facts.CallID || procedure.Native.Family != portable.NonInviteServer || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || procedure.Metadata == nil || !validPolicyFlow(procedure.Metadata.Flow) || !proto.Equal(procedure.Metadata.Flow, flow) && (procedure.Metadata.Flow.Transport != "tls" || flow.Transport != "tls" || current.Transport != "tls") {
		return true, errors.New("BYE duplicate context or flow changed")
	}
	if !slices.Contains(current.NoninitialProcedures, selected.Record) {
		return true, errNativeRecord
	}
	view, err := store.Read(ctx, selected.Identity, originField, responseField, outputField)
	if errors.Is(err, rediscore.ErrAbsent) {
		return true, rediscore.ErrConflict
	}
	if err != nil {
		return true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return true, err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return true, err
	}
	if !portable.MatchRequest(tx.Origin, message) || !tx.Origin.EqualWire(message) {
		return true, errors.New("BYE duplicate mismatch")
	}
	attributeNativeCall(ctx, current)
	// Reliable server transactions terminate after their final response; a
	// retained duplicate is suppressed just as Memory's retired disposition.
	if tx.Reliable || tx.Phase != portable.Completed || flow.Transport != "udp" {
		return true, nil
	}
	timer := tx.Timers[portable.TimerJ]
	if !timer.Active || timer.At <= time.Now().UnixMilli() {
		return true, nil
	}
	if procedure.Metadata.ProvisionalEmissions > procedure.Metadata.Emissions || procedure.Metadata.Emissions >= stateTransactionEmissions || procedure.Metadata.Emissions-procedure.Metadata.ProvisionalEmissions >= stateProfileTransmissions {
		return true, nil
	}
	oldOutput, err := decodeNativeOutput(view.Records[outputField])
	if err != nil || oldOutput.Method != "BYE" || oldOutput.Status != 200 || oldOutput.Procedure != selected.Record || oldOutput.Dialog == "" || !nativeMessageField(oldOutput.Dialog) || !proto.Equal(oldOutput.Write.Effect.Context, current.Context) || !proto.Equal(oldOutput.Write.Flow, procedure.Metadata.Flow) {
		return true, errNativeRecord
	}
	dialogView, e := store.Read(ctx, selected.Identity, oldOutput.Dialog)
	if errors.Is(e, rediscore.ErrAbsent) {
		return true, rediscore.ErrConflict
	}
	if e != nil {
		return true, e
	}
	if dialogView.Revision != view.Revision || dialogView.Timer != view.Timer {
		return true, rediscore.ErrConflict
	}
	dialog, e := decodeNativeDialog(dialogView.Records[oldOutput.Dialog])
	if e != nil || !slices.Contains(dialog.Metadata.Outputs, outputField) {
		return true, errNativeRecord
	}
	oldRef, err := nativeOutputRef(oldOutput.Write)
	if err != nil {
		return true, err
	}
	_, err = store.InspectOutput(ctx, oldRef)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return true, err
	}
	if !procedure.Metadata.OutputConsumed {
		return true, nil
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
	if err != nil {
		return true, err
	}
	if len(effects) != 1 || effects[0].Kind != portable.Send || !effects[0].Replay || effects[0].Message == nil || !effects[0].Message.EqualWire(*tx.LastResponse) {
		return true, errNativeRecord
	}
	until := minTime(deadline, time.Now().Add(2*time.Second))
	until = minTime(until, time.UnixMilli(timer.At))
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return true, err
	}
	intent, outputChanges, _, err := prepareRedisCallByeOutput(snapshot, flow, key, *tx.LastResponse, selected.Record, oldOutput.Dialog, responseField, "r:call-bye-wire-"+id, outputField, until, true)
	if err != nil {
		return true, err
	}
	metadata := proto.Clone(procedure.Metadata).(*nativepb.Procedure)
	metadata.OutputConsumed = false
	metadata.FinalResponseWrittenAcknowledged = false
	metadata.Emissions++
	procedureData, err := encodeNativeProcedure(next, procedure.Refs, metadata)
	if err != nil {
		return true, err
	}
	changes := append([]rediscore.Change{{Field: selected.Record, Value: procedureData}}, outputChanges...)
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: until, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: view.Timer, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}}})
	return true, err
}
