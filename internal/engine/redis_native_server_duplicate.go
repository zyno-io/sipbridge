package engine

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// tryServerRequestDuplicate consumes only an exact already admitted request.
// Reliable duplicates need no new application notification or output attempt.
func (e *redisNative) tryServerRequestDuplicate(parent context.Context, flow *pb.FlowRef, message portable.Message) (bool, error) {
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return true, err
	}
	defer cancel()
	serverKey, err := message.ServerTxKey()
	if err != nil {
		return true, err
	}
	selected, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, serverKey), "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if !strings.HasPrefix(selected.Record, "r:procedure-") {
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
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.CallId != facts.CallID || procedure.Native.Family != portable.InviteServer && procedure.Native.Family != portable.NonInviteServer || !proto.Equal(procedure.Metadata.Flow, flow) || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) {
		return true, errNativeRecord
	}
	fields := []string{procedure.Refs.Origin}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
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
	if _, err = memo.Adopt(message, "admitted"); err != nil {
		return true, err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return true, err
	}
	if !portable.MatchRequest(tx.Origin, message) || !tx.Origin.EqualWire(message) {
		return true, rejectIngest(481, "server transaction request changed")
	}
	attributeNativeCall(ctx, current)
	if flow.Transport != "udp" || procedure.Metadata.DecisionPending || tx.LastResponse == nil {
		return true, nil
	}
	// The marker describes this procedure's immutable selected final, and is
	// recorded only from its exact positively written physical attempt.
	if !procedure.Metadata.FinalResponseWrittenAcknowledged {
		return true, nil
	}
	cutoff := int64(0)
	for _, timer := range []portable.Timer{portable.TimerH, portable.TimerL, portable.TimerJ} {
		if due := tx.Timers[timer]; due.Active && due.Generation > 0 && (cutoff == 0 || due.At < cutoff) {
			cutoff = due.At
		}
	}
	if cutoff <= time.Now().UnixMilli() {
		return true, nil
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.ReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
	if err != nil {
		return true, err
	}
	if len(effects) == 0 {
		return true, nil
	}
	if len(effects) != 1 || effects[0].Kind != portable.Send && effects[0].Kind != portable.Respond || !effects[0].Replay || effects[0].Message == nil || !effects[0].Message.EqualWire(*tx.LastResponse) || next.Phase != tx.Phase {
		return true, errNativeRecord
	}
	for _, timer := range []portable.Timer{portable.TimerH, portable.TimerL, portable.TimerJ} {
		if next.Timers[timer] != tx.Timers[timer] {
			return true, errNativeRecord
		}
	}
	if e.cfg.WriteNativeRetry == nil {
		return true, errors.New("native UDP response writer unavailable")
	}
	response, err := effects[0].Message.WithObservedResponseFlow(flow.SourceIp, int(flow.SourcePort))
	if err != nil {
		return true, err
	}
	finalView, err := store.Read(ctx, selected.Identity, selected.Record)
	if err != nil {
		return true, err
	}
	if finalView.Revision != view.Revision || finalView.Timer != view.Timer {
		return true, rediscore.ErrConflict
	}
	// This is one native retransmission under the live operation's worker
	// lease, original timer and unchanged exact flow; it is no fresh attempt.
	until := minTime(deadline, time.UnixMilli(cutoff))
	_, _, leaseUntil, leaseErr := e.driver.Current()
	if leaseErr != nil {
		return true, leaseErr
	}
	until = minTime(until, nativeUDPRetryDeadline(leaseUntil, cutoff))
	if ctx.Err() != nil || !time.Now().Before(until) {
		return true, rediscore.ErrExpired
	}
	return true, e.cfg.WriteNativeRetry(ctx, flow, response.WireBytes(), until)
}

// recordRedisServerFinalWritten retains one bounded native fact after exact W
// proof consumption. It never infers a write from a result, absence or timeout.
func recordRedisServerFinalWritten(ctx context.Context, store *rediscore.Store, selected selectedInitialInviteOutput) error {
	if selected.capture.proof.State != rediscore.OutputWritten || selected.capture.metadata.Status < 200 || selected.capture.metadata.Write.Flow.Transport != "udp" {
		return nil
	}
	procedure, err := readPrivateProcedureRecord(selected.selected.View.Records[selected.capture.metadata.Procedure])
	if err != nil {
		return err
	}
	response := strings.Replace(selected.capture.proof.MetadataField, "r:invite-output-", "r:invite-response-", 1)
	if strings.HasPrefix(selected.capture.proof.MetadataField, "r:dialog-output-") {
		response = strings.Replace(selected.capture.proof.MetadataField, "r:dialog-output-", "r:dialog-response-", 1)
	}
	if procedure.LastResponse != response || procedure.FinalResponseWrittenAcknowledged {
		return nil
	}
	procedure.FinalResponseWrittenAcknowledged = true
	encoded, err := marshalNativeRecord(storagecodec.NativeProcedure, procedure)
	if err != nil {
		return err
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: deadlineOr(ctx, time.Now().Add(2*time.Second)), SpendReservation: true, Changes: []rediscore.Change{{Field: selected.capture.metadata.Procedure, Value: encoded}}})
	return err
}
