package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

// replayRedisServerCancel never repeats the INVITE transition or its 487.
// The caller selected the exact retained CANCEL before considering fresh work.
func replayRedisServerCancel(ctx context.Context, store *rediscore.Store, selected *selectedRedisServerCancel, key []byte, deadline time.Time) error {
	if store == nil || selected == nil || selected.selected == nil || selected.context == nil || selected.prior == nil || selected.prior.Metadata == nil || !time.Now().Before(deadline) || ctx.Err() != nil {
		return errNativeRecord
	}
	prior := selected.prior
	// Reliable retired replies and expired UDP transactions are suppressed.
	if prior.Native.Reliable || prior.Native.Phase != portable.Completed || selected.flow.Transport != "udp" {
		return nil
	}
	j := prior.Native.Timers[portable.TimerJ]
	if !j.Active || j.Generation == 0 || j.At <= time.Now().UnixMilli() {
		return nil
	}
	if prior.Metadata.ProvisionalEmissions != 0 || prior.Metadata.Emissions >= stateProfileTransmissions {
		return nil
	}
	id := strings.TrimPrefix(selected.cancelField, "r:call-cancel-")
	outputField := "r:call-cancel-output-" + id
	if len(id) != 32 || selected.cancelField != "r:call-cancel-"+id || prior.Metadata.RequestId != id || len(prior.Metadata.Outputs) != 1 || prior.Metadata.Outputs[0] != outputField || selected.nextCancel.LastResponse == nil || len(selected.effects) != 1 || selected.effects[0].Kind != portable.Send || !selected.effects[0].Replay || selected.effects[0].Message == nil || !selected.effects[0].Message.EqualWire(*selected.nextCancel.LastResponse) {
		return errNativeRecord
	}
	output, err := decodeNativeOutput(selected.selected.View.Records[outputField])
	if err != nil {
		return err
	}
	if output.Method != "CANCEL" || output.Status != 200 && output.Status != 481 || output.Procedure != selected.cancelField || output.Dialog != "" || output.Result != "" || output.Operation != "" || !proto.Equal(output.Write.Effect.Context, selected.context.Context) || !proto.Equal(output.Write.Flow, prior.Metadata.Flow) || !proto.Equal(prior.Metadata.Flow, selected.flow) || !slices.Contains(selected.context.NoninitialProcedures, selected.cancelField) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(output.Write)
	if err != nil || ref.RequireApplication {
		return errNativeRecord
	}
	_, err = store.InspectOutput(ctx, ref)
	if err == nil {
		return nil
	}
	if !errors.Is(err, rediscore.ErrAbsent) {
		return err
	}
	if !prior.Metadata.OutputConsumed {
		return nil
	}
	snapshot, err := nativeProjectSnapshot(selected.context, nil, nil, nil)
	if err != nil {
		return err
	}
	until := minTime(deadline, time.Now().Add(2*time.Second))
	until = minTime(until, time.UnixMilli(j.At))
	intent, changes, shared, err := prepareRedisCallResponseOutput("CANCEL", output.Status, snapshot, selected.flow, key, *selected.nextCancel.LastResponse, selected.cancelField, "", selected.cancelResponse, "r:call-cancel-wire-"+id, outputField, until, true)
	if err != nil {
		return err
	}
	// Flow equality above preserves the previously retained logical/physical
	// response selection; a replay cannot introduce an unindexed wire variant.
	if !shared && len(selected.selected.View.Records["r:call-cancel-wire-"+id]) == 0 {
		return errNativeRecord
	}
	metadata := proto.Clone(prior.Metadata).(*nativepb.Procedure)
	metadata.Emissions++
	metadata.OutputConsumed = false
	data, err := encodeNativeProcedure(selected.nextCancel, prior.Refs, metadata)
	if err != nil {
		return err
	}
	changes = append(changes, rediscore.Change{Field: selected.cancelField, Value: data})
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.selected.Identity, Revision: selected.selected.View.Revision, Deadline: until, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent},
		Timers: &rediscore.TimerMutation{Expected: selected.selected.View.Timer, Target: selected.selected.View.Timer, Changes: []rediscore.TimerChange{{Member: "out-" + outputField[2:], Due: until}}}})
	return err
}
