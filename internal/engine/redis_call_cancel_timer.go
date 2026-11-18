package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// fireRedisServerCancelJ performs only the exact native Timer J transition.
// Physical cleanup reselects the output and never treats J as delivery proof.
func fireRedisServerCancelJ(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) error {
	const prefix = "call-cancel-tx-"
	if store == nil || due == nil || !strings.HasPrefix(due.Member, prefix) || !strings.HasSuffix(due.Member, "-J") || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	id := strings.TrimSuffix(strings.TrimPrefix(due.Member, prefix), "-J")
	if len(id) != 32 || due.Member != prefix+id+"-J" {
		return errNativeRecord
	}
	procedureField, originField, responseField, outputField := "r:call-cancel-"+id, "r:call-cancel-origin-"+id, "r:call-cancel-final-"+id, "r:call-cancel-output-"+id
	view, err := store.Read(ctx, due.Identity, "r:context", procedureField, originField, responseField, outputField)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil {
		return err
	}
	procedure, err := decodeNativeProcedure(view.Records[procedureField])
	if err != nil {
		return err
	}
	j := procedure.Native.Timers[portable.TimerJ]
	if current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !slices.Contains(current.NoninitialProcedures, procedureField) || procedure.Metadata.RequestId != id || procedure.Native.Version != portable.Version || procedure.Native.Family != portable.NonInviteServer || procedure.Native.Reliable || procedure.Native.Phase != portable.Completed || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || !j.Active || j.Generation == 0 || j.At != due.DeadlineMS || len(procedure.Metadata.Outputs) != 1 || procedure.Metadata.Outputs[0] != outputField {
		return errNativeRecord
	}
	output, err := decodeNativeOutput(view.Records[outputField])
	if err != nil || output.Procedure != procedureField || output.Method != "CANCEL" || output.Status != 200 && output.Status != 481 || !proto.Equal(output.Write.Effect.Context, current.Context) || !proto.Equal(output.Write.Flow, procedure.Metadata.Flow) {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	next, effects, err := portable.StepTransaction(tx, portable.Input{Kind: portable.TimerFired, Timer: portable.TimerJ, Generation: j.Generation, Now: due.CheckedAtMS})
	if err != nil {
		return err
	}
	if next.Phase != portable.Terminated {
		return errNativeRecord
	}
	for _, effect := range effects {
		if effect.Kind != portable.Ended && (effect.Kind != portable.CancelTimer || effect.Timer != portable.TimerJ) {
			return errors.New("unexpected server CANCEL Timer J effect")
		}
	}
	data, err := encodeNativeProcedure(next, procedure.Refs, procedure.Metadata)
	if err != nil {
		return err
	}
	timers := []rediscore.TimerChange{{Member: due.Member, Delete: true}, {Member: "out-" + outputField[2:], Due: time.UnixMilli(int64(output.Write.Permit.Authorization.DeadlineMs))}}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true,
		Changes: []rediscore.Change{{Field: procedureField, Value: data}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
	return err
}
