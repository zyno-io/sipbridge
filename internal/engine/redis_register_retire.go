package engine

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/state"
)

// retireRedisRegisterWritten removes the exact finished reliable REGISTER only
// after the persisted dispatcher marker exists. Terminal preflight refuses any
// additional record, selector, timer or still-pinned physical output.
func retireRedisRegisterWritten(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, metadataField string, deadline time.Time) error {
	return retireRedisRegister(ctx, store, write, metadataField, deadline, false, false)
}

func retireRedisRegisterPostJ(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, metadataField string, deadline time.Time) error {
	return retireRedisRegister(ctx, store, write, metadataField, deadline, false, true)
}

func retireRedisRegister(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, metadataField string, deadline time.Time, obsolete, postJ bool) error {
	if store == nil || write == nil || write.Effect == nil || write.Effect.Context == nil || write.Effect.Context.State == nil || !nativeMessageField(metadataField) || metadataField == "" || !time.Now().Before(deadline) {
		return errNativeRecord
	}
	contextID, inc := write.Effect.Context.Id, write.Effect.Context.State.Incarnation
	first, err := store.ObserveContext(ctx, contextID, inc, "r:context", metadataField)
	if err != nil {
		return err
	}
	current, err := decodeNativeContext(first.View.Records["r:context"])
	if err != nil {
		return err
	}
	metadata, err := decodeNativeOutput(first.View.Records[metadataField])
	if err != nil {
		return err
	}
	if !proto.Equal(metadata.Write, write) || !nativeRegisterOrRefusalOutput(current, metadata) || !strings.HasPrefix(metadata.Procedure, "r:procedure-") || !proto.Equal(current.Context, write.Effect.Context) || current.Kind != pb.ContextSnapshot_TRANSACTION {
		return errors.New("REGISTER retirement identity changed")
	}
	requestID := strings.TrimPrefix(metadata.Procedure, "r:procedure-")
	if !state.ValidReference(requestID) || len(requestID) > 180 {
		return errNativeRecord
	}
	originField, responseField := "r:origin-"+requestID, "r:response-"+requestID
	fields := []string{"r:context", metadataField, metadata.Procedure, originField, responseField}
	currentResult := ""
	if current.CommandSequence != 0 {
		currentResult = "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
		fields = append(fields, currentResult)
	}
	if metadataField != "r:output-"+strconv.FormatUint(current.CommandSequence, 10) || (metadata.Result != "" && metadata.Result != currentResult) {
		return rediscore.ErrConflict
	}
	selected, err := store.ObserveContext(ctx, contextID, inc, fields...)
	if err != nil {
		return err
	}
	if selected.View.Revision != first.View.Revision || selected.Identity != first.Identity {
		return rediscore.ErrConflict
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[metadata.Procedure])
	if err != nil {
		return err
	}
	if procedure.Metadata.RequestId != requestID || (!obsolete && !procedure.Metadata.FinalResponseWrittenAcknowledged) || (obsolete && (!current.Ended || procedure.Metadata.FinalResponseWrittenAcknowledged)) || procedure.Metadata.DecisionPending || procedure.Refs.Origin != originField || procedure.Refs.LastResponse != responseField || procedure.Native.Phase != portable.Terminated {
		return errors.New("REGISTER retirement lacks exact native marker")
	}
	if currentResult != "" {
		result, e := decodeNativeResult(selected.View.Records[currentResult])
		if e != nil || result.Sequence != current.CommandSequence {
			return errors.New("REGISTER retirement result changed")
		}
	}
	memo, err := selectedmessage.NewMemo(2)
	if err != nil {
		return err
	}
	tx, err := procedure.attach(&nativeSelectedMessages{records: selected.View.Records, memo: memo, provenance: "admitted"}, true, true, false)
	if err != nil {
		return err
	}
	if tx.LastResponse == nil {
		return errNativeRecord
	}
	origin, e1 := tx.Origin.Facts()
	final, e2 := tx.LastResponse.Facts()
	if e1 != nil || e2 != nil || !origin.IsRequest || string(origin.Method) != metadata.Method || final.IsRequest || final.Status != int(metadata.Status) || !portable.MatchResponse(tx.Origin, *tx.LastResponse) || (!obsolete && !postJ && !nativeReliableRegisterOrRefusalMayRetire(current, metadata.Method, metadata.Status, write.Flow.Transport)) {
		return errors.New("REGISTER retirement transaction changed")
	}
	if postJ && (write.Flow.Transport != "udp" || tx.Reliable || tx.Family != portable.NonInviteServer) {
		return errors.New("REGISTER post-J transport changed")
	}
	if obsolete || postJ {
		for _, timer := range tx.Timers {
			if timer.Active {
				return errors.New("REGISTER terminal transaction has an active timer")
			}
		}
	}
	serverKey, err := tx.Origin.ServerTxKey()
	if err != nil {
		return err
	}
	// An unpaired CANCEL refusal owns CANCEL's server transaction, not the
	// INVITE route used for ordinary paired cancellation lookup.
	selectors := []rediscore.Selector{nativeSelector(rediscore.SelectorServerTransaction, serverKey)}
	changes := make([]rediscore.Change, 0, len(fields)+1)
	seen := make(map[string]bool)
	for _, field := range append(fields, "r:wire-"+strconv.FormatUint(current.CommandSequence, 10), "r:tw-"+requestID) {
		if !seen[field] {
			changes = append(changes, rediscore.Change{Field: field, Delete: true})
			seen[field] = true
		}
	}
	if selected.View.Timer.Worker == "" {
		return rediscore.ErrConflict
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: selected.Identity, Revision: selected.View.Revision, Terminal: true, Deadline: deadline, Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: metadata.Procedure, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: selected.View.Timer, Target: selected.View.Timer, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(metadataField, "r:"), Delete: true}}}})
	return err
}
