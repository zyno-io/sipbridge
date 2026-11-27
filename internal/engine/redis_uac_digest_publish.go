package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
	"sipbridge/internal/storagecodec"
)

// redisUACDigestTicket is retained only by the active bounded ingress call.
// A lost process or reply never recreates a credential lookup or SIP attempt.
type redisUACDigestTicket struct {
	identity        rediscore.Identity
	revision        uint64
	assignment      rediscore.TimerAssignment
	procedure, root string
	response        string
	context         *pb.ContextRef
	application     *pb.ProcessRef
	policy          *pb.CommandAdmission
	operation       string
	deadline        time.Time
	candidate       *redisUACDigestCandidate
}

// publishRedisUACDigestChallenge commits the exact native failure ACK and
// challenge before credential I/O. A duplicate never launches another lookup.
func publishRedisUACDigestChallenge(ctx context.Context, store *rediscore.Store, flow *pb.FlowRef, message portable.Message, key []byte, executor rediscore.TimerAssignment, deadline time.Time, preselected ...*rediscore.SelectorLookup) (*redisUACDigestTicket, bool, error) {
	if store == nil || !validPolicyFlow(flow) || validateRedisNotifyResponseMetadata(message, flow) != nil || len(key) < 32 || len(key) > 1024 || ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, true, errNativeRecord
	}
	ids, err := nativeSelectorIDs(message, false, false)
	if err != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorClientTransaction {
		return nil, true, errNativeRecord
	}
	var selected *rediscore.SelectorLookup
	if len(preselected) > 1 {
		return nil, true, errNativeRecord
	}
	if len(preselected) == 1 {
		selected = preselected[0]
	}
	if selected == nil {
		selected, err = store.LookupSelector(ctx, ids[0], "r:context", "r:result-1")
		if errors.Is(err, rediscore.ErrAbsent) {
			return nil, false, nil
		}
		if err != nil {
			return nil, true, err
		}
	}
	if !strings.HasPrefix(selected.Record, "r:uac-procedure-") {
		return nil, true, errNativeRecord
	}
	id := strings.TrimPrefix(selected.Record, "r:uac-procedure-")
	if len(id) != 32 {
		return nil, true, errNativeRecord
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, true, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return nil, true, err
	}
	result, err := decodeNativeResult(selected.View.Records["r:result-1"])
	if err != nil {
		return nil, true, err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || current.Kind != pb.ContextSnapshot_UAC || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || !slices.Contains(current.InitialProcedures, selected.Record) || procedure.Native.Version != portable.Version || procedure.Native.Family != portable.InviteClient || procedure.Refs.Origin != "r:uac-origin-"+id || procedure.Metadata.CommandResult != "r:result-1" || result.Sequence != 1 || !proto.Equal(procedure.Metadata.Flow, flow) {
		return nil, true, errNativeRecord
	}
	if procedure.Metadata.DigestHandled {
		handled, replayErr := replayRedisUACFailure(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, procedure, result.OperationId, message, flow, key, executor, deadline)
		return nil, handled, replayErr
	}
	if procedure.Metadata.Credential == nil || procedure.Metadata.Credential.Id == "" || procedure.Metadata.Credential.Version == "" || procedure.Metadata.DigestAttempts >= stateDigestRetryLimit || procedure.Metadata.DeadlineMs <= time.Now().UnixMilli() {
		return nil, false, nil
	}
	if current.Ended || current.Answered || procedure.Native.Cancelled || selected.Identity.Owner == "" || selected.Identity.Term == "" || current.ApplicationLeaseTerm != selected.Identity.Term || procedure.Metadata.Policy == nil || procedure.Metadata.Policy.DirectoryEpoch != current.DirectoryEpoch || !proto.Equal(procedure.Metadata.Policy.ExpectedDelivery, current.Delivery) {
		return nil, false, nil
	}
	app, err := nativeApplicationReference(key, current.Application)
	if err != nil || app != selected.Identity.Owner {
		return nil, true, rediscore.ErrAuthority
	}
	rootField := procedure.Metadata.Root
	if rootField == "" {
		rootField = selected.Record
	}
	if !slices.Contains(current.InitialProcedures, rootField) {
		return nil, true, errNativeRecord
	}
	root := &procedure
	var rootRecord *nativepb.Procedure
	if rootField != selected.Record {
		rootView, e := store.Read(ctx, selected.Identity, rootField)
		if e != nil {
			return nil, true, e
		}
		if rootView.Revision != selected.View.Revision || rootView.Timer != selected.View.Timer {
			return nil, true, rediscore.ErrConflict
		}
		value, e := decodeNativeProcedure(rootView.Records[rootField])
		if e != nil || value.Metadata.Root != "" || value.Metadata.CommandResult != "r:result-1" || value.Native.Family != portable.InviteClient {
			return nil, true, errNativeRecord
		}
		root = &value
		rootRecord, e = readPrivateProcedureRecord(rootView.Records[rootField])
		if e != nil {
			return nil, true, e
		}
	}
	fields := []string{procedure.Refs.Origin}
	if procedure.Refs.LastResponse != "" {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	for _, field := range root.Metadata.Events {
		if !slices.Contains(fields, field) {
			fields = append(fields, field)
		}
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if err != nil {
		return nil, true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return nil, true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(3)
	if err != nil {
		return nil, true, err
	}
	message, err = memo.Adopt(message, "admitted")
	if err != nil {
		return nil, true, err
	}
	messages := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	tx, err := procedure.attach(messages, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return nil, true, err
	}
	candidate, handled, err := prepareRedisUACDigest(key, selected.Identity, tx, procedure.Metadata, message, time.Now())
	if err != nil || !handled {
		return nil, handled, err
	}
	if candidate != nil {
		attributeNativeCall(ctx, current)
	}
	if candidate == nil {
		// Existing selected ACK/proof remains the only replay authority.
		handled, replayErr := replayRedisUACFailure(ctx, store, selected.Identity, view.Revision, view.Timer, current, selected.Record, procedure, result.OperationId, message, flow, key, executor, deadline)
		return nil, handled, replayErr
	}
	if procedure.Refs.ACK != "" || procedure.Metadata.AckEmissions >= stateProfileTransmissions || current.Revision >= 1<<53-1 {
		return nil, true, errNativeRecord
	}
	now := time.Now()
	if ctx.Err() != nil || !now.Before(deadline) || now.UnixMilli() >= procedure.Metadata.DeadlineMs || now.UnixMilli() >= int64(procedure.Metadata.Policy.ExpiresAtMs) {
		return nil, true, rediscore.ErrExpired
	}
	var changes []rediscore.Change
	var timers []rediscore.TimerChange
	retained := make([]string, 0, len(root.Metadata.Events))
	oldOwned := false
	for _, field := range root.Metadata.Events {
		event, e := decodeNativeLifecycle(view.Records[field])
		if e != nil || event.Root != rootField || event.Message == "" {
			return nil, true, errNativeRecord
		}
		if event.Kind == pb.DialogEvent_PROVISIONAL {
			if len(event.Owners) > 1 || len(event.Owners) == 1 && (event.Owners[0] != selected.Record || event.Message != procedure.Refs.LastResponse) {
				return nil, true, errNativeRecord
			}
			changes = append(changes, rediscore.Change{Field: field, Delete: true})
			timers = append(timers, rediscore.TimerChange{Member: "event-" + strings.TrimPrefix(field, "r:event-"), Delete: true})
			if len(event.Owners) == 0 && event.Message != procedure.Refs.LastResponse {
				changes = append(changes, rediscore.Change{Field: event.Message, Delete: true})
			}
			continue
		}
		retained = append(retained, field)
		if event.Message == procedure.Refs.LastResponse {
			oldOwned = true
			if slices.Contains(event.Owners, selected.Record) {
				return nil, true, errNativeRecord
			}
		}
	}
	root.Metadata.Events = retained
	if procedure.Refs.LastResponse != "" && !oldOwned {
		changes = append(changes, rediscore.Change{Field: procedure.Refs.LastResponse, Delete: true})
	}
	responseField := "r:uac-response-" + uuid.NewString()
	response, err := selectedmessage.Marshal(message)
	if err != nil {
		return nil, true, err
	}
	changes = append(changes, rediscore.Change{Field: responseField, Value: response})
	procedure.Refs.LastResponse = responseField
	ackField, outputField := "r:uac-ack-"+id, "r:uac-ack-output-"+uuid.NewString()
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return nil, true, err
	}
	until := minTime(deadline, now.Add(2*time.Second))
	intent, outputChanges, err := prepareRedisUACFailureACK(snapshot, flow, key, *candidate.failure.ACK, selected.Record, "", "", ackField, outputField, result.OperationId, until, false, false)
	if err != nil {
		return nil, true, err
	}
	changes = append(changes, outputChanges...)
	procedure.Refs.ACK = ackField
	procedure.Metadata.AckEmissions++
	procedure.Metadata.DigestHandled = true
	procedure.Metadata.DigestAttempts = candidate.work.Attempt
	procedure.Metadata.Outputs = append(procedure.Metadata.Outputs, outputField)
	slices.Sort(procedure.Metadata.Outputs)
	timers = append(timers, rediscore.TimerChange{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until})
	sends := 0
	for _, effect := range candidate.failure.Effects {
		switch effect.Kind {
		case portable.CancelTimer:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Delete: true})
			}
		case portable.Schedule:
			if effect.Timer != portable.TimerA {
				timers = append(timers, rediscore.TimerChange{Member: "call-tx-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		case portable.Send:
			if effect.Message == nil || !effect.Message.Equal(*candidate.failure.ACK) || effect.Replay {
				return nil, true, errNativeRecord
			}
			sends++
		case portable.ReportResponse, portable.Ended:
		default:
			return nil, true, errNativeRecord
		}
	}
	if sends != 1 {
		return nil, true, errNativeRecord
	}
	current.Revision++
	contextData, err := encodeNativeContext(current)
	if err != nil {
		return nil, true, err
	}
	procedureData, err := encodeNativeProcedure(candidate.failure.Transaction, procedure.Refs, procedure.Metadata)
	if err != nil {
		return nil, true, err
	}
	changes = append(changes, rediscore.Change{Field: "r:context", Value: contextData}, rediscore.Change{Field: selected.Record, Value: procedureData})
	if rootField != selected.Record {
		rootRecord.Events = retained
		rootData, e := marshalNativeRecord(storagecodec.NativeProcedure, rootRecord)
		if e != nil {
			return nil, true, e
		}
		changes = append(changes, rediscore.Change{Field: rootField, Value: rootData})
	}
	target := view.Timer
	if target == (rediscore.TimerAssignment{}) {
		target = executor
	}
	publication := rediscore.Publication{Identity: selected.Identity, Revision: view.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, NewOutputs: []rediscore.OutputIntent{intent}, Timers: &rediscore.TimerMutation{Expected: view.Timer, Target: target, Changes: timers}}
	if flow.Transport == "udp" {
		worker, e := nativeWorkerReference(flow.Ingress)
		if e != nil {
			return nil, true, e
		}
		publication.UDPHint = &rediscore.UDPHint{Worker: worker, Context: selected.Identity.Context, Incarnation: selected.Identity.Incarnation, Procedure: selected.Record, Stop: true}
	}
	revision, err := store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrExpired) && view.Timer != (rediscore.TimerAssignment{}) && view.Timer != executor && ctx.Err() == nil && time.Now().Before(deadline) {
		publication.Timers.Target = executor
		revision, err = store.Commit(ctx, publication)
	}
	if err != nil {
		return nil, true, err
	}
	return &redisUACDigestTicket{identity: selected.Identity, revision: revision, assignment: publication.Timers.Target, procedure: selected.Record, root: rootField, response: responseField, context: proto.Clone(current.Context).(*pb.ContextRef), application: proto.Clone(current.Application).(*pb.ProcessRef), policy: proto.Clone(procedure.Metadata.Policy).(*pb.CommandAdmission), operation: result.OperationId, deadline: time.UnixMilli(procedure.Metadata.DeadlineMs), candidate: candidate}, true, nil
}
