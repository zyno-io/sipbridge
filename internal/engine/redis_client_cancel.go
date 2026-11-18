package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// redisClientCancelID is derived from the exact parent native transaction.
// Both parent retirement and child creation use this same bounded identity.
func redisClientCancelID(key []byte, current *nativepb.Context, parentKey string) (string, error) {
	if len(key) < 32 || len(key) > 1024 || current == nil || current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || parentKey == "" {
		return "", errNativeRecord
	}
	authority := current.Context.State.Authority
	return nativeIdentity(key, "cancel-client", authority.PrimaryId, current.Context.State.Incarnation, current.Context.Id, parentKey)[:32], nil
}

type redisPairedClientCancel struct {
	field     string
	changes   []rediscore.Change
	outputs   []rediscore.OutputIntent
	selectors []rediscore.SelectorChange
	timers    []rediscore.TimerChange
}

// prepareRedisPairedClientCancel constructs only the actual native child
// emitted by RequestCancel or by the subsequent provisional response. Its
// deterministic identity lets the INVITE retain its admitted credit until
// this child's own output and transaction duties are closed.
func prepareRedisPairedClientCancel(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, parentField string, parent *nativepb.Procedure, invite portable.Transaction, effects []portable.Effect, key []byte, deadline time.Time) (redisPairedClientCancel, error) {
	var out redisPairedClientCancel
	if store == nil || current == nil || current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || parent == nil || !slices.Contains(current.InitialProcedures, parentField) && !slices.Contains(current.NoninitialProcedures, parentField) || parent.Native == nil || parent.Native.Family != string(portable.InviteClient) || parent.Native.Key != invite.Key || !validPolicyFlow(parent.Flow) || len(key) < 32 || len(key) > 1024 || !time.Now().Before(deadline) {
		return out, errNativeRecord
	}
	now := time.Now()
	tx, started, err := nativePairedCancel(invite, effects, now.UnixMilli())
	if err != nil || tx == nil {
		return out, err
	}
	if !invite.Cancelled || !invite.CancelSent || len(current.InitialProcedures)+len(current.NoninitialProcedures) >= stateNativeProcedures {
		return out, errNativeRecord
	}
	authority := current.Context.State.Authority
	id, err := redisClientCancelID(key, current, invite.Key)
	if err != nil {
		return out, err
	}
	procField, originField, outputField := "r:cancel-client-"+id, "r:cancel-client-origin-"+id, "r:cancel-client-output-"+id
	if slices.Contains(current.NoninitialProcedures, procField) {
		return out, rediscore.ErrConflict
	}
	absent, err := store.Read(ctx, identity, procField, originField, outputField)
	if err != nil {
		return out, err
	}
	if absent.Revision != revision || absent.Timer != assignment || len(absent.Records[procField])+len(absent.Records[originField])+len(absent.Records[outputField]) != 0 {
		return out, rediscore.ErrConflict
	}
	selectors, err := nativeSelectorIDs(tx.Origin, true, false)
	if err != nil || len(selectors) == 0 || selectors[0].Kind != rediscore.SelectorClientTransaction {
		return out, errNativeRecord
	}
	facts, err := tx.Origin.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.CANCEL || !portable.MatchRequest(invite.Origin, tx.Origin) {
		return out, errNativeRecord
	}
	snapshot, err := nativeProjectSnapshot(current, nil, nil, nil)
	if err != nil {
		return out, err
	}
	until := minTime(deadline, now.Add(2*time.Second))
	if !now.Before(until) {
		return out, rediscore.ErrExpired
	}
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, current.Context.State.GroupId, current.Context.State.Incarnation, current.Context.State.CallId, current.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_TERMINAL_CLEANUP, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: current.ApplicationLeaseTerm, CompletionScope: scope}
	hash, err := SendFingerprint(snapshot, &pb.SipMessage{Method: "CANCEL", Wire: tx.Origin.WireBytes()}, auth, parent.Flow)
	if err != nil {
		return out, err
	}
	ref.Id, auth.Fingerprint = "e-"+hash, hash
	write := &pb.FlowWrite{Effect: ref, Flow: parent.Flow, Permit: &pb.SendPermit{Authorization: auth, Flow: parent.Flow, DirectoryEpoch: authority.PrimaryId, OrderingProof: nativeIdentity(key, "native-write-authority-v1", hash), CompletionScope: scope}}
	drain := drainNativeMessageObservation(tx.Origin, facts)
	output := &nativepb.Output{Write: write, Procedure: procField, Method: "CANCEL", Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	intent, changes, err := nativePreparePhysicalOutput(output, tx.Origin, originField, outputField, nil, nil)
	if err != nil {
		return out, err
	}
	metadata := &nativepb.Procedure{Flow: proto.Clone(parent.Flow).(*pb.FlowRef), Cleanup: true, Emissions: 1, Outputs: []string{outputField}}
	if parent.Application != nil {
		metadata.Application = proto.Clone(parent.Application).(*pb.ProcessRef)
	}
	data, err := encodeNativeProcedure(*tx, nativeProcedureRefs{Origin: originField}, metadata)
	if err != nil {
		return out, err
	}
	changes = append(changes, rediscore.Change{Field: procField, Value: data})
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}
	sends := 0
	for _, effect := range started {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || !effect.Message.Equal(tx.Origin) || effect.Replay {
				return out, errNativeRecord
			}
			sends++
		case portable.Schedule:
			if effect.Timer != portable.TimerE {
				timers = append(timers, rediscore.TimerChange{Member: "cancel-client-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			}
		default:
			return out, errors.New("unexpected paired CANCEL effect")
		}
	}
	if sends != 1 {
		return out, errNativeRecord
	}
	out = redisPairedClientCancel{field: procField, changes: changes, outputs: []rediscore.OutputIntent{intent}, selectors: []rediscore.SelectorChange{{Selector: selectors[0], Record: procField}}, timers: timers}
	return out, nil
}

// selectRedisClientCancelParent derives the existing INVITE client selector
// from the exact selected CANCEL via the pinned SDK client-key rule. It never
// treats an SDK key as a Redis field name.
func selectRedisClientCancelParent(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, childField, childKey, childOrigin string, childFlow *pb.FlowRef, cancel portable.Message, key []byte) (*rediscore.SelectorLookup, error) {
	if store == nil || len(key) < 32 || len(key) > 1024 || current == nil || current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || !slices.Contains(current.NoninitialProcedures, childField) || !validPolicyFlow(childFlow) {
		return nil, errNativeRecord
	}
	facts, err := cancel.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.CANCEL || facts.ViaBranch == "" {
		return nil, errNativeRecord
	}
	selectedKey, err := cancel.ClientTxKey()
	if err != nil || selectedKey != facts.ViaBranch+sip.TxSeperator+string(sip.CANCEL) || childKey != selectedKey {
		return nil, errNativeRecord
	}
	parentKey := facts.ViaBranch + sip.TxSeperator + string(sip.INVITE)
	selected, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorClientTransaction, parentKey))
	if err != nil {
		return nil, err
	}
	if selected.Identity != identity || selected.View.Revision != revision || selected.View.Timer != assignment {
		return nil, rediscore.ErrConflict
	}
	if !slices.Contains(current.InitialProcedures, selected.Record) && !slices.Contains(current.NoninitialProcedures, selected.Record) {
		return nil, errNativeRecord
	}
	parent, err := readPrivateProcedureRecord(selected.View.Records[selected.Record])
	if err != nil || parent.Native.Version != portable.Version || parent.Native.Family != string(portable.InviteClient) || parent.Native.Key != parentKey || !proto.Equal(parent.Flow, childFlow) {
		return nil, errNativeRecord
	}
	expected, err := redisClientCancelID(key, current, parentKey)
	if err != nil {
		return nil, err
	}
	if childField != "r:cancel-client-"+expected || childOrigin != "r:cancel-client-origin-"+expected {
		return nil, errNativeRecord
	}
	view, err := store.Read(ctx, identity, parent.Origin)
	if err != nil {
		return nil, err
	}
	if view.Revision != revision || view.Timer != assignment {
		return nil, rediscore.ErrConflict
	}
	origin, err := selectedmessage.DecodeOpaque(view.Records[parent.Origin])
	if err != nil || !portable.MatchRequest(origin, cancel) {
		return nil, errNativeRecord
	}
	return selected, nil
}
