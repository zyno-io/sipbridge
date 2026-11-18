package engine

import (
	"context"
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
)

type redisOwnerByeStage struct {
	dialog    []byte
	changes   []rediscore.Change
	outputs   []rediscore.OutputIntent
	selectors []rediscore.SelectorChange
	timers    []rediscore.TimerChange
}

// prepareRedisOwnerBye creates one actual native cleanup continuation. Its
// caller publishes the returned state together with the exact selected dialog.
func prepareRedisOwnerBye(ctx context.Context, store *rediscore.Store, identity rediscore.Identity, revision uint64, assignment rediscore.TimerAssignment, current *nativepb.Context, dialogField string, dialog portable.Dialog, refs nativeDialogRefs, metadata *nativepb.Dialog, flow *pb.FlowRef, key []byte, now, deadline time.Time) (*redisOwnerByeStage, error) {
	if store == nil || current == nil || current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || current.Application == nil || metadata == nil || len(key) < 32 || len(key) > 1024 || !validPolicyFlow(flow) || flow.Transport != dialog.Transport || !slices.Contains(current.Dialogs, dialogField) || len(current.NoninitialProcedures)+len(current.InitialProcedures) >= stateNativeProcedures || !time.Now().Before(deadline) {
		return nil, errNativeRecord
	}
	branch := sip.RFC3261BranchMagicCookie + nativeIdentity(key, "owner-loss-bye", identity.Incarnation, dialogField)[:32]
	candidate, eligible, err := nativePrepareOwnerBye(dialog, metadata.AckWritten, false, branch, now.UnixMilli())
	if err != nil || !eligible {
		return nil, err
	}
	authority := current.Context.State.Authority
	id := nativeIdentity(key, "call-ack-timeout-bye", authority.PrimaryId, identity.Incarnation, identity.Context, dialogField)[:32]
	procedureField, wireField, outputField := "r:call-cleanup-"+id, "r:call-cleanup-origin-"+id, "r:call-cleanup-output-"+id
	occupied, err := store.Read(ctx, identity, procedureField, wireField, outputField)
	if err != nil {
		return nil, err
	}
	if occupied.Revision != revision || occupied.Timer != assignment {
		return nil, rediscore.ErrConflict
	}
	if len(occupied.Records[procedureField])+len(occupied.Records[wireField])+len(occupied.Records[outputField]) != 0 || slices.Contains(current.NoninitialProcedures, procedureField) || slices.Contains(metadata.Outputs, outputField) {
		return nil, rediscore.ErrConflict
	}
	ids, err := nativeSelectorIDs(candidate.Transaction.Origin, true, false)
	if err != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorClientTransaction {
		return nil, errNativeRecord
	}
	procedure, err := encodeNativeProcedure(candidate.Transaction, nativeProcedureRefs{Origin: wireField}, &nativepb.Procedure{Flow: proto.Clone(flow).(*pb.FlowRef), Application: proto.Clone(current.Application).(*pb.ProcessRef), Cleanup: true, Emissions: 1})
	if err != nil {
		return nil, err
	}
	refs.LocalBye = wireField
	dialogMetadata := proto.Clone(metadata).(*nativepb.Dialog)
	dialogMetadata.LocalTransaction = candidate.Transaction.Key
	dialogMetadata.Outputs = append(dialogMetadata.Outputs, outputField)
	slices.Sort(dialogMetadata.Outputs)
	dialogData, err := encodeNativeDialog(candidate.Dialog, refs, dialogMetadata)
	if err != nil {
		return nil, err
	}
	current.NoninitialProcedures = append(current.NoninitialProcedures, procedureField)
	slices.Sort(current.NoninitialProcedures)
	snapshot, err := nativeProjectSnapshot(current, &candidate.Dialog, nil, nil)
	if err != nil {
		return nil, err
	}
	until := minTime(deadline, now.Add(2*time.Second))
	ref := &pb.StateEffectRef{Context: snapshot.Context, AttemptId: "a-" + uuid.NewString()}
	scope := nativeIdentity(key, "native-completion-v1", authority.Service, authority.Generation, authority.PrimaryId, snapshot.Context.State.GroupId, snapshot.Context.State.Incarnation, snapshot.CallId, snapshot.Context.Id)
	auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: pb.EffectAuthorization_TERMINAL_CLEANUP, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: snapshot.ApplicationLeaseTerm, CompletionScope: scope}
	fingerprint, err := SendFingerprint(snapshot, &pb.SipMessage{Method: "BYE", Wire: candidate.Transaction.Origin.WireBytes()}, auth, flow)
	if err != nil {
		return nil, err
	}
	ref.Id, auth.Fingerprint = "e-"+fingerprint, fingerprint
	write := &pb.FlowWrite{Effect: ref, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: authority.PrimaryId, OrderingProof: nativeIdentity(key, "native-write-authority-v1", fingerprint), CompletionScope: scope}}
	facts, err := candidate.Transaction.Origin.Facts()
	if err != nil {
		return nil, err
	}
	drain := drainNativeMessageObservation(candidate.Transaction.Origin, facts)
	output := &nativepb.Output{Write: write, Procedure: procedureField, Dialog: dialogField, Method: "BYE", Drain: &nativepb.DrainObservation{Transaction: drain.Transaction[:], Ack: drain.ACK[:], Status: uint32(drain.Status), Request: drain.Request, Invite: drain.Invite, IsAck: drain.Ack, Cancel: drain.Cancel, Initial: drain.Initial}}
	intent, changes, err := nativePreparePhysicalOutput(output, candidate.Transaction.Origin, wireField, outputField, nil, nil)
	if err != nil {
		return nil, err
	}
	changes = append(changes, rediscore.Change{Field: procedureField, Value: procedure})
	timers := []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(outputField, "r:"), Due: until}}
	sends, finalTimers := 0, 0
	for _, effect := range candidate.Effects {
		switch effect.Kind {
		case portable.Send:
			if effect.Message == nil || !effect.Message.EqualWire(candidate.Transaction.Origin) {
				return nil, errNativeRecord
			}
			sends++
		case portable.Schedule:
			if effect.Timer == portable.TimerF {
				finalTimers++
				timers = append(timers, rediscore.TimerChange{Member: "call-client-" + id + "-" + string(effect.Timer), Due: time.UnixMilli(effect.Deadline.At)})
			} else if effect.Timer != portable.TimerE {
				return nil, errNativeRecord
			}
		default:
			return nil, errNativeRecord
		}
	}
	if sends != 1 || finalTimers != 1 {
		return nil, errNativeRecord
	}
	return &redisOwnerByeStage{dialog: dialogData, changes: changes, outputs: []rediscore.OutputIntent{intent}, selectors: []rediscore.SelectorChange{{Selector: ids[0], Record: procedureField}}, timers: timers}, nil
}
