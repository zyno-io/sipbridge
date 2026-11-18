package engine

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

type selectedRedisServerCancel struct {
	selected       *rediscore.SelectorLookup
	context        *nativepb.Context
	inviteField    string
	cancelField    string
	cancelOrigin   string
	cancelResponse string
	invite         nativeProcedureRecord
	prior          *nativeProcedureRecord
	nextInvite     portable.Transaction
	nextCancel     portable.Transaction
	effects        []portable.Effect
	flow           *pb.FlowRef
	messages       *nativeSelectedMessages
}

// selectRedisServerCancel computes only the exact SDK pair. Publication and
// physical completion remain separate; no application lease or callback runs.
func selectRedisServerCancel(ctx context.Context, store *rediscore.Store, local *pb.ProcessRef, flow *pb.FlowRef, message portable.Message, key []byte, deadline time.Time) (*selectedRedisServerCancel, bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return nil, false, err
	}
	if !facts.IsRequest || facts.Method != sip.CANCEL {
		return nil, false, nil
	}
	if store == nil || !validSendProcess(local) || !validPolicyFlow(flow) || !sameProcess(flow.Ingress, local) || message.Transport() != flow.Transport || message.Source() != net.JoinHostPort(flow.SourceIp, strconv.Itoa(int(flow.SourcePort))) || len(key) < 32 || len(key) > 1024 || ctx.Err() != nil || !time.Now().Before(deadline) {
		return nil, true, errNativeRecord
	}
	inviteKey, err := message.CancelServerTxKey()
	if err != nil {
		return nil, true, err
	}
	selected, err := store.LookupSelector(ctx, nativeSelector(rediscore.SelectorServerTransaction, inviteKey), "r:context")
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	current, err := decodeNativeContext(selected.View.Records["r:context"])
	if err != nil {
		return nil, true, err
	}
	procedure, err := decodeNativeProcedure(selected.View.Records[selected.Record])
	if err != nil {
		return nil, true, err
	}
	if current.Context == nil || current.Context.State == nil || current.Context.State.Authority == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || current.CallId != facts.CallID || !slices.Contains(current.InitialProcedures, selected.Record) && !slices.Contains(current.NoninitialProcedures, selected.Record) || procedure.Native.Version != portable.Version || procedure.Native.Family != portable.InviteServer || procedure.Native.Key != inviteKey || procedure.Refs.Origin == "" || !validPolicyFlow(procedure.Metadata.Flow) || !proto.Equal(procedure.Metadata.Flow, flow) && (procedure.Metadata.Flow.Transport != "tls" || flow.Transport != "tls" || current.Transport != "tls") {
		return nil, true, errNativeRecord
	}
	cancelKey, err := message.ServerTxKey()
	if err != nil {
		return nil, true, err
	}
	id := redisServerCancelID(key, current.Context.State.Authority.PrimaryId, selected.Identity, cancelKey)
	field, origin, response := "r:call-cancel-"+id, "r:call-cancel-origin-"+id, "r:call-cancel-final-"+id
	fields := []string{procedure.Refs.Origin, field, origin, response,
		"r:call-cancel-output-" + id, "r:call-cancel-wire-" + id,
		"r:call-cancel-invite-output-" + id, "r:call-cancel-invite-wire-" + id, "r:call-cancel-invite-final-" + id,
		"r:dialog-output-" + procedure.Metadata.RequestId + "-0", "r:dialog-wire-" + procedure.Metadata.RequestId + "-0", "r:dialog-response-" + procedure.Metadata.RequestId + "-0"}
	if procedure.Refs.LastResponse != "" && !slices.Contains(fields, procedure.Refs.LastResponse) {
		fields = append(fields, procedure.Refs.LastResponse)
	}
	view, err := store.Read(ctx, selected.Identity, fields...)
	if errors.Is(err, rediscore.ErrAbsent) {
		return nil, true, rediscore.ErrConflict
	}
	if err != nil {
		return nil, true, err
	}
	if view.Revision != selected.View.Revision || view.Timer != selected.View.Timer {
		return nil, true, rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(5)
	if err != nil {
		return nil, true, err
	}
	if _, err = memo.Adopt(message, "admitted"); err != nil {
		return nil, true, err
	}
	source := &nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	invite, err := procedure.attach(source, true, procedure.Refs.LastResponse != "", false)
	if err != nil {
		return nil, true, err
	}
	if !portable.MatchRequest(invite.Origin, message) {
		return nil, true, errNativeRecord
	}
	var prior *nativeProcedureRecord
	var nativeCancel *portable.Transaction
	if len(view.Records[field]) != 0 {
		value, e := decodeNativeProcedure(view.Records[field])
		if e != nil {
			return nil, true, e
		}
		if !slices.Contains(current.NoninitialProcedures, field) || value.Native.Family != portable.NonInviteServer || value.Native.Key != cancelKey || value.Refs.Origin != origin || value.Refs.LastResponse != response || !validPolicyFlow(value.Metadata.Flow) || !proto.Equal(value.Metadata.Flow, flow) && (value.Metadata.Flow.Transport != "tls" || flow.Transport != "tls" || current.Transport != "tls") {
			return nil, true, errNativeRecord
		}
		native, e := value.attach(source, true, true, false)
		if e != nil {
			return nil, true, e
		}
		if !portable.MatchRequest(native.Origin, message) || !native.Origin.EqualWire(message) {
			return nil, true, errNativeRecord
		}
		prior, nativeCancel = &value, &native
	} else if len(view.Records[origin]) != 0 || len(view.Records[response]) != 0 || slices.Contains(current.NoninitialProcedures, field) {
		return nil, true, errNativeRecord
	}
	nextInvite, nextCancel, effects, err := portable.StepServerCancel(invite, nativeCancel, portable.Input{Kind: portable.ReceiveRequest, Message: message, Now: time.Now().UnixMilli()})
	if err != nil {
		return nil, true, err
	}
	// Keep only the selected metadata and the one operation's immutable values.
	selected.View = view
	return &selectedRedisServerCancel{selected: selected, context: current, inviteField: selected.Record, cancelField: field,
		cancelOrigin: origin, cancelResponse: response, invite: procedure, prior: prior, nextInvite: nextInvite, nextCancel: nextCancel,
		effects: effects, flow: proto.Clone(flow).(*pb.FlowRef), messages: source}, true, nil
}

// redisServerCancelID identifies the one actual CANCEL child of an INVITE.
func redisServerCancelID(key []byte, authority string, identity rediscore.Identity, cancelKey string) string {
	return nativeIdentity(key, "call-server-cancel", authority, identity.Context, identity.Incarnation, cancelKey)[:32]
}
