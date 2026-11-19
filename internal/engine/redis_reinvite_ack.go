package engine

import (
	"context"
	"slices"
	"strings"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// An ACK event has its own frozen wire, independent of the dialog's current
// remote INVITE. The exact server procedure keeps it reachable until settlement.
func validReinviteACKLifecycle(record *nativepb.Lifecycle, field string) bool {
	return record != nil && record.Kind == pb.DialogEvent_ACKNOWLEDGED && record.Message != "" &&
		strings.HasPrefix(field, "r:event-") && record.Message == "r:event-ack-"+strings.TrimPrefix(field, "r:event-") &&
		record.Root != "" && record.OperationId == "" && record.Error == "" && len(record.Owners) == 0
}

func projectRedisReinviteACK(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, revision uint64, field string, record *nativepb.Lifecycle, public *pb.DialogEvent) error {
	if !validReinviteACKLifecycle(record, field) {
		return errNativeRecord
	}
	view, err := store.Read(ctx, due.Identity, "r:context", record.Root, record.Message)
	if err != nil {
		return err
	}
	if view.Revision != revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	current, err := decodeNativeContext(view.Records["r:context"])
	if err != nil || !proto.Equal(current.Context, record.Snapshot.Context) || current.Kind != record.Snapshot.Kind || current.CallId != record.Snapshot.CallId || !slices.Contains(current.NoninitialProcedures, record.Root) || current.ApplicationLeaseTerm != record.Snapshot.ApplicationLeaseTerm || !proto.Equal(current.Application, record.Snapshot.Application) {
		return errNativeRecord
	}
	owner, err := readPrivateProcedureRecord(view.Records[record.Root])
	if err != nil {
		return err
	}
	id := strings.TrimPrefix(record.Root, "r:procedure-")
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id || record.Root != "r:procedure-"+id || owner.RequestId != id || owner.Native.Version != portable.Version || owner.Native.Family != string(portable.InviteServer) || !slices.Contains(owner.Events, field) {
		return errNativeRecord
	}
	memo, err := selectedmessage.NewMemo(1)
	if err != nil {
		return err
	}
	source := nativeSelectedMessages{records: view.Records, memo: memo, provenance: "admitted"}
	message, err := source.get(record.Message)
	if err != nil {
		return err
	}
	facts, err := message.Facts()
	if err != nil || !facts.IsRequest || facts.Method != sip.ACK || facts.CSeqMethod != sip.ACK || facts.CallID != record.Snapshot.CallId || facts.ToTag != record.Snapshot.LocalTag || facts.FromTag != record.Snapshot.RemoteTag {
		return errNativeRecord
	}
	public.Message, err = projectNativeMessage(message, false)
	if err != nil {
		return err
	}
	public.Message.Selectors = append([]string(nil), record.Selectors...)
	return projectReinviteACK(public, message)
}
