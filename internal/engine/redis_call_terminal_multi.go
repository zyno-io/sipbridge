package engine

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
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
	"sipbridge/internal/storagecodec"
)

// terminalCallOwners is operation-local metadata only. It is never retained
// between the exact revision-fenced reads and their candidate publication.
type terminalCallOwners struct {
	identity    rediscore.Identity
	current     *nativepb.Context
	procedures  map[string]*nativepb.Procedure
	dialogs     map[string]*nativepb.Dialog
	results     map[string]*nativepb.Result
	output      string
	early       string
	unprovedACK string
}

// selectRedisTerminalOwners selects only actual rooted owner records; no SIP
// message is loaded. A later batch observes precisely the first revision and
// assignment, otherwise the entire candidate is discarded.
func selectRedisTerminalOwners(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, current *nativepb.Context, deadline time.Time) (*terminalCallOwners, error) {
	if store == nil || due == nil || current == nil || current.Context == nil || current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAC && current.Kind != pb.ContextSnapshot_UAS || !current.Ended || current.CallRetireAtMs <= 0 || due.CheckedAtMS < current.CallRetireAtMs || !time.Now().Before(deadline) {
		return nil, errNativeRecord
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	out := &terminalCallOwners{identity: due.Identity, current: current, procedures: make(map[string]*nativepb.Procedure), dialogs: make(map[string]*nativepb.Dialog), results: make(map[string]*nativepb.Result)}
	fields := append([]string(nil), current.InitialProcedures...)
	fields = append(fields, current.NoninitialProcedures...)
	fields = append(fields, current.Dialogs...)
	read := func(fields []string) (map[string][]byte, error) {
		records := make(map[string][]byte, len(fields))
		for len(fields) > 0 {
			n := min(128, len(fields))
			view, err := store.Read(ctx, due.Identity, fields[:n]...)
			if err != nil {
				return nil, err
			}
			if view.Revision != due.Revision || view.Timer != due.Assignment {
				return nil, rediscore.ErrConflict
			}
			for field, value := range view.Records {
				records[field] = value
			}
			fields = fields[n:]
		}
		return records, nil
	}
	records, err := read(fields)
	if err != nil {
		return nil, err
	}
	var resultFields []string
	var outputFields []string
	outputs := make(map[string]*nativepb.Output)
	seenOutputs := make(map[string]bool)
	if current.CommandSequence != 0 && !nativeInitialRefusalContext(current) {
		resultFields = append(resultFields, "r:result-"+strconv.FormatUint(current.CommandSequence, 10))
	}
	addOutput := func(field string) {
		if !seenOutputs[field] {
			seenOutputs[field] = true
			outputFields = append(outputFields, field)
		}
		if out.output == "" || field < out.output {
			out.output = field
		}
	}
	for _, field := range append(append([]string(nil), current.InitialProcedures...), current.NoninitialProcedures...) {
		p, e := readPrivateProcedureRecord(records[field])
		if e != nil {
			return nil, e
		}
		if p.Native.Version != portable.Version || p.Native.Key == "" || p.Native.Phase != string(portable.Terminated) || p.DecisionPending || p.Native.Timers != nil && !nativeDeadlinesInactive(p.Native.Timers.Values) {
			return nil, nil
		}
		if slices.Contains(current.InitialProcedures, field) && p.Native.Family != string(portable.InviteServer) && p.Native.Family != string(portable.InviteClient) {
			return nil, errNativeRecord
		}
		if !slices.Contains(current.InitialProcedures, field) && p.Native.Family != string(portable.InviteServer) && p.Native.Family != string(portable.InviteClient) && p.Native.Family != string(portable.NonInviteServer) && p.Native.Family != string(portable.NonInviteClient) {
			return nil, errNativeRecord
		}
		if len(p.Events) != 0 {
			return nil, nil
		}
		if p.CommandResult != "" && !slices.Contains(resultFields, p.CommandResult) {
			resultFields = append(resultFields, p.CommandResult)
		}
		if p.SuccessfulAck != nil && p.SuccessfulAck.AckResult != "" && !slices.Contains(resultFields, p.SuccessfulAck.AckResult) {
			resultFields = append(resultFields, p.SuccessfulAck.AckResult)
		}
		for _, output := range p.Outputs {
			addOutput(output)
		}
		out.procedures[field] = p
	}
	for _, field := range current.InitialProcedures {
		p := out.procedures[field]
		if p.Root != "" {
			root, ok := out.procedures[p.Root]
			if !ok || !slices.Contains(current.InitialProcedures, p.Root) || root.Root != "" || p.Native.Family != string(portable.InviteClient) || root.Native.Family != string(portable.InviteClient) || p.CommandResult == "" || p.CommandResult != root.CommandResult {
				return nil, errNativeRecord
			}
		}
	}
	for _, field := range current.Dialogs {
		d, e := readPrivateDialogRecord(records[field])
		if e != nil {
			return nil, e
		}
		if d.Native.Version != portable.Version || d.Native.CallId != current.CallId || d.Native.Timers != nil && !nativeDeadlinesInactive(d.Native.Timers.Values) {
			return nil, nil
		}
		if d.Native.Phase != string(portable.DialogEnded) && d.Native.Phase != string(portable.DialogEarly) {
			if due.Identity.Owner != "" || due.Identity.Term != "" || current.Kind != pb.ContextSnapshot_UAC || !current.Answered || current.InitialAck || d.Native.Role != string(portable.UAC) || d.Native.LocalTag != current.LocalTag || d.Native.RemoteTag != current.RemoteTag || d.Native.LocalInvite == nil || !d.Native.LocalInvite.Initial || d.Native.LocalInvite.CancelRequested || d.Native.LocalInvite.State != string(portable.InviteAcked) || d.AckWritten || d.AckResult == "" || d.LocalBye != "" || len(d.Outputs) != 1 || !strings.HasPrefix(d.Outputs[0], "r:app-ack-output-") || out.unprovedACK != "" {
				return nil, nil
			}
			out.unprovedACK = field
		}
		if d.Native.Phase == string(portable.DialogEarly) && out.early == "" {
			out.early = field
		}
		for _, output := range d.Outputs {
			addOutput(output)
		}
		out.dialogs[field] = d
	}
	// Superseded results can be retained solely through another actual
	// output after that output's U deadline was removed. Select their bounded
	// metadata in batches before deciding that any shared wire is unowned.
	for len(outputFields) > 0 {
		n := min(128, len(outputFields))
		batch, e := read(outputFields[:n])
		if e != nil {
			return nil, e
		}
		for _, field := range outputFields[:n] {
			output, e := decodeNativeOutput(batch[field])
			if e != nil {
				return nil, e
			}
			outputs[field] = output
			if output.Result != "" && !slices.Contains(resultFields, output.Result) {
				resultFields = append(resultFields, output.Result)
			}
		}
		outputFields = outputFields[n:]
	}
	resultRecords, err := read(resultFields)
	if err != nil {
		return nil, err
	}
	for _, field := range resultFields {
		sequence, e := strconv.ParseUint(strings.TrimPrefix(field, "r:result-"), 10, 53)
		if e != nil || sequence == 0 || sequence > current.CommandSequence || field != "r:result-"+strconv.FormatUint(sequence, 10) {
			return nil, errNativeRecord
		}
		r, e := decodeNativeResult(resultRecords[field])
		if e != nil || r.Sequence != sequence {
			return nil, errNativeRecord
		}
		out.results[field] = r
	}
	for field, output := range outputs {
		if err = validateRedisTerminalOutput(field, output, out); err != nil {
			return nil, err
		}
		if strings.HasPrefix(field, "r:app-invite-ack-output-") && output.Dialog != "" {
			if err = validateRedisInviteACKAssociation(ctx, store, due.Identity, due.Revision, due.Assignment, current, out.dialogs[output.Dialog], out.procedures[output.Procedure], output); err != nil {
				return nil, err
			}
		}
	}
	var retainedWires []string
	for field, output := range outputs {
		if strings.HasPrefix(field, "r:retained-ack-output-") {
			wire := out.procedures[output.Procedure].SuccessfulAck.Ack
			if !slices.Contains(retainedWires, wire) {
				retainedWires = append(retainedWires, wire)
			}
		}
	}
	if len(retainedWires) != 0 {
		wires, e := read(retainedWires)
		if e != nil {
			return nil, e
		}
		for field, output := range outputs {
			if !strings.HasPrefix(field, "r:retained-ack-output-") {
				continue
			}
			ack, e := selectedmessage.DecodeOpaque(wires[out.procedures[output.Procedure].SuccessfulAck.Ack])
			if e != nil {
				return nil, e
			}
			facts, e := ack.Facts()
			if e != nil || !facts.IsRequest || facts.Method != sip.ACK || facts.CSeq != output.AckSequence || ack.Transport() != output.Write.Flow.Transport {
				return nil, errNativeRecord
			}
		}
	}
	if out.unprovedACK != "" {
		if len(outputs) != 1 || out.early != "" {
			return nil, nil
		}
		dialog := out.dialogs[out.unprovedACK]
		output := outputs[dialog.Outputs[0]]
		result := out.results[dialog.AckResult]
		if output == nil || output.Dialog != out.unprovedACK || output.CleanupAfterAck || result == nil || result.Pending || result.Certainty != pb.OperationResult_UNKNOWN || result.Output != dialog.Outputs[0] || result.Request != "" || result.Response != "" {
			return nil, errNativeRecord
		}
		for _, receipt := range out.results {
			if receipt.Pending || receipt.CutoffMs > due.CheckedAtMS {
				return nil, nil
			}
		}
	}
	return out, nil
}

// terminateRedisTerminalEarlyDialog uses the existing SDK terminal transition
// before any actual owner/output is discarded. It does not create an event.
func terminateRedisTerminalEarlyDialog(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, owners *terminalCallOwners, deadline time.Time) error {
	if owners == nil || owners.early == "" {
		return errNativeRecord
	}
	view, err := store.Read(ctx, due.Identity, owners.early)
	if err != nil {
		return err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	dialog, err := decodeNativeDialog(view.Records[owners.early])
	if err != nil || dialog.Native.Phase != portable.DialogEarly {
		return errNativeRecord
	}
	fields := callDialogMessageRefs(dialog.Refs)
	wire, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return err
	}
	if wire.Revision != due.Revision || wire.Timer != due.Assignment {
		return rediscore.ErrConflict
	}
	memo, err := selectedmessage.NewMemo(len(fields) + 1)
	if err != nil {
		return err
	}
	full, err := dialog.complete(&nativeSelectedMessages{records: wire.Records, memo: memo, provenance: "admitted"})
	if err != nil {
		return err
	}
	next, effects, err := portable.StepDialog(full, portable.DialogInput{Kind: portable.DialogTerminate, Now: due.CheckedAtMS})
	if err != nil {
		return err
	}
	if next.Phase != portable.DialogEnded {
		return errNativeRecord
	}
	for _, effect := range effects {
		if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
			return errNativeRecord
		}
	}
	data, err := encodeNativeDialog(next, dialog.Refs, dialog.Metadata)
	if err != nil {
		return err
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: owners.early, Value: data}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(owners.current.CallRetireAtMs)}}}})
	return err
}

// reconcileRedisTerminalOutput performs no SIP transition. Definite physical
// evidence returns to its existing exact native consumer; UNKNOWN only releases
// proof after the independent terminal witness and fixed original cutoffs.
func validateRedisTerminalOutput(field string, output *nativepb.Output, owners *terminalCallOwners) error {
	applicationClient := strings.HasPrefix(field, "r:app-bye-output-") || strings.HasPrefix(field, "r:app-client-output-")
	applicationACK := strings.HasPrefix(field, "r:app-ack-output-")
	applicationInvite := strings.HasPrefix(field, "r:app-invite-output-")
	applicationInviteACK := strings.HasPrefix(field, "r:app-invite-ack-output-")
	supported := applicationClient || applicationACK || applicationInvite || applicationInviteACK || strings.HasPrefix(field, "r:retained-ack-output-") || strings.HasPrefix(field, "r:cancel-client-output-") || strings.HasPrefix(field, "r:invite-output-") || strings.HasPrefix(field, "r:output-") && strings.HasSuffix(field, "-100") || strings.HasPrefix(field, "r:uac-output-") || strings.HasPrefix(field, "r:uac-ack-output-") || strings.HasPrefix(field, "r:uac-dialog-ack-output-") || strings.HasPrefix(field, "r:call-cleanup-output-") || strings.HasPrefix(field, "r:dialog-output-") || strings.HasPrefix(field, "r:call-bye-output-") || strings.HasPrefix(field, "r:call-cancel-output-") || strings.HasPrefix(field, "r:call-cancel-invite-output-")
	if !supported {
		return errNativeRecord
	}
	if strings.HasPrefix(field, "r:invite-output-") && strings.HasSuffix(field, "-0") {
		id := strings.TrimSuffix(strings.TrimPrefix(field, "r:invite-output-"), "-0")
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.String() != id || !validRedisInitialInviteTimeoutOutput(owners.identity, owners.current, owners.procedures[output.Procedure], output, id, field) {
			return errNativeRecord
		}
	}
	if strings.HasPrefix(field, "r:retained-ack-output-") {
		p := owners.procedures[output.Procedure]
		if !validReadyRetainedACK(output, field) || p == nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteClient) || p.Native.Phase != string(portable.Terminated) || p.SuccessfulAck == nil || !validNativeSuccessfulACK(p.SuccessfulAck) || !slices.Contains(p.Outputs, field) || !proto.Equal(p.SuccessfulAck.Flow, output.Write.Flow) {
			return errNativeRecord
		}
	}
	if applicationInvite {
		p, d, r := owners.procedures[output.Procedure], owners.dialogs[output.Dialog], owners.results[output.Result]
		id := strings.TrimPrefix(field, "r:app-invite-output-")
		if !validReadyApplicationInvite(output, field) || p == nil || d == nil || r == nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteClient) || p.Native.Phase != string(portable.Terminated) || p.Origin != "r:app-invite-origin-"+id || p.CommandResult != output.Result || d.LocalInvite == nil || d.LocalInvite.Request != p.Origin || !slices.Contains(p.Outputs, field) || !slices.Contains(d.Outputs, field) || r.Output != field || r.OperationId != output.Operation || r.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
			return errNativeRecord
		}
		expected, e := redisApplicationByeID(r.Fingerprint)
		if e != nil || expected != id {
			return errNativeRecord
		}
	}
	if applicationInviteACK {
		p := owners.procedures[output.Procedure]
		id := strings.TrimPrefix(output.Procedure, "r:app-invite-")
		if !validReadyApplicationInviteACK(output, field) || p == nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteClient) || p.Native.Phase != string(portable.Terminated) || p.Origin != "r:app-invite-origin-"+id || !proto.Equal(p.Flow, output.Write.Flow) || !slices.Contains(p.Outputs, field) {
			return errNativeRecord
		}
		if output.Dialog != "" {
			d := owners.dialogs[output.Dialog]
			if d == nil || d.LocalInvite == nil || d.Native.LocalInvite == nil || d.LocalInvite.Request != p.Origin || !validRedisApplicationInviteDialogACKWire(id, d.LocalInvite.Ack) || d.AckEmissions == 0 || d.Native.LocalInvite.Sequence != output.AckSequence || !slices.Contains(d.Outputs, field) || !(d.Native.Phase == string(portable.DialogEnded) && !output.CleanupAfterAck) && !validUACDialogLocalAssociation(d.LocalBye, d.LocalTransaction, p.Native.Key) && !candidateRedisApplicationByeAssociation(d) {
				return errNativeRecord
			}
		} else if p.Ack != "r:app-invite-tx-ack-"+id || p.AckEmissions == 0 {
			return errNativeRecord
		}
	}
	if strings.HasPrefix(field, "r:cancel-client-output-") {
		id := strings.TrimPrefix(field, "r:cancel-client-output-")
		p := owners.procedures[output.Procedure]
		if len(id) != 32 || output.Procedure != "r:cancel-client-"+id || output.Method != "CANCEL" || output.Status != 0 || output.Dialog != "" || output.Result != "" || output.Operation != "" || output.Write == nil || output.Write.Permit == nil || output.Write.Permit.Authorization == nil || output.Write.Permit.Authorization.AuthorityClass != pb.EffectAuthorization_TERMINAL_CLEANUP || p == nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.NonInviteClient) || p.Native.Phase != string(portable.Terminated) || !p.Cleanup || p.CreditContribution != 0 || p.CommandResult != "" || p.Origin != "r:cancel-client-origin-"+id || !slices.Contains(p.Outputs, field) || !proto.Equal(p.Flow, output.Write.Flow) {
			return errNativeRecord
		}
	}
	if applicationClient {
		if !validReadyApplicationBye(output, field) {
			return errNativeRecord
		}
		p := owners.procedures[output.Procedure]
		d := owners.dialogs[output.Dialog]
		r := owners.results[output.Result]
		id := strings.TrimPrefix(field, "r:"+redisApplicationClientPrefix(output.Method)+"output-")
		if p == nil || d == nil || r == nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.NonInviteClient) || p.Cleanup || p.CommandResult != output.Result || p.Origin != "r:"+redisApplicationClientPrefix(output.Method)+"origin-"+id || !slices.Contains(p.Outputs, field) || !slices.Contains(d.Outputs, field) || output.Method == "BYE" && (d.LocalTransaction != p.Native.Key || d.LocalBye != p.Origin) || r.Output != field || r.OperationId != output.Operation || r.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
			return errNativeRecord
		}
		expectedID, err := redisApplicationByeID(r.Fingerprint)
		if err != nil || expectedID != id {
			return errNativeRecord
		}
	}
	if applicationACK {
		if !validReadyApplicationACK(output, field) {
			return errNativeRecord
		}
		p, d, r := owners.procedures[output.Procedure], owners.dialogs[output.Dialog], owners.results[output.Result]
		id := strings.TrimPrefix(field, "r:app-ack-output-")
		if p == nil || d == nil || r == nil || p.Native.Version != portable.Version || p.Native.Family != string(portable.InviteClient) || d.Native.Version != portable.Version || d.Native.LocalInvite == nil || d.Native.LocalInvite.Sequence != output.AckSequence || d.LocalInvite == nil || d.LocalInvite.Request != p.Origin || d.LocalInvite.Ack != "r:app-ack-wire-"+id || d.AckResult != output.Result || !validUACDialogLocalAssociation(d.LocalBye, d.LocalTransaction, p.Native.Key) || !slices.Contains(p.Outputs, field) || !slices.Contains(d.Outputs, field) || r.Output != field || r.OperationId != output.Operation || r.DecidingLeaseTerm != output.Write.Permit.Authorization.ApplicationLeaseTerm {
			return errNativeRecord
		}
		expectedID, err := redisApplicationByeID(r.Fingerprint)
		if err != nil || expectedID != id {
			return errNativeRecord
		}
	}
	if strings.HasPrefix(field, "r:invite-output-") && (output.Method != "INVITE" || output.Status < 100 || output.Status > 699) || strings.HasPrefix(field, "r:output-") && (output.Method != "INVITE" || output.Status != 100) || strings.HasPrefix(field, "r:uac-output-") && (output.Method != "INVITE" || output.Status != 0) || (strings.HasPrefix(field, "r:uac-ack-output-") || strings.HasPrefix(field, "r:uac-dialog-ack-output-")) && (output.Method != "ACK" || output.Status != 0) || strings.HasPrefix(field, "r:call-cleanup-output-") && (output.Method != "BYE" || output.Status != 0) || strings.HasPrefix(field, "r:call-bye-output-") && (output.Method != "BYE" || output.Status != 200) || strings.HasPrefix(field, "r:call-cancel-output-") && (output.Method != "CANCEL" || output.Status != 200 && output.Status != 481) || strings.HasPrefix(field, "r:call-cancel-invite-output-") && (output.Method != "INVITE" || output.Status != 487) {
		return errNativeRecord
	}
	if strings.HasPrefix(field, "r:dialog-output-") {
		rest := strings.TrimPrefix(field, "r:dialog-output-")
		if len(rest) < 38 || rest[36] != '-' {
			return errNativeRecord
		}
		id, err := uuid.Parse(rest[:36])
		sequence, seqErr := strconv.ParseUint(rest[37:], 10, 53)
		family := redisDialogServerFamily(output.Method)
		if err != nil || id.String() != rest[:36] || seqErr != nil || rest[37:] != strconv.FormatUint(sequence, 10) || family == "" || output.Status < 100 || output.Status > 699 || output.Procedure != "r:procedure-"+id.String() || sequence == 0 && output.Result != "" || output.Result != "" && output.Result != "r:result-"+strconv.FormatUint(sequence, 10) {
			return errNativeRecord
		}
		p := owners.procedures[output.Procedure]
		d := owners.dialogs[output.Dialog]
		if p == nil || d == nil || p.Native.Family != family || p.RequestId != id.String() || p.Origin != "r:origin-"+id.String() || !slices.Contains(p.Outputs, field) || !slices.Contains(d.Outputs, field) {
			return errNativeRecord
		}
	}
	if output.Write == nil || output.Write.Effect == nil || output.Write.Permit == nil || output.Write.Permit.Authorization == nil || !proto.Equal(output.Write.Effect.Context, owners.current.Context) {
		return errNativeRecord
	}
	proc, hasProc := owners.procedures[output.Procedure]
	_, hasDialog := owners.dialogs[output.Dialog]
	owned := false
	for name, p := range owners.procedures {
		if slices.Contains(p.Outputs, field) {
			if name != output.Procedure {
				return errNativeRecord
			}
			owned = true
		}
	}
	for name, d := range owners.dialogs {
		if slices.Contains(d.Outputs, field) {
			if name != output.Dialog {
				return errNativeRecord
			}
			owned = true
		}
	}
	if !owned || !hasProc || output.Dialog != "" && !hasDialog || !proto.Equal(proc.Flow, output.Write.Flow) && output.Method != "ACK" {
		return errNativeRecord
	}
	return nil
}

func reconcileRedisTerminalOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, owners *terminalCallOwners, deadline time.Time) (bool, error) {
	if owners == nil || owners.current == nil || owners.output == "" {
		return false, errNativeRecord
	}
	field := owners.output
	view, err := store.Read(ctx, due.Identity, field)
	if err != nil {
		return false, err
	}
	if view.Revision != due.Revision || view.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	output, err := decodeNativeOutput(view.Records[field])
	if err != nil {
		return false, err
	}
	if err = validateRedisTerminalOutput(field, output, owners); err != nil {
		return false, err
	}
	proc := owners.procedures[output.Procedure]
	dialog, hasDialog := owners.dialogs[output.Dialog]
	ref, err := nativeOutputRef(output.Write)
	if err != nil {
		return false, err
	}
	physical := output.Write.Permit.Authorization.DeadlineMs
	if physical == 0 || physical > uint64((1<<53-1)-rediscore.OutputObsoleteGrace.Milliseconds()) {
		return false, errNativeRecord
	}
	rearm := func() (bool, error) {
		_, e := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: "out-" + strings.TrimPrefix(field, "r:"), Due: time.UnixMilli(int64(physical))}}}})
		return e == nil, e
	}
	proof, inspectErr := store.InspectOutput(ctx, ref)
	if inspectErr == nil {
		if proof.MetadataField != field || proof.DeadlineMS != int64(physical) || !bytes.Equal(proof.Metadata, view.Records[field]) {
			return false, rediscore.ErrConflict
		}
		if proof.State == rediscore.OutputPending || proof.State == rediscore.OutputStarted || proof.State == rediscore.OutputWritten || proof.State == rediscore.OutputNotWritten {
			return rearm()
		}
		if proof.State != rediscore.OutputUnknown {
			return false, errNativeRecord
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: output}
		switch {
		case strings.HasPrefix(field, "r:retained-ack-output-"):
			selected, e := inspectRedisRetainedACK(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:cancel-client-output-"):
			selected, e := inspectRedisClientCancelOutput(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:invite-output-"):
			selected, e := inspectRedisInitialInviteOutput(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:output-"):
			trying, e := nativeReadyInviteTrying(output, field)
			id := strings.TrimPrefix(output.Procedure, "r:procedure-")
			if e != nil || !trying || proof.MessageField != "r:response-"+id+"-100" && proof.MessageField != "r:physical-"+id+"-100" || proc.Native.Family != string(portable.InviteServer) || proc.RequestId != id || proc.Origin != "r:origin-"+id {
				return false, errNativeRecord
			}
		case strings.HasPrefix(field, "r:uac-output-"):
			id := strings.TrimPrefix(field, "r:uac-output-")
			if len(id) != 32 || output.Procedure != "r:uac-procedure-"+id || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:uac-origin-"+id || proof.MessageField != proc.Origin || output.Result != "r:result-1" || proc.CommandResult != output.Result {
				return false, errNativeRecord
			}
		case strings.HasPrefix(field, "r:uac-ack-output-"):
			selected, e := inspectRedisUACFailureACK(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:app-ack-output-"):
			selected, e := inspectRedisApplicationACK(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:app-invite-output-"):
			selected, e := inspectRedisApplicationByeOutput(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:app-invite-ack-output-"):
			selected, e := inspectRedisApplicationInviteACK(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:uac-dialog-ack-output-"):
			selected, e := inspectRedisUACSuccessACK(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:call-cleanup-output-"):
			selected, e := inspectRedisCleanupByeOutput(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:app-bye-output-"), strings.HasPrefix(field, "r:app-client-output-"):
			selected, e := inspectRedisApplicationByeOutput(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		case strings.HasPrefix(field, "r:dialog-output-"):
			rest := strings.TrimPrefix(field, "r:dialog-output-")
			if ref.RequireApplication || proof.MessageField != "r:dialog-response-"+rest && proof.MessageField != "r:dialog-wire-"+rest {
				return false, errNativeRecord
			}
		case strings.HasPrefix(field, "r:call-bye-output-"):
			id := strings.TrimPrefix(field, "r:call-bye-output-")
			if len(id) != 32 || output.Procedure != "r:call-bye-"+id || output.Result != "" || output.Operation != "" || ref.RequireApplication || proc.Native.Family != string(portable.NonInviteServer) || proc.Native.Reliable != (output.Write.Flow.Transport != "udp") || proc.Origin != "r:call-bye-origin-"+id || proc.LastResponse != "r:call-bye-final-"+id || proof.MessageField != "r:call-bye-final-"+id && proof.MessageField != "r:call-bye-wire-"+id || !hasDialog || dialog.Native.Phase != string(portable.DialogEnded) {
				return false, errNativeRecord
			}
		case strings.HasPrefix(field, "r:call-cancel-output-"), strings.HasPrefix(field, "r:call-cancel-invite-output-"):
			selected, e := inspectRedisCancelOutput(ctx, store, output.Write, capture)
			if e != nil {
				return false, e
			}
			if selected.selected.Identity != due.Identity || selected.selected.View.Revision != due.Revision || selected.selected.View.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
		}
	} else if !errors.Is(inspectErr, rediscore.ErrAbsent) {
		return false, inspectErr
	}
	// A losing successful ACK must not lose its still-unproved write/BYE duty.
	if output.Method == "ACK" && output.Dialog != "" {
		applicationDialogACK := strings.HasPrefix(field, "r:app-ack-output-") || strings.HasPrefix(field, "r:app-invite-ack-output-")
		closedInviteACK := hasDialog && strings.HasPrefix(field, "r:app-invite-ack-output-") && dialog.Native.Phase == string(portable.DialogEnded) && (!output.CleanupAfterAck || dialog.LocalBye != "")
		if !hasDialog || dialog.Native.LocalInvite == nil || dialog.Native.LocalInvite.Sequence != output.AckSequence || dialog.LocalInvite == nil || dialog.LocalInvite.Request != proc.Origin || dialog.LocalInvite.Ack == "" || applicationDialogACK && !closedInviteACK && !validUACDialogLocalAssociation(dialog.LocalBye, dialog.LocalTransaction, proc.Native.Key) || !applicationDialogACK && (dialog.Native.Role != string(portable.UAC) || !dialog.Native.LocalInvite.Initial) {
			return false, errNativeRecord
		}
		if !applicationDialogACK {
			if err := validateRedisInviteACKAssociation(ctx, store, due.Identity, due.Revision, due.Assignment, owners.current, dialog, proc, output); err != nil {
				return false, err
			}
		}
		// A winning dialog independently ended after its result and all native
		// duties closed has no remaining ACK-dependent action. This does not
		// infer a write or grant the losing-fork cleanup exception.
		closedWinner := owners.current.Answered && owners.current.CallId == dialog.Native.CallId && owners.current.LocalTag == dialog.Native.LocalTag && owners.current.RemoteTag == dialog.Native.RemoteTag && dialog.Native.Phase == string(portable.DialogEnded) && !output.CleanupAfterAck
		unproved := owners.unprovedACK == output.Dialog && strings.HasPrefix(field, "r:app-ack-output-") && !dialog.AckWritten && !output.CleanupAfterAck
		if !dialog.AckWritten && !closedWinner && !closedInviteACK && !unproved || output.CleanupAfterAck && dialog.LocalBye == "" {
			return false, errNativeRecord
		}
	}
	var lastCutoff int64
	for _, result := range owners.results {
		if result.Pending {
			return false, errNativeRecord
		}
		lastCutoff = max(lastCutoff, result.CutoffMs)
	}
	if output.Result != "" {
		result, ok := owners.results[output.Result]
		if !ok {
			rv, e := store.Read(ctx, due.Identity, output.Result)
			if e != nil {
				return false, e
			}
			if rv.Revision != due.Revision || rv.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
			result, e = decodeNativeResult(rv.Records[output.Result])
			if e != nil {
				return false, e
			}
		}
		if result.Sequence == 0 || result.Sequence > owners.current.CommandSequence || output.Result != "r:result-"+strconv.FormatUint(result.Sequence, 10) || result.OperationId != output.Operation {
			return false, errNativeRecord
		}
		if result.Pending || strings.HasPrefix(field, "r:dialog-output-") && result.Output != field {
			return false, errNativeRecord
		}
		lastCutoff = max(lastCutoff, result.CutoffMs)
	}
	if lastCutoff > due.CheckedAtMS {
		_, e := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(lastCutoff)}}}})
		return e == nil, e
	}
	cutoff := max(owners.current.CallRetireAtMs, int64(physical)+rediscore.OutputObsoleteGrace.Milliseconds())
	if due.DeadlineMS != cutoff {
		_, e := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(cutoff)}}}})
		return e == nil, e
	}
	if inspectErr == nil {
		if err = store.ForgetObsoleteCallOutput(ctx, due, ref, owners.current.CallRetireAtMs, true); err != nil {
			return false, err
		}
		return true, nil
	}
	if owners.unprovedACK != "" && field == owners.dialogs[owners.unprovedACK].Outputs[0] {
		return finishRedisAbandonedApplicationACK(ctx, store, due, owners, output, field, deadline)
	}
	switch {
	case strings.HasPrefix(field, "r:app-invite-output-"), strings.HasPrefix(field, "r:app-invite-ack-output-"):
		owner := proto.Clone(proc).(*nativepb.Procedure)
		index, found := slices.BinarySearch(owner.Outputs, field)
		if !found {
			return false, errNativeRecord
		}
		owner.Outputs = slices.Delete(owner.Outputs, index, index+1)
		value, e := marshalNativeRecord(storagecodec.NativeProcedure, owner)
		if e != nil {
			return false, e
		}
		changes := []rediscore.Change{{Field: output.Procedure, Value: value}, {Field: field, Delete: true}}
		if output.Dialog != "" {
			if !hasDialog {
				return false, errNativeRecord
			}
			d := proto.Clone(dialog).(*nativepb.Dialog)
			i, ok := slices.BinarySearch(d.Outputs, field)
			if !ok {
				return false, errNativeRecord
			}
			d.Outputs = slices.Delete(d.Outputs, i, i+1)
			data, e := marshalNativeRecord(storagecodec.NativeDialog, d)
			if e != nil {
				return false, e
			}
			changes = append(changes, rediscore.Change{Field: output.Dialog, Value: data})
		}
		if output.Result != "" {
			result := owners.results[output.Result]
			if result == nil || result.Pending || result.Output != field || result.CutoffMs > due.CheckedAtMS {
				return false, errNativeRecord
			}
			receipt := proto.Clone(result).(*nativepb.Result)
			receipt.Output = ""
			data, e := encodeNativeResult(receipt)
			if e != nil {
				return false, e
			}
			changes = append(changes, rediscore.Change{Field: output.Result, Value: data})
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(owners.current.CallRetireAtMs)}}}})
		return e == nil, e
	case strings.HasPrefix(field, "r:cancel-client-output-"):
		id := strings.TrimPrefix(field, "r:cancel-client-output-")
		index, ok := slices.BinarySearch(proc.Outputs, field)
		if !ok || proc.Native.Phase != string(portable.Terminated) {
			return false, errNativeRecord
		}
		owner := proto.Clone(proc).(*nativepb.Procedure)
		owner.Outputs = slices.Delete(owner.Outputs, index, index+1)
		data, e := marshalNativeRecord(storagecodec.NativeProcedure, owner)
		if e != nil {
			return false, e
		}
		timers := []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(owners.current.CallRetireAtMs)}, {Member: "cancel-client-retire-" + id, Due: time.UnixMilli(due.CheckedAtMS)}}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: data}, {Field: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return e == nil, e
	case strings.HasPrefix(field, "r:call-bye-output-"), strings.HasPrefix(field, "r:call-cancel-output-"), strings.HasPrefix(field, "r:call-cancel-invite-output-"):
		return retireRedisTerminalServerOutput(ctx, store, due, owners, output, field, deadline)
	case strings.HasPrefix(field, "r:app-bye-output-"), strings.HasPrefix(field, "r:app-client-output-"):
		result := owners.results[output.Result]
		if !hasDialog || result == nil || result.Pending || result.Output != field || result.CutoffMs > due.CheckedAtMS || proc.Native.Phase != string(portable.Terminated) || proc.CreditContribution == 0 || proc.CreditContribution > 1<<53-1 {
			return false, errNativeRecord
		}
		current := proto.Clone(owners.current).(*nativepb.Context)
		owner := proto.Clone(dialog).(*nativepb.Dialog)
		receipt := proto.Clone(result).(*nativepb.Result)
		receipt.Output = ""
		changes, selector, e := planRedisCleanupByeRetirement(ctx, store, due, current, owner, output.Procedure, proc, receipt)
		if e != nil {
			return false, e
		}
		for _, wire := range []string{proc.Origin, proc.LastResponse} {
			if wire == "" {
				continue
			}
			held := false
			for name, p := range owners.procedures {
				if name != output.Procedure && nativeProcedureReferencesWire(p, wire) {
					held = true
				}
			}
			for _, d := range owners.dialogs {
				if nativeDialogReferencesWire(d, wire) {
					held = true
				}
			}
			for _, r := range owners.results {
				if r.Request == wire || r.Response == wire {
					held = true
				}
			}
			if held {
				changes = slices.DeleteFunc(changes, func(change rediscore.Change) bool { return change.Delete && change.Field == wire })
			}
		}
		index, found := slices.BinarySearch(owner.Outputs, field)
		if !found {
			return false, errNativeRecord
		}
		owner.Outputs = slices.Delete(owner.Outputs, index, index+1)
		ownerBytes, e := marshalNativeRecord(storagecodec.NativeDialog, owner)
		if e != nil {
			return false, e
		}
		receiptBytes, e := encodeNativeResult(receipt)
		if e != nil {
			return false, e
		}
		changes = append(changes, rediscore.Change{Field: output.Dialog, Value: ownerBytes}, rediscore.Change{Field: output.Result, Value: receiptBytes}, rediscore.Change{Field: field, Delete: true})
		timers := []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(current.CallRetireAtMs)}}
		if current.CommandSequence > receipt.Sequence {
			timers = append(timers, rediscore.TimerChange{Member: "orig-result-" + strconv.FormatUint(receipt.Sequence, 10), Due: time.UnixMilli(receipt.CutoffMs)})
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, CreditDelta: -int64(proc.CreditContribution), SpendReservation: true, Changes: changes, Selectors: []rediscore.SelectorChange{selector}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: timers}})
		return e == nil, e
	case strings.HasPrefix(field, "r:call-cleanup-output-"):
		id := strings.TrimPrefix(field, "r:call-cleanup-output-")
		if len(id) != 32 || output.Procedure != "r:call-cleanup-"+id || proc.Native.Family != string(portable.NonInviteClient) || !proc.Cleanup || proc.Origin != "r:call-cleanup-origin-"+id || output.Result != "" || output.Operation != "" || !hasDialog || dialog.LocalTransaction != proc.Native.Key || dialog.LocalBye != proc.Origin || !slices.Contains(dialog.Outputs, field) || len(proc.Outputs) > 1 || len(proc.Outputs) == 1 && proc.Outputs[0] != field {
			return false, errNativeRecord
		}
		current := proto.Clone(owners.current).(*nativepb.Context)
		owner := proto.Clone(dialog).(*nativepb.Dialog)
		changes, selector, e := planRedisCleanupByeRetirement(ctx, store, due, current, owner, output.Procedure, proc)
		if e != nil {
			return false, e
		}
		held := false
		for name, p := range owners.procedures {
			if name != output.Procedure && nativeProcedureReferencesWire(p, proc.LastResponse) {
				held = true
			}
		}
		for _, d := range owners.dialogs {
			if nativeDialogReferencesWire(d, proc.LastResponse) {
				held = true
			}
		}
		for _, r := range owners.results {
			if proc.LastResponse != "" && (r.Request == proc.LastResponse || r.Response == proc.LastResponse) {
				held = true
			}
		}
		if held && proc.LastResponse != "" {
			changes = slices.DeleteFunc(changes, func(change rediscore.Change) bool { return change.Delete && change.Field == proc.LastResponse })
		}
		index, found := slices.BinarySearch(owner.Outputs, field)
		if !found {
			return false, errNativeRecord
		}
		owner.Outputs = slices.Delete(owner.Outputs, index, index+1)
		ownerBytes, e := marshalNativeRecord(storagecodec.NativeDialog, owner)
		if e != nil {
			return false, e
		}
		changes = append(changes, rediscore.Change{Field: output.Dialog, Value: ownerBytes}, rediscore.Change{Field: field, Delete: true})
		_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: changes, Selectors: []rediscore.SelectorChange{selector}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(current.CallRetireAtMs)}}}})
		return e == nil, e
	case strings.HasPrefix(field, "r:uac-output-"):
		id := strings.TrimPrefix(field, "r:uac-output-")
		result := owners.results[output.Result]
		if len(id) != 32 || output.Procedure != "r:uac-procedure-"+id || proc.Native.Family != string(portable.InviteClient) || proc.Origin != "r:uac-origin-"+id || proc.CommandResult != "r:result-1" || output.Result != proc.CommandResult || result == nil || result.Sequence != 1 || result.OperationId != output.Operation || result.Pending || result.CutoffMs > due.CheckedAtMS {
			return false, errNativeRecord
		}
		if proc.Root == "" {
			if result.Output != field {
				return false, errNativeRecord
			}
		} else {
			root := owners.procedures[proc.Root]
			if root == nil || proc.Root == output.Procedure || proc.CreditContribution != 0 || !slices.Contains(owners.current.InitialProcedures, proc.Root) || root.Root != "" || root.Native.Version != portable.Version || root.Native.Family != string(portable.InviteClient) || root.CreditContribution == 0 || root.CommandResult != proc.CommandResult || result.Output == field || result.Request != "" && result.Request != root.Origin {
				return false, errNativeRecord
			}
		}
		index, found := slices.BinarySearch(proc.Outputs, field)
		if !found {
			return false, errNativeRecord
		}
		proc.Outputs = slices.Delete(proc.Outputs, index, index+1)
		procBytes, e := marshalNativeRecord(storagecodec.NativeProcedure, proc)
		if e != nil {
			return false, e
		}
		if proc.Root == "" {
			result.Output = ""
		}
		resultBytes, e := encodeNativeResult(result)
		if e != nil {
			return false, e
		}
		_, e = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Changes: []rediscore.Change{{Field: output.Procedure, Value: procBytes}, {Field: output.Result, Value: resultBytes}, {Field: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(owners.current.CallRetireAtMs)}}}})
		return e == nil, e
	case strings.HasPrefix(field, "r:retained-ack-output-"), strings.HasPrefix(field, "r:invite-output-"), strings.HasPrefix(field, "r:output-") && strings.HasSuffix(field, "-100"), strings.HasPrefix(field, "r:uac-ack-output-"), strings.HasPrefix(field, "r:uac-dialog-ack-output-"), strings.HasPrefix(field, "r:app-ack-output-"), strings.HasPrefix(field, "r:dialog-output-"):
		return rearm()
	default:
		return false, errNativeRecord
	}
}

// retireRedisTerminalServerOutput releases one already-forgotten server
// response without manufacturing a physical result or consumed marker.
func retireRedisTerminalServerOutput(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, owners *terminalCallOwners, output *nativepb.Output, field string, deadline time.Time) (bool, error) {
	proc := owners.procedures[output.Procedure]
	if proc == nil || proc.Native.Key == "" || output.Result != "" || output.Operation != "" {
		return false, errNativeRecord
	}
	current := proto.Clone(owners.current).(*nativepb.Context)
	var changes []rediscore.Change
	var selectors []rediscore.SelectorChange
	var released []string
	var credit int64
	physicalField := ""
	removeProcedure := false
	switch {
	case strings.HasPrefix(field, "r:call-bye-output-"):
		id := strings.TrimPrefix(field, "r:call-bye-output-")
		d := owners.dialogs[output.Dialog]
		if len(id) != 32 || output.Procedure != "r:call-bye-"+id || proc.Native.Family != string(portable.NonInviteServer) || proc.Origin != "r:call-bye-origin-"+id || proc.LastResponse != "r:call-bye-final-"+id || d == nil || !slices.Contains(d.Outputs, field) || len(proc.Outputs) != 0 || proc.CommandResult != "" {
			return false, errNativeRecord
		}
		owner := proto.Clone(d).(*nativepb.Dialog)
		index, _ := slices.BinarySearch(owner.Outputs, field)
		owner.Outputs = slices.Delete(owner.Outputs, index, index+1)
		value, e := marshalNativeRecord(storagecodec.NativeDialog, owner)
		if e != nil {
			return false, e
		}
		changes = append(changes, rediscore.Change{Field: output.Dialog, Value: value})
		physicalField = "r:call-bye-wire-" + id
		removeProcedure = true
	case strings.HasPrefix(field, "r:call-cancel-output-"):
		id := strings.TrimPrefix(field, "r:call-cancel-output-")
		if len(id) != 32 || output.Procedure != "r:call-cancel-"+id || proc.Native.Family != string(portable.NonInviteServer) || proc.RequestId != id || proc.Origin != "r:call-cancel-origin-"+id || proc.LastResponse != "r:call-cancel-final-"+id || len(proc.Outputs) != 1 || proc.Outputs[0] != field || proc.CommandResult != "" || output.Dialog != "" {
			return false, errNativeRecord
		}
		physicalField = "r:call-cancel-wire-" + id
		removeProcedure = true
	case strings.HasPrefix(field, "r:call-cancel-invite-output-"):
		id := strings.TrimPrefix(field, "r:call-cancel-invite-output-")
		if len(id) != 32 || !slices.Contains(current.InitialProcedures, output.Procedure) || proc.Native.Family != string(portable.InviteServer) || proc.LastResponse != "r:call-cancel-invite-final-"+id || !slices.Contains(proc.Outputs, field) || output.Dialog != "" {
			return false, errNativeRecord
		}
		updated := proto.Clone(proc).(*nativepb.Procedure)
		index, _ := slices.BinarySearch(updated.Outputs, field)
		updated.Outputs = slices.Delete(updated.Outputs, index, index+1)
		value, e := marshalNativeRecord(storagecodec.NativeProcedure, updated)
		if e != nil {
			return false, e
		}
		changes = append(changes, rediscore.Change{Field: output.Procedure, Value: value})
		physicalField = "r:call-cancel-invite-wire-" + id
	default:
		return false, errNativeRecord
	}
	if removeProcedure {
		index, found := slices.BinarySearch(current.NoninitialProcedures, output.Procedure)
		if !found {
			return false, errNativeRecord
		}
		current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, index, index+1)
		data, e := encodeNativeContext(current)
		if e != nil {
			return false, e
		}
		changes = append(changes, rediscore.Change{Field: "r:context", Value: data}, rediscore.Change{Field: output.Procedure, Delete: true})
		selectors = append(selectors, rediscore.SelectorChange{Selector: nativeSelector(rediscore.SelectorServerTransaction, proc.Native.Key), Record: output.Procedure, Delete: true})
		released = append(released, proc.Origin, proc.LastResponse)
		credit = -int64(proc.CreditContribution)
	}
	released = append(released, physicalField)
	for _, wire := range released {
		if wire == "" {
			continue
		}
		duplicate := false
		for _, change := range changes {
			if change.Field == wire {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		held := false
		for name, p := range owners.procedures {
			if (!removeProcedure || name != output.Procedure) && nativeProcedureReferencesWire(p, wire) {
				held = true
				break
			}
		}
		if !held {
			for _, d := range owners.dialogs {
				if nativeDialogReferencesWire(d, wire) {
					held = true
					break
				}
			}
		}
		if !held {
			for _, r := range owners.results {
				if r.Request == wire || r.Response == wire {
					held = true
					break
				}
			}
		}
		if !held {
			changes = append(changes, rediscore.Change{Field: wire, Delete: true})
		}
	}
	changes = append(changes, rediscore.Change{Field: field, Delete: true})
	_, err := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, CreditDelta: credit, Changes: changes, Selectors: selectors, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(current.CallRetireAtMs)}}}})
	return err == nil, err
}

// retireRedisTerminalProcedure removes one independently terminated noninitial
// owner only after every actual physical output has closed.
func retireRedisTerminalProcedure(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, owners *terminalCallOwners, deadline time.Time) (bool, error) {
	if owners == nil || owners.current == nil || owners.output != "" || len(owners.current.NoninitialProcedures) == 0 {
		return false, errNativeRecord
	}
	field := owners.current.NoninitialProcedures[0]
	proc := owners.procedures[field]
	if proc == nil || len(proc.Outputs) != 0 || len(proc.Events) != 0 || proc.DecisionPending || proc.Root != "" {
		return false, errNativeRecord
	}
	var kind rediscore.SelectorKind
	switch proc.Native.Family {
	case string(portable.InviteServer), string(portable.NonInviteServer):
		kind = rediscore.SelectorServerTransaction
	case string(portable.InviteClient), string(portable.NonInviteClient):
		kind = rediscore.SelectorClientTransaction
	default:
		return false, errNativeRecord
	}
	// Exact current and anchor receipts remain until their own last owners close.
	var marker string
	if proc.CommandResult != "" {
		r := owners.results[proc.CommandResult]
		if r == nil {
			view, e := store.Read(ctx, due.Identity, proc.CommandResult)
			if e != nil {
				return false, e
			}
			if view.Revision != due.Revision || view.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
			r, e = decodeNativeResult(view.Records[proc.CommandResult])
			if e != nil {
				return false, e
			}
			owners.results[proc.CommandResult] = r
		}
		if r.Pending || r.Output != "" {
			return false, errNativeRecord
		}
		if r.CutoffMs > due.CheckedAtMS {
			_, e := store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(r.CutoffMs)}}}})
			return e == nil, e
		}
		currentResult := "r:result-" + strconv.FormatUint(owners.current.CommandSequence, 10)
		if proc.CommandResult != currentResult {
			last := true
			for name, p := range owners.procedures {
				if name != field && p.CommandResult == proc.CommandResult {
					last = false
					break
				}
			}
			if last {
				marker = proc.CommandResult
			}
		}
	}
	current := proto.Clone(owners.current).(*nativepb.Context)
	current.NoninitialProcedures = slices.Delete(current.NoninitialProcedures, 0, 1)
	data, err := encodeNativeContext(current)
	if err != nil {
		return false, err
	}
	changes := []rediscore.Change{{Field: "r:context", Value: data}, {Field: field, Delete: true}}
	released := []string{proc.Origin, proc.LastResponse, proc.Ack}
	if proc.SuccessfulAck != nil {
		released = append(released, proc.SuccessfulAck.FinalResponse, proc.SuccessfulAck.Ack)
	}
	if marker != "" {
		r := owners.results[marker]
		released = append(released, r.Request, r.Response)
		changes = append(changes, rediscore.Change{Field: marker, Delete: true})
	}
	for _, wire := range released {
		if wire == "" {
			continue
		}
		duplicate := false
		for _, change := range changes {
			if change.Field == wire {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		held := false
		for name, p := range owners.procedures {
			if name != field && nativeProcedureReferencesWire(p, wire) {
				held = true
				break
			}
		}
		if !held {
			for _, d := range owners.dialogs {
				if nativeDialogReferencesWire(d, wire) {
					held = true
					break
				}
			}
		}
		if !held {
			for name, r := range owners.results {
				if name != marker && (r.Request == wire || r.Response == wire) {
					held = true
					break
				}
			}
		}
		if !held {
			changes = append(changes, rediscore.Change{Field: wire, Delete: true})
		}
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, CreditDelta: -int64(proc.CreditContribution), Changes: changes, Selectors: []rediscore.SelectorChange{{Selector: nativeSelector(kind, proc.Native.Key), Record: field, Delete: true}}, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(current.CallRetireAtMs)}}}})
	return err == nil, err
}
