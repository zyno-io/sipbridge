package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

// retireRedisQuiescentCall is a qualification-only candidate. Do not route
// production retirement to it until every call procedure producer and closer
// maintains the bounded actual associations. False means a selected duty
// still owns work; deadline absence alone never proves procedures are gone.
func retireRedisQuiescentCall(ctx context.Context, store *rediscore.Store, due *rediscore.DueContext, deadline time.Time) (bool, error) {
	if store == nil || due == nil || due.Member != "call-retire" || due.CheckedAtMS < due.DeadlineMS || !time.Now().Before(deadline) {
		return false, errNativeRecord
	}
	head, err := store.Read(ctx, due.Identity, "r:context", "r:delivery-tag")
	if err != nil {
		return false, err
	}
	if head.Revision != due.Revision || head.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	current, err := decodeNativeContext(head.Records["r:context"])
	if err != nil {
		return false, err
	}
	setup := !current.Ended && current.Kind == pb.ContextSnapshot_UAS && !current.Answered && len(current.Dialogs) == 0 && len(current.InitialProcedures) <= 1
	if current.Context.State == nil || current.Context.Id != due.Identity.Context || current.Context.State.Incarnation != due.Identity.Incarnation || current.Kind != pb.ContextSnapshot_UAS && current.Kind != pb.ContextSnapshot_UAC || !current.Ended && !setup || current.CallRetireAtMs <= 0 || due.CheckedAtMS < current.CallRetireAtMs {
		return false, errNativeRecord
	}
	if len(current.NoninitialProcedures) != 0 {
		return false, nil
	}
	other, err := store.PeekContextDeadline(ctx, due.Identity, due.Revision, due.Member)
	if err != nil {
		return false, err
	}
	if other != nil {
		return false, nil
	}
	fields := append([]string(nil), current.InitialProcedures...)
	fields = append(fields, current.Dialogs...)
	resultField := ""
	if current.CommandSequence != 0 && !nativeInitialRefusalContext(current) {
		resultField = "r:result-" + strconv.FormatUint(current.CommandSequence, 10)
		fields = append(fields, resultField)
	}
	selected, err := store.Read(ctx, due.Identity, fields...)
	if err != nil {
		return false, err
	}
	if selected.Revision != due.Revision || selected.Timer != due.Assignment {
		return false, rediscore.ErrConflict
	}
	// Current and original anchor receipts are selected separately below.
	// Several digest anchors can share the same original scalar marker.
	results := make(map[string]*nativepb.Result)
	resultFields := []string{}
	if resultField != "" {
		resultFields = append(resultFields, resultField)
	}
	procedures := make([]nativeProcedureRecord, len(current.InitialProcedures))
	dialogs := make([]nativeDialogRecord, len(current.Dialogs))
	counts := make(map[string]int)
	addRefs := func(refs ...string) {
		seen := make(map[string]bool)
		for _, ref := range refs {
			if ref != "" && !seen[ref] {
				seen[ref] = true
				counts[ref]++
			}
		}
	}
	for i, field := range current.InitialProcedures {
		p, e := decodeNativeProcedure(selected.Records[field])
		if e != nil {
			return false, e
		}
		if p.Native.Version != portable.Version || p.Native.Family != portable.InviteServer && p.Native.Family != portable.InviteClient || p.Native.Key == "" {
			return false, errNativeRecord
		}
		if p.Native.Phase != portable.Terminated || len(p.Metadata.Outputs) != 0 || len(p.Metadata.Events) != 0 {
			return false, nil
		}
		// Only the first-CANCEL producer creates an unended setup cutoff.
		// Its native cancelled bit and exact final namespace retain that
		// provenance without loading or parsing the retired SIP messages.
		if setup && (p.Native.Family != portable.InviteServer || !p.Native.Cancelled || !strings.HasPrefix(p.Refs.LastResponse, "r:call-cancel-invite-final-") || len(p.Refs.LastResponse) != len("r:call-cancel-invite-final-")+32) {
			return false, errNativeRecord
		}
		for _, timer := range p.Native.Timers {
			if timer.Active {
				return false, nil
			}
		}
		procedures[i] = p
		refs := []string{p.Refs.Origin, p.Refs.LastResponse, p.Refs.ACK}
		if p.Metadata.SuccessfulAck != nil {
			refs = append(refs, p.Metadata.SuccessfulAck.FinalResponse, p.Metadata.SuccessfulAck.Ack)
		}
		addRefs(refs...)
		if marker := p.Metadata.CommandResult; marker != "" && !slices.Contains(resultFields, marker) {
			resultFields = append(resultFields, marker)
		}
		if p.Metadata.SuccessfulAck != nil {
			if marker := p.Metadata.SuccessfulAck.AckResult; marker != "" && !slices.Contains(resultFields, marker) {
				resultFields = append(resultFields, marker)
			}
		}
	}
	// A digest child points directly at its retained original anchor.
	// Remove a child before that root so neither identity nor marker
	// references can dangle between bounded retirement publications.
	referenced := make([]bool, len(procedures))
	for _, child := range procedures {
		if child.Metadata.Root == "" {
			continue
		}
		rootIndex := slices.Index(current.InitialProcedures, child.Metadata.Root)
		if rootIndex < 0 || procedures[rootIndex].Metadata.Root != "" || child.Native.Version != portable.Version || procedures[rootIndex].Native.Version != portable.Version || child.Native.Family != portable.InviteClient || procedures[rootIndex].Native.Family != portable.InviteClient || child.Metadata.CommandResult == "" || child.Metadata.CommandResult != procedures[rootIndex].Metadata.CommandResult {
			return false, errNativeRecord
		}
		referenced[rootIndex] = true
	}
	index := slices.Index(referenced, false)
	if len(procedures) > 0 && index < 0 {
		return false, errNativeRecord
	}
	var extraResults []string
	for _, field := range resultFields {
		if field != resultField {
			extraResults = append(extraResults, field)
		}
	}
	if len(extraResults) != 0 {
		extra, e := store.Read(ctx, due.Identity, extraResults...)
		if e != nil {
			return false, e
		}
		if extra.Revision != due.Revision || extra.Timer != due.Assignment {
			return false, rediscore.ErrConflict
		}
		for field, value := range extra.Records {
			selected.Records[field] = value
		}
	}
	var lastCutoff int64
	for _, field := range resultFields {
		sequence, e := strconv.ParseUint(strings.TrimPrefix(field, "r:result-"), 10, 53)
		if e != nil || sequence == 0 || field != "r:result-"+strconv.FormatUint(sequence, 10) || sequence > current.CommandSequence {
			return false, errNativeRecord
		}
		result, e := decodeNativeResult(selected.Records[field])
		if e != nil || result.Sequence != sequence {
			return false, errNativeRecord
		}
		if result.Pending || result.Output != "" {
			return false, nil
		}
		lastCutoff = max(lastCutoff, result.CutoffMs)
		results[field] = result
		addRefs(result.Request, result.Response)
	}
	if lastCutoff > due.CheckedAtMS {
		_, err = store.Commit(ctx, rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(lastCutoff)}}}})
		return false, err
	}
	for i, field := range current.Dialogs {
		d, e := decodeNativeDialog(selected.Records[field])
		if e != nil {
			return false, e
		}
		if len(d.Metadata.Outputs) != 0 {
			return false, nil
		}
		for _, timer := range d.Native.Timers {
			if timer.Active {
				return false, nil
			}
		}
		dialogs[i] = d
		refs := callDialogMessageRefs(d.Refs)
		addRefs(refs...)
	}
	publication := rediscore.Publication{Identity: due.Identity, Revision: due.Revision, Deadline: deadline, SpendReservation: true, Timers: &rediscore.TimerMutation{Expected: due.Assignment, Target: due.Assignment, RequireLease: true, Changes: []rediscore.TimerChange{{Member: due.Member, Due: time.UnixMilli(due.DeadlineMS)}}}}
	if len(dialogs) > 0 {
		d := dialogs[0]
		field := current.Dialogs[0]
		if d.Native.Phase == portable.DialogEarly {
			refs := callDialogMessageRefs(d.Refs)
			wireView, e := store.Read(ctx, due.Identity, refs...)
			if e != nil {
				return false, e
			}
			if wireView.Revision != due.Revision || wireView.Timer != due.Assignment {
				return false, rediscore.ErrConflict
			}
			memo, e := selectedmessage.NewMemo(len(refs) + 1)
			if e != nil {
				return false, e
			}
			full, e := d.complete(&nativeSelectedMessages{records: wireView.Records, memo: memo, provenance: "admitted"})
			if e != nil {
				return false, e
			}
			next, effects, e := portable.StepDialog(full, portable.DialogInput{Kind: portable.DialogTerminate, Now: due.CheckedAtMS})
			if e != nil {
				return false, e
			}
			if next.Phase != portable.DialogEnded {
				return false, errNativeRecord
			}
			for _, effect := range effects {
				if effect.Kind != portable.Ended && effect.Kind != portable.CancelTimer {
					return false, errNativeRecord
				}
			}
		} else if d.Native.Phase != portable.DialogEnded {
			return false, nil
		}
		a, b := d.Native.LocalTag, d.Native.RemoteTag
		if a == "" || b == "" || d.Native.CallID == "" {
			return false, errNativeRecord
		}
		if a > b {
			a, b = b, a
		}
		selector := nativeSelector(rediscore.SelectorDialog, d.Native.CallID, a, b)
		next := proto.Clone(current).(*nativepb.Context)
		next.Dialogs = slices.Delete(next.Dialogs, 0, 1)
		data, e := encodeNativeContext(next)
		if e != nil {
			return false, e
		}
		publication.Changes = []rediscore.Change{{Field: "r:context", Value: data}, {Field: field, Delete: true}}
		for _, ref := range callDialogMessageRefs(d.Refs) {
			if counts[ref] == 1 {
				publication.Changes = append(publication.Changes, rediscore.Change{Field: ref, Delete: true})
			}
		}
		publication.Selectors = []rediscore.SelectorChange{{Selector: selector, Record: field, Delete: true}}
		_, err = store.Commit(ctx, publication)
		return false, err
	}
	if len(procedures) > 0 {

		p := procedures[index]
		field := current.InitialProcedures[index]
		var kind rediscore.SelectorKind
		switch p.Native.Family {
		case portable.InviteServer:
			kind = rediscore.SelectorServerTransaction
		case portable.InviteClient:
			kind = rediscore.SelectorClientTransaction
		default:
			return false, errNativeRecord
		}
		if p.Native.Key == "" {
			return false, errNativeRecord
		}
		next := proto.Clone(current).(*nativepb.Context)
		next.InitialProcedures = slices.Delete(next.InitialProcedures, index, index+1)
		data, e := encodeNativeContext(next)
		if e != nil {
			return false, e
		}
		publication.Changes = []rediscore.Change{{Field: "r:context", Value: data}, {Field: field, Delete: true}}
		removedCounts := make(map[string]int)
		countRemoved := func(refs ...string) {
			seen := make(map[string]bool)
			for _, ref := range refs {
				if ref != "" && !seen[ref] {
					seen[ref] = true
					removedCounts[ref]++
				}
			}
		}
		refs := []string{p.Refs.Origin, p.Refs.LastResponse, p.Refs.ACK}
		if p.Metadata.SuccessfulAck != nil {
			refs = append(refs, p.Metadata.SuccessfulAck.FinalResponse, p.Metadata.SuccessfulAck.Ack)
		}
		countRemoved(refs...)
		marker := p.Metadata.CommandResult
		if marker != "" && marker != resultField {
			last := true
			for i, other := range procedures {
				if i == index {
					continue
				}
				if other.Metadata.CommandResult == marker {
					last = false
					break
				}
			}
			if last {
				r := results[marker]
				if r == nil {
					return false, errNativeRecord
				}
				publication.Changes = append(publication.Changes, rediscore.Change{Field: marker, Delete: true})
				countRemoved(r.Request, r.Response)
			}
		}
		for ref, removed := range removedCounts {
			if counts[ref] == removed {
				publication.Changes = append(publication.Changes, rediscore.Change{Field: ref, Delete: true})
			}
		}
		publication.Selectors = []rediscore.SelectorChange{{Selector: nativeSelector(kind, p.Native.Key), Record: field, Delete: true}}
		publication.CreditDelta = -int64(p.Metadata.CreditContribution)
		_, err = store.Commit(ctx, publication)
		return false, err
	}
	publication.SpendReservation = false
	publication.Terminal = true
	publication.Timers.Changes = []rediscore.TimerChange{{Member: due.Member, Delete: true}}
	publication.Changes = []rediscore.Change{{Field: "r:context", Delete: true}}
	if len(head.Records["r:delivery-tag"]) != 0 {
		publication.Changes = append(publication.Changes, rediscore.Change{Field: "r:delivery-tag", Delete: true})
	}
	if result := results[resultField]; result != nil {
		publication.Changes = append(publication.Changes, rediscore.Change{Field: resultField, Delete: true})
		seen := make(map[string]bool)
		for _, ref := range []string{result.Request, result.Response} {
			if ref != "" && !seen[ref] {
				publication.Changes = append(publication.Changes, rediscore.Change{Field: ref, Delete: true})
				seen[ref] = true
			}
		}
	}
	_, err = store.Commit(ctx, publication)
	if errors.Is(err, rediscore.ErrConflict) {
		return false, err
	}
	return err == nil, err
}

func callDialogMessageRefs(refs nativeDialogRefs) []string {
	out := make([]string, 0, 7)
	add := func(field string) {
		if field != "" && !slices.Contains(out, field) {
			out = append(out, field)
		}
	}
	for _, invite := range []*nativeInviteRefs{refs.Local, refs.Remote} {
		if invite != nil {
			add(invite.Request)
			add(invite.Final)
			add(invite.ACK)
		}
	}
	add(refs.LocalBye)
	return out
}
