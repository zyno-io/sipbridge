package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
)

func TestStateRetiringDialogLookupRefusesResumeBeforeBusinessRouting(t *testing.T) {
	f, ref, _ := publicByeFixture(t)
	f.engine.work = make(chan struct{}, 8)
	_, before := stateTestLoad(t, f.engine, ref)
	c := before.Contexts[ref.Id]
	var remote portable.Dialog
	for _, tx := range c.Transactions {
		if tx.Native.Family == portable.InviteServer && tx.Native.LastResponse != nil {
			var err error
			remote, err = portable.NewDialog(portable.UAC, tx.Native.Origin, *tx.Native.LastResponse, portable.DefaultTimers())
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	resume, err := portable.BuildDialogRequest(remote, sip.INVITE, 2, nil, "z9hG4bK-retired-resume")
	if err != nil {
		t.Fatal(err)
	}
	message, err := Observe(resume.Wire, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.engine.LocateRoute(context.Background(), message)
	var refusal *ingestError
	if !errors.As(err, &refusal) || refusal.status != 481 {
		t.Fatalf("retiring resume entered business routing instead of native 481: %v", err)
	}
	_, after := stateTestLoad(t, f.engine, ref)
	if !reflect.DeepEqual(c.Dialogs, after.Contexts[ref.Id].Dialogs) || before.NextAllocation != after.NextAllocation ||
		len(after.Allocations) != len(before.Allocations) || after.Contexts[ref.Id].Snapshot.CommandSequence != c.Snapshot.CommandSequence {
		t.Fatal("negative lookup changed the retired dialog or allocated application work")
	}
	// The actual ingress rejection remains a retained exact-flow transaction,
	// with no connected Controller or business callback required by this fixture.
	if err = f.engine.Reject(context.Background(), f.flow, message, 481); err != nil {
		t.Fatal(err)
	}
	_, rejected := stateTestLoad(t, f.engine, ref)
	found := false
	for _, candidate := range rejected.Contexts {
		if !candidate.Rejection {
			continue
		}
		for _, tx := range candidate.Transactions {
			if !stateExactServerRequest(tx, resume) || tx.Native.LastResponse == nil {
				continue
			}
			parsed, parseErr := tx.Native.LastResponse.Parse()
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			response, ok := parsed.(*sip.Response)
			found = ok && response.StatusCode == 481
		}
	}
	if !found || !reflect.DeepEqual(c.Dialogs, rejected.Contexts[ref.Id].Dialogs) {
		t.Fatal("native rejection lost its exact 481 or changed the original dialog")
	}
}
