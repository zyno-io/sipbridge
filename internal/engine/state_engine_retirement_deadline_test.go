package engine

import (
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
)

func TestContextRetirementWaitsForExactRequestDispositionCutoff(t *testing.T) {
	r, c, tx := serverFinalFixture(t, sip.OPTIONS)
	selectFinal(t, r, c, tx, 1)
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	// Controller event settlement is qualified separately; this fixture has no
	// remaining physical or controller obligation.
	r.payload.Effects = nil
	if _, err := r.finish(); err != nil {
		t.Fatal(err)
	}
	if tx.Native.Phase != portable.Terminated || c.RetireMS == 0 {
		t.Fatal("fixture did not finish the reliable transaction")
	}
	// A response may finish before the original control deadline. Its admitted
	// request still has an immutable reconciliation floor beyond retirement.
	cutoff := tx.DispositionUntilMS
	if cutoff <= c.RetireMS {
		t.Fatal("fixture needs a disposition cutoff after context retirement")
	}
	r.now = c.RetireMS
	if _, err := r.finish(); err != nil {
		t.Fatal(err)
	}
	if c.Transactions[tx.Native.Key] != tx || r.group.Contexts[c.Snapshot.Context.Id] != c {
		t.Fatal("request proof retired before its cutoff")
	}
	retireID := r.timerID(c.Snapshot.Context.Id, "context", "retire", "")
	found := false
	for _, timer := range r.payload.Timers {
		if timer.ID == retireID {
			found = true
			if timer.DueMS != cutoff || timer.Generation != strconv.FormatInt(cutoff, 10) {
				t.Fatal("retirement polls before known cutoff")
			}
		}
	}
	if !found {
		t.Fatal("known retirement deadline lost")
	}
	r.now = cutoff
	if _, err := r.finish(); err != nil {
		t.Fatal(err)
	}
	if r.group.Contexts[c.Snapshot.Context.Id] != nil {
		t.Fatal("proven context did not retire at its fixed cutoff")
	}
}
