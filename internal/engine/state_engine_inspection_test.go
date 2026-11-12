package engine

import (
	"encoding/json"
	"github.com/emiago/sipgo/sip"
	"strings"
	"testing"
)

func TestRetainedInspectionRetirementGuardsRemainNonidentifying(t *testing.T) {
	r, c, tx := advertiseFixture(t, "tls", sip.SUBSCRIBE, "")
	c.Snapshot.Ended = true
	c.RetireMS, c.ReplayUntilMS = r.now+120000, r.now+122000
	r.now = c.ReplayUntilMS + 1
	r.group.Version = 1
	const secret = "opaque-secret-completion-or-effect"
	c.CompletionScope, c.OwnerProbeEffect = secret, secret
	c.OwnerCleanupAtMS = r.now - 1
	tx.CompletionScope, tx.DigestEffect = secret, secret
	tx.DeferredPresent, tx.OwnerDeferred, tx.Written = true, true, true
	tx.RespondStatus, tx.RespondDeadlineMS = 503, r.now-2
	tx.RetireMS = r.now - 3
	data, err := r.group.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	view, err := InspectRetainedProtocol(data)
	if err != nil {
		t.Fatal(err)
	}
	if view.ContextRetirementGuards["completionScope"] != 1 || view.ContextRetirementGuards["ownerProbeEffect"] != 1 ||
		view.TransactionRetirementGuards["deferredPresent"] != 1 || view.TransactionRetirementGuards["respondStatus"] != 1 ||
		view.LatestReplayUntilMS != c.ReplayUntilMS || view.LatestTransactionRetireMS != tx.RetireMS || view.LatestRespondDeadlineMS != tx.RespondDeadlineMS {
		t.Fatal("retirement blocker classification omitted retained evidence")
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), c.Snapshot.CallId) {
		t.Fatal("redacted inspection exported identity or authority")
	}
}
