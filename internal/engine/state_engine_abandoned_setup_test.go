package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// Only the profile is needed by this pure lifecycle test; no store operation is
// available through the fixture. End-to-end CAS admission is covered separately.
type abandonedSetupProfile struct{ state.Backend }

func (abandonedSetupProfile) Versions() state.Versions { return NativeStateVersions() }

func abandonedSubscribeFixture(t *testing.T) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, tx := advertiseFixture(t, "tls", sip.SUBSCRIBE, "")
	r.engine.store = abandonedSetupProfile{}
	c.Snapshot.Ended, c.Snapshot.Answered = true, false
	c.CompletionScope = ""
	c.RetireMS, c.ReplayUntilMS = r.now+120000, r.now+122000
	r.now = max(c.RetireMS, c.ReplayUntilMS, tx.DispositionUntilMS) + 1
	return r, c, tx
}

func TestAbandonedSubscribeSetupTerminatesWithoutAnotherRetentionWindow(t *testing.T) {
	r, c, tx := abandonedSubscribeFixture(t)
	if err := r.expireAbandonedSetupTransactions(); err != nil {
		t.Fatal(err)
	}
	if tx.Native.Phase != portable.Terminated || tx.RetireMS != c.RetireMS || tx.RetireMS > r.now || len(r.payload.Effects) != 0 {
		t.Fatal("abandoned setup retained Trying, renewed retention or emitted work")
	}
	r.pruneTransactions()
	if len(c.Transactions) != 0 || !r.contextRetirementProven(c.Snapshot.Context.Id, c) {
		t.Fatal("abandoned setup did not release transaction/retirement accounting")
	}
}

func TestAbandonedSetupCannotCollectLiveObligations(t *testing.T) {
	guards := map[string]func(*stateReduction, *stateContext, *stateTransaction){
		"live context":  func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.Snapshot.Ended = false },
		"answered call": func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.Snapshot.Answered = true },
		"dialog":        func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.Dialog = "dialog" },
		"retained dialog": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			c.Dialogs["dialog"] = portable.Dialog{}
		},
		"subscription":         func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.Subscription = &stateSubscription{} },
		"completion scope":     func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.CompletionScope = "scope" },
		"owner cleanup":        func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.OwnerCleanupAtMS = r.now + 1 },
		"owner probe":          func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.OwnerProbeEffect = "probe" },
		"retirement floor":     func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.RetireMS = r.now + 1 },
		"replay floor":         func(r *stateReduction, c *stateContext, tx *stateTransaction) { c.ReplayUntilMS = r.now + 1 },
		"disposition floor":    func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.DispositionUntilMS = r.now + 1 },
		"unproven disposition": func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.DispositionUntilMS = 0 },
		"current request": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			r.requestFence = &stateRequestFence{context: c.Snapshot.Context.Id, transaction: tx.Native.Key}
		},
		"client NOTIFY": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.Native.Family = portable.NonInviteClient
		},
		"INVITE server": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.Native.Family = portable.InviteServer
		},
		"selected response": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.Native.LastResponse = &portable.Message{Wire: []byte("selected final")}
		},
		"native timer": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.Native.Timers[portable.TimerF] = portable.Deadline{Active: true, At: r.now + 1}
		},
		"written":          func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.Written = true },
		"deferred owner":   func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.OwnerDeferred = true },
		"deferred present": func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.DeferredPresent = true },
		"pending response": func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.PendingResponse = &pb.Respond{} },
		"final response":   func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.FinalResponse = &stateServerFinal{} },
		"credential":       func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.DigestEffect = "credential" },
		"response budget":  func(r *stateReduction, c *stateContext, tx *stateTransaction) { tx.RespondDeadlineMS = r.now + 1 },
		"unknown physical": func(r *stateReduction, c *stateContext, tx *stateTransaction) {
			data, err := (stateEngineEffect{Context: c.Snapshot.Context.Id, Native: true}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			r.payload.Effects = []state.Effect{{Kind: "deliver", Status: state.EffectUnknown, Payload: data}}
		},
	}
	for name, guard := range guards {
		t.Run(name, func(t *testing.T) {
			r, c, tx := abandonedSubscribeFixture(t)
			guard(r, c, tx)
			if err := r.expireAbandonedSetupTransactions(); err != nil || tx.Native.Phase != portable.Trying || tx.RetireMS != 0 {
				t.Fatal("live evidence was collected", err)
			}
		})
	}
}
