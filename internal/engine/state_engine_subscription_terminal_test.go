package engine

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// An accepted subscription can expire while its refresh is still waiting for
// the controller. SUBSCRIPTION is separate from Snapshot.Answered (INVITE only).
func liveSubscriptionWithUnansweredRefresh(t *testing.T) (*stateReduction, *stateContext, *stateTransaction) {
	t.Helper()
	r, c, initial := serverFinalFixture(t, sip.SUBSCRIBE)
	c.CompletionScope = ""
	selectFinal(t, r, c, initial, 1)
	completeLatestResponse(t, r, pb.DeliveryResult_WRITTEN)
	if c.Subscription == nil || c.Snapshot.Answered || c.Snapshot.Ended {
		t.Fatal("fixture needs accepted live subscription")
	}
	parsed, err := initial.Native.Origin.Parse()
	if err != nil {
		t.Fatal(err)
	}
	parsed.To().Params.Add("tag", c.Snapshot.LocalTag)
	parsed.CSeq().SeqNo++
	parsed.Via().Params.Add("branch", "z9hG4bK-unanswered-refresh")
	refresh := portable.Message{Wire: []byte(parsed.String()), Transport: "tls"}
	native, err := portable.NewTransaction(portable.NonInviteServer, refresh, portable.DefaultTimers(), c.Tag)
	if err != nil {
		t.Fatal(err)
	}
	native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		t.Fatal(err)
	}
	tx := &stateTransaction{Native: native, Flow: c.Flow, Application: c.Snapshot.Application, RequestID: "refresh"}
	if _, err = r.admitRequestFlow(tx, c.Flow, refresh.Wire); err != nil {
		t.Fatal(err)
	}
	c.Transactions[native.Key], c.Requests[tx.RequestID], c.Subscription.Pending = tx, native.Key, native.Key
	// Prior initial-response delivery is settled. The refresh itself has never
	// selected or emitted a response.
	r.payload.Effects = nil
	f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
	r.engine.store = f.engine.store
	r.group.Version = 1
	return r, c, tx
}

func expireSubscriptionFixture(t *testing.T, r *stateReduction, c *stateContext) {
	t.Helper()
	for _, at := range []int64{c.Subscription.ExpiresMS, c.Subscription.ExpiresMS + 5000} {
		r.now = at
		r.in = stateEngineInput{Kind: "timer", Timer: r.timerID(c.Snapshot.Context.Id, "subscription", "expiry", ""), Generation: strconv.FormatInt(at, 10)}
		if handled, err := r.lifecycleTimer(); err != nil || !handled {
			t.Fatal("subscription expiry failed", err)
		}
	}
	if !c.Snapshot.Ended {
		t.Fatal("subscription did not end")
	}
}

func TestSubscriptionExpiryEndsUnansweredRefreshAtTerminalTransition(t *testing.T) {
	r, c, tx := liveSubscriptionWithUnansweredRefresh(t)
	disposition := tx.DispositionUntilMS
	expireSubscriptionFixture(t, r, c)
	if tx.Native.Phase != portable.Terminated || tx.RetireMS != max(c.RetireMS, disposition) || tx.DispositionUntilMS != disposition {
		t.Fatal("terminal subscription transition left an unanswered refresh active or changed its proof")
	}
	r.pruneTransactions()
	if c.Transactions[tx.Native.Key] != tx {
		t.Fatal("request proof disappeared before retention cutoff")
	}
	// This fixture settles all generated controller events before retirement.
	r.payload.Effects = nil
	r.now = max(c.RetireMS, tx.RetireMS, tx.DispositionUntilMS) + 1
	payload, err := r.finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.group.Contexts) != 0 || len(payload.Timers) != 0 || payload.ActiveTransactions != 0 || payload.ReservedCompletionBytes != 0 || !payload.Retention.Terminal {
		t.Fatal("ended subscription retained polling or protocol completion reservation")
	}
}

func subscriptionNotifyFixture(t *testing.T, r *stateReduction, c *stateContext, refresh *stateTransaction) *stateTransaction {
	t.Helper()
	parsed, err := refresh.Native.Origin.Parse()
	if err != nil {
		t.Fatal(err)
	}
	request := parsed.(*sip.Request)
	request.Method, request.CSeq().MethodName = sip.NOTIFY, sip.NOTIFY
	request.Via().Params.Add("branch", "z9hG4bK-outstanding-notify")
	native, err := portable.NewTransaction(portable.NonInviteClient, portable.Message{Wire: []byte(request.String()), Transport: "tls"}, portable.DefaultTimers(), "")
	if err != nil {
		t.Fatal(err)
	}
	native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: r.now})
	if err != nil {
		t.Fatal(err)
	}
	tx := &stateTransaction{Native: native, Flow: c.Flow}
	c.Transactions[native.Key] = tx
	return tx
}

func TestSubscriptionTerminalNotifyEndsOnlyUnansweredServerRefresh(t *testing.T) {
	for _, status := range []int{200, 481} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			r, c, refresh := liveSubscriptionWithUnansweredRefresh(t)
			notify := subscriptionNotifyFixture(t, r, c, refresh)
			before, err := json.Marshal(notify)
			if err != nil {
				t.Fatal(err)
			}
			c.Subscription.Final = status == 200
			response, err := portable.BuildResponseWithHeaders(notify.Native.Origin, status, "test", nil, c.Tag, r.engine.cfg.ContactURI, nil)
			if err != nil {
				t.Fatal(err)
			}
			r.now += 100
			if err := r.subscriptionResponse(c, notify, response); err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(notify)
			if err != nil {
				t.Fatal(err)
			}
			if !c.Snapshot.Ended || refresh.Native.Phase != portable.Terminated || refresh.RetireMS != max(c.RetireMS, refresh.DispositionUntilMS) {
				t.Fatal("terminal NOTIFY did not end the unanswered refresh at its original proof floor")
			}
			if !bytes.Equal(before, after) || !notify.Native.Timers[portable.TimerF].Active {
				t.Fatal("terminal subscription path changed client NOTIFY state")
			}
			// A late duplicate response cannot extend retirement or retrospectively
			// terminate a preexisting ended record's Trying transaction.
			refresh.Native.Phase, refresh.RetireMS = portable.Trying, 0
			retire := c.RetireMS
			r.now += 1000
			if err := r.subscriptionResponse(c, notify, response); err != nil {
				t.Fatal(err)
			}
			if c.RetireMS != retire || refresh.Native.Phase != portable.Trying || refresh.RetireMS != 0 {
				t.Fatal("already ended subscription was retrospectively changed")
			}
		})
	}
}

func TestSubscriptionTerminalPreservesSelectedAndUncertainProcedures(t *testing.T) {
	guards := map[string]func(*testing.T, *stateReduction, *stateContext, *stateTransaction){
		"selected response": func(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction) {
			response, err := portable.BuildResponseWithHeaders(tx.Native.Origin, 200, "OK", nil, c.Tag, r.engine.cfg.ContactURI, nil)
			if err != nil {
				t.Fatal(err)
			}
			tx.Native.LastResponse = &response
		},
		"pending response": func(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.PendingResponse = &pb.Respond{Status: 200}
		},
		"deferred presentation": func(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.DeferredPresent = true
		},
		"native timer": func(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction) {
			tx.Native.Timers[portable.TimerJ] = portable.Deadline{Active: true, At: r.now + 1000}
		},
		"physical unknown": func(t *testing.T, r *stateReduction, c *stateContext, tx *stateTransaction) {
			data, err := (stateEngineEffect{Context: c.Snapshot.Context.Id, Transaction: tx.Native.Key, Native: true}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			r.payload.Effects = append(r.payload.Effects, state.Effect{Kind: "deliver", Status: state.EffectUnknown, Payload: data})
		},
	}
	for name, guard := range guards {
		t.Run(name, func(t *testing.T) {
			r, c, tx := liveSubscriptionWithUnansweredRefresh(t)
			guard(t, r, c, tx)
			before, err := json.Marshal(tx)
			if err != nil {
				t.Fatal(err)
			}
			expireSubscriptionFixture(t, r, c)
			after, err := json.Marshal(tx)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("subscription termination changed selected/uncertain transaction")
			}
			if name == "physical unknown" && r.payload.Effects[0].Status != state.EffectUnknown {
				t.Fatal("subscription termination changed unknown physical evidence")
			}
		})
	}
}
