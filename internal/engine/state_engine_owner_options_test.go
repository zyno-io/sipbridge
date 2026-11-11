package engine

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestNativeOwnerLossEndsUnsendableRetainedOptionsWithoutPhysicalRetry(t *testing.T) {
	for _, branch := range []string{"", "old-branch", sip.RFC3261BranchMagicCookie, sip.RFC3261BranchMagicCookie + "-supported"} {
		t.Run("branch-"+branch, func(t *testing.T) {
			f := newNativeOwnerFixtureBackend(t, state.MemoryBackend)
			message := stateTestRequest("OPTIONS", 1, "", "unanswered-options")
			ref, err := f.engine.Locate(context.Background(), message)
			if err != nil {
				t.Fatal(err)
			}
			result := f.apply(t, ref, "initial-options", stateEngineInput{Kind: "ingest", Application: f.app, ApplicationLeaseTerm: f.term,
				Ingest: &pb.OwnerIngest{Context: ref, Application: f.app, Flow: f.flow, Wire: message.Wire}})
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			record, group := stateTestLoad(t, f.engine, ref)
			c := group.Contexts[ref.Id]
			var tx *stateTransaction
			for _, current := range c.Transactions {
				tx = current
			}
			if tx == nil {
				t.Fatal("fixture did not admit OPTIONS")
			}
			// Reconstruct a previously retained request, without passing the new
			// ingress validation. The pinned reducer still owns its transaction.
			parsed, err := tx.Native.Origin.Parse()
			if err != nil {
				t.Fatal(err)
			}
			parsed.Via().Params.Remove("branch")
			if branch != "" {
				parsed.Via().Params.Add("branch", branch)
			}
			origin := tx.Native.Origin
			origin.Wire = []byte(parsed.String())
			native, err := portable.NewTransaction(portable.NonInviteServer, origin, portable.DefaultTimers(), c.Tag)
			if err != nil {
				t.Fatal(err)
			}
			native, _, err = portable.StepTransaction(native, portable.Input{Kind: portable.Start, Now: record.ApplicationLeases[f.engine.applicationLeaseID(f.app)].CheckedAtMS})
			if err != nil {
				t.Fatal(err)
			}
			tx.Native, c.Origin = native, origin
			c.Transactions = map[string]*stateTransaction{native.Key: tx}
			c.Requests = map[string]string{tx.RequestID: native.Key}
			proofs := append([]string(nil), tx.AdmittedFlowProofs...)
			record.Payload.Effects = nil
			record.Payload.ProtocolState, err = marshalRetainedFixture(group)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Add(30 * time.Minute).UnixMilli()
			leaseID := f.engine.applicationLeaseID(f.app)
			lease := record.ApplicationLeases[leaseID]
			lease.CheckedAtMS, lease.ExpiresAtMS, lease.RecoveryUntilMS = now, now-60000, now-50000
			record.ApplicationLeases[leaseID] = lease
			// The physical socket belonged to the former process. No controller
			// or transport implementation is installed in this pure reduction.
			f.engine.cfg.Process = &pb.ProcessRef{InstanceId: "replacement-worker", Epoch: "new-epoch"}
			r := &stateReduction{engine: f.engine}
			input, err := marshalRetainedFixture(stateEngineInput{Kind: "timer", Timer: r.timerID(ref.Id, "owner", "probe", ""), Generation: strconv.FormatInt(c.OwnerProbeAtMS, 10)})
			if err != nil {
				t.Fatal(err)
			}
			next, _, err := f.engine.reduce(record, state.Input{Kind: "timer", Proof: true, Payload: input, DeadlineMS: now + 2000}, now)
			if err != nil {
				t.Fatal("owner timer could not fence the dead process", err)
			}
			updated := record
			updated.Payload = next
			retained, err := decodeStateGroup(updated)
			if err != nil {
				t.Fatal(err)
			}
			c = retained.Contexts[ref.Id]
			if c == nil || !c.OwnerLost || len(next.CommitLeaseFences) != 1 || next.CommitLeaseFences[0].Term != f.term {
				t.Fatal("owner loss did not preserve the permanent fence and retained context")
			}
			if tx := c.Transactions[native.Key]; tx == nil || tx.Native.Phase != portable.Terminated || len(tx.AdmittedFlowProofs) != len(proofs) || tx.AdmittedFlowProofs[0] != proofs[0] {
				t.Fatal("termination lost original request-disposition proof")
			}
			deliveries := 0
			for _, effect := range next.Effects {
				if effect.Kind == "wire" || effect.Kind == "deliver" {
					deliveries++
				}
			}
			want := 0
			if branch == sip.RFC3261BranchMagicCookie+"-supported" {
				want = 1
			}
			if deliveries != want {
				t.Fatal("unsendable response created output, or supported response was suppressed", deliveries, want)
			}
			for _, timer := range next.Timers {
				if timer.DueMS <= now {
					t.Fatal("owner timer left the old permanently due head")
				}
			}
		})
	}
}

func TestObserveRequiresSupportedViaBranchBeforeAdmission(t *testing.T) {
	for _, method := range []string{"OPTIONS", "INVITE", "REGISTER", "SUBSCRIBE", "ACK", "BYE", "CANCEL"} {
		for _, branch := range []string{"", "old-branch", sip.RFC3261BranchMagicCookie, sip.RFC3261BranchMagicCookie + "-supported"} {
			t.Run(method+"/"+branch, func(t *testing.T) {
				message := stateTestRequest(method, 1, "", "branch-admission")
				parsed, err := sip.ParseMessage(message.Wire)
				if err != nil {
					t.Fatal(err)
				}
				parsed.Via().Params.Remove("branch")
				if branch != "" {
					parsed.Via().Params.Add("branch", branch)
				}
				_, err = Observe([]byte(parsed.String()), false)
				if (err == nil) != (branch == sip.RFC3261BranchMagicCookie+"-supported") {
					t.Fatal("unsupported request reached observation or supported request was refused", err)
				}
			})
		}
	}
}
