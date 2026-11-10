package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestApplicationLeaseObservationsJoinPayloadAndPendingInput(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	for _, id := range []string{"owner-a", "owner-b"} {
		if _, err := s.AcquireApplicationLease(ctx, id, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	ref := group(t, s, "lease-observations")
	initial := input("initial", true)
	initial.ApplicationLeaseIDs = []string{"owner-a"}
	first, err := s.Begin(ctx, ref, "worker-a", initial)
	if err != nil || first.Step == nil || len(first.Step.Record.ApplicationLeases) != 1 {
		t.Fatalf("fresh input did not observe its owner: %+v %v", first, err)
	}
	if _, err = s.Commit(ctx, *first.Step, Payload{ApplicationLeaseIDs: []string{"owner-a"}}, nil); err != nil {
		t.Fatal(err)
	}
	ref = first.Step.Record.Ref
	next := input("next", false)
	next.ApplicationLeaseIDs = []string{"owner-a", "owner-b"}
	pending, err := s.Begin(ctx, ref, "worker-a", next)
	if err != nil || pending.Step == nil || len(pending.Step.Record.ApplicationLeases) != 2 {
		t.Fatalf("input/payload projection did not join: %+v %v", pending, err)
	}
	expireApplicationLease(t, s, "owner-a")
	loaded, err := s.Load(ctx, ref)
	if err != nil || len(loaded.ApplicationLeases) != 2 || loaded.ApplicationLeases["owner-a"].ExpiresAtMS > loaded.ApplicationLeases["owner-a"].CheckedAtMS {
		t.Fatalf("load did not refresh the admitted projection: %+v %v", loaded.ApplicationLeases, err)
	}
	if err = s.client.HSet(ctx, s.keys(ref)[0], "claimUntilMs", "0").Err(); err != nil {
		t.Fatal(err)
	}
	resumed, err := s.Resume(ctx, ref, "worker-b")
	if err != nil || resumed.Step == nil || resumed.Step.Input.ID != next.ID || len(resumed.Step.Record.ApplicationLeases) != 2 {
		t.Fatalf("resume did not refresh original input owners: %+v %v", resumed, err)
	}
	for _, lease := range resumed.Step.Record.ApplicationLeases {
		if lease.CheckedAtMS != resumed.Step.NowMS {
			t.Fatal("lease evidence and group snapshot were not read at the same Redis time")
		}
	}
}

func TestApplicationLeaseCommitFencePreservesPendingAndReleasesClaim(t *testing.T) {
	s := testStore(t, nil)
	readyStore(t, s)
	ctx := context.Background()
	lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ref := group(t, s, "commit-fence")
	candidate := input("business", true)
	candidate.ApplicationLeaseIDs = []string{"owner"}
	begun, err := s.Begin(ctx, ref, "worker-a", candidate)
	if err != nil {
		t.Fatal(err)
	}
	step := *begun.Step
	expireApplicationLease(t, s, "owner")
	next := Payload{ProtocolState: []byte("must-not-commit"), ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{{ID: "owner", Term: lease.Term}}}
	if _, err = s.Commit(ctx, step, next, []byte("must-not-return")); !errors.Is(err, ErrStale) {
		t.Fatalf("expired lease committed business effects: %v", err)
	}
	current, err := s.Load(ctx, step.Record.Ref)
	if err != nil || current.Pending == nil || current.Pending.ID != candidate.ID || current.Revision != step.Record.Revision || len(current.Payload.ProtocolState) != 0 {
		t.Fatalf("guard failure changed or lost the admitted operation: %+v %v", current, err)
	}
	resumed, err := s.Resume(ctx, step.Record.Ref, "worker-b")
	if err != nil || resumed.Step == nil || resumed.Step.Claim.Token == step.Claim.Token {
		t.Fatalf("guard failure retained a blocking computation claim: %+v %v", resumed, err)
	}
	if observed := resumed.Step.Record.ApplicationLeases["owner"]; observed.Term != lease.Term || observed.ExpiresAtMS > observed.CheckedAtMS {
		t.Fatalf("retry did not observe exact expired authority: %+v", observed)
	}
}

func TestApplicationLeaseImplicitReserveFenceAndHistoricalUnknown(t *testing.T) {
	for _, rotation := range []bool{false, true} {
		t.Run(map[bool]string{false: "expiry", true: "successor-term"}[rotation], func(t *testing.T) {
			s := testStore(t, nil)
			readyStore(t, s)
			ctx := context.Background()
			lease, err := s.AcquireApplicationLease(ctx, "owner", uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			ref := group(t, s, "reserve-fence")
			step := begin(t, s, ref, "authorized", true)
			guard := &ApplicationLeaseRef{ID: "owner", Term: lease.Term}
			payload := Payload{ApplicationLeaseIDs: []string{"owner"}, Effects: []Effect{
				{ID: "app", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: time.Now().Add(time.Minute).UnixMilli(), ApplicationLease: guard},
				{ID: "unknown", Kind: "deliver", Status: EffectUnknown, AttemptID: "physical-attempt", DeadlineMS: 1, ApplicationLease: guard},
				{ID: "cleanup", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: time.Now().Add(time.Minute).UnixMilli()},
			}}
			if _, err = s.Commit(ctx, *step, payload, nil); err != nil {
				t.Fatal(err)
			}
			ref = step.Record.Ref
			reservation := begin(t, s, ref, "reserve", false)
			if got := reservation.Record.ApplicationLeases["owner"]; got.Term != lease.Term || got.ExpiresAtMS <= reservation.NowMS {
				t.Fatalf("reservation lost its conservative start horizon: %+v", got)
			}
			expireApplicationLease(t, s, "owner")
			if rotation {
				if _, err = s.AcquireApplicationLease(ctx, "owner", uuid.NewString()); err != nil {
					t.Fatal(err)
				}
			}
			// Native reducers own and may mutate their loaded effect slice. The
			// Store must retain its original reservation fence independently.
			payload = reservation.Record.Payload
			payload.Effects[0].Status, payload.Effects[0].AttemptID = EffectDispatching, "attempt"
			if _, err = s.Commit(ctx, *reservation, payload, []byte("wire-must-not-escape")); !errors.Is(err, ErrStale) {
				t.Fatalf("lease fence was not implicit at reservation: %v", err)
			}
			loaded, err := s.Load(ctx, ref)
			if err != nil || loaded.Payload.Effects[0].Status != EffectAuthorized || loaded.Payload.Effects[1].Status != EffectUnknown || loaded.Payload.Effects[1].AttemptID != "physical-attempt" {
				t.Fatalf("refused reservation changed physical evidence: %+v %v", loaded.Payload.Effects, err)
			}
			retry, err := s.Resume(ctx, ref, "worker-b")
			if err != nil {
				t.Fatal(err)
			}
			payload = retry.Step.Record.Payload
			payload.Effects[2].Status, payload.Effects[2].AttemptID = EffectDispatching, "cleanup-attempt"
			if _, err = s.Commit(ctx, *retry.Step, payload, []byte("native-cleanup")); err != nil {
				t.Fatalf("native cleanup incorrectly required expired business authority: %v", err)
			}
		})
	}
}

func TestApplicationLeaseGuardCannotBeStrippedAndMetadataIsBounded(t *testing.T) {
	term := uuid.NewString()
	guard := &ApplicationLeaseRef{ID: "owner", Term: term}
	previous := Payload{ApplicationLeaseIDs: []string{"owner"}, Effects: []Effect{{ID: "effect", ApplicationLease: guard}}}
	next := Payload{Effects: []Effect{{ID: "effect", Status: EffectDispatching}}}
	if _, err := commitApplicationLeaseGuards(previous, next); !errors.Is(err, ErrConflict) {
		t.Fatalf("reservation stripped its admitted lease guard: %v", err)
	}
	next = Payload{ApplicationLeaseIDs: []string{"owner"}, CommitLeaseGuards: []ApplicationLeaseRef{*guard}, Effects: previous.Effects}
	encoded, err := json.Marshal(next)
	if err != nil || strings.Contains(string(encoded), "commitLeaseGuards") || !strings.Contains(string(encoded), "applicationLease") {
		t.Fatalf("transient requirements leaked or persistent guards disappeared: %s %v", encoded, err)
	}
	projection, _ := json.Marshal(next.ApplicationLeaseIDs)
	s := payloadValidationStore()
	s.cfg.Limits.GroupBytes = len(encoded) + len(projection) + 4096
	if err := s.validatePayloadSize(next, len(encoded)); err != nil {
		t.Fatalf("exact lease metadata boundary rejected: %v", err)
	}
	s.cfg.Limits.GroupBytes--
	if err := s.validatePayloadSize(next, len(encoded)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("lease projection/guard metadata escaped byte accounting: %v", err)
	}
	for _, ids := range [][]string{{"owner", "owner"}, {"owner\n"}, make([]string, 1025)} {
		if err := validateApplicationLeaseIDs(ids); err == nil {
			t.Fatal("invalid/repeated/oversized lease projection was accepted")
		}
	}
}
