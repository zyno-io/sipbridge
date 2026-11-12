package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

type casRead struct {
	result state.UpdateResult
	err    error
}

type casScriptedStore struct {
	reads       []casRead
	readRefs    []state.Ref
	casErrors   []error
	casCalls    int
	inspectedAt state.Ref
	inspection  state.Inspection
	inspections int
}

func (s *casScriptedStore) ReadUpdate(_ context.Context, ref state.Ref, _ state.Input) (state.UpdateResult, error) {
	s.readRefs = append(s.readRefs, ref)
	if len(s.reads) == 0 {
		return state.UpdateResult{}, errors.New("unexpected update read")
	}
	read := s.reads[0]
	s.reads = s.reads[1:]
	return read.result, read.err
}

func (s *casScriptedStore) CompareAndSwap(_ context.Context, update state.Update, _ state.Payload, out []byte) (state.Receipt, error) {
	s.casCalls++
	var err error
	if len(s.casErrors) > 0 {
		err, s.casErrors = s.casErrors[0], s.casErrors[1:]
	}
	return state.Receipt{OperationID: update.Input.ID, Fingerprint: update.Input.Fingerprint, Outcome: out}, err
}

func (s *casScriptedStore) InspectOperation(_ context.Context, ref state.Ref, _, _ string) (state.Inspection, error) {
	s.inspectedAt = ref
	s.inspections++
	return s.inspection, nil
}

func updateRead(in state.Input, existing bool) casRead {
	ref := state.Ref{ID: "group", Incarnation: "observed-incarnation"}
	read := casRead{result: state.UpdateResult{Update: &state.Update{Record: state.Record{Ref: ref}, Input: in, NowMS: 100}}}
	if existing {
		read.result.Ref = &ref
	}
	return read
}

func casTestDriver(t *testing.T, store *casScriptedStore) *CASRuntime {
	t.Helper()
	driver, err := NewCAS(store, ReducerFunc(func(record state.Record, in state.Input, _ int64) (state.Payload, []byte, error) {
		return record.Payload, []byte(in.ID), nil
	}), 1)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func TestCASDriverNeedsOnlyCASStoreAndExposesNoResume(t *testing.T) {
	in := state.Input{ID: "command", Fingerprint: "payload"}
	known := state.Receipt{OperationID: in.ID, Fingerprint: in.Fingerprint, Outcome: []byte(in.ID)}
	store := &casScriptedStore{reads: []casRead{updateRead(in, true), {result: state.UpdateResult{Receipt: &known}}}}
	// This store implements only read/update, CAS and exact inspection: no
	// Begin, Resume or claimed Commit methods exist to satisfy construction.
	var driver Driver = casTestDriver(t, store)
	if !driver.UsesCAS() {
		t.Fatal("CAS driver reported legacy execution")
	}
	if _, ok := driver.(interface {
		Resume(context.Context, state.Ref) (state.Receipt, error)
	}); ok {
		t.Fatal("CAS driver exposes legacy recovery")
	}
	fresh, err := driver.ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in)
	if err != nil || string(fresh.Receipt.Outcome) != in.ID || fresh.Committed == nil {
		t.Fatalf("CAS-only store lost fresh commit: %+v, %v", fresh, err)
	}
	replay, err := driver.ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in)
	if err != nil || string(replay.Receipt.Outcome) != in.ID || replay.Committed != nil || store.casCalls != 1 {
		t.Fatalf("CAS-only store replay recommitted: %+v CAS=%d, %v", replay, store.casCalls, err)
	}
}

func TestCASDriverRejectsMissingStore(t *testing.T) {
	_, err := NewCAS(nil, ReducerFunc(func(state.Record, state.Input, int64) (state.Payload, []byte, error) {
		t.Fatal("missing store reduced input")
		return state.Payload{}, nil, nil
	}), 1)
	if err == nil {
		t.Fatal("accepted a missing store")
	}
}

func TestCASDriverRefusesUnexpectedPendingWithoutReduction(t *testing.T) {
	in := state.Input{ID: "command", Fingerprint: "payload"}
	read := updateRead(in, true)
	read.result.Update.Record.Pending = &state.Input{ID: "legacy"}
	store := &casScriptedStore{reads: []casRead{read}}
	driver, err := NewCAS(store, ReducerFunc(func(state.Record, state.Input, int64) (state.Payload, []byte, error) {
		t.Fatal("CAS driver reduced retained legacy input")
		return state.Payload{}, nil, nil
	}), 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = driver.Apply(context.Background(), state.Ref{ID: "group"}, in)
	if !errors.Is(err, ErrUncommitted) || !errors.Is(err, state.ErrBusy) || store.casCalls != 0 || len(store.readRefs) != 1 {
		t.Fatalf("unexpected pending input was not refused: reads=%d CAS=%d err=%v", len(store.readRefs), store.casCalls, err)
	}
}

func TestCASDriverPinsExistingIncarnationAfterDefiniteConflict(t *testing.T) {
	in := state.Input{ID: "command", Fingerprint: "payload"}
	store := &casScriptedStore{reads: []casRead{updateRead(in, true), {err: state.ErrNotFound}}, casErrors: []error{state.ErrStale}}
	driver := casTestDriver(t, store)
	_, err := driver.Apply(context.Background(), state.Ref{ID: "group"}, in)
	if !errors.Is(err, state.ErrNotFound) || !errors.Is(err, ErrUncommitted) || len(store.readRefs) != 2 || store.readRefs[1].Incarnation != "observed-incarnation" || store.casCalls != 1 {
		t.Fatalf("existing group could become a fresh creator: refs=%+v CAS=%d err=%v", store.readRefs, store.casCalls, err)
	}
}

func TestCASDriverBusyLegacyInputIsBoundedAndPinsIncarnation(t *testing.T) {
	ref := state.Ref{ID: "group", Incarnation: "legacy-incarnation"}
	busy := casRead{result: state.UpdateResult{Ref: &ref}, err: state.ErrBusy}
	store := &casScriptedStore{reads: make([]casRead, 100)}
	for n := range store.reads {
		store.reads[n] = busy
	}
	in := state.Input{ID: "command", DeadlineMS: time.Now().Add(20 * time.Millisecond).UnixMilli()}
	_, err := casTestDriver(t, store).Apply(context.Background(), state.Ref{ID: "group"}, in)
	if !errors.Is(err, ErrUncommitted) || !errors.Is(err, context.DeadlineExceeded) || len(store.readRefs) < 2 || len(store.readRefs) > 20 || store.readRefs[1].Incarnation != ref.Incarnation || store.casCalls != 0 {
		t.Fatalf("busy work was stolen or retried without bound: reads=%+v CAS=%d err=%v", store.readRefs, store.casCalls, err)
	}
}

func TestCASDriverContentionDoesNotDiscardHealthyWorkAfterThreeRaces(t *testing.T) {
	in := state.Input{ID: "command", Fingerprint: "payload", DeadlineMS: time.Now().Add(time.Second).UnixMilli()}
	store := &casScriptedStore{reads: make([]casRead, 5), casErrors: []error{state.ErrStale, state.ErrStale, state.ErrStale, state.ErrStale, nil}}
	for n := range store.reads {
		store.reads[n] = updateRead(in, true)
	}
	result, err := casTestDriver(t, store).Apply(context.Background(), state.Ref{ID: "group"}, in)
	if err != nil || result.OperationID != in.ID || store.casCalls != 5 {
		t.Fatalf("normal contention discarded admitted work: %+v CAS=%d err=%v", result, store.casCalls, err)
	}
}

func TestCASDriverIssuedMutationTransportChangeIsNotRetried(t *testing.T) {
	in := state.Input{ID: "internal", Fingerprint: "payload", Proof: true, Kind: "complete"}
	store := &casScriptedStore{reads: []casRead{updateRead(in, true)}, casErrors: []error{errors.Join(state.ErrUnknown, state.ErrStale)}}
	_, err := casTestDriver(t, store).Apply(context.Background(), state.Ref{ID: "group"}, in)
	if !errors.Is(err, state.ErrUnknown) || errors.Is(err, ErrUncommitted) || len(store.readRefs) != 1 || store.casCalls != 1 || store.inspections != 0 {
		t.Fatalf("uncertain internal mutation replayed: reads=%d CAS=%d inspections=%d err=%v", len(store.readRefs), store.casCalls, store.inspections, err)
	}
}

func TestCASDriverUnknownCleanupInspectsSameProposedIncarnation(t *testing.T) {
	in := state.Input{ID: "cleanup", Fingerprint: "payload", Proof: true, Kind: "command"}
	store := &casScriptedStore{reads: []casRead{updateRead(in, false)}, casErrors: []error{state.ErrUnknown}}
	store.inspection = state.Inspection{Receipt: &state.Receipt{OperationID: in.ID, Fingerprint: in.Fingerprint, Outcome: []byte("known")}}
	result, err := casTestDriver(t, store).Apply(context.Background(), state.Ref{ID: "group"}, in)
	if err != nil || string(result.Outcome) != "known" || store.inspectedAt.Incarnation != "observed-incarnation" || store.inspections != 1 || store.casCalls != 1 {
		t.Fatalf("unknown cleanup lost exact proof: %+v at=%+v CAS=%d err=%v", result, store.inspectedAt, store.casCalls, err)
	}
}

func TestCASDriverReadOutageIsKnownUncommitted(t *testing.T) {
	store := &casScriptedStore{reads: []casRead{{err: state.ErrAuthority}}}
	_, err := casTestDriver(t, store).Apply(context.Background(), state.Ref{ID: "group"}, state.Input{})
	if !errors.Is(err, ErrUncommitted) || !errors.Is(err, state.ErrAuthority) || errors.Is(err, state.ErrUnknown) || store.casCalls != 0 {
		t.Fatalf("read outage manufactured uncertainty: CAS=%d err=%v", store.casCalls, err)
	}
}

func TestCASDriverExpiredAdmissionStillReturnsExactCommittedReplay(t *testing.T) {
	in := state.Input{ID: "known", Fingerprint: "payload", DeadlineMS: time.Now().Add(-time.Second).UnixMilli()}
	store := &casScriptedStore{reads: []casRead{{result: state.UpdateResult{Receipt: &state.Receipt{OperationID: in.ID, Outcome: []byte("known-result")}}}}}
	result, err := casTestDriver(t, store).Apply(context.Background(), state.Ref{ID: "group"}, in)
	if err != nil || string(result.Outcome) != "known-result" || store.casCalls != 0 {
		t.Fatalf("fresh admission expiry hid a committed result: %+v CAS=%d err=%v", result, store.casCalls, err)
	}
}

func TestCASFreshCommitReturnsOwnedDispatchProjection(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "effects"}[populated], func(t *testing.T) {
			in := state.Input{ID: "command"}
			read := updateRead(in, true)
			if populated {
				read.result.Update.Record.Payload.Effects = []state.Effect{{ID: "effect", Kind: "deliver", Payload: []byte("committed"),
					ApplicationLease: &state.ApplicationLeaseRef{ID: "lease", Term: "term"}}}
				read.result.Update.Record.Payload.LocalObservations = []state.LocalObservation{{ContextID: "context", OperationID: "command", Outcome: []byte("local")}}
				read.result.Update.Record.Payload.LocalResult = &pb.OperationResult{SipStatus: 200}
			}
			store := &casScriptedStore{reads: []casRead{read}}
			result, err := casTestDriver(t, store).ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in)
			if err != nil || result.Committed == nil || result.Committed.Ref.Incarnation != "observed-incarnation" ||
				len(result.Committed.Effects) != len(read.result.Update.Record.Payload.Effects) {
				t.Fatalf("fresh commit lost dispatch evidence: %+v, %v", result, err)
			}
			if populated {
				read.result.Update.Record.Payload.LocalResult.SipStatus = 503
				if result.Committed.LocalResult == nil || result.Committed.LocalResult.SipStatus != 200 {
					t.Fatal("fresh result DTO shared reducer-owned memory")
				}
				read.result.Update.Record.Payload.LocalObservations[0].Outcome[0] = 'X'
				if len(result.Committed.LocalObservations) != 1 || string(result.Committed.LocalObservations[0].Outcome) != "local" {
					t.Fatal("local observation shared reducer-owned memory")
				}
				source := &read.result.Update.Record.Payload.Effects[0]
				source.Payload[0] = 'X'
				source.ApplicationLease.Term = "changed"
				if string(result.Committed.Effects[0].Payload) != "committed" || result.Committed.Effects[0].ApplicationLease.Term != "term" {
					t.Fatal("projection shared reducer-owned memory")
				}
			}
		})
	}
}

func TestCASReplayOrUnknownInspectionDoesNotProjectUnprovenEffects(t *testing.T) {
	in := state.Input{ID: "command", Kind: "command"}
	known := state.Receipt{OperationID: in.ID, Outcome: []byte("known")}
	for _, inspected := range []bool{false, true} {
		store := &casScriptedStore{reads: []casRead{{result: state.UpdateResult{Receipt: &known}}}}
		if inspected {
			store.reads = []casRead{updateRead(in, true)}
			store.reads[0].result.Update.Record.Payload.LocalObservations = []state.LocalObservation{{ContextID: "context", OperationID: "command", Outcome: []byte("unproven")}}
			store.reads[0].result.Update.Record.Payload.LocalResult = &pb.OperationResult{SipStatus: 200}
			store.casErrors = []error{state.ErrUnknown}
			store.inspection.Receipt = &known
		}
		result, err := casTestDriver(t, store).ApplyCommitted(context.Background(), state.Ref{ID: "group"}, in)
		if err != nil || string(result.Receipt.Outcome) != "known" || result.Committed != nil {
			t.Fatalf("replay projected unproven state: %+v, %v", result, err)
		}
	}
}
