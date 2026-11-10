package state

import (
	"container/heap"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// MemoryStore is a single-process authority. Only this adapter retains protocol
// values; the engine uses the same transient CAS execution as Redis.
type MemoryStore struct {
	metadata
	gate                chan struct{}
	available           atomic.Bool
	closed              bool
	clock               func() time.Time
	lastClock           time.Time
	groups              map[string]*memoryGroup
	schedules           []memorySchedule
	leases              map[string]*memoryLease
	applicationContexts map[string]map[string]ApplicationContextEntry
	leaseOrder          list.List
}

type memoryGroup struct {
	ref                Ref
	revision           string
	data               []byte
	receipts           []Receipt
	proof              *Receipt
	ready, due, retire int64
}

type memoryLease struct {
	ApplicationLease
	references           int64
	sealed               bool
	idleUntil            int64
	element              *list.Element
	tokenHash            string
	lastTokenHash        string
	lastNextTokenHash    string
	lastAttachGeneration uint64
}

type memoryUpdate struct {
	store            *MemoryStore
	ref              Ref
	revision, digest string
	priorEffects     map[string]applicationLeaseEffect
}

func NewMemory(cfg Config) (*MemoryStore, error) { return newMemory(cfg, time.Now) }

func newMemory(cfg Config, clock func() time.Time) (*MemoryStore, error) {
	if len(cfg.NamespaceKey) == 0 {
		cfg.NamespaceKey = make([]byte, 32)
		if _, err := rand.Read(cfg.NamespaceKey); err != nil {
			return nil, err
		}
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "sipbridge"
	}
	if err := commonDefaults(&cfg); err != nil {
		return nil, err
	}
	if cfg.Memory.MaxGroups == 0 {
		cfg.Memory.MaxGroups = 128
	}
	if cfg.Memory.MaxBytes == 0 {
		cfg.Memory.MaxBytes = 1 << 30
	}
	if cfg.Memory.MaxGroups < 1 || cfg.Memory.MaxGroups > 1<<20 || cfg.Memory.MaxBytes < int64(cfg.Limits.GroupBytes) {
		return nil, errors.New("invalid SIP memory capacity")
	}
	m := &MemoryStore{metadata: newMetadata(cfg), gate: make(chan struct{}, 1), clock: clock,
		groups: make(map[string]*memoryGroup), leases: make(map[string]*memoryLease), schedules: make([]memorySchedule, cfg.Partitions)}
	// Even explicitly identical configuration never joins independent local stores.
	m.authority.RunID = uuid.NewString()
	m.available.Store(true)
	return m, nil
}

func (m *MemoryStore) lock(ctx context.Context) (int64, error) {
	select {
	case m.gate <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		m.unlock()
		return 0, err
	}
	if m.closed {
		m.unlock()
		return 0, ErrAuthority
	}
	now := m.clock()
	previous := m.lastClock
	m.lastClock = now
	if now.UnixMilli() <= 0 || now.UnixMilli() > 1<<53-1-applicationLeaseIdleTTL.Milliseconds() ||
		!previous.IsZero() && verifyClock(previous, now) != nil {
		m.available.Store(false)
		m.unlock()
		return 0, ErrClock
	}
	m.available.Store(true)
	return now.UnixMilli(), nil
}
func (m *MemoryStore) unlock()         { <-m.gate }
func (m *MemoryStore) Available() bool { return m.available.Load() }
func (m *MemoryStore) Ready(ctx context.Context) error {
	_, err := m.lock(ctx)
	if err == nil {
		m.unlock()
	}
	return err
}
func (m *MemoryStore) Close() error {
	m.gate <- struct{}{}
	defer m.unlock()
	m.closed = true
	m.available.Store(false)
	m.groups = nil
	m.leases = nil
	m.applicationContexts = nil
	m.schedules = nil
	m.leaseOrder.Init()
	return nil
}

func (m *MemoryStore) group(ref Ref) (*memoryGroup, error) {
	if err := m.validateRef(ref); err != nil {
		return nil, err
	}
	g := m.groups[ref.ID]
	if g == nil {
		return nil, ErrNotFound
	}
	if ref.CallID != "" && ref.CallID != g.ref.CallID {
		return nil, ErrConflict
	}
	if ref.Incarnation != "" && ref.Incarnation != g.ref.Incarnation {
		return nil, ErrStale
	}
	return g, nil
}

func copyReceipt(r Receipt) Receipt { r.Outcome = append([]byte(nil), r.Outcome...); return r }
func (m *MemoryStore) record(g *memoryGroup, now int64, inputIDs []string) (Record, error) {
	r := Record{Ref: g.ref, Authority: m.authority, Versions: m.cfg.Versions, Revision: g.revision}
	if err := r.Payload.UnmarshalJSON(g.data); err != nil {
		return Record{}, ErrCorrupt
	}
	for _, receipt := range g.receipts {
		r.Receipts = append(r.Receipts, copyReceipt(receipt))
	}
	if g.proof != nil {
		proof := copyReceipt(*g.proof)
		r.Proof = &proof
	}
	r.ApplicationLeases = make(map[string]ApplicationLease)
	for _, id := range append(append([]string(nil), r.Payload.ApplicationLeaseIDs...), inputIDs...) {
		if _, ok := r.ApplicationLeases[id]; !ok {
			if len(r.ApplicationLeases) >= applicationLeaseLimit {
				return Record{}, ErrCapacity
			}
			r.ApplicationLeases[id] = m.observeLease(id, now)
		}
	}
	return r, nil
}
func (m *MemoryStore) LoadAt(ctx context.Context, ref Ref) (Record, int64, error) {
	now, err := m.lock(ctx)
	if err != nil {
		return Record{}, 0, err
	}
	defer m.unlock()
	g, err := m.group(ref)
	if err != nil {
		return Record{}, 0, err
	}
	r, err := m.record(g, now, nil)
	return r, now, err
}
func (m *MemoryStore) Load(ctx context.Context, ref Ref) (Record, error) {
	r, _, err := m.LoadAt(ctx, ref)
	return r, err
}

func retainedReceipt(r Record, id, fingerprint string, now int64) (*Receipt, error) {
	receipts := r.Receipts
	if r.Proof != nil {
		receipts = append(receipts, *r.Proof)
	}
	for _, receipt := range receipts {
		if receipt.OperationID == id && receipt.ExpiresMS > now {
			if receipt.Fingerprint != fingerprint {
				return nil, ErrConflict
			}
			result := copyReceipt(receipt)
			return &result, nil
		}
	}
	return nil, nil
}

func (m *MemoryStore) ReadUpdate(ctx context.Context, ref Ref, input Input) (UpdateResult, error) {
	if err := m.validateRef(ref); err != nil {
		return UpdateResult{}, err
	}
	if err := m.validateInput(input); err != nil {
		return UpdateResult{}, err
	}
	digest, encoded, err := digestInput(input)
	if err != nil {
		return UpdateResult{}, err
	}
	// Own the immutable input, including payload and lease-ID slices.
	var owned Input
	if err := json.Unmarshal(encoded, &owned); err != nil {
		return UpdateResult{}, err
	}
	input = owned
	now, err := m.lock(ctx)
	if err != nil {
		return UpdateResult{}, err
	}
	defer m.unlock()
	g, err := m.group(ref)
	existing := err == nil
	var r Record
	if errors.Is(err, ErrNotFound) {
		if !input.Create || ref.Incarnation != "" || ref.CallID == "" {
			return UpdateResult{}, ErrNotFound
		}
		ref.Incarnation = uuid.NewString()
		r = Record{Ref: ref, Authority: m.authority, Versions: m.cfg.Versions, Revision: "0", ApplicationLeases: make(map[string]ApplicationLease)}
		for _, id := range input.ApplicationLeaseIDs {
			r.ApplicationLeases[id] = m.observeLease(id, now)
		}
	} else if err != nil {
		return UpdateResult{}, err
	} else {
		r, err = m.record(g, now, input.ApplicationLeaseIDs)
		if err != nil {
			return UpdateResult{}, err
		}
	}
	receipt, err := retainedReceipt(r, input.ID, input.Fingerprint, now)
	if err != nil {
		return UpdateResult{}, err
	}
	if receipt != nil {
		return UpdateResult{Receipt: receipt}, nil
	}
	if input.DeadlineMS <= now {
		return UpdateResult{}, ErrExpired
	}
	token := &memoryUpdate{store: m, ref: r.Ref, revision: r.Revision, digest: digest, priorEffects: snapshotApplicationEffects(r.Payload.Effects)}
	result := UpdateResult{Update: &Update{Record: r, Input: input, NowMS: now, token: token}}
	if existing {
		observed := r.Ref
		result.Ref = &observed
	}
	return result, nil
}

func (m *MemoryStore) CompareAndSwap(ctx context.Context, update Update, next Payload, outcome []byte) (Receipt, error) {
	token, ok := update.token.(*memoryUpdate)
	if !ok || token.store != m {
		return Receipt{}, ErrStale
	}
	if update.Record.Ref != token.ref || update.Record.Revision != token.revision || update.Record.Authority != m.authority {
		return Receipt{}, ErrConflict
	}
	digest, _, err := digestInput(update.Input)
	if err != nil || digest != token.digest {
		return Receipt{}, ErrConflict
	}
	if err := m.validatePayloadStructure(next); err != nil {
		return Receipt{}, err
	}
	guards, err := commitApplicationLeaseGuardsForEffects(token.priorEffects, next)
	if err != nil {
		return Receipt{}, err
	}
	if len(next.CommitLeaseFences) > applicationLeaseLimit {
		return Receipt{}, ErrCapacity
	}
	for _, fence := range next.CommitLeaseFences {
		if !validApplicationLeaseRef(fence) {
			return Receipt{}, ErrCorrupt
		}
	}
	data, err := marshalCommitPayload(next)
	if err != nil {
		return Receipt{}, err
	}
	if err := m.validatePayloadSize(next, len(data)); err != nil {
		return Receipt{}, err
	}
	if len(outcome) > m.cfg.Limits.OutcomeBytes {
		return Receipt{}, ErrCapacity
	}
	revision, err := strconv.ParseUint(token.revision, 10, 63)
	if err != nil || revision == 1<<63-1 {
		return Receipt{}, ErrCapacity
	}
	receipt := Receipt{OperationID: update.Input.ID, Fingerprint: update.Input.Fingerprint, InputDigest: digest,
		Revision: strconv.FormatUint(revision+1, 10), ExpiresMS: update.Input.ReceiptUntilMS, Outcome: append([]byte(nil), outcome...)}
	now, err := m.lock(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer m.unlock()
	if update.Input.DeadlineMS <= now {
		return Receipt{}, ErrExpired
	}
	g, err := m.group(token.ref)
	if errors.Is(err, ErrNotFound) {
		if !update.Input.Create || token.ref.CallID == "" || token.revision != "0" {
			return Receipt{}, ErrStale
		}
		if len(m.groups) >= m.cfg.Memory.MaxGroups || int64(len(m.groups)+1) > m.cfg.Memory.MaxBytes/int64(m.cfg.Limits.GroupBytes) {
			return Receipt{}, ErrCapacity
		}
		g = &memoryGroup{ref: token.ref}
	} else if err != nil {
		return Receipt{}, err
	} else if g.revision != token.revision {
		return Receipt{}, ErrStale
	}
	for _, guard := range guards {
		lease := m.liveLease(guard.ID, now)
		if lease == nil || lease.Term != guard.Term || lease.Generation != guard.Generation || lease.ExpiresAtMS <= now || lease.sealed || lease.AttachmentUntilMS > 0 {
			return Receipt{}, ErrStale
		}
	}
	var before Payload
	if len(g.data) != 0 && before.UnmarshalJSON(g.data) != nil {
		return Receipt{}, ErrCorrupt
	}
	changes, err := m.planLeases(before.ApplicationLeaseIDs, next.ApplicationLeaseIDs, next.CommitLeaseFences, now)
	if err != nil {
		return Receipt{}, err
	}
	contextChanges, err := m.planApplicationContexts(g.ref, before.ApplicationContexts, next.ApplicationContexts)
	if err != nil {
		return Receipt{}, err
	}
	var receipts []Receipt
	for _, r := range g.receipts {
		if r.ExpiresMS > now {
			receipts = append(receipts, r)
		}
	}
	proof := g.proof
	if update.Input.Proof {
		if update.Input.Kind == "command" {
			saved := receipt
			proof = &saved
		}
	} else if receipt.ExpiresMS > now {
		receipts = append(receipts, receipt)
	}
	if len(receipts) > m.cfg.Limits.Receipts {
		return Receipt{}, ErrCapacity
	}
	encodedReceipts, _ := json.Marshal(receipts)
	if len(receipts) == 0 {
		encodedReceipts = []byte("[]")
	}
	projection, _ := json.Marshal(next.ApplicationLeaseIDs)
	projectionBytes := 0
	if len(next.ApplicationLeaseIDs) > 0 {
		projectionBytes = len(projection)
	}
	contexts, err := applicationContextProjections(g.ref, next.ApplicationContexts)
	if err != nil {
		return Receipt{}, err
	}
	if len(contexts) > 0 {
		encodedContexts, _ := json.Marshal(contexts)
		projectionBytes += len(encodedContexts)
	}
	outcomeReserve := base64.StdEncoding.EncodedLen(m.cfg.Limits.OutcomeBytes) + 1024
	if int64(len(data))+int64(len(encodedReceipts))+4096+int64(next.ReservedCompletionBytes)+int64(outcomeReserve)+int64(projectionBytes) > int64(m.cfg.Limits.GroupBytes) {
		return Receipt{}, ErrCapacity
	}
	ready, due, retire := scheduleTimes(next, update.NowMS, receipts, proof)
	// Cancellation before mutation is definite no-commit. After this point all
	// related changes commit together and the caller receives the exact receipt.
	if err := ctx.Err(); err != nil {
		return Receipt{}, err
	}
	for id, change := range changes {
		lease := m.leases[id]
		if lease == nil {
			continue
		}
		lease.references = change.references
		lease.sealed = lease.sealed || change.seal
		if lease.references > 0 {
			lease.idleUntil = 0
		} else {
			lease.idleUntil = now + applicationLeaseIdleTTL.Milliseconds()
		}
	}
	m.applyApplicationContexts(contextChanges)
	g.data, g.revision, g.receipts, g.proof = data, receipt.Revision, receipts, proof
	g.ready, g.due, g.retire = ready, due, retire
	m.groups[g.ref.ID] = g
	m.schedule(g)
	return copyReceipt(receipt), nil
}

func scheduleTimes(next Payload, now int64, receipts []Receipt, proof *Receipt) (ready, due, retire int64) {
	for _, effect := range next.Effects {
		if effect.Status == EffectPending || effect.Status == EffectAuthorized {
			ready = max(now, 1)
			break
		}
	}
	for _, timer := range next.Timers {
		if due == 0 || timer.DueMS < due {
			due = timer.DueMS
		}
	}
	if next.Retention.Terminal && next.ActiveTransactions == 0 && len(next.Effects) == 0 && len(next.Timers) == 0 {
		retire = next.Retention.UntilMS
		if retire == 0 {
			retire = now
		}
		for _, receipt := range receipts {
			retire = max(retire, receipt.ExpiresMS)
		}
		if proof != nil {
			retire = max(retire, proof.ExpiresMS)
		}
		due = retire
	}
	return
}

func (m *MemoryStore) InspectOperation(ctx context.Context, ref Ref, id, fingerprint string) (Inspection, error) {
	if !referencePattern.MatchString(id) || !validText(fingerprint, 256) {
		return Inspection{}, ErrConflict
	}
	r, now, err := m.LoadAt(ctx, ref)
	if err != nil {
		return Inspection{}, err
	}
	receipt, err := retainedReceipt(r, id, fingerprint, now)
	return Inspection{Receipt: receipt, Revision: r.Revision, NowMS: now}, err
}
func (m *MemoryStore) Retire(ctx context.Context, ref Ref) error {
	if ref.Incarnation == "" {
		return ErrConflict
	}
	now, err := m.lock(ctx)
	if err != nil {
		return err
	}
	defer m.unlock()
	g, err := m.group(ref)
	if err != nil {
		return err
	}
	if g.retire <= 0 || g.retire > now {
		return ErrNotReady
	}
	var payload Payload
	if payload.UnmarshalJSON(g.data) != nil {
		return ErrCorrupt
	}
	changes, err := m.planLeases(payload.ApplicationLeaseIDs, nil, nil, now)
	if err != nil {
		return err
	}
	contextChanges, err := m.planApplicationContexts(g.ref, payload.ApplicationContexts, nil)
	if err != nil {
		return err
	}
	for id, change := range changes {
		if lease := m.leases[id]; lease != nil {
			lease.references = change.references
			lease.idleUntil = now + applicationLeaseIdleTTL.Milliseconds()
		}
	}
	m.applyApplicationContexts(contextChanges)
	delete(m.groups, ref.ID)
	m.schedules[ref.Partition].ready.set(ref, 0)
	m.schedules[ref.Partition].due.set(ref, 0)
	return nil
}

func (m *MemoryStore) liveLease(id string, now int64) *memoryLease {
	lease := m.leases[id]
	if lease != nil && lease.references == 0 && lease.idleUntil <= now {
		delete(m.leases, id)
		m.leaseOrder.Remove(lease.element)
		return nil
	}
	return lease
}
func (m *MemoryStore) observeLease(id string, now int64) ApplicationLease {
	lease := m.liveLease(id, now)
	if lease == nil {
		return ApplicationLease{ID: id, CheckedAtMS: now}
	}
	if !lease.sealed && lease.ExpiresAtMS <= now && lease.RecoveryUntilMS == 0 {
		if lease.Recoverable {
			lease.RecoveryUntilMS = lease.ExpiresAtMS + lease.ReattachTimeoutMS
			if lease.AttachmentUntilMS > 0 {
				lease.RecoveryUntilMS = lease.AttachmentUntilMS
			}
		} else {
			lease.RecoveryUntilMS = now + ApplicationLeaseRecoveryGrace.Milliseconds()
		}
	}
	result := lease.ApplicationLease
	result.CheckedAtMS = now
	result.Sealed = lease.sealed
	return result
}

type memoryLeaseChange struct {
	references int64
	seal       bool
}

func (m *MemoryStore) planLeases(before, after []string, fences []ApplicationLeaseRef, now int64) (map[string]memoryLeaseChange, error) {
	delta := make(map[string]int64)
	for _, id := range before {
		delta[id]--
	}
	for _, id := range after {
		delta[id]++
	}
	changes := make(map[string]memoryLeaseChange)
	for id, d := range delta {
		if d == 0 {
			continue
		}
		lease := m.liveLease(id, now)
		if lease == nil {
			if d > 0 {
				return nil, ErrCorrupt
			}
			continue
		}
		refs := lease.references + d
		if refs < 0 || refs > 1<<53-1 {
			return nil, ErrCorrupt
		}
		changes[id] = memoryLeaseChange{references: refs}
	}
	for _, fence := range fences {
		lease := m.liveLease(fence.ID, now)
		if lease == nil || lease.Term != fence.Term {
			continue
		}
		if lease.Generation != fence.Generation {
			return nil, ErrStale
		}
		if !lease.sealed && (lease.ExpiresAtMS > now || lease.RecoveryUntilMS == 0 || lease.RecoveryUntilMS > now) {
			return nil, ErrStale
		}
		change, ok := changes[fence.ID]
		if !ok {
			change.references = lease.references
		}
		change.seal = true
		changes[fence.ID] = change
	}
	return changes, nil
}
func (m *MemoryStore) applicationLease(ctx context.Context, id, term, mode string) (ApplicationLease, error) {
	if !referencePattern.MatchString(id) {
		return ApplicationLease{}, errors.New("invalid SIP application lease identity")
	}
	if mode != "read" {
		parsed, err := uuid.Parse(term)
		if err != nil || parsed == uuid.Nil || parsed.String() != term {
			return ApplicationLease{}, errors.New("invalid SIP application lease term")
		}
	}
	now, err := m.lock(ctx)
	if err != nil {
		return ApplicationLease{}, err
	}
	defer m.unlock()
	lease := m.liveLease(id, now)
	if mode == "read" {
		return m.observeLease(id, now), nil
	}
	if lease != nil && lease.Recoverable {
		return ApplicationLease{}, ErrStale
	}
	switch mode {
	case "renew", "resume":
		if lease == nil || lease.sealed || mode == "renew" && lease.ExpiresAtMS <= now {
			return ApplicationLease{}, ErrExpired
		}
		if lease.Term != term {
			return ApplicationLease{}, ErrStale
		}
	case "acquire":
		if lease != nil && !lease.sealed && lease.ExpiresAtMS > now {
			term = lease.Term
		} else if lease != nil && lease.Term == term {
			return ApplicationLease{}, ErrConflict
		}
	default:
		return ApplicationLease{}, ErrCorrupt
	}
	// Incremental, fair pruning: no map-wide scan and no pinned-lease starvation.
	for n, count := 0, min(applicationLeasePrune, m.leaseOrder.Len()); n < count; n++ {
		element := m.leaseOrder.Front()
		candidate := element.Value.(string)
		m.leaseOrder.MoveToBack(element)
		saved := m.liveLease(candidate, now)
		if saved != nil && saved.references == 0 && saved.ExpiresAtMS <= now && (!saved.Recoverable || saved.ExpiresAtMS+saved.ReattachTimeoutMS <= now) {
			delete(m.leases, candidate)
			m.leaseOrder.Remove(element)
		}
	}
	// Pruning may have removed this expired unreferenced lease; preserve the
	// validated operation's exact term/reference semantics when replacing it.
	if m.leases[id] == nil && len(m.leases) >= applicationLeaseLimit {
		return ApplicationLease{}, ErrCapacity
	}
	expires := now + ApplicationLeaseLifetime.Milliseconds()
	if lease != nil && lease.Term == term {
		expires = max(expires, lease.ExpiresAtMS)
	}
	refs := int64(0)
	if lease != nil {
		refs = lease.references
	}
	element := (*list.Element)(nil)
	if current := m.leases[id]; current != nil {
		element = current.element
	} else {
		element = m.leaseOrder.PushBack(id)
	}
	lease = &memoryLease{ApplicationLease: ApplicationLease{ID: id, Term: term, ExpiresAtMS: expires, CheckedAtMS: now}, references: refs, element: element}
	if refs == 0 {
		lease.idleUntil = now + applicationLeaseIdleTTL.Milliseconds()
	}
	m.leases[id] = lease
	return lease.ApplicationLease, nil
}
func (m *MemoryStore) AcquireApplicationLease(ctx context.Context, id, term string) (ApplicationLease, error) {
	return m.applicationLease(ctx, id, term, "acquire")
}
func (m *MemoryStore) RenewApplicationLease(ctx context.Context, id, term string) (ApplicationLease, error) {
	return m.applicationLease(ctx, id, term, "renew")
}
func (m *MemoryStore) ResumeApplicationLease(ctx context.Context, id, term string) (ApplicationLease, error) {
	return m.applicationLease(ctx, id, term, "resume")
}
func (m *MemoryStore) ReadApplicationLease(ctx context.Context, id string) (ApplicationLease, error) {
	return m.applicationLease(ctx, id, "", "read")
}

// Each group has at most one entry in each index; repeated commits cannot grow
// stale heap entries. Index mutation is in the same critical section as state.
type memoryHint struct {
	ref   Ref
	due   int64
	index int
}
type hintHeap struct {
	items []*memoryHint
	byID  map[string]*memoryHint
}

func (h hintHeap) Len() int { return len(h.items) }
func (h hintHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	return a.due < b.due || a.due == b.due && a.ref.ID < b.ref.ID
}
func (h hintHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *hintHeap) Push(v any) {
	item := v.(*memoryHint)
	item.index = len(h.items)
	h.items = append(h.items, item)
}
func (h *hintHeap) Pop() any {
	last := len(h.items) - 1
	item := h.items[last]
	h.items[last] = nil
	h.items = h.items[:last]
	return item
}
func (h *hintHeap) set(ref Ref, due int64) {
	item := h.byID[ref.ID]
	if due <= 0 {
		if item != nil {
			heap.Remove(h, item.index)
			delete(h.byID, ref.ID)
		}
		return
	}
	if item != nil {
		item.ref, item.due = ref, due
		heap.Fix(h, item.index)
		return
	}
	if h.byID == nil {
		h.byID = make(map[string]*memoryHint)
	}
	item = &memoryHint{ref: ref, due: due}
	h.byID[ref.ID] = item
	heap.Push(h, item)
}
func (h *hintHeap) hints(now int64, limit int) []Hint {
	var selected []*memoryHint
	for h.Len() > 0 && len(selected) < limit && h.items[0].due <= now {
		selected = append(selected, heap.Pop(h).(*memoryHint))
	}
	var result []Hint
	for _, item := range selected {
		result = append(result, Hint{Ref: item.ref, DueMS: item.due})
		heap.Push(h, item)
	}
	return result
}

type memorySchedule struct{ ready, due hintHeap }

func (m *MemoryStore) schedule(g *memoryGroup) {
	index := &m.schedules[g.ref.Partition]
	index.ready.set(g.ref, g.ready)
	index.due.set(g.ref, g.due)
}
func (m *MemoryStore) ScheduleHints(ctx context.Context, first uint16, count, limit int) ([]HintBatch, error) {
	if first >= m.cfg.Partitions || count < 1 || count > min(8, int(m.cfg.Partitions)) || limit < 1 || limit > 256 {
		return nil, errors.New("invalid scheduling batch")
	}
	now, err := m.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer m.unlock()
	batches := make([]HintBatch, count)
	for n := range batches {
		index := &m.schedules[(int(first)+n)%int(m.cfg.Partitions)]
		batches[n] = HintBatch{Ready: index.ready.hints(now, limit), Due: index.due.hints(now, limit)}
	}
	return batches, nil
}
func (m *MemoryStore) RepairSchedule(ctx context.Context, cursor uint64, count int) (uint64, int, error) {
	if count < 1 || count > 256 {
		return cursor, 0, errors.New("invalid repair batch")
	}
	return 0, 0, m.Ready(ctx)
}
