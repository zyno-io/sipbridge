package state

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var referencePattern = regexp.MustCompile(`^[A-Za-z0-9_.:@+\-]{1,96}$`)
var workerPattern = regexp.MustCompile(`^[A-Za-z0-9_.:@+\-]{1,192}$`)
var prefixPattern = regexp.MustCompile(`^[A-Za-z0-9_.:\-]{1,96}$`)
var runIDPattern = regexp.MustCompile(`(?m)^run_id:([a-f0-9]{40})\r?$`)
var sentinelModePattern = regexp.MustCompile(`(?m)^redis_mode:sentinel\r?$`)
var masterPattern = regexp.MustCompile(`(?m)^role:master\r?$`)

// Store contains configuration and connection resources only. It does not cache
// protocol records. Empty Redis state needs no external initialization.
type Store struct {
	metadata
	client     *redis.Client
	ready      atomic.Bool
	transport  atomic.Pointer[primaryObservation]
	validated  atomic.Pointer[primaryObservation]
	validation singleflight.Group
}

// OpenDeferred creates connection resources without opening Redis. Logical
// authority is configuration-derived; public listeners can fail unavailable
// while Redis starts. New protocol records are allocated only by fresh ingress.
func OpenDeferred(cfg Config) (*Store, error) {
	if err := defaults(&cfg); err != nil {
		return nil, err
	}
	s := &Store{metadata: newMetadata(cfg)}
	onConnect := func(ctx context.Context, conn *redis.Conn) error { return s.observeConnection(ctx, conn) }

	if len(cfg.Redis.SentinelAddresses) > 0 {
		s.client = redis.NewFailoverClient(&redis.FailoverOptions{MasterName: cfg.Redis.SentinelMaster,
			SentinelAddrs: cfg.Redis.SentinelAddresses, SentinelUsername: cfg.Redis.SentinelUsername,
			SentinelPassword: cfg.Redis.SentinelPassword, Username: cfg.Redis.Username, Password: cfg.Redis.Password,
			TLSConfig: cfg.Redis.TLS, DialTimeout: cfg.Redis.DialTimeout, ReadTimeout: cfg.Redis.IOTimeout,
			WriteTimeout: cfg.Redis.IOTimeout, PoolSize: cfg.Redis.PoolSize, MaxRetries: -1,
			ContextTimeoutEnabled: true, OnConnect: onConnect})
	} else {
		s.client = redis.NewClient(&redis.Options{Addr: cfg.Redis.Address, Username: cfg.Redis.Username,
			Password: cfg.Redis.Password, TLSConfig: cfg.Redis.TLS, DialTimeout: cfg.Redis.DialTimeout,
			ReadTimeout: cfg.Redis.IOTimeout, WriteTimeout: cfg.Redis.IOTimeout, PoolSize: cfg.Redis.PoolSize,
			MaxRetries: -1, ContextTimeoutEnabled: true, OnConnect: onConnect})
	}
	return s, nil
}

// Open verifies a current writable Redis primary. Empty state is ready.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	s, err := OpenDeferred(cfg)
	if err != nil {
		return nil, err
	}
	if err = s.Ready(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func commonDefaults(cfg *Config) error {
	if cfg.Generation == "" {
		cfg.Generation = "default"
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "sipstate:v2"
	}
	if cfg.Partitions == 0 {
		cfg.Partitions = 64
	}
	if cfg.ClaimTTL == 0 {
		cfg.ClaimTTL = time.Second
	}
	if cfg.Limits.GroupBytes == 0 {
		cfg.Limits.GroupBytes = 64 << 20
	}
	if cfg.Limits.InputBytes == 0 {
		cfg.Limits.InputBytes = 1 << 20
	}
	if cfg.Limits.OutcomeBytes == 0 {
		cfg.Limits.OutcomeBytes = 1 << 20
	}
	if cfg.Limits.Effects == 0 {
		cfg.Limits.Effects = 4096 // 32 regular procedures plus paired CANCEL and directional fork cleanup.
	}
	if cfg.Limits.Timers == 0 {
		cfg.Limits.Timers = 4096 // Effects plus four TX and two dialog timers per procedure.
	}
	if cfg.Limits.Receipts == 0 {
		cfg.Limits.Receipts = 128
	}
	if cfg.Limits.Transactions == 0 {
		cfg.Limits.Transactions = 128 // 32 regular + 32 paired CANCEL + 2*16 directional BYE, rounded up.
	}
	if !prefixPattern.MatchString(cfg.Prefix) || !referencePattern.MatchString(cfg.Namespace) ||
		!referencePattern.MatchString(cfg.Generation) || len(cfg.NamespaceKey) < 32 || cfg.Partitions > 4096 ||
		!referencePattern.MatchString(cfg.Versions.Schema) || !referencePattern.MatchString(cfg.Versions.Reducer) ||
		!referencePattern.MatchString(cfg.Versions.Timers) || cfg.ClaimTTL < time.Millisecond || cfg.ClaimTTL > 5*time.Second {
		return errors.New("invalid SIP state configuration")
	}
	cfg.NamespaceKey = append([]byte(nil), cfg.NamespaceKey...)
	l := cfg.Limits
	if l.GroupBytes < 8192 || l.GroupBytes > 64<<20 || l.InputBytes < 1 || l.InputBytes > l.GroupBytes ||
		l.OutcomeBytes < 1 || l.OutcomeBytes > l.GroupBytes || l.Effects < 1 || l.Effects > 4096 ||
		l.Timers < 1 || l.Timers > 4096 || l.Receipts < 1 || l.Receipts > 4096 || l.Transactions < 1 || l.Transactions > 4096 {
		return errors.New("invalid SIP state bounds")
	}
	return nil
}

// Available is only a readiness hint; every operation still validates Redis.
func (s *Store) Available() bool {
	observation := s.transport.Load()
	return s.ready.Load() && observation != nil && s.validated.Load() == observation
}

func (s *Store) Close() error { return s.client.Close() }

func (s *Store) partitionPrefix(p uint16) string { return fmt.Sprintf("%s{p%04d}:", s.root, p) }
func (s *Store) keys(ref Ref) []string {
	p := s.partitionPrefix(ref.Partition)
	return []string{p + "group:" + ref.ID, p + "ready", p + "due"}
}

// Ready checks current primary connectivity. Each script validates its own record.
func (s *Store) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The probe has its own finite lifetime: a 100ms scheduler caller cannot
	// cancel a shared connectivity check needed by later control admission.
	result := s.validation.DoChan("ready", func() (interface{}, error) {
		probe, cancel := context.WithTimeout(context.Background(), s.cfg.Redis.IOTimeout)
		defer cancel()
		return nil, s.checkReady(probe)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case checked := <-result:
		if err := ctx.Err(); err != nil {
			return err
		}
		return checked.Err
	}
}

// ensurePrimary observes transport identity, never namespace initialization.
func (s *Store) ensurePrimary(ctx context.Context) (*primaryObservation, error) {
	for {
		observation := s.transport.Load()
		if observation != nil && s.validated.Load() == observation {
			return observation, nil
		}
		if err := s.Ready(ctx); err != nil {
			return nil, err
		}
	}
}

func (s *Store) checkReady(ctx context.Context) (result error) {
	observation := s.transport.Load()
	defer func() {
		if result != nil {
			s.ready.Store(false)
			if errors.Is(result, ErrAuthority) {
				s.validated.CompareAndSwap(observation, nil)
			}
		}
	}()
	start := time.Now()
	clockCtx, cancel := context.WithTimeout(ctx, s.cfg.Redis.IOTimeout)
	redisTime, err := s.client.Time(clockCtx).Result()
	finish := time.Now()
	cancel()
	if err != nil {
		return err
	}
	if err = verifyClock(start, finish, redisTime.UnixMilli()); err != nil {
		return err
	}
	observation = s.transport.Load()
	info, err := s.client.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	match := runIDPattern.FindStringSubmatch(info)
	if observation == nil || len(match) != 2 || observation.runID != match[1] {
		return ErrAuthority
	}
	role, err := s.client.Info(ctx, "replication").Result()
	if err != nil {
		return err
	}
	if !masterPattern.MatchString(role) {
		return ErrAuthority
	}
	if observation != s.transport.Load() {
		return ErrStale
	}
	s.validated.Store(observation)
	s.ready.Store(true)
	return nil
}

func digestInput(input Input) (string, []byte, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), data, nil
}
func validText(value string, max int) bool {
	return value != "" && len(value) <= max && !strings.ContainsAny(value, "\r\n\x00")
}

func (s *Store) baseArgs(ref Ref, observed *primaryObservation) []interface{} {
	return []interface{}{s.authority.identity(), observed.runID, s.cfg.Namespace, ref.CallID, s.versions, ref.Incarnation}
}

func (s *Store) run(ctx context.Context, script *redis.Script, ref Ref, extra []interface{}, mutation bool, expected ...*primaryObservation) ([]interface{}, error) {
	return s.runKeys(ctx, script, ref, s.keys(ref), extra, mutation, expected...)
}

func (s *Store) runKeys(ctx context.Context, script *redis.Script, ref Ref, keys []string, extra []interface{}, mutation bool, expected ...*primaryObservation) ([]interface{}, error) {
	observation, err := s.ensurePrimary(ctx)
	if err != nil {
		return nil, err
	}
	if len(expected) > 0 && expected[0] != observation {
		return nil, ErrStale
	}
	start := time.Now()
	low, high := clockBoundsAt(start)
	args := append(s.baseArgs(ref, observation), extra...)
	args = append(args, s.partitionPrefix(0)+"application-lease:")
	deadline := start.Add(s.cfg.Redis.IOTimeout)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	// This envelope only bounds Redis execution latency. Every native input,
	// claim and SIP emission retains its separate original absolute deadline.
	window := max(int64(0), deadline.Sub(start).Milliseconds())
	high += window
	args = append(args, strconv.FormatInt(low, 10), strconv.FormatInt(high, 10))
	ioCtx, cancel := context.WithDeadline(ctx, deadline)
	value, err := script.Run(ioCtx, s.client, keys, args...).Result()
	finish := time.Now()
	cancel()
	if observation != s.transport.Load() {
		if mutation {
			return nil, errors.Join(ErrUnknown, ErrStale)
		}
		return nil, ErrStale
	}
	if err != nil {
		// An exhausted caller budget is not proof that Redis is unavailable.
		// Keep the complete same-primary proof even on a genuine I/O outage;
		// a later script can reestablish health through its own atomic guards.
		s.noteIOFailure(ctx, finish, observation, err)
		if mutation {
			return nil, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		return nil, fmt.Errorf("SIP state read: %w", err)
	}
	observed, ok := value.([]interface{})
	if !ok || len(observed) != 4 || observed[0] != "SIPSTATE-CLOCK-V1" {
		return nil, ErrCorrupt
	}
	startedMS, startedErr := millisAt(observed, 1)
	finishedMS, finishedErr := millisAt(observed, 2)
	if startedErr != nil || finishedErr != nil {
		return nil, ErrCorrupt
	}
	if clockErr := verifyClock(start, finish, startedMS, finishedMS); clockErr != nil {
		s.ready.Store(false)
		if mutation {
			return nil, errors.Join(ErrUnknown, clockErr)
		}
		return nil, clockErr
	}
	values, ok := observed[3].([]interface{})
	if !ok || len(values) == 0 {
		return nil, ErrCorrupt
	}
	code, ok := values[0].(string)
	if !ok {
		return nil, ErrCorrupt
	}
	if code == "ERROR" {
		if len(values) != 2 {
			return nil, ErrCorrupt
		}
		err := storeError(fmt.Sprint(values[1]))
		if errors.Is(err, ErrClock) {
			// A delayed command may execute after its original I/O envelope.
			// The returned execution timestamps prove healthy clocks; this is
			// a transient no-result window, not a proven clock discontinuity.
			if mutation {
				return nil, fmt.Errorf("%w: Redis execution exceeded original clock window", ErrUnknown)
			}
			return nil, fmt.Errorf("Redis execution window: %w", context.DeadlineExceeded)
		}
		if errors.Is(err, ErrAuthority) {
			s.validated.CompareAndSwap(observation, nil)
			s.ready.Store(false)
		} else if s.validated.Load() == observation && s.transport.Load() == observation {
			s.ready.Store(true) // A semantic refusal still proves a healthy data path.
		}
		return nil, err
	}
	if s.validated.Load() == observation && s.transport.Load() == observation {
		s.ready.Store(true)
	}
	return values, nil
}

func (s *Store) noteIOFailure(ctx context.Context, finish time.Time, observation *primaryObservation, err error) {
	if errors.Is(err, ErrAuthority) {
		s.validated.CompareAndSwap(observation, nil)
		s.ready.Store(false)
		return
	}
	if ctx.Err() != nil {
		return
	}
	if deadline, ok := ctx.Deadline(); ok && !finish.Before(deadline) {
		return
	}
	s.ready.Store(false)
}

func storeError(code string) error {
	switch code {
	case "CLOCK":
		return ErrClock
	case "AUTHORITY":
		return ErrAuthority
	case "NOT_FOUND":
		return ErrNotFound
	case "BUSY":
		return ErrBusy
	case "IDLE":
		return ErrIdle
	case "STALE":
		return ErrStale
	case "CONFLICT":
		return ErrConflict
	case "EXPIRED":
		return ErrExpired
	case "CAPACITY":
		return ErrCapacity
	case "VERSION":
		return ErrVersion
	case "NOT_READY":
		return ErrNotReady
	default:
		return ErrCorrupt
	}
}
func textAt(values []interface{}, index int) (string, error) {
	if index >= len(values) {
		return "", ErrCorrupt
	}
	value, ok := values[index].(string)
	if !ok {
		return "", ErrCorrupt
	}
	return value, nil
}
func millisAt(values []interface{}, index int) (int64, error) {
	value, err := textAt(values, index)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 || n > 1<<53-1 {
		return 0, ErrCorrupt
	}
	return n, nil
}

func (s *Store) read(ctx context.Context, ref Ref) (Record, int64, error) {
	if err := s.validateRef(ref); err != nil {
		return Record{}, 0, err
	}
	values, err := s.run(ctx, readScript, ref, nil, false)
	if err != nil {
		return Record{}, 0, err
	}
	if len(values) != 10 || values[0] != "RECORD" {
		return Record{}, 0, ErrCorrupt
	}
	now, err := millisAt(values, 1)
	if err != nil {
		return Record{}, 0, err
	}
	fields := make([]string, 7)
	for i := range fields {
		fields[i], err = textAt(values, i+2)
		if err != nil {
			return Record{}, 0, err
		}
	}
	record, err := s.decodeRecord(ref, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6])
	if err != nil {
		return Record{}, 0, err
	}
	leases, err := textAt(values, 9)
	if err == nil {
		var inputIDs []string
		if record.Pending != nil {
			inputIDs = record.Pending.ApplicationLeaseIDs
		}
		err = decodeApplicationLeases(&record, leases, now, inputIDs)
	}
	return record, now, err
}

func (s *Store) Load(ctx context.Context, ref Ref) (Record, error) {
	record, _, err := s.read(ctx, ref)
	return record, err
}

// LoadAt returns the Redis time observed by the same validated record read.
func (s *Store) LoadAt(ctx context.Context, ref Ref) (Record, int64, error) {
	return s.read(ctx, ref)
}

func decimal(value string) bool {
	if value == "" || len(value) > 19 || len(value) > 1 && value[0] == '0' {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 63)
	return err == nil
}

func (s *Store) commit(ctx context.Context, step Step, next Payload, outcome []byte, script *redis.Script, extra []interface{}) (Receipt, error) {
	if step.Claim.Authority != s.authority || step.Record.Authority != s.authority {
		return Receipt{}, ErrAuthority
	}
	if step.Claim.transport != s.transport.Load() {
		return Receipt{}, ErrStale
	}
	if err := s.validateRef(step.Claim.Ref); err != nil {
		return Receipt{}, err
	}
	if err := s.validatePayloadStructure(next); err != nil {
		return Receipt{}, err
	}
	previousEffects := step.Claim.applicationEffects
	if previousEffects == nil {
		previousEffects = snapshotApplicationEffects(step.Record.Payload.Effects)
	}
	leaseGuards, err := commitApplicationLeaseGuardsForEffects(previousEffects, next)
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
	encodedLeaseFences, err := json.Marshal(next.CommitLeaseFences)
	if err != nil {
		return Receipt{}, err
	}
	if next.CommitLeaseFences == nil {
		encodedLeaseFences = []byte("[]")
	}
	encodedLeaseGuards, err := json.Marshal(leaseGuards)
	if err != nil {
		return Receipt{}, err
	}
	leaseIDs := next.ApplicationLeaseIDs
	if leaseIDs == nil {
		leaseIDs = []string{}
	}
	encodedLeaseIDs, err := json.Marshal(leaseIDs)
	if err != nil {
		return Receipt{}, err
	}
	data, err := marshalCommitPayload(next)
	if err != nil {
		return Receipt{}, err
	}
	if err := s.validatePayloadSize(next, len(data)); err != nil {
		return Receipt{}, err
	}
	if len(outcome) > s.cfg.Limits.OutcomeBytes {
		return Receipt{}, ErrCapacity
	}
	if !decimal(step.Claim.Token) || !decimal(step.Claim.BaseRevision) {
		return Receipt{}, ErrCorrupt
	}
	digest, _, err := digestInput(step.Input)
	if err != nil || digest != step.Claim.InputDigest {
		return Receipt{}, ErrConflict
	}
	receipt := Receipt{OperationID: step.Input.ID, Fingerprint: step.Input.Fingerprint, InputDigest: digest, Revision: step.Claim.Token, ExpiresMS: step.Input.ReceiptUntilMS, Outcome: append([]byte(nil), outcome...)}
	encoded, _ := json.Marshal(receipt)
	var ready, due, retire int64
	for _, effect := range next.Effects {
		if effect.Status == EffectPending || effect.Status == EffectAuthorized {
			ready = step.NowMS
			if ready == 0 {
				ready = 1
			}
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
			retire = step.NowMS
		}
		if due == 0 || retire < due {
			due = retire
		}
	}
	args := []interface{}{step.Claim.ID, step.Claim.Token, step.Claim.Worker, step.Claim.BaseRevision, step.Claim.InputDigest, string(data), "", string(encoded), ready, due, retire, next.ReservedCompletionBytes, s.cfg.Limits.GroupBytes, s.cfg.Limits.Receipts, base64.StdEncoding.EncodedLen(s.cfg.Limits.OutcomeBytes) + 1024, string(encodedLeaseGuards), string(encodedLeaseIDs), string(encodedLeaseFences)}
	args = append(args, extra...)
	contexts, err := applicationContextProjections(step.Record.Ref, next.ApplicationContexts)
	if err != nil {
		return Receipt{}, err
	}
	contextProjection, err := json.Marshal(contexts)
	if err != nil {
		return Receipt{}, err
	}
	args = append(args, string(contextProjection))
	values, err := s.run(ctx, script, step.Claim.Ref, args, true, step.Claim.transport)
	if err != nil {
		return Receipt{}, err
	}
	if len(values) != 2 || values[0] != "RECEIPT" {
		return Receipt{}, ErrCorrupt
	}
	return receipt, nil
}

func (s *Store) InspectOperation(ctx context.Context, ref Ref, operationID, fingerprint string) (Inspection, error) {
	if !referencePattern.MatchString(operationID) || !validText(fingerprint, 256) {
		return Inspection{}, ErrConflict
	}
	record, now, err := s.read(ctx, ref)
	if err != nil {
		return Inspection{}, err
	}
	inspection := Inspection{Revision: record.Revision, NowMS: now}
	receipts := record.Receipts
	if record.Proof != nil {
		receipts = append(append([]Receipt(nil), receipts...), *record.Proof)
	}
	for _, receipt := range receipts {
		if receipt.OperationID == operationID && receipt.ExpiresMS > now {
			if receipt.Fingerprint != fingerprint {
				return Inspection{}, ErrConflict
			}
			copy := receipt
			inspection.Receipt = &copy
			return inspection, nil
		}
	}
	if record.Pending != nil && record.Pending.ID == operationID {
		if record.Pending.Fingerprint != fingerprint {
			return Inspection{}, ErrConflict
		}
		inspection.Pending = true
	}
	return inspection, nil
}

func (s *Store) hintRef(key string) (Ref, error) {
	for p := uint16(0); p < s.cfg.Partitions; p++ {
		prefix := s.partitionPrefix(p) + "group:"
		if strings.HasPrefix(key, prefix) {
			ref := Ref{ID: strings.TrimPrefix(key, prefix), Partition: p}
			return ref, s.validateRef(ref)
		}
	}
	return Ref{}, ErrCorrupt
}
func (s *Store) decodeHints(values []interface{}, partition uint16, limit int) ([]Hint, error) {
	if len(values)%2 != 0 || len(values) > 2*limit {
		return nil, ErrCorrupt
	}
	result := make([]Hint, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, err := textAt(values, i)
		if err != nil {
			return nil, err
		}
		ref, err := s.hintRef(key)
		if err != nil {
			return nil, err
		}
		if ref.Partition != partition {
			return nil, ErrCorrupt
		}
		score, err := textAt(values, i+1)
		if err != nil {
			return nil, err
		}
		due, err := strconv.ParseFloat(score, 64)
		if err != nil || due < 0 || due > 1<<53-1 || math.Trunc(due) != due {
			return nil, ErrCorrupt
		}
		result = append(result, Hint{Ref: ref, DueMS: int64(due)})
	}
	return result, nil
}

// ScheduleHints reads consecutive partitions, wrapping at the namespace bound.
// Each ready/due set retains its own limit, score order and corruption error.
// Shared authority/clock/transport failures reject the poll. Hints grant no claims.
// This bounded cross-partition read requires the single-primary/Sentinel topology.
func (s *Store) ScheduleHints(ctx context.Context, first uint16, count, limit int) ([]HintBatch, error) {
	if first >= s.cfg.Partitions || count < 1 || count > min(8, int(s.cfg.Partitions)) || limit < 1 || limit > 256 {
		return nil, errors.New("invalid scheduling batch")
	}
	keys := make([]string, 0, count*2)
	for n := 0; n < count; n++ {
		partition := (first + uint16(n)) % s.cfg.Partitions
		prefix := s.partitionPrefix(partition)
		keys = append(keys, prefix+"ready", prefix+"due")
	}
	values, err := s.runKeys(ctx, scheduleHintsScript, Ref{Partition: first}, keys, []interface{}{limit}, false)
	if err != nil {
		return nil, err
	}
	if len(values) != count+1 || values[0] != "SCHEDULE_HINTS" {
		return nil, ErrCorrupt
	}
	batches := make([]HintBatch, count)
	for n := range batches {
		classes, ok := values[n+1].([]interface{})
		if !ok || len(classes) != 2 {
			return nil, ErrCorrupt
		}
		partition := (first + uint16(n)) % s.cfg.Partitions
		batches[n].Ready, batches[n].ReadyError = s.decodeHintClass(classes[0], partition, limit)
		batches[n].Due, batches[n].DueError = s.decodeHintClass(classes[1], partition, limit)
	}
	return batches, nil
}

func (s *Store) decodeHintClass(value interface{}, partition uint16, limit int) ([]Hint, error) {
	values, ok := value.([]interface{})
	if !ok || len(values) == 0 || values[0] != "HINTS" {
		return nil, ErrCorrupt
	}
	return s.decodeHints(values[1:], partition, limit)
}

// RepairSchedule repairs one bounded SCAN page. The caller must complete scans
// frequently enough for its admitted protocol deadlines; no notification is truth.
func (s *Store) RepairSchedule(ctx context.Context, cursor uint64, count int) (uint64, int, error) {
	if count < 1 || count > 256 {
		return cursor, 0, errors.New("invalid repair batch")
	}
	if _, err := s.ensurePrimary(ctx); err != nil {
		return cursor, 0, err
	}
	keys, next, err := s.client.Scan(ctx, cursor, s.root+"*:group:*", int64(count)).Result()
	if err != nil {
		return cursor, 0, err
	}
	for i, key := range keys {
		ref, err := s.hintRef(key)
		if err != nil {
			return cursor, i, err
		}
		_, err = s.run(ctx, repairScript, ref, nil, true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return cursor, i, err
		}
	}
	return next, len(keys), nil
}

func (s *Store) Retire(ctx context.Context, ref Ref) error {
	if ref.Incarnation == "" {
		return ErrConflict
	}
	record, now, err := s.read(ctx, ref)
	if err != nil {
		return err
	}
	p := record.Payload
	if !p.Retention.Terminal || p.Retention.UntilMS > now || p.ActiveTransactions != 0 || len(p.Effects) != 0 || len(p.Timers) != 0 || record.Pending != nil {
		return ErrNotReady
	}
	for _, receipt := range record.Receipts {
		if receipt.ExpiresMS > now {
			return ErrNotReady
		}
	}
	_, err = s.run(ctx, retireScript, ref, []interface{}{record.Revision}, true)
	return err
}

func defaults(cfg *Config) error {
	if err := commonDefaults(cfg); err != nil {
		return err
	}
	if cfg.Redis.DialTimeout == 0 {
		cfg.Redis.DialTimeout = 2 * time.Second
	}
	if cfg.Redis.IOTimeout == 0 {
		cfg.Redis.IOTimeout = 2 * time.Second
	}
	if cfg.Redis.PoolSize == 0 {
		cfg.Redis.PoolSize = 16
	}
	if cfg.Redis.PoolSize < 1 || cfg.Redis.PoolSize > 256 || cfg.Redis.DialTimeout <= 0 || cfg.Redis.IOTimeout <= 0 {
		return errors.New("invalid Redis connection bounds")
	}
	if (cfg.Redis.Address != "") == (len(cfg.Redis.SentinelAddresses) > 0) ||
		(len(cfg.Redis.SentinelAddresses) > 0 && cfg.Redis.SentinelMaster == "") {
		return errors.New("select one Redis primary discovery source")
	}
	if cfg.Redis.TLS != nil {
		cfg.Redis.TLS = cfg.Redis.TLS.Clone()
	}
	return nil
}
