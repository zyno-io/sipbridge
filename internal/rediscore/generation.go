package rediscore

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// DefaultGeneration selects the Redis keyspace for this storage layout.
// Increase it when an incompatible layout needs a fresh keyspace.
const DefaultGeneration = "5"

const (
	generationHeartbeatInterval = time.Minute
	generationSweepInterval     = 15 * time.Minute
	generationRetainFor         = time.Hour
	generationClaimLifetime     = 2 * time.Minute
	generationBatchSize         = 64
	generationBatchInterval     = 50 * time.Millisecond
)

type generationScope struct {
	pool string
	id   string
}

func generationNumber(id string) uint64 {
	n, err := strconv.ParseUint(id, 10, 53)
	if err != nil || strconv.FormatUint(n, 10) != id {
		return 0
	}
	return n
}

func managedGeneration(root, id string) (generationScope, error) {
	if id == "" || strings.Trim(id, "0123456789") != "" {
		return generationScope{}, nil
	}
	pool, ok := strings.CutSuffix(root, id+":")
	if generationNumber(id) == 0 || !ok || len(pool) < 2 || !strings.HasSuffix(pool, ":") {
		return generationScope{}, ErrAuthority
	}
	for _, ch := range pool {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("_.:-", ch)) {
			return generationScope{}, ErrAuthority
		}
	}
	return generationScope{pool: pool, id: id}, nil
}

func (g generationScope) registry() string              { return g.pool + "generations" }
func (g generationScope) nativePrefix(id string) string { return g.pool + id + ":core:" }
func (g generationScope) claim(id string) string        { return g.pool + "cleanup:" + id }

type generationMaintenance struct {
	nextHeartbeat, nextSweep time.Time
	sweeping                 atomic.Bool
	jobs                     sync.WaitGroup
}

// Called from the normal worker loop. Redis maintenance runs separately from
// the lease, SIP deadline, and physical-write lanes.
func (m *generationMaintenance) schedule(d *Driver, s *Store, ctx context.Context, coordinator bool, now time.Time) {
	g := d.cfg.Connection.generation
	if g.id == "" {
		return
	}
	if !now.Before(m.nextHeartbeat) {
		m.nextHeartbeat = now.Add(generationHeartbeatInterval)
		m.jobs.Add(1)
		go func() {
			defer m.jobs.Done()
			if err := s.recordGenerationHeartbeat(ctx, g, d.cfg.Worker, d.cfg.Epoch); err != nil && ctx.Err() == nil {
				d.diagnostic("generation-heartbeat", err, false)
			}
		}()
	}
	if coordinator && !now.Before(m.nextSweep) && m.sweeping.CompareAndSwap(false, true) {
		m.nextSweep = now.Add(generationSweepInterval)
		m.jobs.Add(1)
		go func() {
			defer m.jobs.Done()
			defer m.sweeping.Store(false)
			err := s.sweepGenerations(ctx, g, d.cfg.Worker, d.cfg.Epoch, func(id string, removed int64) {
				d.report(DriverDiagnostic{Stage: "generation-cleanup", Category: "complete", Pool: g.pool, Generation: id, KeysRemoved: removed})
			})
			if err != nil && ctx.Err() == nil {
				d.diagnostic("generation-cleanup", err, false)
			}
		}()
	}
}

func (s *Store) recordGenerationHeartbeat(ctx context.Context, g generationScope, worker, epoch string) error {
	ctx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	result, err := generationHeartbeatScript.Run(ctx, s.client, []string{s.profileKey(), g.registry(), s.workerKey(worker)},
		s.profile, epoch, g.id, s.primary).Slice()
	if err != nil {
		return err
	}
	return status(result)
}

func (s *Store) sweepGenerations(ctx context.Context, g generationScope, worker, epoch string, completed func(string, int64)) error {
	ticks := time.NewTicker(generationBatchInterval)
	defer ticks.Stop()
	own := generationNumber(g.id)
	var cursor uint64
	for {
		if err := awaitGenerationBatch(ctx, ticks.C); err != nil {
			return err
		}
		page, cancel := context.WithTimeout(ctx, workerRedisTimeout)
		fields, next, err := s.client.HScan(page, g.registry(), cursor, "", generationBatchSize).Result()
		cancel()
		if err != nil {
			return err
		}
		if len(fields)%2 != 0 {
			return ErrCorrupt
		}
		for i := 0; i < len(fields); i += 2 {
			id := fields[i]
			n := generationNumber(id)
			if n == 0 || n >= own {
				continue
			}
			if err := awaitGenerationBatch(ctx, ticks.C); err != nil {
				return err
			}
			token := uuid.NewString()
			claimed, err := s.claimGeneration(ctx, g, worker, epoch, id, token)
			if err != nil {
				return err
			}
			if !claimed {
				continue
			}
			removed, err := s.sweepGeneration(ctx, g, id, token, ticks.C)
			if err != nil {
				return err
			}
			completed(id, removed)
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

func awaitGenerationBatch(ctx context.Context, ticks <-chan time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ticks:
		return nil
	}
}

func (s *Store) claimGeneration(ctx context.Context, g generationScope, worker, epoch, id, token string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	old := g.nativePrefix(id)
	result, err := claimGenerationScript.Run(ctx, s.client,
		[]string{s.profileKey(), g.registry(), s.workerKey(worker), s.coordinatorKey(), old + "profile", old + "coordinator", g.claim(id)},
		s.profile, worker, epoch, g.id, id, token, generationRetainFor.Milliseconds(), int64(generationClaimLifetime/time.Second), s.primary).Slice()
	if err != nil {
		return false, err
	}
	if err := status(result); err != nil {
		return false, err
	}
	if len(result) != 2 || result[1] != "0" && result[1] != "1" {
		return false, ErrCorrupt
	}
	return result[1] == "1", nil
}

func (s *Store) sweepGeneration(ctx context.Context, g generationScope, id, token string, ticks <-chan time.Time) (int64, error) {
	var cursor uint64
	var removed int64
	refreshAt := time.Now().Add(generationClaimLifetime / 2)
	for {
		if err := awaitGenerationBatch(ctx, ticks); err != nil {
			return removed, err
		}
		page, cancel := context.WithTimeout(ctx, workerRedisTimeout)
		keys, next, err := s.client.Scan(page, cursor, g.nativePrefix(id)+"*", generationBatchSize).Result()
		cancel()
		if err != nil {
			return removed, err
		}
		for len(keys) > 0 {
			batch := min(generationBatchSize, len(keys))
			if err := awaitGenerationBatch(ctx, ticks); err != nil {
				return removed, err
			}
			count, err := s.sweepGenerationBatch(ctx, g, id, token, keys[:batch], false)
			if err != nil {
				return removed, err
			}
			removed += count
			keys = keys[batch:]
			refreshAt = time.Now().Add(generationClaimLifetime / 2)
		}
		if next == 0 {
			_, err = s.sweepGenerationBatch(ctx, g, id, token, nil, true)
			return removed, err
		}
		if !time.Now().Before(refreshAt) {
			if _, err := s.sweepGenerationBatch(ctx, g, id, token, nil, false); err != nil {
				return removed, err
			}
			refreshAt = time.Now().Add(generationClaimLifetime / 2)
		}
		cursor = next
	}
}

func (s *Store) sweepGenerationBatch(ctx context.Context, g generationScope, id, token string, keys []string, done bool) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, workerRedisTimeout)
	defer cancel()
	prefix := g.nativePrefix(id)
	redisKeys := append([]string{s.profileKey(), g.registry(), g.claim(id), prefix + "profile"}, keys...)
	finish := "0"
	if done {
		finish = "1"
	}
	result, err := sweepGenerationScript.Run(ctx, s.client, redisKeys,
		s.profile, token, prefix, id, finish, generationClaimLifetime.Milliseconds(), s.primary).Slice()
	if err != nil {
		return 0, errors.Join(ErrUnknown, err)
	}
	if err := status(result); err != nil {
		return 0, err
	}
	if len(result) != 2 {
		return 0, ErrCorrupt
	}
	return responseInt(result[1])
}
