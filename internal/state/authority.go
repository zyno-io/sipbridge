package state

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// A primary observation is process-local transport metadata, never SIP state.
// Pointer identity also invalidates computations if a process is observed again
// after an intervening promotion.
type primaryObservation struct{ runID string }

func (s *Store) primaryID() string {
	if observed := s.transport.Load(); observed != nil {
		return observed.runID
	}
	return ""
}

func (s *Store) observeConnection(ctx context.Context, conn *redis.Conn) error {
	server, err := conn.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	if len(s.cfg.Redis.SentinelAddresses) > 0 && sentinelModePattern.MatchString(server) {
		return nil
	}
	role, err := conn.Info(ctx, "replication").Result()
	if err != nil {
		return err
	}
	match := runIDPattern.FindStringSubmatch(server)
	if len(match) != 2 || !masterPattern.MatchString(role) {
		return ErrAuthority
	}
	for {
		previous := s.transport.Load()
		if previous != nil && previous.runID == match[1] {
			return nil
		}
		// Do not recalibrate away server skew on startup or promotion.
		start := time.Now()
		now, err := conn.Time(ctx).Result()
		finish := time.Now()
		if err != nil {
			return err
		}
		if err := verifyClock(start, finish, now.UnixMilli()); err != nil {
			return err
		}
		next := &primaryObservation{runID: match[1]}
		if s.transport.CompareAndSwap(previous, next) {
			if previous != nil {
				s.ready.Store(false)
			}
			return nil
		}
	}
}
