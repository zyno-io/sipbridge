package state

import (
	"context"
	"errors"
)

func (s *Store) hints(ctx context.Context, partition uint16, kind string, limit int) ([]Hint, error) {
	if partition >= s.cfg.Partitions || limit < 1 || limit > 256 {
		return nil, errors.New("invalid scheduling batch")
	}
	values, err := s.run(ctx, hintsScript, Ref{Partition: partition}, []interface{}{kind, limit}, false)
	if err != nil {
		return nil, err
	}
	if values[0] != "HINTS" || len(values)%2 != 1 {
		return nil, ErrCorrupt
	}
	return s.decodeHints(values[1:], partition, limit)
}

func (s *Store) ReadyHints(ctx context.Context, partition uint16, limit int) ([]Hint, error) {
	return s.hints(ctx, partition, "ready", limit)
}
func (s *Store) DueHints(ctx context.Context, partition uint16, limit int) ([]Hint, error) {
	return s.hints(ctx, partition, "due", limit)
}
