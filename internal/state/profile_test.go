package state

import (
	"context"
	"errors"
	"testing"
)

func TestEachRecordRejectsIncompatibleConfiguration(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	ref := group(t, s, "profile-call")
	step := begin(t, s, ref, "initial", true)
	if _, err := s.Commit(ctx, *step, Payload{}, nil); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Config){
		"reducer": func(c *Config) { c.Versions.Reducer = "incompatible" },
		"timers":  func(c *Config) { c.Versions.Timers = "incompatible" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := s.cfg
			change(&cfg)
			other, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if _, err := other.Load(ctx, step.Record.Ref); !errors.Is(err, ErrAuthority) && !errors.Is(err, ErrVersion) {
				t.Fatal("incompatible record accepted", err)
			}
		})
	}
	if err := s.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}
