package engine

import (
	"context"
	"errors"

	"sipbridge/internal/control"
	"sipbridge/internal/execution"
	executionfixture "sipbridge/internal/execution/fixture"
	"sipbridge/internal/state"
)

// newStateForFixture keeps the retired 1/7 runtime available only to historical
// protocol assertions. Native process tests still construct the public engine.
func newStateForFixture(ctx context.Context, cfg Config, store *state.Store, server *control.Server, mode string) (*StateEngine, error) {
	if store != nil && store.Versions() == (state.Versions{Schema: "3", Reducer: "9", Timers: "1"}) {
		return NewState(ctx, cfg, store, server)
	}
	if cfg.Process == nil || cfg.AdvertiseAddress == "" || cfg.ContactURI == "" || len(cfg.StateIdentityKey) < 32 || store == nil || server == nil || store.Versions() != (state.Versions{Schema: "1", Reducer: "7", Timers: "1"}) {
		return nil, errors.New("invalid historical Redis fixture")
	}
	validation := &StateEngine{cfg: cfg}
	for _, transport := range []string{"tls", "udp", "tcp"} {
		if _, _, err := validation.advertisement(transport); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	e := &StateEngine{cfg: cfg, store: store, control: server, ctx: ctx, cancel: cancel, work: make(chan struct{}, 64), background: make(chan struct{}, 48), scheduled: make(map[string]bool), dispatching: make(map[string]bool)}
	e.cfg.StateIdentityKey = append([]byte(nil), cfg.StateIdentityKey...)
	var err error
	switch mode {
	case "", "legacy":
		e.runtime, err = executionfixture.New(store, execution.ReducerFunc(e.reduce), cfg.Process.InstanceId, 64, e.resumeConnectedOwners)
	case "cas":
		e.runtime, err = execution.NewCAS(store, execution.ReducerFunc(e.reduce), 64)
	default:
		err = errors.New("invalid historical fixture mode")
	}
	if err != nil {
		cancel()
		return nil, err
	}
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.schedule() }()
	return e, nil
}
