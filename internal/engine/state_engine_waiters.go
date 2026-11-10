package engine

import "sipbridge/internal/state"

// Only admitted control commands register. The existing work limit bounds these
// transient hints; they contain no protocol snapshot or authoritative result.
func (e *StateEngine) watchOperation(ref state.Ref) (<-chan struct{}, func()) {
	key := ref.ID + ":" + ref.Incarnation
	signal := make(chan struct{}, 1)
	e.lifecycleMu.Lock()
	if e.waiters == nil {
		e.waiters = make(map[string]map[chan struct{}]struct{})
	}
	if e.waiters[key] == nil {
		e.waiters[key] = make(map[chan struct{}]struct{})
	}
	e.waiters[key][signal] = struct{}{}
	e.lifecycleMu.Unlock()
	return signal, func() {
		e.lifecycleMu.Lock()
		delete(e.waiters[key], signal)
		if len(e.waiters[key]) == 0 {
			delete(e.waiters, key)
		}
		e.lifecycleMu.Unlock()
	}
}

func (e *StateEngine) signalOperation(ref state.Ref) {
	key := ref.ID + ":" + ref.Incarnation
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	for signal := range e.waiters[key] {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
}
