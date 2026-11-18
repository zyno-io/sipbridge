package engine

import "sipbridge/internal/state"

// NativeStateVersions is the initial service state contract. Historical reducer
// fixtures select their separate profile explicitly and cannot start the service.
func NativeStateVersions() state.Versions {
	return state.Versions{Schema: "1", Reducer: "1", Timers: "1"}
}
