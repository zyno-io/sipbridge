package engine

import "sipbridge/internal/state"

// RetainedInspection contains only bounded classes/counts, never SIP identities
// or payloads. It is for offline diagnosis and grants no execution authority.
type RetainedInspection struct {
	Contexts, Transactions, Dialogs, Allocations, Retired int
	Answered, Ended, OwnerLost                            int
}

func InspectRetainedProtocol(data []byte) (RetainedInspection, error) {
	group, err := decodeStateGroup(state.Record{Payload: state.Payload{ProtocolState: data}, Versions: NativeStateVersions()})
	if err != nil {
		return RetainedInspection{}, err
	}
	result := RetainedInspection{Contexts: len(group.Contexts), Allocations: len(group.Allocations), Retired: len(group.Retired)}
	for _, context := range group.Contexts {
		if context.Snapshot == nil {
			return RetainedInspection{}, state.ErrCorrupt
		}
		result.Transactions += len(context.Transactions)
		result.Dialogs += len(context.Dialogs)
		if context.Snapshot.Answered {
			result.Answered++
		}
		if context.Snapshot.Ended {
			result.Ended++
		}
		if context.OwnerLost {
			result.OwnerLost++
		}
	}
	return result, nil
}
