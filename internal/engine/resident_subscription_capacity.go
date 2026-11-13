package engine

import "unsafe"

func residentSubscriptionBytes(sub *stateSubscription) int64 {
	if sub == nil {
		return 0
	}
	size := int64(unsafe.Sizeof(*sub)) + int64(len(sub.Event)+len(sub.Target)+len(sub.Pending))
	for _, route := range sub.Routes {
		size += int64(len(route)) + 16
	}
	return size
}
