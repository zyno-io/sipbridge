package engine

import "sipbridge/internal/rediscore"

func (e *redisNative) udpHint(h rediscore.UDPHint) {
	if e.udp == nil || h.Worker != e.worker {
		return
	}
	key := nativeUDPRetryKey{Context: h.Context, Incarnation: h.Incarnation, Procedure: h.Procedure}
	if h.Stop {
		e.udp.Stop(key)
	} else {
		e.udp.Proceeding(key)
	}
}
