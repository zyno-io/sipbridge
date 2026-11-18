package engine

import "github.com/emiago/sipgo/sip/portable"

func stateTestPortablePtr(wire []byte, transport, source, destination string) *portable.Message {
	message := portable.RestoreMessage(wire, transport, source, destination)
	return &message
}
