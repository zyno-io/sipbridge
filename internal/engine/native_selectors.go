package engine

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/rediscore"
)

// Native Redis selectors derive from the same matching facts as Memory's
// public opaque selectors, but use an unambiguous binary-only private domain.
func nativeSelector(kind rediscore.SelectorKind, parts ...string) rediscore.Selector {
	h := sha256.New()
	_, _ = h.Write([]byte("sipbridge.native.selector.v1"))
	_, _ = h.Write([]byte{byte(kind)})
	var length [binary.MaxVarintLen64]byte
	for _, part := range parts {
		n := binary.PutUvarint(length[:], uint64(len(part)))
		_, _ = h.Write(length[:n])
		_, _ = h.Write([]byte(part))
	}
	var sum [32]byte
	h.Sum(sum[:0])
	return rediscore.Selector{Kind: kind, Digest: sum}
}
func nativeSelectorIDs(message portable.Message, outbound, observed bool) ([]rediscore.Selector, error) {
	var facts portable.MessageFacts
	var err error
	if observed {
		facts, err = message.ObservedFacts()
	} else {
		facts, err = message.Facts()
	}
	if err != nil {
		return nil, err
	}
	var key string
	var kind rediscore.SelectorKind
	if facts.IsRequest != outbound {
		kind = rediscore.SelectorServerTransaction
		if observed {
			key, err = message.ObservedCancelServerTxKey()
		} else {
			key, err = message.CancelServerTxKey()
		}
	} else {
		kind = rediscore.SelectorClientTransaction
		if observed {
			key, err = message.ObservedClientTxKey()
		} else {
			key, err = message.ClientTxKey()
		}
	}
	var out []rediscore.Selector
	if err == nil && key != "" {
		out = append(out, nativeSelector(kind, key))
	}
	if facts.FromTag != "" && facts.ToTag != "" {
		a, b := facts.FromTag, facts.ToTag
		if a > b {
			a, b = b, a
		}
		out = append(out, nativeSelector(rediscore.SelectorDialog, facts.CallID, a, b))
	}
	return out, nil
}
