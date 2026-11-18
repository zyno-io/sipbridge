package engine

import (
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"sipbridge/internal/rediscore"
)

func TestNativeSelectorUsesSharedMatchingFacts(t *testing.T) {
	request := stateTestRequest("INVITE", 1, "", "selector-native")
	origin, err := portable.ParseMessage(request.Wire, "tls", "", "")
	if err != nil {
		t.Fatal(err)
	}
	tx, _, err := nativeServerStart(origin, "local", 1000)
	if err != nil {
		t.Fatal(err)
	}
	response, err := nativeServerBuildResponse(tx, 200, "OK", nil, "local", "sip:edge@example.test;transport=tls", nil, 1001)
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := nativeSelectorIDs(origin, false, false)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := nativeSelectorIDs(response.Message, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(incoming) == 0 || len(sent) == 0 || incoming[0] != sent[0] || incoming[0].Kind != rediscore.SelectorServerTransaction {
		t.Fatal("server transaction matching changed")
	}
	outgoing, err := nativeSelectorIDs(origin, true, false)
	if err != nil {
		t.Fatal(err)
	}
	received, err := nativeSelectorIDs(response.Message, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(outgoing) == 0 || len(received) == 0 || outgoing[0] != received[0] || outgoing[0].Kind != rediscore.SelectorClientTransaction || outgoing[0] == incoming[0] {
		t.Fatal("client transaction matching changed")
	}
	if len(sent) != 2 || sent[1].Kind != rediscore.SelectorDialog {
		t.Fatal("missing dialog selector")
	}
	if nativeSelector(rediscore.SelectorDialog, "ab", "c") == nativeSelector(rediscore.SelectorDialog, "a", "bc") {
		t.Fatal("ambiguous selector tuple")
	}
}
