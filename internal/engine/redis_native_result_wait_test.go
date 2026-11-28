package engine

import (
	"testing"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
)

func TestNativeResultWaiterHintsAreExactAndCoalesced(t *testing.T) {
	e := &redisNative{resultWaiters: make(map[rediscore.ResultHint]*nativeResultWaiter)}
	key := rediscore.ResultHint{Context: "ctx", Incarnation: "inc", Sequence: 7}
	waiter, err := e.registerResultWaiter(key)
	if err != nil {
		t.Fatal(err)
	}
	defer e.releaseResultWaiter(key, waiter)
	first := e.resultWaiterSignal(waiter)
	e.resultHint(rediscore.ResultHint{Context: "ctx", Incarnation: "inc", Sequence: 8})
	select {
	case <-first:
		t.Fatal("unrelated result woke waiter")
	default:
	}
	e.resultHint(key)
	e.resultHint(key)
	select {
	case <-first:
	default:
		t.Fatal("matching result did not wake waiter")
	}
	next := e.resultWaiterSignal(waiter)
	select {
	case <-next:
		t.Fatal("new generation already signaled")
	default:
	}
	e.resultHint(key)
	select {
	case <-next:
	default:
		t.Fatal("second generation did not wake waiter")
	}
}

func TestNativePendingResultRequiresUnfinishedUnknown(t *testing.T) {
	result := &nativepb.Result{Certainty: pb.OperationResult_UNKNOWN, Pending: true, CutoffMs: 1, Sequence: 1, OperationId: "operation", Fingerprint: make([]byte, 32), DecidingLeaseTerm: "term"}
	encoded, err := encodeNativeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeResult(encoded)
	if err != nil || !decoded.Pending {
		t.Fatalf("pending result: %v", err)
	}
	decoded.Certainty = pb.OperationResult_COMPLETED
	if _, err = encodeNativeResult(decoded); err == nil {
		t.Fatal("pending completed result accepted")
	}
	decoded.Certainty = pb.OperationResult_UNKNOWN
	decoded.Response = "r:response"
	if _, err = encodeNativeResult(decoded); err == nil {
		t.Fatal("pending final response accepted")
	}
}
