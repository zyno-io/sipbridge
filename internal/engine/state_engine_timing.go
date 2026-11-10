package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"time"
	"unicode"

	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/execution"
)

func controlCallIDHash(ref *pb.ContextRef) string {
	return diagnosticCallIDHash(ref.GetState().GetCallId())
}

func diagnosticCallIDHash(callID string) string {
	// Match JavaScript trim: Unicode White_Space except NEL, plus BOM.
	value := strings.TrimFunc(callID, func(r rune) bool {
		return r == '\ufeff' || unicode.IsSpace(r) && r != '\u0085'
	})
	if len(value) == 0 || len(value) > 512 {
		return ""
	}
	for _, b := range []byte(value) {
		if b < 33 || b > 126 {
			return ""
		}
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// One bounded terminal summary, never SIP bytes, addresses, credentials or
// retained protocol state. Counts are request-local and do not add Redis reads.
func (e *StateEngine) reportControlTiming(envelope *pb.Envelope, result *pb.OperationResult, timing *execution.Timing, started, replyStarted time.Time, sendFailed bool) {
	total := time.Since(started)
	if total < 100*time.Millisecond && !sendFailed {
		return
	}
	kind := "other"
	var ref *pb.ContextRef
	if ingest := envelope.GetIngest(); ingest != nil {
		kind, ref = "ingest", ingest.Context
	} else if command := envelope.GetCommand(); command != nil {
		kind, ref = "command", command.Context
	} else if inspect := envelope.GetInspect(); inspect != nil {
		kind, ref = "inspect", inspect.Context
	}
	e.lifecycleMu.Lock()
	scheduled := len(e.scheduled)
	e.lifecycleMu.Unlock()
	summary := struct {
		Event, Correlation, Context, Kind, Certainty       string
		CallIDHash                                         string `json:",omitempty"`
		BudgetMS                                           uint32
		TotalMS, ReplyMS, PreflightMS, GroupWaitMS, SlotMS float64
		ReadMS, ReduceMS, CommitMS                         float64
		Reads, Reductions, Commits                         int
		Stale, Busy, Unknown                               int
		Work, Background, Scheduled                        int
		SendFailed                                         bool
	}{
		Event: "sipbridge-control-slow", Correlation: envelope.CorrelationId, Context: ref.GetId(), Kind: kind,
		CallIDHash: controlCallIDHash(ref),
		Certainty:  result.GetCertainty().String(), BudgetMS: envelope.RemainingBudgetMs,
		TotalMS: float64(total) / float64(time.Millisecond), ReplyMS: float64(time.Since(replyStarted)) / float64(time.Millisecond),
		PreflightMS: float64(timing.Preflight) / float64(time.Millisecond), GroupWaitMS: float64(timing.GroupWait) / float64(time.Millisecond),
		SlotMS: float64(timing.Slot) / float64(time.Millisecond),
		ReadMS: float64(timing.Read) / float64(time.Millisecond), ReduceMS: float64(timing.Reduce) / float64(time.Millisecond),
		CommitMS: float64(timing.Commit) / float64(time.Millisecond), Reads: timing.Reads, Reductions: timing.Reductions, Commits: timing.Commits,
		Stale: timing.Stale, Busy: timing.Busy, Unknown: timing.Unknown,
		Work: len(e.work), Background: len(e.background), Scheduled: scheduled, SendFailed: sendFailed,
	}
	if data, err := json.Marshal(summary); err == nil {
		_, _ = os.Stdout.Write(append(data, '\n'))
	}
}
