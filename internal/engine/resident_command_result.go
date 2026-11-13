package engine

import (
	"context"
	"strings"
	"unicode/utf8"
	"unsafe"

	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
)

// A receipt owns only its immutable outcome marker and the exact wire needed by
// its original waiter or the current replay slot. The admission-created pin
// exists before the caller unlocks, so a newer command cannot discard the
// original waiter's evidence before that waiter reaches await.
type residentCommand struct {
	operation       string
	sequence        uint64
	certainty       pb.OperationResult_Certainty
	status          uint32
	err             string
	requestWire     []byte
	responseWire    []byte
	engine          *ResidentMemory
	budget          *residentBudget
	borrowed        int64
	references      uint32
	baseBytes       int64
	baseBorrowed    int64
	baseBudget      *residentBudget
	errorReserve    int64
	errorBorrowed   int64
	originalPending bool
	registered      bool
	replayWaiters   int
	done            chan struct{}
	finished        bool
}

func newResidentCommand(operation string, sequence uint64) *residentCommand {
	return &residentCommand{operation: operation, sequence: sequence, certainty: pb.OperationResult_ACCEPTED, originalPending: true, done: make(chan struct{})}
}

// The channel is an unbuffered runtime allocation; 128 bytes conservatively
// accounts for its fixed 64-bit runtime header on supported Go targets.
const (
	residentCommandChannelBytes = int64(128)
	residentCommandErrorLimit   = 4 * 1024
)

func (e *ResidentMemory) prepareResidentCommandLocked(c *residentContext, command *residentCommand, budget *residentBudget) error {
	command.operation = strings.Clone(command.operation)
	command.baseBytes = int64(unsafe.Sizeof(*command)) + residentCommandChannelBytes + int64(len(command.operation))
	var err error
	if budget != nil && budget.open {
		command.baseBudget = budget
		command.baseBorrowed, err = e.chargeResidentBudgetLocked(c, budget, command.baseBytes)
	} else {
		err = e.accountResidentLocked(c, command.baseBytes, 0)
	}
	if err != nil {
		return err
	}
	if command.baseBudget != nil {
		command.errorBorrowed = min(command.baseBudget.remaining, int64(residentCommandErrorLimit))
		command.baseBudget.remaining -= command.errorBorrowed
	}
	extra := int64(residentCommandErrorLimit) - command.errorBorrowed
	if err = e.accountResidentLocked(c, 0, extra); err != nil {
		if command.baseBudget != nil {
			command.baseBudget.remaining += command.errorBorrowed
			_ = e.refundResidentBudgetLocked(c, command.baseBudget, command.baseBytes, command.baseBorrowed)
		} else {
			_ = e.accountResidentLocked(c, -command.baseBytes, 0)
		}
		return err
	}
	command.errorReserve = residentCommandErrorLimit
	command.references = 2 // current replay slot and original operation
	return nil
}

func residentCommandError(text string) string {
	text = strings.ToValidUTF8(text, "�")
	const suffix = "...[truncated]"
	if len(text) > residentCommandErrorLimit {
		end := residentCommandErrorLimit - len(suffix)
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		text = text[:end] + suffix
	}
	return strings.Clone(text)
}

func retainResidentCommandLocked(command *residentCommand) {
	if command != nil && command.registered {
		command.references++
	}
}

func releaseResidentCommandLocked(c *residentContext, command *residentCommand) {
	if command == nil || !command.registered {
		return
	}
	if command.references == 0 {
		panic("resident command reference underflow")
	}
	command.references--
	if command.references != 0 {
		return
	}
	releaseResidentCommandWireLocked(c, command)
	returned := int64(0)
	if command.baseBudget != nil && command.baseBudget.open {
		returned = command.errorBorrowed
		command.baseBudget.remaining += returned
	}
	_ = command.engine.accountResidentLocked(c, -int64(len(command.err)), returned-command.errorReserve)
	command.err, command.errorReserve, command.errorBorrowed = "", 0, 0
	if command.baseBudget != nil {
		_ = command.engine.refundResidentBudgetLocked(c, command.baseBudget, command.baseBytes, command.baseBorrowed)
	} else {
		_ = command.engine.accountResidentLocked(c, -command.baseBytes, 0)
	}
	command.baseBytes, command.baseBorrowed = 0, 0
}

func residentCommandVisible(c *residentContext, command *residentCommand) bool {
	return c.command == command || command.originalPending || command.replayWaiters != 0
}

func releaseResidentCommandWireLocked(c *residentContext, command *residentCommand) {
	if command == nil || residentCommandVisible(c, command) {
		return
	}
	bytes := int64(len(command.requestWire) + len(command.responseWire))
	if bytes > 0 && command.engine != nil {
		_ = command.engine.refundResidentBudgetLocked(c, command.budget, bytes, command.borrowed)
	}
	command.requestWire, command.responseWire, command.borrowed = nil, nil, 0
	if command.registered && command.engine != nil {
		returned := int64(0)
		if command.baseBudget != nil && command.baseBudget.open {
			returned = command.errorBorrowed
			command.baseBudget.remaining += returned
		}
		_ = command.engine.accountResidentLocked(c, -int64(len(command.err)), returned-command.errorReserve)
		command.err, command.errorReserve, command.errorBorrowed = "", 0, 0
	}
}

// residentCommandResultLocked materializes a fresh public DTO. The outcome and
// exact request/response bytes are historical; Snapshot projects current state.
func residentCommandResultLocked(c *residentContext, command *residentCommand) *pb.OperationResult {
	result := &pb.OperationResult{Certainty: command.certainty, SipStatus: command.status, Error: command.err}
	if len(command.requestWire) != 0 {
		result.Request = mustObserve(append([]byte(nil), command.requestWire...), true)
	}
	if len(command.responseWire) != 0 {
		result.Response = mustObserve(append([]byte(nil), command.responseWire...), false)
	}
	if c.snapshot != nil {
		result.Snapshot = proto.Clone(c.snapshot).(*pb.ContextSnapshot)
	}
	return result
}

func finishResidentCommandLocked(c *residentContext, command *residentCommand, result *pb.OperationResult) {
	finishResidentCommandWireLocked(c, command, result.GetCertainty(), result.GetSipStatus(), result.GetError(), result.GetRequest().GetWire(), result.GetResponse().GetWire())
}

// finishResidentCommandWireLocked retains only immutable wire evidence; rich
// DTOs are materialized later when a public result is actually requested.
func finishResidentCommandWireLocked(c *residentContext, command *residentCommand, certainty pb.OperationResult_Certainty, status uint32, failure string, requestWire, responseWire []byte) {
	if command == nil {
		return
	}
	if command.finished {
		before, after := command.certainty, certainty
		upgrade := before == pb.OperationResult_UNKNOWN && (after == pb.OperationResult_ACCEPTED || after == pb.OperationResult_COMPLETED) || before == pb.OperationResult_ACCEPTED && after == pb.OperationResult_COMPLETED
		if !upgrade {
			return
		}
	}
	if !residentCommandVisible(c, command) {
		requestWire, responseWire = nil, nil
	}
	oldBytes := int64(len(command.requestWire) + len(command.responseWire))
	newBytes := int64(len(requestWire) + len(responseWire))
	delta := newBytes - oldBytes
	if delta > 0 && command.engine != nil {
		borrowed, err := command.engine.chargeResidentBudgetLocked(c, command.budget, delta)
		if err != nil {
			if command.finished {
				// Preserve already-retained evidence when a later upgrade cannot
				// retain its own required bytes.
				return
			}
			// Do not assert a known result when its required replay evidence could
			// not be retained. Admission should reserve this continuation space.
			certainty, status, failure = pb.OperationResult_UNKNOWN, 0, "command evidence capacity unavailable"
			requestWire, responseWire, newBytes, delta = nil, nil, 0, -oldBytes
		} else {
			command.borrowed += borrowed
		}
	}
	if delta < 0 && command.engine != nil {
		released := -delta
		borrowed := min(command.borrowed, released)
		_ = command.engine.refundResidentBudgetLocked(c, command.budget, released, borrowed)
		command.borrowed -= borrowed
	}
	errText := ""
	if residentCommandVisible(c, command) {
		errText = residentCommandError(failure)
	}
	errorDelta := int64(len(errText) - len(command.err))
	if command.registered && errorDelta != 0 {
		if err := command.engine.accountResidentLocked(c, errorDelta, -errorDelta); err != nil {
			return
		}
		command.errorReserve -= errorDelta
	}
	command.requestWire = append([]byte(nil), requestWire...)
	command.responseWire = append([]byte(nil), responseWire...)
	command.certainty, command.status, command.err = certainty, status, errText
	wasFinished := command.finished
	command.finished = true
	if !wasFinished {
		close(command.done)
	}
}

func awaitResidentCommand(ctx context.Context, c *residentContext, command *residentCommand) (*pb.OperationResult, error) {
	return awaitResidentCommandPin(ctx, c, command, true)
}
func awaitResidentReplay(ctx context.Context, c *residentContext, command *residentCommand) (*pb.OperationResult, error) {
	return awaitResidentCommandPin(ctx, c, command, false)
}
func awaitResidentCommandPin(ctx context.Context, c *residentContext, command *residentCommand, original bool) (*pb.OperationResult, error) {
	select {
	case <-command.done:
	case <-ctx.Done():
		c.mu.Lock()
		if !command.finished {
			finishResidentCommandLocked(c, command, &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "command outcome unavailable before deadline"})
		}
		c.mu.Unlock()
	}
	c.mu.Lock()
	result := residentCommandResultLocked(c, command)
	releasePin := false
	if original {
		if command.originalPending {
			releasePin = true
			if command.registered {
				c.receiptPins--
			}
		}
		command.originalPending = false
	} else if command.replayWaiters > 0 {
		command.replayWaiters--
		releasePin = true
		if command.registered {
			c.receiptPins--
		}
	}
	releaseResidentCommandWireLocked(c, command)
	if releasePin {
		releaseResidentCommandLocked(c, command)
	}
	if c.retireReady && command.registered && command.engine != nil {
		command.engine.afterResidentOutputReleaseLocked(c)
	}
	c.mu.Unlock()
	return result, nil
}
