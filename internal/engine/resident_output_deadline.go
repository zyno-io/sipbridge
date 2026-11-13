package engine

import (
	"time"

	pb "sipbridge/internal/controlpb"
)

// Only actual start/retirement deadlines enter this path. A missed or uncertain
// physical attempt never creates a retry entry or another socket attempt.
func (e *ResidentMemory) fireResidentOutputDeadline(item residentDeadline) bool {
	if item.key.kind != residentOutputDeadline {
		return false
	}
	id, effectID := item.key.contextID, item.key.record
	e.mu.Lock()
	c := e.contexts[id]
	e.mu.Unlock()
	if c == nil {
		return true
	}
	c.mu.Lock()
	output := c.outputs[effectID]
	if output == nil || time.Now().Before(item.at) {
		c.mu.Unlock()
		return true
	}
	if item.generation == 2 {
		if output.settled && (output.state == residentOutputWritten || output.state == residentOutputNotWritten) {
			if e.releaseResidentOutputLocked(c, output) == nil {
				e.afterResidentOutputReleaseLocked(c)
			}
		}
		c.mu.Unlock()
		return true
	}
	write := output.write
	outcome := pb.DeliveryResult_UNKNOWN
	if output.state == residentOutputAuthorized {
		outcome = pb.DeliveryResult_NOT_WRITTEN
	}
	terminal := output.state == residentOutputWritten || output.state == residentOutputNotWritten
	if terminal && output.settled {
		e.scheduleResidentOutputCutoffLocked(c, output)
	}
	c.mu.Unlock()
	if !terminal {
		_ = e.Complete(e.ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: outcome, Error: "physical start/completion deadline elapsed"})
	}
	// Late Complete owns any newly definite cutoff. Never reinsert a popped
	// deadline after unlocking: the dispatcher may have released this proof.
	return true
}
