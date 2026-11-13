package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

// stageResidentOutputsLocked validates the complete output batch before
// publishing any of it. The caller commits its already-staged native values
// immediately afterward, then releases c.mu before dispatch.
func (e *ResidentMemory) stageResidentOutputsLocked(c *residentContext, tx *residentTransaction, effects []portable.Effect, policy *pb.CommandAdmission, operation string, deadline time.Time) ([]*pb.FlowWrite, error) {
	return e.stageResidentMixedOutputsLocked(c, tx, effects, policy, operation, deadline, nil)
}

// Mixed paired native transitions publish one all-or-nothing output batch. Each
// response retains its own return flow.
func (e *ResidentMemory) stageResidentMixedOutputsLocked(c *residentContext, tx *residentTransaction, effects []portable.Effect, policy *pb.CommandAdmission, operation string, deadline time.Time, selectTransaction func(portable.Effect) *residentTransaction) ([]*pb.FlowWrite, error) {
	return e.stageResidentOutputGroupsLocked(c, []residentOutputGroup{{tx: tx, effects: effects, policy: policy, selectTransaction: selectTransaction}}, operation, deadline)
}

type residentOutputGroup struct {
	tx                *residentTransaction
	effects           []portable.Effect
	policy            *pb.CommandAdmission
	nativeCompletion  bool
	selectTransaction func(portable.Effect) *residentTransaction
}

// A decision can stage its response and required NOTIFY together. All native
// validation and all budget charges succeed before any output becomes visible.
func (e *ResidentMemory) stageResidentOutputGroupsLocked(c *residentContext, groups []residentOutputGroup, operation string, deadline time.Time, publishAuthority ...func() error) ([]*pb.FlowWrite, error) {
	var staged []*residentOutput
	type budgetCharge struct {
		bytes, consumed int64
		count           int
		charged         bool
	}
	charges := make(map[*residentBudget]*budgetCharge)
	type emissionCount struct{ total, provisional uint16 }
	counts := make(map[*residentTransaction]emissionCount)
	acks := make(map[*residentBudget]map[residentACKIdentity]uint16)
	for _, group := range groups {
		tx, effects, policy, selectTransaction := group.tx, group.effects, group.policy, group.selectTransaction
		for _, effect := range effects {
			if effect.Kind != portable.Send && effect.Kind != portable.Respond {
				continue
			}
			if effect.Message == nil {
				return nil, errors.New("native send lacks wire")
			}
			selected := tx
			if selectTransaction != nil {
				selected = selectTransaction(effect)
			}
			if selected == nil {
				return nil, errors.New("missing output transaction")
			}
			budget := selected.budget
			if budget == nil {
				budget = c.budget
			}
			flow := selected.flow
			if policy != nil && policy.SelectedFlow != nil {
				flow = policy.SelectedFlow
			}
			if !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
				return nil, errors.New("local output flow required")
			}
			message := *effect.Message
			if message.Transport != flow.Transport {
				return nil, errors.New("output transport changed")
			}
			parsed, err := message.Parse()
			if err != nil {
				return nil, err
			}
			// Physical authority needs only method/status/wire. Do not allocate a
			// controller header/address/digest DTO or parse the same output again.
			observed := &pb.SipMessage{Method: string(parsed.CSeq().MethodName)}
			if response, ok := parsed.(*sip.Response); ok {
				response.Via().Params.Add("received", flow.SourceIp)
				if response.Via().Params.Has("rport") {
					response.Via().Params.Add("rport", strconv.Itoa(int(flow.SourcePort)))
				}
				message.Wire = []byte(response.String())
				observed.Status = uint32(response.StatusCode)
			}
			observed.Wire = message.Wire
			if observed.Method == "ACK" && observed.Status == 0 {
				identity := residentACKIdentity{local: tag(parsed.From().Params), remote: tag(parsed.To().Params), sequence: parsed.CSeq().SeqNo}
				if budget != nil && budget.acks[identity]+acks[budget][identity] >= stateProfileTransmissions {
					continue
				}
				if acks[budget] == nil {
					acks[budget] = make(map[residentACKIdentity]uint16)
				}
				acks[budget][identity]++
			} else {
				count, ok := counts[selected]
				if !ok {
					count = emissionCount{selected.emissions, selected.provisionalEmissions}
				}
				provisional := observed.Status > 0 && observed.Status < 200
				full := count.total >= stateTransactionEmissions || provisional && count.provisional >= stateProfileTransmissions || observed.Status >= 200 && count.total-count.provisional >= stateProfileTransmissions
				if full {
					if operation != "" && !effect.Replay {
						return nil, errors.New("procedure emission capacity")
					}
					continue
				}
				count.total++
				if provisional {
					count.provisional++
				}
				counts[selected] = count
			}
			until := time.Now().Add(2 * time.Second)
			if !deadline.IsZero() && deadline.Before(until) {
				until = deadline
			}
			if policy != nil && time.UnixMilli(int64(policy.ExpiresAtMs)).Before(until) {
				until = time.UnixMilli(int64(policy.ExpiresAtMs))
			}
			if !time.Now().Before(until) {
				return nil, errors.New("output deadline elapsed")
			}
			class := pb.EffectAuthorization_APPLICATION
			if selected.cleanup {
				class = pb.EffectAuthorization_TERMINAL_CLEANUP
			} else if observed.Status > 0 {
				class = pb.EffectAuthorization_RESPONSE
			} else if group.nativeCompletion || effect.Replay || observed.Method == "ACK" || observed.Method == "CANCEL" {
				class = pb.EffectAuthorization_NATIVE_COMPLETION
			}
			ref := &pb.StateEffectRef{Context: c.snapshot.Context, AttemptId: "a-" + uuid.NewString()}
			auth := &pb.EffectAuthorization{Effect: ref, AuthorityClass: class, DeadlineMs: uint64(until.UnixMilli()), ApplicationLeaseTerm: c.snapshot.ApplicationLeaseTerm,
				CompletionScope: e.id("native-completion-v1", e.authority.Service, e.authority.Generation, e.authority.PrimaryId, c.snapshot.Context.State.GroupId, c.snapshot.Context.State.Incarnation, c.snapshot.CallId, c.snapshot.Context.Id)}
			fingerprint, err := SendFingerprint(c.snapshot, observed, auth, flow)
			if err != nil {
				return nil, err
			}
			ref.Id, auth.Fingerprint = "e-"+fingerprint, fingerprint
			epoch := e.authority.PrimaryId
			if policy != nil {
				epoch = policy.DirectoryEpoch
			}
			write := &pb.FlowWrite{Effect: ref, Flow: flow, Permit: &pb.SendPermit{Authorization: auth, Flow: flow, DirectoryEpoch: epoch,
				OrderingProof: e.id("native-write-authority-v1", fingerprint), CompletionScope: auth.CompletionScope}}
			output, err := newResidentOutput(write, message.Wire, until)
			if err != nil {
				return nil, err
			}
			output.transaction, output.operation, output.command = selected.native.Key, operation, selected.command
			output.replay = effect.Replay
			output.method, output.status, output.drain = observed.Method, observed.Status, drainMessageObservation(parsed)
			if observed.Method == "ACK" {
				output.ackSequence = parsed.CSeq().SeqNo
			}
			output.bytes = int64(unsafe.Sizeof(*output)) + int64(len(message.Wire)) + residentFlowBytes(flow) + residentProofBytes(output.write) + 512
			if class == pb.EffectAuthorization_APPLICATION && policy != nil && policy.ExpectedDelivery != nil {
				output.expectedDelivery = proto.Clone(policy.ExpectedDelivery).(*pb.ContextDelivery)
			}
			output.budget = budget
			charge := charges[budget]
			if charge == nil {
				charge = &budgetCharge{}
				charges[budget] = charge
			}
			charge.bytes += output.bytes
			charge.count++
			staged = append(staged, output)
		}
	}
	if len(staged) == 0 {
		return nil, nil
	}
	if len(c.outputs)+c.reservedOutputs > stateNativeProcedures*stateTransactionEmissions {
		return nil, errors.New("context output capacity")
	}
	for budget, charge := range charges {
		if budget == nil || !budget.open || budget.outputs < charge.count {
			return nil, errors.New("procedure output capacity")
		}
	}
	for budget, charge := range charges {
		consumed, err := e.chargeResidentBudgetLocked(c, budget, charge.bytes)
		if err != nil {
			for owner, prior := range charges {
				if prior.charged {
					_ = e.refundResidentBudgetLocked(c, owner, prior.bytes, prior.consumed)
				}
			}
			return nil, err
		}
		charge.consumed, charge.charged = consumed, true
	}
	if len(publishAuthority) > 0 {
		if err := publishAuthority[0](); err != nil {
			for budget, charge := range charges {
				_ = e.refundResidentBudgetLocked(c, budget, charge.bytes, charge.consumed)
			}
			return nil, err
		}
	}
	for budget, charge := range charges {
		budget.outputs -= charge.count
		c.reservedOutputs -= charge.count
	}
	for selected, count := range counts {
		selected.emissions, selected.provisionalEmissions = count.total, count.provisional
	}
	for budget, identities := range acks {
		if budget.acks == nil {
			budget.acks = make(map[residentACKIdentity]uint16)
		}
		for identity, count := range identities {
			budget.acks[identity] += count
		}
	}
	if c.outputs == nil {
		c.outputs = make(map[string]*residentOutput)
	}
	writes := make([]*pb.FlowWrite, 0, len(staged))
	for _, output := range staged {
		charge := charges[output.budget]
		output.borrowed = min(charge.consumed, output.bytes)
		charge.consumed -= output.borrowed
		c.outputs[output.write.Effect.Id] = output
		retainResidentCommandLocked(output.command)
		writes = append(writes, output.write)
	}
	e.mu.Lock()
	published := e.contexts[c.snapshot.Context.Id] == c
	e.mu.Unlock()
	if published {
		e.scheduleResidentOutputDeadlinesLocked(c, writes)
	}
	return writes, nil
}

func (e *ResidentMemory) Reserve(ctx context.Context, write *pb.FlowWrite) (*CommittedWrite, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if write == nil || write.Effect == nil || !sameProcess(write.GetFlow().GetIngress(), e.cfg.Process) {
		return nil, errors.New("invalid local output")
	}
	c, err := e.findContext(write.Effect.Context)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
		}
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return e.reserveResidentOutputLocked(c, write)
}

func (e *ResidentMemory) reserveResidentOutputLocked(c *residentContext, write *pb.FlowWrite) (*CommittedWrite, error) {
	if !e.residentContextCurrentLocked(c, write.Effect.Context) {
		e.mu.Lock()
		retired := e.contexts[write.Effect.Context.Id] != c
		e.mu.Unlock()
		if retired || c.outputs[write.Effect.Id] == nil {
			return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
		}
		return nil, errors.New("output context changed")
	}
	output := c.outputs[write.Effect.Id]
	if output == nil {
		return &CommittedWrite{Known: true, Outcome: pb.DeliveryResult_UNKNOWN}, nil
	}
	if !proto.Equal(c.snapshot.GetContext().GetState(), write.Effect.Context.State) {
		return nil, errors.New("output context changed")
	}
	if output.state == residentOutputAuthorized && output.write.Permit.Authorization.AuthorityClass == pb.EffectAuthorization_APPLICATION {
		e.mu.Lock()
		lease := e.leases[residentControllerKey(c.snapshot.Application)]
		valid := lease != nil && lease.term == output.write.Permit.Authorization.ApplicationLeaseTerm && time.Now().Before(lease.expires)
		e.mu.Unlock()
		if !valid || !proto.Equal(output.expectedDelivery, c.snapshot.Delivery) {
			return nil, errors.New("application output authority changed")
		}
	}
	reserved, err := output.reserve(write, time.Now())
	if err != nil {
		return nil, err
	}
	if !reserved.Known {
		observation := output.drain
		reserved.Drain = &observation
	}
	return reserved, nil
}

func (e *ResidentMemory) Complete(ctx context.Context, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if write == nil || write.Effect == nil {
		return errors.New("output completion identity required")
	}
	c, err := e.findContext(write.Effect.Context)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if !e.residentContextCurrentLocked(c, write.Effect.Context) {
		c.mu.Unlock()
		return errors.New("output context changed")
	}
	output := c.outputs[write.Effect.Id]
	if output == nil {
		c.mu.Unlock()
		return errors.New("output not found")
	}
	before := output.state
	if err = output.complete(write, result); err != nil {
		c.mu.Unlock()
		return err
	}
	if (before == residentOutputWritten || before == residentOutputNotWritten) && output.settled {
		c.mu.Unlock()
		return nil
	}
	if output.wireBytes > 0 {
		returned := min(output.borrowed, output.wireBytes)
		if err = e.refundResidentBudgetLocked(c, output.budget, output.wireBytes, returned); err != nil {
			c.mu.Unlock()
			return err
		}
		output.bytes -= output.wireBytes
		output.borrowed -= returned
		output.wireBytes = 0
	}
	writes, err := e.residentOutputCompleted(c, output, result)
	if err == nil && (output.state == residentOutputWritten || output.state == residentOutputNotWritten) {
		output.settled = true
		command := output.command
		output.command = nil
		releaseResidentCommandLocked(c, command)
		// The original start deadline provides the unacknowledged fallback.
		// A late settlement must install it here if that deadline already fired.
		if !time.Now().Before(output.deadline) {
			e.scheduleResidentOutputCutoffLocked(c, output)
		}
	}
	if err == nil {
		e.finishResidentOwnerCleanupLocked(c)
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	e.dispatchResident(e.ctx, writes)
	return nil
}

func (e *ResidentMemory) scheduleResidentOutputCutoffLocked(c *residentContext, output *residentOutput) {
	e.mu.Lock()
	e.deadlines.set(residentDeadlineKey{kind: residentOutputDeadline, contextID: c.snapshot.Context.Id, record: output.write.Effect.Id}, 2, output.deadline.Add(2*time.Minute))
	e.mu.Unlock()
	e.wakeDeadline()
}

func (e *ResidentMemory) InspectWrite(ctx context.Context, write *pb.FlowWrite) (*pb.DeliveryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if write == nil || write.Effect == nil {
		return nil, errors.New("output inspection identity required")
	}
	c, err := e.findContext(write.Effect.Context)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !e.residentContextCurrentLocked(c, write.Effect.Context) {
		return nil, errors.New("output context changed")
	}
	output := c.outputs[write.Effect.Id]
	if output == nil || !output.matches(write) {
		return nil, errors.New("output authority mismatch")
	}
	outcome := pb.DeliveryResult_UNKNOWN
	if output.state == residentOutputWritten {
		outcome = pb.DeliveryResult_WRITTEN
	}
	if output.state == residentOutputNotWritten {
		outcome = pb.DeliveryResult_NOT_WRITTEN
	}
	return &pb.DeliveryResult{Effect: proto.Clone(write.Effect).(*pb.StateEffectRef), Outcome: outcome, Recorded: outcome == pb.DeliveryResult_WRITTEN || outcome == pb.DeliveryResult_NOT_WRITTEN}, nil
}

func (e *ResidentMemory) dispatchResident(ctx context.Context, writes []*pb.FlowWrite) {
	for _, write := range writes {
		if e.cfg.WriteCommitted == nil {
			_ = e.Complete(ctx, write, &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN, Error: "socket writer unavailable"})
			e.acknowledgeResidentOutput(write)
			continue
		}
		// The ingress reserves this exact immutable proof, writes at most once,
		// and calls Complete. There is no generic dispatch/retry queue.
		result := e.cfg.WriteCommitted(ctx, write)
		if result == nil {
			continue
		}
		if !result.Recorded && (result.Outcome == pb.DeliveryResult_WRITTEN || result.Outcome == pb.DeliveryResult_NOT_WRITTEN) {
			_ = e.Complete(ctx, write, result)
		}
		e.acknowledgeResidentOutput(write)
	}
}

// A dispatcher return acknowledges this exact internal instruction. Native
// settlement and independent command receipts must already be complete before
// definite output evidence can be forgotten. Uncertain/unacknowledged work stays.
func (e *ResidentMemory) acknowledgeResidentOutput(write *pb.FlowWrite) {
	if write == nil || write.Effect == nil {
		return
	}
	c, err := e.findContext(write.Effect.Context)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !e.residentContextCurrentLocked(c, write.Effect.Context) {
		return
	}
	output := c.outputs[write.Effect.Id]
	if output == nil || !output.matches(write) || !output.settled || output.state != residentOutputWritten && output.state != residentOutputNotWritten {
		return
	}
	if e.releaseResidentOutputLocked(c, output) == nil {
		e.afterResidentOutputReleaseLocked(c)
	}
}

func (e *ResidentMemory) releaseResidentOutputLocked(c *residentContext, output *residentOutput) error {
	if output == nil || c.outputs[output.write.Effect.Id] != output {
		return fmt.Errorf("output release identity mismatch")
	}
	if err := e.refundResidentBudgetLocked(c, output.budget, output.bytes, output.borrowed); err != nil {
		return err
	}
	delete(c.outputs, output.write.Effect.Id)
	command := output.command
	output.command = nil
	releaseResidentCommandLocked(c, command)
	if output.budget != nil && output.budget.open {
		output.budget.outputs++
		c.reservedOutputs++
	}
	e.afterResidentTransactionOutputReleaseLocked(c, output.transaction)
	e.mu.Lock()
	e.deadlines.remove(residentDeadlineKey{kind: residentOutputDeadline, contextID: c.snapshot.Context.Id, record: output.write.Effect.Id})
	e.mu.Unlock()
	return nil
}

// Called after a new context is indexed, while c.mu is still held. Existing
// contexts have their deadline entries installed by ordinary output staging.
func (e *ResidentMemory) scheduleResidentOutputDeadlinesLocked(c *residentContext, writes []*pb.FlowWrite) {
	e.mu.Lock()
	for _, write := range writes {
		if output := c.outputs[write.Effect.Id]; output != nil {
			e.deadlines.set(residentDeadlineKey{kind: residentOutputDeadline, contextID: c.snapshot.Context.Id, record: write.Effect.Id}, 1, output.deadline)
		}
	}
	e.mu.Unlock()
	e.wakeDeadline()
}
