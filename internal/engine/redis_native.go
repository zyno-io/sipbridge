package engine

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/sip/portable"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/rediscore"
)

// redisNative integrates selected operations for qualification. It is not wired
// to the serving selector until the complete SIP surface is qualified. No native
// records, controller leases or per-context timers are retained in this value.
type redisNative struct {
	cfg               Config
	control           *control.Server
	connection        *rediscore.Connection
	driver            *rediscore.Driver
	worker            string
	ctx               context.Context
	cancel            context.CancelFunc
	mu                sync.Mutex
	closed            bool
	work              chan struct{}
	eventWork         chan struct{}
	events            map[redisNativeEventKey]struct{}
	resultWaiters     map[rediscore.ResultHint]*nativeResultWaiter
	resultWaiterCount int
	udp               *nativeUDPRetry
	wg                sync.WaitGroup
}

func newRedisNative(parent context.Context, cfg Config, connection *rediscore.Connection, server *control.Server) (*redisNative, error) {
	if parent == nil || connection == nil || server == nil || !validSendProcess(cfg.Process) || len(cfg.StateIdentityKey) < 32 || len(cfg.StateIdentityKey) > 1024 || cfg.WriteCommitted == nil {
		return nil, errors.New("native Redis identity, connection, control and writer required")
	}
	for _, transport := range []string{"tls", "tcp", "udp"} {
		if _, _, err := advertisement(cfg, transport); err != nil {
			return nil, err
		}
	}
	worker, err := nativeWorkerReference(cfg.Process)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	e := &redisNative{cfg: cfg, control: server, connection: connection, worker: worker, ctx: ctx, cancel: cancel,
		work: make(chan struct{}, 48), eventWork: make(chan struct{}, 8), events: make(map[redisNativeEventKey]struct{}, 8), resultWaiters: make(map[rediscore.ResultHint]*nativeResultWaiter)}
	e.cfg.Process = proto.Clone(cfg.Process).(*pb.ProcessRef)
	e.cfg.StateIdentityKey = append([]byte(nil), cfg.StateIdentityKey...)
	if cfg.WriteNativeRetry != nil {
		e.udp, err = newNativeUDPRetry(ctx, nativeUDPRetryLimits{Entries: 1024, Hints: 2048, Writers: 8, Bytes: 16 << 20}, func() (context.Context, time.Time, error) {
			if e.driver == nil {
				return nil, time.Time{}, rediscore.ErrAuthority
			}
			_, lease, until, leaseErr := e.driver.Current()
			return lease, until, leaseErr
		}, cfg.WriteNativeRetry)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	e.driver, err = rediscore.NewDriver(rediscore.DriverConfig{Connection: connection, Worker: worker, Epoch: cfg.Process.Epoch,
		Due: e.due, ReadyOutput: e.readyOutput, OwnerExpired: e.expireRedisOwner, ResultHint: e.resultHint, UDPHint: e.udpHint,
		NativeRecordError: errNativeRecord, OnDiagnostic: cfg.OnNativeDriverDiagnostic})
	if err != nil {
		if e.udp != nil {
			e.udp.Close()
		}
		cancel()
		return nil, err
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		_ = e.driver.Run(ctx)
	}()
	return e, nil
}

func (e *redisNative) Close() {
	e.mu.Lock()
	e.closed = true
	e.cancel()
	e.mu.Unlock()
	if e.udp != nil {
		e.udp.Close()
	}
	_ = e.driver.Close()
	e.wg.Wait()
}

func (e *redisNative) Ready() error                       { return e.driver.Ready() }
func (e *redisNative) StateAuthority() *pb.StateAuthority { return e.connection.Authority() }

// tryUACResponseCompletion is private until the complete ingress surface is qualified.
func (e *redisNative) tryUACResponseCompletion(parent context.Context, flow *pb.FlowRef, message portable.Message, selectors []string) (bool, error) {
	facts, err := message.Facts()
	if err != nil {
		return false, err
	}
	if facts.IsRequest || facts.CSeqMethod != sip.INVITE && facts.CSeqMethod != sip.CANCEL && !redisApplicationClientMethod(string(facts.CSeqMethod)) && !redisStandaloneMethod(string(facts.CSeqMethod)) {
		return false, nil
	}
	if !validPolicyFlow(flow) || !sameProcess(flow.Ingress, e.cfg.Process) {
		return true, errNativeRecord
	}
	deadline := minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
	ctx, cancel, store, err := e.operation(parent, deadline)
	if err != nil {
		return true, err
	}
	defer cancel()
	executor := rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}
	for attempts := 0; attempts < 8; attempts++ {
		var handled bool
		if facts.CSeqMethod == sip.INVITE {
			ids, selectErr := nativeSelectorIDs(message, false, false)
			if selectErr != nil || len(ids) == 0 || ids[0].Kind != rediscore.SelectorClientTransaction {
				return true, errNativeRecord
			}
			selected, selectErr := store.LookupSelector(ctx, ids[0], "r:context", "r:result-1")
			if errors.Is(selectErr, rediscore.ErrAbsent) {
				return false, nil
			}
			if selectErr != nil {
				return true, selectErr
			}
			procedure, selectErr := decodeNativeProcedure(selected.View.Records[selected.Record])
			if selectErr != nil {
				return true, selectErr
			}
			if procedure.Metadata.SuccessfulAck != nil && facts.Status >= 200 && facts.Status < 300 {
				current, decodeErr := decodeNativeContext(selected.View.Records["r:context"])
				if decodeErr != nil {
					return true, decodeErr
				}
				liveStore, _, leaseUntil, liveErr := e.driver.Current()
				if liveErr != nil || liveStore != store {
					return true, rediscore.ErrAuthority
				}
				handled, err = receiveRedisRetainedSuccessfulACK(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, procedure, flow, message, e.cfg.StateIdentityKey, executor, leaseUntil, deadline)
				if handled || err != nil {
					if errors.Is(err, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) {
						continue
					}
					return handled, err
				}
			}
			if strings.HasPrefix(selected.Record, "r:app-invite-") && procedure.Native.Phase == portable.Accepted && facts.Status >= 200 && facts.Status < 300 {
				current, decodeErr := decodeNativeContext(selected.View.Records["r:context"])
				if decodeErr != nil {
					return true, decodeErr
				}
				liveStore, _, leaseUntil, liveErr := e.driver.Current()
				if liveErr != nil || liveStore != store {
					return true, rediscore.ErrAuthority
				}
				handled, err = replayRedisCurrentInviteACK(ctx, store, selected, current, procedure, flow, message, e.cfg.StateIdentityKey, executor, leaseUntil, deadline)
				if errors.Is(err, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) {
					continue
				}
				return handled, err
			}
			if strings.HasPrefix(selected.Record, "r:app-invite-") && facts.Status >= 300 && (procedure.Native.Phase == portable.Completed || procedure.Native.Phase == portable.Terminated) {
				current, decodeErr := decodeNativeContext(selected.View.Records["r:context"])
				if decodeErr != nil || current.Context == nil || current.Context.State == nil || current.Context.Id != selected.Identity.Context || current.Context.State.Incarnation != selected.Identity.Incarnation || !slices.Contains(current.NoninitialProcedures, selected.Record) || procedure.Metadata.CommandResult == "" {
					return true, errNativeRecord
				}
				receiptView, readErr := store.Read(ctx, selected.Identity, procedure.Metadata.CommandResult)
				if readErr != nil {
					return true, readErr
				}
				if receiptView.Revision != selected.View.Revision || receiptView.Timer != selected.View.Timer {
					continue
				}
				receipt, decodeErr := decodeNativeResult(receiptView.Records[procedure.Metadata.CommandResult])
				if decodeErr != nil || receipt.Sequence == 0 || receipt.Sequence > current.CommandSequence || procedure.Metadata.CommandResult != "r:result-"+strconv.FormatUint(receipt.Sequence, 10) {
					return true, errNativeRecord
				}
				liveStore, _, leaseUntil, liveErr := e.driver.Current()
				if liveErr != nil || liveStore != store {
					return true, rediscore.ErrAuthority
				}
				handled, err = replayRedisPreviousInviteFailure(ctx, store, selected.Identity, selected.View.Revision, selected.View.Timer, current, selected.Record, procedure, receipt.OperationId, message, flow, e.cfg.StateIdentityKey, executor, leaseUntil, deadline)
				if errors.Is(err, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) {
					continue
				}
				return handled, err
			}
			handled, err = completeRedisApplicationInviteResponse(ctx, store, flow, message, e.cfg.StateIdentityKey, deadline, selected)
			if handled || err != nil {
				if errors.Is(err, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) {
					continue
				}
				return handled, err
			}
			if facts.Status == 401 || facts.Status == 407 {
				var ticket *redisUACDigestTicket
				ticket, handled, err = publishRedisUACDigestChallenge(ctx, store, flow, message, e.cfg.StateIdentityKey, executor, deadline, selected)
				if err == nil && ticket != nil {
					return true, e.completeRedisUACDigest(ctx, store, ticket, executor, deadline)
				}
				if handled || err != nil {
					if !errors.Is(err, rediscore.ErrConflict) || ctx.Err() != nil || !time.Now().Before(deadline) {
						return handled, err
					}
					continue
				}
			}
			handled, err = receiveRedisUACProvisional(ctx, store, flow, message, e.cfg.StateIdentityKey, executor, deadline, selected)
		} else if facts.CSeqMethod == sip.CANCEL {
			handled, err = receiveRedisClientCancelResponse(ctx, store, flow, message, e.cfg.StateIdentityKey, executor, deadline)
		} else {
			if redisStandaloneMethod(string(facts.CSeqMethod)) {
				handled, err = receiveRedisStandaloneResponse(ctx, store, flow, message, executor, deadline)
				if handled || err != nil {
					if errors.Is(err, rediscore.ErrConflict) && ctx.Err() == nil && time.Now().Before(deadline) {
						continue
					}
					return handled, err
				}
			}
			handled, err = completeRedisCleanupByeResponse(ctx, store, flow, message, selectors, deadline)
		}
		if !errors.Is(err, rediscore.ErrConflict) || ctx.Err() != nil || !time.Now().Before(deadline) {
			return handled, err
		}
	}
	return true, rediscore.ErrConflict
}

// operation attaches a caller's original deadline to this live physical process
// binding. Healthy lease renewals do not create a new operation or extend it.
func (e *redisNative) operation(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc, *rediscore.Store, error) {
	store, lease, _, err := e.driver.Current()
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	stop := context.AfterFunc(lease, cancel)
	if lease.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }, store, nil
}

func (e *redisNative) ApplicationLease(parent context.Context, application *pb.ProcessRef, term string) (*pb.ApplicationLease, error) {
	id, err := nativeApplicationReference(e.cfg.StateIdentityKey, application)
	if err != nil {
		return nil, err
	}
	ctx, cancel, store, err := e.operation(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	if err != nil {
		return nil, err
	}
	defer cancel()
	var lease rediscore.ApplicationLease
	if term == "" {
		lease, err = store.AcquireApplication(ctx, id, uuid.NewString())
	} else {
		lease, err = store.RenewApplication(ctx, rediscore.ApplicationLeaseRef{Application: id, Term: term})
	}
	if err != nil {
		if term != "" && !errors.Is(err, rediscore.ErrUnknown) && (errors.Is(err, rediscore.ErrAuthority) || errors.Is(err, rediscore.ErrExpired) || errors.Is(err, rediscore.ErrAbsent) || errors.Is(err, rediscore.ErrCorrupt)) {
			return nil, errors.Join(control.ErrApplicationLeaseFenced, err)
		}
		return nil, err
	}
	return &pb.ApplicationLease{Application: proto.Clone(application).(*pb.ProcessRef), Authority: e.StateAuthority(), Term: lease.Term,
		CheckedAtMs: uint64(lease.CheckedAtMS), ExpiresAtMs: uint64(lease.ExpiresAtMS), LeaseLifetimeMs: uint32(rediscore.ApplicationLeaseLifetime.Milliseconds())}, nil
}

func (e *redisNative) ControllerSession(ctx context.Context, peer *control.Peer, hello *pb.SessionHello) (*pb.ApplicationLease, error) {
	if peer == nil || hello == nil || hello.Recovery != nil {
		return nil, errors.New("native Redis controller recovery is unavailable")
	}
	return e.ApplicationLease(ctx, peer.Process(), "")
}

func (e *redisNative) ControllerRenew(ctx context.Context, peer *control.Peer) (*pb.ApplicationLease, error) {
	if peer == nil || peer.ApplicationLease() == nil || peer.ApplicationLease().Term == "" {
		return nil, control.ErrApplicationLeaseFenced
	}
	return e.ApplicationLease(ctx, peer.Application(), peer.ApplicationLease().Term)
}

func (e *redisNative) controllerWitness(peer *control.Peer) (rediscore.ApplicationLeaseRef, error) {
	if peer == nil || !peer.HasCapability(control.RequestDecisionCapability) {
		return rediscore.ApplicationLeaseRef{}, control.ErrApplicationLeaseFenced
	}
	lease := peer.ApplicationLease()
	if lease == nil || lease.Term == "" || !sameProcess(lease.Application, peer.Application()) || !proto.Equal(lease.Authority, e.StateAuthority()) {
		return rediscore.ApplicationLeaseRef{}, control.ErrApplicationLeaseFenced
	}
	id, err := nativeApplicationReference(e.cfg.StateIdentityKey, peer.Application())
	return rediscore.ApplicationLeaseRef{Application: id, Term: lease.Term}, err
}

func (e *redisNative) ControllerReady(parent context.Context, peer *control.Peer) error {
	witness, err := e.controllerWitness(peer)
	if err != nil {
		return err
	}
	ctx, cancel, store, err := e.operation(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	if err != nil {
		return err
	}
	defer cancel()
	current, err := store.ObserveApplication(ctx, witness.Application)
	if err != nil {
		return err
	}
	if current.Term != witness.Term {
		return control.ErrApplicationLeaseFenced
	}
	return nil
}

func (e *redisNative) Reserve(parent context.Context, write *pb.FlowWrite) (*CommittedWrite, error) {
	ctx, cancel, store, err := e.operation(parent, deadlineOr(parent, time.Now().Add(2*time.Second)))
	if err != nil {
		return nil, err
	}
	defer cancel()
	return (nativeRedisWriter{store: store}).Reserve(ctx, write)
}

// The physical fact outlives the write's request/lease cancellation. A fenced
// binding may record only that exact outcome, never resume native work.
func (e *redisNative) Complete(parent context.Context, write *pb.FlowWrite, result *pb.DeliveryResult) error {
	if e == nil || e.ctx == nil || e.driver == nil || e.connection == nil {
		return errNativeRecord
	}
	proof, cancel := context.WithTimeout(e.ctx, 2*time.Second)
	defer cancel()
	store, lease, _, bindingErr := e.driver.Current()
	if bindingErr != nil {
		var err error
		store, err = e.connection.Refresh(proof)
		if err != nil {
			return err
		}
	}
	if err := (nativeRedisWriter{store: store}).Complete(proof, write, result); err != nil {
		return err
	}
	if bindingErr != nil || parent.Err() != nil || lease.Err() != nil {
		return nil
	}
	_, currentLease, _, currentErr := e.driver.Current()
	if currentErr != nil || currentLease != lease {
		return nil
	}
	ctx, finish := context.WithDeadline(parent, minTime(deadlineOr(parent, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
	defer finish()
	stop := context.AfterFunc(lease, finish)
	defer stop()
	return settleNativeOutput(ctx, store, write, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, e.cfg.StateIdentityKey)
}

func completeNativeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, result *pb.DeliveryResult, executor rediscore.TimerAssignment, key ...[]byte) error {
	if err := (nativeRedisWriter{store: store}).Complete(ctx, write, result); err != nil {
		return err
	}
	return settleNativeOutput(ctx, store, write, executor, key...)
}

func settleNativeOutput(ctx context.Context, store *rediscore.Store, write *pb.FlowWrite, executor rediscore.TimerAssignment, key ...[]byte) error {
	ref, err := nativeOutputRef(write)
	if err != nil {
		return err
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	metadata, err := decodeNativeOutput(proof.Metadata)
	if err != nil || !proto.Equal(metadata.Write, write) {
		return errors.New("invalid selected physical proof")
	}
	if strings.HasPrefix(proof.MetadataField, "r:transaction-output-") {
		return settleRedisStandaloneOutput(ctx, store, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
	}
	if metadata.Method == "REGISTER" || metadata.Method == "SUBSCRIBE" {
		return errNativeRecord
	}
	if nativeLocalRefusalOutput(metadata, proof.MetadataField) {
		return settleRedisRegisterOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata})
	}
	switch metadata.Method {
	case "CANCEL":
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}
		deadline := minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
		if strings.HasPrefix(proof.MetadataField, "r:cancel-client-output-") {
			return settleRedisClientCancelOutput(ctx, store, write, capture, executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:call-cancel-output-") {
			return settleRedisCancelOutput(ctx, store, write, capture, executor, deadline)
		}
		return errors.New("unsupported selected CANCEL output")
	case "REGISTER":
		return errNativeRecord
	case "SUBSCRIBE", "NOTIFY":
		if metadata.Method == "NOTIFY" && strings.HasPrefix(proof.MetadataField, "r:app-client-output-") {
			return settleRedisApplicationByeOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
		}
		if metadata.Method == "NOTIFY" && strings.HasPrefix(proof.MetadataField, "r:dialog-output-") {
			return settleRedisInitialInviteOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
		}
		return errNativeRecord
	case "BYE":
		if metadata.Status == 0 && strings.HasPrefix(proof.MetadataField, "r:app-bye-output-") {
			return settleRedisApplicationByeOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
		}
		if metadata.Status == 0 && strings.HasPrefix(proof.MetadataField, "r:call-cleanup-output-") {
			return settleRedisCleanupByeOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
		}
		if metadata.Status == 200 {
			return settleRedisCallByeOutputAndRedrive(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
		}
		return errors.New("unsupported selected BYE output")
	case "INVITE":
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}
		deadline := minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
		if strings.HasPrefix(proof.MetadataField, "r:app-invite-output-") {
			return settleRedisApplicationByeOutput(ctx, store, write, capture, executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:call-cancel-invite-output-") {
			return settleRedisCancelOutput(ctx, store, write, capture, executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:dialog-output-") {
			return settleRedisInitialInviteOutput(ctx, store, write, capture, executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:uac-output-") {
			return settleRedisUACInitialOutput(ctx, store, write, capture, executor, deadline)
		}
		trying, classifyErr := nativeReadyInviteTrying(metadata, proof.MetadataField)
		if classifyErr != nil {
			return classifyErr
		}
		if trying {
			return settleRedisInviteTryingOutput(ctx, store, write, capture, executor, deadline)
		}
		return settleRedisInitialInviteOutput(ctx, store, write, capture, executor, deadline)
	case "INFO", "REFER", "UPDATE":
		if strings.HasPrefix(proof.MetadataField, "r:app-client-output-") {
			return settleRedisApplicationByeOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
		}
		if !strings.HasPrefix(proof.MetadataField, "r:dialog-output-") {
			return errors.New("unsupported selected dialog response")
		}
		return settleRedisInitialInviteOutput(ctx, store, write, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, executor, minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second)))
	case "ACK":
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}
		deadline := minTime(deadlineOr(ctx, time.Now().Add(2*time.Second)), time.Now().Add(2*time.Second))
		if strings.HasPrefix(proof.MetadataField, "r:retained-ack-output-") {
			return settleRedisRetainedACK(ctx, store, write, capture, executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:app-invite-ack-output-") && len(key) == 1 {
			return settleRedisApplicationInviteACK(ctx, store, write, capture, key[0], executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:app-ack-output-") && len(key) == 1 {
			return settleRedisApplicationACK(ctx, store, write, capture, key[0], executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:uac-ack-output-") {
			return settleRedisUACFailureACK(ctx, store, write, capture, executor, deadline)
		}
		if strings.HasPrefix(proof.MetadataField, "r:uac-dialog-ack-output-") && len(key) == 1 {
			return settleRedisUACSuccessACK(ctx, store, write, capture, key[0], executor, deadline)
		}
		return errors.New("unsupported selected ACK output")
	default:
		return errors.New("unsupported selected physical method")
	}
}

func (e *redisNative) readyOutput(ctx context.Context, store *rediscore.Store, ready rediscore.ReadyOutput) error {
	metadata, err := decodeNativeOutput(ready.Metadata)
	if err != nil {
		return err
	}
	if strings.HasPrefix(ready.MetadataField, "r:transaction-output-") {
		ref, x := nativeOutputRef(metadata.Write)
		if x != nil || ref != ready.Ref || metadata.Write.Permit.Authorization.DeadlineMs != uint64(ready.DeadlineMS) || !sameProcess(metadata.Write.Flow.Ingress, e.cfg.Process) {
			return errNativeRecord
		}
		return e.readyStandaloneTransaction(ctx, store, ready, metadata)
	}
	if metadata.Method == "REGISTER" || metadata.Method == "SUBSCRIBE" {
		return errNativeRecord
	}
	localRefusal := nativeLocalRefusalOutput(metadata, ready.MetadataField)
	if ready.MetadataField == "r:output-0" && !nativeZeroServerOutput(metadata, ready.MetadataField) {
		return errNativeRecord
	}
	ref, err := nativeOutputRef(metadata.Write)
	if err != nil || ref != ready.Ref || metadata.Write.Permit.Authorization.DeadlineMs != uint64(ready.DeadlineMS) || !sameProcess(metadata.Write.Flow.Ingress, e.cfg.Process) || metadata.Method != "REGISTER" && metadata.Method != "SUBSCRIBE" && metadata.Method != "NOTIFY" && metadata.Method != "INVITE" && metadata.Method != "INFO" && metadata.Method != "REFER" && metadata.Method != "UPDATE" && metadata.Method != "ACK" && metadata.Method != "BYE" && metadata.Method != "CANCEL" && !localRefusal {
		return errors.New("invalid selected native output")
	}
	if !localRefusal && ((metadata.Method == "INFO" || metadata.Method == "REFER" || metadata.Method == "UPDATE") && !strings.HasPrefix(ready.MetadataField, "r:dialog-output-") && !strings.HasPrefix(ready.MetadataField, "r:app-client-output-") || metadata.Method == "ACK" && !strings.HasPrefix(ready.MetadataField, "r:uac-ack-output-") && !strings.HasPrefix(ready.MetadataField, "r:uac-dialog-ack-output-") && !strings.HasPrefix(ready.MetadataField, "r:app-ack-output-") && !strings.HasPrefix(ready.MetadataField, "r:app-invite-ack-output-") && !strings.HasPrefix(ready.MetadataField, "r:retained-ack-output-") || metadata.Method == "BYE" && metadata.Status != 200 && !(metadata.Status == 0 && (strings.HasPrefix(ready.MetadataField, "r:call-cleanup-output-") || strings.HasPrefix(ready.MetadataField, "r:app-bye-output-")))) {
		return errors.New("unsupported selected native output")
	}
	if strings.HasPrefix(ready.MetadataField, "r:app-ack-output-") && !validReadyApplicationACK(metadata, ready.MetadataField) {
		return errors.New("invalid selected application ACK output")
	}
	if strings.HasPrefix(ready.MetadataField, "r:retained-ack-output-") {
		if !validReadyRetainedACK(metadata, ready.MetadataField) {
			return errNativeRecord
		}
		if _, err = inspectRedisRetainedACK(ctx, store, metadata.Write, nil); err != nil {
			return err
		}
	}
	if strings.HasPrefix(ready.MetadataField, "r:app-invite-output-") && !validReadyApplicationInvite(metadata, ready.MetadataField) || strings.HasPrefix(ready.MetadataField, "r:app-invite-ack-output-") && !validReadyApplicationInviteACK(metadata, ready.MetadataField) {
		return errors.New("invalid selected re-INVITE output")
	}
	if (strings.HasPrefix(ready.MetadataField, "r:app-bye-output-") || strings.HasPrefix(ready.MetadataField, "r:app-client-output-")) && !validReadyApplicationBye(metadata, ready.MetadataField) {
		return errors.New("invalid selected application BYE output")
	}
	if !localRefusal && (metadata.Method == "CANCEL" || strings.HasPrefix(ready.MetadataField, "r:call-cancel-invite-output-")) {
		if strings.HasPrefix(ready.MetadataField, "r:cancel-client-output-") {
			if _, err = inspectRedisClientCancelOutput(ctx, store, metadata.Write, nil); err != nil {
				return err
			}
		} else if strings.HasPrefix(ready.MetadataField, "r:call-cancel-output-") || strings.HasPrefix(ready.MetadataField, "r:call-cancel-invite-output-") {
			if _, err = inspectRedisCancelOutput(ctx, store, metadata.Write, nil); err != nil {
				return err
			}
		} else {
			return errNativeRecord
		}
	}
	if ready.MetadataField == "r:output-0" {
		if err = validateRedisInitialRefusalOutput(ctx, store, ready, metadata); err != nil {
			return err
		}
	}
	trying := false
	if metadata.Method == "INVITE" && !strings.HasPrefix(ready.MetadataField, "r:uac-output-") && !strings.HasPrefix(ready.MetadataField, "r:app-invite-output-") && !strings.HasPrefix(ready.MetadataField, "r:dialog-output-") && !strings.HasPrefix(ready.MetadataField, "r:call-cancel-invite-output-") {
		trying, err = nativeReadyInviteTrying(metadata, ready.MetadataField)
		if err != nil {
			return err
		}
	}
	if metadata.Method == "INVITE" && metadata.Status == 503 && strings.HasPrefix(ready.MetadataField, "r:invite-output-") && strings.HasSuffix(ready.MetadataField, "-0") {
		if _, err = inspectRedisInitialInviteOutput(ctx, store, metadata.Write, nil); err != nil {
			return err
		}
	}
	result := e.cfg.WriteCommitted(ctx, metadata.Write)
	if result == nil || !proto.Equal(result.Effect, metadata.Write.Effect) {
		return rediscore.ErrUnknown
	}
	if !result.Recorded {
		if err = completeNativeOutput(ctx, store, metadata.Write, result, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, e.cfg.StateIdentityKey); err != nil {
			return err
		}
	}
	if result.Outcome != pb.DeliveryResult_WRITTEN && result.Outcome != pb.DeliveryResult_NOT_WRITTEN {
		return nil
	}
	deadline := time.Now().Add(2 * time.Second)
	if metadata.Method == "CANCEL" && metadata.Status == 0 && metadata.Write.Flow.Transport == "udp" && result.Outcome == pb.DeliveryResult_WRITTEN && e.udp != nil {
		_ = e.armRedisClientCancelUDP(ctx, store, ref, metadata)
	}
	if metadata.Method == "INVITE" && metadata.Status == 0 && metadata.Write.Flow.Transport == "udp" && result.Outcome == pb.DeliveryResult_WRITTEN && e.udp != nil {
		if strings.HasPrefix(ready.MetadataField, "r:app-invite-output-") {
			_ = e.armRedisApplicationInviteUDP(ctx, store, ref, metadata)
		} else {
			_ = e.armRedisInitialUACUDP(ctx, store, ref, metadata)
		}
	}

	if metadata.Status == 0 && (metadata.Method == "BYE" || strings.HasPrefix(ready.MetadataField, "r:app-client-output-")) && result.Outcome == pb.DeliveryResult_WRITTEN && metadata.Write.Flow.Transport == "udp" && e.udp != nil {
		_ = e.armRedisCleanupByeUDP(ctx, store, ref, metadata)
	}
	if metadata.Method == "INVITE" && !trying && metadata.Status >= 200 && result.Outcome == pb.DeliveryResult_WRITTEN && e.udp != nil {
		// Derivative admission is optional and never changes the recorded proof.
		_ = e.armRedisInitialInviteRetry(ctx, store, ref, metadata)
	}
	if localRefusal {
		return acknowledgeRedisRegisterOutput(ctx, store, metadata.Write, deadline)
	}
	switch metadata.Method {
	case "CANCEL":
		if strings.HasPrefix(ready.MetadataField, "r:cancel-client-output-") {
			return acknowledgeRedisClientCancelOutput(ctx, store, metadata.Write, deadline)
		}
		return acknowledgeRedisCancelOutput(ctx, store, metadata.Write, deadline)
	case "REGISTER":
		return acknowledgeRedisRegisterOutput(ctx, store, metadata.Write, deadline)
	case "SUBSCRIBE":
		return errNativeRecord
	case "BYE":
		if metadata.Status == 0 && strings.HasPrefix(ready.MetadataField, "r:app-bye-output-") {
			return acknowledgeRedisApplicationByeOutput(ctx, store, metadata.Write, deadline)
		}
		if metadata.Status == 0 && strings.HasPrefix(ready.MetadataField, "r:call-cleanup-output-") {
			return acknowledgeRedisCleanupByeOutput(ctx, store, metadata.Write, deadline)
		}
		return acknowledgeRedisCallByeOutput(ctx, store, metadata.Write, deadline)
	case "INVITE":
		if strings.HasPrefix(ready.MetadataField, "r:app-invite-output-") {
			return acknowledgeRedisApplicationByeOutput(ctx, store, metadata.Write, deadline)
		}
		if strings.HasPrefix(ready.MetadataField, "r:call-cancel-invite-output-") {
			return acknowledgeRedisCancelOutput(ctx, store, metadata.Write, deadline)
		}
		if strings.HasPrefix(ready.MetadataField, "r:uac-output-") {
			proof, inspectErr := store.InspectOutput(ctx, ref)
			if inspectErr != nil {
				return inspectErr
			}
			return acknowledgeRedisUACInitialOutput(ctx, store, &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}, deadline)
		}
		if strings.HasPrefix(ready.MetadataField, "r:dialog-output-") {
			return acknowledgeRedisInitialInviteOutput(ctx, store, metadata.Write)
		}
		if trying {
			return acknowledgeRedisInviteTryingOutput(ctx, store, metadata.Write, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}, deadline)
		}
		return acknowledgeRedisInitialInviteOutput(ctx, store, metadata.Write)
	case "INFO", "REFER", "UPDATE":
		if strings.HasPrefix(ready.MetadataField, "r:app-client-output-") {
			return acknowledgeRedisApplicationByeOutput(ctx, store, metadata.Write, deadline)
		}
		return acknowledgeRedisInitialInviteOutput(ctx, store, metadata.Write)
	case "NOTIFY":
		if strings.HasPrefix(ready.MetadataField, "r:app-client-output-") {
			return acknowledgeRedisApplicationByeOutput(ctx, store, metadata.Write, deadline)
		}
		if strings.HasPrefix(ready.MetadataField, "r:dialog-output-") {
			return acknowledgeRedisInitialInviteOutput(ctx, store, metadata.Write)
		}
		return errNativeRecord
	case "ACK":
		if strings.HasPrefix(ready.MetadataField, "r:retained-ack-output-") {
			return acknowledgeRedisRetainedACK(ctx, store, metadata.Write, nil)
		}
		if strings.HasPrefix(ready.MetadataField, "r:app-invite-ack-output-") {
			return acknowledgeRedisApplicationInviteACK(ctx, store, metadata.Write, deadline)
		}
		proof, inspectErr := store.InspectOutput(ctx, ref)
		if inspectErr != nil {
			return inspectErr
		}
		capture := &nativeOutputCapture{ref: ref, proof: proof, metadata: metadata}
		if strings.HasPrefix(ready.MetadataField, "r:app-ack-output-") {
			return acknowledgeRedisApplicationACK(ctx, store, metadata.Write, deadline)
		}
		if strings.HasPrefix(ready.MetadataField, "r:uac-ack-output-") {
			return acknowledgeRedisUACFailureACK(ctx, store, metadata.Write, capture)
		}
		if strings.HasPrefix(ready.MetadataField, "r:uac-dialog-ack-output-") {
			return acknowledgeRedisUACSuccessACK(ctx, store, metadata.Write, capture)
		}
		return errors.New("unsupported selected ACK output")
	default:
		return errNativeRecord
	}
}
