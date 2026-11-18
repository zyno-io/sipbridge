package engine

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"strings"
	"time"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/proto"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/rediscore"
	"sipbridge/internal/selectedmessage"
)

func (e *redisNative) startStandaloneTransaction(ctx context.Context, store *rediscore.Store, in redisUACQualification) (*pb.OperationResult, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(in.Command)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(encoded)
	hint := rediscore.ResultHint{Context: in.Context.Id, Incarnation: in.Context.State.Incarnation, Sequence: 1}
	waiter, err := e.registerResultWaiter(hint)
	if err != nil {
		return &pb.OperationResult{Certainty: pb.OperationResult_NOT_ACCEPTED, Error: "result waiter capacity"}, nil
	}
	defer e.releaseResultWaiter(hint, waiter)
	_, err = admitRedisUACStart(ctx, store, in, true)
	if err != nil {
		return nil, err
	}
	in.Command = nil
	names, err := redisTransactionNames(nativeIdentity(in.Key, "uac-initial", in.Context.State.Authority.PrimaryId, in.Context.State.Incarnation, in.Context.Id, in.Operation)[:32])
	if err != nil {
		return nil, err
	}
	_, _, anchor, err := e.driver.Current()
	if err != nil {
		return nil, err
	}
	for {
		signal := e.resultWaiterSignal(waiter)
		selected, x := store.ObserveApplicationWitness(ctx, hint.Context, hint.Incarnation, in.Lease, "r:context", "r:result-1")
		if x != nil {
			return nil, x
		}
		current, x := decodeNativeContext(selected.View.Records["r:context"])
		if x != nil {
			return nil, x
		}
		result, x := decodeNativeResult(selected.View.Records["r:result-1"])
		if x != nil {
			return nil, x
		}
		if current.Kind != pb.ContextSnapshot_TRANSACTION || !proto.Equal(current.Context, in.Context) || current.CommandSequence != 1 || len(current.InitialProcedures) != 1 || current.InitialProcedures[0] != names.procedure || result.Sequence != 1 || result.Output != names.output || result.Request != "" && result.Request != names.origin || result.Response != "" && (result.Request != names.origin || result.Response != names.response) || result.Certainty != pb.OperationResult_UNKNOWN && result.Certainty != pb.OperationResult_COMPLETED || result.Pending && result.Certainty != pb.OperationResult_UNKNOWN || result.OperationId != in.Operation || result.DecidingLeaseTerm != in.Lease.Term || !hmac.Equal(result.Fingerprint, fingerprint[:]) || result.CutoffMs != in.Deadline.UnixMilli() {
			return nil, rediscore.ErrConflict
		}
		snapshot, x := nativeProjectSnapshot(current, nil, nil, nil)
		if x != nil {
			return nil, x
		}
		cutoff := nativeUDPRetryDeadline(anchor, result.CutoffMs)
		if !result.Pending {
			answer := &pb.OperationResult{Certainty: result.Certainty, Error: result.Error, SipStatus: result.SipStatus, Snapshot: snapshot}
			fields := make([]string, 0, 2)
			if result.Request != "" {
				fields = append(fields, result.Request)
			}
			if result.Response != "" {
				fields = append(fields, result.Response)
			}
			if len(fields) > 0 {
				wires, x := store.ObserveApplicationWitness(ctx, hint.Context, hint.Incarnation, in.Lease, fields...)
				if x != nil {
					return nil, x
				}
				if wires.Identity != selected.Identity || wires.View.Revision != selected.View.Revision || wires.View.Timer != selected.View.Timer {
					continue
				}
				memo, x := selectedmessage.NewMemo(len(fields))
				if x != nil {
					return nil, x
				}
				messages := nativeSelectedMessages{records: wires.View.Records, memo: memo, provenance: "admitted"}
				if result.Request != "" {
					message, x := messages.get(result.Request)
					if x != nil {
						return nil, x
					}
					answer.Request, x = projectNativeMessage(message, true)
					if x != nil {
						return nil, x
					}
				}
				if result.Response != "" {
					message, x := messages.get(result.Response)
					if x != nil {
						return nil, x
					}
					answer.Response, x = projectNativeMessage(message, false)
					if x != nil {
						return nil, x
					}
				}
			}
			if ctx.Err() != nil || !time.Now().Before(in.Deadline) {
				return nil, rediscore.ErrUnknown
			}
			return answer, nil
		}
		if !time.Now().Before(cutoff) {
			return &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Error: "transaction outcome unavailable before deadline", Snapshot: snapshot}, nil
		}
		timer := time.NewTimer(time.Until(cutoff))
		select {
		case <-signal:
		case <-timer.C:
		case <-ctx.Done():
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// Validation happens before the first socket attempt, including recovered work.
func (e *redisNative) readyStandaloneTransaction(ctx context.Context, store *rediscore.Store, ready rediscore.ReadyOutput, metadata *nativepb.Output) error {
	if !validReadyStandaloneOutput(metadata, ready.MetadataField) {
		return errNativeRecord
	}
	s, err := selectRedisTransaction(ctx, store, ready.Ref.Context, ready.Ref.Incarnation, strings.TrimPrefix(ready.MetadataField, "r:transaction-output-"))
	if err != nil {
		return err
	}
	if !proto.Equal(s.output, metadata) || !bytes.Equal(s.selected.View.Records[ready.MetadataField], ready.Metadata) {
		return rediscore.ErrConflict
	}
	result := e.cfg.WriteCommitted(ctx, metadata.Write)
	if result == nil || !proto.Equal(result.Effect, metadata.Write.Effect) {
		return rediscore.ErrUnknown
	}
	if !result.Recorded {
		if err = completeNativeOutput(ctx, store, metadata.Write, result, rediscore.TimerAssignment{Worker: e.worker, Epoch: e.cfg.Process.Epoch, Term: e.cfg.Process.Epoch}); err != nil {
			return err
		}
	}
	if result.Outcome != pb.DeliveryResult_WRITTEN && result.Outcome != pb.DeliveryResult_NOT_WRITTEN {
		return nil
	}
	if result.Outcome == pb.DeliveryResult_WRITTEN && metadata.Write.Flow.Transport == "udp" && e.udp != nil {
		_ = e.armRedisStandaloneUDP(ctx, store, ready.Ref, metadata)
	}
	return acknowledgeRedisStandaloneOutput(ctx, store, metadata.Write, time.Now().Add(2*time.Second))
}

func (e *redisNative) armRedisStandaloneUDP(ctx context.Context, store *rediscore.Store, ref rediscore.OutputRef, expected *nativepb.Output) error {
	if e.udp == nil || !ref.RequireApplication || !validReadyStandaloneOutput(expected, "r:transaction-output-"+strings.TrimPrefix(expected.GetProcedure(), "r:transaction-procedure-")) || expected.Write.Flow.Transport != "udp" || !sameProcess(expected.Write.Flow.Ingress, e.cfg.Process) {
		return errNativeRecord
	}
	proof, err := store.InspectOutput(ctx, ref)
	if err != nil {
		return err
	}
	if proof.State != rediscore.OutputWritten || !proof.Settled {
		return errNativeRecord
	}
	captured := time.Now()
	s, err := selectRedisTransaction(ctx, store, ref.Context, ref.Incarnation, strings.TrimPrefix(proof.MetadataField, "r:transaction-output-"))
	if err != nil {
		return err
	}
	if err = redisTransactionProof(s, &nativeOutputCapture{ref: ref, proof: proof, metadata: expected}); err != nil {
		return err
	}
	tx, err := s.attach(ctx, store, nil)
	if err != nil {
		return err
	}
	if tx.Origin.WireHash() != proof.WireHash || tx.Phase != portable.Trying && tx.Phase != portable.Proceeding {
		return errNativeRecord
	}
	return e.udp.Arm(nativeUDPRetrySeed{Key: nativeUDPRetryKey{Context: ref.Context, Incarnation: ref.Incarnation, Procedure: s.names.procedure}, Attempt: ref, Proof: proof, Captured: captured, Transaction: tx, Flow: expected.Write.Flow})
}

func releaseExpiredRedisStandalone(ctx context.Context, store *rediscore.Store, item rediscore.ExpiredApplicationContext, current *nativepb.Context, deadline time.Time) error {
	if current == nil || len(current.InitialProcedures) != 1 {
		return errNativeRecord
	}
	s, err := selectRedisTransaction(ctx, store, item.Identity.Context, item.Identity.Incarnation, strings.TrimPrefix(current.InitialProcedures[0], "r:transaction-procedure-"))
	if err != nil {
		return err
	}
	if s.selected.Identity != item.Identity || s.selected.View.Revision != item.Revision || current.ApplicationLeaseTerm != item.Identity.Term || current.Revision >= 1<<53-1 {
		return rediscore.ErrConflict
	}
	current.Revision++
	data, err := encodeNativeContext(current)
	if err != nil {
		return err
	}
	_, err = store.Commit(ctx, rediscore.Publication{Identity: item.Identity, Revision: item.Revision, Deadline: deadline, SpendReservation: true, ReleaseApplication: true, Changes: []rediscore.Change{{Field: "r:context", Value: data}}})
	return err
}
