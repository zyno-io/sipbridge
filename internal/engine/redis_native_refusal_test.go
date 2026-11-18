package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/nativepb"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

// The socket recorder uses the actual reserve/completion protocol. No
// controller is attached and no application owner is created for a refusal.
func TestRedisNativeInitialRefusalHasOnlyNativeDuties(t *testing.T) {
	testRedisNativeRefusalDuties(t, []string{"CANCEL", "BYE", "MESSAGE", "INVITE", "BYE-tagged", "INVITE-tagged", "INVITE-tagged-no-ACK"})
}

func TestRedisNativeTaggedRefusalMatchedCANCEL(t *testing.T) {
	testRedisNativeRefusalDuties(t, []string{"INVITE-tagged-CANCEL"})
}

func testRedisNativeRefusalDuties(t *testing.T, cases []string) {
	address := os.Getenv("SIPBRIDGE_CORE_TEST_REDIS")
	if address == "" {
		t.Skip("isolated Redis fixture required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	root := "sipstate:test:refusal-" + uuid.NewString() + ":"
	key := bytes.Repeat([]byte{7}, 32)
	process := &pb.ProcessRef{InstanceId: "refusal", Epoch: uuid.NewString()}
	connection, err := rediscore.NewConnection(rediscore.ConnectionConfig{Redis: redisclient.Options{Address: address, IOTimeout: time.Second}, Profile: rediscore.ProfileConfig{Root: root, Service: "test", Generation: "one", IdentityKey: key, RecordBudget: 8 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := redis.NewClient(&redis.Options{Addr: address, MaxRetries: -1})
	defer func() {
		// Fixture-only deletion is confined to this random root; no serving
		// migration, reaper or namespace cleanup is introduced.
		var cursor uint64
		for {
			keys, next, e := client.Scan(context.Background(), cursor, root+"*", 128).Result()
			if e != nil {
				t.Error(e)
				break
			}
			if len(keys) > 0 {
				if e = client.Del(context.Background(), keys...).Err(); e != nil {
					t.Error(e)
				}
			}
			if next == 0 {
				break
			}
			cursor = next
		}
		_ = client.Close()
	}()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err = os.WriteFile(tokenFile, []byte(strings.Repeat("synthetic-token-", 4)), 0600); err != nil {
		t.Fatal(err)
	}
	auth, err := control.NewBearerAuthenticator(tokenFile, "refusal-test")
	if err != nil {
		t.Fatal(err)
	}
	server, err := control.NewServer(control.Config{Process: process, Authenticator: auth, AllowPlain: true, StateAuthority: connection.Authority()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	written, retried := make(chan []byte, 16), make(chan []byte, 16)
	var e *redisNative
	var writerCalls atomic.Int64
	cfg := Config{Process: process, AdvertiseAddress: "sip.example.test:5061", ContactURI: "sips:sip.example.test:5061", StateIdentityKey: key}
	cfg.WriteNativeRetry = func(_ context.Context, _ *pb.FlowRef, wire []byte, _ time.Time) error {
		retried <- bytes.Clone(wire)
		return nil
	}
	cfg.WriteCommitted = func(ctx context.Context, write *pb.FlowWrite) *pb.DeliveryResult {
		writerCalls.Add(1)
		result := &pb.DeliveryResult{Effect: write.Effect, Outcome: pb.DeliveryResult_NOT_WRITTEN}
		reserved, reserveErr := e.Reserve(ctx, write)
		if reserveErr != nil {
			result.Error = reserveErr.Error()
			return result
		}
		if reserved.Known {
			result.Outcome, result.Recorded = reserved.Outcome, true
			return result
		}
		written <- bytes.Clone(reserved.Wire)
		result.Outcome = pb.DeliveryResult_WRITTEN
		if completeErr := e.Complete(ctx, write, result); completeErr != nil {
			result.Error = completeErr.Error()
			return result
		}
		result.Recorded = true
		return result
	}
	e, err = newRedisNative(ctx, cfg, connection, server)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for e.Ready() != nil {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("worker not ready")
		}
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			method, toTag, status := strings.Split(name, "-")[0], "", uint32(503)
			if strings.Contains(name, "-tagged") {
				toTag, status = "original-dialog-tag", 481
			}
			flow := &pb.FlowRef{Ingress: process, ConnectionId: "udp-listener", SourceIp: "192.0.2.20", SourcePort: 5060, Listener: "sip.example.test:5061", Transport: "udp"}
			request := stateTestRequest(method, 1, toTag, "initial-refusal-"+name)
			native, freezeErr := e.redisIngressMessage(flow, request, nil)
			if freezeErr != nil {
				t.Fatal(freezeErr)
			}
			if rejectErr := e.RejectParsed(ctx, flow, request, status, nil, native); rejectErr != nil {
				t.Fatal(rejectErr)
			}
			var final []byte
			select {
			case final = <-written:
			case <-ctx.Done():
				t.Fatal("no native refusal output")
			}
			if !bytes.HasPrefix(final, []byte(fmt.Sprintf("SIP/2.0 %d ", status))) {
				t.Fatalf("wrong selected final: %q", final)
			}
			if toTag != "" && !bytes.Contains(final, []byte(";tag="+toTag)) {
				t.Fatal("tagged refusal changed the original dialog tag")
			}
			_, _, _, driverErr := e.driver.Current()
			if driverErr != nil {
				t.Fatal(driverErr)
			}
			store, storeErr := connection.Refresh(ctx)
			if storeErr != nil {
				t.Fatal(storeErr)
			}
			serverKey, selectorErr := native.ServerTxKey()
			selectors := []rediscore.Selector{nativeSelector(rediscore.SelectorServerTransaction, serverKey)}
			if selectorErr != nil {
				t.Fatal(selectorErr)
			}
			// Wait for actual positive-W native consumption, not a sleep that
			// assumes the asynchronous dispatcher already acknowledged it.
			var selected *rediscore.SelectorLookup
			for {
				selected, err = store.LookupSelector(ctx, selectors[0], "r:context", "r:result-1", "r:subscription")
				if err != nil {
					t.Fatal(err)
				}
				procedure, decodeErr := readPrivateProcedureRecord(selected.View.Records[selected.Record])
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if procedure.FinalResponseWrittenAcknowledged {
					break
				}
				select {
				case <-time.After(5 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal("positive refusal proof not consumed")
				}
			}
			current, decodeErr := decodeNativeContext(selected.View.Records["r:context"])
			if decodeErr != nil || !nativeInitialRefusalContext(current) || selected.Identity.Owner != "" || selected.Identity.Term != "" || len(selected.View.Records["r:result-1"]) != 0 || len(selected.View.Records["r:subscription"]) != 0 {
				t.Fatal("refusal acquired application/subscription history", decodeErr)
			}
			if toTag != "" {
				ids, selectErr := nativeSelectorIDs(native, false, false)
				if selectErr != nil || len(ids) != 2 {
					t.Fatal("tagged request has no exact dialog selector", selectErr)
				}
				if _, lookupErr := store.LookupSelector(ctx, ids[1], "r:context"); !errors.Is(lookupErr, rediscore.ErrAbsent) {
					t.Fatal("refusal published a dialog selector", lookupErr)
				}
			}
			handled, completionErr := e.TryNativeCompletionParsed(ctx, flow, request, nil, native)
			if completionErr != nil || !handled {
				t.Fatal("exact refusal duplicate not consumed", handled, completionErr)
			}
			select {
			case replay := <-retried:
				if !bytes.Equal(replay, final) {
					t.Fatal("duplicate selected a different final")
				}
			case <-ctx.Done():
				t.Fatal("UDP refusal not replayed")
			}
			select {
			case <-written:
				t.Fatal("duplicate created a new physical attempt")
			default:
			}
			// A second admission cannot publish another final or context.
			if rejectErr := e.RejectParsed(ctx, flow, request, 486, nil, native); rejectErr != nil {
				t.Fatal(rejectErr)
			}
			selectedAgain, lookupErr := store.LookupSelector(ctx, selectors[0], "r:context")
			if lookupErr != nil || selectedAgain.Identity != selected.Identity {
				t.Fatal("refusal selector replaced", lookupErr)
			}
			if method == "REGISTER" {
				view, viewErr := store.Read(ctx, selected.Identity, "r:output-0")
				if viewErr != nil {
					t.Fatal(viewErr)
				}
				output, outputErr := decodeNativeOutput(view.Records["r:output-0"])
				if outputErr != nil {
					t.Fatal(outputErr)
				}
				ref, refErr := nativeOutputRef(output.Write)
				if refErr != nil {
					t.Fatal(refErr)
				}
				before := writerCalls.Load()
				for _, mutation := range []string{"dialog", "operation", "replay", "procedure"} {
					changed := proto.Clone(output).(*nativepb.Output)
					switch mutation {
					case "dialog":
						changed.Dialog = "r:dialog-invalid"
					case "operation":
						changed.Operation = "forged-operation"
					case "replay":
						changed.Replay = true
					case "procedure":
						changed.Procedure = "r:procedure-invalid"
					}
					encoded, encodeErr := encodeNativeOutput(changed)
					if encodeErr != nil {
						t.Fatal(encodeErr)
					}
					ready := rediscore.ReadyOutput{Ref: ref, Metadata: encoded, MetadataField: "r:output-0", DeadlineMS: int64(output.Write.Permit.Authorization.DeadlineMs)}
					if readyErr := e.readyOutput(ctx, store, ready); readyErr == nil {
						t.Fatal("malformed refusal reached writer", mutation)
					}
					if writerCalls.Load() != before {
						t.Fatal("malformed refusal invoked physical writer", mutation)
					}
				}
			}
			if method == "INVITE" && toTag != "" {
				if strings.HasSuffix(name, "-CANCEL") {
					cancelRequest := stateTestRequest("CANCEL", 1, toTag, "initial-refusal-"+name)
					handled, cancelErr := e.TryNativeCompletion(ctx, flow, cancelRequest)
					if cancelErr != nil || !handled {
						t.Fatal("matched CANCEL after tagged refusal", handled, cancelErr)
					}
					select {
					case wire := <-written:
						if !bytes.HasPrefix(wire, []byte("SIP/2.0 200 ")) || !bytes.Contains(wire, []byte("CSeq: 1 CANCEL")) {
							t.Fatal("matched CANCEL did not select its own 200")
						}
					case <-ctx.Done():
						t.Fatal("matched CANCEL response missing")
					}
					latest, readErr := store.LookupSelector(ctx, selectors[0], "r:context", "r:result-1")
					if readErr != nil || latest.Identity != selected.Identity {
						t.Fatal("CANCEL changed refusal identity", readErr)
					}
					context, decodeErr := decodeNativeContext(latest.View.Records["r:context"])
					invite, procedureErr := readPrivateProcedureRecord(latest.View.Records[latest.Record])
					if decodeErr != nil || procedureErr != nil || context.CommandSequence != 1 || len(context.NoninitialProcedures) != 1 ||
						len(latest.View.Records["r:result-1"]) != 0 || invite.LastResponse != "r:invite-response-"+invite.RequestId+"-1" || invite.Native.Cancelled {
						t.Fatal("CANCEL replaced final or acquired application state", decodeErr, procedureErr)
					}
				}
				if !strings.HasSuffix(name, "-no-ACK") {
					ack := stateTestRequest("ACK", 1, toTag, "initial-refusal-"+name)
					handled, ackErr := e.TryNativeCompletion(ctx, flow, ack)
					if ackErr != nil || !handled {
						t.Fatal("tagged native-only refusal ACK", handled, ackErr)
					}
				}
				// Keep the actual I/H interval and physical cutoff unchanged.
				// The refusal owns no business dialog even though its request is tagged.
				for {
					if readyErr := e.Ready(); readyErr != nil {
						t.Fatal("tagged refusal native deadline fenced worker", readyErr)
					}
					_, lookupErr := store.LookupSelector(ctx, selectors[0], "r:context")
					if errors.Is(lookupErr, rediscore.ErrAbsent) {
						break
					}
					if lookupErr != nil {
						t.Fatal(lookupErr)
					}
					select {
					case <-retried:
					case <-time.After(5 * time.Millisecond):
					case <-ctx.Done():
						t.Fatal("tagged refusal did not retire at its original native deadline")
					}
				}
			} else if method == "INVITE" {
				// Non-2xx ACK terminates the reliable shape in the separate
				// pure selector test; UDP retains its original Timer I.
				owners, ownersErr := selectRedisTerminalOwners(ctx, store, &rediscore.DueContext{Identity: selected.Identity, Revision: selected.View.Revision, Assignment: selected.View.Timer, CheckedAtMS: time.Now().UnixMilli()}, current, time.Now().Add(time.Second))
				if ownersErr != nil && !errors.Is(ownersErr, rediscore.ErrConflict) {
					t.Fatal("native-only terminal selection demanded an application result", ownersErr)
				}
				if owners != nil {
					t.Fatal("live INVITE native timer unexpectedly retired")
				}
			}
		})
	}
	t.Run("reliable INVITE ACK releases native-only context", func(t *testing.T) {
		flow := &pb.FlowRef{Ingress: process, ConnectionId: "tls-phone", SourceIp: "192.0.2.21", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
		request := stateTestRequest("INVITE", 1, "", "reliable-refusal")
		native, freezeErr := e.redisIngressMessage(flow, request, nil)
		if freezeErr != nil {
			t.Fatal(freezeErr)
		}
		if rejectErr := e.RejectParsed(ctx, flow, request, 486, nil, native); rejectErr != nil {
			t.Fatal(rejectErr)
		}
		select {
		case <-written:
		case <-ctx.Done():
			t.Fatal("reliable refusal not written")
		}
		store, storeErr := connection.Refresh(ctx)
		if storeErr != nil {
			t.Fatal(storeErr)
		}
		serverKey, keyErr := native.ServerTxKey()
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		selector := nativeSelector(rediscore.SelectorServerTransaction, serverKey)
		selected, selectErr := store.LookupSelector(ctx, selector, "r:context")
		if selectErr != nil {
			t.Fatal(selectErr)
		}
		current, decodeErr := decodeNativeContext(selected.View.Records["r:context"])
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		ack := stateTestRequest("ACK", 1, current.LocalTag, "reliable-refusal")
		handled, ackErr := e.TryNativeCompletion(ctx, flow, ack)
		if ackErr != nil || !handled {
			t.Fatal("non-2xx ACK not consumed", handled, ackErr)
		}
		for {
			_, lookupErr := store.LookupSelector(ctx, selector, "r:context")
			if errors.Is(lookupErr, rediscore.ErrAbsent) {
				break
			}
			if lookupErr != nil {
				t.Fatal(lookupErr)
			}
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal("native-only refusal could not retire after ACK")
			}
		}
	})

	t.Run("reliable unmatched CANCEL releases its own selector", func(t *testing.T) {
		flow := &pb.FlowRef{Ingress: process, ConnectionId: "tls-cancel-phone", SourceIp: "192.0.2.22", SourcePort: 5061, Listener: "sip.example.test:5061", Transport: "tls"}
		request := stateTestRequest("CANCEL", 1, "", "reliable-cancel-refusal")
		native, freezeErr := e.redisIngressMessage(flow, request, nil)
		if freezeErr != nil {
			t.Fatal(freezeErr)
		}
		if rejectErr := e.RejectParsed(ctx, flow, request, 481, nil, native); rejectErr != nil {
			t.Fatal(rejectErr)
		}
		select {
		case <-written:
		case <-ctx.Done():
			t.Fatal("reliable CANCEL refusal not written")
		}
		store, storeErr := connection.Refresh(ctx)
		if storeErr != nil {
			t.Fatal(storeErr)
		}
		serverKey, keyErr := native.ServerTxKey()
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		selector := nativeSelector(rediscore.SelectorServerTransaction, serverKey)
		for {
			_, lookupErr := store.LookupSelector(ctx, selector, "r:context")
			if errors.Is(lookupErr, rediscore.ErrAbsent) {
				break
			}
			if lookupErr != nil {
				t.Fatal(lookupErr)
			}
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal("CANCEL refusal retained the wrong transaction selector")
			}
		}
	})

}
