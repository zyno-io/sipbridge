package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestPreparedInputPreservesCanonicalEnvelopeAndDigest(t *testing.T) {
	input := Input{Proof: true, ID: "input-one", Kind: "command", Fingerprint: "fixed<&>", Payload: []byte{0, 1, 2, 3, 254, 255}, DeadlineMS: 1000, ReceiptUntilMS: 2000, Create: true, ApplicationLeaseIDs: []string{"owner-a", "owner-b"}}
	const encoded = `{"proof":true,"id":"input-one","kind":"command","fingerprint":"fixed\u003c\u0026\u003e","payload":"AAECA/7/","deadlineMs":1000,"receiptUntilMs":2000,"create":true,"applicationLeaseIds":["owner-a","owner-b"]}`
	const digest = "c4c50765d9941726a70c8a4c99bd74a956143f14811f1d93e8bf552c4fc21b10"
	metadata := newMetadata(contractConfig())
	for _, retain := range []bool{false, true} {
		prepared, err := metadata.prepareInput(input, retain)
		if err != nil || prepared.digest != digest || retain && prepared.encoded != encoded || !retain && prepared.encoded != "" || !prepared.matches(input) {
			t.Fatal("canonical input changed", err)
		}
	}
	metadata.cfg.Limits.InputBytes = len(encoded) - 1
	if _, err := metadata.prepareInput(input, true); !errors.Is(err, ErrCapacity) {
		t.Fatal("prepared input bypassed original encoded envelope size limit", err)
	}
}

func TestPreparedInputBindingCoversEveryInputField(t *testing.T) {
	mutations := map[string]func(*Input){
		"Proof":          func(in *Input) { in.Proof = !in.Proof },
		"ID":             func(in *Input) { in.ID += "-changed" },
		"Kind":           func(in *Input) { in.Kind = "other" },
		"Fingerprint":    func(in *Input) { in.Fingerprint += "-changed" },
		"Payload":        func(in *Input) { in.Payload[0] ^= 1 },
		"DeadlineMS":     func(in *Input) { in.DeadlineMS++ },
		"ReceiptUntilMS": func(in *Input) { in.ReceiptUntilMS++ },
		"Create":         func(in *Input) { in.Create = !in.Create },
		"ApplicationLeaseIDs": func(in *Input) {
			in.ApplicationLeaseIDs[0], in.ApplicationLeaseIDs[1] = in.ApplicationLeaseIDs[1], in.ApplicationLeaseIDs[0]
		},
	}
	fields := reflect.TypeOf(Input{})
	if fields.NumField() != len(mutations) {
		t.Fatal("new input field lacks an integrity mutation case")
	}
	for n := 0; n < fields.NumField(); n++ {
		if mutations[fields.Field(n).Name] == nil {
			t.Fatal("input field lacks an integrity mutation case", fields.Field(n).Name)
		}
	}
	backendContract(t, func(t *testing.T, store Backend) {
		for name, mutate := range mutations {
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				ref := contractRef(t, store, "binding-"+name)
				input := contractInput("binding-"+name, true)
				input.Payload, input.ApplicationLeaseIDs = []byte("immutable"), []string{"owner-a", "owner-b"}
				digest, _, err := digestInput(input)
				if err != nil {
					t.Fatal(err)
				}
				update := contractRead(t, store, ref, input)
				mutate(&update.Input)
				if _, err := store.CompareAndSwap(ctx, update, Payload{}, nil); !errors.Is(err, ErrConflict) {
					t.Fatal("mutated input crossed CAS integrity boundary", name, err)
				}
				if _, err := store.Load(ctx, ref); !errors.Is(err, ErrNotFound) {
					t.Fatal("input integrity refusal allocated a group", err)
				}
				update.Input = ownedInput(input)
				receipt, err := store.CompareAndSwap(ctx, update, Payload{}, nil)
				if err != nil || receipt.InputDigest != digest {
					t.Fatal("restored exact input did not preserve its original digest", err)
				}
			})
		}
	})
}

func TestPreparedInputOwnsCallerSlicesAndPreservesOmittedEquivalence(t *testing.T) {
	backendContract(t, func(t *testing.T, store Backend) {
		input := contractInput("owned-input", true)
		input.Payload, input.ApplicationLeaseIDs = []byte("immutable"), []string{"owner-a"}
		expected := ownedInput(input)
		digest, _, _ := digestInput(input)
		update := contractRead(t, store, contractRef(t, store, "owned-input"), input)
		input.Payload[0], input.ApplicationLeaseIDs[0] = 'X', "changed-owner"
		if !reflect.DeepEqual(update.Input, expected) {
			t.Fatal("admitted input aliases caller-owned payload or lease IDs")
		}
		receipt, err := store.CompareAndSwap(context.Background(), update, Payload{}, nil)
		if err != nil || receipt.InputDigest != digest {
			t.Fatal("caller mutation changed the owned input", err)
		}
		for _, nonnil := range []bool{false, true} {
			input := contractInput(fmt.Sprintf("omitted-%t", nonnil), true)
			if nonnil {
				input.Payload, input.ApplicationLeaseIDs = []byte{}, []string{}
			}
			update := contractRead(t, store, contractRef(t, store, input.ID), input)
			if nonnil {
				update.Input.Payload, update.Input.ApplicationLeaseIDs = nil, nil
			} else {
				update.Input.Payload, update.Input.ApplicationLeaseIDs = []byte{}, []string{}
			}
			if _, err := store.CompareAndSwap(context.Background(), update, Payload{}, nil); err != nil {
				t.Fatal("canonical omitempty-equivalent input was rejected", err)
			}
		}
	})
}

func TestPreparedMemoryInputPreservesFormerJSONProjection(t *testing.T) {
	store, err := NewMemory(contractConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input := contractInput("invalid-utf8", true)
	input.Fingerprint = string([]byte{'a', 0xff, 0xfe, 'b'})
	input.Payload, input.ApplicationLeaseIDs = []byte{}, []string{}
	originalDigest, encoded, _ := digestInput(input)
	var previous Input
	if err := json.Unmarshal(encoded, &previous); err != nil {
		t.Fatal(err)
	}
	update := contractRead(t, store, contractRef(t, store, "invalid-utf8"), input)
	if !reflect.DeepEqual(update.Input, previous) {
		t.Fatal("direct input ownership changed the former JSON projection")
	}
	previousDigest, _, _ := digestInput(previous)
	_, err = store.CompareAndSwap(context.Background(), update, Payload{}, nil)
	if previousDigest != originalDigest && !errors.Is(err, ErrConflict) {
		t.Fatal("direct ownership changed the former malformed-string integrity refusal", err)
	}
}

var benchmarkPreparedInput *preparedInput
var benchmarkOwnedInput Input

// Isolates the previous JSON work at admission/CAS versus the prepared binding.
// It excludes Redis I/O and protocol reduction; B/op is transient allocation.
func BenchmarkInputEnvelopeBinding(b *testing.B) {
	for _, size := range []int{256, 4096, 65536} {
		for _, backend := range []string{"memory", "redis"} {
			for _, prepared := range []bool{false, true} {
				b.Run(fmt.Sprintf("%d/%s/prepared-%t", size, backend, prepared), func(b *testing.B) {
					metadata := newMetadata(contractConfig())
					metadata.cfg.Limits.InputBytes = 1 << 20
					input := contractInput("benchmark-input", true)
					input.Payload = bytes.Repeat([]byte{'x'}, size)
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						if prepared {
							binding, err := metadata.prepareInput(input, backend == "redis")
							owned := ownedInput(input)
							if err != nil || !binding.matches(owned) {
								b.Fatal(err)
							}
							benchmarkPreparedInput, benchmarkOwnedInput = binding, owned
							continue
						}
						if err := metadata.validateInput(input); err != nil {
							b.Fatal(err)
						}
						digest, encoded, err := digestInput(input)
						owned := input
						if backend == "memory" {
							owned = Input{}
							err = json.Unmarshal(encoded, &owned)
						}
						checked, _, checkErr := digestInput(owned)
						if err != nil || checkErr != nil || checked != digest {
							b.Fatal("previous input validation failed", err, checkErr)
						}
						if backend == "redis" {
							checked, _, checkErr = digestInput(owned)
							if checkErr != nil || checked != digest {
								b.Fatal(checkErr)
							}
						}
						benchmarkOwnedInput = owned
					}
				})
			}
		}
	}
}

func BenchmarkPreparedInputMemoryCAS(b *testing.B) {
	for _, size := range []int{256, 4096, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			cfg := contractConfig()
			cfg.Limits.InputBytes = 1 << 20
			cfg.Limits.GroupBytes = 2 << 20
			store, err := NewMemory(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			ref, err := store.GroupForCallID("prepared-cas")
			if err != nil {
				b.Fatal(err)
			}
			input := contractInput("operation", true)
			input.Kind, input.Proof = "timer", true
			input.DeadlineMS = time.Now().Add(5 * time.Minute).UnixMilli()
			input.ReceiptUntilMS = input.DeadlineMS + 120000
			input.Payload = bytes.Repeat([]byte{'x'}, size)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				read, err := store.ReadUpdate(context.Background(), ref, input)
				if err != nil || read.Update == nil {
					b.Fatal(err)
				}
				if _, err := store.CompareAndSwap(context.Background(), *read.Update, Payload{}, nil); err != nil {
					b.Fatal(err)
				}
				ref, input.Create = read.Update.Record.Ref, false
			}
		})
	}
}
