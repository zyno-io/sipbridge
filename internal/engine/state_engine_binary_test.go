package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
)

func TestBinaryGroupPreservesMapPresenceAndIntegerIdentity(t *testing.T) {
	value := stateGroup{Version: 1, NextAllocation: 1<<53 - 1, Contexts: map[string]*stateContext{}, Selectors: map[string]string{}, Retired: map[string]int64{}}
	encoded, err := value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded stateGroup
	if err := decoded.UnmarshalBinary(encoded); err != nil || !reflect.DeepEqual(value, decoded) {
		t.Fatal("presence/integer changed", err)
	}
	value.Contexts = nil
	encoded, err = value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := decoded.UnmarshalBinary(encoded); err != nil || decoded.Contexts != nil {
		t.Fatal("nil map became allocated", err)
	}
	value.Contexts = map[string]*stateContext{"invalid": nil}
	encoded, err = value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	before := decoded
	if err := decoded.UnmarshalBinary(encoded); err == nil || !reflect.DeepEqual(decoded, before) {
		t.Fatal("null context exposed partial group")
	}
}
func TestBinaryInputsAreDeterministicAndKeepOneofs(t *testing.T) {
	value := stateEngineInput{Kind: "command", ApplicationGeneration: ^uint64(0), Command: &pb.Command{Action: &pb.Command_Cancel{Cancel: &pb.Cancel{}}}}
	encoded, err := value.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded stateEngineInput
	if err := decoded.UnmarshalBinary(encoded); err != nil || decoded.Command.GetCancel() == nil || decoded.ApplicationGeneration != ^uint64(0) {
		t.Fatal("oneof/integer changed", err)
	}
	again, err := decoded.MarshalBinary()
	if err != nil || !bytes.Equal(encoded, again) {
		t.Fatal("non-deterministic retained input", err)
	}
	if err := decoded.UnmarshalBinary([]byte(`{"kind":"command"}`)); err == nil {
		t.Fatal("legacy JSON input accepted")
	}
	value.Reply = &pb.Envelope{Payload: &pb.Envelope_CredentialResult{CredentialResult: &pb.CredentialResult{Password: "never-retain"}}}
	if _, err := value.MarshalBinary(); err == nil {
		t.Fatal("credential response persisted")
	}
}
func benchmarkBinaryGroup(transactions int) stateGroup {
	wire := []byte("REGISTER sip:example.test SIP/2.0\r\nCall-ID: fixture\r\nCSeq: 1 REGISTER\r\nContent-Length: 0\r\n\r\n" + strings.Repeat("x", 2048))
	context := &stateContext{Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "fixture"}}, Transactions: map[string]*stateTransaction{}, Requests: map[string]string{}, Dialogs: map[string]portable.Dialog{}}
	for n := 0; n < transactions; n++ {
		key := fmt.Sprintf("tx-%d", n)
		context.Transactions[key] = &stateTransaction{Native: portable.Transaction{Version: 1, Key: key, Origin: portable.Message{Wire: wire, Transport: "tls"}, Timers: map[portable.Timer]portable.Deadline{}, Profile: portable.DefaultTimers()}}
	}
	return stateGroup{Version: 1, Contexts: map[string]*stateContext{"fixture": context}, Selectors: map[string]string{"fixture": "fixture"}}
}
func BenchmarkRetainedGroupCodec(b *testing.B) {
	for _, count := range []int{1, 16, 64} {
		value := benchmarkBinaryGroup(count)
		for _, binary := range []bool{false, true} {
			profile := "json"
			encode := func() ([]byte, error) { return json.Marshal(value) }
			if binary {
				profile = "protobuf-v1"
				encode = value.MarshalBinary
			}
			data, err := encode()
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%d/%s/encode", count, profile), func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(data)), "record-bytes")
				for n := 0; n < b.N; n++ {
					if _, err := encode(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("%d/%s/decode", count, profile), func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(data)), "record-bytes")
				for n := 0; n < b.N; n++ {
					var decoded stateGroup
					var err error
					if binary {
						err = decoded.UnmarshalBinary(data)
					} else {
						err = json.Unmarshal(data, &decoded)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkRetainedRecordCodec(b *testing.B) {
	for _, count := range []int{1, 16, 64} {
		value := benchmarkBinaryGroup(count)
		for _, binary := range []bool{false, true} {
			profile := "json"
			if binary {
				profile = "protobuf-v1"
			}
			encode := func() ([]byte, error) {
				var protocol []byte
				var err error
				if binary {
					protocol, err = value.MarshalBinary()
				} else {
					protocol, err = json.Marshal(value)
				}
				if err != nil {
					return nil, err
				}
				payload := state.Payload{ProtocolState: protocol, ActiveTransactions: count}
				for n := 0; n < min(count, 8); n++ {
					body := stateEngineEffect{Context: "fixture", Wire: value.Contexts["fixture"].Transactions["tx-0"].Native.Origin.Wire}
					var encoded []byte
					if binary {
						encoded, err = body.MarshalBinary()
					} else {
						encoded, err = json.Marshal(body)
					}
					if err != nil {
						return nil, err
					}
					payload.Effects = append(payload.Effects, state.Effect{ID: fmt.Sprintf("effect-%d", n), Kind: "deliver", Status: state.EffectPending, DeadlineMS: 1, Payload: encoded})
				}
				if binary {
					return payload.MarshalBinary()
				}
				return payload.MarshalJSON()
			}
			data, err := encode()
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%d/%s/encode", count, profile), func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(data)), "record-bytes")
				for n := 0; n < b.N; n++ {
					if _, err := encode(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("%d/%s/decode", count, profile), func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(len(data)), "record-bytes")
				for n := 0; n < b.N; n++ {
					var payload state.Payload
					var group stateGroup
					var err error
					if binary {
						err = payload.UnmarshalBinary(data)
						if err == nil {
							err = group.UnmarshalBinary(payload.ProtocolState)
						}
					} else {
						err = payload.UnmarshalJSON(data)
						if err == nil {
							err = json.Unmarshal(payload.ProtocolState, &group)
						}
					}
					if err != nil {
						b.Fatal(err)
					}
					for _, effect := range payload.Effects {
						var body stateEngineEffect
						if binary {
							err = body.UnmarshalBinary(effect.Payload)
						} else {
							err = body.UnmarshalJSON(effect.Payload)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
