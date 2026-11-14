package engine

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/emiago/sipgo/sip/portable"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
	"sipbridge/internal/state"
	"sipbridge/internal/storagecodec"
	"sipbridge/internal/storagepb"
)

// This oracle intentionally retains the complete existing protobuf graph and
// adapter. It compares exactly the proposed replacement boundary, before the
// unchanged stateGroup.UnmarshalBinary semantic checks and result normalization.
func directGroupOracle(data []byte) (stateGroup, error) {
	decoded := new(storagepb.EngineStateGroup)
	if err := storagecodec.Unmarshal(data, storagecodec.Group, decoded); err != nil {
		return stateGroup{}, err
	}
	return storageDecodeEngineStateGroup(decoded), nil
}

func directGroupEncode(value stateGroup) ([]byte, error) {
	return storagecodec.Marshal(storagecodec.Group, storageEncodeEngineStateGroup(value))
}

// Exercise nonzero signed/unsigned scalars, every reachable private struct and
// collection, and public protobuf leaves without a duplicate field catalog.
func fillDirectFixture(v reflect.Value, depth int) {
	if depth > 12 || !v.CanSet() {
		return
	}
	if v.CanInterface() {
		if enum, ok := v.Interface().(protoreflect.Enum); ok {
			values := enum.Descriptor().Values()
			v.SetInt(int64(values.Get(values.Len() - 1).Number()))
			return
		}
	}
	if v.Kind() == reflect.Pointer {
		v.Set(reflect.New(v.Type().Elem()))
		if message, ok := v.Interface().(proto.Message); ok {
			fillDirectPublicFixture(message.ProtoReflect(), 0)
			return
		}
		fillDirectFixture(v.Elem(), depth+1)
		return
	}
	switch v.Kind() {
	case reflect.Struct:
		for n := 0; n < v.NumField(); n++ {
			fillDirectFixture(v.Field(n), depth+1)
		}
	case reflect.Map:
		v.Set(reflect.MakeMapWithSize(v.Type(), 2))
		key := reflect.New(v.Type().Key()).Elem()
		key.SetString("fixture")
		value := reflect.New(v.Type().Elem()).Elem()
		fillDirectFixture(value, depth+1)
		v.SetMapIndex(key, value)
		if value.Kind() == reflect.Pointer {
			key.SetString("nil")
			v.SetMapIndex(key, reflect.Zero(value.Type()))
		}
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 2, 2))
		for n := 0; n < v.Len(); n++ {
			fillDirectFixture(v.Index(n), depth+1)
		}
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("fixture")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(-7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(127)
	}
}

func fillDirectPublicFixture(message protoreflect.Message, depth int) {
	if depth > 2 {
		return
	}
	fields := message.Descriptor().Fields()
	for n := 0; n < fields.Len(); n++ {
		field := fields.Get(n)
		if field.IsMap() || field.IsList() {
			continue
		}
		switch field.Kind() {
		case protoreflect.MessageKind:
			fillDirectPublicFixture(message.Mutable(field).Message(), depth+1)
		case protoreflect.StringKind:
			message.Set(field, protoreflect.ValueOfString("fixture"))
		case protoreflect.BytesKind:
			message.Set(field, protoreflect.ValueOfBytes([]byte{0, 1, 255}))
		case protoreflect.BoolKind:
			message.Set(field, protoreflect.ValueOfBool(true))
		case protoreflect.Int32Kind, protoreflect.Int64Kind:
			message.Set(field, protoreflect.ValueOfInt64(7))
		case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
			message.Set(field, protoreflect.ValueOfUint64(7))
		case protoreflect.EnumKind:
			message.Set(field, protoreflect.ValueOfEnum(field.Enum().Values().Get(field.Enum().Values().Len()-1).Number()))
		}
	}
}

func TestDirectGroupDenseReflectedGraphParity(t *testing.T) {
	var value stateGroup
	fillDirectFixture(reflect.ValueOf(&value).Elem(), 0)
	data, err := directGroupEncode(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := assertDirectGroupParity(t, data); !valid {
		t.Fatal("dense fixture did not pass canonical preflight")
	}
}

// Compare nil/empty collections and private values exactly, while public
// protobuf messages use semantic equality rather than runtime cache internals.
func directValueEqual(a, b reflect.Value) bool {
	if !a.IsValid() || !b.IsValid() {
		return a.IsValid() == b.IsValid()
	}
	if a.Type() != b.Type() {
		return false
	}
	if a.CanInterface() && b.CanInterface() {
		if left, ok := a.Interface().(proto.Message); ok {
			right, ok := b.Interface().(proto.Message)
			return ok && proto.Equal(left, right)
		}
	}
	switch a.Kind() {
	case reflect.Pointer, reflect.Interface:
		return a.IsNil() == b.IsNil() && (a.IsNil() || directValueEqual(a.Elem(), b.Elem()))
	case reflect.Slice, reflect.Map:
		if a.IsNil() != b.IsNil() || a.Len() != b.Len() {
			return false
		}
		if a.Kind() == reflect.Map {
			for _, key := range a.MapKeys() {
				if !directValueEqual(a.MapIndex(key), b.MapIndex(key)) {
					return false
				}
			}
		} else {
			for n := 0; n < a.Len(); n++ {
				if !directValueEqual(a.Index(n), b.Index(n)) {
					return false
				}
			}
		}
		return true
	case reflect.Struct:
		for n := 0; n < a.NumField(); n++ {
			if !directValueEqual(a.Field(n), b.Field(n)) {
				return false
			}
		}
		return true
	case reflect.String:
		return a.String() == b.String()
	case reflect.Bool:
		return a.Bool() == b.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return a.Int() == b.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return a.Uint() == b.Uint()
	default:
		panic("unexpected retained graph kind: " + a.Kind().String())
	}
}

func assertDirectGroupParity(t testing.TB, data []byte) (stateGroup, bool) {
	t.Helper()
	want, oldErr := directGroupOracle(data)
	got, newErr := readStateGroupDirect(data)
	if (oldErr == nil) != (newErr == nil) {
		t.Fatalf("decoder acceptance differs: oracle=%v direct=%v", oldErr, newErr)
	}
	if oldErr != nil {
		if !reflect.DeepEqual(got, stateGroup{}) {
			t.Fatal("failed direct decode published partial state")
		}
		return got, false
	}
	if !directValueEqual(reflect.ValueOf(want), reflect.ValueOf(got)) {
		t.Fatal("direct decoder changed retained values or collection presence")
	}
	wantBytes, err := directGroupEncode(want)
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := directGroupEncode(got)
	if err != nil || !bytes.Equal(wantBytes, gotBytes) {
		t.Fatal("direct decoder changed deterministic canonical bytes", err)
	}
	return got, true
}

func TestDirectGroupPresenceDefaultsAndOwnership(t *testing.T) {
	for n, value := range []stateGroup{
		{},
		{Version: 1, Contexts: map[string]*stateContext{}, Selectors: map[string]string{}, Retired: map[string]int64{}},
		{Version: 1, Contexts: map[string]*stateContext{"nil": nil, "empty": {}}, Selectors: map[string]string{"": ""}, Retired: map[string]int64{"": 0}},
		{Version: 1, NextAllocation: 1<<53 - 1, Contexts: map[string]*stateContext{"full": {
			Snapshot: &pb.ContextSnapshot{Context: &pb.ContextRef{Id: "context"}, LocalSdp: []byte{0, 255}, Selectors: []string{"selector"}},
			Origin:   portable.RestoreMessage([]byte{0, 1, 255}, "tls", "", ""),
			Transactions: map[string]*stateTransaction{"nil": nil, "value": {Native: portable.Transaction{Version: 1, Key: "tx", LastResponse: stateTestPortablePtr([]byte{3, 4}, "", "", ""),
				Timers: map[portable.Timer]portable.Deadline{portable.TimerA: {At: 42, Active: true}}}}},
			Requests: map[string]string{}, Dialogs: map[string]portable.Dialog{"dialog": {Timers: map[portable.Timer]portable.Deadline{}, LocalBye: stateTestPortablePtr([]byte{5, 6}, "", "", "")}},
			Result:   &pb.OperationResult{Certainty: pb.OperationResult_UNKNOWN, Response: &pb.SipMessage{Wire: []byte{7, 8}, Body: []byte{9}}},
			Delivery: &pb.ContextDelivery{Flow: &pb.FlowRef{Ingress: &pb.ProcessRef{InstanceId: "fixture"}, SourcePort: 5061}},
		}}, Selectors: map[string]string{"a": "full"}},
	} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			data, err := directGroupEncode(value)
			if err != nil {
				t.Fatal(err)
			}
			assertDirectGroupParity(t, data)
			left, err := readStateGroupDirect(data)
			if err != nil {
				t.Fatal(err)
			}
			right, err := readStateGroupDirect(data)
			if err != nil {
				t.Fatal(err)
			}
			before, err := directGroupEncode(right)
			if err != nil {
				t.Fatal(err)
			}
			clear(data)
			leftAfterInputMutation, err := directGroupEncode(left)
			if err != nil || !bytes.Equal(before, leftAfterInputMutation) {
				t.Fatal("direct output aliases input frame", err)
			}
			if c := left.Contexts["full"]; c != nil {
				originWire := c.Origin.WireBytes()
				originWire[0] = 99
				c.Origin = portable.RestoreMessage(originWire, c.Origin.Transport(), c.Origin.Source(), c.Origin.Destination())
				c.Snapshot.LocalSdp[0] = 99
				response := c.Transactions["value"].Native.LastResponse
				responseWire := response.WireBytes()
				responseWire[0] = 99
				*response = portable.RestoreMessage(responseWire, response.Transport(), response.Source(), response.Destination())
				bye := c.Dialogs["dialog"].LocalBye
				byeWire := bye.WireBytes()
				byeWire[0] = 99
				*bye = portable.RestoreMessage(byeWire, bye.Transport(), bye.Source(), bye.Destination())
				c.Result.Response.Body[0] = 99
				c.Delivery.Flow.SourcePort = 99
				delete(c.Transactions, "nil")
			}
			after, err := directGroupEncode(right)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("direct output aliases input bytes or a separate decode", err)
			}
		})
	}
}

func directTestField(descriptor protoreflect.MessageDescriptor, field string, body []byte) []byte {
	number := descriptor.Fields().ByName(protoreflect.Name(field)).Number()
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), body)
}

func directTestFrame(body []byte) []byte {
	return append([]byte{'S', 'I', 'P', 'B', 1, storagecodec.Group}, body...)
}

func TestDirectGroupStrictWireAndNoncanonicalDefaults(t *testing.T) {
	groupDesc := (&storagepb.EngineStateGroup{}).ProtoReflect().Descriptor()
	selectorsDesc := (&storagepb.EngineStateGroupSelectorsValues{}).ProtoReflect().Descriptor()
	entryDesc := selectorsDesc.Fields().ByName("values").Message()
	entry := append(directTestField(entryDesc, "key", []byte("same")), directTestField(entryDesc, "value", []byte("value"))...)
	wrapped := directTestField(selectorsDesc, "values", entry)
	version := protowire.AppendVarint(protowire.AppendTag(nil, groupDesc.Fields().ByName("version").Number(), protowire.VarintType), 1)
	zeroVersion := protowire.AppendVarint(protowire.AppendTag(nil, groupDesc.Fields().ByName("version").Number(), protowire.VarintType), 0)
	var tooMany []byte
	for n := 0; n < storagecodec.MaxCollection+1; n++ {
		entry := directTestField(entryDesc, "key", []byte(fmt.Sprint(n)))
		tooMany = append(tooMany, directTestField(selectorsDesc, "values", entry)...)
	}
	contextsDesc := (&storagepb.EngineStateGroupContextsValues{}).ProtoReflect().Descriptor()
	contextEntryDesc := contextsDesc.Fields().ByName("values").Message()
	nullableDesc := contextEntryDesc.Fields().ByName("value").Message()
	var nullableContexts []byte
	for n, name := range []string{"omitted-value", "empty-nullable", "present-zero-context"} {
		entry := directTestField(contextEntryDesc, "key", []byte(name))
		if n > 0 {
			var nullable []byte
			if n == 2 {
				nullable = directTestField(nullableDesc, "value", nil)
			}
			entry = append(entry, directTestField(contextEntryDesc, "value", nullable)...)
		}
		nullableContexts = append(nullableContexts, directTestField(contextsDesc, "values", entry)...)
	}
	for _, sample := range []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"empty-frame", directTestFrame(nil), true},
		{"explicit-zero-version", directTestFrame(zeroVersion), true},
		{"nullable-map-presence", directTestFrame(directTestField(groupDesc, "contexts", nullableContexts)), true},
		{"empty-selectors-wrapper", directTestFrame(directTestField(groupDesc, "selectors", nil)), true},
		{"missing-map-key-value", directTestFrame(directTestField(groupDesc, "selectors", directTestField(selectorsDesc, "values", nil))), true},
		{"absent-frame", nil, false},
		{"json", []byte(`{"version":1}`), false},
		{"wrong-kind", []byte{'S', 'I', 'P', 'B', 1, storagecodec.Payload}, false},
		{"unknown", directTestFrame(protowire.AppendVarint(protowire.AppendTag(nil, 63, protowire.VarintType), 1)), false},
		{"duplicate", directTestFrame(append(bytes.Clone(version), version...)), false},
		{"wrong-wire", directTestFrame(protowire.AppendBytes(protowire.AppendTag(nil, groupDesc.Fields().ByName("version").Number(), protowire.BytesType), nil)), false},
		{"truncated", directTestFrame([]byte{0x12, 0xff}), false},
		{"invalid-utf8", directTestFrame(directTestField(groupDesc, "outbound_id", []byte{255})), false},
		{"duplicate-map", directTestFrame(directTestField(groupDesc, "selectors", append(bytes.Clone(wrapped), wrapped...))), false},
		{"map-count", directTestFrame(directTestField(groupDesc, "selectors", tooMany)), false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			_, valid := assertDirectGroupParity(t, sample.data)
			if valid != sample.valid {
				t.Fatal("unexpected fixture acceptance")
			}
		})
	}
	for n, snapshot := range []*pb.ContextSnapshot{{Kind: pb.ContextSnapshot_Kind(999)}, {Answered: true}} {
		value := &storagepb.EngineStateGroup{Contexts: &storagepb.EngineStateGroupContextsValues{Values: map[string]*storagepb.EngineStateGroupContextsValuesEntryValue{
			"context": {Value: &storagepb.EngineStateContext{Snapshot: snapshot}},
		}}}
		data, err := storagecodec.Marshal(storagecodec.Group, value)
		if err != nil {
			t.Fatal(err)
		}
		_, valid := assertDirectGroupParity(t, data)
		if valid != (n == 1) {
			t.Fatal("invalid enum did not retain strict rejection")
		}
	}
}

func FuzzDirectGroupDecoderParity(f *testing.F) {
	for _, value := range []stateGroup{{}, {Version: 1, Contexts: map[string]*stateContext{"nil": nil, "value": {}}}, benchmarkBinaryGroup(2)} {
		data, err := directGroupEncode(value)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte("SIPB\x01\x02\x08\x01"))
	f.Fuzz(func(t *testing.T, data []byte) { assertDirectGroupParity(t, data) })
}

var directBenchmarkGroup stateGroup

func BenchmarkDirectGroupHistoryDecoding(b *testing.B) {
	fixture := idleSubscriptionOwnerFixture(b, 16)
	var payload state.Payload
	if err := payload.UnmarshalBinary(fixture.Payload); err != nil {
		b.Fatal(err)
	}
	assertDirectGroupParity(b, payload.ProtocolState)
	for _, outer := range []bool{false, true} {
		for _, direct := range []bool{false, true} {
			name := "group/protobuf-oracle"
			if outer {
				name = "payload-and-group/protobuf-oracle"
			}
			if direct {
				name = name[:len(name)-len("protobuf-oracle")] + "direct"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(payload.ProtocolState)))
				for b.Loop() {
					data := payload.ProtocolState
					if outer {
						var fresh state.Payload
						if err := fresh.UnmarshalBinary(fixture.Payload); err != nil {
							b.Fatal(err)
						}
						data = fresh.ProtocolState
					}
					var err error
					if direct {
						directBenchmarkGroup, err = readStateGroupDirect(data)
					} else {
						directBenchmarkGroup, err = directGroupOracle(data)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
