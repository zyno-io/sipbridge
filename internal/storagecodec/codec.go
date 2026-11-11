// Package storagecodec validates the private persisted protobuf profile. It is
// independent of the public JSON control framing and never reads legacy JSON.
package storagecodec

import (
	"bytes"
	"errors"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const Encoding = "protobuf-v1"
const MaxBytes = 64 << 20
const MaxCollection = 4096
const MaxDepth = 32
const MaxFields = 1 << 18

const (
	Payload byte = iota + 1
	Group
	Input
	Effect
	Digest
	NetworkTarget
	Policy
	Disposition
)

var ErrRecord = errors.New("invalid retained protobuf record")
var magic = []byte{'S', 'I', 'P', 'B', 1}

// Marshal is deterministic wherever bytes participate in replay fingerprints.
// Callers retain ownership of value; the returned record is independently owned.
func Marshal(kind byte, value proto.Message) (result []byte, err error) {
	started := time.Now()
	defer func() { observe(kind, true, started, len(result), err) }()
	if kind < Payload || kind > Disposition || proto.Size(value) > MaxBytes-len(magic)-1 {
		return nil, ErrRecord
	}
	data := make([]byte, len(magic)+1, len(magic)+1+proto.Size(value))
	copy(data, magic)
	data[len(magic)] = kind
	encoded, err := (proto.MarshalOptions{Deterministic: true}).MarshalAppend(data, value)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// Unmarshal preflights the complete wire before allocating the generated graph.
// No unknown fields, duplicate singular/oneof/map values, invalid enums, excessive
// recursion or unbounded collection allocation reach the reducer. The caller
// provides a fresh local message and publishes it only after semantic validation.
func Unmarshal(data []byte, kind byte, value proto.Message) (err error) {
	started := time.Now()
	defer func() { observe(kind, false, started, len(data), err) }()
	if len(data) > MaxBytes || len(data) < len(magic)+1 || !bytes.Equal(data[:len(magic)], magic) || data[len(magic)] != kind {
		return ErrRecord
	}
	data = data[len(magic)+1:]
	remaining := MaxFields
	if err := preflight(data, value.ProtoReflect().Descriptor(), 0, &remaining); err != nil {
		return err
	}
	return (proto.UnmarshalOptions{RecursionLimit: MaxDepth, DiscardUnknown: false}).Unmarshal(data, value)
}

func preflight(data []byte, descriptor protoreflect.MessageDescriptor, depth int, remaining *int) error {
	if descriptor.FullName() == "sipbridge.storage.v1.EngineStatePolicyAdmission" && len(data) > 4096 {
		return ErrRecord
	}
	if depth >= MaxDepth {
		return ErrRecord
	}
	var seen uint64
	var counts [64]uint16
	var oneofs uint64
	var mapKeys map[protoreflect.FieldNumber]map[string]struct{}
	for len(data) > 0 {
		*remaining--
		if *remaining < 0 {
			return ErrRecord
		}
		number, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return ErrRecord
		}
		field := descriptor.Fields().ByNumber(number)
		if field == nil || number <= 0 || number >= 64 {
			return ErrRecord
		}
		index := uint(number)
		bit := uint64(1) << index
		if field.IsList() || field.IsMap() {
			counts[index]++
			if counts[index] > MaxCollection {
				return ErrRecord
			}
		} else {
			if seen&bit != 0 {
				return ErrRecord
			}
			seen |= bit
			if oneof := field.ContainingOneof(); oneof != nil {
				if oneof.Index() >= 64 {
					return ErrRecord
				}
				oneofBit := uint64(1) << uint(oneof.Index())
				if oneofs&oneofBit != 0 {
					return ErrRecord
				}
				oneofs |= oneofBit
			}
		}
		body := data[tagBytes:]
		length := protowire.ConsumeFieldValue(number, wireType, body)
		if length < 0 {
			return ErrRecord
		}
		switch field.Kind() {
		case protoreflect.MessageKind:
			if wireType != protowire.BytesType {
				return ErrRecord
			}
			nested, n := protowire.ConsumeBytes(body)
			if n < 0 || preflight(nested, field.Message(), depth+1, remaining) != nil {
				return ErrRecord
			}
			if field.IsMap() {
				// All private/control maps have string keys. Parse the validated
				// key without allocating a second decoded message graph.
				if field.MapKey().Kind() != protoreflect.StringKind {
					return ErrRecord
				}
				key := ""
				for len(nested) > 0 {
					num, typ, size := protowire.ConsumeTag(nested)
					fieldSize := protowire.ConsumeFieldValue(num, typ, nested[size:])
					if num == 1 {
						raw, _ := protowire.ConsumeBytes(nested[size:])
						key = string(raw)
					}
					nested = nested[size+fieldSize:]
				}
				if mapKeys == nil {
					mapKeys = make(map[protoreflect.FieldNumber]map[string]struct{})
				}
				keys := mapKeys[field.Number()]
				if keys == nil {
					keys = make(map[string]struct{})
					mapKeys[field.Number()] = keys
				}
				if _, duplicate := keys[key]; duplicate {
					return ErrRecord
				}
				keys[key] = struct{}{}
			}
		case protoreflect.StringKind, protoreflect.BytesKind:
			if wireType != protowire.BytesType {
				return ErrRecord
			}
			raw, n := protowire.ConsumeBytes(body)
			if n < 0 || field.Kind() == protoreflect.StringKind && !utf8.Valid(raw) {
				return ErrRecord
			}
		case protoreflect.BoolKind, protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind, protoreflect.Uint64Kind:
			if wireType != protowire.VarintType {
				return ErrRecord
			}
			value, n := protowire.ConsumeVarint(body)
			if n < 0 || field.Kind() == protoreflect.BoolKind && value > 1 || field.Kind() == protoreflect.Uint32Kind && value > 1<<32-1 {
				return ErrRecord
			}
			if field.Kind() == protoreflect.EnumKind && field.Enum().Values().ByNumber(protoreflect.EnumNumber(value)) == nil {
				return ErrRecord
			}
		default:
			return ErrRecord // A schema extension needs explicit storage qualification.
		}
		data = body[length:]
	}
	return nil
}
