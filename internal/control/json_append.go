package control

import (
	"encoding/base64"
	"errors"
	"strconv"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Encode the canonical descriptor directly into one buffer. Object field order
// is insignificant; sorting fields and building per-string buffers adds no wire
// guarantee. Validation is part of this walk, not a second message traversal.
func appendJSONMessage(dst []byte, message protoreflect.Message, depth int) ([]byte, error) {
	if depth >= 32 {
		return nil, errors.New("JSON nesting limit")
	}
	oneofs := message.Descriptor().Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		if message.WhichOneof(oneofs.Get(i)) == nil {
			return nil, errors.New("missing JSON variant")
		}
	}
	dst = append(dst, '{')
	first := true
	var err error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		// Descriptor JSON names are protobuf identifiers, never caller strings.
		dst = append(dst, '"')
		dst = append(dst, field.JSONName()...)
		dst = append(dst, '"', ':')
		switch {
		case field.IsMap():
			if depth+1 >= 32 {
				err = errors.New("JSON nesting limit")
				return false
			}
			dst = append(dst, '{')
			firstEntry := true
			value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				if !firstEntry {
					dst = append(dst, ',')
				}
				firstEntry = false
				dst, err = appendJSONString(dst, key.String())
				if err == nil {
					dst = append(dst, ':')
					dst, err = appendJSONScalar(dst, entry, field.MapValue(), depth+2)
				}
				return err == nil
			})
			dst = append(dst, '}')
		case field.IsList():
			if depth+1 >= 32 {
				err = errors.New("JSON nesting limit")
				return false
			}
			dst = append(dst, '[')
			entries := value.List()
			for i := 0; i < entries.Len(); i++ {
				if i > 0 {
					dst = append(dst, ',')
				}
				dst, err = appendJSONScalar(dst, entries.Get(i), field, depth+2)
				if err != nil {
					break
				}
			}
			dst = append(dst, ']')
		default:
			dst, err = appendJSONScalar(dst, value, field, depth+1)
		}
		if err == nil && len(dst) > MaxEnvelope {
			err = errors.New("invalid JSON envelope bytes")
		}
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	return append(dst, '}'), nil
}

func appendJSONScalar(dst []byte, value protoreflect.Value, field protoreflect.FieldDescriptor, depth int) ([]byte, error) {
	switch field.Kind() {
	case protoreflect.MessageKind:
		return appendJSONMessage(dst, value.Message(), depth)
	case protoreflect.StringKind:
		return appendJSONString(dst, value.String())
	case protoreflect.BoolKind:
		return strconv.AppendBool(dst, value.Bool()), nil
	case protoreflect.Uint32Kind:
		return strconv.AppendUint(dst, value.Uint(), 10), nil
	case protoreflect.Uint64Kind:
		dst = append(dst, '"')
		dst = strconv.AppendUint(dst, value.Uint(), 10)
		return append(dst, '"'), nil
	case protoreflect.BytesKind:
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, value.Bytes())
		return append(dst, '"'), nil
	case protoreflect.EnumKind:
		if selected := field.Enum().Values().ByNumber(value.Enum()); selected != nil {
			return appendJSONString(dst, string(selected.Name()))
		}
	}
	return nil, errors.New("invalid JSON field type or value")
}

func appendJSONString(dst []byte, value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, errors.New("invalid JSON UTF-8")
	}
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, value[start:i]...)
		dst = append(dst, '\\')
		switch c {
		case '"', '\\':
			dst = append(dst, c)
		case '\n':
			dst = append(dst, 'n')
		case '\r':
			dst = append(dst, 'r')
		case '\t':
			dst = append(dst, 't')
		case '\b':
			dst = append(dst, 'b')
		case '\f':
			dst = append(dst, 'f')
		default:
			const digits = "0123456789abcdef"
			dst = append(dst, 'u', '0', '0', digits[c>>4], digits[c&15])
		}
		start = i + 1
	}
	dst = append(dst, value[start:]...)
	return append(dst, '"'), nil
}
