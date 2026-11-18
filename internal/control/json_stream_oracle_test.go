package control

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	pb "sipbridge/internal/controlpb"
)

// The original DOM decoder stays independent of the streaming implementation.
var treeDecimalUint64 = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
var treePaddedBase64 = regexp.MustCompile(`^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$`)

// Read tokens before schema conversion so duplicate keys, including escaped
// equivalents, cannot acquire different meanings in different control peers.
func treeJSONValue(decoder *json.Decoder, depth int) (interface{}, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	if depth >= 32 {
		return nil, errors.New("JSON nesting limit")
	}
	switch delimiter {
	case '{':
		object := make(map[string]interface{})
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := token.(string)
			if !ok {
				return nil, errors.New("invalid JSON object key")
			}
			if _, exists := object[key]; exists {
				return nil, errors.New("duplicate JSON key")
			}
			value, err := treeJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		_, err := decoder.Token()
		return object, err
	case '[':
		array := make([]interface{}, 0)
		for decoder.More() {
			value, err := treeJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		_, err := decoder.Token()
		return array, err
	default:
		return nil, errors.New("invalid JSON container")
	}
}

func validateTreeJSONMessage(value interface{}, descriptor protoreflect.MessageDescriptor) error {
	object, ok := value.(map[string]interface{})
	if !ok {
		return errors.New("JSON message must be an object")
	}
	oneofs := make(map[protoreflect.FullName]bool)
	fields := descriptor.Fields()
	for key, value := range object {
		field := fields.ByJSONName(key)
		if field == nil {
			if fields.ByName(protoreflect.Name(key)) != nil {
				return errors.New("noncanonical JSON field name")
			}
			continue // Unknown optional fields are additive; never an operation.
		}
		if oneof := field.ContainingOneof(); oneof != nil {
			if oneofs[oneof.FullName()] {
				return errors.New("multiple JSON variants")
			}
			oneofs[oneof.FullName()] = true
		}
		if field.IsMap() {
			entries, ok := value.(map[string]interface{})
			if !ok {
				return errors.New("JSON map must be an object")
			}
			for _, entry := range entries {
				if err := validateTreeJSONScalar(entry, field.MapValue()); err != nil {
					return err
				}
			}
		} else if field.IsList() {
			entries, ok := value.([]interface{})
			if !ok {
				return errors.New("JSON repeated field must be an array")
			}
			for _, entry := range entries {
				if err := validateTreeJSONScalar(entry, field); err != nil {
					return err
				}
			}
		} else if err := validateTreeJSONScalar(value, field); err != nil {
			return err
		}
	}
	for i := 0; i < descriptor.Oneofs().Len(); i++ {
		if !oneofs[descriptor.Oneofs().Get(i).FullName()] {
			return errors.New("missing JSON variant")
		}
	}
	return nil
}

func validateTreeJSONScalar(value interface{}, field protoreflect.FieldDescriptor) error {
	switch field.Kind() {
	case protoreflect.MessageKind:
		return validateTreeJSONMessage(value, field.Message())
	case protoreflect.StringKind:
		if _, ok := value.(string); ok {
			return nil
		}
	case protoreflect.BoolKind:
		if _, ok := value.(bool); ok {
			return nil
		}
	case protoreflect.Uint64Kind:
		if text, ok := value.(string); ok && treeDecimalUint64.MatchString(text) {
			_, err := strconv.ParseUint(text, 10, 64)
			return err
		}
	case protoreflect.Uint32Kind:
		if number, ok := value.(json.Number); ok {
			parsed, err := strconv.ParseFloat(string(number), 64)
			if err == nil && parsed >= 0 && parsed <= 4294967295 && parsed == float64(uint32(parsed)) {
				return nil
			}
		}
	case protoreflect.BytesKind:
		if text, ok := value.(string); ok && treePaddedBase64.MatchString(text) {
			_, err := base64.StdEncoding.Strict().DecodeString(text)
			return err
		}
	case protoreflect.EnumKind:
		if text, ok := value.(string); ok && field.Enum().Values().ByName(protoreflect.Name(text)) != nil {
			return nil
		}
	}
	return errors.New("invalid JSON field type or value")
}

func decodeJSONEnvelopeTree(data []byte) (*pb.Envelope, error) {
	if len(data) == 0 || len(data) > MaxEnvelope || !utf8.Valid(data) {
		return nil, errors.New("invalid JSON envelope bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := treeJSONValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON content")
	}
	envelope := &pb.Envelope{}
	if err := validateTreeJSONMessage(value, envelope.ProtoReflect().Descriptor()); err != nil {
		return nil, err
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}
