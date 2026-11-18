package control

import (
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// The opening token is already consumed. A descriptor selects message fields;
// element selects singular values in a repeated field or map. With neither,
// unknown additive values still enforce duplicate-key and nesting bounds.
func validateJSONStreamContainer(decoder *json.Decoder, opening json.Delim, depth int, descriptor protoreflect.MessageDescriptor, element protoreflect.FieldDescriptor) error {
	if depth >= 32 {
		return errors.New("JSON nesting limit")
	}
	seen := make(map[string]struct{})
	oneofs := make(map[protoreflect.FullName]bool)
	for decoder.More() {
		field, singular := element, true
		if opening == '{' {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate JSON key")
			}
			seen[key] = struct{}{}
			if descriptor != nil {
				fields := descriptor.Fields()
				field, singular = fields.ByJSONName(key), false
				if field == nil && fields.ByName(protoreflect.Name(key)) != nil {
					return errors.New("noncanonical JSON field name")
				}
				if field != nil {
					if oneof := field.ContainingOneof(); oneof != nil {
						if oneofs[oneof.FullName()] {
							return errors.New("multiple JSON variants")
						}
						oneofs[oneof.FullName()] = true
					}
				}
			}
		}
		if err := validateJSONStreamValue(decoder, depth+1, field, singular); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	expected := json.Delim(']')
	if opening == '{' {
		expected = '}'
	}
	if closing != expected {
		return errors.New("invalid JSON container")
	}
	if descriptor != nil {
		for i := 0; i < descriptor.Oneofs().Len(); i++ {
			if !oneofs[descriptor.Oneofs().Get(i).FullName()] {
				return errors.New("missing JSON variant")
			}
		}
	}
	return nil
}

func validateJSONStreamValue(decoder *json.Decoder, depth int, field protoreflect.FieldDescriptor, singular bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if field != nil {
		switch {
		case !singular && field.IsMap():
			if token != json.Delim('{') {
				return errors.New("JSON map must be an object")
			}
			return validateJSONStreamContainer(decoder, '{', depth, nil, field.MapValue())
		case !singular && field.IsList():
			if token != json.Delim('[') {
				return errors.New("JSON repeated field must be an array")
			}
			return validateJSONStreamContainer(decoder, '[', depth, nil, field)
		case field.Kind() == protoreflect.MessageKind:
			if token != json.Delim('{') {
				return errors.New("JSON message must be an object")
			}
			return validateJSONStreamContainer(decoder, '{', depth, field.Message(), nil)
		default:
			return validateJSONScalar(token, field)
		}
	}
	if opening, container := token.(json.Delim); container {
		if opening != '{' && opening != '[' {
			return errors.New("invalid JSON container")
		}
		return validateJSONStreamContainer(decoder, opening, depth, nil, nil)
	}
	return nil
}
