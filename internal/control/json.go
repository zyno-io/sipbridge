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

var decimalUint64 = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
var paddedBase64 = regexp.MustCompile(`^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$`)

func validateJSONScalar(value interface{}, field protoreflect.FieldDescriptor) error {
	switch field.Kind() {
	case protoreflect.StringKind:
		if _, ok := value.(string); ok {
			return nil
		}
	case protoreflect.BoolKind:
		if _, ok := value.(bool); ok {
			return nil
		}
	case protoreflect.Uint64Kind:
		if text, ok := value.(string); ok && decimalUint64.MatchString(text) {
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
		if text, ok := value.(string); ok && paddedBase64.MatchString(text) {
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

func decodeJSONEnvelope(data []byte) (*pb.Envelope, error) {
	if len(data) == 0 || len(data) > MaxEnvelope || !utf8.Valid(data) {
		return nil, errors.New("invalid JSON envelope bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if opening != json.Delim('{') {
		return nil, errors.New("JSON message must be an object")
	}
	envelope := &pb.Envelope{}
	if err := validateJSONStreamContainer(decoder, '{', 0, envelope.ProtoReflect().Descriptor(), nil); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON content")
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

func encodeJSONEnvelope(envelope *pb.Envelope) ([]byte, error) {
	data, err := appendJSONMessage(make([]byte, 0, 1024), envelope.ProtoReflect(), 0)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > MaxEnvelope {
		return nil, errors.New("invalid JSON envelope bytes")
	}
	return data, nil
}
