package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"sipbridge/internal/state"
)

// Invoked by scripts/gen-state-storage.py; reflection is generation-only.
func TestGenerateStorageTypes(t *testing.T) {
	if os.Getenv("SIPBRIDGE_GENERATE_STORAGE") != "1" {
		t.Skip("storage generation only")
	}
	types := make(map[string]map[string]interface{})
	var visit func(reflect.Type) string
	visit = func(typ reflect.Type) string {
		if typ.Kind() == reflect.Pointer {
			return "*" + visit(typ.Elem())
		}
		if typ.Kind() == reflect.Slice {
			return "[]" + visit(typ.Elem())
		}
		if typ.Kind() == reflect.Map {
			return "map[" + visit(typ.Key()) + "]" + visit(typ.Elem())
		}
		if typ.PkgPath() == "sipbridge/internal/controlpb" {
			if typ.Kind() == reflect.Struct {
				value := reflect.New(typ).Interface().(proto.Message)
				return "pb:" + string(value.ProtoReflect().Descriptor().FullName()) + ":" + typ.Name()
			}
			return "enum:" + typ.Name()
		}
		if typ.Name() == "" || typ.PkgPath() == "" {
			return typ.Kind().String()
		}
		name := typ.PkgPath() + "." + typ.Name()
		if _, exists := types[name]; exists {
			return name
		}
		item := map[string]interface{}{"kind": typ.Kind().String(), "name": typ.Name(), "package": typ.PkgPath()}
		types[name] = item
		if typ.Kind() == reflect.Struct {
			var fields []map[string]string
			for n := 0; n < typ.NumField(); n++ {
				field := typ.Field(n)
				if field.Tag.Get("json") == "-" || field.PkgPath != "" && !field.Anonymous {
					continue
				}
				fields = append(fields, map[string]string{"name": field.Name, "type": visit(field.Type), "json": strings.Split(field.Tag.Get("json"), ",")[0]})
			}
			item["fields"] = fields
		}
		return name
	}
	for _, root := range []reflect.Type{reflect.TypeOf(state.Payload{}), reflect.TypeOf(stateGroup{}), reflect.TypeOf(stateEngineInput{}), reflect.TypeOf(stateEngineEffect{}), reflect.TypeOf(stateDigestChallenge{})} {
		visit(root)
	}
	encoded, err := json.MarshalIndent(types, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("STORAGE_TYPES " + string(encoded))
}
