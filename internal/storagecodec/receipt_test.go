package storagecodec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStorageIdentityFencesEitherDescriptorWithoutFencingSourceOnlyChanges(t *testing.T) {
	for _, path := range []string{"proto/sipbridge-storage.proto", "proto/sipbridge-control.proto", "internal/state/metadata.go"} {
		t.Run(path, func(t *testing.T) {
			var value struct {
				Encoding string            `json:"encoding"`
				SHA256   map[string]string `json:"sha256"`
			}
			if err := json.Unmarshal(Receipt(), &value); err != nil {
				t.Fatal(err)
			}
			value.SHA256[path] = strings.Repeat("f", 64)
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := schemaIdentityFromReceipt(data)
			if err != nil {
				t.Fatal(err)
			}
			if changed := identity != SchemaIdentity(); changed != strings.HasPrefix(path, "proto/") {
				t.Fatal("schema label alone admitted a changed descriptor or source-only change fenced compatible state")
			}
		})
	}
}
