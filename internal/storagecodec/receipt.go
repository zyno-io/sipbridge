package storagecodec

import (
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
)

//go:embed receipt.json
var receipt []byte

var storageSchemaIdentity = func() string {
	identity, err := schemaIdentityFromReceipt(receipt)
	if err != nil {
		panic("invalid embedded storage schema receipt")
	}
	return identity
}()

// SchemaIdentity binds the strict decoder's descriptor graph, independently of
// source-only changes and the public logical schema/reducer/timer version tuple.
func SchemaIdentity() string { return storageSchemaIdentity }

func schemaIdentityFromReceipt(data []byte) (string, error) {
	var value struct {
		Encoding string            `json:"encoding"`
		SHA256   map[string]string `json:"sha256"`
	}
	if json.Unmarshal(data, &value) != nil || value.Encoding != Encoding {
		return "", errors.New("invalid storage schema receipt")
	}
	storage, control := value.SHA256["proto/sipbridge-storage.proto"], value.SHA256["proto/sipbridge-control.proto"]
	for _, digest := range []string{storage, control} {
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != digest {
			return "", errors.New("invalid storage descriptor digest")
		}
	}
	return Encoding + ":" + storage + ":" + control, nil
}

// Receipt returns a fresh immutable-build receipt, separate from public control.
func Receipt() []byte { return append([]byte(nil), receipt...) }
