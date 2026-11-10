package control

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestSharedJSONContractCases(t *testing.T) {
	data, err := os.ReadFile("testdata/json-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name  string `json:"name"`
		Valid bool   `json:"valid"`
		JSON  string `json:"json"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range cases {
		t.Run(fixture.Name, func(t *testing.T) {
			envelope, err := decodeJSONEnvelope([]byte(fixture.JSON))
			if (err == nil) != fixture.Valid {
				t.Fatalf("acceptance mismatch: valid=%v err=%v", fixture.Valid, err)
			}
			if fixture.Valid {
				encoded, err := encodeJSONEnvelope(envelope)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := decodeJSONEnvelope(encoded); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestJSONByteAndNestingBounds(t *testing.T) {
	for _, data := range [][]byte{
		append([]byte(`{"correlationId":"case","ping":{"nonce":"`), append([]byte{0xff}, []byte(`"}}`)...)...),
		[]byte(`{"correlationId":"case","ping":{},"future":` + strings.Repeat("[", 32) + "0" + strings.Repeat("]", 32) + "}"),
		[]byte(strings.Repeat(" ", MaxEnvelope+1)),
	} {
		if _, err := decodeJSONEnvelope(data); err == nil {
			t.Fatal("invalid bytes or excessive JSON nesting/size accepted")
		}
	}
}
