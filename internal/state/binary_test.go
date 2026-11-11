package state

import (
	"reflect"
	"strings"
	"testing"
)

func TestBinaryPayloadPreservesDispatchTargets(t *testing.T) {
	for _, worker := range []string{"", AnyDispatchWorker, strings.Repeat("a", 64)} {
		value := Payload{Effects: []Effect{{ID: "physical", Kind: "deliver", Status: EffectAuthorized, DeadlineMS: 1000, Worker: worker, Payload: []byte{0, 255, 1}}}}
		data, err := value.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var decoded Payload
		if err := decoded.UnmarshalBinary(data); err != nil || !reflect.DeepEqual(value, decoded) {
			t.Fatal("binary persistence changed physical/controller dispatch identity", worker, err)
		}
	}
}
