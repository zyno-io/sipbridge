package main

import (
	"bytes"
	"sipbridge/internal/engine"
	"testing"
)

func TestBinaryRuntimeDoesNotInventLegacyMetrics(t *testing.T) {
	var output bytes.Buffer
	writeNativeMetrics(&output, &engine.RedisNative{})
	if output.Len() != 0 {
		t.Fatal("binary runtime emitted legacy metric values", output.String())
	}
}
