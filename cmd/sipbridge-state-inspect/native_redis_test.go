package main

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func TestNativeFixtureConfigurationMatchesBinaryRootAndRequiresExplicitTLS(t *testing.T) {
	values := map[string]string{"SIPBRIDGE_REDIS_ADDRESS": "127.0.0.1:6379", "SIPBRIDGE_REDIS_ALLOW_PLAINTEXT": "true", "SIPBRIDGE_STATE_IDENTITY_KEY": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))), "SIPBRIDGE_STATE_NAMESPACE": "fixture", "SIPBRIDGE_STATE_GENERATION": "one"}
	get := func(key string) string { return values[key] }
	noFile := func(string) ([]byte, error) { return nil, errors.New("must not read trust") }
	config, err := nativeFixtureConfiguration(get, noFile)
	if err != nil || config.Profile.Root != "sipstate:v1:fixture:one:" || config.Profile.RecordBudget != 32<<20 || config.Redis.TLS != nil {
		t.Fatal("wrong native fixture configuration", err)
	}
	for _, invalid := range []struct{ key, value string }{{"SIPBRIDGE_STATE_IDENTITY_KEY", "private-key-must-not-print"}, {"SIPBRIDGE_REDIS_ALLOW_PLAINTEXT", "false"}, {"SIPBRIDGE_REDIS_CA_FILE", "trust.pem"}, {"SIPBRIDGE_STATE_NAMESPACE", "bad/name"}, {"SIPBRIDGE_REDIS_ADDRESS", "redis://private-key-must-not-print@host:6379"}, {"SIPBRIDGE_REDIS_ADDRESS", "localhost:0"}} {
		old := values[invalid.key]
		values[invalid.key] = invalid.value
		_, err = nativeFixtureConfiguration(get, noFile)
		if err == nil || strings.Contains(err.Error(), "private-key-must-not-print") {
			t.Fatal("accepted invalid fixture configuration or leaked key")
		}
		values[invalid.key] = old
	}
	delete(values, "SIPBRIDGE_STATE_GENERATION")
	config, err = nativeFixtureConfiguration(get, noFile)
	if err != nil || config.Profile.Root != "sipstate:v1:fixture:1:" {
		t.Fatal("fixture Redis generation default", err)
	}
}
