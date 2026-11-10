package main

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func TestStateConfigurationRequiresRedisAndDiscovery(t *testing.T) {
	values := map[string]string{"SIPBRIDGE_STATE_BACKEND": "redis", "SIPBRIDGE_STATE_NAMESPACE": "test", "SIPBRIDGE_STATE_GENERATION": "one",
		"SIPBRIDGE_STATE_IDENTITY_KEY": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))), "SIPBRIDGE_REDIS_ADDRESS": "127.0.0.1:6379", "SIPBRIDGE_REDIS_ALLOW_PLAINTEXT": "true"}
	read := func(string) ([]byte, error) { return nil, errors.New("no file") }
	get := func(key string) string { return values[key] }
	cfg, err := stateConfiguration(get, read)
	if err != nil || cfg == nil || len(cfg.NamespaceKey) != 32 || cfg.Redis.TLS != nil {
		t.Fatalf("local configuration: %v", err)
	}
	if cfg.Versions.Schema != "3" || cfg.Versions.Reducer != "9" || cfg.Versions.Timers != "1" {
		t.Fatal("service must fence older native reducer profiles")
	}
	for _, invalid := range []struct{ key, value string }{
		{"SIPBRIDGE_STATE_NAMESPACE", "bad/name"}, {"SIPBRIDGE_STATE_GENERATION", "bad/name"},
		{"SIPBRIDGE_STATE_IDENTITY_KEY", "do-not-leak-secret"}, {"SIPBRIDGE_STATE_PARTITIONS", "0"}, {"SIPBRIDGE_STATE_PARTITIONS", "4097"},
		{"SIPBRIDGE_REDIS_ADDRESS", "redis://user:secret@redis:6379"}, {"SIPBRIDGE_REDIS_ADDRESS", "redis:0"}, {"SIPBRIDGE_REDIS_ALLOW_PLAINTEXT", "false"},
		{"SIPBRIDGE_REDIS_SENTINELS", "sentinel:26379"},
	} {
		prior := values[invalid.key]
		values[invalid.key] = invalid.value
		if _, err := stateConfiguration(get, read); err == nil || strings.Contains(err.Error(), "do-not-leak-secret") {
			t.Fatalf("accepted or exposed %s", invalid.key)
		}
		values[invalid.key] = prior
	}
	values["SIPBRIDGE_REDIS_ADDRESS"] = ""
	values["SIPBRIDGE_REDIS_SENTINELS"] = "sentinel-a:26379,[::1]:26379"
	values["SIPBRIDGE_REDIS_MASTER"] = "edge"
	cfg, err = stateConfiguration(get, read)
	if err != nil || len(cfg.Redis.SentinelAddresses) != 2 {
		t.Fatalf("sentinel configuration: %v", err)
	}
	delete(values, "SIPBRIDGE_STATE_GENERATION")
	delete(values, "SIPBRIDGE_STATE_NAMESPACE")
	if cfg, err = stateConfiguration(get, read); err != nil || cfg.Generation != "default" || cfg.Namespace != "sipbridge" {
		t.Fatal("configuration defaults failed", err)
	}

}

func TestStateConfigurationNeverSilentlyDisablesTLS(t *testing.T) {
	values := map[string]string{"SIPBRIDGE_STATE_BACKEND": "redis", "SIPBRIDGE_STATE_NAMESPACE": "test", "SIPBRIDGE_STATE_GENERATION": "one",
		"SIPBRIDGE_STATE_IDENTITY_KEY": base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))), "SIPBRIDGE_REDIS_ADDRESS": "redis:6379",
		"SIPBRIDGE_REDIS_CA_FILE": "fixture.crt", "SIPBRIDGE_REDIS_SERVER_NAME": "redis.test"}
	get := func(key string) string { return values[key] }
	if _, err := stateConfiguration(get, func(string) ([]byte, error) { return []byte("invalid certificate"), nil }); err == nil {
		t.Fatal("invalid trust accepted")
	}
	values["SIPBRIDGE_REDIS_ALLOW_PLAINTEXT"] = "true"
	if _, err := stateConfiguration(get, func(string) ([]byte, error) {
		t.Fatal("must reject conflicting configuration before reading")
		return nil, nil
	}); err == nil {
		t.Fatal("conflicting TLS/plaintext accepted")
	}
}

func TestStateConfigurationDefaultsToStandaloneMemory(t *testing.T) {
	values := map[string]string{}
	get := func(key string) string { return values[key] }
	read := func(string) ([]byte, error) { t.Fatal("memory must not read Redis trust"); return nil, nil }
	first, err := stateConfiguration(get, read)
	if err != nil || first.Backend != "memory" || len(first.NamespaceKey) != 32 {
		t.Fatal("memory defaults", err)
	}
	second, err := stateConfiguration(get, read)
	if err != nil || string(first.NamespaceKey) == string(second.NamespaceKey) {
		t.Fatal("process key reused", err)
	}
	for _, invalid := range []struct{ key, value string }{
		{"SIPBRIDGE_STATE_BACKEND", "unknown"}, {"SIPBRIDGE_REDIS_ADDRESS", "redis:6379"},
		{"SIPBRIDGE_REDIS_PASSWORD", "secret-do-not-print"}, {"SIPBRIDGE_STATE_IDENTITY_KEY", "invalid"},
		{"SIPBRIDGE_MEMORY_MAX_GROUPS", "0"}, {"SIPBRIDGE_MEMORY_MAX_GROUPS", "1048577"},
		{"SIPBRIDGE_MEMORY_MAX_BYTES", "8388607"}, {"SIPBRIDGE_MEMORY_MAX_BYTES", "-1"},
	} {
		values[invalid.key] = invalid.value
		_, err := stateConfiguration(get, read)
		if err == nil || strings.Contains(err.Error(), "secret-do-not-print") {
			t.Fatalf("invalid or exposed %s", invalid.key)
		}
		delete(values, invalid.key)
	}
	values["SIPBRIDGE_MEMORY_MAX_GROUPS"] = "8"
	values["SIPBRIDGE_MEMORY_MAX_BYTES"] = "67108864"
	bounded, err := stateConfiguration(get, read)
	if err != nil || bounded.Memory.MaxGroups != 8 || bounded.Memory.MaxBytes != 67108864 {
		t.Fatal("memory limits", err)
	}
}
