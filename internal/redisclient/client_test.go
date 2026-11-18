package redisclient

import (
	"crypto/tls"
	"testing"
	"time"
)

func TestNormalizeConnectionOptions(t *testing.T) {
	input := Options{SentinelAddresses: []string{"localhost:26379"}, SentinelMaster: "primary", TLS: &tls.Config{ServerName: "redis.local"}}
	got, err := Normalize(input)
	if err != nil {
		t.Fatal(err)
	}
	if got.DialTimeout != 2*time.Second || got.IOTimeout != 2*time.Second || got.PoolSize != 16 {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	input.SentinelAddresses[0] = "changed"
	input.TLS.ServerName = "changed"
	if got.SentinelAddresses[0] != "localhost:26379" || got.TLS.ServerName != "redis.local" {
		t.Fatal("mutable configuration was not independently owned")
	}
	for _, bad := range []Options{{}, {Address: "localhost:6379", SentinelAddresses: []string{"localhost:26379"}, SentinelMaster: "primary"}, {SentinelAddresses: []string{"localhost:26379"}}, {Address: "localhost:6379", PoolSize: 257}, {Address: "localhost:6379", IOTimeout: -time.Second}} {
		if _, err := Normalize(bad); err == nil {
			t.Fatalf("accepted invalid options: %+v", bad)
		}
	}
}

func TestClientFactoryIsLazyAndDisablesRetries(t *testing.T) {
	for _, options := range []Options{{Address: "127.0.0.1:1"}, {SentinelAddresses: []string{"127.0.0.1:1"}, SentinelMaster: "primary"}} {
		normalized, err := Normalize(options)
		if err != nil {
			t.Fatal(err)
		}
		client := New(normalized, normalized.PoolSize, nil)
		actual := client.Options()
		if actual.MaxRetries != 0 || !actual.ContextTimeoutEnabled || actual.DialTimeout != 2*time.Second || actual.ReadTimeout != 2*time.Second || actual.WriteTimeout != 2*time.Second {
			t.Fatalf("unexpected connection settings: %+v", actual)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
