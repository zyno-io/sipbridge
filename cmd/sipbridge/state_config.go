package main

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"sipbridge/internal/engine"
	"sipbridge/internal/state"
)

var stateIdentity = regexp.MustCompile(`^[A-Za-z0-9_.:@+\-]{1,96}$`)
var statePrefix = regexp.MustCompile(`^[A-Za-z0-9_.:\-]{1,96}$`)

// stateConfiguration validates configuration without connecting or provisioning.
// No error contains an endpoint password or the shared identity key.
func stateConfiguration(get func(string) string, readFile func(string) ([]byte, error)) (*state.Config, error) {
	cfg := &state.Config{Backend: get("SIPBRIDGE_STATE_BACKEND"), Namespace: get("SIPBRIDGE_STATE_NAMESPACE"), Generation: get("SIPBRIDGE_STATE_GENERATION"),
		Prefix: get("SIPBRIDGE_STATE_PREFIX"), Versions: engine.NativeStateVersions()}
	if cfg.Backend == "" {
		cfg.Backend = state.MemoryBackend
	}
	if cfg.Backend != state.MemoryBackend && cfg.Backend != state.RedisBackend {
		return nil, errors.New("unknown SIP state backend")
	}
	if cfg.Generation == "" {
		cfg.Generation = "default"
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "sipbridge"
	}
	if !stateIdentity.MatchString(cfg.Namespace) || !stateIdentity.MatchString(cfg.Generation) || (cfg.Prefix != "" && !statePrefix.MatchString(cfg.Prefix)) {
		return nil, errors.New("SIP state namespace, generation or prefix invalid")
	}
	rawKey := get("SIPBRIDGE_STATE_IDENTITY_KEY")
	if rawKey == "" && cfg.Backend == state.MemoryBackend {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, errors.New("cannot create SIP state identity key")
		}
		rawKey = base64.StdEncoding.EncodeToString(key)
	}
	key, err := base64.StdEncoding.Strict().DecodeString(rawKey)
	if err != nil || len(key) < 32 || len(key) > 256 {
		return nil, errors.New("SIP state identity key must be base64 encoding of 32 to 256 bytes")
	}
	cfg.NamespaceKey = key
	if raw := get("SIPBRIDGE_STATE_PARTITIONS"); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 16)
		if err != nil || n == 0 || n > 4096 {
			return nil, errors.New("SIP state partitions must be between 1 and 4096")
		}
		cfg.Partitions = uint16(n)
	}
	if cfg.Backend == state.MemoryBackend {
		for _, name := range []string{"ADDRESS", "MASTER", "USERNAME", "PASSWORD", "SENTINEL_USERNAME", "SENTINEL_PASSWORD", "SENTINELS", "CA_FILE", "SERVER_NAME", "ALLOW_PLAINTEXT"} {
			if get("SIPBRIDGE_REDIS_"+name) != "" {
				return nil, errors.New("Redis settings require SIPBRIDGE_STATE_BACKEND=redis")
			}
		}
		if raw := get("SIPBRIDGE_MEMORY_MAX_GROUPS"); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || value < 1 || value > 1<<20 {
				return nil, errors.New("invalid SIP memory group limit")
			}
			cfg.Memory.MaxGroups = int(value)
		}
		if raw := get("SIPBRIDGE_MEMORY_MAX_BYTES"); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 8<<20 {
				return nil, errors.New("SIP memory byte limit must be at least 8388608")
			}
			cfg.Memory.MaxBytes = value
		}
		return cfg, nil
	}
	if get("SIPBRIDGE_MEMORY_MAX_GROUPS") != "" || get("SIPBRIDGE_MEMORY_MAX_BYTES") != "" {
		return nil, errors.New("memory settings require SIPBRIDGE_STATE_BACKEND=memory")
	}
	cfg.Redis = state.RedisOptions{Address: get("SIPBRIDGE_REDIS_ADDRESS"), SentinelMaster: get("SIPBRIDGE_REDIS_MASTER"),
		Username: get("SIPBRIDGE_REDIS_USERNAME"), Password: get("SIPBRIDGE_REDIS_PASSWORD"),
		SentinelUsername: get("SIPBRIDGE_REDIS_SENTINEL_USERNAME"), SentinelPassword: get("SIPBRIDGE_REDIS_SENTINEL_PASSWORD")}
	if raw := get("SIPBRIDGE_REDIS_SENTINELS"); raw != "" {
		for _, address := range strings.Split(raw, ",") {
			address = strings.TrimSpace(address)
			if !validRedisAddress(address) {
				return nil, errors.New("invalid SIP Redis Sentinel endpoint")
			}
			cfg.Redis.SentinelAddresses = append(cfg.Redis.SentinelAddresses, address)
		}
		if len(cfg.Redis.SentinelAddresses) > 16 {
			return nil, errors.New("too many SIP Redis Sentinel endpoints")
		}
	}
	if (cfg.Redis.Address != "") == (len(cfg.Redis.SentinelAddresses) > 0) || (cfg.Redis.Address != "" && !validRedisAddress(cfg.Redis.Address)) ||
		(len(cfg.Redis.SentinelAddresses) > 0 && !stateIdentity.MatchString(cfg.Redis.SentinelMaster)) {
		return nil, errors.New("select exactly one SIP Redis primary or Sentinel discovery source")
	}
	caFile, serverName := get("SIPBRIDGE_REDIS_CA_FILE"), get("SIPBRIDGE_REDIS_SERVER_NAME")
	plaintext := get("SIPBRIDGE_REDIS_ALLOW_PLAINTEXT")
	if plaintext != "" && plaintext != "false" && plaintext != "true" {
		return nil, errors.New("invalid SIP Redis plaintext setting")
	}
	if caFile == "" && serverName == "" && plaintext == "true" {
		return cfg, nil
	}
	if caFile == "" || serverName == "" || plaintext == "true" {
		return nil, errors.New("SIP Redis requires CA and verified server name, or explicit local plaintext")
	}
	ca, err := readFile(caFile)
	if err != nil {
		return nil, errors.New("cannot read SIP Redis CA bundle")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid SIP Redis CA bundle")
	}
	cfg.Redis.TLS = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName}
	return cfg, nil
}

func validRedisAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	n, parseErr := strconv.ParseUint(port, 10, 16)
	return err == nil && parseErr == nil && n > 0 && host != "" && !strings.ContainsAny(host, "/@? \t\r\n")
}

func loadStateConfiguration() (*state.Config, error) {
	return stateConfiguration(os.Getenv, os.ReadFile)
}
