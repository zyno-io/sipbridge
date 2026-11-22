package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"sipbridge/internal/engine"
	"sipbridge/internal/redisclient"
	"sipbridge/internal/rediscore"
)

func nativeFixtureConfiguration(get func(string) string, readFile func(string) ([]byte, error)) (rediscore.ConnectionConfig, error) {
	value := func(key, fallback string) string {
		v := get(key)
		if v == "" {
			return fallback
		}
		return v
	}
	namespace := value("SIPBRIDGE_STATE_NAMESPACE", "sipbridge")
	generation := value("SIPBRIDGE_STATE_GENERATION", rediscore.DefaultGeneration)
	prefix := value("SIPBRIDGE_STATE_PREFIX", "sipstate:v1")
	key, err := base64.StdEncoding.Strict().DecodeString(get("SIPBRIDGE_STATE_IDENTITY_KEY"))
	if err != nil || len(key) < 32 || len(key) > 256 {
		return rediscore.ConnectionConfig{}, errors.New("fixture identity key must be base64 of 32 to 256 bytes")
	}
	config := rediscore.ConnectionConfig{Profile: rediscore.ProfileConfig{Root: prefix + ":" + namespace + ":" + generation + ":", Service: namespace, Generation: generation, IdentityKey: key, RecordBudget: 32 << 20, InstanceAdmission: true}, Redis: redisclient.Options{Address: get("SIPBRIDGE_REDIS_ADDRESS"), SentinelMaster: get("SIPBRIDGE_REDIS_MASTER"), Username: get("SIPBRIDGE_REDIS_USERNAME"), Password: get("SIPBRIDGE_REDIS_PASSWORD"), SentinelUsername: get("SIPBRIDGE_REDIS_SENTINEL_USERNAME"), SentinelPassword: get("SIPBRIDGE_REDIS_SENTINEL_PASSWORD")}}
	if raw := get("SIPBRIDGE_REDIS_SENTINELS"); raw != "" {
		for _, address := range strings.Split(raw, ",") {
			config.Redis.SentinelAddresses = append(config.Redis.SentinelAddresses, strings.TrimSpace(address))
		}
	}
	if len(config.Redis.SentinelAddresses) > 16 {
		return config, errors.New("too many fixture Redis Sentinels")
	}
	addresses := append([]string{}, config.Redis.SentinelAddresses...)
	if config.Redis.Address != "" {
		addresses = append(addresses, config.Redis.Address)
	}
	for _, address := range addresses {
		host, port, e := net.SplitHostPort(address)
		number, parseErr := strconv.ParseUint(port, 10, 16)
		if e != nil || parseErr != nil || number == 0 || host == "" || strings.ContainsAny(host, "/@? \t\r\n") {
			return config, errors.New("invalid fixture Redis endpoint")
		}
	}
	ca, serverName, plaintext := get("SIPBRIDGE_REDIS_CA_FILE"), get("SIPBRIDGE_REDIS_SERVER_NAME"), get("SIPBRIDGE_REDIS_ALLOW_PLAINTEXT")
	if !(ca == "" && serverName == "" && plaintext == "true") {
		if ca == "" || serverName == "" || plaintext != "" && plaintext != "false" {
			return config, errors.New("fixture Redis requires CA/server name or explicit plaintext")
		}
		data, e := readFile(ca)
		if e != nil {
			return config, errors.New("cannot read fixture Redis CA")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return config, errors.New("invalid fixture Redis CA")
		}
		config.Redis.TLS = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: serverName}
	}
	options, err := redisclient.Normalize(config.Redis)
	if err != nil {
		return config, err
	}
	config.Redis = options
	if _, err = rediscore.BindProfile(config.Profile); err != nil {
		return config, errors.New("invalid fixture binary profile")
	}
	return config, nil
}

func inspectNativeRedis(query engine.NativeFixtureQuery, get func(string) string, writer io.Writer) error {
	config, err := nativeFixtureConfiguration(get, os.ReadFile)
	if err != nil {
		return err
	}
	client := redisclient.New(config.Redis, config.Redis.PoolSize, nil)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	projection, err := engine.InspectNativeFixtureRedis(ctx, client, config.Profile, query)
	if err != nil {
		return err
	}
	return json.NewEncoder(writer).Encode(projection)
}
