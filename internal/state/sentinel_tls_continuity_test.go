//go:build redis_restart

package state

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type continuityTLS struct {
	config                                  *tls.Config
	certificate, key, acl, sentinelACL      string
	operator, replication, state, discovery string
}

func newContinuityTLS(t *testing.T, prefix string) continuityTLS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Redis continuity fixture"}, DNSNames: []string{"redis.fixture.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	material, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	out := continuityTLS{certificate: filepath.Join(dir, "server.crt"), key: filepath.Join(dir, "server.key"), acl: filepath.Join(dir, "redis.acl"), sentinelACL: filepath.Join(dir, "sentinel.acl"), operator: uuid.NewString(), replication: uuid.NewString(), state: uuid.NewString(), discovery: uuid.NewString()}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(out.certificate, cert, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out.key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: material}), 0600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(cert)
	out.config = &tls.Config{RootCAs: roots, ServerName: "redis.fixture.test", MinVersion: tls.VersionTLS12}
	commands := "+hello +ping +client|setinfo +client|setname +info +mget +scan +get +set +time +type +exists +pttl +hget +hset +hincrby +hlen +hmget +zadd +zrem +zrangebyscore +zcard +zscore +pexpire +persist +del +eval +evalsha"
	acl := fmt.Sprintf("user default reset off\nuser operator reset on >%s +@all ~* &*\nuser replication reset on >%s +psync +replconf +ping\nuser sipbridge reset on >%s %s ~%s*\n", out.operator, out.replication, out.state, commands, prefix)
	sentinel := fmt.Sprintf("user default reset off\nuser operator reset on >%s +@all ~* &*\nuser discovery reset on >%s +hello +ping +info +client|setinfo +client|setname +sentinel|get-master-addr-by-name +sentinel|sentinels +sentinel|replicas +subscribe &+switch-master\n", out.operator, out.discovery)
	if err := os.WriteFile(out.acl, []byte(acl), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out.sentinelACL, []byte(sentinel), 0600); err != nil {
		t.Fatal(err)
	}
	return out
}

func (m continuityTLS) admin(t *testing.T, address string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: address, TLSConfig: m.config, Username: "operator", Password: m.operator, MaxRetries: -1, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true})
	t.Cleanup(func() { client.Close() })
	return client
}

func (m continuityTLS) node(t *testing.T) *standaloneRestartRedis {
	t.Helper()
	program, err := exec.LookPath("redis-server")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	listener.Close()
	node := &standaloneRestartRedis{program: program, address: address, port: port, dir: t.TempDir(), tls: m.config, username: "operator", password: m.operator,
		extra: []string{"--port", "0", "--tls-port", port, "--tls-cert-file", m.certificate, "--tls-key-file", m.key, "--tls-ca-cert-file", m.certificate, "--tls-auth-clients", "no", "--tls-replication", "yes", "--aclfile", m.acl, "--masteruser", "replication", "--masterauth", m.replication}}
	t.Cleanup(func() { node.stop(t, false) })
	node.start(t)
	return node
}

func (m continuityTLS) sentinel(t *testing.T, program, primaryPort string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_, port, _ := net.SplitHostPort(address)
	listener.Close()
	dir := t.TempDir()
	config := filepath.Join(dir, "sentinel.conf")
	contents := fmt.Sprintf("bind 127.0.0.1\nport 0\ntls-port %s\ntls-cert-file %s\ntls-key-file %s\ntls-ca-cert-file %s\ntls-auth-clients no\ntls-replication yes\naclfile %s\nprotected-mode yes\ndir %s\nsentinel monitor fixture 127.0.0.1 %s 2\nsentinel auth-user fixture operator\nsentinel auth-pass fixture %s\nsentinel sentinel-user operator\nsentinel sentinel-pass %s\nsentinel down-after-milliseconds fixture 500\nsentinel failover-timeout fixture 2000\nsentinel parallel-syncs fixture 1\n", port, m.certificate, m.key, m.certificate, m.sentinelACL, dir, primaryPort, m.operator, m.operator)
	if err := os.WriteFile(config, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(program, config, "--sentinel")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		command.Process.Kill()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("TLS Sentinel not reaped")
		}
	})
	admin := m.admin(t, address)
	continuityEventually(t, "TLS Sentinel listener", func() bool { return admin.Ping(context.Background()).Err() == nil })
	return address
}

func TestStateScopedTLSAuthoritySurvivesRealSentinelPromotion(t *testing.T) {
	ctx := context.Background()
	namespace := "test-" + uuid.NewString()
	prefix := "sipstate:tls-continuity"
	material := newContinuityTLS(t, prefix+":"+namespace+":")
	primary := material.node(t)
	replicas := []*standaloneRestartRedis{material.node(t), material.node(t)}
	for _, replica := range replicas {
		if err := material.admin(t, replica.address).Do(ctx, "REPLICAOF", "127.0.0.1", primary.port).Err(); err != nil {
			t.Fatal(err)
		}
	}
	endpoints := make([]string, 3)
	for n := range endpoints {
		endpoints[n] = material.sentinel(t, primary.program, primary.port)
	}
	for _, endpoint := range endpoints {
		admin := material.admin(t, endpoint)
		continuityEventually(t, "authenticated TLS Sentinel quorum", func() bool {
			raw, err := admin.Do(ctx, "SENTINEL", "SENTINELS", "fixture").Slice()
			return err == nil && len(raw) == 2
		})
	}
	cfg := Config{Redis: RedisOptions{SentinelAddresses: endpoints, SentinelMaster: "fixture", SentinelUsername: "discovery", SentinelPassword: material.discovery, Username: "sipbridge", Password: material.state, TLS: material.config, DialTimeout: 300 * time.Millisecond, IOTimeout: 500 * time.Millisecond, PoolSize: 2}, Prefix: prefix, Namespace: namespace, NamespaceKey: bytes.Repeat([]byte{92}, 32), Partitions: 2, Generation: "retained-tls", Versions: Versions{Schema: "2", Reducer: "9", Timers: "1"}, ClaimTTL: 200 * time.Millisecond}
	store, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	original := store.Authority()
	actual := store.primaryID()
	lease, ref := pinContinuityLease(t, store, "authenticated-owner")
	if err := store.client.Get(ctx, "firewall:foreign").Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatal("native client crossed namespace ACL", err)
	}
	if err := store.client.Do(ctx, "CONFIG", "GET", "*").Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatal("native client acquired administration", err)
	}
	// A missing PERSIST grant must reject before any owner reference is written.
	admin := material.admin(t, primary.address)
	if err := admin.Do(ctx, "ACL", "SETUSER", "sipbridge", "-persist").Err(); err != nil {
		t.Fatal(err)
	}
	candidate, err := store.AcquireApplicationLease(ctx, "permission-owner", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	step := begin(t, store, group(t, store, "acl-permission"), "permission-input", true)
	if _, err := store.Commit(ctx, *step, Payload{ApplicationLeaseIDs: []string{"permission-owner"}}, nil); !errors.Is(err, ErrAuthority) {
		t.Fatal("missing PERSIST permission did not reject", err)
	}
	if refs := admin.HGet(ctx, store.applicationLeaseKeys("permission-owner")[0], "references").Val(); refs != "0" {
		t.Fatal("ACL refusal partially changed owner record", refs)
	}
	if err := admin.Do(ctx, "ACL", "SETUSER", "sipbridge", "+persist").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, *step, Payload{ApplicationLeaseIDs: []string{candidate.ID}}, nil); err != nil {
		t.Fatal("restored explicit PERSIST permission did not succeed", err)
	}
	pending := begin(t, store, ref, "tls-pending", false)
	for _, replica := range replicas {
		admin := material.admin(t, replica.address)
		continuityEventually(t, "TLS replica retained state", func() bool { return admin.HGet(ctx, store.keys(ref)[0], "claimId").Val() == pending.Claim.ID })
	}
	primary.stop(t, true)
	continuityEventually(t, "verified TLS/ACL promotion adopted", func() bool { return store.Ready(ctx) == nil && store.primaryID() != actual })
	if store.Authority() != original {
		t.Fatal("TLS promotion changed logical authority")
	}
	record, err := store.Load(ctx, ref)
	if err != nil || record.ApplicationLeases[lease.ID].Term != lease.Term {
		t.Fatal("TLS promotion lost scoped lease/state", err)
	}
	if _, err := store.Commit(ctx, *pending, Payload{}, nil); !errors.Is(err, ErrStale) {
		t.Fatal("TLS old transport claim committed", err)
	}
	peer, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if peer.Authority() != original {
		t.Fatal("new TLS worker did not retain logical authority")
	}
	for _, name := range []string{"wrong-ca", "wrong-name", "wrong-discovery-password", "wrong-data-password"} {
		t.Run(name, func(t *testing.T) {
			invalid := cfg
			invalid.Redis.TLS = cfg.Redis.TLS.Clone()
			switch name {
			case "wrong-ca":
				invalid.Redis.TLS.RootCAs = x509.NewCertPool()
			case "wrong-name":
				invalid.Redis.TLS.ServerName = "wrong.fixture.test"
			case "wrong-discovery-password":
				invalid.Redis.SentinelPassword = uuid.NewString()
			case "wrong-data-password":
				invalid.Redis.Password = uuid.NewString()
			}
			attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			rejected, err := Open(attempt, invalid)
			if err == nil {
				rejected.Close()
				t.Fatal("invalid TLS/auth configuration accepted")
			}
		})
	}
}
