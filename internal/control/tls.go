package control

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TLSFiles struct{ Certificate, Key, Trust string }

// TLSContext installs complete validated credentials for future handshakes only.
type TLSContext struct {
	files                 TLSFiles
	mu                    sync.Mutex
	hash                  [32]byte
	validFrom, validUntil time.Time
	current               atomic.Pointer[tls.Config]
	public                bool
}

func NewTLSContext(files TLSFiles) (*TLSContext, error) {
	c := &TLSContext{files: files}
	if err := c.Reload(); err != nil {
		return nil, err
	}
	return c, nil
}

func NewPublicTLSContext(files TLSFiles) (*TLSContext, error) {
	c := &TLSContext{files: files, public: true}
	if err := c.Reload(); err != nil {
		return nil, err
	}
	return c, nil
}
func (c *TLSContext) Valid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	return c.current.Load() != nil && !now.Before(c.validFrom) && now.Before(c.validUntil)
}

func (c *TLSContext) Reload() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	certPEM, err := os.ReadFile(c.files.Certificate)
	if err != nil {
		return fmt.Errorf("read server identity: %w", err)
	}
	keyPEM, err := os.ReadFile(c.files.Key)
	if err != nil {
		return fmt.Errorf("read server key: %w", err)
	}
	var caPEM []byte
	if !c.public {
		caPEM, err = os.ReadFile(c.files.Trust)
		if err != nil {
			return fmt.Errorf("read client trust: %w", err)
		}
	}
	digest := sha256.New()
	for _, b := range [][]byte{certPEM, keyPEM, caPEM} {
		digest.Write(b)
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	if c.current.Load() != nil && sum == c.hash {
		if now := time.Now(); now.Before(c.validFrom) || !now.Before(c.validUntil) {
			return errors.New("server certificate is outside its validity period")
		}
		return nil
	}
	identity, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return errors.New("server identity/key replacement is invalid")
	}
	leaf, err := x509.ParseCertificate(identity.Certificate[0])
	if err != nil {
		return errors.New("invalid server certificate")
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return errors.New("server certificate is outside its validity period")
	}
	pool := x509.NewCertPool()
	if !c.public && !pool.AppendCertsFromPEM(caPEM) {
		return errors.New("client trust contains no certificates")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{identity}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	if c.public {
		cfg.ClientAuth = tls.NoClientCert
		cfg.ClientCAs = nil
	}
	c.current.Store(cfg)
	c.hash = sum
	c.validFrom, c.validUntil = leaf.NotBefore, leaf.NotAfter
	return nil
}

func (c *TLSContext) Config() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return c.current.Load(), nil }}
}
