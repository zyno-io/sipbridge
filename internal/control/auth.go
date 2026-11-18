package control

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

var ErrUnauthenticated = errors.New("control authentication failed")

// Authenticator proves the principal before any WebSocket or process claim.
// Transport security is configured separately; neither mode falls back to the other.
type Authenticator interface {
	Authenticate(*http.Request) (string, error)
	Reload() error
	Valid() bool
}

func validPrincipal(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, character := range []byte(value) {
		if character < 0x21 || character > 0x7e || character == '*' {
			return false
		}
	}
	return true
}

type MTLSAuthenticator struct{ allowed []string }

func NewMTLSAuthenticator(allowed []string) (*MTLSAuthenticator, error) {
	if len(allowed) == 0 {
		return nil, errors.New("mutual TLS requires allowed controller URI identities")
	}
	for _, identity := range allowed {
		if !validPrincipal(identity) {
			return nil, errors.New("controller URI identities must be exact")
		}
	}
	return &MTLSAuthenticator{allowed: slices.Clone(allowed)}, nil
}

func (a *MTLSAuthenticator) Authenticate(r *http.Request) (string, error) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return "", ErrUnauthenticated
	}
	// Choose by configuration order, independent of the certificate's URI ordering.
	for _, allowed := range a.allowed {
		for _, identity := range r.TLS.PeerCertificates[0].URIs {
			if identity.String() == allowed {
				return allowed, nil
			}
		}
	}
	return "", ErrUnauthenticated
}
func (*MTLSAuthenticator) Reload() error { return nil }
func (a *MTLSAuthenticator) Valid() bool { return a != nil && len(a.allowed) > 0 }

type bearerMaterial struct{ digest [sha256.Size]byte }

// BearerAuthenticator installs a complete token atomically for future upgrades.
// The file is never copied into a loggable configuration or a protocol message.
type BearerAuthenticator struct {
	file      string
	principal string
	mu        sync.Mutex
	current   atomic.Pointer[bearerMaterial]
}

func NewBearerAuthenticator(file, principal string) (*BearerAuthenticator, error) {
	if file == "" || !validPrincipal(principal) {
		return nil, errors.New("bearer authentication requires a token file and stable principal")
	}
	a := &BearerAuthenticator{file: file, principal: principal}
	if err := a.Reload(); err != nil {
		return nil, err
	}
	return a, nil
}

func (*BearerAuthenticator) String() string   { return "bearer authenticator [redacted]" }
func (*BearerAuthenticator) GoString() string { return "bearer authenticator [redacted]" }

func (a *BearerAuthenticator) Reload() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	file, err := os.Open(a.file)
	if err != nil {
		return errors.New("bearer token file is unavailable")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4099))
	if err != nil {
		return errors.New("bearer token file is invalid")
	}
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
	}
	if len(data) < 32 || len(data) > 4096 {
		return errors.New("bearer token must contain 32 to 4096 printable non-space bytes")
	}
	for _, character := range data {
		if character < 0x21 || character > 0x7e {
			return errors.New("bearer token contains invalid characters")
		}
	}
	a.current.Store(&bearerMaterial{digest: sha256.Sum256(data)})
	return nil
}

func (a *BearerAuthenticator) Authenticate(r *http.Request) (string, error) {
	material := a.current.Load()
	values := r.Header.Values("Authorization")
	if material == nil || len(values) != 1 {
		return "", ErrUnauthenticated
	}
	scheme, token, found := strings.Cut(values[0], " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", ErrUnauthenticated
	}
	actual := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(material.digest[:], actual[:]) != 1 {
		return "", ErrUnauthenticated
	}
	return a.principal, nil
}

func (a *BearerAuthenticator) Valid() bool { return a != nil && a.current.Load() != nil }
