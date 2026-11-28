package capture

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type workloadCaptureCredential struct {
	fingerprint [32]byte
	cert        tls.Certificate
}

type captureCredentials struct {
	certFile, keyFile string
	credential        atomic.Pointer[workloadCaptureCredential]
	recheckAt         time.Time // Only the exporter worker accesses this after startup.
}

func NewAuthenticated(destination, instance, epoch, serverCA, certFile, keyFile string) (*Exporter, error) {
	caPEM, err := os.ReadFile(serverCA)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("invalid capture server CA")
	}
	credentials := &captureCredentials{certFile: certFile, keyFile: keyFile}
	if err := credentials.reload(time.Now()); err != nil {
		return nil, err
	}
	config := &tls.Config{
		RootCAs:    roots,
		MinVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &credentials.credential.Load().cert, nil
		},
	}
	return newExporter(destination, instance, epoch, config, credentials)
}

func (c *captureCredentials) reload(now time.Time) error {
	if now.Before(c.recheckAt) {
		return nil
	}
	c.recheckAt = now.Add(time.Minute)
	certPEM, err := os.ReadFile(c.certFile)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(certPEM)
	if current := c.credential.Load(); current != nil && current.fingerprint == digest {
		return nil
	}
	keyPEM, err := os.ReadFile(c.keyFile)
	if err != nil {
		return err
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return err
	}
	c.credential.Store(&workloadCaptureCredential{
		fingerprint: digest,
		cert:        cert,
	})
	return nil
}
