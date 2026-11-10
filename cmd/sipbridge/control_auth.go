package main

import (
	"errors"
	"strings"

	"sipbridge/internal/control"
)

type controlSecurity struct {
	TLS           *control.TLSContext
	Authenticator control.Authenticator
	AllowedURIs   []string
	AllowPlain    bool
}

func loadControlSecurity(mode, transport, tokenFile, principal, allowed string, files control.TLSFiles) (controlSecurity, error) {
	var security controlSecurity
	if transport != "tls" && transport != "plain" {
		return security, errors.New("control transport must be tls or plain")
	}
	switch mode {
	case "mtls":
		if transport != "tls" || tokenFile != "" || principal != "" {
			return security, errors.New("mutual TLS requires TLS transport and no bearer settings")
		}
		security.AllowedURIs = strings.Split(allowed, ",")
		authenticator, err := control.NewMTLSAuthenticator(security.AllowedURIs)
		if err != nil {
			return security, err
		}
		security.Authenticator = authenticator
		security.TLS, err = control.NewTLSContext(files)
		return security, err
	case "bearer":
		if allowed != "" || files.Trust != "" {
			return security, errors.New("bearer authentication does not use client CA or URI allowlist settings")
		}
		authenticator, err := control.NewBearerAuthenticator(tokenFile, principal)
		if err != nil {
			return security, err
		}
		security.Authenticator = authenticator
		if transport == "plain" {
			if files.Certificate != "" || files.Key != "" {
				return security, errors.New("plaintext control does not use TLS certificate settings")
			}
			security.AllowPlain = true
			return security, nil
		}
		security.TLS, err = control.NewPublicTLSContext(files)
		return security, err
	default:
		return security, errors.New("control authentication must be mtls or bearer")
	}
}
