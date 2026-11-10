package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sipbridge/internal/control"
)

func TestExplicitPlainBearerControlConfigurationNeedsNoPKI(t *testing.T) {
	file := filepath.Join(t.TempDir(), "control-token")
	token := strings.Repeat("synthetic-token-", 3)
	if err := os.WriteFile(file, []byte(token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	security, err := loadControlSecurity("bearer", "plain", file, "standalone", "", control.TLSFiles{})
	if err != nil {
		t.Fatal(err)
	}
	if security.TLS != nil || !security.AllowPlain || !security.Authenticator.Valid() {
		t.Fatal("explicit bearer configuration still requires client/server certificates")
	}
}

func TestControlAuthenticationConfigurationRejectsAmbiguousAndUnauthenticatedModes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "control-token")
	if err := os.WriteFile(file, []byte(strings.Repeat("synthetic-token-", 3)), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		mode, transport, token, principal, allowed string
		files                                      control.TLSFiles
	}{
		{mode: "none", transport: "plain"},
		{mode: "bearer", transport: "unknown", token: file, principal: "standalone"},
		{mode: "mtls", transport: "plain", allowed: "spiffe://test/controller"},
		{mode: "mtls", transport: "tls", token: file, principal: "standalone", allowed: "spiffe://test/controller"},
		{mode: "mtls", transport: "tls"},
		{mode: "bearer", transport: "plain", token: file},
		{mode: "bearer", transport: "plain", token: file, principal: "standalone", allowed: "spiffe://test/controller"},
		{mode: "bearer", transport: "plain", token: file, principal: "standalone", files: control.TLSFiles{Trust: "unused-ca"}},
		{mode: "bearer", transport: "plain", token: file, principal: "standalone", files: control.TLSFiles{Certificate: "unused-server-certificate"}},
		{mode: "bearer", transport: "tls", token: file, principal: "standalone"},
	} {
		if _, err := loadControlSecurity(test.mode, test.transport, test.token, test.principal, test.allowed, test.files); err == nil {
			t.Fatalf("invalid mode accepted: %s/%s", test.mode, test.transport)
		}
	}
}
