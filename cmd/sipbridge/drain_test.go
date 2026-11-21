package main

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStartupDrainWithdrawsReadinessBeforeClosingTLS(t *testing.T) {
	testStateStartupWithShutdown(t, "memory", false, false, "mtls", true)
}

func testStartupDrain(t *testing.T, process *exec.Cmd, client *http.Client, admin, public string, config *tls.Config, exited <-chan struct{}, processError *error) {
	t.Helper()
	phone, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", public, config)
	if err != nil {
		t.Fatal(err)
	}
	defer phone.Close()
	phone.SetDeadline(time.Now().Add(time.Second))
	if _, err = phone.Write([]byte("\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	var pong [2]byte
	if _, err = io.ReadFull(phone, pong[:]); err != nil {
		t.Fatal("idle phone not accepted before shutdown", err)
	}
	if err = process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(300 * time.Millisecond)
	withdrawn := false
	for time.Now().Before(until) {
		response, err := client.Get("http://" + admin + "/sip-readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 503 {
				withdrawn = true
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !withdrawn {
		t.Fatal("readiness was not withdrawn while runtime remained live")
	}
	for _, check := range []struct {
		path   string
		status int
	}{{"/sip-servingz", 200}, {"/healthz", 200}} {
		response, err := client.Get("http://" + admin + check.path)
		if err != nil {
			t.Fatal("admin runtime stopped before drain", err)
		}
		response.Body.Close()
		if response.StatusCode != check.status {
			t.Fatalf("%s status %d", check.path, response.StatusCode)
		}
	}
	fresh, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", public, config)
	if err != nil {
		t.Fatal("new TLS connection refused during withdrawal", err)
	}
	defer fresh.Close()
	fresh.SetDeadline(time.Now().Add(time.Second))
	options := "OPTIONS sip:bridge@test SIP/2.0\r\nVia: SIP/2.0/TLS phone.test;branch=z9hG4bK-drain-options\r\n" +
		"From: <sip:phone@test>;tag=phone\r\nTo: <sip:bridge@test>\r\nCall-ID: drain-options@test\r\n" +
		"CSeq: 1 OPTIONS\r\nMax-Forwards: 70\r\nContent-Length: 0\r\n\r\n"
	if _, err := io.WriteString(fresh, options); err != nil {
		t.Fatal(err)
	}
	status, err := bufio.NewReader(fresh).ReadString('\n')
	if err != nil || !strings.HasPrefix(status, "SIP/2.0 200 ") {
		t.Fatalf("SIP refused after readiness withdrawal: %q %v", status, err)
	}
	phone.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	_, err = phone.Read(make([]byte, 1))
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal("TLS closed before withdrawal grace", err)
	}
	phone.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = phone.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle TLS did not close after withdrawal grace")
	}
	select {
	case <-exited:
		if *processError != nil {
			t.Fatal("shutdown failed", *processError)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded shutdown did not exit")
	}
}
