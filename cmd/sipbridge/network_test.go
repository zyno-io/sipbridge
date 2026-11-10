package main

import (
	"net"
	"testing"
)

func TestInterfaceIPv4RejectsMissingAndAmbiguousAddresses(t *testing.T) {
	address := func(raw string) net.Addr { _, network, _ := net.ParseCIDR(raw); return network }
	for _, addresses := range [][]net.Addr{nil, {address("fe80::1/64")}, {address("169.254.1.1/16")}, {address("192.0.2.1/32"), address("192.0.2.2/32")}} {
		if ip, err := interfaceIPv4(addresses); err == nil {
			t.Fatalf("accepted %v: %v", addresses, ip)
		}
	}
	ip, err := interfaceIPv4([]net.Addr{address("192.0.2.1/32"), address("2001:db8::1/128")})
	if err != nil || !ip.Equal(net.ParseIP("192.0.2.1")) {
		t.Fatalf("selection: %v %v", ip, err)
	}
}

func TestInterfaceBindPreservesPortAndRejectsConflictingOrDynamicListeners(t *testing.T) {
	ip := net.ParseIP("192.0.2.1")
	for _, listener := range []string{":5061", "0.0.0.0:5061", "192.0.2.1:5061"} {
		actual, err := bindSIPListener(listener, ip)
		if err != nil || actual != "192.0.2.1:5061" {
			t.Fatalf("%s: %s %v", listener, actual, err)
		}
	}
	for _, listener := range []string{"192.0.2.2:5061", "[::]:5061", ":0", "invalid"} {
		if _, err := bindSIPListener(listener, ip); err == nil {
			t.Fatalf("accepted %s", listener)
		}
	}
}

func TestPlainAdvertisementUsesPublicHostAndActualListenerPort(t *testing.T) {
	for _, test := range []struct{ secure, listener, explicit, expected string }{
		{"sip.example.test:5061", "192.0.2.1:5080", "", "sip.example.test:5080"},
		{"sip.example.test:5061", ":5060", "public-plain.example.test:6080", "public-plain.example.test:6080"},
		{"[2001:db8::1]:5061", "[::]:5080", "", "[2001:db8::1]:5080"},
		{"sip.example.test:5061", "", "", "sip.example.test:5060"},
	} {
		actual, err := derivePlainAdvertisement(test.secure, test.listener, test.explicit)
		if err != nil || actual != test.expected {
			t.Fatalf("%+v: %s %v", test, actual, err)
		}
	}
	for _, listener := range []string{":0", ":65536", "invalid"} {
		if _, err := derivePlainAdvertisement("sip.example.test:5061", listener, ""); err == nil {
			t.Fatalf("accepted %s", listener)
		}
	}
}
