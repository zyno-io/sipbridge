package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func interfaceIPv4(addresses []net.Addr) (net.IP, error) {
	var selected net.IP
	for _, address := range addresses {
		ip, _, err := net.ParseCIDR(address.String())
		if err != nil || ip.To4() == nil || !ip.IsGlobalUnicast() {
			continue
		}
		if selected != nil && !selected.Equal(ip) {
			return nil, fmt.Errorf("SIP interface has multiple IPv4 addresses")
		}
		selected = ip.To4()
	}
	if selected == nil {
		return nil, fmt.Errorf("SIP interface requires one global-unicast IPv4 address")
	}
	return selected, nil
}

func bindSIPListener(address string, source net.IP) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	if host != "" && host != "0.0.0.0" && !net.ParseIP(host).Equal(source) {
		return "", fmt.Errorf("SIP listener conflicts with interface address")
	}
	if port == "" || port == "0" {
		return "", fmt.Errorf("SIP interface requires a fixed listener port")
	}
	return net.JoinHostPort(source.String(), port), nil
}

// derivePlainAdvertisement keeps public routing independent from the local bind
// host, while using the actual configured plain listener port by default.
func derivePlainAdvertisement(secure, listener, explicit string) (string, error) {
	address := explicit
	if address == "" {
		host, _, err := net.SplitHostPort(secure)
		if err != nil {
			return "", err
		}
		port := "5060"
		if listener != "" {
			_, port, err = net.SplitHostPort(listener)
			if err != nil {
				return "", err
			}
		}
		address = net.JoinHostPort(host, port)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.ContainsAny(host, " \t\r\n") {
		return "", fmt.Errorf("invalid plain SIP advertisement")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", fmt.Errorf("plain SIP advertisement requires a fixed numeric port")
	}
	return net.JoinHostPort(host, strconv.Itoa(number)), nil
}
