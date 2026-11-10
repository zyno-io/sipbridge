// sipbridge-control runs the private session transport only. It cannot serve SIP.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"sipbridge/internal/control"
	pb "sipbridge/internal/controlpb"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8443", "private control bind address")
	cert := flag.String("cert", "", "server certificate PEM file")
	key := flag.String("key", "", "server key PEM file")
	trust := flag.String("client-ca", "", "client trust PEM file")
	allowed := flag.String("allowed-client-uri", "", "comma-separated exact workload URI SANs")
	instance := flag.String("instance", "", "stable sipbridge inventory ID")
	reload := flag.Duration("reload-interval", 5*time.Second, "credential reload interval")
	flag.Parse()
	if err := run(*listen, *cert, *key, *trust, *allowed, *instance, *reload); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(listen, cert, key, trust, allowed, instance string, reload time.Duration) error {
	credentials, err := control.NewTLSContext(control.TLSFiles{Certificate: cert, Key: key, Trust: trust})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server, err := control.NewServer(control.Config{
		Process: &pb.ProcessRef{InstanceId: instance, Epoch: uuid.NewString()}, TLS: credentials, AllowedURIs: strings.Split(allowed, ","), ReloadInterval: reload,
		OnReloadError: func(error) { fmt.Fprintln(os.Stderr, "credential reload rejected; retaining previous context") },
	})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "control-listening", "address": listener.Addr().String(), "sip": "not implemented"}); err != nil {
		return err
	}
	return server.Serve(ctx, listener)
}
