# Getting Started

This walkthrough runs one sipbridge process with the default memory state on
loopback, uses a throwaway certificate authority, and opens one control session
from a small demonstration client. Every name, key and address is synthetic.

## Prerequisites

- Go 1.24.2. The pinned SIP library is vendored in `third_party/sipgo`.
- The `openssl` command-line tool (1.1.1 or later), `curl` and a POSIX shell.

## Build

From a checkout of the repository:

```sh
go build -o bin/sipbridge ./cmd/sipbridge
./bin/sipbridge --version
```

A plain `go build` reports `sipbridge dev`; image builds set the version and
revision. To confirm the vendored SIP library matches its pinned manifest, run
`python3 scripts/verify_dependency.py`.

## Create synthetic credentials

sipbridge needs three TLS identities: a control server certificate, the public
SIP server certificate and a trust bundle for controller client certificates.
The controller needs a client certificate with a URI SAN that sipbridge will
allow. This script creates all of them from one seven-day test CA:

```sh
mkdir -p dev-pki && cd dev-pki

cat > ca.cnf <<'EOF'
[req]
distinguished_name = dn
prompt = no
[dn]
CN = sipbridge dev CA
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
openssl req -x509 -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout ca.key -out ca.crt -days 7 -config ca.cnf -extensions v3_ca

issue() { # file name, common name, extensions
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$1.key" -out "$1.csr" -subj "/CN=$2"
  printf '%s\n' "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 7 -out "$1.crt" -extfile "$1.ext"
}

# Private control listener; the controller verifies this DNS name.
issue control sipbridge-0-control "subjectAltName=DNS:sipbridge-0.control.example.test,IP:127.0.0.1
extendedKeyUsage=serverAuth"

# Controller client identity; sipbridge authorizes this exact URI SAN.
issue controller controller-a "subjectAltName=URI:spiffe://example.test/sip-controller
extendedKeyUsage=clientAuth"

# Public SIP TLS listener.
issue sip sip.example.test "subjectAltName=DNS:sip.example.test,IP:127.0.0.1
extendedKeyUsage=serverAuth"

cd ..
```

Keep `dev-pki` out of version control and delete it when you are done.

## Run

```sh
./bin/sipbridge \
  --instance sipbridge-0 \
  --advertise 127.0.0.1:5061 \
  --sip-listen 127.0.0.1:5061 \
  --sip-plain-listen 127.0.0.1:5060 \
  --sip-cert dev-pki/sip.crt --sip-key dev-pki/sip.key \
  --control-listen 127.0.0.1:8443 \
  --control-cert dev-pki/control.crt --control-key dev-pki/control.key \
  --client-ca dev-pki/ca.crt \
  --allowed-client-uri spiffe://example.test/sip-controller \
  --initial-nexus controller-a
```

No state variables are set, so the process uses memory state with a random
identity key. When the listeners are open it writes one JSON line to stdout:

```json
{"control":"127.0.0.1:8443","epoch":"<random UUID>","event":"sipbridge-listening","instance":"sipbridge-0","profile":"sip.state.v1","revision":"unknown","sip":"127.0.0.1:5061","stateBackend":"memory","version":"dev"}
```

The `epoch` changes on every start. The admin listener defaults to
`127.0.0.1:8800`:

```sh
curl -i http://127.0.0.1:8800/healthz       # 200: process is alive
curl -i http://127.0.0.1:8800/sip-servingz  # 200: listeners and public TLS are serving
curl -i http://127.0.0.1:8800/sip-readyz    # 503 "signaling unavailable": no ready controller yet
```

Without a ready controller, sipbridge refuses new SIP work with `503`. That is the
expected state until a controller connects.

## Open a demonstration control session

The program below opens a control session, sends `hello`, prints `helloAck`,
declares readiness with `status` and answers heartbeats for one minute. It is a
session demonstration, not a controller: it does not answer `ingress` routing
requests, so SIP requests sent during the demo still fail after the routing
budget.

Create a separate module outside the repository and save this as `main.go`:

```go
// Command hello-controller is a synthetic control-session demonstration.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"
)

type heartbeat struct {
	Nonce string `json:"nonce"`
}

type envelope struct {
	CorrelationID string          `json:"correlationId"`
	SessionID     string          `json:"sessionId"`
	HelloAck      json.RawMessage `json:"helloAck"`
	Ping          *heartbeat      `json:"ping"`
}

const helloTemplate = `{"correlationId":"hello-1","hello":{"nexus":{"instanceId":"controller-a","epoch":%q},` +
	`"connectionAttempt":"1","major":1,"minMinor":0,"maxMinor":0,` +
	`"capabilities":["sip.v1","sip.state.v1","sip.native-completion.v1","sip.directory-reconcile.v1",` +
	`"sip.completion-relay.v1","sip.command-admission.v1","sip.command-budget.v1","sip.dialog-continuity.v1"],` +
	`"bounds":{"maxEnvelopeBytes":1048576}}}`

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: hello-controller <pki-directory>")
	}
	dir := os.Args[1]
	identity, err := tls.LoadX509KeyPair(dir+"/controller.crt", dir+"/controller.key")
	check(err)
	caPEM, err := os.ReadFile(dir + "/ca.crt")
	check(err)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		log.Fatal("no CA certificates")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      roots,
		Certificates: []tls.Certificate{identity},
		ServerName:   "sipbridge-0.control.example.test",
	}}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "wss://127.0.0.1:8443/v1/control", &websocket.DialOptions{
		HTTPClient:      client,
		Subprotocols:    []string{"sipbridge.control.json.v1"},
		CompressionMode: websocket.CompressionDisabled,
	})
	check(err)
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	epoch := make([]byte, 16)
	_, err = rand.Read(epoch)
	check(err)
	hello := fmt.Sprintf(helloTemplate, hex.EncodeToString(epoch))
	check(conn.Write(ctx, websocket.MessageText, []byte(hello)))
	_, data, err := conn.Read(ctx)
	check(err)
	var ack envelope
	check(json.Unmarshal(data, &ack))
	if ack.HelloAck == nil {
		log.Fatalf("expected helloAck, got %s", data)
	}
	fmt.Printf("helloAck: %s\n", data)

	// Declare retained and binding readiness. The demo admits no calls or
	// subscriptions because it cannot route SIP.
	status := fmt.Sprintf(`{"correlationId":"status-1","sessionId":%q,"status":{"retainedReady":true,"bindingsReady":true}}`, ack.SessionID)
	check(conn.Write(ctx, websocket.MessageText, []byte(status)))

	for {
		_, data, err := conn.Read(ctx)
		if ctx.Err() != nil {
			conn.Close(websocket.StatusNormalClosure, "demo complete")
			return
		}
		check(err)
		var message envelope
		check(json.Unmarshal(data, &message))
		if message.Ping == nil {
			fmt.Printf("unhandled: %s\n", data)
			continue
		}
		pong := fmt.Sprintf(`{"correlationId":%q,"sessionId":%q,"pong":{"nonce":%q}}`,
			message.CorrelationID, ack.SessionID, message.Ping.Nonce)
		check(conn.Write(ctx, websocket.MessageText, []byte(pong)))
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
```

Run it against the `dev-pki` directory:

```sh
go mod init example.test/hello-controller
go get github.com/coder/websocket@v1.8.15
go run . /path/to/sipbridge/dev-pki
```

The names line up deliberately:

| Value | Where it appears |
| --- | --- |
| `spiffe://example.test/sip-controller` | Controller certificate URI SAN and `--allowed-client-uri` |
| `sipbridge-0.control.example.test` | Control certificate DNS SAN and the client's `ServerName` |
| `controller-a` | `hello.nexus.instanceId` and `--initial-nexus` |
| `sipbridge-0` | `--instance` and `helloAck.sipbridge.instanceId` |

While the demo runs, `/sip-readyz` returns `200`. After it exits, readiness
returns `503` again. The [Session](../protocol/session.md) page explains each
message.

## Next steps

- [Configuration](./configuration.md) lists every flag and environment variable.
- [State Backends](./state-backends.md) explains memory limits and explicit Redis.
- [Application Responsibilities](./application.md) describes what a real
  controller must implement.
