---
description: Run sipbridge locally with in-memory state, connect a small working controller over loopback, and answer a real SIP request.
---

# Getting Started

In about fifteen minutes you will:

1. build sipbridge and start it on your own machine;
2. connect a small controller to it over a local WebSocket;
3. send real SIP requests and watch them get answered: one by sipbridge on its
   own, one by your controller.

Everything here runs on `127.0.0.1` with made-up names and a throwaway token.
The setup is for learning only; [Identities and TLS](./security.md) covers the
production options.

## What you need

- Go 1.24.2 or later. The SIP library sipbridge uses is already in the
  repository under `third_party/sipgo`.
- `openssl`, `curl` and Python 3 (only for the SIP test script).
- A checkout of [the sipbridge repository](https://github.com/zyno-io/sipbridge).

## 1. Build sipbridge

From the root of the checkout:

```sh
go build -o bin/sipbridge ./cmd/sipbridge
./bin/sipbridge --version
```

A local build prints `sipbridge dev`.

## 2. Create a certificate and a control token

sipbridge always runs a SIP-over-TLS listener, so it needs a certificate even
for a local test. The controller will authenticate with a bearer token, which
is just a random string in a file.

```sh
mkdir -p demo
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout demo/sip.key -out demo/sip.crt -days 7 \
  -subj "/CN=sip.example.test" \
  -addext "subjectAltName=DNS:sip.example.test,IP:127.0.0.1"
(umask 077 && openssl rand -hex 32 > demo/control-token)
```

Keep `demo/` out of version control and delete it when you are done.

## 3. Start sipbridge

```sh
./bin/sipbridge \
  --instance sipbridge-0 \
  --advertise 127.0.0.1:5061 \
  --sip-listen 127.0.0.1:5061 \
  --sip-plain-listen 127.0.0.1:5060 \
  --sip-cert demo/sip.crt --sip-key demo/sip.key \
  --control-listen 127.0.0.1:8443 \
  --control-auth bearer --control-transport plain \
  --control-bearer-token-file demo/control-token \
  --control-principal demo-controller
```

What the flags mean:

| Flag | Why |
| --- | --- |
| `--instance` | A stable name for this sipbridge process. |
| `--advertise` | The address sipbridge writes into the SIP headers it generates. Phones must be able to reach it. |
| `--sip-listen`, `--sip-plain-listen` | SIP over TLS, and SIP over UDP and TCP. |
| `--control-auth bearer --control-transport plain` | The controller connects with a token over an unencrypted WebSocket. sipbridge allows plain control only on a loopback address. |
| `--control-principal` | The identity sipbridge assigns to whoever presents the token. |

No state settings are given, so sipbridge keeps its state in memory. When the
listeners are open it prints one JSON line (your `epoch` and version differ):

```json
{"control":"127.0.0.1:8443","epoch":"6b1f0c52-6a43-4d0e-9a51-2f8f7c1d9e30","event":"sipbridge-listening","instance":"sipbridge-0","profile":"sip.state.v1","revision":"unknown","sip":"127.0.0.1:5061","stateBackend":"memory","version":"dev"}
```

In a second terminal, check its health:

```sh
curl -i http://127.0.0.1:8800/healthz       # 200: the process is alive
curl -i http://127.0.0.1:8800/sip-readyz    # 503 "signaling unavailable"
```

The `503` is expected. sipbridge has no controller yet, so it has nobody to ask
what to do with a call.

## 4. Send SIP before a controller connects

Save this script as `sip-probe.py`. It sends one SIP request over UDP and
prints the status line of every reply:

```python
#!/usr/bin/env python3
"""Send one SIP request to a local sipbridge over UDP and print each reply."""
import socket
import sys
import uuid

method = (sys.argv[1] if len(sys.argv) > 1 else "OPTIONS").upper()
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", 0))
sock.settimeout(5)
port = sock.getsockname()[1]

lines = [
    f"{method} sip:example.test SIP/2.0",
    f"Via: SIP/2.0/UDP 127.0.0.1:{port};branch=z9hG4bK{uuid.uuid4().hex};rport",
    "Max-Forwards: 70",
    f"From: <sip:alice@example.test>;tag={uuid.uuid4().hex[:16]}",
    "To: <sip:alice@example.test>",
    f"Call-ID: {uuid.uuid4()}",
    f"CSeq: 1 {method}",
    f"Contact: <sip:alice@127.0.0.1:{port}>",
]
if method == "REGISTER":
    lines.append("Expires: 300")
lines += ["Content-Length: 0", "", ""]
sock.sendto("\r\n".join(lines).encode(), ("127.0.0.1", 5060))

while True:
    reply, _ = sock.recvfrom(65535)
    status_line = reply.split(b"\r\n", 1)[0].decode()
    print(status_line)
    if not status_line.split()[1].startswith("1"):
        break  # a final (non-1xx) response ends the transaction
```

Try it now:

```sh
python3 sip-probe.py OPTIONS    # SIP/2.0 503 Service Unavailable
python3 sip-probe.py REGISTER   # SIP/2.0 503 Service Unavailable
```

sipbridge answered both requests itself. `OPTIONS` is a "are you there?" probe
that sipbridge always answers locally; it says `503` because no controller is
ready. The `REGISTER` needs a decision from a controller, and there isn't one.

## 5. Run a minimal controller

A **controller** is your application: the program that decides how each SIP
request is answered. The one below is about as small as a working controller
can be. It:

1. opens the WebSocket with the bearer token;
2. sends `hello` with the capabilities sipbridge requires, and reads `helloAck`;
3. sends `status` to say it is ready for work;
4. answers sipbridge's heartbeat `ping` messages;
5. answers every incoming SIP request with a `requestDecision`: `200` for
   REGISTER, `403` for everything else.

::: warning A demo, not a registrar
This controller accepts every REGISTER without checking a password and
remembers nothing. Run it only against a sipbridge on your own machine. A real
controller verifies credentials first; see
[Building a Controller](./application.md).
:::

Create a new directory outside the sipbridge checkout and save this as
`main.go`:

```go
// Command demo-controller is a minimal sipbridge controller for local testing.
// It accepts every REGISTER without checking credentials and refuses every
// other request with 403.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// envelope holds only the fields this demo reads. Values that are sent back
// unchanged, such as context references, stay as raw JSON.
type envelope struct {
	CorrelationID    string          `json:"correlationId"`
	HelloAck         *helloAck       `json:"helloAck"`
	Ping             *heartbeat      `json:"ping"`
	Request          *request        `json:"request"`
	Result           *result         `json:"result"`
	DialogEvent      *dialogEvent    `json:"dialogEvent"`
	FlowAdoption     json.RawMessage `json:"flowAdoption"`
	CredentialLookup json.RawMessage `json:"credentialLookup"`
	FlowClosed       json.RawMessage `json:"flowClosed"`
}

type helloAck struct {
	SessionID    string   `json:"sessionId"`
	Capabilities []string `json:"capabilities"`
}

type heartbeat struct {
	Nonce string `json:"nonce"`
}

type request struct {
	RequestID string `json:"requestId"`
	Snapshot  struct {
		Context         json.RawMessage `json:"context"`
		CommandSequence string          `json:"commandSequence"`
		Delivery        json.RawMessage `json:"delivery"`
	} `json:"snapshot"`
	Message struct {
		Wire []byte `json:"wire"` // base64 in JSON; encoding/json decodes it
	} `json:"message"`
	FlowAdoption json.RawMessage `json:"flowAdoption"`
}

type result struct {
	Certainty string `json:"certainty"`
	SipStatus int    `json:"sipStatus"`
	Error     string `json:"error"`
}

type dialogEvent struct {
	Kind string `json:"kind"`
}

// sipbridge refuses a hello that does not offer all of these.
var capabilities = []string{
	"sip.v1", "sip.state.v1", "sip.native-completion.v1", "sip.request-decision.v1",
	"sip.command-admission.v1", "sip.command-budget.v1", "sip.dialog-continuity.v1",
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: demo-controller <control-token-file>")
	}
	token, err := os.ReadFile(os.Args[1])
	check(err)
	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, "ws://127.0.0.1:8443/v1/control", &websocket.DialOptions{
		HTTPHeader:      http.Header{"Authorization": {"Bearer " + strings.TrimSpace(string(token))}},
		Subprotocols:    []string{"sipbridge.control.json.v1"},
		CompressionMode: websocket.CompressionDisabled,
	})
	check(err)
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)

	sessionID := ""
	send := func(message map[string]any) {
		if sessionID != "" {
			message["sessionId"] = sessionID
		}
		data, err := json.Marshal(message)
		check(err)
		check(conn.Write(ctx, websocket.MessageText, data))
	}
	read := func() (envelope, []byte) {
		_, data, err := conn.Read(ctx)
		check(err)
		var in envelope
		check(json.Unmarshal(data, &in))
		return in, data
	}

	// A new random epoch for every start of this process.
	epoch := make([]byte, 16)
	_, err = rand.Read(epoch)
	check(err)
	send(map[string]any{"correlationId": "hello-1", "hello": map[string]any{
		"controller":        map[string]any{"instanceId": "demo-controller", "epoch": hex.EncodeToString(epoch)},
		"connectionAttempt": "1",
		"major":             1,
		"capabilities":      capabilities,
		"bounds":            map[string]any{"maxEnvelopeBytes": 1 << 20},
	}})
	ack, data := read()
	if ack.HelloAck == nil {
		log.Fatalf("expected helloAck, got %s", data)
	}
	sessionID = ack.HelloAck.SessionID
	log.Printf("connected; negotiated %s", strings.Join(ack.HelloAck.Capabilities, ", "))

	// Ready: sipbridge starts sending SIP work to this session.
	send(map[string]any{"correlationId": "status-1", "status": map[string]any{
		"retainedReady": true, "bindingsReady": true, "admitCalls": true, "admitSubscriptions": true,
	}})

	decisions := 0
	for {
		in, data := read()
		switch {
		case in.Ping != nil:
			send(map[string]any{"correlationId": in.CorrelationID, "pong": map[string]any{"nonce": in.Ping.Nonce}})
		case in.Request != nil:
			decisions++
			id := fmt.Sprintf("decision-%d", decisions)
			decision, summary := decide(in.Request)
			log.Printf("%s: %s", id, summary)
			send(map[string]any{"correlationId": id, "remainingBudgetMs": 2000, "requestDecision": decision})
		case in.Result != nil:
			log.Printf("%s: result %s %d %s", in.CorrelationID, in.Result.Certainty, in.Result.SipStatus, in.Result.Error)
		case in.DialogEvent != nil:
			log.Printf("dialog event %s", in.DialogEvent.Kind)
			send(map[string]any{"correlationId": in.CorrelationID, "result": map[string]any{"certainty": "ACCEPTED"}})
		case in.FlowAdoption != nil:
			send(map[string]any{"correlationId": in.CorrelationID, "flowAdoptionResult": map[string]any{"error": "not supported by this demo"}})
		case in.CredentialLookup != nil:
			send(map[string]any{"correlationId": in.CorrelationID, "credentialResult": map[string]any{"error": "not supported by this demo"}})
		case in.FlowClosed != nil:
			log.Printf("a SIP connection closed")
		default:
			log.Printf("ignored: %s", data)
		}
	}
}

// decide builds one requestDecision for a request sipbridge is holding.
func decide(r *request) (map[string]any, string) {
	firstLine, _, _ := strings.Cut(string(r.Message.Wire), "\r\n")
	method, _, _ := strings.Cut(firstLine, " ")
	sequence := uint64(0)
	if r.Snapshot.CommandSequence != "" {
		var err error
		sequence, err = strconv.ParseUint(r.Snapshot.CommandSequence, 10, 64)
		check(err)
	}
	admission := map[string]any{
		"directoryEpoch": "demo-directory",
		"expiresAtMs":    strconv.FormatInt(time.Now().Add(2*time.Second).UnixMilli(), 10),
	}
	if len(r.Snapshot.Delivery) > 0 {
		admission["expectedDelivery"] = r.Snapshot.Delivery
	}
	decision := map[string]any{
		"context":   r.Snapshot.Context,
		"requestId": r.RequestID,
		"sequence":  strconv.FormatUint(sequence+1, 10),
		"admission": admission,
		"status":    403,
	}
	// A request on a new connection for an existing dialog asks the controller
	// to publish that connection first. This demo declines those.
	if method == "REGISTER" && len(r.FlowAdoption) == 0 {
		decision["status"] = 200
		decision["basis"] = "AUTHENTICATED_BINDING"
		decision["principalId"] = "demo-user"
		decision["bindingId"] = "demo-binding"
	}
	return decision, fmt.Sprintf("%s -> %v", firstLine, decision["status"])
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
```

Run it from that directory, pointing it at the token file:

```sh
go mod init example.test/demo-controller
go get github.com/coder/websocket@v1.8.15
go run . /path/to/sipbridge/demo/control-token
```

It logs the negotiated capabilities and then waits. Readiness has changed:

```sh
curl -i http://127.0.0.1:8800/sip-readyz    # 200
```

## 6. Send SIP again

```sh
python3 sip-probe.py OPTIONS    # SIP/2.0 200 OK
python3 sip-probe.py REGISTER   # SIP/2.0 200 OK
python3 sip-probe.py MESSAGE    # SIP/2.0 403 Forbidden
```

- `OPTIONS` is still answered by sipbridge alone, but now with `200`, because a
  controller is ready. Your controller never saw it.
- `REGISTER` reached your controller. Its log shows the request line, the
  decision it sent, and sipbridge's `ACCEPTED` result:

  ```text
  decision-1: REGISTER sip:example.test SIP/2.0 -> 200
  decision-1: result ACCEPTED 0
  ```

  `ACCEPTED` means sipbridge committed the `200` response and started sending
  it. The probe printing `SIP/2.0 200 OK` is the proof that it arrived.
- `MESSAGE` also reached the controller, which refused it with `403`.

Stop the controller with Ctrl-C. Within a few seconds `/sip-readyz` returns
`503` again, and a new REGISTER gets `503`.

## What you just saw

- sipbridge did all of the SIP work: parsing, the transaction, headers, tags
  and the UDP reply. The controller only chose a status code.
- Each request was offered to the controller **once**, as a `request` message
  holding a `requestId`. The controller answered with exactly one
  `requestDecision` for it, before the request's deadline of about two seconds.
- The decision carried a few things beyond the status: the next command
  `sequence` for that request's context, an `admission` block that says under
  which version of your data (`directoryEpoch`) the decision was made and until
  when it is valid, and for an accepted request the account it belongs to
  (`principalId`, `bindingId`).

If the controller did not connect, compare these values, which have to match:

| Value | Where it appears |
| --- | --- |
| Token file contents | `--control-bearer-token-file` and the controller's `Authorization: Bearer` header |
| `127.0.0.1:8443` | `--control-listen` and the controller's URL |
| The seven capabilities | The controller's `hello`; all are required |

[Troubleshooting](./troubleshooting.md#controller-cannot-connect) lists the
exact error for each mistake.

## Next steps

- [A Call, Step by Step](./call-walkthrough.md) follows an incoming and an
  outgoing call through the protocol.
- [Building a Controller](./application.md) explains what a production
  controller has to do.
- [Configuration](./configuration.md) lists every flag and environment variable.
