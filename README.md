# sipbridge

sipbridge is a SIP signaling server that you control from your own
application. It accepts SIP over UDP, TCP and TLS, holds the connections to
phones and trunks, and runs the protocol machinery: transactions, dialogs,
retransmissions and timers. A separate **controller**, which you write,
connects over an authenticated WebSocket and decides how each request is
answered and which requests to send.

- **No SIP state machine in your code.** Your controller receives each new
  request once, answers it with one decision, and sends typed commands such as
  "respond 200 with this SDP" or "hang up". sipbridge builds the messages and
  handles ACK, CANCEL and retransmissions.
- **Any language.** The control protocol is strict JSON over a WebSocket,
  defined by a protobuf schema, with generated Go and TypeScript types.
- **Honest results.** Every send is reported as written, not written or
  unknown. Nothing uncertain is retried behind your back.
- **Memory or Redis.** One instance keeps its state in memory. With Redis,
  several instances share call state, so a phone whose TLS connection moves to
  another instance can continue its call.

sipbridge is early-stage software. The
[project status](docs/reference/status.md) page lists what works, what is not
built and how it is tested.

## Quick start

With Go 1.24.2 or later:

```sh
go build -o bin/sipbridge ./cmd/sipbridge
./bin/sipbridge --version
```

[Getting Started](docs/guide/getting-started.md) then walks you through
starting sipbridge on loopback, connecting a small working controller, and
answering a real SIP REGISTER, in about fifteen minutes.

## Documentation

The documentation site is <https://zyno-io.github.io/sipbridge/>. Its sources
live under [docs](docs):

- [Overview](docs/guide/overview.md): what sipbridge does and the design
  choices that affect your application.
- [A Call, Step by Step](docs/guide/call-walkthrough.md): every control message
  of an incoming and an outgoing call.
- [Building a Controller](docs/guide/application.md) and the
  [Control Protocol](docs/protocol/overview.md).
- Operating guides: [configuration](docs/guide/configuration.md),
  [state backends](docs/guide/state-backends.md),
  [health](docs/guide/health.md), [deployment](docs/guide/deployment.md) and
  [troubleshooting](docs/guide/troubleshooting.md).
- Specifications: [ARCHITECTURE.md](ARCHITECTURE.md) (component boundaries),
  [PROTOCOL.md](PROTOCOL.md) (control protocol rules),
  [docs/runtime.md](docs/runtime.md) (serving runtime, bounds and lifecycle),
  [docs/memory-core.md](docs/memory-core.md) and
  [docs/redis-core.md](docs/redis-core.md) (the two state runtimes), and
  [docs/verification.md](docs/verification.md) (checks and what they cover).
  When a guide and a specification differ, the specification is right.

To preview the site locally, with Node 24:

```sh
cd docs && npm ci && npm run docs:dev
```

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/sipbridge` | The service. |
| `cmd/sipbridge-state-inspect` | Redacted, read-only inspection of one stored Redis record, offline by default. It never restores or changes state. |
| `cmd/sipbridge-control`, `cmd/sipbridge-fixture` | Test programs, not ways to run the service. See [fixtures](docs/fixtures.md). |
| `proto/` | The control protocol schema (`sipbridge-control.proto`) and shared JSON test cases. |
| `generated/typescript` | Generated TypeScript types, codecs and strict JSON validation. |
| `dependencies/` | Pins and checksums: the control schema receipt and the sipgo snapshot. |
| `third_party/sipgo` | The pinned SIP library, built from the `zyno/consolidated` branch recorded in [dependencies/sipgo.json](dependencies/sipgo.json). Never edited in place. |
| `docs/` | This documentation and the specifications. |

## Choosing state

sipbridge keeps its SIP state in process memory unless you select Redis:

```sh
SIPBRIDGE_STATE_BACKEND=redis   # plus Redis address, TLS and a shared identity key
```

Memory needs no settings; its state is lost on restart. Redis lets instances
share calls; phone registrations in progress and subscriptions stay on the
instance that accepted them. The choice is fixed at startup, with no automatic
fallback or migration. See [State Backends](docs/guide/state-backends.md).

## Building an image

```sh
SIPBRIDGE_IMAGE=sipbridge:local BUILD_VERSION=dev bash scripts/build-image.sh
docker run --rm sipbridge:local --version
```

The image runs as UID/GID 65532 from `scratch`, includes dependency license
notices, and carries its control schema receipt at
`/usr/share/sipbridge/control-schema.json`, so a controller can check that it
was built against the same schema. Deploy images by digest, and release
sipbridge independently of your controller: restarting sipbridge closes the
connections it holds. See [Deployment](docs/guide/deployment.md).

## Checks

```sh
python3 scripts/verify_dependency.py         # pinned SIP library is unmodified
python3 scripts/verify_control_schema.py     # schema and generated code agree
test -z "$(gofmt -l cmd internal)" && go vet ./...
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...
```

Point `SIPBRIDGE_STATE_TEST_REDIS` at a disposable Redis; without it the
Redis tests are skipped. [Development](docs/guide/development.md) and
[Verification](docs/verification.md) list every check, including the pinned
library's own tests and the TLS wire tests.

## Project home

The public source, protocol and documentation live at
[github.com/zyno-io/sipbridge](https://github.com/zyno-io/sipbridge).
