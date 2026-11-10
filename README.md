# sipbridge

sipbridge is a SIP signaling server that you control from your own application.
It accepts SIP over UDP, TCP and TLS, holds the connections to phones and
trunks, and runs the protocol machinery: transactions, dialogs, retransmissions
and timers. A separate **controller** (your application) connects over an
authenticated JSON WebSocket and decides how each request is answered. Mutual TLS
is the default; bearer authentication is an explicit alternative.

Why you might use it:

- Your application sends short JSON commands and never implements a SIP state
  machine.
- Phones stay connected to sipbridge while you restart or redeploy the
  controller.
- With shared Redis state, a client whose TLS connection moves to another
  sipbridge instance can continue its dialog there. This is what the project
  was first built for.
- Every send is reported as written, not written or unknown, with no hidden
  retries.

sipbridge is early-stage software. The
[status page](docs/reference/status.md) lists what works, what is not built yet
and how it is tested.

## Quick start

With Go 1.24.2:

```bash
go build -o bin/sipbridge ./cmd/sipbridge
./bin/sipbridge --version
```

The process needs TLS certificates and a controller before it does anything
useful. [Getting Started](docs/guide/getting-started.md) walks through creating
test certificates, starting sipbridge with in-memory state and connecting a
small demonstration controller.

## Documentation

The documentation is published at <https://zyno-io.github.io/sipbridge/> and
lives under [docs](docs):

- [Overview](docs/guide/overview.md): what sipbridge does and the design
  choices that affect your application.
- [Guides](docs/guide/getting-started.md) for running, configuring and
  operating it, and for [building a controller](docs/guide/application.md).
- [Control protocol](docs/protocol/overview.md): the JSON messages a controller
  exchanges with sipbridge.
- Specifications for maintainers: [ARCHITECTURE.md](ARCHITECTURE.md) (component
  boundaries), [PROTOCOL.md](PROTOCOL.md) (control protocol rules),
  [docs/runtime.md](docs/runtime.md) (state, execution and limits),
  [docs/fixtures.md](docs/fixtures.md) (test programs) and
  [docs/verification.md](docs/verification.md) (checks and what they cover).
  These are the authoritative sources when a guide and a specification differ.

`cmd/sipbridge` is the service. `cmd/sipbridge-control` and
`cmd/sipbridge-fixture` are test programs. The SIP library is a pinned copy of
sipgo in `third_party/sipgo`; [dependencies/sipgo.json](dependencies/sipgo.json)
records the `zyno/consolidated` branch it comes from and the upstream proposals
for its cancellation, digest URI, random-tag, dialog identity/sequence,
transport-ingress and response-matching changes.

Build or preview the site locally with Node 24:

```bash
cd docs && npm ci && npm run docs:dev
```

[GitHub](https://github.com/zyno-io/sipbridge) contains the service source,
protocol, generated contracts, pinned SIP library and documentation. Reviewed
current-source snapshots are published with an independent public Git history;
`SOURCE_REVISION` identifies the source snapshot. The GitHub Pages workflow
builds this site from the same public tree under `/sipbridge/`.

The public project repository is [GitHub](https://github.com/zyno-io/sipbridge).
Image publication verifies dependency and schema pins, formatting, static checks,
native regression, race and TLS wire tests. Controllers choose an independently
published image by digest and verify its schema receipt. The Docker E2E harness
can use a locally built candidate image.

## State backend

The service defaults to process-local memory state, suitable for one standalone
instance. It requires no Redis settings or state identity key. State is lost on
restart; the controller remains the call-control and policy provider. Memory controllers
must negotiate `sip.state.v1`. Memory limits and recovery semantics are defined in
[Runtime](docs/runtime.md#configuration-identity-and-loss).

Deployments that share SIP state across instances must explicitly select Redis:

```bash
SIPBRIDGE_STATE_BACKEND=redis
```

Supply the existing Redis endpoint/Sentinel, TLS, namespace and shared identity-key
settings with that selection. Redis settings without explicit Redis selection fail
startup. Every backend requires the `sip.state.v1` controller capability.
Backend selection is fixed at startup; there is no automatic fallback or migration.

## Image publication

`main` publishes `:<commit-sha>` and `:main`. Other branches publish
`:<commit-sha>` and `:canary`; merge requests run checks without publication.
The manual `release:latest` job promotes the tested `main` image to `:latest`
without rebuilding it. No pipeline deploys or restarts a SIP process.

The build job verifies `/sipbridge --version`, attaches source/version/revision
OCI labels and exports `image.env` with the immutable image digest. Deployments
should select that digest independently of backend releases. `latest` is not
automatically promoted by an ordinary push.
The image carries its control-contract receipt at
`/usr/share/sipbridge/control-schema.json` so consumers can verify the pinned
image's schema before qualification without fetching the source repository.

For a local image build:

```bash
SIPBRIDGE_IMAGE=sipbridge:local BUILD_VERSION=dev bash scripts/build-image.sh
docker run --rm sipbridge:local --version
```

The scratch image runs as UID/GID 65532 and includes dependency license notices.

## Service qualification

Build with Go 1.24.2; the pinned native source is included in `third_party/sipgo`:

```bash
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...
go build -o bin/sipbridge ./cmd/sipbridge
```

Use a disposable Redis endpoint; without `SIPBRIDGE_STATE_TEST_REDIS`, central
state/engine/startup checks can skip. [Verification](docs/verification.md) is the
current test matrix, including Redis/Sentinel restart and pinned-native checks.

For a carrier-facing service, `--allow-register=false --allow-subscribe=false`
rejects those inbound methods locally with SIP 405 without consulting Redis or
the controller. Both options default to true. Call methods and SIP responses retain their
normal handling. The policy is fixed at startup; changing it requires the normal
safe SIP-process replacement procedure. See the
[runtime method policy](docs/runtime.md#process-local-method-policy).

`--require-auth-methods=REGISTER,INVITE,SUBSCRIBE` optionally moves missing-credential
initial 401 challenges into sipbridge. It requires a controller negotiating
`sip.local-challenge.v1` and publishing a finite issuance policy. Digest verification
and account/replay authorization remain in the controller and its account backend; neither passwords nor
application secrets are copied into sipbridge. Nonces fit Cisco's 127-character
buffer. Empty configuration preserves existing admission. ACK/CANCEL and retained
dialog traffic are not challenged. See the
[local challenge contract](PROTOCOL.md#local-initial-digest-challenges) before activation.

[Redis-native process/socket qualification](docs/redis-process-qualification.md)
uses separate native subprocesses, real Redis and SIP/TLS with controlled failure
barriers. Full application/media integration is a separate test surface: use real
controllers, account policy, registrations and media endpoints in your own setup.
Keep the SIP image digest pinned independently of the controller image so an
application update does not replace the process holding client connections.
Projected TLS directories reload without restarting sipbridge.

## Control-session E2E

The protocol schema is recorded in
[dependencies/sipbridge-control.proto](dependencies/sipbridge-control.proto).
[dependencies/control-schema.json](dependencies/control-schema.json) records
schema and generated-binding hashes. The session-only executable negotiates no
SIP capabilities and refuses SIP readiness; the service executable negotiates
the `sip.v1` family.

Build the session-only executable and run the control tests:

```bash
go build -race -o bin/sipbridge-control-race ./cmd/sipbridge-control
go test -race ./internal/control
python3 scripts/verify_control_schema.py
```

The Go tests cover client certificate/URI authorization, heartbeat timeout,
admission bounds, supersession, malformed frames and TLS reload. Test credentials
are temporary. Controllers should additionally test their generated JSON codecs
against a real service or the session-only executable. Check each published
image's schema receipt against the bindings used to build the controller.

Schema and generated consumer changes must be reviewed together. The recorded
hashes are checked locally without requiring another application's checkout.

## Independent native-wire fixture

The native source snapshot must match its pinned manifest. The fixture is
self-contained. A container can provide Go while the host supplies Python/OpenSSL:

```bash
python3 scripts/verify_dependency.py
docker run --rm -v "$PWD:/work/sipbridge" -w /work/sipbridge golang:1.24.2-bookworm go build -o bin/sipbridge-fixture ./cmd/sipbridge-fixture
python3 test/run.py
```

The fixture binary requires `SIPBRIDGE_FIXTURE_STDIO=1`, set only by the test
runner. It refuses default invocation rather than presenting an the fixture as
service mode. Test tokens are fixtures, not production authentication.
The dependency preflight rejects changed, missing or extra native source files;
the wire runner performs it again before executing cases.

The suite exercises real TLS on two ingress processes, one live protocol owner,
cross-ingress CANCEL and native final/cached/timer output, call signaling,
subscription refresh/expiry/unsubscribe, identity isolation, native write errors
and uncertain delivery. Artifacts contain source and binary hashes plus traces.
Run an instrumented build with `-race` and point `PROOF_GO_BINARY` and
`PROOF_GO_EVIDENCE_DIR` at its binary/artifact directory for the same suite.

Native branch tests are documented in the PR drafts under
[docs/upstream](docs/upstream). Run `sh scripts/test-native.sh` to test the pinned
source in a disposable copy, including generated upstream TLS test certificates.
[docs/verification.md](docs/verification.md) records the completed checks and
their qualification limits.

## Release intent

sipbridge is released as its own immutable image, selected by digest independently
of controller and backend releases. Restarting a SIP process drops the sockets it
holds, so the controller and its account backend, policy, discovery and ordinary certificate/config changes
must not roll it. The selected gateway functions belong in sipbridge, while
privileged kernel enforcement and shared control-plane services stay in separate
components. [Verification](docs/verification.md) records what the checks cover.
