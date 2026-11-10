# Development

Before a non-trivial change, read the normative
[Architecture](../spec/architecture.md) and the supplement that owns the
behavior: [Runtime](../runtime.md) for state, execution and bounds,
[Protocol](../spec/protocol.md) for wire and business admission, and
[Fixtures](../fixtures.md) for test programs. Update the owning document first
when behavior changes. Contributor rules are in
[AGENTS.md](../../AGENTS.md).

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/sipbridge` | The service executable. |
| `cmd/sipbridge-control` | Session-only control fixture: mTLS WebSocket sessions, no SIP. |
| `cmd/sipbridge-fixture` | Independent stdio native-wire fixture, driven only by `test/run.py`. |
| `internal/engine` | Ingress, native state engine, physical writer. |
| `internal/execution` | Bounded read, pure reduction and compare-and-swap driver. |
| `internal/state` | Memory and Redis state adapters. |
| `internal/control` | Control server, JSON codec validation, TLS reload. |
| `internal/controlpb` | Go types generated from the control schema. |
| `internal/capture` | Optional SIP capture exporter. |
| `third_party/sipgo` | Immutable pinned SIP library snapshot. |
| `dependencies` | Pin manifests, checksums and the read-only control schema snapshot. |
| `docs` | Normative supplements and this documentation site. |

## Rules that protect correctness

- SIP transaction, dialog and timer semantics stay in the pinned sipgo reducers.
- Never edit `third_party/sipgo`. It is verified against
  `dependencies/sipgo.json` and its checksum manifest. Generate native test
  fixtures only in the disposable copy made by `scripts/test-native.sh`.
- Do not edit generated control code or the schema snapshot by hand. The
  canonical schema lives in the original platform repository; regenerate there.
- Redis-enabled execution must not keep a second resident authoritative state
  machine.
- Never add real credentials to tests or artifacts.

## Checks

Use Go 1.24.2 and run the narrowest checks that cover the change. The full
matrix and its evidence limits are in [Verification](../verification.md).

```sh
python3 scripts/verify_dependency.py            # pinned SIP library
python3 scripts/verify_control_schema.py        # control schema snapshot hashes
test -z "$(gofmt -l cmd internal)" && go vet ./...
go build -o bin/sipbridge ./cmd/sipbridge
```

Memory state, shared state contracts and default-memory startup tests run
without Redis:

```sh
go test -race ./internal/state -run 'TestMemory|TestBackend'
go test -race ./...
```

Without `SIPBRIDGE_STATE_TEST_REDIS`, Redis-dependent state, engine and startup
tests skip, so an unconfigured run is not the qualification gate. Point it at a
disposable Redis only; tests use separate namespaces and some inject failures:

```sh
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...
```

Other gates:

```sh
sh scripts/test-native.sh                       # pinned native reducers, in a disposable copy
go build -race -o bin/sipbridge-fixture ./cmd/sipbridge-fixture && python3 test/run.py
go test -race -tags=redis_restart ./internal/state \
  -run '^TestState(StandaloneRedisRestartTotalLoss|SentinelDiscoveryAuthority|RetainedAuthoritySurvivesRealSentinelPromotion|ScopedTLSAuthoritySurvivesRealSentinelPromotion)$' \
  -count=1 -timeout=180s                        # needs a local redis-server
```

A dependency change to `third_party/sipgo` requires native branch tests and the
wire qualification again.

## Evidence boundaries

Keep these categories distinct when describing results:

| Category | Examples | What it can show |
| --- | --- | --- |
| Implemented service behavior | `cmd/sipbridge` with memory or Redis | What the service does |
| Local service checks | Go race tests, Redis process tests, startup tests | That behavior holds under local synthetic peers |
| Fixture-only utilities | `cmd/sipbridge-control`, `cmd/sipbridge-fixture`, historical 1/7 reducer fixtures | Isolated contracts. Not service mode, not production evidence |
| Historical evidence | Dated entries in [Verification](../verification.md) | What a past build did. Not current readiness |
| Proposed work | Drain protocol, context inventory, output queues in PROTOCOL.md | Requirements, not implementation |

Fixture success, a negotiated capability or a passing local suite does not prove
full platform conformance or production readiness.

## Documentation site

This site is built with [VitePress](https://vitepress.dev/) from the `docs`
directory:

```sh
cd docs
npm ci            # or npm install to create/update package-lock.json
npm run docs:dev  # local preview with hot reload
npm run docs:build
npm run docs:preview
```

The root `ARCHITECTURE.md` and `PROTOCOL.md` are rendered in place through
wrapper pages in `docs/spec/`; edit the root files, not the wrappers. Relative
links in all Markdown sources are written for repository browsing. At build
time, links to site pages become site links and links to other repository files
become source links. `SIPBRIDGE_DOCS_REPOSITORY_URL` and
`SIPBRIDGE_DOCS_SOURCE_URL` override the source link targets.

`docs/upstream/` holds proposals for the upstream SIP library and is not
published on the site. The build fails on dead internal links.

Canonical GitLab CI validates the site before `scripts/publish-docs.sh` exports
only documentation, root specification files, the link checker and the Pages
workflow to the public GitHub docs repository. It never exports runtime source
or private Git history. The publisher uses a protected, masked project variable
`SIPBRIDGE_DOCS_GITHUB_SSH_KEY_B64` containing a repository-scoped write deploy
key; SSH verifies GitHub's pinned host key. Publication is serialized and uses
normal fast-forward pushes, so conflicting publication fails rather than
overwriting history. Make edits in canonical GitLab.

Adding or changing the GitHub Actions workflow requires a maintainer identity
with workflow authority. The initial workflow is seeded through maintainer SSH;
the scoped CI key publishes ordinary documentation changes. If GitHub rejects a
workflow update for missing OAuth `workflow` scope, publish that reviewed
canonical revision with maintainer SSH (`bash scripts/publish-docs.sh`) before
retrying CI. Do not broaden the CI key to a user credential.
