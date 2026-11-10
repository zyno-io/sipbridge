# Development

This page is for people changing sipbridge itself. The source is in the
[GitLab repository](https://git.s24.dev/signal24/oss/sipbridge); the GitHub
repository holds only the documentation site.

The specification documents are the design record, and each one covers a
particular area: [Architecture](../spec/architecture.md) for components and
their boundaries, [Runtime](../runtime.md) for state, execution and limits,
[Protocol](../spec/protocol.md) for the control protocol and admission rules,
and [Fixtures](../fixtures.md) for the test programs. Before a non-trivial
change, read Architecture and the document covering the area you are touching,
and when you change behavior, update that document first. Contributor rules are
in [AGENTS.md](../../AGENTS.md).

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/sipbridge` | The service executable. |
| `cmd/sipbridge-control` | Test program that runs control sessions only (mutual TLS WebSocket, no SIP). |
| `cmd/sipbridge-fixture` | Test program for SIP-over-TLS wire tests, driven only by `test/run.py`. |
| `internal/engine` | Public listeners, the SIP state engine and the socket writer. |
| `internal/execution` | The read, compute, compare-and-swap loop that every operation goes through. |
| `internal/state` | Memory and Redis state stores. |
| `internal/control` | Control server, JSON validation, TLS reload. |
| `internal/controlpb` | Go types generated from the control schema. |
| `internal/capture` | Optional SIP capture exporter. |
| `third_party/sipgo` | The pinned copy of the SIP library. Never edited in place. |
| `dependencies` | Version pins, checksums and the read-only copy of the control schema. |
| `docs` | Specification documents and this documentation site. |

## Rules that protect correctness

- SIP transaction, dialog and timer logic stays in the pinned sipgo library. Do
  not reimplement it in sipbridge.
- Never edit `third_party/sipgo`. It is verified against
  `dependencies/sipgo.json` and its checksum manifest. Generate test fixtures
  for the library only in the disposable copy made by `scripts/test-native.sh`.
- Do not edit generated control code or the schema copy by hand. The schema's
  home is the Talk platform repository; regenerate there.
- The state store is the only place protocol state lives. Do not add a second
  in-process copy that the service treats as the truth, with either backend.
- Never add real credentials to tests or artifacts.

## Checks

Use Go 1.24.2 and run the narrowest checks that cover the change. The full list
of checks, and what each one covers, is in [Verification](../verification.md).

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

Without `SIPBRIDGE_STATE_TEST_REDIS`, the tests that need Redis are skipped, so
a run without it has not exercised the Redis store. Point the variable at a
throwaway Redis only; the tests use their own namespaces, but some deliberately
inject failures:

```sh
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...
```

CI also runs these:

```sh
sh scripts/test-native.sh                       # pinned native reducers, in a disposable copy
go build -race -o bin/sipbridge-fixture ./cmd/sipbridge-fixture && python3 test/run.py
go test -race -tags=redis_restart ./internal/state \
  -run '^TestState(StandaloneRedisRestartTotalLoss|SentinelDiscoveryAuthority|RetainedAuthoritySurvivesRealSentinelPromotion|ScopedTLSAuthoritySurvivesRealSentinelPromotion)$' \
  -count=1 -timeout=180s                        # needs a local redis-server
```

If you update `third_party/sipgo`, rerun the library tests
(`scripts/test-native.sh`) and the wire tests (`test/run.py`).

## Evidence boundaries

When you describe a test result in a document or merge request, say what kind
of test produced it. The repository has several kinds, and they show different
things:

| Kind | Examples | What it shows |
| --- | --- | --- |
| Service tests | Go race tests, Redis process tests and startup tests against `cmd/sipbridge` | The service behaves as specified when talking to simulated controllers and SIP peers on one machine. |
| Test programs | `cmd/sipbridge-control`, `cmd/sipbridge-fixture`, the older "1/7" reducer fixtures | One isolated piece works. These programs are not the service. |
| Dated notes | Entries in [Verification](../verification.md) | What an earlier build did on that date. |
| Proposals | Drain protocol, context inventory and output queues in PROTOCOL.md | A design that has not been implemented. |

The automated tests use simulated peers rather than real phones, carriers or
heavy load, so describe a result in terms of what was actually run.

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

The guide, protocol and reference pages are written for readers learning
sipbridge; keep them in plain language and put exhaustive rules in the
specification documents. The root `ARCHITECTURE.md` and `PROTOCOL.md` appear on
the site through wrapper pages in `docs/spec/`; edit the root files, not the
wrappers. Write relative links as they should work when browsing the
repository. At build time, links to site pages become site links and links to
other repository files become links to the source.
`SIPBRIDGE_DOCS_REPOSITORY_URL` and `SIPBRIDGE_DOCS_SOURCE_URL` override where
source links point.

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
