---
description: "For contributors: repository layout, the rules that protect correctness, local checks, protocol generation and the documentation site."
---

# Development

This page is for people changing sipbridge itself. The source, protocol and
documentation live in [the sipbridge repository](https://github.com/zyno-io/sipbridge);
the commands below run from a checkout.

The specification documents are the design record. [Architecture](../spec/architecture.md)
covers components and their boundaries; [Service runtime](../runtime.md),
[Resident Memory core](../memory-core.md) and [Binary Redis core](../redis-core.md)
cover state, execution and limits; [Protocol](../spec/protocol.md) covers the
control protocol; [Fixtures](../fixtures.md) covers the test programs. Before a
non-trivial change, read Architecture and the document for the area you are
touching, and when you change behavior, update that document first.
Contributor rules are in [AGENTS.md](../../AGENTS.md).

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/sipbridge` | The service executable. |
| `cmd/sipbridge-state-inspect` | Redacted, read-only inspection of stored Redis records. |
| `cmd/sipbridge-control` | Test program: control sessions only, no SIP. |
| `cmd/sipbridge-fixture` | Test program for the SIP-over-TLS wire tests, driven only by `test/run.py`. |
| `internal/engine` | Public listeners, the memory runtime, the Redis call runtime and the socket writer. |
| `internal/rediscore` | Redis records, atomic operations, leases and scheduling for shared calls. |
| `internal/control` | Control server, strict JSON decoding and validation, authentication, TLS reload. |
| `internal/controlpb` | Go types generated from the control schema. |
| `internal/storagecodec`, `internal/storagepb`, `internal/nativepb` | The private binary storage format and its generated code. |
| `internal/state`, `internal/execution` | The older checkpoint store and execution loop, kept for test fixtures. Not a serving path. |
| `internal/capture` | Optional SIP capture exporter. |
| `third_party/sipgo` | The pinned SIP library. Never edited in place. |
| `proto` | Control schema, storage schema and shared contract test cases. |
| `dependencies` | Version pins, checksums and the distributed schema copy. |
| `tools/protocol` | Pinned schema and code generation tools. |
| `generated/typescript` | Generated TypeScript codecs, bigint counters and strict JSON validation. |
| `docs` | The specifications and this documentation site. |

## Rules that protect correctness

- SIP transaction, dialog and timer decisions stay in the pinned sipgo
  library. Do not reimplement them in sipbridge.
- Never edit `third_party/sipgo`. It is verified against
  `dependencies/sipgo.json` and its checksum manifest. Generate test fixtures
  for the library only in the disposable copy made by `scripts/test-native.sh`.
- Keep the protocol schema, generated code and recorded hashes in agreement.
  Change the schema before the code that produces or consumes its messages,
  and regenerate rather than editing generated output by hand.
- Each piece of SIP state has exactly one owner: memory records, or Redis
  records for shared calls. Do not add a second copy that the service treats
  as the truth.
- Never add real credentials to tests or artifacts.

## Checks

Use Go 1.24.2 and run the narrowest checks that cover your change.
[Verification](../verification.md) lists every check and what it covers.

```sh
python3 scripts/verify_dependency.py            # pinned SIP library
python3 scripts/verify_control_schema.py        # protocol schema and generated code hashes
python3 scripts/verify_storage_schema.py        # private storage schema
test -z "$(gofmt -l cmd internal)" && go vet ./...
go build -o bin/sipbridge ./cmd/sipbridge
```

Memory state, protocol and default startup tests run without Redis:

```sh
go test -race ./...
```

Without `SIPBRIDGE_STATE_TEST_REDIS`, tests that need Redis are skipped, so
such a run has not exercised the Redis runtime. Point the variable at a
throwaway Redis only; some tests deliberately inject failures:

```sh
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...
```

CI also runs:

```sh
sh scripts/test-native.sh                       # pinned library tests, in a disposable copy
go build -race -o bin/sipbridge-fixture ./cmd/sipbridge-fixture && python3 test/run.py
go test -race -tags=redis_restart ./internal/state \
  -run '^TestState(StandaloneRedisRestartTotalLoss|SentinelDiscoveryAuthority|RetainedAuthoritySurvivesRealSentinelPromotion|ScopedTLSAuthoritySurvivesRealSentinelPromotion)$' \
  -count=1 -timeout=180s                        # needs a local redis-server
```

If you update `third_party/sipgo`, rerun the library tests
(`scripts/test-native.sh`) and the wire tests (`test/run.py`).

## Describing test results

Say what kind of test produced a result. The repository's tests use simulated
controllers and SIP peers on one machine:

| Kind | Examples | What it shows |
| --- | --- | --- |
| Service tests | Go race tests, Redis process tests, startup tests against `cmd/sipbridge` | The service behaves as specified with simulated peers. |
| Test programs | `cmd/sipbridge-control`, `cmd/sipbridge-fixture`, checkpoint fixtures | One isolated piece works. These programs are not the service. |

They do not cover real phones, carriers, media, your controller's logic or
load. Describe a result in terms of what actually ran.

## Protocol generation

The schema and shared fixtures live under `proto/`. Install the pinned Node
dependencies with `npm ci --prefix tools/protocol`, then run
`node scripts/generate-control.mjs`. Use protoc 3.21.12 and protoc-gen-go
1.36.12; `SIPBRIDGE_PROTOC` and `SIPBRIDGE_PROTOC_GEN_GO` can select their
executables.

`python3 scripts/verify_control_schema.py --regenerate` reproduces every
distributed file and verifies its receipt. `npm test --prefix tools/protocol`
strict-compiles the TypeScript distribution and checks its JSON codecs against
the shared valid and malformed cases. See the
[protocol source README](../../proto/README.md) for artifact paths.

The private storage codec is generated separately with
`PROTOC=/path/to/protoc-3.21.12 python3 scripts/gen-state-storage.py`; review
the stable field registry and its receipt with any storage change.

## Documentation site

The site is built with [VitePress](https://vitepress.dev/) from `docs/`:

```sh
cd docs
npm ci
npm run docs:dev      # local preview with hot reload
npm run docs:build
```

Guides, protocol and reference pages are for people learning sipbridge: keep
them in plain language and put exhaustive rules in the specifications. The
root `ARCHITECTURE.md` and `PROTOCOL.md` appear on the site through wrapper
pages in `docs/spec/`; edit the root files, not the wrappers. Write relative
links as they work when browsing the repository; at build time, links to site
pages become site links and links to other files become links to the source on
GitHub. `docs/upstream/` holds proposals for the upstream SIP library and is
not published.

After building, run `python3 scripts/check-docs-links.py` from the repository
root to check internal links and heading anchors. JSON examples that contain a
`correlationId` or a payload field must be complete envelopes that pass the
generated strict validator. GitHub Pages builds the published site from the
repository.
