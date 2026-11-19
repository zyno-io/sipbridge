---
description: The check matrix, what each passing check shows and does not show, and the acceptance work left to integrators.
---

# Verification and evidence

Run the checks appropriate to the changed surface. A passing fixture is evidence
for its stated scope, not production conformance. [Architecture](../ARCHITECTURE.md)
and its [runtime](runtime.md) and [fixture](fixtures.md) supplements define those
boundaries. CI requires all local gates below before an image is published.
Publishing an image produces an immutable commit/digest receipt and does not
deploy it.

## Current check matrix

Use Go 1.24.2. The Redis-enabled suite needs an isolated, disposable writable Redis;
its tests use separate namespaces. Do not point failure-injection tests at production.

| Check | Command / prerequisite | Evidence |
| --- | --- | --- |
| Native dependency pin | `python3 scripts/verify_dependency.py` | Exact consolidated source/tree checksums and provenance; no edit to pinned snapshot. |
| Standalone protocol contract | `python3 scripts/verify_control_schema.py` | Schema and generated-binding hashes recorded in this repository. |
| Reproducible protocol generation | `npm ci --prefix tools/protocol`, `python3 scripts/verify_control_schema.py --regenerate`, `npm test --prefix tools/protocol`; pinned compiler tools | Locally owned schema, Go/TypeScript output and shared strict JSON fixtures, without an external checkout. |
| Private retained codec | `python3 scripts/verify_storage_schema.py`; `PROTOC=/path/to/protoc-3.21.12 python3 scripts/gen-state-storage.py --check` with protoc-gen-go 1.36.12 | Separate storage-only schema, stable field numbers and generated typed adapters; clean temporary-copy reproduction. |
| Formatting / vet | `test -z "$(gofmt -l cmd internal)"` and `go vet ./...` | This module's formatting/static checks; separate from upstream baseline warnings. |
| Service, state, control, capture and fixture tests | `SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...` | Includes real-client Redis integration, native CAS/lease/command admission, service subprocess/socket tests and explicitly historical reducer fixtures. |
| Redis/Sentinel restart | Command below; local `redis-server` executable | Tests own all processes they stop; run-ID change, total loss, retained promotion and verified TLS/scoped credentials. |
| Pinned native reducers | `sh scripts/test-native.sh` | Native branch regressions in a disposable copy, including generated TLS certificates; pinned tree stays immutable. |
| Independent TLS wire fixture | `go build -race -o bin/sipbridge-fixture ./cmd/sipbridge-fixture` then `python3 test/run.py`; Python/OpenSSL | Real two-ingress TLS/library wire behavior and source/binary hashes. Separate resident stdio fixture, not service Redis execution. |
| Service binary | `go build -o bin/sipbridge ./cmd/sipbridge` | Service builds; image CI additionally verifies version/receipt/digest. |
| Two-legged call capacity | [Call capacity test](#call-capacity-test) | Verifies 512 simultaneous two-legged TLS calls per instance with Redis and a test controller. |

Memory state, shared backend contracts and default-memory service startup run
without Redis. The shared contract suite runs both adapters when the disposable
endpoint is set. Native completion/CANCEL and public BYE write-reservation scenarios
also run against both stores. These are local contract/service checks, not production
qualification. Shared-state deployment tests must explicitly select Redis.

Without `SIPBRIDGE_STATE_TEST_REDIS`, state/engine/startup integration tests may skip.
An unconfigured `go test -race ./...` is useful locally but is not the Redis-enabled
qualification gate. CI supplies volatile Redis with `noeviction` and explicitly sets
the variable. Tests with native 1/1 leases/admission provide current service-path
evidence; historical 1/7 assertions remain separately named test-only fixture
coverage, never an alternative service setting.

```sh
go test -race -tags=redis_restart ./internal/state \
  -run '^TestState(StandaloneRedisRestartTotalLoss|SentinelDiscoveryAuthority|RetainedAuthoritySurvivesRealSentinelPromotion|ScopedTLSAuthoritySurvivesRealSentinelPromotion)$' \
  -count=1 -timeout=180s
```

CI builds the tagged test binary and runs it inside the pinned Redis image. These
tests never restart the shared integration Redis endpoint. Detailed process barriers,
physical evidence and test limits are in
[Redis process qualification](redis-process-qualification.md).

## External acceptance

Application integration tests should cover real controller/service mTLS
interoperability, two-SIP/two-controller routing and complete media flows.
[README](../README.md) records the service binary, image and schema-receipt entry
points. Verify schema and generated consumers together; checking only one side's
hashes does not prove interoperability.

Synthetic controllers cannot qualify account mutation, trusted-directory
policy, media/CDR, handset/carrier behavior, full version skew, shared load or SIP-core
drain. Redis/Sentinel
fault tests make no exactly-once or rollback-survival guarantee.

## Performance and load

### Call capacity test

Run the optional `test:call-capacity` job on a merge request, or use a
disposable local Redis with this command.

```sh
SIPBRIDGE_CAPACITY_WIRE=1 SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 \
  go test ./cmd/sipbridge -run '^TestBinaryRedisTwoLeggedCallCapacity$' -v
```

The test holds 512 two-legged TLS calls on one sipbridge instance, then holds
512 on each of two instances sharing Redis. It checks that a new caller gets
`503` when the limit for waiting requests is full, while a call already
accepted can still open its outbound leg. It also replaces and hangs up
calls. The log shows how much Redis memory the call records use and how much
room is still reserved for future writes.

Use separate tests for production traffic mixes, latency under load, CPU use
and long-running calls.

### Storage benchmark

The private codec benchmark compares complete storage codec layers on
synthetic records.

```bash
go test ./internal/engine -run '^$' -bench '^BenchmarkRetainedRecordCodec$' -benchtime=300ms -count=3
```

Rerun the current gates for any changed build; earlier test totals, artifacts or
dated observations are not present readiness. Environment-opt-in upstream
network suites are never implied by other passing gates.
