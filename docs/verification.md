# Verification and evidence

Run the checks appropriate to the changed surface. A passing fixture is evidence
for its stated scope, not production conformance. [Architecture](../ARCHITECTURE.md)
and its [runtime](runtime.md) and [fixture](fixtures.md) supplements define those
boundaries. CI requires all local gates below before publication.

## Current check matrix

Use Go 1.24.2. The Redis-enabled suite needs an isolated, disposable writable Redis;
its tests use separate namespaces. Do not point failure-injection tests at production.

| Check | Command / prerequisite | Evidence |
| --- | --- | --- |
| Native dependency pin | `python3 scripts/verify_dependency.py` | Exact consolidated source/tree checksums and provenance; no edit to pinned snapshot. |
| Control schema snapshot | `python3 scripts/verify_control_schema.py` | Standalone schema/generated hashes. Add `--talk-checkout /path/to/talk` for canonical source/TypeScript verification. |
| Formatting / vet | `test -z "$(gofmt -l cmd internal)"` and `go vet ./...` | This module's formatting/static checks; separate from upstream baseline warnings. |
| Service, state, control, capture and fixture tests | `SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./...` | Includes real-client Redis integration, native CAS/lease/command admission, service subprocess/socket tests and explicitly historical reducer fixtures. |
| Redis/Sentinel restart | Command below; local `redis-server` executable | Tests own all processes they stop; run-ID change, total loss, retained promotion and verified TLS/scoped credentials. |
| Pinned native reducers | `sh scripts/test-native.sh` | Native branch regressions in a disposable copy, including generated TLS certificates; pinned tree stays immutable. |
| Independent TLS wire fixture | `go build -race -o bin/sipbridge-fixture ./cmd/sipbridge-fixture` then `python3 test/run.py`; Python/OpenSSL | Real two-ingress TLS/library wire behavior and source/binary hashes. Separate resident stdio fixture, not service Redis execution. |
| Service binary | `go build -o bin/sipbridge ./cmd/sipbridge` | Service builds; image CI additionally verifies version/receipt/digest. |

Memory state, shared backend contracts and default-memory service startup run
without Redis. The shared contract suite runs both adapters when the disposable
endpoint is set. Native completion/CANCEL and public BYE write-reservation scenarios
also run against both stores. These are local contract/service checks, not production
qualification. ZynoTalk deployment and E2E configuration explicitly selects Redis.

Without `SIPBRIDGE_STATE_TEST_REDIS`, state/engine/startup integration tests may skip.
An unconfigured `go test -race ./...` is useful locally but is not the Redis-enabled
qualification gate. CI supplies volatile Redis with `noeviction` and explicitly sets
the variable. Tests with native 2/9 leases/admission provide current service-path
evidence; historical 1/7 assertions remain separately named test-only compatibility
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

Talk owns real Go/TypeScript mTLS interoperability, the two-SIP/two-controller model
and complete backend Docker/media tests. [README](../README.md) records their binary,
image and schema-receipt entry points. The canonical contract owner must verify
schema/generated consumers together; copying the local snapshot is not that proof.

Synthetic controllers cannot qualify Pilot account mutation, trusted-directory
policy, media/CDR, handset/carrier behavior, full version skew, shared load or SIP-core
drain. Production selection needs separately recorded acceptance. Redis/Sentinel
fault tests make no exactly-once or rollback-survival guarantee.

## Historical evidence (not current qualification)

The observations below describe their dated builds. Rerun current gates for a changed
candidate; branch names, test totals and old artifacts are not present readiness.

- **2026-10-26, native foundation:** individual cancellation correctness/identity
  branches and their consolidated snapshot passed focused/full native checks.
  Adapter vet and 21 two-ingress stdio wire cases passed normally and under race.
  Ignored local `artifacts/qualification*` held traces and source/binary hashes.
  This was library/fixture evidence, without deployed Nexus/control integration.
- **2026-10-26, session transport:** local Go/TypeScript mTLS checks covered URI/chain
  authorization, lossless counters, supersession, malformed frames, readiness refusal
  and leaf/CA reload while sessions remained open. These established session-only
  interoperability, not application service conformance.
- **2026-10-27, JSON migration:** strict JSON validation and generated codecs passed
  shared valid/malformed cases and interoperability checks. Earlier staging evidence
  used the preceding binary wire format; it cannot qualify a later JSON candidate.
- **2026-10-27, isolated direct-TLS staging:** a real call retained native/Nexus
  ownership across ingress reconnect, with registration, BLF, re-INVITE/BYE, decoded
  media and terminal CDR observations. A held TLS socket survived a Nexus-only image
  update. Experimental resources were later scaled to zero and original Drachtio/
  Nexus restored. Exact digests, scope and restoration belong to Talk's
  [staging report](https://git.s24.dev/signal24/products/zyno/zynotalk-cluster-backends/-/blob/e2e/sipbridge-staging/docs/sipbridge-staging-evidence.md).
  This did not establish live CallSession-owner replacement or full rollout safety.
- **2026-10-28, transport extension:** pinned parsed UDP/TCP/TLS ingress and source/
  proxy checks passed; an intermediate real Docker run included UDP carrier/media
  smoke and 37 scenario artifacts. Those were intermediate observations, not a
  claim that the full compatibility or operational gates completed.

The upstream module had six baseline vet warnings in unchanged client/test files
in the foundation investigation; local adapter/native-package checks were distinct.
Environment-opt-in upstream network suites are never implied by other passing gates.
Historical task-specific alpha holds/authorizations are not current instructions;
no evidence document grants deployment authority.
