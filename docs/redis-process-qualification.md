# Redis process qualification

[Architecture](../ARCHITECTURE.md) and its [runtime supplement](runtime.md) own
service behavior and exact bounds. These tests use
disposable Redis and synthetic SIP/control peers. They do not replace real
Nexus/Pilot, account authorization, media or handset qualification.

## Normal startup and complete state loss

The service starts from configuration. No provisioning command or authority
marker inventory is required. The startup tests open real SIP/control listeners,
verify bounded SIP failure while Redis is unavailable, and admit authenticated
control after Redis returns. The running-session loss case deletes only its test
namespace and requires normal reconnect with a fresh owner term, without restarting
sipbridge. The store ABA test refuses old context references and stale CAS writes
while allowing a new incarnation.

```sh
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./cmd/sipbridge ./internal/state
```

## Redis process restart and Sentinel promotion

The `redis_restart` tests own every Redis/Sentinel process they stop. They verify
actual run-ID changes, automatic recovery with the same service configuration,
fresh incarnations after total loss, retained-data failover, and verified TLS
with separate restricted data/discovery credentials.

```sh
go test -race -tags=redis_restart ./internal/state \
  -run '^TestState(StandaloneRedisRestartTotalLoss|SentinelDiscoveryAuthority|RetainedAuthoritySurvivesRealSentinelPromotion|ScopedTLSAuthoritySurvivesRealSentinelPromotion)$' \
  -count=1 -timeout=180s
```

A local `redis-server` executable is required. CI compiles a static test binary
and runs it in the pinned Redis image. The tagged tests never restart the shared
integration Redis endpoint.

## SIP processes and physical writes

The engine suite uses real OS subprocesses, JSON/mTLS control and public SIP/TLS
sockets over shared isolated Redis namespaces. Current native tests exercise
owner loss, retained SIP completion, socket reconnect, timers, directional dialog
and CSeq checks, and refusal of stale commands. Explicitly historical 1/7 fixtures
also cover pinned reducer/relay behavior;
their constructor and claimed driver are excluded from the service path.

Physical reservation tests drop actual Redis replies or partition one writer.
They preserve exact attempt identity and physical UNKNOWN/no-resend handling.
Paused-writer tests ensure an expired local computation cannot steal an already
reserved socket write. These checks do not promise exactly-once delivery after
asynchronous Redis rollback.

```sh
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./internal/engine
```

The Talk backend owns full application/media tests. Its Docker suite includes
complete SIP-state loss followed by fresh registration and a new call, as well
as a focused real Sentinel primary-SIGKILL call test. See the backend's
[E2E instructions](https://git.s24.dev/signal24/products/zyno/zynotalk-cluster-backends/-/blob/_dev/e2e/README.md).
