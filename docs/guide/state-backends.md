# State Backends

sipbridge has to remember things between SIP messages: which transactions are
open, where each dialog stands, which timers are running and which messages it
still owes a peer. The selected runtime is the sole authority: Memory holds typed
process-local records; Redis holds checkpoints read and committed per operation.
Neither keeps a second authoritative copy behind the selected runtime.

You choose the store once, at startup, with `SIPBRIDGE_STATE_BACKEND`:

| | `memory` (default) | `redis` |
| --- | --- | --- |
| Good for | One standalone instance; development | Several instances that share state |
| Needs | Nothing | A Redis primary, optionally found through Sentinel |
| After a sipbridge restart | State is gone | State that is still in Redis can be continued |
| Identity key | Random per process unless supplied | Required, same on every instance |

Choose memory when you run a single instance and can accept that a restart
drops calls in progress. Choose Redis when you run more than one instance and
want a client that reconnects to a different instance to continue its dialog
there.

Both runtimes use the same binary control schema and `sip.state.v1` capability.
Memory additionally requires the negotiated direct-decision contract; Redis
retains checkpoint routing/presentation. Controllers select the negotiated path,
not a guessed backend. sipbridge never switches
stores by itself: it does not fall back to memory when Redis is down, and it
cannot migrate live state from one store to the other. Changing the store means
restarting the process with new settings.

The exact rules are in [Runtime: configuration, identity and loss](../runtime.md#configuration-identity-and-loss).

## Memory

Memory state lives inside the process and is lost when the process stops.
Clients have to register again and calls in progress are gone.

Every start is a clean slate, deliberately. A restarted process reports a new
`stateAuthority.primaryId` to controllers, and anything a controller remembered
from the previous run (context references, pending commands) is refused rather
than misapplied. A controller that reconnects after a sipbridge restart should
treat its old calls on that instance as ended.

Memory needs no Redis connection or credentials. The process still needs a
ready controller before it reports ready for signaling.

### Capacity

Memory counts resident contexts and their owned retained bytes, with finite
headroom reserved for completion work admitted with each procedure. It does not
reserve 64 MiB for every Call-ID or encode a checkpoint on every operation.

| Setting | Default | Effect |
| --- | --- | --- |
| `SIPBRIDGE_MEMORY_MAX_GROUPS` | 128 | Maximum resident contexts |
| `SIPBRIDGE_MEMORY_MAX_BYTES` | 1 GiB | Accounted retained bytes plus reserved completion headroom |

These are admission bounds, not an RSS promise. Go runtime metadata and transient
transport/codec buffers add overhead. A full budget refuses new work without
evicting existing obligations. Size for live contexts, unsettled outputs and
retained results; watch actual memory alongside the resident accounting metrics.

Completed TCP/TLS REGISTER procedures can disappear once the selected final is
written, settled and acknowledged and all waiters and other duties end. They are
not retained registration records. The controller owns registration policy and
expiry. See the [owning lifetime rule](../memory-core.md#completed-reliable-register-lifetime)
for the narrow exception and old-retry UNKNOWN behavior. UDP, calls and
subscription/NOTIFY obligations keep their existing lifetimes.

## Redis

Select Redis explicitly and tell sipbridge how to find it, either directly or
through Sentinel:

```sh
SIPBRIDGE_STATE_BACKEND=redis
SIPBRIDGE_STATE_IDENTITY_KEY=<base64 of 32-256 random bytes, shared by all instances>
SIPBRIDGE_STATE_NAMESPACE=sipbridge-example

# Either a single primary...
SIPBRIDGE_REDIS_ADDRESS=redis.example.test:6379
# ...or Sentinel discovery
# SIPBRIDGE_REDIS_SENTINELS=sentinel-0.example.test:26379,sentinel-1.example.test:26379
# SIPBRIDGE_REDIS_MASTER=sip-state

SIPBRIDGE_REDIS_USERNAME=sipbridge
SIPBRIDGE_REDIS_PASSWORD=<from your secret store>
SIPBRIDGE_REDIS_CA_FILE=/etc/sipbridge/redis/ca.crt
SIPBRIDGE_REDIS_SERVER_NAME=redis.example.test
```

Generate an identity key with `openssl rand -base64 32` and store it as a
secret. TLS needs both the CA file and the server name. To connect without TLS
you must set `SIPBRIDGE_REDIS_ALLOW_PLAINTEXT=true` and leave both TLS settings
out; use that only for a throwaway Redis on your own machine.

The configuration is strict so that mistakes show up at startup. Redis settings
without `SIPBRIDGE_STATE_BACKEND=redis` stop the process, as do memory settings
combined with Redis.

sipbridge starts without waiting for Redis. Until Redis is reachable,
`/sip-readyz` returns `503` and SIP requests that need state fail quickly
instead of hanging. A fresh, empty Redis needs no preparation; there is no
setup or bootstrap command.

### Requirements on the Redis deployment

- Use a single writable primary, optionally discovered through Sentinel. Redis
  Cluster is not supported.
- Restrict the Redis user to the configured key prefix (`SIPBRIDGE_STATE_PREFIX`,
  default `sipstate:v1`).
- Do not let Redis evict keys. Evicting SIP state breaks calls in ways that are
  hard to diagnose. The project's own CI runs Redis with `noeviction`.
- Give every instance that shares a namespace the same identity key and
  partition count, and run builds that use the same state format. sipbridge
  cannot detect a mismatch at runtime, and readiness will not show one, so your
  deployment tooling has to guarantee it.

### Failure and recovery

Redis replicates asynchronously, so a failover can lose the most recent
writes. sipbridge reconnects to the new primary without help and continues with
whatever state is there:

- State that survived continues normally.
- State that is missing or inconsistent fails the operation that needed it. In
  practice a call may drop, or a client may have to register again.
- New calls and registrations work immediately.

Redis cannot always tell sipbridge that it lost data. If a failover rolls a
record back to an older but well-formed version, sipbridge may not notice.
There is no rollback protection and no exactly-once guarantee, and sipbridge
has no second copy to rebuild from. Treat Redis as what lets instances share
state and survive a sipbridge restart, not as durable storage for calls.

A Redis outage says nothing about your controller, and sipbridge does not react
as if it did: control sessions and client connections stay open, and work
resumes when Redis returns. Do not restart sipbridge because Redis is down.
While Redis is unreachable, sipbridge may answer new requests with a plain `503`
on a best-effort basis (never ACKs or responses). The edge cases of that reply
are described in
[Runtime: datastore failure](../runtime.md#datastore-failure-and-refusal-arbitration).
