---
description: "The memory and Redis state backends: what each keeps, what survives a restart or failover, capacity, and how to choose."
---

# State Backends

sipbridge has to remember things between SIP messages: which transactions are
open, where each call stands, which timers are running and which messages it
still owes a peer. You choose where it keeps that state once, at startup, with
`SIPBRIDGE_STATE_BACKEND`:

| | `memory` (default) | `redis` |
| --- | --- | --- |
| Good for | One standalone instance; development | Several instances that share calls |
| Needs | Nothing | A Redis primary, optionally found through Sentinel |
| Calls | In the process | Shared in Redis; any instance can continue them |
| Registrations in progress, subscriptions | In the process | Still in the process that accepted them |
| After a sipbridge restart | Everything that instance knew is gone | Shared calls can continue on a reconnected phone; that instance's subscriptions are gone |

Both backends use the same control protocol and capabilities; a controller
does not need to know which one is selected. sipbridge never switches by
itself: it does not fall back to memory when Redis is down, and it cannot move
live state from one backend to the other.

Neither backend keeps a controller's calls alive when that controller's
session drops: see [Restarts and Disconnects](./lifecycle.md).

## Memory

Memory state lives inside the process as ordinary data structures and is lost
when the process stops. Phones must register again and calls in progress end.

Every start is a clean slate on purpose. A restarted process reports a new
`stateAuthority.primaryId` in `helloAck`, and anything a controller remembered
from the previous run (context references, pending commands) is refused rather
than misapplied. A controller that sees a new `primaryId` should treat its old
calls on that instance as ended.

### Capacity

Two settings bound how much work memory state admits:

| Setting | Default | Effect |
| --- | --- | --- |
| `SIPBRIDGE_MEMORY_MAX_GROUPS` | 128 | Up to 128 inbound contexts and 128 outbound contexts |
| `SIPBRIDGE_MEMORY_MAX_BYTES` | 1 GiB | Bytes of retained state plus room reserved for finishing work already accepted |

A context holds SIP state for a call leg, a subscription or a transaction.
When sipbridge connects an incoming call to another endpoint, it uses one
inbound context and one outbound context. Leave room for subscriptions and
registrations in progress as well.

When new work would exceed a limit, sipbridge refuses it and keeps the work
it has already accepted. The Go runtime and network buffers add to the
process's memory use. Watch the `zt_sipbridge_resident_*` metrics alongside
the process's memory use.

A completed TCP or TLS REGISTER transaction is released as soon as its
response is written and nothing else depends on it; sipbridge does not keep a
registration record. An identical retry after that point is offered to your
controller again as a new request.

## Redis

Select Redis explicitly and tell sipbridge how to reach it, either directly or
through Sentinel:

```sh
SIPBRIDGE_STATE_BACKEND=redis
SIPBRIDGE_STATE_IDENTITY_KEY=...   # base64 of 32-256 random bytes, the same on every instance
SIPBRIDGE_STATE_NAMESPACE=sipbridge-example

# Either a single primary...
SIPBRIDGE_REDIS_ADDRESS=redis.example.test:6379
# ...or Sentinel discovery
# SIPBRIDGE_REDIS_SENTINELS=sentinel-0.example.test:26379,sentinel-1.example.test:26379
# SIPBRIDGE_REDIS_MASTER=sip-state

SIPBRIDGE_REDIS_USERNAME=sipbridge
SIPBRIDGE_REDIS_PASSWORD=...       # from your secret store
SIPBRIDGE_REDIS_CA_FILE=/etc/sipbridge/redis/ca.crt
SIPBRIDGE_REDIS_SERVER_NAME=redis.example.test
```

Generate an identity key with `openssl rand -base64 32` and keep it secret.
TLS needs both the CA file and the server name. To connect without TLS, set
`SIPBRIDGE_REDIS_ALLOW_PLAINTEXT=true` and leave both TLS settings out; use
that only for a throwaway Redis on your own machine.

The configuration is strict so that mistakes show up at startup: Redis
settings without `SIPBRIDGE_STATE_BACKEND=redis` stop the process, as do
memory settings combined with Redis.

sipbridge starts without waiting for Redis. Until Redis is reachable,
`/sip-readyz` returns `503` and requests that need shared state fail quickly
instead of hanging. A fresh, empty Redis needs no preparation.

### What is shared and what is not

With Redis, sipbridge splits its work in two:

- **Calls are shared.** INVITE dialogs, everything inside them (including
  NOTIFY within a call), and outgoing standalone requests are stored in Redis.
  Each operation reads only the records it needs and commits its changes
  atomically. When a phone's TLS connection drops and it reconnects to another
  instance, that instance can continue the call after checking the dialog's
  identity, and can answer the phone's BYE even without a controller.
- **Phone registrations in progress and subscriptions stay local.** An
  incoming REGISTER transaction, and the whole life of a phone's SUBSCRIBE and
  its NOTIFYs, are kept in memory on the instance that accepted them, exactly
  as with memory state. They are not copied to Redis and cannot move to
  another instance; if that instance restarts, the phone has to register and
  subscribe again. Each instance has its own memory limits for this phone
  state. See [State capacity](../reference/limits.md#state-capacity).

Registrations themselves (who is registered where) are your controller's data
in either case.

With Redis, a phone can start only REGISTER, SUBSCRIBE and INVITE outside an
existing call (OPTIONS is always answered locally). Other new standalone
requests are refused with `501`.

### Requirements on the Redis deployment

- Use a single writable primary, optionally discovered through Sentinel. Redis
  Cluster is not supported.
- Restrict the Redis user to the configured key prefix (`SIPBRIDGE_STATE_PREFIX`,
  default `sipstate:v1`). The full prefix is
  `<prefix>:<namespace>:<generation>:`.
- Do not let Redis evict keys (`maxmemory-policy noeviction`). Evicted SIP
  state breaks calls in ways that are hard to diagnose.
- Give every instance that shares a namespace the same identity key,
  namespace, generation and prefix, and run the same sipbridge build. A build
  with a different storage format refuses to use the namespace; compare the
  output of `sipbridge --state-schema` when in doubt.

### Capacity with Redis

Each instance has room for up to 1,024 inbound call legs and 1,024 outbound
call legs. When one instance handles both legs of a call, it uses one slot
in each limit. Adding an instance gives the deployment another set of
slots, even when every instance uses the same Redis namespace. Size the Redis
primary for the combined workload, including the data and indexes sipbridge
needs to finish calls.

Each instance can have up to 128 requests waiting for a controller decision,
with up to 32 from any one source address it sees. When either limit fills,
sipbridge can answer new requests with SIP `503` before asking the controller.
Those replies have their own slots, leaving the controller's slots available
as decisions finish. Calls already in progress keep their slots. An accepted
call can still open its outbound leg if that side has room.
See [State capacity](../reference/limits.md#state-capacity).

### Failure and recovery

Redis replicates asynchronously, so a failover can lose the most recent
writes. sipbridge reconnects to the new primary by itself and continues with
whatever state is there:

- State that survived continues normally.
- State that is missing fails the operation that needed it. In practice a call
  may drop.
- New calls work immediately.

Redis cannot always tell sipbridge that it lost data. If a failover rolls a
record back to an older but well-formed version, sipbridge may not notice.
There is no rollback protection and no exactly-once guarantee. Treat Redis as
what lets instances share calls, not as durable storage for them.

During a Redis outage, phone connections stay open, and local registrations
and subscriptions keep working for as long as their controller's lease is
still valid.
Shared call work fails until Redis returns, and a controller whose lease
cannot be renewed may lose its calls. Do not restart sipbridge because Redis
is down; it recovers by itself.

The exact rules are in [Service runtime](../runtime.md#selected-serving-runtime)
and [Binary Redis core](../redis-core.md).
