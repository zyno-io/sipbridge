# State Backends

sipbridge has to remember things between SIP messages: which transactions are
open, where each dialog stands, which timers are running and which messages it
still owes a peer. All of that lives in one place, the **state store**. The
process works from the store on every operation and keeps no separate copy of
its own, so whatever the store holds is the truth.

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

The two stores behave the same from a controller's point of view. The control
protocol, its fields and the `sip.state.v1` capability are identical, so a
controller does not need to know which one is in use. sipbridge never switches
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

The unit of accounting is the **Call-ID group**: all the state sipbridge holds
for one SIP Call-ID. Count distinct Call-IDs when sizing it: a call your
controller bridges between two parties usually has one for the inbound leg
and another for the outbound leg. A subscription holds a group for as long as it lasts. A group can
contain several related pieces, for example the forked branches of one outgoing
call.

Each group reserves a fixed 64 MiB of the memory budget from the moment it is
created until it is fully finished. The reservation is large on purpose: it
guarantees that a call which has started always has room to complete, including
cleanup after forking and every retransmission. Two settings bound the total:

| Setting | Default | Effect |
| --- | --- | --- |
| `SIPBRIDGE_MEMORY_MAX_GROUPS` | 128 | Maximum number of groups |
| `SIPBRIDGE_MEMORY_MAX_BYTES` | 1 GiB | Maximum reserved bytes: 1 GiB / 64 MiB = **16 groups** |

With the defaults the byte budget is the limit that matters, so a default
instance holds state for **16 Call-IDs at a time**. That is enough to try things
out; raise it before you send real traffic. To allow more, raise
`SIPBRIDGE_MEMORY_MAX_BYTES` by 64 MiB per group, and raise
`SIPBRIDGE_MEMORY_MAX_GROUPS` as well if you go beyond 128. For example, 64
groups needs:

```sh
SIPBRIDGE_MEMORY_MAX_BYTES=4294967296   # 64 x 64 MiB
```

Two practical notes:

- The budget counts encoded state, not the memory the process actually uses.
  A reservation is an accounting ceiling rather than allocated memory, and Go's
  own overhead comes on top of whatever the state really occupies. Watch real
  usage and give the container headroom instead of deriving its memory limit
  from this number.
- When the budget is full, sipbridge refuses new groups. It never evicts an
  existing call to make room.

A group keeps its reservation until its SIP work is completely finished, which
includes a retention period of at least two minutes after a call ends (see
[Lifecycle and Retention](./lifecycle.md#retention-and-retirement)). Size the
budget for calls that recently ended as well as calls in progress.

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
  default `sipstate:v2`).
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
