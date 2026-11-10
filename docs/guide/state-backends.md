# State Backends

The SIP state store is the only authority for retained native protocol state:
transactions, dialogs, timers, owner leases and committed effects. The engine
keeps no second authoritative copy. `SIPBRIDGE_STATE_BACKEND` selects the store
once at startup:

| | `memory` (default) | `redis` (explicit) |
| --- | --- | --- |
| Intended use | One standalone instance | Workers sharing retained state |
| External dependency | None | Redis primary or Sentinel-discovered primary |
| Survives process restart | No | Retained compatible records can resume |
| Identity key | Random per process unless supplied | Required, shared by all workers |
| Controller capability | `sip.state.v1` | `sip.state.v1` |

Both stores implement the same read, reduce and compare-and-swap contract, lease
rules and retirement rules. Backend choice does not change wire fields,
capabilities or the schema 2 / reducer 9 / timers 1 profile. There is no
automatic fallback between stores, no live migration and no alternate execution
mode. Changing the backend means replacing the process.

The normative details are in [Runtime: configuration, identity and loss](../runtime.md#configuration-identity-and-loss).

## Memory

Memory state lives inside the process and is lost when it stops. Each memory
store gets a fresh authority identity, even if two processes are configured
identically, so references from another process or an earlier run can never
execute. A controller that reconnects after a restart sees a new state
authority and must establish fresh work.

Memory needs no Redis connection or credentials. Readiness checks local lifecycle
and clock health instead of Redis; signaling readiness still requires a ready
controller.

### Capacity

Each retained Call-ID group reserves the full 64 MiB per-group bound from the
moment it is first committed until it is retired. That reservation guarantees
room for every mandatory completion path, such as fork cleanup and
retransmissions. The defaults are:

| Setting | Default | Effect |
| --- | --- | --- |
| `SIPBRIDGE_MEMORY_MAX_GROUPS` | 128 | Group count ceiling |
| `SIPBRIDGE_MEMORY_MAX_BYTES` | 1 GiB | Reservation ceiling: 1 GiB / 64 MiB = **16 groups** |

With the defaults, the byte reservation is the binding limit: at most 16 Call-ID
groups are retained at once. A group can hold several native contexts. To admit
more concurrent groups, raise `SIPBRIDGE_MEMORY_MAX_BYTES` by 64 MiB per group,
and raise `SIPBRIDGE_MEMORY_MAX_GROUPS` too when going beyond 128 groups. For
example, 64 groups needs `SIPBRIDGE_MEMORY_MAX_BYTES=4294967296`.

The accounting covers encoded state, not process memory. Go objects and transient
snapshots add overhead, so size the container's memory limit with headroom and
observe real usage. When capacity is full, new groups are refused; nothing is
evicted, and existing groups keep their reserved completion capacity.

## Redis

Select Redis explicitly and supply exactly one discovery source:

```sh
SIPBRIDGE_STATE_BACKEND=redis
SIPBRIDGE_STATE_IDENTITY_KEY=<base64 of 32-256 random bytes, shared by all workers>
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
secret. TLS requires both the CA file and the server name. Plaintext requires
`SIPBRIDGE_REDIS_ALLOW_PLAINTEXT=true` with neither TLS setting, and is meant for
local, disposable Redis only.

The store opens without waiting for Redis. Until Redis is reachable, readiness
reports `503` and affected SIP work fails boundedly. An empty namespace is valid
empty state; there is no provisioning or bootstrap command.

### Requirements on the Redis deployment

- A single writable primary, optionally discovered through Sentinel. Data
  connections must reach a writable primary. Redis Cluster is not supported.
- Restrict the Redis user to the configured key prefix (`SIPBRIDGE_STATE_PREFIX`,
  default `sipstate:v2`).
- Do not let Redis evict SIP state. The project's CI uses `noeviction`.
- Every worker sharing a namespace must use the same identity key, partition
  count and state profile. Readiness cannot detect a mismatch, so deployment
  configuration must guarantee it.

### Failure and recovery

Redis replication is asynchronous. After failover sipbridge reconnects to the
writable primary. Compatible retained records resume. Missing or inconsistent
records fail the affected old operation: a call can be lost, or a client may
need to register again. Fresh requests create new records immediately. A
structurally valid rollback may be undetectable. Redis provides no rollback or
exactly-once guarantee, and sipbridge keeps no resident copy to rebuild from.

Datastore errors are not evidence that a controller died. They do not close
healthy control or public SIP sockets. While the datastore is provably
unavailable, sipbridge may send a best-effort stateless `503` to new requests
(never to ACK or responses). The race limits of that reply are described in
[Runtime: datastore failure](../runtime.md#datastore-failure-and-refusal-arbitration).
