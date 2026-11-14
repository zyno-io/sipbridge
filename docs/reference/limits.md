# Limits and Timers

Every queue, table and timer in sipbridge has a fixed upper bound, so that
overload leads to refused work instead of unbounded memory growth. This page
collects those numbers for capacity planning and for controller authors who
need the timeouts. The values are compiled in unless a configuration variable
is named. They are maximums; nothing is allocated up front.

The tables these are drawn from are in
[PROTOCOL.md section 10](../spec/protocol.md#_10-bounds-scheduling-and-deadlines)
and [Runtime: service bounds](../runtime.md#service-bounds-and-encoding).

## Control

| Resource | Bound |
| --- | --- |
| Envelope size | 1 MiB maximum; negotiable down to 512 bytes |
| Reference and correlation strings | `[A-Za-z0-9_.:@+-]{1,96}` |
| Capabilities per `hello` | 32 |
| Hello deadline | 5 seconds |
| Control write, including writer-token wait | 2 seconds |
| Heartbeat interval / timeout | 1 second / 6 seconds |
| Control sessions | 64 |
| Controller instance claims | 256 |
| Claim and late-reply retention after disconnect | 5 minutes |
| Outbound RPC waiters / expired-reply markers per session | 128 / 1,024 |
| Ordinary RPC budget | 2 seconds |
| Command budget with `sip.command-budget.v1` | 40 seconds |
| Command admission JSON | 4 KiB |

## Public SIP

| Resource | Bound |
| --- | --- |
| SIP message | 64 KiB |
| Concurrent TCP/TLS streams (inbound and outbound) and tracked flows | 4,096 each |
| UDP admitted plus queued routing work | 128; overload drops without blocking |
| Route lookup budget | 2 seconds, including UDP queueing, location, redirects and ingest |
| Redirects per request | 2, no repeated controller |
| Initial outbound network open | 5 seconds within semantic expiry |
| Unused outbound TCP/TLS stream lease | Operation deadline plus 2 minutes |
| TLS handshake versions | TLS 1.2 and later |

## Native state

The group/encoded limits below describe checkpoint state. Resident Memory uses
[typed record accounting](../memory-core.md#what-lives-in-memory). A *group* is all the state
for one Call-ID; a *context* is one call leg, subscription or standalone
transaction within it.

| Resource | Bound |
| --- | --- |
| Encoded Call-ID group | 64 MiB |
| Contexts per group | 32 |
| Directional fork branches per context | 16 |
| Transactions per group | 128 (32 regular, 32 paired CANCEL, two BYEs per fork) |
| Emissions per transaction | 22 (server: 11 provisional and 11 final) |
| Effects / timers per group | 4,096 each |
| Store input and operation outcome | 1 MiB each |
| Arbitration proofs per server transaction | 22 |
| Retired-context markers | 128 |
| Checkpoint terminal context / transaction retention floor | 2 minutes |
| Terminal subscription delivery window | 5 seconds |
| Default partitions | 64 (`SIPBRIDGE_STATE_PARTITIONS`, 1–4096) |
| Resident Memory contexts | 128 (`SIPBRIDGE_MEMORY_MAX_GROUPS`) |
| Resident Memory accounted bytes and completion headroom | 1 GiB (`SIPBRIDGE_MEMORY_MAX_BYTES`) |

Memory completed reliable REGISTER procedures have the narrow
[lifetime exception](../memory-core.md#completed-reliable-register-lifetime); do
not apply the checkpoint floor or a per-group encoded reservation to them.

## Leases and recovery

These timers govern how sipbridge detects a lost controller; see
[Owner lease](../guide/lifecycle.md#owner-lease).

| Timer | Value |
| --- | --- |
| Ordinary controller lease lifetime | 30 seconds |
| Recoverable controller lease / grace | 5 seconds / 10 seconds, fixed in this release |
| Owner context inventory page / quota | 64 references / 4,096 references |
| Lease renewal | At most every 2 seconds per session |
| Grace period after a lease is first seen expired | 5 seconds |
| Typical time to detect a lost controller | About 35 seconds after last renewal, plus bounded scheduling/admission latency |
| SIP cleanup after a controller is declared lost | 64 seconds |
| Existing-context registration transport lease | 4 hours |
| External receipt reconciliation | 120 seconds |
| Idle term TTL after last reference | 120 seconds |
| Redis clock tolerance | 250 ms |

## Scheduling

| Item | Value |
| --- | --- |
| Worker timer ownership | One earliest-deadline entry per scheduled group |
| Worker heartbeat / lifetime | 500 ms / 2 seconds |
| Coordinator renewal / lifetime | 500 ms / 2 seconds |
| Orphan transfer | At most 64 groups per page; 500 ms idle check and 20 ms continuation |
| Unowned fallback scheduling | 20 ms, visiting 1–8 partitions with a 100 ms read budget |
| Healthy fallback sweep at 64 partitions | At most 160 ms, excluding leadership handoff |
| Local completion handoff | At most one 1 MiB optional result per active matching waiter |
| Compare-and-swap retry pause | 5 ms, within the original deadline |
| Local execution stripes | 1,024 |

## Proposed, not implemented

PROTOCOL.md also gives numbers for features that are designed but not built:
inventory pages (128), output queues (256 messages and 8 MiB, with 16 slots and
1 MiB reserved) and additional operation markers (4,096 for five minutes). They
do not apply to the current service.
