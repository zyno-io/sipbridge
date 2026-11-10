# Limits and Timers

Implemented bounds of the current service. Values are compiled in unless a
configuration variable is named. Authoritative tables are in
[PROTOCOL.md section 10](../spec/protocol.md#_10-bounds-scheduling-and-deadlines)
and [Runtime: service bounds](../runtime.md#service-bounds-and-encoding).
Ceilings are limits, not eager allocations.

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
| Terminal context / transaction retention floor | 2 minutes |
| Terminal subscription delivery window | 5 seconds |
| Default partitions | 64 (`SIPBRIDGE_STATE_PARTITIONS`, 1–4096) |
| Memory groups | 128 (`SIPBRIDGE_MEMORY_MAX_GROUPS`) |
| Memory reserved bytes | 1 GiB (`SIPBRIDGE_MEMORY_MAX_BYTES`), so 16 groups by default |

## Leases and recovery

| Timer | Value |
| --- | --- |
| SIP-owner term | 30 seconds |
| Term renewal | At most every 2 seconds per session |
| Recovery window after first observed expiry | 5 seconds |
| Typical owner-loss detection | About 40 seconds after last renewal |
| Protocol cleanup after owner-loss fence | 64 seconds |
| Existing-context registration transport lease | 4 hours |
| External receipt reconciliation | 120 seconds |
| Idle term TTL after last reference | 120 seconds |
| Redis clock tolerance | 250 ms |

## Scheduling

| Item | Value |
| --- | --- |
| Scheduler tick | 20 ms, visiting 1–8 partitions with a 100 ms read budget |
| Full sweep at 64 partitions | At most 160 ms, below the 500 ms SIP T1 |
| Compare-and-swap retry pause | 5 ms, within the original deadline |
| Local execution stripes | 1,024 |

## Proposed, not implemented

PROTOCOL.md also names inventory pages (128), output queues (256 messages and
8 MiB with 16 slots / 1 MiB reserved) and additional operation markers
(4,096 for five minutes). These are proposed requirements, not implemented
queues or negotiated limits.
