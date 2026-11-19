---
description: Fixed bounds and timers for control sessions, SIP traffic, state capacity, controller leases and shutdown, for capacity planning and controller timeouts.
---

# Limits and Timers

Every queue, table and timer in sipbridge has an upper bound, so overload
leads to refused work instead of unbounded memory growth. The values are
compiled in unless a setting is named, and they are maximums: nothing is
allocated up front. The exact rules are in
[Protocol section 10](../spec/protocol.md#_10-bounds-scheduling-and-deadlines),
[Service runtime](../runtime.md), [Resident Memory core](../memory-core.md) and
[Binary Redis core](../redis-core.md).

## Control sessions

| Resource | Bound |
| --- | --- |
| Envelope size | 1 MiB maximum; can be negotiated down to 512 bytes |
| Reference and correlation strings | `[A-Za-z0-9_.:@+-]{1,96}` |
| Capabilities per `hello` | 32 |
| Time to send `hello` | 5 seconds |
| One control write, including waiting for the writer | 2 seconds |
| Heartbeat interval / timeout | 1 second / 6 seconds |
| Control sessions | 64 |
| Controller instance IDs remembered | 256 |
| Memory of a closed session's connection attempt, and of late replies | 5 minutes |
| Pending outgoing calls / remembered late-reply markers per session | 128 / 1,024 |

## Requests, decisions and commands

| Item | Bound |
| --- | --- |
| Time to answer a `request` with a decision | About 2 seconds from arrival; then sipbridge answers `503` |
| `remainingBudgetMs` for decisions and other requests | 2,000 ms |
| `remainingBudgetMs` for a `command` | 40,000 ms |
| `admission` block | 4 KiB |
| Error text in a stored command result | 4 KiB (memory state; longer text is truncated) |
| One `dialogEvent` delivery attempt | 2 seconds; retried with pauses growing from 1 to 10 seconds until its deadline |
| Forked answers to one outgoing INVITE | 16 dialogs |

## Public SIP

| Resource | Bound |
| --- | --- |
| SIP message | 64 KiB |
| Concurrent TCP/TLS connections (inbound and outbound) and tracked flows | 4,096 each |
| UDP messages being handled or queued | 128; more are dropped without blocking the reader |
| Unused outbound TCP/TLS connection | Closed 2 minutes after its operation's deadline |
| TLS versions | TLS 1.2 and later |

## State capacity

| Resource | Default | Setting |
| --- | --- | --- |
| Inbound and outbound contexts in Memory | 128 each | `SIPBRIDGE_MEMORY_MAX_GROUPS` (1 to 1,048,576) |
| Memory state plus room to finish accepted work | 1 GiB | `SIPBRIDGE_MEMORY_MAX_BYTES` (at least 8 MiB) |
| Controller processes with a lease in Memory | 1,024 | fixed |
| Requests waiting for a controller decision per instance | 128 total, 32 per observed source address | fixed |
| Waiting requests plus retained refusals from the controller path per instance | 1,024 when admitting new requests | fixed |
| Retained refusals sipbridge generated before controller admission, per instance | 1,024 | fixed |
| Memory records kept to recognize duplicate refused requests | 4,096 | fixed |
| Inbound and outbound call legs with Redis, per instance | 1,024 each | fixed |
| Outgoing requests outside a call with Redis, per instance | 512 | fixed |
| Redis records plus reserved room per context | 32 MiB | fixed |
| Local registrations in progress and subscriptions with Redis, per instance | 131,072 inbound contexts, 1 GiB | fixed |
| Controller processes admitted at once with Redis, per namespace | 1,024 | fixed |

Requests waiting for a decision count toward the inbound limit, so there is
room to keep them if the controller accepts. These limits govern new work.
Allow extra memory for the Go runtime, network buffers and Redis overhead.
See [State Backends](../guide/state-backends.md).

## Controller leases

| Timer | Value |
| --- | --- |
| Lease lifetime | 30 seconds |
| Lease renewal | By heartbeat, at most every 2 seconds per session |
| Lease end after a session closes or misses heartbeats | Immediate |
| Idle lease record (Redis, no calls) | Removed after 120 seconds |
| Local challenge policy lifetime | At most 60 seconds ahead |

## Shutdown

| Timer | Default | Setting |
| --- | --- | --- |
| Grace before idle connections close | 5 seconds | `--sip-withdrawal-grace` (0 to 60 s) |
| Total drain until remaining connections close | 40 seconds | `--sip-drain-timeout` (above the grace, at most 2 min) |
| Completion reporting after the drain | Up to 2 seconds | fixed |
| Drain tracking per connection / per process | 64 / 8,192 outstanding transactions | fixed |

## Credentials

| Item | Value |
| --- | --- |
| Certificate, key and token reload interval | 5 seconds |
| Bearer token length | 32 to 4,096 bytes |
