---
description: What works today, what is not built, the limitations to plan around, how sipbridge is tested and where to get it.
---

# Project Status

sipbridge is early-stage software under active development. The core service
works and is described throughout these guides. Some things are not built,
some limitations are part of the current design, and its automated tests run
against simulated peers. This page sets each of those out so you can judge how
it fits what you need.

## What works today

The `sipbridge` service, with memory state (the default) or Redis:

- SIP over UDP, TCP and TLS. A dialog stays on the transport it started with.
- SIP transactions, dialogs and timers, run by a pinned build of the sipgo
  library.
- The v1 JSON control protocol over an authenticated WebSocket: client
  certificates by default, or bearer tokens; heartbeats; credential reload.
- One-time request offers answered by a single controller decision, and the
  `respond`, `openUac`, `dialogRequest` and `cancel` commands.
- Incoming and outgoing calls, re-INVITE, BYE, CANCEL, INFO, REFER, UPDATE and
  in-dialog NOTIFY; forked answers; digest authentication of outgoing requests
  with controller-supplied credentials.
- REGISTER and SUBSCRIBE handling with controller decisions; `dialog` (BLF)
  and `message-summary` (MWI) subscriptions with sipbridge-managed NOTIFY
  delivery.
- Delivery outcomes reported as `WRITTEN`, `NOT_WRITTEN` or `UNKNOWN`.
- With Redis: calls shared between instances, including continuing a call
  when a phone's TLS connection moves to another instance.
- Optional local rejection of REGISTER and SUBSCRIBE, local OPTIONS answers,
  and optional local digest challenges.
- A bounded connection drain on `SIGTERM`.
- Liveness, serving and readiness endpoints, Prometheus metrics, structured
  log events and optional SIP capture.

## Not built

| Feature | State |
| --- | --- |
| Moving calls from a disconnected or replaced controller to another one | Not available. A controller's calls end with its session. See [Controller Restarts](../guide/controller-recovery.md). |
| Replacing a sipbridge process without phones reconnecting | Not possible; sockets cannot move between processes. |
| Sharing registrations in progress or subscriptions between instances | Not available; they stay on the accepting instance even with Redis. |
| Listing every context on an instance | Not available. `inspect` reads one context you already know. |
| A client SDK | Generated Go and TypeScript types are provided; controllers implement the session and call logic themselves. |
| Redis Cluster | Not supported. Use a single primary, optionally with Sentinel. |
| PRACK, early UPDATE, REFER/Replaces call transfer flows, media | Not verified as complete features; sipbridge carries the messages but handles no media. |

## Limitations to plan around

Each of these is explained where it matters:

- **Controller sessions carry calls.** When a controller's session closes,
  its calls end. See [Restarts and Disconnects](../guide/lifecycle.md).
- **Memory state is lost on restart.** Default limits are 128 contexts and
  1 GiB. See [State Backends](../guide/state-backends.md#capacity).
- **Redis replication is asynchronous.** A failover can lose recent state and
  drop calls that depended on it. See
  [Failure and recovery](../guide/state-backends.md#failure-and-recovery).
- **Some outcomes are `UNKNOWN`.** The controller has to inspect rather than
  retry. See [Handling UNKNOWN](../protocol/commands.md#handling-unknown).
- **Restarts disconnect phones.** The drain limits the damage but phones still
  reconnect. See [Replacing a process](../guide/deployment.md#replacing-a-process).

## How it is tested

Continuous integration runs these on every change:

- Go unit and integration tests with the race detector, against memory state
  and a real Redis.
- Redis restart and Sentinel failover tests that start and stop real Redis
  processes.
- Tests that start the real `sipbridge` binary and exchange SIP and control
  messages with it over real sockets.
- The pinned SIP library's own test suite.
- A SIP-over-TLS wire test using two listener processes.

These use simulated controllers and SIP peers on one machine. They show that
the service does what its specification says under those conditions. They do
not cover what depends on your environment: your phones and carriers, media,
your controller's logic and your traffic levels. sipbridge has not been
load-tested or qualified for production use; test those things in your own
setup before relying on them. [Verification](../verification.md) lists the
exact checks.

## Test programs in the repository

| Program | Purpose |
| --- | --- |
| `cmd/sipbridge-control` | Runs control sessions only (handshake, heartbeats, strict JSON validation, certificate reload), with no SIP. Useful for testing a controller's connection code. |
| `cmd/sipbridge-fixture` | Used by the SIP-over-TLS wire test. Driven by `test/run.py`; refuses to start any other way. |

The repository also keeps an older checkpoint execution engine and its test
data for fixtures. It is not a way to run the service. [Fixtures](../fixtures.md)
describes them.

## Where to get it

The source, protocol and documentation live at
[github.com/zyno-io/sipbridge](https://github.com/zyno-io/sipbridge). Build an
image as described in [Deployment](../guide/deployment.md#image).
