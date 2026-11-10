# Project Status

sipbridge is early-stage software under active development. The core service
works and is described throughout these guides; some features are designed but
not built; and its automated tests run against simulated peers. This page sets
out each of those so you can judge how it fits what you need.

## What works today

The `sipbridge` service, with in-memory state (the default) or Redis:

- SIP over UDP, TCP and TLS. A dialog stays on the transport it started with.
- SIP transactions, dialogs and timers, handled by a pinned build of the sipgo
  library.
- The v1 JSON control protocol over authenticated WebSocket: default mTLS,
  explicit bearer alternatives, heartbeats and credential reload.
- Controller leases and opt-in timed reattachment, with generation fencing and
  bounded owner-scoped context listing. Native SIP cleanup handles expired owners.
- Delivery outcomes reported as `WRITTEN`, `NOT_WRITTEN` or `UNKNOWN`.
- Continuing a dialog when a client's TLS connection is replaced, including on
  another instance that shares Redis state.
- Optional local rejection of REGISTER and SUBSCRIBE, and optional local digest
  challenges.
- Liveness, serving and readiness endpoints, Prometheus counters, structured
  log events and optional SIP capture.

## Not built yet

| Feature | State |
| --- | --- |
| Draining an instance before restart (`drain.begin` / `drain.status` / `drain.complete`) | Designed in PROTOCOL.md; not in the schema or the service. Restarting sipbridge closes its connections. See [Replacing a process](../guide/deployment.md#replacing-a-process). |
| Listing all contexts across an instance | Not available. Recovery exposes only the authenticated owner's bounded context pages. |
| Output queues and additional operation markers | Designed; not built. |
| Replacing a sipbridge process without disconnecting clients | Not available. |
| A client SDK for writing controllers | None. Controllers implement the JSON protocol directly. |
| Redis Cluster | Not supported. Use a single primary, optionally with Sentinel. |

## Limitations to plan around

These are properties of the current design that affect how you deploy and
build. Each is explained where it matters:

- In-memory state is lost on restart and defaults to 16 Call-IDs at a time.
  See [State Backends](../guide/state-backends.md#capacity).
- Redis replication is asynchronous, so a failover can lose recent state and
  drop the calls that depended on it. See
  [Failure and recovery](../guide/state-backends.md#failure-and-recovery).
- Controller restart recovery needs the optional capability, saved token/handle
  and restored application state. It cannot recreate lost SIP or media processes.
  See [Controller recovery](../guide/controller-recovery.md).
- Some commands and sends end as `UNKNOWN`, and the controller has to inspect
  rather than retry. See
  [Commands and Outcomes](../protocol/commands.md#handling-unknown).

## How it is tested

Continuous integration runs these on every change, before an image is
published:

- Go unit and integration tests with the race detector, against both the
  memory store and a real Redis.
- Redis restart and Sentinel failover tests that start and stop real Redis
  processes.
- Tests that start the real `sipbridge` binary and exchange SIP with it over
  real sockets.
- The pinned SIP library's own test suite.
- A SIP-over-TLS wire test using two listener processes.

These scenarios use simulated controllers and SIP peers in controlled local
environments.
They show that the service does what its specification says under those
conditions. What they do not cover is everything that depends on your
environment: your phones and carriers, media, your controller's logic, and
behavior under your traffic levels. These suites do not measure capacity under your workload. Test those things
in your own setup before you rely on them.

[Verification](../verification.md) lists the exact commands and what each check
covers. It also keeps dated notes from earlier builds, including a short trial
with real calls in an isolated staging environment. Those notes describe the
builds tested at the time, not the current code.

## Test programs in the repository

Two other executables live in the repository. They exist for testing and are
not alternative ways to run sipbridge:

| Program | Purpose |
| --- | --- |
| `cmd/sipbridge-control` | Runs control sessions only (handshake, heartbeats, JSON validation, certificate reload), with no SIP. Useful for testing a controller's connection code. |
| `cmd/sipbridge-fixture` | Used by the SIP-over-TLS wire test. It is driven by `test/run.py` and refuses to start any other way. |

The repository also keeps test data from an earlier internal design (labelled
"1/7" in the specification). It is not part of the service.
[Fixtures](../fixtures.md) describes all of these.

## Where to get it

The public project home is
[zyno-io/sipbridge](https://github.com/zyno-io/sipbridge). To build an image from
a source checkout, follow [Deployment](../guide/deployment.md#image).
