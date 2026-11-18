# Controlled Memory SIP comparison

This isolated localhost fixture compares Drachtio/Sofia with an explicitly
supplied sipbridge Memory image. An optional `--old-image` adds a checkpoint
fixture image supplied by you; there is no private registry dependency. It creates only uniquely named Docker containers and networks. It does
not access Redis, Kubernetes, a carrier, or an existing user container.

The application accepts synthetic identities without digest policy or a database.
Each TLS phone registers and maintains one BLF and one MWI subscription. Every
accepted initial SUBSCRIBE, refresh, and unsubscribe requires its own NOTIFY and
phone 200 response. The runner aborts on failures, missing notifications, duplicate
final responses, or NOTIFY retransmissions. It never substitutes OPTIONS traffic.
The same phone script, payloads, cadence, history, CPU and memory limits apply to
all backends. SIP TLS is fixed to TLS 1.2 with ECDHE-RSA-AES128-GCM-SHA256;
the load report records the actual negotiation. Application controllers are
measured separately, and combined CPU is reported to expose work shifted out of the SIP core.

## Run

Use Docker, Python 3.11 or newer, OpenSSL, and a local Unix Docker endpoint. The
runner builds two small fixture application images; the Drachtio application uses
Node 26 and drachtio-srf 5.0.27. All benchmark containers default to Linux amd64.
Docker may need registry access to pull the pinned core images and helper bases.

From the repository root:

```bash
python3 test/performance/run.py \
  --phones 17,130 --histories 0,2 --duration 120 --cadence 15 \
  --output /tmp/sip-memory-comparison
```

Supply a separately built candidate to include Resident Memory:

```bash
python3 test/performance/run.py \
  --candidate sipbridge-memory-candidate:local \
  --phones 17,130 --histories 0,2 --duration 120 --cadence 15 \
  --output /tmp/sip-memory-candidate-comparison
```

For a quick functional check, use `--phones 1 --histories 0 --duration 2
--cadence 1 --idle-seconds 1 --warmup 0`. Reuse the fixture application images with
`--skip-build`. Output directories must be new. Failed runs retain logs and load
results and a failure manifest, while the runner removes only its own containers and network. `--only`
selects one backend for diagnosis; `--debug` adds Sofia tracing and makes its CPU
results unsuitable for comparison. Files are copied through Docker, without
relying on host bind mounts.

## Evidence and limits

`evidence.json` contains image inspection, configuration, workload counts, latency
quantiles, successful phone NOTIFY-200 counts, setup/history counts,
duplicate/retransmission counts, and separate empty-idle,
populated-idle, and renewal measurements. CPU uses Docker's cumulative container
CPU nanoseconds, divided by elapsed wall time. RSS sums the container's processes
using Docker's process table; cgroup usage is retained separately. The controller
has its own CPU/RSS series, outside the SIP-core result.

Native allocation, live-heap and GC counters come from its existing metrics
endpoint, sampled at window boundaries. Allocation bytes/objects per second and
GC cycles per second accompany cumulative GC CPU time. Resident Memory also
reports its exact context count, accounted retained bytes, and completion-reserved
bytes in each window. These optional gauges are retained in `runtimeBefore` and
`runtimeAfter`, with availability and deltas under `residentRecords`. The baseline
checkpoint image and Drachtio do not expose these gauges; absence is explicit,
never interpreted as zero. The Drachtio image has no equivalent
cumulative allocator counter: its allocation rate is explicitly
unavailable, not zero. Capture and extra SIP tracing are disabled. Existing server
loggers are unchanged. The current runner uses native JSON/mTLS WebSocket control and
records that transport separately from Drachtio's srf admin TCP and the matched
SIP/TLS workload. Older JSON-control evidence remains evidence for its recorded
build only; do not relabel historical manifests.

History zero starts fresh. History two creates and terminates two prior generations
of both subscriptions per phone before keeping the same live population. Each
backend restarts for each population/history case. Cross-history results compare
final RSS, latest-GC live heap, and idle/active CPU after the same renewal load,
recording how long ago the prior generations ended. A finite comparison cannot
prove every record retired or establish bounded growth; RSS also includes allocator
retention, and live heap reflects the last GC rather than a forced collection.
Where all three resident gauges are present, `residentHistory` compares the final
fresh population (`before`) with the final historical population (`after`). Its
`noGrowthObserved` requires no increase in context count, accounted retained bytes,
or completion-reserved bytes; missing gauges leave this result unavailable. This
is exact accounting evidence for the measured populations, not an unbounded-history
proof. Each run uses the same logical Memory admission cap to avoid the checkpoint implementation's fictitious 64 MiB
per-group reservation; its actual container memory limit remains 512 MiB.

The local CPU threshold is `max(1.25 × Drachtio CPU, Drachtio CPU + 2 millicores)`.
An ARM host running these amd64 images uses emulation on both sides. For a
useful comparison, match the CPU architecture, traffic, limits and measurement
windows, and account for the controller's cost as well as the SIP server's.
See [Performance and load](../../docs/verification.md#performance-and-load) for
what the measurements cover. This fixture exercises phone registrations and
subscriptions; it does not exercise established calls, account authentication,
or Redis continuity. [Memory core](../../docs/memory-core.md) describes the
runtime being measured.

The one-day TLS certificates and keys are synthetic fixture material under the
private output directory. They authenticate only this local run.

Configuration references: [Drachtio server](https://drachtio.org/docs/drachtio-server)
and [Drachtio application API](https://drachtio.org/api).
