---
description: JSON stdout events, stderr messages, Prometheus metrics on the admin listener and optional SIP message capture.
---

# Observability

sipbridge gives you four views of what it is doing: JSON event lines on stdout,
short operational messages on stderr, Prometheus counters on the admin
listener, and an optional capture of the SIP messages themselves.

It is quiet by design. It logs problems and slow operations, not a line for
every message handled, and its routine output never contains SIP message
content or credentials. When you need to see the SIP traffic itself, turn on
[SIP capture](#sip-capture).

## Metrics

`GET /metrics` on the admin listener returns Prometheus text format. Every
runtime reports these:

| Metric | Meaning |
| --- | --- |
| `zt_sipbridge_ingress_rejected_total` | Public connections or flows refused at the 4,096 stream/flow capacity. |
| `zt_sipbridge_tls_handshake_failures_total` | Failed public TLS handshakes. |
| `zt_sipbridge_invalid_messages_total` | Public SIP messages that failed framing or parsing, or exceeded 64 KiB. |
| `zt_sipbridge_method_rejected_total` | Local `405` responses from `--allow-register=false` / `--allow-subscribe=false`. |
| `zt_sipbridge_runtime_*` | Go runtime: `gomaxprocs`, `goroutines`, GC CPU seconds and cycles, heap allocation and live heap bytes. |
| `sipbridge_state_codec_*_total` | Stored-record encoding and decoding: operations, bytes, seconds and failures, labeled by fixed record `kind` and `operation`. |
| `zt_sipbridge_capture_dropped_total` | Capture records dropped because the queue was full. Only with capture enabled. |
| `zt_sipbridge_capture_export_failures_total` | Capture batches the collector did not accept. Only with capture enabled. |

With memory state, these are reported as well:

| Metric | Meaning |
| --- | --- |
| `zt_sipbridge_resident_contexts` | Contexts currently held. Compare with `SIPBRIDGE_MEMORY_MAX_GROUPS`. |
| `zt_sipbridge_resident_retained_bytes`, `zt_sipbridge_resident_completion_reserved_bytes` | Bytes held and bytes reserved for finishing accepted work. Their sum is limited by `SIPBRIDGE_MEMORY_MAX_BYTES`. |
| `zt_sipbridge_callback_total` | Controller callbacks (`kind` such as `event`) by fixed source and outcome labels. |
| `zt_sipbridge_scheduler_*`, `zt_sipbridge_worker_*` | Internal timer-scheduling counters. |

A runtime that does not collect a family leaves it out rather than reporting
zeros. The `zt_` prefix is historical. Counters start at zero when the process
starts, and labels use fixed vocabularies; no metric contains SIP, account or
call identities. A rising `ingress_rejected` count means the instance is at its
connection limit; a rising `capture_dropped` count means the collector is not
keeping up.

## Structured stdout events

Each line is one JSON object. The `event` field (or `Event`, for diagnostics)
identifies it.

### `sipbridge-listening`

Written once when listeners are open.

```json
{"control":"127.0.0.1:8443","epoch":"6b1f0c52-6a43-4d0e-9a51-2f8f7c1d9e30","event":"sipbridge-listening","instance":"sipbridge-0","profile":"sip.state.v1","revision":"unknown","sip":"127.0.0.1:5061","stateBackend":"memory","version":"dev"}
```

### `sipbridge-draining`

Written once when `SIGTERM` or an interrupt starts the
[drain](./deployment.md#replacing-a-process): `{"event":"sipbridge-draining"}`.

### `sipbridge-native-driver-fence`

With Redis state: the shared-state worker lost its lease or Redis connection
and stopped its in-flight work. It carries only a fixed `stage` and
`category`. Brief
occurrences during a Redis failover are expected; frequent ones point to Redis
or network trouble.

### `sipbridge-native-context-parked` and `sipbridge-native-context-abandoned`

Redis state isolation emits these events to stderr. Match the opaque `context`
and `incarnation` to trace one failed call from isolation to automatic
retirement. Both events carry the selected `revision`. The parking event also
carries the error `category`, work type, and the timer or output reference when
known. These events omit SIP payloads and credentials.

### `sipbridge-control-failure`

A control session read, write, protocol or heartbeat failure. Fields: `Event`,
`Stage` (for example `read-transport`, `read-frame`, `read-decode`,
`read-envelope`, `protocol`, `write`, `heartbeat-timeout`), and when known
`IOClass`, `Session`, `Peer`, `Correlation`, `Kind` (the envelope payload type),
`CloseCode`, `BudgetMS`, plus booleans `Deadline`, `Canceled`, `CallerExpired`
and `SessionExpired`. It never contains error text, peer close reasons, frames
or SIP values.

### `sipbridge-ingress-route`

A summary of how one inbound SIP message was handled, written when handling
failed or took at least 100 ms. Fields: `Event`, `Method` (a fixed SIP method
name or `other`), `Outcome`, `CallIDHash` (SHA-256 of the trimmed Call-ID,
omitted when invalid), `SipStatus` and timing fields such as
`CompletionProbeMS`, `RefusalMS`, `InitialBudgetMS`, `RemainingMS` and
`TotalMS`.

| `Outcome` | Meaning |
| --- | --- |
| `accepted` | Offered to a controller as a `request`. |
| `native-completed` | Handled by sipbridge alone: an ACK, CANCEL, BYE, retransmission or response. |
| `no-ready-peer` | No controller was ready; the request was refused. |
| `decision-notification-error` | The request could not be offered to the controller and was refused. |
| `completion-error` | Matching the message to existing state failed. |
| `draining` | Refused because the process is draining. |
| `local-options`, `method-disabled`, `local-challenge`, `local-challenge-ack` | Answered locally: OPTIONS, a disabled method, or a local digest challenge and its ACK. |
| `expired` | The message's handling deadline passed before it could be processed. |

Refusals have their own budget, so `TotalMS` can exceed the request budget.
Use `CallIDHash` to correlate events about the same call without logging the
Call-ID itself.

## Stderr messages

| Message | Meaning |
| --- | --- |
| `private credentials invalid; retaining previous context` | A control certificate, key or trust update failed validation. |
| `public credentials invalid; retaining previous context` | A public SIP certificate or key update failed validation, or the current one has expired. |
| `SIP capture shutdown timed out` | Capture export did not stop within three seconds of shutdown. |

Startup errors are also written to stderr; see [Troubleshooting](./troubleshooting.md).

## SIP capture

`--sip-capture-url=http://<collector>/loki/api/v1/push` sends a copy of every
SIP message to a Loki-compatible push endpoint, which is the practical way to
debug a call. sipbridge sends the capture over plain HTTP without credentials,
so run the collector on the same node or in the same pod.

- Records cover complete inbound frames and actual outbound write attempts, as
  plaintext at the process boundary with real socket endpoints. Distinct
  retransmissions are separate records; CRLF keepalives are excluded.
- Each record holds `version`, `instance`, `epoch`, `sequence`, `direction`,
  `transport`, `source`, `destination`, `outcome`, `bytesWritten`, `wire` and
  `wireBytes`. A non-UTF-8 body is replaced in `wire` with `[NNN bytes]`; the
  headers and `wireBytes` still describe the original message.
- A partial or failed write record does not prove delivery.
- Capture never slows down or changes signaling. Records wait in a queue of 256
  and are sent in batches of up to 32; if the collector falls behind or is
  down, records are dropped and counted rather than delaying SIP.

Capture contains full SIP messages, including `Authorization` headers and SDP.
Protect the collector and its storage accordingly. The exact capture format is
specified in [Runtime: network advertisement and observability](../runtime.md#network-advertisement-and-observability).
