# Observability

sipbridge gives you four views of what it is doing: JSON event lines on stdout,
short operational messages on stderr, Prometheus counters on the admin
listener, and an optional capture of the SIP messages themselves.

It is quiet by design. It logs problems and slow operations, not a line for
every message handled, and its routine output never contains SIP message
content or credentials. When you need to see the SIP traffic itself, turn on
[SIP capture](#sip-capture).

## Metrics

`GET /metrics` on the admin listener returns text exposition format:

| Metric | Meaning |
| --- | --- |
| `zt_sipbridge_ingress_rejected_total` | Public connections or flows refused at the 4,096 stream/flow capacity. |
| `zt_sipbridge_tls_handshake_failures_total` | Failed public TLS handshakes. |
| `zt_sipbridge_invalid_messages_total` | Public SIP messages that failed framing or parsing, or exceeded 64 KiB. |
| `zt_sipbridge_method_rejected_total` | Local `405` responses from `--allow-register=false` / `--allow-subscribe=false`. |
| `zt_sipbridge_capture_dropped_total` | Capture records dropped because the queue was full. Present only with capture enabled. |
| `zt_sipbridge_capture_export_failures_total` | Capture batches the collector did not accept. Present only with capture enabled. |

The `zt_` prefix is historical. The counters start at zero when the process
starts and have no labels. A rising `ingress_rejected` count means the
instance is at its connection limit; a rising `capture_dropped` count means the
collector is not keeping up.

## Structured stdout events

Each line is one JSON object. The `event` field (or `Event`, for diagnostics)
identifies it.

### `sipbridge-listening`

Written once when listeners are open.

```json
{"control":"127.0.0.1:8443","epoch":"<random UUID>","event":"sipbridge-listening","instance":"sipbridge-0","profile":"sip.state.v1","revision":"<git revision>","sip":"127.0.0.1:5061","stateBackend":"memory","version":"<version>"}
```

### `sipbridge-control-failure`

A control session read, write, protocol or heartbeat failure. Fields: `Event`,
`Stage` (for example `read-transport`, `read-frame`, `read-decode`,
`read-envelope`, `protocol`, `write`, `heartbeat-timeout`), and when known
`IOClass`, `Session`, `Peer`, `Correlation`, `Kind` (the envelope payload type),
`CloseCode`, `BudgetMS`, plus booleans `Deadline`, `Canceled`, `CallerExpired`
and `SessionExpired`. It never contains error text, peer close reasons, frames
or SIP values.

### `sipbridge-ingress-route`

A summary of one inbound routing attempt, written when routing failed or took at
least 100 ms. Fields: `Event`, `Method` (a fixed SIP method name or `other`),
`Outcome` (for example `accepted`, `rejected`, `no-ready-peer`, `expired`,
`redirect-loop`, `redirect-unavailable`, `route-rpc-error`, `locate-error`),
`CallIDHash` (SHA-256 of the trimmed Call-ID, omitted when invalid), `SipStatus`,
timing fields `CompletionProbeMS`, `LocateMS`, `RouteRPCMS`, `RefusalMS`,
`InitialBudgetMS`, `RemainingMS`, `TotalMS`, `FirstHopBudgetMS`, and `Hops`.
Refusal has its own budget, so `TotalMS` can exceed the route budget.

### `sipbridge-control-slow`

A summary of a controller `ingest`, `command` or `inspect` that took at least
100 ms or whose reply failed to send. Fields include `Correlation`, `Context`,
`Kind`, `Certainty`, `CallIDHash`, `BudgetMS`, timing fields (`TotalMS`,
`ReplyMS`, `PreflightMS`, `GroupWaitMS`, `SlotMS`, `ReadMS`, `ReduceMS`,
`CommitMS`), counters (`Reads`, `Reductions`, `Commits`, `Stale`, `Busy`,
`Unknown`), worker occupancy (`Work`, `Background`, `Scheduled`) and `SendFailed`.

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
