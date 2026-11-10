# Admin Endpoints

The admin listener (`--admin-listen`, default `127.0.0.1:8800`) serves plain HTTP
with no authentication. Any method is accepted. Unknown paths return `404`.

| Path | Success | Failure | Notes |
| --- | --- | --- | --- |
| `/healthz` | `200`, empty body | none | Liveness. True whenever the admin server responds. |
| `/sip-servingz` | `200`, empty body | `503 SIP listeners unavailable` | Listeners started, no shutdown in progress, public TLS certificate valid now. |
| `/sip-readyz` | `200`, empty body | `503 signaling unavailable`, `503 authentication policy unavailable` | See [Health and Readiness](../guide/health.md). |
| `/metrics` | `200`, `text/plain; version=0.0.4` | none | Counters listed in [Observability](../guide/observability.md#metrics). |

Example `/metrics` output with capture disabled:

```text
zt_sipbridge_ingress_rejected_total 0
zt_sipbridge_tls_handshake_failures_total 0
zt_sipbridge_invalid_messages_total 0
zt_sipbridge_method_rejected_total 0
```

The private control listener is a different server. Apart from
`GET /v1/control`, it answers only `/sip-readyz` (always `503`) to authenticated
controllers and returns `404` otherwise.
