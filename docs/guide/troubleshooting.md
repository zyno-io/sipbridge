# Troubleshooting

Messages below are quoted from the current source. Startup errors are printed to
stderr and the process exits with status 1.

## Startup failures

### Flags and identity

| Message | Cause |
| --- | --- |
| `instance and public advertisement are required` | `--instance` or `--advertise` is missing (and `--sip-interface` is not set). |
| `process, TLS and allowed URI identities are required` | `--instance` does not match `[A-Za-z0-9_.:@+-]{1,96}`. |
| `URI identity allowlist must be exact` | `--allowed-client-uri` is empty, has an empty entry or contains `*`. |
| `required authentication conflicts with method policy` | A `--require-auth-methods` entry is duplicated or names a method disabled by `--allow-register=false` / `--allow-subscribe=false`. |
| `required authentication permits only REGISTER, INVITE and SUBSCRIBE` | Unknown or lowercase method in `--require-auth-methods`. |
| `invalid plain SIP advertisement` / `plain SIP advertisement requires a fixed numeric port` | Bad `--advertise-plain` or `--advertise` value. |

### Network

| Message | Cause |
| --- | --- |
| `SIP interface requires one global-unicast IPv4 address` / `SIP interface has multiple IPv4 addresses` | `--sip-interface` has no usable IPv4 address, or more than one. |
| `SIP listener conflicts with interface address` | A listener names a host other than the interface address. |
| `SIP interface requires a fixed listener port` | A listener uses port `0` or no port with `--sip-interface`. |
| `SIP capture requires UDP listener bound to a specific IP address` | `--sip-capture-url` is set while `--sip-plain-listen` uses a wildcard host. |
| `invalid capture collector configuration` | Capture URL is not `http://<host>/loki/api/v1/push` without credentials, query or fragment. |
| `listen tcp ...: bind: address already in use` (or `listen udp ...`) | Another process holds the port. |

### TLS files

| Message | Cause |
| --- | --- |
| `read server identity: ...`, `read server key: ...`, `read client trust: ...` | A path is missing or unreadable. All of `--control-cert`, `--control-key`, `--client-ca`, `--sip-cert` and `--sip-key` are required. |
| `server identity/key replacement is invalid` | Certificate and key do not match or are not PEM. |
| `server certificate is outside its validity period` | Not yet valid or expired. Check clocks too. |
| `client trust contains no certificates` | `--client-ca` has no PEM certificates. |

### State

| Message | Cause |
| --- | --- |
| `unknown SIP state backend` | `SIPBRIDGE_STATE_BACKEND` is not `memory` or `redis`. |
| `Redis settings require SIPBRIDGE_STATE_BACKEND=redis` | A `SIPBRIDGE_REDIS_*` variable is set while using memory. |
| `memory settings require SIPBRIDGE_STATE_BACKEND=memory` | A `SIPBRIDGE_MEMORY_*` variable is set while using Redis. |
| `SIP state identity key must be base64 encoding of 32 to 256 bytes` | Missing (Redis) or malformed `SIPBRIDGE_STATE_IDENTITY_KEY`. Use standard, padded base64. |
| `SIP state namespace, generation or prefix invalid` | Value does not match its allowed pattern. |
| `SIP state partitions must be between 1 and 4096` | Bad `SIPBRIDGE_STATE_PARTITIONS`. |
| `invalid SIP memory group limit` | `SIPBRIDGE_MEMORY_MAX_GROUPS` outside 1 to 1,048,576. |
| `SIP memory byte limit must be at least 8388608` / `invalid SIP memory capacity` | `SIPBRIDGE_MEMORY_MAX_BYTES` is too small. In practice it must be at least `67108864` (one 64 MiB group). |
| `select exactly one SIP Redis primary or Sentinel discovery source` | Both or neither of `SIPBRIDGE_REDIS_ADDRESS` and `SIPBRIDGE_REDIS_SENTINELS` are set, an address is malformed, or `SIPBRIDGE_REDIS_MASTER` is missing. |
| `invalid SIP Redis Sentinel endpoint` / `too many SIP Redis Sentinel endpoints` | Malformed entry, or more than 16. |
| `SIP Redis requires CA and verified server name, or explicit local plaintext` | TLS needs both `SIPBRIDGE_REDIS_CA_FILE` and `SIPBRIDGE_REDIS_SERVER_NAME`; plaintext needs `SIPBRIDGE_REDIS_ALLOW_PLAINTEXT=true` and neither TLS variable. |
| `cannot read SIP Redis CA bundle` / `invalid SIP Redis CA bundle` | CA file missing or contains no PEM certificates. |

## Controller cannot connect

HTTP responses before the WebSocket upgrade:

| Response | Cause |
| --- | --- |
| TLS handshake failure | Client certificate missing, or not issued by `--client-ca`. |
| `403 mutual TLS required` | No verified client chain. |
| `403 workload identity denied` | No client URI SAN exactly matches `--allowed-client-uri`. Compare the full URI string. |
| `404` | Path is not exactly `/v1/control`, a query string is present, or the method is not `GET`. |
| `400 unsupported control upgrade` | An `Origin` header was sent, or the `sipbridge.control.json.v1` subprotocol was not offered. |
| `503 control session capacity` | 64 sessions are already active. |

After the upgrade, a failed handshake closes the WebSocket with status `1008`
(policy violation) and a reason:

| Close reason | Cause |
| --- | --- |
| `hello required` | First message was not a valid envelope within 5 seconds. |
| `unsupported hello` | `sessionId` or `remainingBudgetMs` set; missing or invalid `nexus`; `connectionAttempt` is `0`; `major` is not `1`; `minMinor` is not `0` or exceeds `maxMinor`; `maxEnvelopeBytes` outside 512 to 1,048,576; more than 32 capabilities. |
| `invalid capabilities` | A duplicate or malformed capability string. |
| `state runtime requires sip.v1 and a compatible state capability` | `sip.v1` or `sip.state.v1` not offered. |
| `state runtime requires sip.directory-reconcile.v1` / `...sip.completion-relay.v1` / `...sip.native-completion.v1` | A required compatibility capability is missing. |
| `configured initial authentication requires sip.local-challenge.v1` | `--require-auth-methods` is set and the controller did not offer the capability. |
| `stale connection attempt` | Same instance and epoch reconnected with a `connectionAttempt` that is not higher than the last accepted one. |
| `active epoch collision` | Another epoch of the same instance ID still has an active session. |
| `claim capacity` | Too many distinct controller instance claims (256). |
| `native application lease acquisition failed` | The state store could not issue an owner lease, for example Redis is unavailable. |
| `invalid control message` | A later message was malformed, used the wrong `sessionId`, wrong direction or an unnegotiated family. |

### Session closes every few seconds

sipbridge pings each session every second and closes it when a ping has had no
valid `pong` for six seconds. The `pong` must echo the ping's `correlationId`
and `nonce` and carry the session's `sessionId`. Look for
`sipbridge-control-failure` lines with stage `heartbeat-timeout`.

## Readiness stays at 503

| Body | Check |
| --- | --- |
| `signaling unavailable` | Has a controller sent `status` with both `retainedReady` and `bindingsReady` true? Are both certificates within validity? Is Redis reachable (Redis mode)? |
| `authentication policy unavailable` | With `--require-auth-methods`, the controller must send a current `digestChallengePolicy` in `status`. |

`/sip-servingz` returning `503` means the listeners are not up, shutdown started
or the public certificate is outside its validity period.

## SIP clients see errors

| Symptom | Likely cause |
| --- | --- |
| `503` to new requests | No ready controller, route budget expired, controller returned `UNAVAILABLE` or redirected to an unknown controller, or the datastore is unavailable. Check `sipbridge-ingress-route` lines for `Outcome`. |
| `405` to REGISTER or SUBSCRIBE | The method is disabled with `--allow-register=false` or `--allow-subscribe=false`. Counted in `zt_sipbridge_method_rejected_total`. |
| `401` without controller involvement | Local initial challenge from `--require-auth-methods`. |
| `481` | The state store proves the dialog or transaction does not exist, for example after a memory-state restart. |
| UDP requests silently dropped | The 128-slot UDP routing queue is full. Overload drops without blocking. |
| TLS connections refused | Stream capacity (4,096) reached, or public certificate problem. Check `zt_sipbridge_ingress_rejected_total` and `zt_sipbridge_tls_handshake_failures_total`. |
| Wrong address in Via/Contact | Check `--advertise`, `--advertise-plain` and `--contact`. |

Do not restart sipbridge to fix controller or datastore outages. The process
recovers when they return, and a restart closes every client connection.
