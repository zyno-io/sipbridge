---
description: Startup, controller connection, readiness and SIP client errors, quoted exactly as sipbridge reports them, with causes and fixes.
---

# Troubleshooting

Find the message or symptom you see in the tables below. Messages are quoted
exactly as sipbridge prints them, so you can search this page for the text.
Startup errors go to stderr and the process exits with status 1.

## Startup failures

### Flags and identity

| Message | Cause |
| --- | --- |
| `instance and public advertisement are required` | `--instance` or `--advertise` is missing (and `--sip-interface` is not set). |
| `control process identity is required` | `--instance` does not match `[A-Za-z0-9_.:@+-]{1,96}`. |
| `required authentication conflicts with method policy` | A `--require-auth-methods` entry is duplicated or names a method disabled with `--allow-register=false` / `--allow-subscribe=false`. |
| `required authentication permits only REGISTER, INVITE and SUBSCRIBE` | Unknown or lowercase method in `--require-auth-methods`. |
| `invalid plain SIP advertisement` / `plain SIP advertisement requires a fixed numeric port` | Bad `--advertise-plain` or `--advertise` value. |
| `SIP drain requires withdrawal grace 0..60s and timeout greater than grace and at most 2m` | `--sip-withdrawal-grace` or `--sip-drain-timeout` is out of range. |

### Control authentication

| Message | Cause |
| --- | --- |
| `control authentication must be mtls or bearer` / `control transport must be tls or plain` | Typo in `--control-auth` or `--control-transport`. |
| `mutual TLS requires TLS transport and no bearer settings` | mTLS was combined with `--control-transport=plain`, a token file or a principal. |
| `controller URI identities must be exact` | `--allowed-client-uri` is empty, has an empty entry, spaces or `*`. |
| `bearer authentication does not use client CA or URI allowlist settings` | Bearer mode was combined with `--client-ca` or `--allowed-client-uri`. |
| `bearer authentication requires a token file and stable principal` | `--control-bearer-token-file` or `--control-principal` is missing or invalid. |
| `bearer token file is unavailable` / `bearer token must contain 32 to 4096 printable non-space bytes` / `bearer token contains invalid characters` | The token file is missing, too short or contains spaces or extra newlines. |
| `plaintext control does not use TLS certificate settings` | Plain control was combined with `--control-cert` or `--control-key`. |
| `plaintext control requires a loopback listener` | Plain control needs `--control-listen` on a loopback address such as `127.0.0.1:8443`. |

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
| `read server identity: ...`, `read client trust: ...` | A certificate, key or CA path is missing or unreadable. `--sip-cert` and `--sip-key` are always required; mTLS control also needs `--control-cert`, `--control-key` and `--client-ca`. |
| `server identity/key replacement is invalid` | Certificate and key do not match or are not PEM. |
| `server certificate is outside its validity period` | Not yet valid or expired. Check the clock too. |
| `client trust contains no certificates` | `--client-ca` has no PEM certificates. |

### State

| Message | Cause |
| --- | --- |
| `unknown SIP state backend` | `SIPBRIDGE_STATE_BACKEND` is not `memory` or `redis`. |
| `Redis settings require SIPBRIDGE_STATE_BACKEND=redis` | A `SIPBRIDGE_REDIS_*` variable is set while using memory. |
| `memory settings require SIPBRIDGE_STATE_BACKEND=memory` | A `SIPBRIDGE_MEMORY_*` variable is set while using Redis. |
| `SIP state identity key must be base64 encoding of 32 to 256 bytes` | Missing (Redis) or malformed `SIPBRIDGE_STATE_IDENTITY_KEY`. Use standard, padded base64. |
| `SIP state namespace, generation or prefix invalid` | A value does not match its allowed pattern. |
| `SIPBRIDGE_STATE_PARTITIONS is obsolete; binary Redis uses indexed deadlines` | Remove `SIPBRIDGE_STATE_PARTITIONS`. |
| `invalid SIP memory group limit` | `SIPBRIDGE_MEMORY_MAX_GROUPS` outside 1 to 1,048,576. |
| `SIP memory byte limit must be at least 8388608` | `SIPBRIDGE_MEMORY_MAX_BYTES` below 8 MiB or not a number. |
| `select exactly one SIP Redis primary or Sentinel discovery source` | Both or neither of `SIPBRIDGE_REDIS_ADDRESS` and `SIPBRIDGE_REDIS_SENTINELS` are set, an address is malformed, or `SIPBRIDGE_REDIS_MASTER` is missing. |
| `invalid SIP Redis Sentinel endpoint` / `too many SIP Redis Sentinel endpoints` | Malformed entry, or more than 16. |
| `SIP Redis requires CA and verified server name, or explicit local plaintext` | TLS needs both `SIPBRIDGE_REDIS_CA_FILE` and `SIPBRIDGE_REDIS_SERVER_NAME`; plaintext needs `SIPBRIDGE_REDIS_ALLOW_PLAINTEXT=true` and neither TLS variable. |
| `invalid SIP Redis plaintext setting` | `SIPBRIDGE_REDIS_ALLOW_PLAINTEXT` is not `true` or `false`. |
| `cannot read SIP Redis CA bundle` / `invalid SIP Redis CA bundle` | CA file missing or contains no PEM certificates. |

## Controller cannot connect

A connection can fail before the WebSocket opens, with an HTTP error:

| Response | Cause |
| --- | --- |
| TLS handshake failure | mTLS: client certificate missing or not issued by `--client-ca`. Any TLS: the controller does not trust sipbridge's control certificate or uses the wrong server name. |
| `401 control authentication failed` | Bearer mode: the token is missing or wrong. |
| `403 control authentication failed` | mTLS: the client certificate has no URI SAN matching `--allowed-client-uri`. Compare the full URI string. |
| `404` | Path is not exactly `/v1/control`, a query string is present, or the method is not `GET`. |
| `400 unsupported control upgrade` | An `Origin` header was sent, or the `sipbridge.control.json.v1` subprotocol was not offered. |
| `503 control session capacity` | 64 sessions are already open. |

Once the WebSocket is open, a problem with `hello` closes it with status
`1008` and one of these reasons:

| Close reason | Cause |
| --- | --- |
| `hello required` | The first message was not a valid envelope within 5 seconds. |
| `unsupported hello` | `sessionId` or `remainingBudgetMs` set; `controller` missing or invalid; `connectionAttempt` is `0`; `major` is not `1`; `minMinor` is not `0` or exceeds `maxMinor`; `maxEnvelopeBytes` outside 512 to 1,048,576; more than 32 capabilities. |
| `invalid capabilities` | A duplicate or malformed capability string. |
| `state runtime requires sip.v1 and a compatible state capability` | `sip.v1` or `sip.state.v1` not offered. |
| `state runtime requires sip.native-completion.v1` | `sip.native-completion.v1` not offered. |
| `memory runtime requires sip.request-decision.v1` | `sip.request-decision.v1` not offered. The text says "memory" but applies to both backends. |
| `request decision dependencies are not negotiated` | One of `sip.command-admission.v1`, `sip.command-budget.v1` or `sip.dialog-continuity.v1` not offered. See [Capabilities](../protocol/capabilities.md). |
| `configured initial authentication requires sip.local-challenge.v1` | sipbridge runs with `--require-auth-methods` and the controller did not offer the capability. |
| `controller recovery capability required` | `hello.recovery` was sent. Controller reattachment is not available. |
| `stale connection attempt` | Same instance and epoch reconnected with a `connectionAttempt` that is not higher than the last accepted one. |
| `active epoch collision` | Another epoch of the same instance ID still has an open session. |
| `controller handshake in progress` | Another handshake for the same instance ID is still running. |
| `claim capacity` | Too many distinct controller instance IDs (256 by default). |
| `native application lease acquisition failed` | sipbridge could not issue the controller's lease, for example because Redis is unreachable. |
| `invalid control message` | A later message was malformed, carried the wrong `sessionId`, went the wrong direction, used an unsupported message type, or lacked `remainingBudgetMs` on a `requestDecision`. |

### Session closes every few seconds

sipbridge pings each session every second and closes it when a ping has had
no valid `pong` for six seconds. The `pong` must echo the ping's
`correlationId` and `nonce` and carry the session's `sessionId`. Look for
`sipbridge-control-failure` lines with stage `heartbeat-timeout`. Remember
that a closed session ends that controller's calls.

## Readiness stays at 503

| Body | Check |
| --- | --- |
| `signaling unavailable` | Has a controller sent `status` with both `retainedReady` and `bindingsReady` true? Are both certificates within validity? Is Redis reachable (Redis mode)? Is the process draining? |
| `authentication policy unavailable` | With `--require-auth-methods`, the controller must send a current `digestChallengePolicy` in `status`. |

`/sip-servingz` returning `503` means the listeners are not up, the drain has
started, or the public certificate is outside its validity period.

## Decisions and commands are refused

| `error` contains | Cause |
| --- | --- |
| `pending native decision unavailable` | The decision arrived after the request's deadline, the request was already answered or cancelled, or the `requestId` or context is wrong. |
| `REGISTER and SUBSCRIBE require a final decision` | `status: 0` was used for REGISTER or SUBSCRIBE. |
| `accepted SUBSCRIBE requires exactly one NOTIFY template` | An accepted SUBSCRIBE lacks `notify`, or `notify` was sent on another decision. |
| `denial cannot publish account or flow authority` | A rejection carried `principalId`, `bindingId` or `delivery`. |
| `authenticated binding required` | `AUTHENTICATED_BINDING` without both `principalId` and `bindingId`. |
| `ordered flow receipt required` / `unsolicited flow receipt` | `delivery` missing or wrong for a request with a `flowAdoption` proposal, or present without one. |
| `decision policy required` / `command policy admission refused` | The `admission` block is missing, malformed or out of date: expired `expiresAtMs`, changed `directoryEpoch`, or an `expectedDelivery` that does not match the snapshot. |
| `controller attachment unavailable` | The command arrived on a session whose lease has ended. Reconnect; the old calls are gone. |

## SIP clients see errors

| Symptom | Likely cause |
| --- | --- |
| `503` to new requests | No ready controller, no decision within the deadline, the process is draining, or (Redis) shared state is unavailable. Check `sipbridge-ingress-route` lines for `Outcome`. |
| `501` to a new MESSAGE or similar | Redis mode accepts only REGISTER, SUBSCRIBE and INVITE outside a call. |
| `405` to REGISTER or SUBSCRIBE | The method is disabled with `--allow-register=false` or `--allow-subscribe=false`. Counted in `zt_sipbridge_method_rejected_total`. |
| `401` without controller involvement | Local initial challenge from `--require-auth-methods`. |
| `481` | sipbridge has no such dialog or transaction, for example after a memory-state restart or after the call ended. |
| TLS connections refused | Stream capacity (4,096) reached, or a public certificate problem. Check `zt_sipbridge_ingress_rejected_total` and `zt_sipbridge_tls_handshake_failures_total`. |
| Wrong address in Via/Contact | Check `--advertise`, `--advertise-plain` and `--contact`. |
| Calls drop when the controller restarts | Expected: a controller's calls end with its session. See [Controller Restarts](./controller-recovery.md). |

If the cause is a controller or Redis outage, fix that and leave sipbridge
running. It recovers by itself when they return, whereas a restart would
disconnect every phone.
