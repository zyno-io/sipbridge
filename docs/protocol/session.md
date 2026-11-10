# Session

A session starts with `hello` from the controller and `helloAck` from sipbridge.
Until `helloAck` is written and the owner lease is acquired, the session is not
usable. Examples use synthetic identities that match
[Getting Started](../guide/getting-started.md).

## hello

Sent by the controller as the first message, within 5 seconds of the upgrade.

```json
{
  "correlationId": "hello-1",
  "hello": {
    "nexus": { "instanceId": "controller-a", "epoch": "boot-0001" },
    "connectionAttempt": "1",
    "major": 1,
    "minMinor": 0,
    "maxMinor": 0,
    "capabilities": [
      "sip.v1",
      "sip.state.v1",
      "sip.native-completion.v1",
      "sip.directory-reconcile.v1",
      "sip.completion-relay.v1",
      "sip.command-admission.v1",
      "sip.command-budget.v1",
      "sip.dialog-continuity.v1"
    ],
    "bounds": { "maxEnvelopeBytes": 1048576 }
  }
}
```

| Field | Rule |
| --- | --- |
| `nexus.instanceId` | Stable controller inventory ID. Reference pattern. |
| `nexus.epoch` | Unpredictable per controller process start. Never reused. |
| `connectionAttempt` | `uint64` string greater than zero, increasing on each reconnect of the same instance and epoch. |
| `major` | Must be `1`. |
| `minMinor`, `maxMinor` | `minMinor` must be `0` and not exceed `maxMinor`. sipbridge selects minor `0`. |
| `capabilities` | At most 32 unique strings. Unknown entries are ignored. Required entries are listed in [Capabilities](./capabilities.md). |
| `bounds.maxEnvelopeBytes` | 512 to 1,048,576. Becomes the session's envelope bound in both directions. |

`hello` carries no `sessionId` and no `remainingBudgetMs`. Any failure closes
the WebSocket with status `1008` and a reason; see
[Troubleshooting](../guide/troubleshooting.md#controller-cannot-connect).

## helloAck

sipbridge's reply. Illustrative output, pretty-printed; generated IDs differ on
every run and fields at default values are omitted:

```json
{
  "correlationId": "hello-1",
  "sessionId": "<session UUID>",
  "helloAck": {
    "sipbridge": { "instanceId": "sipbridge-0", "epoch": "<sipbridge process UUID>" },
    "sessionId": "<session UUID>",
    "major": 1,
    "capabilities": [
      "sip.v1",
      "sip.state.v1",
      "sip.native-completion.v1",
      "sip.directory-reconcile.v1",
      "sip.completion-relay.v1",
      "sip.command-admission.v1",
      "sip.command-budget.v1",
      "sip.dialog-continuity.v1"
    ],
    "bounds": { "maxEnvelopeBytes": 1048576 },
    "heartbeatIntervalMs": 1000,
    "heartbeatTimeoutMs": 6000,
    "stateAuthority": {
      "service": "sipbridge",
      "generation": "default",
      "primaryId": "<state authority ID>",
      "schemaVersion": 2
    },
    "applicationLease": {
      "application": { "instanceId": "controller-a", "epoch": "boot-0001" },
      "authority": {
        "service": "sipbridge",
        "generation": "default",
        "primaryId": "<state authority ID>",
        "schemaVersion": 2
      },
      "term": "<opaque owner term>",
      "checkedAtMs": "<uint64 milliseconds>",
      "expiresAtMs": "<uint64 milliseconds>"
    }
  }
}
```

| Field | Meaning |
| --- | --- |
| `sipbridge` | sipbridge's `--instance` and its random per-start epoch. |
| `sessionId` | Required on every later message from either side. |
| `capabilities` | Intersection of the offer and the service's set, in the controller's order. |
| `stateAuthority` | The SIP state authority. `service` is the namespace. For memory, `primaryId` is unique to this process run, so a change means previous state is gone. |
| `applicationLease` | The controller's SIP-owner term, valid for about 30 seconds from `checkedAtMs`. Contexts pin this term. |
| `inboundAuthPolicy` | Present only with `--require-auth-methods`: `requireRegister`, `requireInvite`, `requireSubscribe`. |

## status

The controller reports readiness at any time after `helloAck`. sipbridge does
not reply.

```json
{"correlationId":"status-1","sessionId":"<session UUID>","status":{"retainedReady":true,"bindingsReady":true,"admitCalls":true,"admitSubscriptions":true}}
```

| Field | Use by sipbridge |
| --- | --- |
| `retainedReady`, `bindingsReady` | Both `true` makes the session ready: eligible for routing and counted by `/sip-readyz`. |
| `admitCalls`, `admitSubscriptions`, `draining` | Stored, not used by sipbridge. They describe the controller's own admission. |
| `digestChallengePolicy` | Installs local challenge policy. Requires `sip.local-challenge.v1`. Sensitive; never log it. |

A `status` that sipbridge rejects, for example an invalid challenge policy,
closes the session. A `hello` or heartbeat alone does not make a session ready.

## Heartbeats

Both sides can ping. sipbridge sends a `ping` every second while none is
outstanding and closes the session when a ping has gone six seconds without a
valid `pong`. Unrelated traffic does not reset that deadline.

sipbridge to controller:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"<session UUID>","ping":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

Controller reply; `correlationId` and `nonce` must both echo the ping's nonce:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"<session UUID>","pong":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

A controller ping gets a `pong` with the same `correlationId` and payload. Each
valid `pong` that sipbridge receives also renews the controller's owner lease,
at most every two seconds per session, so answering sipbridge's pings keeps the
lease alive.

Session failure fences that session and fails its pending operations. It does not
close client SIP sockets, delete native contexts or reset SIP timers.

## Supersession and claims

sipbridge keeps one claim per controller instance ID.

- A higher `connectionAttempt` from the same instance and epoch replaces the
  current session immediately. The old session is closed; its in-flight
  mutations are unknown unless already proven.
- A different epoch for an instance ID that still has an active session is
  refused (`active epoch collision`). Once that session has closed, a new epoch
  can claim the instance.
- After a session closes, its claim keeps the last accepted attempt number for
  five minutes, so the same epoch cannot reconnect with an attempt that is not
  higher (`stale connection attempt`).
- The replacement session becomes usable only after the owner lease is acquired
  and `helloAck` is written. A failed handshake publishes no usable session.
- At most 64 sessions and 256 claim records exist at once.

These transport claims are separate from the SIP-owner lease in
`applicationLease`, which is described in
[Lifecycle and Retention](../guide/lifecycle.md#owner-lease).
