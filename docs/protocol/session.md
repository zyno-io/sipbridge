# Session

A session is one WebSocket connection between a controller and sipbridge. It
goes through three stages: the controller introduces itself with `hello` and
sipbridge replies with `helloAck`; the controller declares itself ready with
`status`; and from then on both sides keep the session alive with heartbeats
while SIP work flows over it. This page covers each stage and what happens when
a controller reconnects. The examples use the same made-up identities as
[Getting Started](../guide/getting-started.md).

## hello

The controller sends `hello` as its first message, within 5 seconds of the
WebSocket opening. It says who the controller is and which protocol features it
supports.

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
| `nexus.instanceId` | A stable name for this controller, such as its pod name. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. (The field is called `nexus` after the first controller written for sipbridge.) |
| `nexus.epoch` | A random value generated once each time the controller process starts, and never reused. It lets sipbridge tell a restarted controller from a reconnecting one. |
| `connectionAttempt` | A counter, sent as a `uint64` string, starting above zero and increased every time this process reconnects. |
| `major` | Must be `1`. |
| `minMinor`, `maxMinor` | `minMinor` must be `0` and not exceed `maxMinor`. sipbridge selects minor `0`. |
| `capabilities` | At most 32 unique strings. Entries sipbridge does not know are ignored. The required ones are listed in [Capabilities](./capabilities.md). |
| `bounds.maxEnvelopeBytes` | The largest message you will send or accept, 512 to 1,048,576. It applies in both directions for the whole session. |

`hello` carries no `sessionId` and no `remainingBudgetMs`. If sipbridge rejects
it, the WebSocket closes with status `1008` and a reason that says why; they
are listed in
[Troubleshooting](../guide/troubleshooting.md#controller-cannot-connect).

## helloAck

sipbridge's reply tells the controller its session ID and what was agreed. This
example is formatted for reading; IDs differ on every run, and fields with
default values are left out:

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
| `sipbridge` | sipbridge's `--instance` name and its random per-start epoch. A changed epoch means sipbridge restarted. |
| `sessionId` | Put this in every later message. |
| `capabilities` | The capabilities both sides support, in the order the controller listed them. |
| `stateAuthority` | Identifies sipbridge's state store. `service` is the namespace. With memory state, `primaryId` is new on every start, so if it differs from last time, the state you knew about is gone. |
| `applicationLease` | The controller's lease, valid for about 30 seconds from `checkedAtMs` and renewed by heartbeats. Calls this controller sets up are tied to it; see [Owner lease](../guide/lifecycle.md#owner-lease). |
| `inboundAuthPolicy` | Present only with `--require-auth-methods`: `requireRegister`, `requireInvite`, `requireSubscribe`. |

Keep the `stateAuthority` and `applicationLease` values. Later messages refer
to them, and comparing them after a reconnect tells you whether sipbridge still
has the state your calls depend on.

## status

After `helloAck` the session exists but receives no SIP work yet. The
controller turns work on, and can turn it off again, by sending `status`. It
can be sent at any time, and sipbridge does not reply.

```json
{"correlationId":"status-1","sessionId":"<session UUID>","status":{"retainedReady":true,"bindingsReady":true,"admitCalls":true,"admitSubscriptions":true}}
```

| Field | Use by sipbridge |
| --- | --- |
| `retainedReady`, `bindingsReady` | When both are `true` the session is ready: sipbridge routes requests to it and counts it for `/sip-readyz`. Set either to `false` to stop receiving new requests on this session. |
| `admitCalls`, `admitSubscriptions`, `draining` | Stored but not acted on by sipbridge. They describe what the controller itself is willing to take. |
| `digestChallengePolicy` | The policy for [local digest challenges](../guide/application.md#local-initial-digest-challenges). Requires `sip.local-challenge.v1`. It contains a signing key, so never log it. |

A session is not ready until it has sent such a `status`; completing the
handshake or answering heartbeats is not enough. If sipbridge rejects a
`status`, for example because of an invalid challenge policy, it closes the
session.

## Heartbeats

Heartbeats let each side notice quickly when the other has gone away. sipbridge
sends a `ping` every second (waiting for the previous one to be answered first)
and closes the session if a ping goes six seconds without a valid `pong`. Other
traffic on the session does not count as an answer.

sipbridge to controller:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"<session UUID>","ping":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

Controller reply; `correlationId` and `nonce` must both echo the ping's nonce:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"<session UUID>","pong":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

The controller can send its own pings too; sipbridge answers each with a `pong`
carrying the same `correlationId` and payload.

Answering pings does a second job. Each valid `pong` sipbridge receives renews
the controller's lease (at most every two seconds per session), so a controller
that answers heartbeats keeps ownership of its calls without doing anything
else.

When a session fails, sipbridge stops using it and fails the operations that
were waiting on it. Nothing happens to SIP itself: client connections stay
open, call state is kept and SIP timers keep running, so the controller can
reconnect and pick up where it was.

## Supersession and claims

sipbridge allows one session at a time per controller instance ID. When a
controller connects again, these rules decide what happens:

- **Reconnecting (same epoch, higher `connectionAttempt`).** The new session
  replaces the old one at once, and the old one is closed. Commands that were
  in flight on the old session have an unknown outcome unless a result already
  arrived; use `inspect` to check them.
- **Reusing an attempt number.** sipbridge remembers the last accepted attempt
  number for five minutes after a session closes. Reconnecting with a number
  that is not higher is refused (`stale connection attempt`). This is why the
  counter must keep increasing.
- **A restarted controller (new epoch) while the old session is still open.**
  Refused with `active epoch collision`. Once the old session has closed, the
  new process can connect.
- A new session can be used only once its lease has been issued and `helloAck`
  has been sent. A handshake that fails never produces a usable session.
- sipbridge holds at most 64 sessions and remembers at most 256 controller
  instance IDs at once.

These rules are about connections. The lease in `applicationLease`, which
decides who owns calls, is a separate mechanism described in
[Lifecycle and Retention](../guide/lifecycle.md#owner-lease).
