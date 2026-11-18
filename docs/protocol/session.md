---
description: The hello, helloAck and status exchange that opens a control session, heartbeats, reconnecting, and what a dropped session means for calls.
---

# Session

Messages use the [strict JSON encoding](./overview.md#json-encoding).

A session is one WebSocket connection between a controller and sipbridge. It
goes through three steps: the controller introduces itself with `hello` and
sipbridge replies with `helloAck`; the controller declares itself ready with
`status`; and from then on both sides keep the session alive with heartbeats
while SIP work flows over it.

The examples use made-up values. Substitute your own instance name, a fresh
epoch and the `sessionId` sipbridge gives you.

## hello

The controller sends `hello` as its first message, within 5 seconds of the
WebSocket opening:

```json
{
  "correlationId": "hello-1",
  "hello": {
    "controller": {"instanceId": "demo-controller", "epoch": "3f9a1c0e7b2d4a6f8e1c5b7d9a2f4e60"},
    "connectionAttempt": "1",
    "major": 1,
    "minMinor": 0,
    "maxMinor": 0,
    "capabilities": [
      "sip.v1",
      "sip.state.v1",
      "sip.native-completion.v1",
      "sip.request-decision.v1",
      "sip.command-admission.v1",
      "sip.command-budget.v1",
      "sip.dialog-continuity.v1"
    ],
    "bounds": {"maxEnvelopeBytes": 1048576}
  }
}
```

| Field | Rule |
| --- | --- |
| `controller.instanceId` | A stable name for this controller, such as its pod name. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. |
| `controller.epoch` | A random value generated once each time the controller process starts, never reused. Same pattern. It lets sipbridge tell a restarted controller from a reconnecting one. |
| `connectionAttempt` | A `uint64` string, above zero, increased every time this process reconnects. |
| `major` | Must be `1`. |
| `minMinor`, `maxMinor` | `minMinor` must be `0` and not above `maxMinor`. sipbridge selects minor `0`. |
| `capabilities` | At most 32 unique strings. All seven shown here are required; see [Capabilities](./capabilities.md). Unknown entries are ignored. |
| `bounds.maxEnvelopeBytes` | The largest message either side may send on this session, 512 to 1,048,576. |

`hello` carries no `sessionId` and no `remainingBudgetMs`. If sipbridge
rejects it, the WebSocket closes with status `1008` and a reason; the reasons
are listed in
[Troubleshooting](../guide/troubleshooting.md#controller-cannot-connect).

The schema also has a `hello.recovery` field for reattaching to an earlier
controller's calls. No serving sipbridge offers that feature, so a hello that
includes it is refused with `controller recovery capability required`.

## helloAck

sipbridge's reply tells the controller its session ID and what was agreed:

```json
{
  "correlationId": "hello-1",
  "sessionId": "0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841",
  "helloAck": {
    "sipbridge": {"instanceId": "sipbridge-0", "epoch": "6b1f0c52-6a43-4d0e-9a51-2f8f7c1d9e30"},
    "sessionId": "0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841",
    "major": 1,
    "capabilities": ["sip.v1", "sip.state.v1", "sip.native-completion.v1", "sip.request-decision.v1", "sip.command-admission.v1", "sip.command-budget.v1", "sip.dialog-continuity.v1"],
    "bounds": {"maxEnvelopeBytes": 1048576},
    "heartbeatIntervalMs": 1000,
    "heartbeatTimeoutMs": 6000,
    "stateAuthority": {"service": "sipbridge", "generation": "default", "primaryId": "a7c3e9f1-2b4d-4e6a-8c0f-1d3b5a7e9c21", "schemaVersion": 1},
    "applicationLease": {"application": {"instanceId": "demo-controller", "epoch": "3f9a1c0e7b2d4a6f8e1c5b7d9a2f4e60"}, "authority": {"service": "sipbridge", "generation": "default", "primaryId": "a7c3e9f1-2b4d-4e6a-8c0f-1d3b5a7e9c21", "schemaVersion": 1}, "term": "b5e2c8a1-7d3f-4e9b-a0c6-1f4d8b2e6a93", "checkedAtMs": "1795017600000", "expiresAtMs": "1795017630000", "leaseLifetimeMs": 30000}
  }
}
```

| Field | Meaning |
| --- | --- |
| `sipbridge` | sipbridge's `--instance` name and its random per-start epoch. A changed epoch means sipbridge restarted. |
| `sessionId` | Put this in every later message. |
| `capabilities` | The capabilities both sides support. |
| `stateAuthority` | Identifies where sipbridge keeps its state. With memory state, `primaryId` is new on every start: if it changed since your last session, everything that instance knew is gone. |
| `applicationLease` | Your controller's ownership of its calls: a `term` that lasts 30 seconds and is renewed by heartbeats. Context references and command results refer to it. |
| `inboundAuthPolicy` | Present only when sipbridge runs with `--require-auth-methods`: which methods it challenges locally. |

## status

After `helloAck` the session exists but receives no SIP work. The controller
switches work on, and can switch it off again, with `status`. It can be sent
at any time, and sipbridge does not reply:

```json
{"correlationId":"status-1","sessionId":"0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841","status":{"retainedReady":true,"bindingsReady":true,"admitCalls":true,"admitSubscriptions":true}}
```

| Field | What sipbridge does with it |
| --- | --- |
| `retainedReady`, `bindingsReady` | When both are `true`, the session is **ready**: sipbridge offers new requests to it and counts it for `/sip-readyz`. Set either to `false` to stop receiving new requests on this session. |
| `admitCalls`, `admitSubscriptions`, `draining` | Stored but not acted on. They describe what your controller wants; enforce them in your decisions. |
| `digestChallengePolicy` | The signing policy for [local digest challenges](../guide/application.md#local-initial-digest-challenges). Requires `sip.local-challenge.v1`. It contains a key, so never log it. |

Completing the handshake or answering heartbeats does not make a session
ready; only `status` does. If sipbridge rejects a `status`, for example an
invalid challenge policy, it closes the session.

## Heartbeats

sipbridge sends a `ping` every second (after the previous one was answered)
and closes the session when a ping has gone six seconds without a valid
`pong`. Other traffic does not count as an answer.

sipbridge to controller:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841","ping":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

Controller reply. Both `correlationId` and `nonce` echo the ping's nonce:

```json
{"correlationId":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11","sessionId":"0d4f7a52-93b1-4c8e-a6d2-7e15b9c3f841","pong":{"nonce":"0c4b1f0e-3d55-4b0b-a2e5-5f8e9d0a7b11"}}
```

The controller can send its own pings; sipbridge answers each with a `pong`
carrying the same `correlationId` and nonce.

Each valid `pong` also renews the controller's 30-second lease (at most once
every two seconds per session), so a controller that answers heartbeats keeps
ownership of its calls without doing anything else.

## When a session ends

When a session closes or misses its heartbeats, sipbridge:

- fails the operations that were waiting on it; a command without a result is
  `UNKNOWN` from your side;
- ends the term of that controller's lease, which **ends the calls that
  controller owned**. sipbridge still completes SIP work it already committed,
  such as sending an ACK or a cleanup BYE, but nothing is handed to a later
  session;
- leaves phone connections open, keeps phone subscriptions running until their
  own expiry, and leaves registrations alone, since those are your data.

With Redis state, every sipbridge instance sharing the state sees the same
lease, so one dropped session ends that controller's calls on all of them.

A reconnecting controller gets a new term and handles new work only. There is
no command to reclaim the previous calls.
[Restarts and Disconnects](../guide/lifecycle.md) covers the consequences.

## Reconnecting and duplicate sessions

sipbridge allows one session at a time per controller instance ID:

- **Reconnecting (same epoch, higher `connectionAttempt`).** The new session
  replaces the old one at once, and the old one is closed. Closing it ends
  that controller's calls, exactly as described above.
- **Reusing an attempt number.** sipbridge remembers the last accepted attempt
  for five minutes after a session closes. A number that is not higher is
  refused (`stale connection attempt`), which is why the counter must keep
  increasing.
- **A new epoch while the old session is still open.** Refused with
  `active epoch collision`. Once the old session has closed, the new process
  can connect.
- A new session becomes usable only after its lease is issued and `helloAck`
  is sent. A failed handshake never produces a usable session.
- sipbridge holds at most 64 sessions and remembers at most 256 controller
  instance IDs at once by default.
