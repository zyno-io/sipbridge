---
description: What survives when your controller restarts or reconnects, why calls do not, and how to deploy controllers without dropping calls unexpectedly.
---

# Controller Restarts

Your controller will restart: deployments, crashes, scaling. This page
explains what survives, what does not, and how to plan for it.

## The short version

| Survives a controller restart | Does not survive |
| --- | --- |
| Phone connections to sipbridge | Calls the controller owned |
| Registrations (they are your data) | Commands whose results had not arrived (treat them as `UNKNOWN`) |
| Phone subscriptions, until their own expiry or connection close | Anything your controller held only in its own memory |
| sipbridge itself, its readiness once another controller is ready | |

## Calls end with the controller's session

sipbridge ties every call to the controller process that accepted or placed
it, through a lease that heartbeats keep alive. When that controller's session
closes or stops answering heartbeats, the lease ends at once and sipbridge
ends its calls: answered calls get a BYE, unanswered ones are refused or
cancelled. SIP work that was already committed still completes on its normal
timers. The details are in [Restarts and Disconnects](./lifecycle.md#owner-lease).

There is **no controller reattachment**. A reconnecting session, or a new
controller process, cannot claim the previous calls, even with the same
instance name and credentials. The protocol schema contains fields for a
reattachment feature (`hello.recovery`, `sip.controller-reattach.v1`,
`controllerContexts`), but no serving sipbridge offers it; a hello that asks
for it is refused. Those fields exist only for
[test fixtures](../controller-recovery.md).

## Subscriptions can change hands

Subscriptions survive their controller. Once the old controller's lease has
ended, a new controller can take a subscription over in either of two ways:

- **Wait for the refresh.** When the phone refreshes the subscription, the
  refresh arrives as an ordinary `request` at a ready controller. Accepting it
  makes that controller the subscription's new owner.
- **Act first.** If your controller stored the subscription's context
  reference (for example in a shared database), it can `inspect` it and send
  the next NOTIFY with a `dialogRequest`, using the next command sequence.

Before the old lease has ended, a new controller's NOTIFY is refused with
`NOT_ACCEPTED` and does not use up a sequence number.

## Deploying controllers without surprises

- **Drain calls before stopping a controller.** Stop accepting new calls (set
  `admitCalls` to `false` in your own logic and decline new INVITEs), wait for
  calls in progress to end, then close the sessions. Keep the process
  answering heartbeats until then.
- **Overlap old and new processes.** Start the new controller first; it
  connects with its own epoch and receives new requests once ready. The old
  process keeps its sessions, and its calls, until they finish.
- **Never reuse an epoch.** Generate a fresh random epoch at every start, and
  increase `connectionAttempt` on every reconnect within one process.
- **Keep sessions to every sipbridge instance**, including instances that are
  draining: their remaining calls still need you.
- **After any reconnect, inspect before acting.** Commands that were in flight
  when the session dropped have unknown outcomes.

If your service cannot accept losing calls when a controller fails, run more
than one controller process and keep each one's sessions stable; the protocol
cannot move a call from a failed controller to a healthy one.
