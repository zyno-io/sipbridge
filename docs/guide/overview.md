# Overview

sipbridge is a SIP signaling server that you control from your own application.
It accepts SIP over UDP, TCP and TLS, holds the connections to phones and
trunks, and runs the protocol machinery: transactions, dialogs, retransmissions
and timers. It makes no decisions about accounts or calls. Those belong to a
**controller**, a program you write, which connects to sipbridge over a private
WebSocket and tells it how to answer each request and which requests to send.

## Why use it

Building a telephony application usually means either embedding a SIP stack in
your application or running a SIP proxy and scripting it. sipbridge splits the
work differently:

- **Your application stays free of SIP mechanics.** The controller sees a
  request, decides, and sends a short JSON command such as "respond 200 with
  this SDP". sipbridge builds the message, fills in Via, Contact, CSeq and tags,
  retransmits when SIP requires it, and handles ACK and CANCEL.
- **Client connections do not depend on your application's uptime.** Phones keep
  their TLS connections to sipbridge while you restart or redeploy the
  controller. Certificates are reloaded without a restart too.
- **A dialog can follow a client to another instance.** With shared Redis state,
  a client whose TLS connection drops and reconnects to a different sipbridge
  instance can continue its existing call or subscription there. This was the
  original reason for the project.
- **You are told what actually happened.** Every send is reported as written,
  not written, or unknown. sipbridge does not hide an uncertain send behind an
  automatic retry.

sipbridge is early-stage software. [Project Status](../reference/status.md)
lists what works today, what is not built yet and how it is tested.

## How a call flows

Here is an incoming call, simplified:

1. A phone sends `INVITE` over TLS. sipbridge parses it and notes which
   connection it arrived on.
2. sipbridge sends your controller an `ingress` message containing the original
   SIP message and the observed source address and transport.
3. The controller checks credentials and policy. If it wants the call, it admits
   the request with `ingest` and replies `routeResult` with `ACCEPTED`.
4. sipbridge presents the request back as a `request` message with a handle.
   The controller answers with `command` messages: `respond` with `180`, then
   `respond` with `200` and an SDP body it chose.
5. sipbridge builds and sends each response, reports a `result` for each
   command, retransmits the `200` until the phone's `ACK` arrives, and reports
   progress as `dialogEvent` messages.

Outgoing calls work the same way in reverse: the controller sends an `openUac`
command and sipbridge runs the client side of the transaction. The
[Control Protocol](../protocol/overview.md) section describes every message.

## Roles

| Role | Responsibility |
| --- | --- |
| **sipbridge** | Public SIP listeners, parsing, the SIP transaction, dialog and timer state, and the actual socket writes. It records where each message really came from, so the controller never has to trust a Via or Contact header for that. |
| **Controller** (your application) | Opens the control session; authenticates accounts and verifies digest credentials; routes new requests; decides responses and outgoing requests; owns registrations, subscription content and call logic. |
| **State store** | Where sipbridge keeps its protocol state. In process memory by default, or Redis when you select it. See [State Backends](./state-backends.md). |
| **Media server** | RTP and media. sipbridge carries the SDP bodies your controller chooses but handles no media itself. |

The smallest setup is one sipbridge process and one controller. Redis, a
particular controller implementation and a particular account backend are all
optional or up to you.

### Names used in the normative documents

sipbridge was first built for one telephony platform, and the
[specification](../spec/architecture.md) and the wire schema still use that
platform's component names. You will meet them in field names.

| Name | Meaning |
| --- | --- |
| **Nexus** | The controller. The wire field `hello.nexus` carries the controller's process identity. |
| **Pilot** | That platform's account backend. Only the controller talks to it; sipbridge never does. |
| **Talk** | That platform's repository, which owns the control schema and its generated code. |
| **Drachtio** | Another SIP server that platform runs. sipbridge does not depend on it. |
| **rtpbridge** | That platform's media server. |
| **Trusted directory** | The controller's own record of routing and registrations, separate from sipbridge's state store. |

## What sipbridge does

- Listens for SIP on UDP and TCP (default `:5060`) and TLS (default `:5061`).
- Parses SIP with a pinned build of the sipgo library and keeps each message's
  original bytes, transport and source.
- Finishes protocol work it already has everything for, such as CANCEL, ACK,
  repeating a final response and retransmissions, without asking the controller.
- Forwards other requests to a ready controller, which accepts, redirects or
  rejects them.
- Carries out controller commands: respond to a request, start an outgoing
  request, send a request inside a dialog, or cancel.
- Records each outgoing message in the state store before sending it, sends it
  on the exact connection it belongs to, and reports the outcome as `WRITTEN`,
  `NOT_WRITTEN` or `UNKNOWN`.
- Optionally rejects inbound REGISTER or SUBSCRIBE locally, and optionally sends
  the first digest challenge itself using a policy from the controller.

## What to know before you build

A few design choices shape what your application has to do.

**You write the controller.** There is no client SDK. The protocol is JSON over
a WebSocket, with a schema in the repository, so any language works, but the
controller is real work: it must route, authenticate and answer.
[Building a Controller](./application.md) lists the duties.

**Calls belong to one controller process.** sipbridge keeps a client's
connection and SIP dialog alive, but your call logic lives in your controller's
memory. If that controller process dies, sipbridge handles SIP cleanup rather
than handing its calls to another controller.

**Restarting sipbridge closes its connections.** Sockets cannot move between
processes, and there is no built-in drain. Plan sipbridge restarts as
maintenance and keep routine changes (controller releases, certificate renewal)
from restarting it. See [Deployment](./deployment.md#replacing-a-process).

**The default state is in memory.** It is lost on restart and, with default
settings, holds state for 16 Call-IDs at a time. Raise the limits or select Redis as
described in [State Backends](./state-backends.md).

**Some outcomes are unknown, and stay unknown.** When a send or a command might
or might not have taken effect, sipbridge says so. Your controller should look
up the current state instead of sending the same thing again. See
[Commands and Outcomes](../protocol/commands.md).

**sipbridge is not an open proxy.** It sends only to destinations the controller
authorized: a registered client or a target the controller named explicitly. It
does not handle media, store passwords or need elevated privileges such as
`NET_ADMIN`.

## Reading map

| If you want to | Read |
| --- | --- |
| Run it locally | [Getting Started](./getting-started.md) |
| Configure flags and environment | [Configuration](./configuration.md), [State Backends](./state-backends.md) |
| Build a controller | [Building a Controller](./application.md), [Control Protocol](../protocol/overview.md) |
| Operate it | [Health and Readiness](./health.md), [Deployment](./deployment.md), [Troubleshooting](./troubleshooting.md) |
| Know exact behavior | The [Architecture](../spec/architecture.md), [Protocol](../spec/protocol.md) and [Runtime](../runtime.md) specifications |

The guides explain and summarize. The specification pages are the detailed
engineering contracts, written for maintainers; if a guide and a specification
disagree, the specification is right.
