---
description: What sipbridge does, what your controller decides, how a request flows, and the design choices that shape your application.
---

# Overview

sipbridge is a SIP signaling server that you control from your own
application. It accepts SIP over UDP, TCP and TLS, holds the connections to
phones and trunks, and runs the protocol machinery: transactions, dialogs,
retransmissions and timers. It makes no decisions about accounts or calls.
Those belong to a **controller**, a program you write, which connects to
sipbridge over a WebSocket and tells it how to answer each request and which
requests to send.

## Why use it

Building a telephony application usually means embedding a SIP stack in your
application or scripting a SIP proxy. sipbridge splits the work differently:

- **Your application stays free of SIP mechanics.** The controller sees a
  request, decides, and sends a typed command such as "respond 200 with this
  SDP". sipbridge builds the message, fills in Via, Contact, CSeq and tags,
  retransmits when SIP requires it, and handles ACK and CANCEL.
- **You write it in any language.** The protocol is strict JSON over a
  WebSocket, defined by a schema in the repository.
- **You are told what actually happened.** Every send is reported as written,
  not written, or unknown. An uncertain send is never retried behind your
  back.
- **Phones stay connected while your controller is redeployed.** Phone
  connections, registrations and subscriptions do not depend on your
  controller process. Calls in progress do; see the limitations below.
- **Calls can be shared between instances.** With Redis state, several
  sipbridge processes share call state, so a phone whose TLS connection moves
  to another instance can continue its call there.

sipbridge is early-stage software. [Project Status](../reference/status.md)
lists what works today, what is not built and how it is tested.

## How a request flows

1. A phone sends `INVITE`. sipbridge parses it and records which connection it
   arrived on.
2. sipbridge offers it to your controller as a `request`, once.
3. Your controller checks the caller and its policy, and answers with one
   `requestDecision`: reject it, answer it, or accept it and answer later.
4. sipbridge sends the responses you choose, retransmits them as SIP requires,
   and handles the phone's ACK. It reports progress as `dialogEvent` messages.
5. Some work never reaches your controller: OPTIONS probes, retransmissions,
   CANCEL, ACK and BYE matching are handled by sipbridge alone.

Outgoing calls work in reverse: your controller sends `openUac`, and sipbridge
runs the client side. [A Call, Step by Step](./call-walkthrough.md) shows
every message.

## Roles

| Role | Responsibility |
| --- | --- |
| **sipbridge** | Public SIP listeners, parsing, SIP transaction, dialog and timer state, and the actual socket writes. It records where each message really came from, so your controller never has to trust a Via or Contact header for that. |
| **Controller** (your application) | Opens the control session; authenticates accounts and verifies digest credentials; decides responses and outgoing requests; owns registrations, subscription content and call logic. |
| **State store** | Where sipbridge keeps SIP state: process memory by default, or Redis for calls shared between instances. See [State Backends](./state-backends.md). |
| **Media server** | RTP and audio. sipbridge carries the SDP your controller chooses but handles no media. |

The smallest setup is one sipbridge process and one controller.

## Design choices that affect your application

**You write the controller.** There is no client SDK. Generated Go and
TypeScript types are provided, and the controller is real work: it
authenticates accounts, decides requests and keeps registrations.
[Building a Controller](./application.md) lists the duties.

**A controller that disconnects loses its calls.** If your controller's
session drops, sipbridge ends the calls that controller owned. It still
finishes SIP work it already committed, such as an ACK or a cleanup BYE, but
it does not hand calls to a reconnected or replacement controller. Plan
controller deployments so that sessions to sipbridge stay up while calls are
in progress. See [Controller Restarts](./controller-recovery.md).

**Restarting sipbridge closes its connections.** Sockets cannot move between
processes. On `SIGTERM` sipbridge stops taking new connections, lets admitted
work finish for a bounded time, then closes what is left; phones reconnect.
See [Deployment](./deployment.md#replacing-a-process).

**The default state is in memory.** It is lost on restart, so phones register
again and calls in progress end. Redis lets instances share calls, but
registrations in progress and subscriptions still belong to the process that
accepted them. See [State Backends](./state-backends.md).

**Some outcomes are unknown, and stay unknown.** When a send or a command
might or might not have taken effect, sipbridge says so. Your controller reads
the current state instead of sending the same thing again. See
[Commands and Outcomes](../protocol/commands.md#handling-unknown).

**sipbridge is not an open proxy.** It sends only where your controller
explicitly says: a registered phone's current connection or a network target
it names. It stores no passwords, handles no media and needs no special
privileges.

## Reading map

| If you want to | Read |
| --- | --- |
| Run it and answer a request | [Getting Started](./getting-started.md) |
| See a call message by message | [A Call, Step by Step](./call-walkthrough.md) |
| Build a controller | [Building a Controller](./application.md), [Control Protocol](../protocol/overview.md) |
| Configure and operate it | [Configuration](./configuration.md), [Health and Readiness](./health.md), [Deployment](./deployment.md), [Troubleshooting](./troubleshooting.md) |
| Know the exact rules | The [advanced reference](../spec/architecture.md): architecture, protocol and runtime specifications |

The guides explain. The specifications are the detailed engineering
contracts; if a guide and a specification disagree, the specification is
right.
