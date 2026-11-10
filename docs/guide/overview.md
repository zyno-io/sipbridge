# Overview and Scope

sipbridge is an experimental SIP data plane. It accepts SIP over UDP, TCP and
TLS, runs SIP transactions, dialogs and timers natively, and records every
message it sends as a committed state effect. Business decisions such as account
authentication, call routing, registration policy and subscription content
belong to a separate **application controller** that connects over a private,
mutually authenticated WebSocket.

The original goal is to keep a client's SIP protocol context when its TLS
connection moves to another ingress process, without moving the controller's
in-memory call.

::: warning Status
sipbridge is experimental. Local native, process/socket and fixture checks have
separate, limited scopes. Production selection, full platform conformance, load
qualification and safe SIP-core replacement remain gated. Nothing in this
documentation grants deployment authorization. See
[Implementation Status](../reference/status.md).
:::

## Roles

| Role | Responsibility |
| --- | --- |
| **sipbridge** | Public SIP listeners, framing, observed source/transport evidence, native SIP transaction/dialog/timer execution, committed effects and exact socket writes. |
| **Application controller** | Initiates control sessions; routes new requests; authorizes accounts and verifies digest credentials; decides responses and outgoing requests; owns registration bindings, subscription content and call state. |
| **SIP state store** | Retained native protocol state with revisions, owner leases and committed effects. Process-local memory by default, or explicit Redis. |
| **Media server** | RTP/media. sipbridge carries SDP bodies chosen by the controller but processes no media. |
| **External platform services** | PKI issuance, firewall/abuse enforcement, routing, collectors and service discovery remain outside sipbridge. |

A standalone deployment needs one sipbridge process and a controller that
implements the [control protocol](../protocol/overview.md). Redis is not required
for one instance. No particular controller product, account backend or image
publisher is required to run the service; those are deployment choices.

### Names used in the normative documents

The [specification](../spec/architecture.md) and the wire schema were written for
the original platform and use its component names. Field names keep those names.

| Name | Generic meaning |
| --- | --- |
| **Nexus** | The application controller. The wire field `hello.nexus` carries the controller's process identity. |
| **Pilot** | An account-mutation backend reached only through the controller. sipbridge never talks to it. |
| **Talk** | The platform repository that owns the canonical control schema and its generated bindings. |
| **Drachtio** | The SIP stack used by the original platform's supported production deployment. |
| **rtpbridge** | The media server used in the original platform. |
| **Trusted directory** | The controller's own routing/binding authority, separate from sipbridge's SIP-state store. |

## What sipbridge does

- Listens for public SIP on UDP/TCP (default `:5060`) and TLS (default `:5061`).
- Parses and frames SIP with the pinned sipgo library and keeps the original bytes
  and the actual observed transport and source.
- Completes retained native work, such as CANCEL, ACK, cached finals and
  retransmissions, without a controller round trip when it can prove an exact
  retained match.
- Forwards other requests to a ready controller, which accepts, redirects or
  rejects them.
- Executes controller commands (respond, open a client transaction, send an
  in-dialog request, cancel) as native SIP state transitions.
- Writes only committed effects to the exact physical flow that owns them, and
  records the outcome as `WRITTEN`, `NOT_WRITTEN` or `UNKNOWN`.
- Optionally rejects inbound REGISTER/SUBSCRIBE locally, and optionally issues
  initial digest challenges under a policy supplied by the controller.

## What sipbridge does not do

- It does not authenticate accounts, store passwords, or decide call policy.
- It does not process media or choose SDP.
- It does not act as a general-purpose proxy that can dial an arbitrary
  caller-supplied address; outgoing destinations are controller-authorized
  bindings or explicit network targets.
- It does not migrate a controller's in-memory call between controllers.
- It does not provide an in-process drain protocol or zero-disconnect process
  replacement. Restarting a process closes its sockets.
- It does not need `NET_ADMIN`, kernel enforcement, Kubernetes credentials or a
  business database.

## Reading map

| If you want to | Read |
| --- | --- |
| Run it locally | [Getting Started](./getting-started.md) |
| Configure flags and environment | [Configuration](./configuration.md), [State Backends](./state-backends.md) |
| Build a controller | [Application Responsibilities](./application.md), [Control Protocol](../protocol/overview.md) |
| Operate it | [Health and Readiness](./health.md), [Deployment](./deployment.md), [Troubleshooting](./troubleshooting.md) |
| Know exact behavior | The normative [Architecture](../spec/architecture.md), [Protocol](../spec/protocol.md) and [Runtime](../runtime.md) |

The guide pages summarize behavior. Where they differ from the normative
documents, the normative documents win.
