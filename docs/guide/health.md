# Health and Readiness

sipbridge separates three questions: is the process broken, are its SIP listeners
serving, and can it accept new signaling work. The endpoints are on the
plain-HTTP admin listener (`--admin-listen`, default `127.0.0.1:8800`).

| Endpoint | `200` when | `503` body |
| --- | --- | --- |
| `/healthz` | The admin server is answering. | (never `503`) |
| `/sip-servingz` | Listeners have started, shutdown has not begun, and the public TLS certificate is within its validity period. | `SIP listeners unavailable` |
| `/sip-readyz` | At least one controller session is ready, both TLS contexts are valid, the state store is ready, and (with `--require-auth-methods`) a current challenge policy is installed. | `signaling unavailable` or `authentication policy unavailable` |

A controller session is ready when its last `status` had both `retainedReady`
and `bindingsReady` set to `true`. `admitCalls`, `admitSubscriptions` and
`draining` are stored but not used by sipbridge for readiness or routing; the
controller applies them in its own routing decisions.

For the state store, memory readiness checks local lifecycle and clock health.
Redis readiness performs bounded primary and clock checks and never scans keys.

## Using the endpoints

| Probe | Endpoint | Reason |
| --- | --- | --- |
| Liveness | `/healthz` | Restarting the SIP process closes every client connection. Loss of Redis, a controller, a registry or policy must not trigger a restart. |
| Serving / traffic eligibility for listeners | `/sip-servingz` | Reports whether the process is accepting connections at all. |
| Readiness for new signaling work | `/sip-readyz` | Fences new work while the controller or datastore is unavailable. |

Readiness fences *new* work only. Retained native duties, such as
retransmissions, ACK, CANCEL and cached finals, and healthy client sockets
continue regardless of readiness. If your platform removes not-ready instances
from a load balancer, existing connections are unaffected, but new clients will
not reach the instance until a controller is ready.

The admin listener is unauthenticated. When probes come from outside the
process's network namespace (for example a kubelet), bind it to an address the
probe can reach, such as `--admin-listen=:8800`, and restrict access to it with
network policy.

The control listener also answers `/sip-readyz` for authenticated controllers
but always returns `503 SIP service is not implemented`. Use the admin listener.

## Startup sequence

1. Parse flags, method and authentication policy, and state configuration.
   Invalid values exit with an error on stderr.
2. Open the state store. Memory is immediately usable; Redis opens without
   waiting for the server.
3. Load control and public TLS credentials. Invalid or expired files fail startup.
4. Open the control and public TLS listeners, start the admin listener, then
   open the UDP/TCP listener unless it is disabled.
5. Write the `sipbridge-listening` JSON line to stdout. `/sip-servingz` returns `200`.
6. Wait for controllers. Each controller connects, sends `hello`, receives
   `helloAck` and then sends `status`.
7. When a controller is ready and the store is ready, `/sip-readyz` returns `200`.

Before step 7, inbound requests that cannot be completed natively are refused
with `503`, or `481` when the state store proves the dialog does not exist.

## Shutdown

`SIGINT` or `SIGTERM` stops the process promptly: control sessions, listeners and
the state store are closed. There is no in-process drain. Memory state is lost,
and every client connection to the process is closed. Plan replacement with the
procedure in [Deployment](./deployment.md#replacing-a-process).
