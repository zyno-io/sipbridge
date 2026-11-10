# Health and Readiness

sipbridge answers three separate questions about its health: is the process
alive, are its SIP listeners up, and can it take new calls right now. They are
separate because the right reaction to each is different. In particular, a
sipbridge that cannot take new calls is usually still holding client
connections and finishing existing ones, and restarting it would only make
things worse.

The endpoints are served over plain HTTP on the admin listener
(`--admin-listen`, default `127.0.0.1:8800`).

| Endpoint | `200` when | `503` body |
| --- | --- | --- |
| `/healthz` | The admin server is answering. | (never `503`) |
| `/sip-servingz` | Listeners have started, shutdown has not begun, and the public TLS certificate is within its validity period. | `SIP listeners unavailable` |
| `/sip-readyz` | At least one controller session is ready, the configured control credentials and public SIP TLS certificate are valid, and the state store passes its readiness checks, and (with `--require-auth-methods`) a current challenge policy is installed. | `signaling unavailable` or `authentication policy unavailable` |

A controller session counts as ready when its most recent `status` message had
both `retainedReady` and `bindingsReady` set to `true`. The other `status`
fields (`admitCalls`, `admitSubscriptions`, `draining`) are stored but do not
affect readiness or routing in sipbridge; they are for the controller's own use.

The state store check is cheap. With memory state it looks at the process's own
health and clock. With Redis it makes a time-limited check that the primary is
reachable and its clock is sane, and never scans keys.

## Using the endpoints

| Probe | Endpoint | Reason |
| --- | --- | --- |
| Liveness (restart if failing) | `/healthz` | Fails only when the process itself is broken. |
| Listeners up | `/sip-servingz` | Tells you whether the process is accepting connections at all. |
| Ready for new calls | `/sip-readyz` | Goes to `503` while no controller is ready or the state store is unreachable. |

The most important rule is to **point liveness probes at `/healthz` and nothing
else**. Restarting sipbridge closes every client connection it holds. Losing
Redis or the controller makes sipbridge not ready, but the process is fine and
recovers by itself when they return. A liveness probe on `/sip-readyz` would
turn a short controller outage into every phone reconnecting.

Not ready means "send no *new* work here". Readiness alone does not stop work
already under way: retransmissions, ACK and CANCEL handling and repeated final
responses can still run when their state is available, and healthy client
sockets stay open. If your platform removes not-ready instances from a load
balancer, existing connections
are unaffected, but new clients will not reach the instance until a controller
is ready again.

The admin listener has no authentication. When probes come from outside the
process's network namespace (for example from a kubelet), bind it to an address
the probe can reach, such as `--admin-listen=:8800`, and restrict access with
network policy.

The control listener also has a `/sip-readyz` path, but it always returns
`503 SIP service is not implemented`. Probe the admin listener.

## Startup sequence

1. Parse flags, method and authentication policy, and state settings. Invalid
   values exit with an error on stderr.
2. Open the state store. Memory is usable immediately; Redis opens without
   waiting for the server.
3. Load control and public TLS certificates. Invalid or expired files fail
   startup.
4. Open the control and public TLS listeners, start the admin listener, then
   open the UDP/TCP listener unless it is disabled.
5. Write the `sipbridge-listening` JSON line to stdout. `/sip-servingz` returns
   `200` from here on.
6. Wait for controllers. Each controller connects, sends `hello`, receives
   `helloAck` and then sends `status`.
7. Once a controller is ready and the state store is reachable, `/sip-readyz`
   returns `200`.

Between steps 5 and 7 the listeners are open, but new requests needing a
controller receive `503`. Retained SIP work can still complete. A request for a dialog that the state
store shows does not exist gets `481` instead.

## Shutdown

`SIGINT` or `SIGTERM` stops the process promptly. It closes control sessions,
listeners, client connections and the state store without waiting for calls to
finish; there is no built-in drain. With memory state, everything it knew is
gone as well. Because of this, treat a restart as planned maintenance and
follow the steps in [Deployment](./deployment.md#replacing-a-process).
