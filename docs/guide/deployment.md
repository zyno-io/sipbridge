# Deployment

This page describes how the service is packaged and how to replace it safely.
It is guidance for evaluation environments. It is not a production runbook, and
production qualification remains gated (see [Implementation Status](../reference/status.md)).

## Image

Build a local image from a checkout:

```sh
SIPBRIDGE_IMAGE=sipbridge:local BUILD_VERSION=dev bash scripts/build-image.sh
docker run --rm sipbridge:local --version
```

The script verifies the pinned SIP library and control-schema snapshot, then runs
`docker build` with the current Git revision.

| Property | Value |
| --- | --- |
| Base | `scratch`, with the Debian CA bundle and dependency license notices under `/licenses` |
| Entrypoint | `/sipbridge` (pass flags as container arguments) |
| User | UID/GID `65532` |
| Exposed ports | `5060/udp`, `5060/tcp`, `5061/tcp`, `8443/tcp`, `8800/tcp` |
| Control-contract receipt | `/usr/share/sipbridge/control-schema.json` |
| OCI labels | Source, version and revision |

The canonical CI publishes images to the project's own registry. This
documentation does not assume that registry is publicly reachable. Wherever the
image comes from, deploy it by immutable digest, chosen independently of
controller releases, and check the contract receipt against the schema your
controller was built from.

Because the admin listener defaults to `127.0.0.1:8800`, container platforms
that probe from outside the container need `--admin-listen=:8800` (or the pod
address) and network policy restricting who can reach it.

The process needs no `NET_ADMIN` capability and does not write to its
filesystem, so a read-only root filesystem is suitable.

## Topology choices

| Topology | State | Notes |
| --- | --- | --- |
| Single standalone instance | Memory | Simplest. A restart loses all SIP state and connections. |
| Several workers sharing state | Redis | Compatible workers can continue retained logical contexts. Sockets still belong to one process. All workers in a namespace need the same identity key, partitions, profile and public advertisement. |

Run controllers so that every controller holds a session to every sipbridge
process that may need it.

## What restarts a SIP process

Only changes to the sipbridge image, its flags or environment, or its state
backend require replacing the process. These do not:

- certificate, key or trust bundle renewal (reloaded in place);
- controller restarts, rollouts or reconnects;
- Redis failover;
- controller policy, discovery or directory changes.

Keep controller configuration out of the SIP process's deployment template so an
ordinary controller release cannot roll it. On Kubernetes, an `OnDelete`
StatefulSet update strategy stages template changes without restarting live
processes.

## Replacing a process

There is no in-process drain, and the proposed drain protocol is not
implemented. `SIGTERM` closes the process's listeners and sockets promptly.
Replacement is therefore a controlled procedure:

1. **Fence new admission.** Stop routing new clients and calls to the process,
   for example by removing it from load balancing and having controllers stop
   admitting new work through it.
2. **Preserve retained paths.** Keep controller sessions and the process's
   sockets up while existing calls, transactions and subscriptions finish.
3. **Drain.** Wait for native and application work to complete. Registrations
   and subscriptions can keep a process busy indefinitely; decide explicitly how
   to end or move them.
4. **Withdraw** the instance from any backend pools.
5. **Restart deliberately**, then verify `/sip-servingz` and `/sip-readyz`.

Remaining client TLS connections close when the process stops. Controller calls
do not migrate. Zero-disconnect replacement is not provided.

## Rollback

Rollback means deploying a previous image digest with the same replacement
procedure.

- **Memory state:** any restart loses state, so rollback has no state
  compatibility concern beyond normal client re-registration.
- **Redis state:** every worker sharing a namespace must run the same
  schema 2 / reducer 9 / timers 1 profile. Do not mix incompatible builds in one
  namespace.
- **Controller compatibility:** a controller must still accept the capability
  set that the rolled-back build negotiates. PROTOCOL.md requires keeping
  compatibility labels and generated fields until both sides retire them
  together; see [Protocol: compatibility](../spec/protocol.md#_11-compatibility-reload-and-drain).

## Changing configuration

| Change | Method |
| --- | --- |
| Certificates and trust bundles | Replace files in the mounted directory. No restart. |
| Method policy, required-auth methods, advertisement, listeners | Replace the process. |
| State backend, namespace, identity key, partitions | Replace every affected process. Memory state is lost; Redis state is only usable under compatible settings. |
| Allowed controller URI identities | Replace the process. Add new identities before rotating controller certificates. |
