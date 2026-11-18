---
description: Building an image, choosing a topology, and restarting, upgrading or rolling back sipbridge with the least disruption.
---

# Deployment

This page covers building an image, laying out a deployment, and restarting
sipbridge with the least disruption. One fact shapes all of it: sipbridge holds
your phones' connections, and a connection cannot survive the restart of the
process that holds it. A good deployment restarts sipbridge rarely and on
purpose.

## Image

Build an image from a source checkout:

```sh
SIPBRIDGE_IMAGE=sipbridge:local BUILD_VERSION=dev bash scripts/build-image.sh
docker run --rm sipbridge:local --version
```

The script checks the pinned SIP library and the control schema against their
recorded checksums, then runs `docker build` with the current Git revision.

| Property | Value |
| --- | --- |
| Base | `scratch`, with the Debian CA bundle and dependency license notices under `/licenses` |
| Entrypoint | `/sipbridge` (pass flags as container arguments) |
| Also included | `/sipbridge-state-inspect` |
| User | UID/GID `65532` |
| Exposed ports | `5060/udp`, `5060/tcp`, `5061/tcp`, `8443/tcp`, `8800/tcp` |
| Control schema receipt | `/usr/share/sipbridge/control-schema.json` |
| OCI labels | Source, version and revision |

Deploy by image digest rather than by a moving tag, and choose that digest
independently of your controller's releases, so that shipping a new controller
never restarts sipbridge by accident. Compare the image's schema receipt with
the schema your controller was built from; the protocol's major version alone
does not prove compatibility.

The container needs no special privileges and does not write to its
filesystem, so a read-only root filesystem works. The admin listener defaults
to `127.0.0.1:8800`; if your platform probes from outside the container, set
`--admin-listen=:8800` (or the pod address) and limit who can reach it.

## Topology

| Topology | State | Notes |
| --- | --- | --- |
| One instance | Memory | The simplest setup. A restart loses all SIP state and connections. Size [capacity](./state-backends.md#capacity) for your peak. |
| Several instances sharing calls | Redis | A phone that reconnects to another instance can continue its call there. Connections, registrations in progress and subscriptions still belong to one process. All instances in a namespace need the same identity key, namespace settings, advertised addresses and sipbridge build. |

Every controller process should hold a control session to every sipbridge
instance it may have calls on.

## What restarts sipbridge

Only a change to sipbridge itself needs a restart: a new image, different
flags or environment, or a different state backend. None of these do:

- renewing certificates, keys, CA bundles or bearer tokens (reloaded in place);
- restarting, upgrading or reconnecting controllers;
- a Redis failover;
- changes to your controller's policy or data.

Keep it that way. Keep controller settings out of the sipbridge pod template,
or an ordinary controller release will roll sipbridge too. On Kubernetes, a
StatefulSet with the `OnDelete` update strategy works well: template changes
are staged, and each pod picks them up only when you delete it.

## Replacing a process

sipbridge drains on `SIGTERM` (or interrupt):

1. **Readiness is withdrawn.** `/sip-readyz` and `/sip-servingz` return `503`;
   your load balancer must watch them. New connections, new outgoing
   connections and new UDP peers are refused. Control sessions and state
   access stay available.
2. **Admitted work finishes.** Responses, ACKs, CANCELs and writes in progress
   continue. New requests on existing connections get `503`. After
   `--sip-withdrawal-grace` (default 5 seconds), connections with nothing
   outstanding close.
3. **Remaining connections close** at `--sip-drain-timeout` (default 40
   seconds after the signal). Traffic does not extend it. Closing a socket does
   not change what sipbridge has recorded about a call; a send that was in
   progress keeps its real outcome, which may be `UNKNOWN`.
4. **The process exits** after a short reporting tail of up to two seconds.
   Set your platform's termination grace period above the drain timeout plus
   that tail, for example 45 seconds for the defaults.
5. **Check the replacement** (`/sip-servingz`, then `/sip-readyz` once a
   controller connects) before replacing the next instance.

Phones still connected at the end have to reconnect. With memory state, they
register again and their calls are gone. With Redis, a phone that reconnects
can continue a shared call, as long as its controller's session stayed up.
Subscriptions on the stopped process are gone in both cases. Replacing
sipbridge without any phone reconnecting is not possible.

Draining sipbridge does not drain your controller's calls; that is the
controller's decision. The exact rules are in the
[runtime drain contract](../runtime.md#bounded-sip-connection-drain).

## Upgrading instances that share Redis

All instances sharing a Redis namespace must run the same sipbridge build.
Newer builds can write records that older builds do not understand, so a mix
of old and new instances is not safe, and an older build is not a safe
rollback once a newer one has written to the namespace.

Upgrade the whole group in one maintenance step: drain and stop every old
instance in the namespace, confirm none is left, then start all the new ones.
Calls stored in Redis stay there; phones reconnect and, where their controller
kept its session, continue. Afterwards, check controller readiness,
re-registration, subscriptions and NOTIFY delivery. `OnDelete` alone does not
enforce this order; make the stop/start boundary an explicit step in your
deployment tooling.

## Rollback

Roll back by deploying the previous image digest with the same steps.

- **Memory:** a restart loses state anyway; phones register again.
- **Redis:** compare `sipbridge --state-schema` and the control schema receipt
  of both builds. If the storage receipts differ, do not mix them: stop every
  instance and start the old build on an empty namespace (or a new
  `SIPBRIDGE_STATE_GENERATION`), accepting that calls in progress are lost.
- **Controller:** check the image's control schema receipt and required
  capabilities before connecting your controller to it.

## Changing configuration

| Change | How |
| --- | --- |
| Certificates, CA bundles, bearer token | Replace the files in the mounted directory. No restart. |
| Method policy, required-auth methods, advertised addresses, listeners, drain timing | Restart the process. |
| State backend, namespace, generation, prefix, identity key | Restart every affected process. Memory state is lost; Redis state is usable only by instances with matching settings. |
| Allowed controller URI identities | Restart the process. Add the new identity before you rotate controller certificates, so old and new are accepted during the change. |
