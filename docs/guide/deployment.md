# Deployment

This page covers how to get an image, how to lay out a deployment, and how to
restart sipbridge with the least disruption. The one fact that shapes all of it
is that sipbridge holds your clients' connections, and a connection cannot
survive the restart of the process that holds it. A good deployment therefore
restarts sipbridge rarely and on purpose.

## Image

The public project home is the
[sipbridge repository](https://github.com/zyno-io/sipbridge). To build your own
image, run these commands from a sipbridge source checkout:

```sh
SIPBRIDGE_IMAGE=sipbridge:local BUILD_VERSION=dev bash scripts/build-image.sh
docker run --rm sipbridge:local --version
```

The script checks the included SIP library and control schema against their
recorded checksums, then runs `docker build` with the current Git revision.

| Property | Value |
| --- | --- |
| Base | `scratch`, with the Debian CA bundle and dependency license notices under `/licenses` |
| Entrypoint | `/sipbridge` (pass flags as container arguments) |
| User | UID/GID `65532` |
| Exposed ports | `5060/udp`, `5060/tcp`, `5061/tcp`, `8443/tcp`, `8800/tcp` |
| Control schema record | `/usr/share/sipbridge/control-schema.json` |
| OCI labels | Source, version and revision |

Deploy by image digest rather than by a moving tag, and choose that digest
independently of your controller's releases, so that shipping a new controller
can never restart sipbridge by accident. The file
`/usr/share/sipbridge/control-schema.json` inside the image records which
version of the control schema it was built with; compare it with the schema
your controller was built from.

The container needs no special privileges. It does not need `NET_ADMIN` and
does not write to its filesystem, so a read-only root filesystem works. The
admin listener defaults to `127.0.0.1:8800`; on a platform that probes from
outside the container, set `--admin-listen=:8800` (or the pod address) and
limit who can reach it with network policy.

## Topology choices

| Topology | State | Notes |
| --- | --- | --- |
| One standalone instance | Memory | The simplest setup. A restart loses all SIP state and connections. Remember to raise the [memory capacity](./state-backends.md#capacity) above its default of 16 Call-IDs. |
| Several instances sharing state | Redis | A client that reconnects to another instance can continue its dialog there. Each connection still belongs to one process. All instances in a namespace need the same identity key, partition count, state format and advertised addresses. |

Every controller should hold a control session to every sipbridge instance it
may have calls on.

## What restarts a SIP process

Only a change to sipbridge itself needs a restart: a new image, different flags
or environment, or a different state backend. None of the following do:

- renewing certificates, keys or CA bundles (they are reloaded in place);
- restarting, upgrading or reconnecting controllers;
- a Redis failover;
- changes to controller policy, discovery or routing data.

Arrange your deployment so that it stays that way. Keep controller settings out
of the sipbridge pod template, or an ordinary controller release will roll
sipbridge too. On Kubernetes, a StatefulSet with the `OnDelete` update strategy
is a good fit: template changes are staged, and each pod picks them up only
when you delete it.

## Replacing a process

sipbridge has no built-in drain. On `SIGTERM` it closes its listeners and
connections promptly, whatever calls are in progress. (A drain protocol is
proposed but not implemented.) To restart an instance gracefully, you do the
draining around it:

1. **Stop sending it new work.** Take the instance out of load balancing or
   DNS, and have your controllers stop placing new calls through it.
2. **Leave everything else connected.** Keep the controller sessions and the
   process running so that existing calls, transactions and subscriptions can
   finish normally.
3. **Wait for work to finish.** Calls end by themselves; registrations and
   subscriptions may not, since clients keep refreshing them. Decide how long to
   wait and what to do with what remains.
4. **Remove the instance** from any remaining backend pools.
5. **Restart it**, then check `/sip-servingz` and `/sip-readyz`.

Any client still connected when the process stops has to reconnect. With
memory state, lost registrations and calls must be set up again; Redis can
retain contexts for a replacement connection that passes the continuity checks. Calls do not move between controllers, and
replacing sipbridge without disconnecting anyone is not possible today.

## Rollback

To roll back, deploy the previous image digest using the same steps. What to
check first depends on your state backend:

- **Memory:** a restart loses state anyway, so there is nothing to keep
  compatible. Clients register again as after any restart.
- **Redis:** all instances sharing a namespace must use the same state format
  (currently schema 1, reducer 1, timers 1). Do not run builds with different
  state formats against one namespace.
- **Controller:** check the image's schema receipt and required capabilities before
  reconnecting it to your controller.

## Changing configuration

| Change | Method |
| --- | --- |
| Certificates and CA bundles | Replace the files in the mounted directory. No restart. |
| Method policy, required-auth methods, advertised addresses, listeners | Restart the process. |
| State backend, namespace, identity key, partitions | Restart every affected process. Memory state is lost; Redis state is usable only by instances with matching settings. |
| Allowed controller URI identities | Restart the process. Add the new identity before you rotate controller certificates, so both old and new are accepted during the change. |
