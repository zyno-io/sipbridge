# Configuration

sipbridge is configured in two places. Command-line flags set the listeners,
TLS identities and SIP policy. Environment variables choose where state is kept.
There is no configuration file, and a flag cannot be set through the
environment.

Settings are read once at startup, so changing one means restarting the
process. The exception is certificate and key files, which are re-read while
running (see [Identities and TLS](./security.md#credential-reload)).

A minimal invocation needs an instance name, an advertised address and the TLS
files; [Getting Started](./getting-started.md#run) shows one.

## Command-line flags

```sh
sipbridge [flags]
```

### Listeners

| Flag | Default | Description |
| --- | --- | --- |
| `--sip-listen` | `:5061` | Public SIP over TLS listener. Always enabled. |
| `--sip-plain-listen` | `:5060` | Public SIP over UDP and TCP, on the same address. An empty value disables both. |
| `--control-listen` | `:8443` | Private control listener (mutual TLS WebSocket). |
| `--admin-listen` | `127.0.0.1:8800` | Plain-HTTP health and metrics listener with no authentication. See [Admin Endpoints](../reference/admin-endpoints.md). |
| `--sip-interface` | empty | Name of a network interface. When set, the public SIP listeners and outbound TCP/TLS dials bind its single global-unicast IPv4 address. |

### Identities and credentials

| Flag | Default | Description |
| --- | --- | --- |
| `--instance` | none, **required** | Stable name for this process, for example a StatefulSet pod name such as `sipbridge-0`. Must match `[A-Za-z0-9_.:@+-]{1,96}`. sipbridge adds a random per-start ID (the "epoch") so a restarted process is never mistaken for its predecessor. |
| `--control-cert`, `--control-key` | none, **required** | PEM certificate and key for the control listener. |
| `--client-ca` | none, **required** | PEM bundle trusted to issue controller client certificates. |
| `--allowed-client-uri` | none, **required** | Comma-separated list of exact controller certificate URI SANs. No wildcards, empty entries or spaces. |
| `--sip-cert`, `--sip-key` | none, **required** | PEM certificate and key for the public SIP TLS listener. |

### Public advertisement

| Flag | Default | Description |
| --- | --- | --- |
| `--advertise` | none, **required** unless `--sip-interface` is set | Public `host:port` used for TLS Via/Contact advertisement. With `--sip-interface` and no value, it defaults to the bound TLS listener address. |
| `--advertise-plain` | TLS advertised host plus the plain listener port | Public `host:port` for UDP/TCP advertisement, for example a NAT or VIP address. |
| `--contact` | `sip:sipbridge@<advertise>;transport=tls` | Public Contact URI template. |

These addresses are what sipbridge writes into the Via and Contact headers it
generates, so they must be reachable by your SIP clients. Instances that share
Redis state must all advertise the same addresses, because one of them may
continue a dialog that another started. Changing the advertisement never moves
an existing dialog to a different transport or connection.

### Routing and SIP policy

| Flag | Default | Description |
| --- | --- | --- |
| `--initial-nexus` | empty | Instance ID of the controller that should be asked first about new requests. It is used only while that controller is ready, and the controller can still redirect. Without it, new requests rotate across ready controllers. Requests for an existing call prefer the controller that owns it. |
| `--allow-register` | `true` | `false` rejects every inbound REGISTER with `405` without consulting the state store or a controller. Useful on an instance that only faces carriers. |
| `--allow-subscribe` | `true` | `false` rejects every inbound SUBSCRIBE with `405` in the same way. |
| `--require-auth-methods` | empty | Comma-separated subset of `REGISTER`, `INVITE`, `SUBSCRIBE` (uppercase) that receive a local initial digest challenge when they carry no parseable credentials. Requires controller support; see [Local challenges](./application.md#local-initial-digest-challenges). |
| `--sip-capture-url` | empty | Optional edge-local SIP capture collector. See [Observability](./observability.md#sip-capture). |
| `--version` | | Print `sipbridge <version>` and exit. |

Startup fails if `--require-auth-methods` names a method you also disabled, an
unknown method or the same method twice. The `405` response's `Allow` header
lists the methods this process still accepts.

### Validation rules

- `--instance` and an advertisement must be present.
- `--sip-interface` must name an interface with exactly one global-unicast IPv4
  address. A listener with a fixed host must use that address; listeners must
  use fixed ports.
- `--sip-capture-url` must be `http://<host>/loki/api/v1/push` without
  credentials, query or fragment. When the plain listener is enabled it must be
  bound to a specific IP address, not `:5060` or `0.0.0.0`.
- `--advertise-plain` must use a numeric port from 1 to 65535.
- Certificates must be valid now. The control trust bundle must contain at least
  one certificate.

## Environment variables

Environment variables choose and configure the state store. With none of them
set, sipbridge keeps state in memory, which is all a single instance needs.
[State Backends](./state-backends.md) explains the trade-offs.

### Common

| Variable | Default | Description |
| --- | --- | --- |
| `SIPBRIDGE_STATE_BACKEND` | `memory` | `memory` or `redis`. Any other value fails startup. |
| `SIPBRIDGE_STATE_IDENTITY_KEY` | random per process (memory only) | Standard base64 of 32 to 256 bytes. Required for Redis, where every instance sharing a namespace must use the same key. |
| `SIPBRIDGE_STATE_NAMESPACE` | `sipbridge` | Name of the state namespace; Redis-backed instances with matching settings and the same namespace share state. It is also reported to controllers as `stateAuthority.service`. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. |
| `SIPBRIDGE_STATE_GENERATION` | `default` | A label reported to controllers as `stateAuthority.generation`. Changing it does not reset or recover state. Same pattern as the namespace. |
| `SIPBRIDGE_STATE_PREFIX` | `sipstate:v2` | Redis key prefix. Pattern `[A-Za-z0-9_.:-]{1,96}`. |
| `SIPBRIDGE_STATE_PARTITIONS` | `64` | Number of partitions Call-IDs are spread over, 1 to 4096. Must be the same on every instance in a namespace. |

### Memory only

Setting either variable with `SIPBRIDGE_STATE_BACKEND=redis` fails startup.

| Variable | Default | Description |
| --- | --- | --- |
| `SIPBRIDGE_MEMORY_MAX_GROUPS` | `128` | Maximum number of Call-IDs with state held at once, 1 to 1,048,576. |
| `SIPBRIDGE_MEMORY_MAX_BYTES` | `1073741824` (1 GiB) | Total state capacity in bytes. Each Call-ID reserves 64 MiB, so the default allows 16 at once and values below `67108864` fail startup. See [Capacity](./state-backends.md#capacity). |

### Redis only

Setting any of these without `SIPBRIDGE_STATE_BACKEND=redis` fails startup.
That is deliberate: a deployment that supplies Redis settings but forgets to
select Redis stops immediately instead of quietly running on memory.

| Variable | Description |
| --- | --- |
| `SIPBRIDGE_REDIS_ADDRESS` | Primary `host:port`. Use either this or Sentinel discovery. |
| `SIPBRIDGE_REDIS_SENTINELS` | Comma-separated Sentinel `host:port` list, at most 16. |
| `SIPBRIDGE_REDIS_MASTER` | Sentinel master name. Required with `SIPBRIDGE_REDIS_SENTINELS`. |
| `SIPBRIDGE_REDIS_USERNAME`, `SIPBRIDGE_REDIS_PASSWORD` | Data connection credentials. |
| `SIPBRIDGE_REDIS_SENTINEL_USERNAME`, `SIPBRIDGE_REDIS_SENTINEL_PASSWORD` | Sentinel discovery credentials. |
| `SIPBRIDGE_REDIS_CA_FILE` | PEM trust bundle for Redis TLS. |
| `SIPBRIDGE_REDIS_SERVER_NAME` | Verified Redis TLS server name. |
| `SIPBRIDGE_REDIS_ALLOW_PLAINTEXT` | `true` permits plaintext, only when neither CA nor server name is set. Intended for local testing. |

## Fixed values

Some limits are compiled in and cannot be changed: the number of control
sessions, heartbeat timing, control timeouts, the 4,096 limit on public TCP/TLS
connections, the 128-slot UDP routing queue, per-call state limits and the
certificate reload interval. [Limits and Timers](../reference/limits.md) lists
them all.
