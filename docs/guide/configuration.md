# Configuration

`cmd/sipbridge` takes command-line flags for listeners, identities and SIP
policy, and environment variables for state. There is no configuration file, and
flags have no environment-variable equivalents. All settings are fixed for the
life of the process. Only certificate and key files are reloaded while running
(see [Identities and TLS](./security.md#credential-reload)).

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
| `--instance` | none, **required** | Stable inventory ID of this process, for example a StatefulSet ordinal name. Must match `[A-Za-z0-9_.:@+-]{1,96}`. A random epoch is generated on every start. |
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

Every worker that might take over another worker's contexts must use the same
public advertisement. Advertisement never changes an existing context's transport
or flow.

### Routing and SIP policy

| Flag | Default | Description |
| --- | --- | --- |
| `--initial-nexus` | empty | Instance ID of a preferred first-hop controller for new requests. It is used only while that controller is ready; controller redirects still apply. Without it, new requests rotate across ready controllers. Existing contexts prefer their original controller. |
| `--allow-register` | `true` | `false` rejects every inbound REGISTER locally with `405`, before state or controller access. |
| `--allow-subscribe` | `true` | `false` rejects every inbound SUBSCRIBE locally with `405`, before state or controller access. |
| `--require-auth-methods` | empty | Comma-separated subset of `REGISTER`, `INVITE`, `SUBSCRIBE` (uppercase) that receive a local initial digest challenge when they carry no parseable credentials. Requires controller support; see [Local challenges](./application.md#local-initial-digest-challenges). |
| `--sip-capture-url` | empty | Optional edge-local SIP capture collector. See [Observability](./observability.md#sip-capture). |
| `--version` | | Print `sipbridge <version>` and exit. |

Method policy is fixed for the process lifetime. A required-auth method that is
also disabled, an unknown method or a duplicate fails startup. The `405`
response's `Allow` header lists the methods this process still accepts.

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

Environment variables select and configure the SIP state store. Details and
trade-offs are on [State Backends](./state-backends.md).

### Common

| Variable | Default | Description |
| --- | --- | --- |
| `SIPBRIDGE_STATE_BACKEND` | `memory` | `memory` or `redis`. Any other value fails startup. |
| `SIPBRIDGE_STATE_IDENTITY_KEY` | random per process (memory only) | Standard base64 of 32 to 256 bytes. Required for Redis and shared by every worker in a namespace. |
| `SIPBRIDGE_STATE_NAMESPACE` | `sipbridge` | State namespace and authority service name. Pattern `[A-Za-z0-9_.:@+-]{1,96}`. |
| `SIPBRIDGE_STATE_GENERATION` | `default` | Compatibility identity of the configuration. Not a recovery step. Same pattern as the namespace. |
| `SIPBRIDGE_STATE_PREFIX` | `sipstate:v2` | Redis key prefix. Pattern `[A-Za-z0-9_.:-]{1,96}`. |
| `SIPBRIDGE_STATE_PARTITIONS` | `64` | Call-ID partition count, 1 to 4096. Must match across workers in a namespace. |

### Memory only

Setting either variable with `SIPBRIDGE_STATE_BACKEND=redis` fails startup.

| Variable | Default | Description |
| --- | --- | --- |
| `SIPBRIDGE_MEMORY_MAX_GROUPS` | `128` | Maximum retained Call-ID groups, 1 to 1,048,576. |
| `SIPBRIDGE_MEMORY_MAX_BYTES` | `1073741824` (1 GiB) | Total reserved group capacity in bytes. Each group reserves 64 MiB, so values below `67108864` fail startup. |

### Redis only

Any of these set without `SIPBRIDGE_STATE_BACKEND=redis` fails startup, which
catches a deployment that forgot to select Redis.

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

The following are not configurable in the service binary: control session and
claim limits, heartbeat timing, control write and hello timeouts, the 4,096
public stream and flow limit, the 128-slot UDP routing queue, native state
bounds and certificate reload intervals. See [Limits and Timers](../reference/limits.md).
