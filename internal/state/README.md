# SIP state adapters

[Architecture](../../ARCHITECTURE.md) owns component boundaries; its
[runtime supplement](../../docs/runtime.md) owns the exact state/execution contract. The store opens from configuration and treats
an empty namespace as ordinary empty state. No provisioning command or namespace
bootstrap markers are required. Each record retains its schema, incarnation,
revision, owner lease and finite SIP work bounds.

The service uses the `Backend` interface. `OpenBackend` selects memory by default
or explicit Redis. `Open`/`OpenDeferred` remain Redis constructors for existing
fixtures. Shared metadata and validation perform no I/O. Redis retains no protocol
cache; the memory adapter alone owns its in-process records, leases and indexes.

Run memory and shared contract tests without Redis:

```sh
go test -race ./internal/state -run 'TestMemory|TestBackend'
```

Redis subtests skip without a disposable endpoint. To qualify both adapters:

```sh
SIPBRIDGE_STATE_TEST_REDIS=127.0.0.1:6379 go test -race ./internal/state
```

The state user is restricted to its configured SIP key prefix. Firewall keys,
administration and other namespaces remain inaccessible. Redis failover is best
effort; missing state fails the affected operation and fresh SIP requests recover
without coordinated setup.
