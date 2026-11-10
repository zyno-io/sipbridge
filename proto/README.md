# Control protocol sources

This directory owns the sipbridge control protocol. `sipbridge-control.proto`
defines the fields and message types; `PROTOCOL.md` and `docs/runtime.md` define
what they mean. The JSON cases exercise both Go and TypeScript validation, and
the digest fixture is shared with native engine tests.

The current public protocol uses `sipbridge.control.v1` and WebSocket
subprotocol `sipbridge.control.json.v1`. `sip.controller-reattach.v1` names the
first revision of the optional controller recovery feature; capability revisions
are independent of the protocol's major version.

## Generate and check

Use Node 24.15 or later, protoc 3.21.12 and protoc-gen-go 1.36.12. The npm
lockfile pins ts-proto 2.18.3 and the descriptor/formatting dependencies.

```sh
npm ci --prefix tools/protocol
node scripts/generate-control.mjs
python3 scripts/verify_control_schema.py --regenerate
npm test --prefix tools/protocol
```

`SIPBRIDGE_PROTOC` and `SIPBRIDGE_PROTOC_GEN_GO` can select the pinned compiler
executables when they are not on `PATH`.

The generated distribution consists of:

- `dependencies/sipbridge-control.proto`, an exact copy of the canonical schema;
- `internal/controlpb/sipbridge-control.pb.go`, the Go bindings;
- `generated/typescript/sipbridge-control.ts`, the TypeScript JSON bindings;
- `generated/typescript/sipbridge-json.ts`, descriptor-derived validation metadata;
- `generated/typescript/json-validation.ts`, the strict JSON parser/validator;
- the copied Go test fixtures and `dependencies/control-schema.json`, which
  records source/output checksums and the public protocol version.

Consumers pin the receipt and copy the generated distribution or regenerate
from this schema with the pinned tools. Generation and verification need no
other source checkout. The TypeScript artifacts are protocol building blocks,
not a controller SDK; applications still implement connection lifecycle, frame
bounds, ownership and request handling.
