# sipbridge contributor instructions

Read ARCHITECTURE.md before non-trivial changes. It owns component boundaries
and invariants; its normative supplements own exact details: docs/runtime.md
(state, execution, bounds and lifecycle), PROTOCOL.md (wire/business admission),
and docs/fixtures.md (fixture behavior and limits). Read the relevant supplement
and update the owning document first when changing behavior. Keep implemented
service behavior distinct from proposed work and fixture/historical evidence.

Keep SIP transaction, dialog and timer semantics in the pinned sipgo reducers.
Follow ARCHITECTURE.md for native state/runtime and routing/control ownership.
Redis-enabled execution must not retain a second resident authoritative FSM.
Preserve the pinned consolidated dependency and rerun the native branch tests
and wire qualification when it changes.

The public source, protocol and documentation home is GitHub `zyno-io/sipbridge`.
`scripts/publish-docs.sh` publishes reviewed current-source snapshots without
private Git history. GitHub Pages builds the documentation from that public tree.
Publication does not deploy the service.
Keep the pinned `third_party/sipgo` snapshot immutable;
test generated native fixtures in a disposable copy. Do not publish, push or deploy
outside the user's authorized scope. Every new/amended commit must include once, separated
by a blank line: Co-authored-by: Codex <codex@openai.com>.

Use gofmt and the narrowest meaningful Go and wire checks. Do not claim production
qualification from fixture results. Never add real credentials to tests/artifacts.
