# Draft: preserve INVITE response identity during cancellation

Local branch: `fix/cancel-response-identity`, based directly on upstream
`d42152179aa707034cb15d681f9d83d4b12a5417`. Proposed base: upstream main.

When a UAS has emitted 183, automatic cancellation currently generates a 487
with a different To tag. A late application final can also replace the cached
cancellation final even though the FSM has already cancelled the INVITE.

Retain the emitted identity for generated CANCEL/487 responses, serialize
cancellation with application finals, and reject late application responses
after cancellation. Preserve caller-owned/forwarded response objects and the
public Receive contract. This changes no transport routing or application policy.

Validation: focused native race tests cover early/repeated/100-only cancellation,
late CANCEL after final, response immutability, multiple forwarded 2xx tags,
cached 487 preservation and 100 concurrent final/CANCEL rounds. Full native unit
suite passes. The tests require no controller or deployment infrastructure.

Relevant protocol reference: [RFC 3261 §8.2.6.2](https://www.rfc-editor.org/rfc/rfc3261.html#section-8.2.6.2)
and [§9.2](https://www.rfc-editor.org/rfc/rfc3261.html#section-9.2).

The separate original-request identity guard is intentionally absent from this PR.
This is a local draft; no upstream publication has occurred.
