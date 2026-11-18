# Draft for discussion: validate CANCEL's original request identity

Local branch: `fix/cancel-context-identity`, independently based on upstream
`d42152179aa707034cb15d681f9d83d4b12a5417`. Proposed base: upstream main.

After native branch lookup, verify original Call-ID, numeric CSeq and From/To
tags before automatically cancelling the retained request. On mismatch, use the
existing unmatched-CANCEL application handler. Observed source/rport may change,
so reconnect is not an identity mismatch.

This is additional hardening. [RFC 3261 §17.2.3](https://www.rfc-editor.org/rfc/rfc3261.html#section-17.2.3)
defines branch/sent-by/method matching; this proposal adds context checks rather
than claiming that all branch-only matching violates that section. Maintainer
agreement on policy and interoperability is required before upstreaming it.
It does not replace application authorization.

Validation: independent matcher tests plus a native transaction-layer regression
show a borrowed branch follows the unmatched handler, leaves the original
INVITE untouched, and still permits valid cancellation after source changes.
Focused race tests and the full native unit suite pass.

Keep separate from the cancellation-response correctness PR. If characterized as
a vulnerability when publishing, follow upstream SECURITY.md's reporting route.
This file is a local discussion draft; nothing has been reported or published.
