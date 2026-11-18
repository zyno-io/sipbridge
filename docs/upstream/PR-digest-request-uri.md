# Draft: preserve the complete digest request URI

Local branch: `fix/digest-request-uri`, based directly on upstream
`d42152179aa707034cb15d681f9d83d4b12a5417`. Independent fix:
`a03ed607f3faaed1bc17a2b44daa6b536f6d9bc6`.

Dialog `WaitAnswer` and standalone `TransactionDigestAuth` use `Uri.Addr()` for
the digest URI. That omits URI parameters and query headers present in the
serialized request line. A carrier validating `sip:...;transport=udp` therefore
rejects the otherwise correct retry. Use `Uri.String()` at those three sites.

The regression independently checks the credential URI and computes the MD5
response against the actual request line. All four combinations (401/407,
dialog/standalone) fail on the upstream base and pass with this patch. Focused
race tests and the full native suite pass in local Docker. The backend's real
carrier fixture passes both 401 and 407 challenges, ACK ordering, independently
verified credentials, decoded media and BYE completion in local Docker.

This fix is independent of the cancellation and transport API patches. It is
included in the pinned consolidated dependency; no upstream PR has been submitted.
