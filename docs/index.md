---
layout: home
hero:
  name: sipbridge
  text: SIP data plane with external application control
  tagline: Experimental Go service that terminates SIP over UDP, TCP and TLS, executes native SIP transactions and dialogs, and leaves policy and call decisions to an application controller connected over mutual TLS.
  actions:
    - theme: brand
      text: Overview
      link: /guide/overview
    - theme: alt
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: Control Protocol
      link: /protocol/overview

features:
  - title: Native SIP execution
    details: Transactions, dialogs and timers run in a pinned sipgo snapshot as pure reducers. Every output, including retransmissions, ACK, CANCEL and cached finals, is a committed effect.
  - title: External application control
    details: A controller connects to a private JSON WebSocket with mutual TLS and an exact URI SAN allowlist. It routes, authorizes and answers; sipbridge holds no business database or account credentials.
  - title: Memory or Redis state
    details: Process-local memory state is the default for one standalone instance. Redis is an explicit choice for workers that share retained state. There is no automatic fallback or migration.
  - title: Explicit delivery outcomes
    details: Socket writes are recorded as WRITTEN, NOT_WRITTEN or UNKNOWN. Uncertain writes are never retried automatically or sent over an older flow.
  - title: Separate health signals
    details: Liveness, listener serving and signaling readiness are distinct endpoints. Losing Redis or the controller fences new work without forcing a SIP-process restart.
  - title: Stated evidence boundaries
    details: The service is experimental. Fixture and local test results are kept separate from production qualification, which remains gated.
---
