---
layout: home
description: A SIP signaling server that holds phone and trunk connections and runs the protocol while your application decides each call over a WebSocket.
hero:
  name: sipbridge
  text: SIP signaling that your application controls
  tagline: sipbridge holds the SIP connections to phones and trunks and runs the protocol machinery. Your application connects over a WebSocket and decides how each request is answered.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: Overview
      link: /guide/overview
    - theme: alt
      text: A Call, Step by Step
      link: /guide/call-walkthrough
    - theme: alt
      text: Control Protocol
      link: /protocol/overview

features:
  - title: SIP handled for you
    details: sipbridge listens on UDP, TCP and TLS and takes care of transactions, dialogs, retransmissions, ACK and CANCEL, using a pinned build of the sipgo library. Your code never implements a SIP state machine.
  - title: Your application decides
    details: Each new request is offered to your controller once, and it answers with one decision. Commands answer, call, hang up and notify. Any language works; the protocol is strict JSON with generated Go and TypeScript types.
  - title: Clear delivery results
    details: Every send is reported as WRITTEN, NOT_WRITTEN or UNKNOWN. An uncertain send is never retried behind your back, so your application knows what it can assume.
  - title: Start in memory, share with Redis
    details: A single instance keeps its state in memory and needs nothing else. Select Redis when several instances should share calls, so a phone that reconnects elsewhere can continue.
  - title: No secrets to hold
    details: sipbridge stores no accounts or passwords. Your controller authenticates callers, keeps registrations and supplies outbound credentials only when a request is challenged.
  - title: Made to be operated
    details: Separate liveness, serving and readiness endpoints, a bounded drain on shutdown, certificate reload without a restart, Prometheus metrics and optional SIP capture.
---
