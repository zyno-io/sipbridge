---
layout: home
hero:
  name: sipbridge
  text: SIP signaling that your application controls
  tagline: sipbridge holds the SIP connections to phones and trunks and runs the protocol machinery. Your application connects over a WebSocket and decides how each request is answered.
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
    - theme: alt
      text: Project Status
      link: /reference/status

features:
  - title: SIP handled for you
    details: sipbridge listens on UDP, TCP and TLS and takes care of transactions, dialogs, retransmissions and timers, using a pinned build of the sipgo library. Your code never implements a SIP state machine.
  - title: Your application decides
    details: A controller you write connects over a mutually authenticated WebSocket and exchanges JSON messages. It authenticates accounts, routes requests and chooses responses. sipbridge stores no accounts or passwords.
  - title: Connections outlive your deploys
    details: Phones and trunks connect to sipbridge, not to your application. Restarting or redeploying the controller leaves those connections open, although a call in progress ends if the controller process that owns it goes away.
  - title: Start in memory, share with Redis
    details: A single instance keeps its state in memory and needs no external datastore; that state is lost when it restarts. Select Redis when several instances need to share state.
  - title: Clear delivery results
    details: Every send is reported as WRITTEN, NOT_WRITTEN or UNKNOWN. An uncertain send is never retried behind your back, so your application knows what it can assume.
  - title: Made to be operated
    details: Separate liveness, serving and readiness endpoints, certificate reload without a restart, Prometheus counters and optional SIP capture.
---
