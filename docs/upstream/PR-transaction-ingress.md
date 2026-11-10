# Draft: expose parsed ingress for application-owned transports

Local branch: `feat/transaction-ingress`, based on the consolidated cancellation
fixes. Transport proposal revision: `db8f7e537e4b01cc70bc2624381ff5c7bfa0bace`.
The current consumed consolidation is recorded in `dependencies/sipgo.json`.
The transport API addition can be rebased independently onto upstream main.

Expose `TransactionLayer.HandleMessage(Message)` as the supported entry point for
an application-owned transport. The caller supplies already parsed SIP with its
observed source/transport and first installs a connection in the matching native
transport pool. Native transaction matching, CANCEL, retransmission and retirement
remain unchanged. This avoids forcing datagrams through a virtual reliable stream
or depending on private transport-reader ordering.

The regression covers UDP, TCP and TLS, verifies native reliability/timer selection,
and proves response delivery uses the bound connection. Full native unit tests and
focused cancellation/ingress race tests passed locally in Docker. This is an API
extension proposal, separate from the cancellation correctness and hardening PRs;
no upstream PR has been submitted.

The opt-in connection-creation guard makes a missing pool entry fail closed
for application-owned transports, including retirement races. Its default
leaves existing native connection creation unchanged. Both client and server
pool misses are covered for UDP, TCP and TLS.
