// Generated from proto/sipbridge-control.proto. Do not edit.

export interface JsonField {
    kind: 'uint64' | 'uint32' | 'boolean' | 'string' | 'bytes' | 'enum' | 'message';
    values?: readonly string[];
    message?: string;
    repeated?: boolean;
    map?: boolean;
    oneof?: string;
    alias?: string;
}
export interface JsonMessage {
    fields: Record<string, JsonField>;
    oneofs: readonly string[];
}
export const SIPBRIDGE_JSON_MESSAGES: Record<string, JsonMessage> = {
    '.sipbridge.control.v2.Envelope': {
        fields: {
            correlationId: {
                kind: 'string',
                alias: 'correlation_id'
            },
            sessionId: {
                kind: 'string',
                alias: 'session_id'
            },
            remainingBudgetMs: {
                kind: 'uint32',
                alias: 'remaining_budget_ms'
            },
            hello: {
                kind: 'message',
                message: '.sipbridge.control.v2.SessionHello',
                oneof: 'payload'
            },
            helloAck: {
                kind: 'message',
                message: '.sipbridge.control.v2.SessionHelloAck',
                oneof: 'payload',
                alias: 'hello_ack'
            },
            status: {
                kind: 'message',
                message: '.sipbridge.control.v2.SessionStatus',
                oneof: 'payload'
            },
            ping: {
                kind: 'message',
                message: '.sipbridge.control.v2.Heartbeat',
                oneof: 'payload'
            },
            pong: {
                kind: 'message',
                message: '.sipbridge.control.v2.Heartbeat',
                oneof: 'payload'
            },
            ingress: {
                kind: 'message',
                message: '.sipbridge.control.v2.IngressReceived',
                oneof: 'payload'
            },
            routeResult: {
                kind: 'message',
                message: '.sipbridge.control.v2.RouteResult',
                oneof: 'payload',
                alias: 'route_result'
            },
            ingest: {
                kind: 'message',
                message: '.sipbridge.control.v2.OwnerIngest',
                oneof: 'payload'
            },
            request: {
                kind: 'message',
                message: '.sipbridge.control.v2.RequestPresented',
                oneof: 'payload'
            },
            command: {
                kind: 'message',
                message: '.sipbridge.control.v2.Command',
                oneof: 'payload'
            },
            result: {
                kind: 'message',
                message: '.sipbridge.control.v2.OperationResult',
                oneof: 'payload'
            },
            egress: {
                kind: 'message',
                message: '.sipbridge.control.v2.EgressRequest',
                oneof: 'payload'
            },
            write: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowWrite',
                oneof: 'payload'
            },
            delivery: {
                kind: 'message',
                message: '.sipbridge.control.v2.DeliveryResult',
                oneof: 'payload'
            },
            flowClosed: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowClosed',
                oneof: 'payload',
                alias: 'flow_closed'
            },
            dialogEvent: {
                kind: 'message',
                message: '.sipbridge.control.v2.DialogEvent',
                oneof: 'payload',
                alias: 'dialog_event'
            },
            inspect: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextInspect',
                oneof: 'payload'
            },
            flowAdoption: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowAdoptionRequest',
                oneof: 'payload',
                alias: 'flow_adoption'
            },
            flowAdoptionResult: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowAdoptionResult',
                oneof: 'payload',
                alias: 'flow_adoption_result'
            },
            sendPermit: {
                kind: 'message',
                message: '.sipbridge.control.v2.SendPermit',
                oneof: 'payload',
                alias: 'send_permit'
            },
            deliverEffect: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowWrite',
                oneof: 'payload',
                alias: 'deliver_effect'
            },
            credentialLookup: {
                kind: 'message',
                message: '.sipbridge.control.v2.CredentialLookup',
                oneof: 'payload',
                alias: 'credential_lookup'
            },
            credentialResult: {
                kind: 'message',
                message: '.sipbridge.control.v2.CredentialResult',
                oneof: 'payload',
                alias: 'credential_result'
            },
            inspectEffect: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowWrite',
                oneof: 'payload',
                alias: 'inspect_effect'
            },
            settleEffect: {
                kind: 'message',
                message: '.sipbridge.control.v2.EffectSettlement',
                oneof: 'payload',
                alias: 'settle_effect'
            },
            retireAuthority: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextSnapshot',
                oneof: 'payload',
                alias: 'retire_authority'
            },
            probeApplication: {
                kind: 'message',
                message: '.sipbridge.control.v2.ApplicationProbe',
                oneof: 'payload',
                alias: 'probe_application'
            },
            applicationStatus: {
                kind: 'message',
                message: '.sipbridge.control.v2.ApplicationStatus',
                oneof: 'payload',
                alias: 'application_status'
            },
            reconcileContext: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextReconcile',
                oneof: 'payload',
                alias: 'reconcile_context'
            },
            reconcileAuthority: {
                kind: 'message',
                message: '.sipbridge.control.v2.AuthorityReconcile',
                oneof: 'payload',
                alias: 'reconcile_authority'
            },
            authorityReceipt: {
                kind: 'message',
                message: '.sipbridge.control.v2.AuthorityReceipt',
                oneof: 'payload',
                alias: 'authority_receipt'
            },
            controllerContexts: {
                kind: 'message',
                message: '.sipbridge.control.v2.ControllerContexts',
                oneof: 'payload',
                alias: 'controller_contexts'
            },
            controllerContextsResult: {
                kind: 'message',
                message: '.sipbridge.control.v2.ControllerContextsResult',
                oneof: 'payload',
                alias: 'controller_contexts_result'
            }
        },
        oneofs: ['payload']
    },
    '.sipbridge.control.v2.ProcessRef': {
        fields: {
            instanceId: {
                kind: 'string',
                alias: 'instance_id'
            },
            epoch: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Bounds': {
        fields: {
            maxEnvelopeBytes: {
                kind: 'uint32',
                alias: 'max_envelope_bytes'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.SessionHello': {
        fields: {
            controller: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            connectionAttempt: {
                kind: 'uint64',
                alias: 'connection_attempt'
            },
            major: {
                kind: 'uint32'
            },
            minMinor: {
                kind: 'uint32',
                alias: 'min_minor'
            },
            maxMinor: {
                kind: 'uint32',
                alias: 'max_minor'
            },
            capabilities: {
                kind: 'string',
                repeated: true
            },
            bounds: {
                kind: 'message',
                message: '.sipbridge.control.v2.Bounds'
            },
            recovery: {
                kind: 'message',
                message: '.sipbridge.control.v2.ControllerRecovery'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ControllerRecovery': {
        fields: {
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            term: {
                kind: 'string'
            },
            token: {
                kind: 'string'
            },
            attemptId: {
                kind: 'string',
                alias: 'attempt_id'
            },
            attachmentGeneration: {
                kind: 'uint64',
                alias: 'attachment_generation'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ControllerContexts': {
        fields: {
            cursor: {
                kind: 'string'
            },
            limit: {
                kind: 'uint32'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ControllerContextsResult': {
        fields: {
            contexts: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef',
                repeated: true
            },
            cursor: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.SessionHelloAck': {
        fields: {
            sipbridge: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            sessionId: {
                kind: 'string',
                alias: 'session_id'
            },
            major: {
                kind: 'uint32'
            },
            minor: {
                kind: 'uint32'
            },
            capabilities: {
                kind: 'string',
                repeated: true
            },
            bounds: {
                kind: 'message',
                message: '.sipbridge.control.v2.Bounds'
            },
            heartbeatIntervalMs: {
                kind: 'uint32',
                alias: 'heartbeat_interval_ms'
            },
            heartbeatTimeoutMs: {
                kind: 'uint32',
                alias: 'heartbeat_timeout_ms'
            },
            stateAuthority: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateAuthority',
                alias: 'state_authority'
            },
            applicationLease: {
                kind: 'message',
                message: '.sipbridge.control.v2.ApplicationLease',
                alias: 'application_lease'
            },
            inboundAuthPolicy: {
                kind: 'message',
                message: '.sipbridge.control.v2.InboundAuthPolicy',
                alias: 'inbound_auth_policy'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ApplicationLease': {
        fields: {
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            authority: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateAuthority'
            },
            term: {
                kind: 'string'
            },
            checkedAtMs: {
                kind: 'uint64',
                alias: 'checked_at_ms'
            },
            expiresAtMs: {
                kind: 'uint64',
                alias: 'expires_at_ms'
            },
            recoveryToken: {
                kind: 'string',
                alias: 'recovery_token'
            },
            holder: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            attachmentGeneration: {
                kind: 'uint64',
                alias: 'attachment_generation'
            },
            reattachUntilMs: {
                kind: 'uint64',
                alias: 'reattach_until_ms'
            },
            leaseLifetimeMs: {
                kind: 'uint32',
                alias: 'lease_lifetime_ms'
            },
            reattachTimeoutMs: {
                kind: 'uint32',
                alias: 'reattach_timeout_ms'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.SessionStatus': {
        fields: {
            retainedReady: {
                kind: 'boolean',
                alias: 'retained_ready'
            },
            admitCalls: {
                kind: 'boolean',
                alias: 'admit_calls'
            },
            admitSubscriptions: {
                kind: 'boolean',
                alias: 'admit_subscriptions'
            },
            bindingsReady: {
                kind: 'boolean',
                alias: 'bindings_ready'
            },
            draining: {
                kind: 'boolean'
            },
            digestChallengePolicy: {
                kind: 'message',
                message: '.sipbridge.control.v2.DigestChallengePolicy',
                alias: 'digest_challenge_policy'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.InboundAuthPolicy': {
        fields: {
            requireRegister: {
                kind: 'boolean',
                alias: 'require_register'
            },
            requireInvite: {
                kind: 'boolean',
                alias: 'require_invite'
            },
            requireSubscribe: {
                kind: 'boolean',
                alias: 'require_subscribe'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.DigestChallengePolicy': {
        fields: {
            realm: {
                kind: 'string'
            },
            authenticationEpoch: {
                kind: 'string',
                alias: 'authentication_epoch'
            },
            signingKey: {
                kind: 'bytes',
                alias: 'signing_key'
            },
            expiresAtMs: {
                kind: 'uint64',
                alias: 'expires_at_ms'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Heartbeat': {
        fields: {
            nonce: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.FlowRef': {
        fields: {
            ingress: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            connectionId: {
                kind: 'string',
                alias: 'connection_id'
            },
            sourceIp: {
                kind: 'string',
                alias: 'source_ip'
            },
            sourcePort: {
                kind: 'uint32',
                alias: 'source_port'
            },
            listener: {
                kind: 'string'
            },
            transport: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.StateAuthority': {
        fields: {
            service: {
                kind: 'string'
            },
            generation: {
                kind: 'string'
            },
            primaryId: {
                kind: 'string',
                alias: 'primary_id'
            },
            schemaVersion: {
                kind: 'uint32',
                alias: 'schema_version'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.StateRef': {
        fields: {
            authority: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateAuthority'
            },
            groupId: {
                kind: 'string',
                alias: 'group_id'
            },
            incarnation: {
                kind: 'string'
            },
            callId: {
                kind: 'string',
                alias: 'call_id'
            },
            partition: {
                kind: 'uint32'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ContextRef': {
        fields: {
            owner: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            id: {
                kind: 'string'
            },
            state: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Header': {
        fields: {
            name: {
                kind: 'string'
            },
            value: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Address': {
        fields: {
            name: {
                kind: 'string'
            },
            uri: {
                kind: 'string'
            },
            params: {
                kind: 'string',
                map: true
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.AddressList': {
        fields: {
            values: {
                kind: 'message',
                message: '.sipbridge.control.v2.Address',
                repeated: true
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Via': {
        fields: {
            protocol: {
                kind: 'string'
            },
            host: {
                kind: 'string'
            },
            port: {
                kind: 'uint32'
            },
            params: {
                kind: 'string',
                map: true
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.DigestCredentials': {
        fields: {
            username: {
                kind: 'string'
            },
            realm: {
                kind: 'string'
            },
            nonce: {
                kind: 'string'
            },
            uri: {
                kind: 'string'
            },
            response: {
                kind: 'string'
            },
            algorithm: {
                kind: 'string'
            },
            qop: {
                kind: 'string'
            },
            nc: {
                kind: 'string'
            },
            cnonce: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.SipMessage': {
        fields: {
            wire: {
                kind: 'bytes'
            },
            method: {
                kind: 'string'
            },
            uri: {
                kind: 'string'
            },
            status: {
                kind: 'uint32'
            },
            reason: {
                kind: 'string'
            },
            body: {
                kind: 'bytes'
            },
            headers: {
                kind: 'message',
                message: '.sipbridge.control.v2.Header',
                repeated: true
            },
            addresses: {
                kind: 'message',
                message: '.sipbridge.control.v2.AddressList',
                map: true
            },
            vias: {
                kind: 'message',
                message: '.sipbridge.control.v2.Via',
                repeated: true
            },
            selectors: {
                kind: 'string',
                repeated: true
            },
            authorization: {
                kind: 'message',
                message: '.sipbridge.control.v2.DigestCredentials'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.IngressReceived': {
        fields: {
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            },
            message: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.RouteResult': {
        fields: {
            outcome: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'ACCEPTED', 'REDIRECT', 'REJECTED', 'UNAVAILABLE']
            },
            controller: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            sipStatus: {
                kind: 'uint32',
                alias: 'sip_status'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.OwnerIngest': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            },
            wire: {
                kind: 'bytes'
            },
            bindingId: {
                kind: 'string',
                alias: 'binding_id'
            },
            principalId: {
                kind: 'string',
                alias: 'principal_id'
            },
            authenticated: {
                kind: 'boolean'
            },
            admission: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'EXISTING_FLOW', 'AUTHENTICATED_BINDING', 'TLS_DIALOG_CONTINUITY', 'TLS_TRANSACTION_CONTINUITY']
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ContextSnapshot': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            bindingId: {
                kind: 'string',
                alias: 'binding_id'
            },
            principalId: {
                kind: 'string',
                alias: 'principal_id'
            },
            kind: {
                kind: 'enum',
                values: ['UNKNOWN', 'TRANSACTION', 'UAS', 'UAC', 'SUBSCRIPTION']
            },
            dialogId: {
                kind: 'string',
                alias: 'dialog_id'
            },
            callId: {
                kind: 'string',
                alias: 'call_id'
            },
            localTag: {
                kind: 'string',
                alias: 'local_tag'
            },
            remoteTag: {
                kind: 'string',
                alias: 'remote_tag'
            },
            localUri: {
                kind: 'string',
                alias: 'local_uri'
            },
            remoteUri: {
                kind: 'string',
                alias: 'remote_uri'
            },
            remoteTarget: {
                kind: 'string',
                alias: 'remote_target'
            },
            localSdp: {
                kind: 'bytes',
                alias: 'local_sdp'
            },
            remoteSdp: {
                kind: 'bytes',
                alias: 'remote_sdp'
            },
            answered: {
                kind: 'boolean'
            },
            initialAck: {
                kind: 'boolean',
                alias: 'initial_ack'
            },
            ended: {
                kind: 'boolean'
            },
            revision: {
                kind: 'uint64'
            },
            commandSequence: {
                kind: 'uint64',
                alias: 'command_sequence'
            },
            selectors: {
                kind: 'string',
                repeated: true
            },
            transport: {
                kind: 'string'
            },
            delivery: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextDelivery'
            },
            executor: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            applicationLeaseTerm: {
                kind: 'string',
                alias: 'application_lease_term'
            },
            attachmentGeneration: {
                kind: 'uint64',
                alias: 'attachment_generation'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.RequestPresented': {
        fields: {
            snapshot: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextSnapshot'
            },
            requestId: {
                kind: 'string',
                alias: 'request_id'
            },
            message: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ResponseAuthority': {
        fields: {
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            completionScope: {
                kind: 'string',
                alias: 'completion_scope'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Respond': {
        fields: {
            requestId: {
                kind: 'string',
                alias: 'request_id'
            },
            status: {
                kind: 'uint32'
            },
            headers: {
                kind: 'message',
                message: '.sipbridge.control.v2.Header',
                repeated: true
            },
            body: {
                kind: 'bytes'
            },
            principalId: {
                kind: 'string',
                alias: 'principal_id'
            },
            bindingId: {
                kind: 'string',
                alias: 'binding_id'
            },
            authority: {
                kind: 'message',
                message: '.sipbridge.control.v2.ResponseAuthority'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.CredentialRef': {
        fields: {
            id: {
                kind: 'string'
            },
            version: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.CredentialLookup': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            credential: {
                kind: 'message',
                message: '.sipbridge.control.v2.CredentialRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.CredentialResult': {
        fields: {
            username: {
                kind: 'string'
            },
            password: {
                kind: 'string'
            },
            version: {
                kind: 'string'
            },
            error: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.NetworkTarget': {
        fields: {
            proxy: {
                kind: 'string'
            },
            transport: {
                kind: 'string'
            },
            username: {
                kind: 'string'
            },
            password: {
                kind: 'string'
            },
            credential: {
                kind: 'message',
                message: '.sipbridge.control.v2.CredentialRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.OpenUac': {
        fields: {
            uri: {
                kind: 'string'
            },
            body: {
                kind: 'bytes'
            },
            headers: {
                kind: 'message',
                message: '.sipbridge.control.v2.Header',
                repeated: true
            },
            bindingId: {
                kind: 'string',
                alias: 'binding_id'
            },
            principalId: {
                kind: 'string',
                alias: 'principal_id'
            },
            network: {
                kind: 'message',
                message: '.sipbridge.control.v2.NetworkTarget'
            },
            transport: {
                kind: 'string'
            },
            method: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.DialogRequest': {
        fields: {
            method: {
                kind: 'string'
            },
            body: {
                kind: 'bytes'
            },
            headers: {
                kind: 'message',
                message: '.sipbridge.control.v2.Header',
                repeated: true
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Cancel': {
        fields: {},
        oneofs: []
    },
    '.sipbridge.control.v2.CommandAdmission': {
        fields: {
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            expiresAtMs: {
                kind: 'uint64',
                alias: 'expires_at_ms'
            },
            selectedFlow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef',
                alias: 'selected_flow'
            },
            expectedDelivery: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextDelivery',
                alias: 'expected_delivery'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.Command': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            sequence: {
                kind: 'uint64'
            },
            admission: {
                kind: 'message',
                message: '.sipbridge.control.v2.CommandAdmission'
            },
            respond: {
                kind: 'message',
                message: '.sipbridge.control.v2.Respond',
                oneof: 'action'
            },
            openUac: {
                kind: 'message',
                message: '.sipbridge.control.v2.OpenUac',
                oneof: 'action',
                alias: 'open_uac'
            },
            dialogRequest: {
                kind: 'message',
                message: '.sipbridge.control.v2.DialogRequest',
                oneof: 'action',
                alias: 'dialog_request'
            },
            cancel: {
                kind: 'message',
                message: '.sipbridge.control.v2.Cancel',
                oneof: 'action'
            }
        },
        oneofs: ['action']
    },
    '.sipbridge.control.v2.OperationResult': {
        fields: {
            certainty: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'NOT_ACCEPTED', 'ACCEPTED', 'COMPLETED', 'UNKNOWN']
            },
            error: {
                kind: 'string'
            },
            sipStatus: {
                kind: 'uint32',
                alias: 'sip_status'
            },
            snapshot: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextSnapshot'
            },
            request: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            response: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            retirementProof: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRetirementProof',
                alias: 'retirement_proof'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ContextRetirementProof': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            authority: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateAuthority'
            },
            reason: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'GROUP_ABSENT', 'OBSOLETE_INCARNATION', 'CONTEXT_ABSENT']
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.StateEffectRef': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            id: {
                kind: 'string'
            },
            attemptId: {
                kind: 'string',
                alias: 'attempt_id'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.EffectAuthorization': {
        fields: {
            effect: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateEffectRef'
            },
            authorityClass: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'APPLICATION', 'NATIVE_COMPLETION', 'TERMINAL_CLEANUP', 'RESPONSE'],
                alias: 'authority_class'
            },
            completionScope: {
                kind: 'string',
                alias: 'completion_scope'
            },
            fingerprint: {
                kind: 'string'
            },
            deadlineMs: {
                kind: 'uint64',
                alias: 'deadline_ms'
            },
            applicationLeaseTerm: {
                kind: 'string',
                alias: 'application_lease_term'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.SendPermit': {
        fields: {
            authorization: {
                kind: 'message',
                message: '.sipbridge.control.v2.EffectAuthorization'
            },
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            orderingProof: {
                kind: 'string',
                alias: 'ordering_proof'
            },
            completionScope: {
                kind: 'string',
                alias: 'completion_scope'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.EgressRequest': {
        fields: {
            snapshot: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextSnapshot'
            },
            message: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            returnFlow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef',
                alias: 'return_flow'
            },
            authorization: {
                kind: 'message',
                message: '.sipbridge.control.v2.EffectAuthorization'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.FlowWrite': {
        fields: {
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            },
            bindingId: {
                kind: 'string',
                alias: 'binding_id'
            },
            bindingRevision: {
                kind: 'uint64',
                alias: 'binding_revision'
            },
            wire: {
                kind: 'bytes'
            },
            effect: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateEffectRef'
            },
            permit: {
                kind: 'message',
                message: '.sipbridge.control.v2.SendPermit'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.DeliveryResult': {
        fields: {
            outcome: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'WRITTEN', 'NOT_WRITTEN', 'UNKNOWN']
            },
            error: {
                kind: 'string'
            },
            effect: {
                kind: 'message',
                message: '.sipbridge.control.v2.StateEffectRef'
            },
            recorded: {
                kind: 'boolean'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.EffectSettlement': {
        fields: {
            write: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowWrite'
            },
            delivery: {
                kind: 'message',
                message: '.sipbridge.control.v2.DeliveryResult'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ApplicationProbe': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ApplicationStatus': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            live: {
                kind: 'boolean'
            },
            checkedAtMs: {
                kind: 'uint64',
                alias: 'checked_at_ms'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.FlowClosed': {
        fields: {
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.DialogEvent': {
        fields: {
            kind: {
                kind: 'enum',
                values: ['UNSPECIFIED', 'PROVISIONAL', 'ANSWERED', 'ACKNOWLEDGED', 'CANCELLED', 'ENDED', 'FAILED', 'SUBSCRIPTION_EXPIRED']
            },
            snapshot: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextSnapshot'
            },
            message: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            operationId: {
                kind: 'string',
                alias: 'operation_id'
            },
            error: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ContextInspect': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ContextDelivery': {
        fields: {
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            },
            nativeRevision: {
                kind: 'uint64',
                alias: 'native_revision'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            registrationTicket: {
                kind: 'string',
                alias: 'registration_ticket'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.FlowAdoptionRequest': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            principalId: {
                kind: 'string',
                alias: 'principal_id'
            },
            bindingId: {
                kind: 'string',
                alias: 'binding_id'
            },
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            },
            nativeRevision: {
                kind: 'uint64',
                alias: 'native_revision'
            },
            subscription: {
                kind: 'boolean'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.FlowAdoptionResult': {
        fields: {
            receipt: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextDelivery'
            },
            error: {
                kind: 'string'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.ContextReconcile': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            fromDirectoryEpoch: {
                kind: 'string',
                alias: 'from_directory_epoch'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.AuthorityReconcile': {
        fields: {
            snapshot: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextSnapshot'
            },
            fromDirectoryEpoch: {
                kind: 'string',
                alias: 'from_directory_epoch'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            token: {
                kind: 'string'
            },
            barrierRevision: {
                kind: 'uint64',
                alias: 'barrier_revision'
            },
            origin: {
                kind: 'message',
                message: '.sipbridge.control.v2.SipMessage'
            },
            completionScope: {
                kind: 'string',
                alias: 'completion_scope'
            },
            flow: {
                kind: 'message',
                message: '.sipbridge.control.v2.FlowRef'
            }
        },
        oneofs: []
    },
    '.sipbridge.control.v2.AuthorityReceipt': {
        fields: {
            context: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextRef'
            },
            application: {
                kind: 'message',
                message: '.sipbridge.control.v2.ProcessRef'
            },
            fromDirectoryEpoch: {
                kind: 'string',
                alias: 'from_directory_epoch'
            },
            directoryEpoch: {
                kind: 'string',
                alias: 'directory_epoch'
            },
            token: {
                kind: 'string'
            },
            barrierRevision: {
                kind: 'uint64',
                alias: 'barrier_revision'
            },
            completionScope: {
                kind: 'string',
                alias: 'completion_scope'
            },
            delivery: {
                kind: 'message',
                message: '.sipbridge.control.v2.ContextDelivery'
            },
            ownerLost: {
                kind: 'boolean',
                alias: 'owner_lost'
            }
        },
        oneofs: []
    }
};
