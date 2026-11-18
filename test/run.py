"""Qualify sipbridge against its pinned consolidated native dependency."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault('PROOF_GO_BINARY', str(ROOT / 'bin/sipbridge-fixture'))
os.environ.setdefault('PROOF_GO_EVIDENCE_DIR', str(ROOT / 'artifacts/qualification'))
os.environ['PROOF_TRACK_TARGET'] = '1'
os.environ['PROOF_SUBSCRIPTIONS'] = '1'
os.environ['SIPBRIDGE_FIXTURE_STDIO'] = '1'
import scenarios as compare

wire = compare.reconnect
LIBRARY = ROOT / compare.PIN['source_directory']
os.environ['PROOF_LIBRARY_ROOT'] = str(LIBRARY)


async def notify(client, expected_seq=None, state='active', target=None, event=None, response_code=200):
    request = await client.recv('NOTIFY')
    assert wire.field(request, 'subscription-state').startswith(state), request
    if state == 'active':
        assert int(wire.field(request, 'subscription-state').split('expires=')[1].split(';')[0]) > 0, request
    if expected_seq is not None:
        assert wire.field(request, 'cseq') == f'{expected_seq} NOTIFY', request
    if target is not None:
        assert request.split(' ', 2)[1].lower() == target.lower(), request
    if event is not None:
        assert wire.field(request, 'event') == event, request
    await client.send(wire.response(request, response_code))
    return request


async def subscription(h):
    a = await h.connect('A')
    await a.register()
    await a.send(wire.request('SUBSCRIBE', 'subscription-case', extra=['Event: message-summary', 'Expires: 60']))
    subscribed = await a.recv('SUBSCRIBE', 200)
    await notify(a, 1)
    b = await h.connect('B')
    await b.register()
    identifier = await h.command('subscription-case', 'notify')
    await notify(b, 2)
    await h.done(identifier)
    target = 'sip:alice@accepted-refresh.invalid:5072;transport=tls'
    refresh = wire.request('SUBSCRIBE', 'subscription-case', seq=2, tag=wire.local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 60'], contact=target)
    await b.send(refresh)
    accepted = await b.recv('SUBSCRIBE', 200)
    assert wire.local_tag(accepted) == wire.local_tag(subscribed)
    await notify(b, 3, target=target)
    # Exact replay is a response-only outcome; it cannot reset expiry or notify.
    await b.send(refresh)
    await b.recv('SUBSCRIBE', 200)
    await b.quiet(0.15)
    await b.send(wire.request('SUBSCRIBE', 'subscription-case', seq=3, tag=wire.local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 0', 'X-Proof-Reject: yes'],
        contact='sip:alice@rejected-refresh.invalid:5088;transport=tls'))
    await b.recv('SUBSCRIBE', 403)
    await b.quiet(0.1)
    identifier = await h.command('subscription-case', 'notify')
    await notify(b, 4, target=target)
    await h.done(identifier)
    await b.send(wire.request('SUBSCRIBE', 'subscription-case', seq=2, tag=wire.local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 60'], branch='z9hG4bK-stale-refresh', contact=target))
    await b.recv('SUBSCRIBE', 500)
    await b.send(wire.request('SUBSCRIBE', 'subscription-case', seq=4, tag=wire.local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 0'], contact=target))
    unsubscribed = await b.recv('SUBSCRIBE', 200)
    assert wire.field(unsubscribed, 'expires') == '0'
    await notify(b, 5, state='terminated;reason=deactivated', target=target)
    await h.wait(lambda r: r['event'] == 'delivered')
    await asyncio.sleep(0.05)
    assert (await h.stats())['subscriptions'] == 0
    identifier = await h.command('subscription-case', 'notify')
    await h.wait(lambda r: r['event'] == 'command_error' and r['id'] == identifier)
    await b.quiet()
    await a.quiet()
    return {'register_only_notify_delivery': 'B', 'accepted_contact_committed': target,
            'rejected_zero_expiry_preserved_subscription': True, 'duplicate_refresh_has_no_side_effect': True,
            'stale_cseq_rejected': True, 'unsubscribe_final_notify_and_cleanup': True, 'local_notify_cseq': [1, 2, 3, 4, 5]}


async def subscription_expiry(h):
    a = await h.connect('A')
    await a.register()
    call = 'subscription-expiry-case'
    await a.send(wire.request('SUBSCRIBE', call, extra=['Event: message-summary', 'Expires: 1']))
    subscribed = await a.recv('SUBSCRIBE', 200)
    await notify(a, 1)
    b = await h.connect('B')
    await b.register()
    await asyncio.sleep(0.5)
    await b.send(wire.request('SUBSCRIBE', call, seq=2, tag=wire.local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 2']))
    await b.recv('SUBSCRIBE', 200)
    await notify(b, 2)
    await b.quiet(0.7)
    assert (await h.stats())['subscriptions'] == 1, 'old expiry ended renewed subscription'
    await notify(b, 3, state='terminated;reason=timeout')
    await asyncio.sleep(0.05)
    assert (await h.stats())['subscriptions'] == 0
    await b.quiet(0.15)
    await a.quiet(0.1)
    return {'old_expiry_did_not_end_refresh': True, 'final_timeout_notify': 'B', 'subscriptions_after_expiry': 0}


async def subscription_event_isolation(h):
    a = await h.connect('A')
    await a.register()
    call = 'event-isolation-case'
    subscriptions = []
    for name in ('one', 'two'):
        event = f'dialog;id={name}'
        await a.send(wire.request('SUBSCRIBE', call, seq=17, branch=f'z9hG4bK-event-{name}',
            extra=[f'Event: {event}', 'Expires: 60']))
        accepted = await a.recv('SUBSCRIBE', 200)
        request = await notify(a, 1, event=event)
        assert wire.field(request, 'from').endswith(';tag=' + wire.local_tag(accepted))
        subscriptions.append((event, wire.local_tag(accepted)))
    assert subscriptions[0][1] != subscriptions[1][1]
    await a.send(wire.request('SUBSCRIBE', call, seq=18, tag=subscriptions[0][1],
        extra=['Event: dialog;id=two', 'Expires: 0']))
    await a.recv('SUBSCRIBE', 481)
    assert (await h.stats())['subscriptions'] == 2
    b = await h.connect('B')
    await b.register()
    for index, (event, tag) in enumerate(subscriptions):
        identifier = f'isolated-notify-{index}'
        await h.send(h.owner, {'op': 'command', 'id': identifier, 'call_id': call, 'action': 'notify',
            'subscription_event': f'dialog;id={"one" if index == 0 else "two"}'})
        await notify(b, 2, event=event, response_code=481 if index == 0 else 200)
        if index == 0:
            await h.wait(lambda r: r['event'] == 'command_error' and r['id'] == identifier)
        else:
            await h.done(identifier)
        assert (await h.stats())['subscriptions'] == 1
    await b.send(wire.request('SUBSCRIBE', call, seq=18, tag=subscriptions[1][1],
        extra=['Event: dialog;id=two', 'Expires: 0']))
    await b.recv('SUBSCRIBE', 200)
    await notify(b, 3, event='dialog;id=two', state='terminated')
    await asyncio.sleep(0.05)
    assert (await h.stats())['subscriptions'] == 0
    await a.quiet()
    return {'event_id_and_dialog_tuple_isolated': True, 'remote_initial_cseq': 17,
            'notify_cseq_independent': [1, 2, 3], 'notify_481_removed_only_its_subscription': True}


async def late_cancel(h):
    a = await h.connect('A')
    await a.register()
    call = 'late-cancel-case'
    branch = 'z9hG4bK-late-cancel'
    await a.send(wire.request('INVITE', call, branch=branch, extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(wire.request('ACK', call, tag=wire.local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    b = await h.connect('B')
    await b.register()
    cancel = wire.request('CANCEL', call, branch=branch)
    for _ in range(2):
        await b.send(cancel)
        reply = await b.recv('CANCEL', 200)
        assert wire.local_tag(reply) == wire.local_tag(accepted)
    assert (await h.stats())['dialogs'] == 1
    await b.quiet(0.15)
    identifier = await h.command(call, 'bye')
    bye = await b.recv('BYE')
    await b.send(wire.response(bye))
    await h.done(identifier)
    return {'late_cancel_200_preserved_tag': True, 'accepted_dialog_survived': True, 'invite_487_after_answer': False}


async def cancel_context_collision(h):
    a = await h.connect('A')
    await a.register()
    b = await h.connect('B')
    await b.register(device='bob')
    calls = ((a, 'alice', 'guard-alice', 'z9hG4bK-guard-a'), (b, 'bob', 'guard-bob', 'z9hG4bK-guard-b'))
    for client, device, call, branch in calls:
        await client.send(wire.request('INVITE', call, device=device, branch=branch))
        await client.recv('INVITE', 183)
    await b.send(wire.request('CANCEL', 'guard-bob', device='bob', branch='z9hG4bK-guard-a'))
    await b.recv('CANCEL', 481)
    assert (await h.stats())['dialogs'] == 2
    await a.quiet(0.15)
    for client, device, call, branch in calls:
        await client.send(wire.request('CANCEL', call, device=device, branch=branch))
        ok = await client.recv('CANCEL', 200)
        final = await client.recv('INVITE', 487)
        assert wire.local_tag(ok) == wire.local_tag(final)
        await client.send(wire.request('ACK', call, device=device, branch=branch, tag=wire.local_tag(final)))
    return {'borrowed_branch_rejected': 481, 'unrelated_calls_preserved': 2, 'valid_cancellation_still_works': True}


async def subscription_routes_and_policy(h):
    a = await h.connect('A')
    await a.register()
    for index, extra, expected in (
        (0, ['Event: presence', 'Expires: 60'], 489),
        (1, ['Event: message-summary', 'Expires: -1'], 400),
        (2, ['Event: message-summary', 'Expires: 60', 'X-Proof-Reject: yes'], 403),
        (3, ['Event: message-summary', 'Expires: 60', 'Record-Route: <sip:strict.example.test>'], 400)
    ):
        await a.send(wire.request('SUBSCRIBE', f'invalid-sub-{index}', extra=extra))
        await a.recv('SUBSCRIBE', expected)
    assert (await h.stats())['subscriptions'] == 0
    await a.quiet(0.1)
    routes = ['<sip:route-one.example.test;transport=tls;lr>', '<sip:route-two.example.test;transport=tls;lr>']
    await a.send(wire.request('SUBSCRIBE', 'route-sub', extra=['Event: message-summary', 'Expires: 99999',
        *(f'Record-Route: {route}' for route in routes)]))
    accepted = await a.recv('SUBSCRIBE', 200)
    assert wire.field(accepted, 'expires') == '300'
    request = await notify(a, 1)
    assert wire.headers(request)['route'] == routes, request
    b = await h.connect('B')
    await b.register()
    await b.send(wire.request('SUBSCRIBE', 'route-sub', seq=2, tag=wire.local_tag(accepted),
        extra=['Event: message-summary', 'Expires: 60', 'Record-Route: <sip:replacement.example.test;lr>']))
    await b.recv('SUBSCRIBE', 200)
    request = await notify(b, 2)
    assert wire.headers(request)['route'] == routes, 'refresh cannot replace established route set'
    await b.send(wire.request('SUBSCRIBE', 'route-sub', seq=3, tag=wire.local_tag(accepted),
        extra=['Event: message-summary', 'Expires: 0']))
    await b.recv('SUBSCRIBE', 200)
    request = await notify(b, 3, state='terminated')
    assert wire.headers(request)['route'] == routes
    await asyncio.sleep(0.05)
    assert (await h.stats())['subscriptions'] == 0
    return {'unsupported_event': 489, 'invalid_expiry': 400, 'policy_rejection': 403,
            'strict_route_refused': 400, 'expiry_cap': 300, 'loose_route_headers_retained': routes,
            'network_route_traversal_tested': False}


async def subscription_concurrent_stop(h):
    a = await h.connect('A')
    await a.register()
    call = 'subscription-concurrent-stop'
    await a.send(wire.request('SUBSCRIBE', call, extra=['Event: message-summary', 'Expires: 60']))
    accepted = await a.recv('SUBSCRIBE', 200)
    await notify(a, 1)
    b = await h.connect('B')
    await b.register()
    stop = wire.request('SUBSCRIBE', call, seq=2, tag=wire.local_tag(accepted), extra=['Event: message-summary', 'Expires: 0'])
    refresh = wire.request('SUBSCRIBE', call, seq=3, tag=wire.local_tag(accepted), extra=['Event: message-summary', 'Expires: 60'])
    await b.send(stop + refresh)
    statuses = {}
    observed = []
    async def collect():
        while len(statuses) < 2:
            message = await b.recv()
            if message.startswith('NOTIFY '):
                observed.append((int(wire.field(message, 'cseq').split()[0]), wire.field(message, 'subscription-state')))
                await b.send(wire.response(message))
            elif wire.field(message, 'cseq').endswith('SUBSCRIBE'):
                statuses[int(wire.field(message, 'cseq').split()[0])] = int(message.split()[1])
    await asyncio.wait_for(collect(), 3)
    if statuses[2] == 200:
        assert statuses[3] == 481, statuses
        if not any(state.startswith('terminated') for _, state in observed):
            request = await notify(b, state='terminated')
            observed.append((int(wire.field(request, 'cseq').split()[0]), wire.field(request, 'subscription-state')))
    else:
        assert statuses == {2: 500, 3: 200}, statuses
        # Complete any active refresh notification before sending a final stop.
        if not observed:
            request = await notify(b)
            observed.append((int(wire.field(request, 'cseq').split()[0]), wire.field(request, 'subscription-state')))
        await b.send(wire.request('SUBSCRIBE', call, seq=4, tag=wire.local_tag(accepted), extra=['Event: message-summary', 'Expires: 0']))
        await b.recv('SUBSCRIBE', 200)
        request = await notify(b, state='terminated')
        observed.append((int(wire.field(request, 'cseq').split()[0]), wire.field(request, 'subscription-state')))
    assert [seq for seq, _ in observed] == sorted({seq for seq, _ in observed})
    assert observed[-1][1].startswith('terminated')
    await asyncio.sleep(0.05)
    assert (await h.stats())['subscriptions'] == 0
    await b.quiet(0.2)
    return {'concurrent_statuses': statuses, 'serialized_notify_states': observed, 'subscription_resurrected': False}


async def main():
    subprocess.run([sys.executable, str(ROOT / 'scripts/verify_dependency.py')], check=True)
    extras = [
        ('ingress-write-error', lambda h: compare.write_failure(h, True), False),
        ('closed-current-flow', lambda h: compare.write_failure(h, False), False),
        ('delivery-acknowledgment-loss', compare.lost_acknowledgment, False),
        ('same-source-port', compare.same_source_port, False),
        ('rejected-target-refresh', compare.rejected_refresh, False),
        ('late-cancel-after-answer', late_cancel, False),
        ('subscription-expiry', subscription_expiry, False),
        ('subscription-event-isolation', subscription_event_isolation, False),
        ('cancel-context-collision', cancel_context_collision, False),
        ('subscription-routes-and-policy', subscription_routes_and_policy, False),
        ('subscription-concurrent-stop', subscription_concurrent_stop, False)
    ]
    result = await wire.main(overrides={'subscription': subscription}, extra_cases=extras)
    path = wire.EVIDENCE / 'reconnect-evidence.json'
    evidence = json.loads(path.read_text())
    patch = (ROOT / compare.PIN['patch']).read_bytes()
    evidence['library_patch_sha256'] = hashlib.sha256(patch).hexdigest()
    evidence['profile'] = 'sipbridge-consolidated'
    evidence['dependency_pin'] = compare.PIN
    evidence['source_sha256'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (ROOT / 'cmd/sipbridge-fixture/main.go', ROOT / 'cmd/sipbridge-fixture/subscriptions.go', ROOT / 'go.mod', ROOT / 'dependencies/sipgo.json')}
    evidence['library_source_sha256'] = {str(p.relative_to(LIBRARY)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (LIBRARY / 'sip/transaction_server_tx.go', LIBRARY / 'sip/transaction_server_tx_fsm.go',
            LIBRARY / 'sip/transaction_layer.go', LIBRARY / 'sip/transaction_cancel_identity_test.go', LIBRARY / 'sip/transaction_cancel_context_test.go')}
    path.write_text(json.dumps(evidence, indent=2) + '\n')
    return result


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
