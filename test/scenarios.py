"""Reusable wire scenarios for sipbridge's consolidated native dependency."""
import asyncio
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SHARED = ROOT
PIN = json.loads((ROOT / 'dependencies/sipgo.json').read_text())
os.environ.update({
    'PROOF_OWNER_BINARY': os.environ.get('PROOF_GO_BINARY', str(ROOT / 'bin/sipbridge-fixture')),
    'PROOF_EVIDENCE_DIR': os.environ.get('PROOF_GO_EVIDENCE_DIR', str(ROOT / 'artifacts/qualification')),
    'PROOF_LIBRARY_ROOT': str(ROOT / PIN['source_directory']),
    'PROOF_LIBRARY_REVISION': PIN['consolidated_commit']
})
sys.path.insert(0, str(SHARED / 'test'))
import reconnect


async def write_failure(harness, delayed_close):
    a = await harness.connect('A')
    await a.register()
    call = 'write-failure-case'
    await a.send(reconnect.request('INVITE', call, extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(reconnect.request('ACK', call, tag=reconnect.local_tag(accepted)))
    await harness.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    current = a
    if not delayed_close:
        current = await harness.connect('B')
        await current.register()
    harness.hold_closed = delayed_close
    current.writer.close()
    await current.writer.wait_closed()
    await harness.wait(lambda r: r['event'] == 'closed' and r['flow']['remote'] == current.remote)
    start = len(harness.records)
    identifier = await harness.command(call, 'info')
    failure = await harness.wait(lambda r: r['event'] == 'command_error' and r['id'] == identifier)
    assert failure.get('transport_error') is True, failure
    if delayed_close:
        assert 'TLS ingress write:' in failure['error'], failure
        await harness.wait(lambda r: r['event'] == 'delivery_failed', start=start)
    else:
        assert 'current TLS flow is closed' in failure['error'], failure
        assert not [r for r in harness.records[start:] if r['event'] == 'egress'], 'closed binding fell back to another flow'
        await a.quiet()
    return {'native_transport_error': failure['error'], 'close_notification_delayed': delayed_close,
            'fallback_to_old_flow': False}


async def lost_acknowledgment(harness):
    a = await harness.connect('A')
    await a.register()
    call = 'acknowledgment-loss-case'
    await a.send(reconnect.request('INVITE', call, extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(reconnect.request('ACK', call, tag=reconnect.local_tag(accepted)))
    await harness.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    harness.drop_delivery_ack = True
    identifier = await harness.command(call, 'info')
    info = await a.recv('INFO')
    assert info.startswith('INFO ')
    failure = await harness.wait(lambda r: r['event'] == 'command_error' and r['id'] == identifier, timeout=3)
    assert failure.get('transport_error') is True and 'delivery acknowledgment deadline' in failure['error'], failure
    await a.quiet()
    return {'packet_reached_client': True, 'native_error_reports_uncertain_delivery': failure['error'],
            'automatic_retry': False}


async def rejected_refresh(harness):
    a = await harness.connect('A')
    await a.register()
    call = 'target-refresh-case'
    await a.send(reconnect.request('INVITE', call, extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(reconnect.request('ACK', call, tag=reconnect.local_tag(accepted)))
    await harness.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    b = await harness.connect('B')
    await b.register()
    target = 'sip:alice@accepted-target.invalid:5099;transport=tls'
    identifier = await harness.command(call, 'reinvite')
    refresh = await b.recv('INVITE')
    await b.send(reconnect.response(refresh, 200, [f'Contact: <{target}>']))
    ack = await b.recv('ACK')
    assert ack.split(' ', 2)[1].lower() == target
    await harness.done(identifier)
    identifier = await harness.command(call, 'reinvite')
    rejected = await b.recv('INVITE')
    assert rejected.split(' ', 2)[1].lower() == target
    await b.send(reconnect.response(rejected, 403, ['Contact: <sip:alice@rejected-target.invalid:5061;transport=tls>']))
    ack = await b.recv('ACK')
    assert ack.split(' ', 2)[1].lower() == target
    assert reconnect.field(ack, 'cseq') == reconnect.field(rejected, 'cseq').replace('INVITE', 'ACK')
    completed = await harness.wait(lambda r: r['event'] == 'command_done' and r['id'] == identifier)
    assert completed['result'] == 'Ok(Some(403))'
    identifier = await harness.command(call, 'bye')
    bye = await b.recv('BYE')
    assert bye.split(' ', 2)[1].lower() == target
    await b.send(reconnect.response(bye))
    await harness.done(identifier)
    await a.quiet()
    return {'accepted_target_committed': target, 'rejection_preserved_target': True,
            'non_2xx_ack': 'native transaction', 'library_source_patches': 0}


async def same_source_port(harness):
    a = await harness.connect('A', source_port=0)
    await a.register()
    b = await harness.connect('B', source_port=int(a.remote.split(':')[1]))
    await b.register(device='bob')
    assert a.remote == b.remote
    for client, device in ((a, 'alice'), (b, 'bob')):
        call = f'collision-{device}'
        await client.send(reconnect.request('INVITE', call, device=device, extra=['X-Proof-Answer: yes']))
        accepted = await client.recv('INVITE', 200)
        await client.send(reconnect.request('ACK', call, device=device, tag=reconnect.local_tag(accepted)))
        await harness.wait(lambda r: r['event'] == 'dialog_state' and r['state'] == f'Confirmed({call})')
    for client, device in ((a, 'alice'), (b, 'bob')):
        identifier = await harness.command(f'collision-{device}', 'bye')
        bye = await client.recv('BYE')
        assert reconnect.field(bye, 'call-id') == f'collision-{device}'
        await client.send(reconnect.response(bye))
        await harness.done(identifier)
    return {'same_observed_source': a.remote, 'different_devices_and_ingresses': True,
            'cross_device_delivery': False}
