"""Wire acceptance cases. No SIP implementation from either candidate is the client."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import ssl
import subprocess
import sys
import time
import traceback

from ingress import read_sip

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = Path(os.environ.get('PROOF_EVIDENCE_DIR', str(ROOT / 'artifacts/qualification')))
OWNER_BINARY = Path(os.environ.get('PROOF_OWNER_BINARY', str(ROOT / 'bin/sipbridge-fixture')))


def headers(wire):
    result = {}
    for line in wire.split('\r\n')[1:]:
        if not line:
            break
        key, value = line.split(':', 1)
        result.setdefault(key.lower(), []).append(value.strip())
    return result


def field(wire, key):
    return headers(wire)[key.lower()][0]


def request(method, call, seq=1, tag=None, branch=None, extra=(), device='alice', contact=None):
    to = '<sip:runtime@example.test>' + (f';tag={tag}' if tag else '')
    branch = branch or f'z9hG4bK-{call}-{seq}-{method}'
    contact = contact or f'sip:{device}@old-contact.invalid:5061;transport=tls'
    return '\r\n'.join([
        f'{method} sip:runtime@example.test SIP/2.0',
        f'Via: SIP/2.0/TLS device.invalid:5061;branch={branch};rport',
        'Max-Forwards: 70', f'From: <sip:{device}@example.test>;tag={device}-tag',
        f'To: {to}', f'Call-ID: {call}', f'CSeq: {seq} {method}', f'Contact: <{contact}>',
        *extra, 'Content-Length: 0', '', ''
    ])


def response(req, status=200, extra=()):
    h = headers(req)
    values = [f'SIP/2.0 {status} OK']
    values.extend('Via: ' + value for value in h['via'])
    values.extend(f'{key}: {h[key.lower()][0]}' for key in ('From', 'To', 'Call-ID', 'CSeq'))
    values.extend(extra)
    return '\r\n'.join([*values, 'Content-Length: 0', '', ''])


class Harness:
    def __init__(self, name):
        self.name = name
        self.processes = []
        self.tasks = []
        self.clients = []
        self.records = []
        self.condition = asyncio.Condition()
        self.backends = {}
        self.errors = []

    async def send(self, process, record):
        process.stdin.write((json.dumps(record) + '\n').encode())
        await process.stdin.drain()

    async def record(self, origin, record):
        async with self.condition:
            self.records.append({'origin': origin, **record})
            self.condition.notify_all()

    async def spawn(self, origin, *args, env=None):
        log = (EVIDENCE / f'{self.name}-{origin}.log').open('wb')
        process = await asyncio.create_subprocess_exec(*args, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=log, limit=65536, env=env)
        self.processes.append((process, log))

        async def pump():
            while line := await process.stdout.readline():
                event = json.loads(line)
                await self.record(origin, event)
                if origin == 'owner' and event['event'] == 'egress':
                    await self.send(self.backends[event['flow']['backend']], event)
                elif origin != 'owner' and event['event'] in ('ingress', 'closed'):
                    if event['event'] != 'closed' or not getattr(self, 'hold_closed', False):
                        await self.send(self.owner, {'op': event['event'], **{k: v for k, v in event.items() if k != 'event'}})
                elif origin != 'owner' and event['event'] in ('delivered', 'delivery_failed') and 'delivery_id' in event:
                    if not getattr(self, 'drop_delivery_ack', False):
                        await self.send(self.owner, {'op': 'delivery_ack', 'delivery_id': event['delivery_id'],
                            'error': 'ingress could not write TLS flow' if event['event'] == 'delivery_failed' else ''})
                elif event['event'] in ('input_error', 'handler_error', 'ingress_error', 'egress_failed', 'delivery_failed'):
                    self.errors.append(event)

        task = asyncio.create_task(pump())
        self.tasks.append(task)
        return process

    async def wait(self, predicate, start=0, timeout=3):
        async def find():
            async with self.condition:
                while True:
                    for record in self.records[start:]:
                        if predicate(record):
                            return record
                    await self.condition.wait()
        return await asyncio.wait_for(find(), timeout)

    async def start(self, pinned=False):
        env = dict(os.environ)
        if pinned:
            env['PROOF_PIN_INITIAL_FLOW'] = '1'
        self.owner = await self.spawn('owner', str(OWNER_BINARY), env=env)
        await self.wait(lambda r: r['event'] == 'ready')
        for backend in ('A', 'B'):
            self.backends[backend] = await self.spawn(backend, sys.executable, str(ROOT / 'test/ingress.py'),
                backend, str(EVIDENCE / 'certs/server.pem'), str(EVIDENCE / 'certs/server.key'))
        await self.wait(lambda r: r['event'] == 'listening' and r['backend'] == 'B')
        await self.wait(lambda r: r['event'] == 'listening' and r['backend'] == 'A')

    async def connect(self, backend, source_port=None):
        listening = next(r for r in self.records if r['event'] == 'listening' and r['backend'] == backend)
        tls = ssl.create_default_context(cafile=str(EVIDENCE / 'certs/server.pem'))
        if source_port is None:
            reader, writer = await asyncio.open_connection('127.0.0.1', listening['port'], ssl=tls, server_hostname='localhost')
        else:
            transport = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            transport.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            transport.setblocking(False)
            transport.bind(('127.0.0.1', source_port))
            await asyncio.get_running_loop().sock_connect(transport, ('127.0.0.1', listening['port']))
            reader, writer = await asyncio.open_connection(sock=transport, ssl=tls, server_hostname='localhost')
        client = Client(reader, writer, self, backend)
        self.clients.append(client)
        await self.wait(lambda r: r['event'] == 'connected' and r['flow']['backend'] == backend
                        and r['flow']['remote'] == client.remote)
        return client

    async def command(self, call, action):
        identifier = f'{call}-{action}-{len(self.records)}'
        await self.send(self.owner, {'op': 'command', 'id': identifier, 'call_id': call, 'action': action})
        return identifier

    async def done(self, identifier):
        result = await self.wait(lambda r: r['event'] in ('command_done', 'command_error') and r['id'] == identifier)
        assert result['event'] == 'command_done' and result['result'] == 'Ok(Some(200))', result

    async def stats(self):
        identifier = f'stats-{len(self.records)}'
        await self.send(self.owner, {'op': 'stats', 'id': identifier})
        return await self.wait(lambda r: r['event'] == 'stats' and r['id'] == identifier)

    async def stop(self):
        for client in self.clients:
            client.writer.close()
        await asyncio.sleep(0.05)
        for process, log in self.processes:
            if process.returncode is None:
                process.terminate()
        for process, log in self.processes:
            await process.wait()
            log.close()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        (EVIDENCE / f'{self.name}-trace.json').write_text(json.dumps(self.records, indent=2) + '\n')


class Client:
    def __init__(self, reader, writer, harness, backend):
        self.reader, self.writer, self.harness, self.backend = reader, writer, harness, backend
        self.remote = '{}:{}'.format(*writer.get_extra_info('sockname')[:2])
        self.received = []

    async def send(self, wire):
        self.writer.write(wire.encode())
        await self.writer.drain()

    async def recv(self, method=None, code=None, timeout=3):
        async def receive():
            while True:
                wire = await read_sip(self.reader)
                self.received.append(wire)
                if method and field(wire, 'cseq').split()[1] != method:
                    continue
                if code is not None and not wire.startswith(f'SIP/2.0 {code} '):
                    continue
                return wire
        return await asyncio.wait_for(receive(), timeout)

    async def register(self, device='alice', token=None, code=200):
        call = f'reg-{device}-{len(self.harness.records)}'
        self.last_registration = request('REGISTER', call, extra=[f'X-Proof-Device: {device}',
            f'X-Proof-Token: {token if token is not None else "fixture-" + device}'], device=device)
        await self.send(self.last_registration)
        return await self.recv('REGISTER', code)

    async def quiet(self, seconds=0.22):
        try:
            wire = await self.recv(timeout=seconds)
        except TimeoutError:
            return
        raise AssertionError(f'unexpected message on {self.backend}: {wire}')


def local_tag(resp):
    return re.search(r';tag=([^;\s>]+)', field(resp, 'to')).group(1)


async def early_cancel(h, close_mode='silent', pinned=False):
    a = await h.connect('A')
    await a.register()
    invite = request('INVITE', 'cancel-case', branch='z9hG4bK-original')
    await a.send(invite)
    provisional = await a.recv('INVITE', 183)
    assert len(headers(provisional)['via']) == 1
    assert 'received=127.0.0.1' in field(provisional, 'via')
    assert f'rport={a.remote.split(":")[1]}' in field(provisional, 'via')
    if close_mode == 'fin':
        a.writer.close()
        await a.writer.wait_closed()
        await h.wait(lambda r: r['event'] == 'closed' and r['flow']['remote'] == a.remote)
    b = await h.connect('B')
    assert a.remote != b.remote
    await b.register()
    if close_mode == 'late_fin':
        a.writer.close()
        await a.writer.wait_closed()
        await h.wait(lambda r: r['event'] == 'closed' and r['flow']['remote'] == a.remote)
    cancel = request('CANCEL', 'cancel-case', branch='z9hG4bK-original')
    await b.send(cancel)
    if pinned:
        ok = await a.recv('CANCEL', 200)
        final = await a.recv('INVITE', 487)
        await b.quiet()
        return {'negative_control': 'both replies stayed on original A despite REGISTER on B'}
    ok = await b.recv('CANCEL', 200)
    final = await b.recv('INVITE', 487)
    assert field(ok, 'call-id') == field(final, 'call-id') == 'cancel-case'
    assert field(ok, 'cseq') == '1 CANCEL' and field(final, 'cseq') == '1 INVITE'
    observed_tags = {'cancel_200': local_tag(ok), 'invite_487': local_tag(final), 'invite_183': local_tag(provisional)}
    assert all(len(headers(w)['via']) == 1 and 'branch=z9hG4bK-original' in field(w, 'via') for w in (ok, final))
    if close_mode == 'silent':
        await a.quiet()
    await b.send(cancel)
    repeated_cancel = await b.recv('CANCEL', 200)
    observed_tags['repeated_cancel_200'] = local_tag(repeated_cancel)
    await b.send(invite)
    cached_final = await b.recv('INVITE', 487)
    assert field(cached_final, 'to') == field(final, 'to')
    await b.send(request('ACK', 'cancel-case', tag=local_tag(final), branch='z9hG4bK-original'))
    await b.send(request('CANCEL', 'unknown-cancel', branch='z9hG4bK-unknown'))
    await b.recv('CANCEL', 481)
    await asyncio.sleep(0.5)
    stats = await h.stats()
    assert stats['running'] == stats['waiting_ack'] == stats['dialogs'] == 0, stats
    return {'cancel_200_and_invite_487': 'B', 'ack_and_cleanup': stats, 'old_flow': close_mode,
            'observed_tags': observed_tags, 'qualification_gaps': [] if len(set(observed_tags.values())) == 1 else ['487 changed the established early-dialog To tag']}


async def confirmed(h):
    a = await h.connect('A')
    await a.register()
    await a.send(request('INVITE', 'confirmed-case', extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    assert field(accepted, 'contact').lower() == '<sip:runtime@203.0.113.10:5061;transport=tls>', accepted
    b = await h.connect('B')
    await b.register()
    await b.send(request('ACK', 'confirmed-case', tag=local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    # No SUBSCRIBE or in-dialog renewal occurs before this server request.
    identifier = await h.command('confirmed-case', 'info')
    info = await b.recv('INFO')
    assert info.startswith('INFO ')
    assert field(info, 'via').startswith('SIP/2.0/TLS 203.0.113.10:5061;'), info
    assert len(headers(info)['via']) == 1
    await b.send(response(info))
    await h.done(identifier)
    identifier = await h.command('confirmed-case', 'reinvite')
    reinvite = await b.recv('INVITE')
    await b.send(response(reinvite, 183))
    await b.quiet(0.15)
    await b.send(response(reinvite, 200, ['Contact: <sip:alice@new-contact.invalid:5061;transport=tls>']))
    ack = await b.recv('ACK')
    assert ack.lower().startswith('ack sip:alice@new-contact.invalid:5061;transport=tls '), ack
    assert field(ack, 'cseq') == field(reinvite, 'cseq').replace('INVITE', 'ACK'), ack
    await h.done(identifier)
    identifier = await h.command('confirmed-case', 'bye')
    bye = await b.recv('BYE')
    await b.send(response(bye))
    await h.done(identifier)
    await a.quiet()
    return {'server_info_reinvite_ack_bye': 'B', 'public_contact_and_tls_via': True,
            'bye_target': bye.split(' ', 2)[1],
            'qualification_gaps': [] if 'new-contact.invalid' in bye.split(' ', 2)[1] else
                ['successful outgoing re-INVITE Contact was not committed for later BYE']}


async def subscription(h):
    a = await h.connect('A')
    await a.register()
    await a.send(request('SUBSCRIBE', 'subscription-case', extra=['Event: message-summary', 'Expires: 60']))
    subscribed = await a.recv('SUBSCRIBE', 200)
    b = await h.connect('B')
    await b.register()
    identifier = await h.command('subscription-case', 'notify')
    notify = await b.recv('NOTIFY')
    await b.send(response(notify))
    await h.done(identifier)
    await a.quiet()
    accepted_target = 'sip:alice@accepted-refresh.invalid:5072;transport=tls'
    await b.send(request('SUBSCRIBE', 'subscription-case', seq=2, tag=local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 60'], contact=accepted_target))
    await b.recv('SUBSCRIBE', 200)
    identifier = await h.command('subscription-case', 'notify')
    after_accept = await b.recv('NOTIFY')
    await b.send(response(after_accept))
    await h.done(identifier)
    await b.send(request('SUBSCRIBE', 'subscription-case', seq=3, tag=local_tag(subscribed),
        extra=['Event: message-summary', 'Expires: 60', 'X-Proof-Reject: yes'],
        contact='sip:alice@rejected-refresh.invalid:5088;transport=tls'))
    await b.recv('SUBSCRIBE', 403)
    identifier = await h.command('subscription-case', 'notify')
    after_reject = await b.recv('NOTIFY')
    await b.send(response(after_reject))
    await h.done(identifier)
    assert after_reject.split(' ', 2)[1] == after_accept.split(' ', 2)[1], 'rejected refresh changed target'
    return {'notify_after_register_only': 'B', 'request_uri': notify.split(' ', 2)[1],
            'subscription_local_tag': local_tag(subscribed), 'accepted_refresh_target': after_accept.split(' ', 2)[1],
            'rejected_refresh_preserved_target': True,
            'qualification_gaps': [] if after_accept.split(' ', 2)[1].lower() == accepted_target else
                ['accepted SUBSCRIBE refresh did not commit new Contact']}


async def accepted_timer(h):
    a = await h.connect('A')
    await a.register()
    await a.send(request('INVITE', 'timer-case', extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    b = await h.connect('B')
    await b.register()
    after_registration = len(h.records)
    repeated = await b.recv('INVITE', 200, timeout=1)
    assert field(repeated, 'to') == field(accepted, 'to')
    await b.send(request('ACK', 'timer-case', tag=local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    await asyncio.sleep(0.25)
    old_output = [r for r in h.records[after_registration:] if r['event'] == 'egress' and r['flow']['backend'] == 'A']
    assert not old_output, old_output
    identifier = await h.command('timer-case', 'bye')
    bye = await b.recv('BYE')
    await b.send(response(bye))
    await h.done(identifier)
    return {'native_2xx_timer_output': 'B', 'old_flow_egress_after_register': len(old_output)}


async def binding_authority(h):
    a = await h.connect('A')
    await a.register()
    await a.send(request('INVITE', 'authority-case', extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(request('ACK', 'authority-case', tag=local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    b = await h.connect('B')
    await b.register(token='wrong', code=403)
    await b.send(request('BYE', 'authority-case', seq=2, tag=local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'denied' and r['reason'] == 'unbound flow')
    identifier = await h.command('authority-case', 'info')
    info = await a.recv('INFO')
    await a.send(response(info))
    await h.done(identifier)
    await b.quiet()
    await b.register(device='bob')
    await b.send(request('BYE', 'authority-case', seq=2, tag=local_tag(accepted), device='bob'))
    await h.wait(lambda r: r['event'] == 'denied' and r['reason'] == 'context identity')
    identifier = await h.command('authority-case', 'bye')
    bye = await a.recv('BYE')
    await a.send(response(bye))
    await h.done(identifier)
    return {'rejected_register': 'binding unchanged', 'unbound_and_other_device_injection': 'denied'}


async def registration_replay(h):
    a = await h.connect('A')
    await a.register()
    await a.send(request('INVITE', 'replay-case', extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(request('ACK', 'replay-case', tag=local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    b = await h.connect('B')
    await b.register()
    await a.send(a.last_registration)
    await a.recv('REGISTER', 200)
    identifier = await h.command('replay-case', 'bye')
    bye = await b.recv('BYE')
    await b.send(response(bye))
    await h.done(identifier)
    await a.quiet()
    return {'cached_register_reply': 'A', 'current_dialog_binding_after_replay': 'B'}


async def client_bye(h):
    a = await h.connect('A')
    await a.register()
    await a.send(request('INVITE', 'client-bye-case', extra=['X-Proof-Answer: yes']))
    accepted = await a.recv('INVITE', 200)
    await a.send(request('ACK', 'client-bye-case', tag=local_tag(accepted)))
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Confirmed('))
    b = await h.connect('B')
    await b.register()
    await b.send(request('BYE', 'client-bye-case', seq=2, tag='wrong-tag'))
    await b.recv('BYE', 481)
    assert (await h.stats())['dialogs'] == 1
    await b.send(request('BYE', 'client-bye-case', seq=2, tag=local_tag(accepted), branch='z9hG4bK-correct-bye'))
    await b.recv('BYE', 200)
    await h.wait(lambda r: r['event'] == 'dialog_state' and r['state'].startswith('Terminated('))
    assert (await h.stats())['dialogs'] == 0
    await a.quiet()
    return {'wrong_local_tag': 481, 'correct_client_bye_on_B': 200, 'original_owner_dialogs': 0}


def certificates():
    certs = EVIDENCE / 'certs'
    certs.mkdir(parents=True, exist_ok=True)
    valid = (certs / 'server.pem').exists() and subprocess.run(
        ['openssl', 'x509', '-checkend', '3600', '-noout', '-in', str(certs / 'server.pem')],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if not valid:
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
            '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
            '-keyout', str(certs / 'server.key'), '-out', str(certs / 'server.pem')],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (certs / 'server.key').chmod(0o600)


async def main(overrides=None, extra_cases=()):
    pin = json.loads((ROOT / 'dependencies/sipgo.json').read_text())
    library = Path(os.environ.get('PROOF_LIBRARY_ROOT', str(ROOT / pin['source_directory'])))
    assert library.resolve() == (ROOT / pin['source_directory']).resolve(), 'library source directory changed'
    subprocess.run([sys.executable, str(ROOT / 'scripts/verify_dependency.py')], check=True)
    revision = pin['consolidated_commit']
    assert revision == os.environ.get('PROOF_LIBRARY_REVISION', revision), 'library source revision changed'
    certificates()
    results = []
    cases = [
        ('pinned-negative-control', lambda h: early_cancel(h, pinned=True), True),
        ('cross-backend-cancel-silent', early_cancel, False),
        ('cross-backend-cancel-fin', lambda h: early_cancel(h, 'fin'), False),
        ('cross-backend-cancel-late-fin', lambda h: early_cancel(h, 'late_fin'), False),
        ('confirmed-dialog', confirmed, False),
        ('accepted-invite-timer', accepted_timer, False),
        ('subscription', subscription, False),
        ('binding-authority', binding_authority, False),
        ('registration-replay', registration_replay, False),
        ('client-bye', client_bye, False)
    ]
    cases.extend(extra_cases)
    for name, case, pinned in cases:
        case = (overrides or {}).get(name, case)
        harness = Harness(name)
        started = time.monotonic()
        try:
            await harness.start(pinned)
            detail = await case(harness)
            assert not harness.errors, harness.errors
            gaps = detail.get('qualification_gaps', [])
            status = 'qualification_gap' if gaps else 'passed'
            mechanism = None if detail.get('not_exercised') else True
            results.append({'name': name, 'mechanism_passed': mechanism, 'status': status, 'detail': detail})
            print(f'{"GAP" if gaps else "PASS"} {name}', flush=True)
        except Exception as error:
            results.append({'name': name, 'mechanism_passed': False, 'status': 'unexpected_failure',
                            'error': str(error), 'traceback': traceback.format_exc()})
            print(f'FAIL {name}: {error}', flush=True)
        finally:
            results[-1]['elapsed_seconds'] = round(time.monotonic() - started, 3)
            await harness.stop()
    evidence = {'library_revision': revision,
        'binary_sha256': hashlib.sha256(OWNER_BINARY.read_bytes()).hexdigest(),
        'cases': results, 'scope': 'two separate TLS ingress processes, one fixed protocol owner, fixture IPC and authorization'}
    (EVIDENCE / 'reconnect-evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    if any(case['mechanism_passed'] is False for case in results):
        return 1
    return 2 if any(case['status'] == 'qualification_gap' for case in results) else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
