#!/usr/bin/env python3
"""Bounded TLS phone workload, identical for each SIP backend; stdlib only."""
import argparse
import asyncio
import collections
import json
import math
import ssl
import time
import uuid
from pathlib import Path


class Message:
    def __init__(self, head, body):
        lines = head.decode().split('\r\n')
        self.start = lines[0]
        self.headers = collections.defaultdict(list)
        for line in lines[1:]:
            key, value = line.split(':', 1)
            self.headers[key.lower()].append(value.strip())
        self.body = body

    def get(self, name):
        return self.headers[name.lower()][0] if self.headers[name.lower()] else ''


class Phone:
    def __init__(self, number, args, metrics):
        self.number, self.args, self.metrics = number, args, metrics
        self.pending, self.dialogs, self.notifies, self.notify_events = {}, {}, set(), {}
        self.reg_id = f'reg-{number}@bench.invalid'
        self.reg_seq = 0
        self.tag = f'phone-{number}'

    async def open(self):
        tls = ssl.create_default_context(cafile=self.args.ca)
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        tls.maximum_version = ssl.TLSVersion.TLSv1_2
        tls.set_ciphers('ECDHE-RSA-AES128-GCM-SHA256')
        self.reader, self.writer = await asyncio.open_connection(self.args.host, 5061, ssl=tls, server_hostname='core')
        negotiated = self.writer.get_extra_info('ssl_object')
        cipher = negotiated.version()+'/'+negotiated.cipher()[0]
        self.metrics['tls'][cipher] = self.metrics['tls'].get(cipher, 0)+1
        host, port = self.writer.get_extra_info('sockname')[:2]
        self.address = f'{host}:{port}'
        self.task = asyncio.create_task(self.read())

    async def read(self):
        try:
            while True:
                head = await self.reader.readuntil(b'\r\n\r\n')
                length = next((int(x.split(b':', 1)[1]) for x in head.split(b'\r\n') if x.lower().startswith(b'content-length:')), 0)
                if not 0 <= length <= 65536:
                    raise ValueError('fixture message too large')
                body = await self.reader.readexactly(length)
                message = Message(head[:-4], body)
                key = (message.get('call-id'), message.get('cseq'))
                if message.start.startswith('SIP/2.0 '):
                    status = int(message.start.split()[1])
                    if status < 200:
                        continue
                    pending = self.pending.get(key)
                    if pending is None or pending.done():
                        self.metrics['duplicateFinals'] += 1
                    else:
                        pending.set_result(message)
                    continue
                if not message.start.startswith('NOTIFY '):
                    raise ValueError('unexpected request: ' + message.start)
                response = ['SIP/2.0 200 OK']
                for name in ('Via', 'From', 'To', 'Call-ID', 'CSeq'):
                    response.extend(f'{name}: {value}' for value in message.headers[name.lower()])
                response.extend(['Content-Length: 0', '', ''])
                self.writer.write('\r\n'.join(response).encode())
                await self.writer.drain()
                if key in self.notifies:
                    self.metrics['notifyRetransmissions'] += 1
                    continue
                if len(self.notifies) >= 100000:
                    raise ValueError('fixture NOTIFY evidence capacity')
                self.notifies.add(key)
                event = message.get('event')
                content = message.get('content-type')
                if event == 'dialog':
                    if content != 'application/dialog-info+xml' or b'<dialog-info' not in message.body:
                        raise ValueError('invalid BLF notification')
                elif event == 'message-summary':
                    if content != 'application/simple-message-summary' or b'Messages-Waiting: no' not in message.body:
                        raise ValueError('invalid MWI notification')
                else:
                    raise ValueError('unexpected subscription event')
                self.metrics['notify'] += 1
                self.metrics['notify200'] += 1
                self.metrics['notifyBytes'] += len(message.body)
                call_id = message.get('call-id')
                self.notify_events.setdefault(call_id, asyncio.Queue()).put_nowait(message)
        except asyncio.CancelledError:
            pass
        except Exception as error:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(error)
            self.metrics['readerErrors'].append(str(error))

    async def exchange(self, method, call_id, sequence, remote_tag='', event='', expires=3600):
        branch = f'z9hG4bK-{self.number}-{method}-{sequence}-{call_id.split("@")[0]}'
        to = f'<sip:1000@bench.invalid>' + (f';tag={remote_tag}' if remote_tag else '')
        lines = [f'{method} sip:1000@bench.invalid SIP/2.0', f'Via: SIP/2.0/TLS {self.address};branch={branch};rport;alias',
                 'Max-Forwards: 70', f'From: <sip:{self.number}@bench.invalid>;tag={self.tag}', f'To: {to}',
                 f'Call-ID: {call_id}', f'CSeq: {sequence} {method}', f'Contact: <sip:{self.number}@{self.address};transport=tls>',
                 f'Expires: {expires}', 'User-Agent: sipbridge-controlled-benchmark']
        if event:
            lines.extend([f'Event: {event}', 'Accept: ' + ('application/dialog-info+xml' if event == 'dialog' else 'application/simple-message-summary')])
        lines.extend(['Content-Length: 0', '', ''])
        wire = '\r\n'.join(lines).encode()
        key = (call_id, f'{sequence} {method}')
        future = asyncio.get_running_loop().create_future()
        self.pending[key] = future
        started = time.perf_counter()
        try:
            self.writer.write(wire)
            await self.writer.drain()
            response = await asyncio.wait_for(future, self.args.timeout)
            wanted = 200 if method == 'REGISTER' else 202
            if int(response.start.split()[1]) != wanted:
                raise ValueError(f'{method}: expected {wanted}, received {response.start}')
            self.metrics[method.lower()] += 1
            self.metrics['latencyMS'][method].append((time.perf_counter() - started) * 1000)
            if event:
                queue = self.notify_events.setdefault(call_id, asyncio.Queue())
                notify = await asyncio.wait_for(queue.get(), self.args.timeout)
                state = notify.get('subscription-state')
                if notify.get('event') != event or (expires == 0 and not state.startswith('terminated')) or (expires > 0 and not state.startswith('active')):
                    raise ValueError('NOTIFY does not match accepted subscription')
                self.metrics['latencyMS']['SUBSCRIBE+NOTIFY'].append((time.perf_counter() - started) * 1000)
                return response.get('to').split(';tag=', 1)[1].split(';', 1)[0]
        finally:
            self.pending.pop(key, None)

    async def register(self):
        self.reg_seq += 1
        await self.exchange('REGISTER', self.reg_id, self.reg_seq)

    async def subscriptions(self, fresh=False, terminate=False):
        for event in ('dialog', 'message-summary'):
            if fresh:
                self.dialogs[event] = [f'sub-{self.number}-{uuid.uuid4().hex[:12]}@bench.invalid', 0, '']
            dialog = self.dialogs[event]
            dialog[1] += 1
            dialog[2] = await self.exchange('SUBSCRIBE', dialog[0], dialog[1], dialog[2], event, 0 if terminate else 3600)
            if terminate:
                self.notify_events.pop(dialog[0], None)

    async def refresh(self):
        await self.register()
        await self.subscriptions()

    async def close(self):
        self.task.cancel()
        await self.task
        self.writer.close()
        await self.writer.wait_closed()


def summarize(values):
    values = sorted(values)
    return {key: values[min(len(values)-1, math.ceil(len(values)*fraction)-1)] if values else None
            for key, fraction in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}


async def run(args):
    metrics = {'register': 0, 'subscribe': 0, 'notify': 0, 'notify200': 0, 'notifyBytes': 0, 'duplicateFinals': 0,
               'notifyRetransmissions': 0, 'sipRequestRetransmissions': 0, 'readerErrors': [], 'tls': {},
               'latencyMS': {key: [] for key in ('REGISTER', 'SUBSCRIBE', 'SUBSCRIBE+NOTIFY')}}
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    phones = [Phone(1000+i, args, metrics) for i in range(args.phones)]
    try:
        for phone in phones:
            await phone.open()
            await phone.register()
            await phone.subscriptions(fresh=True)
        for _ in range(args.history):
            for phone in phones:
                await phone.subscriptions(terminate=True)
                await phone.subscriptions(fresh=True)
        history_completed = time.monotonic()
        await asyncio.sleep(args.warmup)
        setup_expected = {'register': args.phones, 'subscribe': 2*args.phones*(1+2*args.history),
                          'notify': 2*args.phones*(1+2*args.history), 'notify200': 2*args.phones*(1+2*args.history)}
        assert all(metrics[k] == v for k, v in setup_expected.items()), (metrics, setup_expected)
        assert not metrics['readerErrors'] and metrics['notifyRetransmissions'] == metrics['duplicateFinals'] == 0
        # Runner starts resource measurement only after identical successful setup/history.
        (output/'ready.json').write_text(json.dumps({'phones': args.phones, 'history': args.history, 'expected': setup_expected, 'setup': {k: v for k, v in metrics.items() if isinstance(v, int)}}))
        while not (output/'start').exists():
            await asyncio.sleep(.05)
        before = {k: v for k, v in metrics.items() if isinstance(v, int)}
        latency_start = {k: len(v) for k, v in metrics['latencyMS'].items()}
        start = time.monotonic()
        rounds = int(args.duration // args.cadence)
        async def drive(phone, index):
            # Spread renewals evenly rather than accidentally comparing synchronized bursts.
            for turn in range(rounds):
                due = start + turn*args.cadence + index*args.cadence/args.phones
                await asyncio.sleep(max(0, due-time.monotonic()))
                await phone.refresh()
        await asyncio.gather(*(drive(phone, index) for index, phone in enumerate(phones)))
        await asyncio.sleep(max(0, start+args.duration-time.monotonic()))
        delta = {k: metrics[k]-v for k, v in before.items()}
        expected = {'register': rounds*args.phones, 'subscribe': 2*rounds*args.phones, 'notify': 2*rounds*args.phones}
        assert all(delta[k] == v for k, v in expected.items()), (delta, expected)
        assert not metrics['readerErrors'] and metrics['notifyRetransmissions'] == metrics['duplicateFinals'] == 0
        assert delta['notify200'] == expected['notify']
        result = {'status': 'PASS', 'elapsedSeconds': time.monotonic()-start, 'measurement': delta, 'expected': expected,
                  'historicalGenerationAgeAtEndSeconds': time.monotonic()-history_completed,
                  'liveRegistrations': args.phones, 'liveSubscriptions': 2*args.phones,
                  'allPhases': {k: v for k, v in metrics.items() if isinstance(v, int)},
                  'latencyMS': {key: summarize(value[latency_start[key]:]) for key, value in metrics['latencyMS'].items()},
                  'latencyMSAllPhases': {key: summarize(value) for key, value in metrics['latencyMS'].items()},
                  'transport': 'TLS', 'negotiatedTLS': metrics['tls'], 'requestRetransmissionPolicy': 'No application retry on TLS; failures abort the comparison',
                  'config': vars(args)}
        (output/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    except Exception as error:
        (output/'result.json').write_text(json.dumps({'status': 'FAIL', 'error': str(error), 'counts': {k:v for k,v in metrics.items() if k != 'latencyMS'}}, indent=2)+'\n')
        raise
    finally:
        for phone in phones:
            if hasattr(phone, 'task'):
                await phone.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='core')
    parser.add_argument('--ca', default='/certs/ca.crt')
    parser.add_argument('--output', default='/output')
    parser.add_argument('--phones', type=int, choices=(1,17,130), default=17)
    parser.add_argument('--duration', type=float, default=120)
    parser.add_argument('--cadence', type=float, default=15)
    parser.add_argument('--warmup', type=float, default=5)
    parser.add_argument('--history', type=int, default=0)
    parser.add_argument('--timeout', type=float, default=5)
    args = parser.parse_args()
    assert 1 <= args.cadence <= args.duration <= 3600 and 0 <= args.history <= 10 and 0 <= args.warmup <= 180
    asyncio.run(run(args))
