"""Separate TLS ingress fixture. JSON pipes stand in for the control transport."""
import asyncio
import json
import ssl
import sys
import uuid

LIMIT = 65536


async def read_sip(reader):
    head = await reader.readuntil(b'\r\n\r\n')
    size = 0
    for line in head.split(b'\r\n')[1:]:
        key, sep, value = line.partition(b':')
        if sep and key.lower() in (b'content-length', b'l'):
            size = int(value.strip())
    if size < 0 or len(head) + size > LIMIT:
        raise ValueError('SIP message exceeds fixture limit')
    return (head + await reader.readexactly(size)).decode('utf-8')


async def run(backend, cert, key):
    epoch = str(uuid.uuid4())
    flows = {}
    sequence = 0
    output = asyncio.Queue(128)

    async def emit(record):
        await output.put(record)

    async def flush():
        while True:
            record = await output.get()
            encoded = (json.dumps(record) + '\n').encode()
            if len(encoded) > LIMIT:
                raise ValueError('IPC record exceeds fixture limit')
            sys.stdout.buffer.write(encoded)
            sys.stdout.buffer.flush()

    async def client(reader, writer):
        nonlocal sequence
        if len(flows) >= 32:
            writer.close()
            return
        sequence += 1
        flow = {'backend': backend, 'epoch': epoch, 'id': sequence,
                'remote': '{}:{}'.format(*writer.get_extra_info('peername')[:2])}
        flows[sequence] = (flow, writer)
        await emit({'event': 'connected', 'flow': flow})
        try:
            while True:
                wire = await read_sip(reader)
                await emit({'event': 'ingress', 'flow': flow, 'wire': wire})
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as error:
            await emit({'event': 'ingress_error', 'error': str(error), 'flow': flow})
        finally:
            flows.pop(flow['id'], None)
            writer.close()
            await emit({'event': 'closed', 'flow': flow})

    async def commands():
        reader = asyncio.StreamReader(limit=LIMIT)
        protocol = asyncio.StreamReaderProtocol(reader)
        await asyncio.get_running_loop().connect_read_pipe(lambda: protocol, sys.stdin.buffer)
        while line := await reader.readline():
            if len(line) > LIMIT:
                raise ValueError('IPC line exceeds fixture limit')
            command = json.loads(line)
            flow = command['flow']
            current = flows.get(flow['id'])
            delivery = {'delivery_id': command['delivery_id']} if 'delivery_id' in command else {}
            if not current or current[0] != flow:
                await emit({'event': 'delivery_failed', 'flow': flow, **delivery})
                continue
            writer = current[1]
            try:
                writer.write(command['wire'].encode())
                await writer.drain()
                await emit({'event': 'delivered', 'flow': flow, **delivery})
            except ConnectionError:
                await emit({'event': 'delivery_failed', 'flow': flow, **delivery})

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert, key)
    server = await asyncio.start_server(client, '127.0.0.1', 0, ssl=context, limit=LIMIT)
    await emit({'event': 'listening', 'backend': backend, 'epoch': epoch,
                'port': server.sockets[0].getsockname()[1]})
    await asyncio.gather(flush(), commands(), server.serve_forever())


if __name__ == '__main__':
    asyncio.run(run(*sys.argv[1:]))
