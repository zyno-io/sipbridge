// Same fixture semantics as controller/main.go; no directory, database or media.
const Srf = require('drachtio-srf');
const http = require('node:http');
const srf = new Srf();
const stats = { register: 0, subscribe: 0, notify: 0, failures: 0 };
const fail = error => { stats.failures++; console.error(String(error)); };
const notify = async (dialog, request) => {
    const event = request.get('Event');
    const terminal = request.get('Expires') === '0';
    const content = event === 'dialog' ? 'application/dialog-info+xml' : 'application/simple-message-summary';
    const body = event === 'dialog'
        ? `<?xml version="1.0"?><dialog-info xmlns="urn:ietf:params:xml:ns:dialog-info" version="${request.get('CSeq').split(' ')[0]}" state="full" entity="sip:1000@bench.invalid"></dialog-info>`
        : 'Messages-Waiting: no\r\nMessage-Account: sip:1000@bench.invalid\r\nVoice-Message: 0/0 (0/0)\r\n';
    const response = await dialog.request({ method: 'NOTIFY', headers: {
        Event: event, 'Subscription-State': terminal ? 'terminated;reason=timeout' : `active;expires=${request.get('Expires')}`,
        'Content-Type': content
    }, body });
    if (response.status !== 200) throw new Error(`NOTIFY failed: ${response.status}`);
    stats.notify++;
};
srf.register((request, response) => {
    response.send(200, { headers: { Contact: request.get('Contact'), Expires: request.get('Expires') } });
    stats.register++;
});
srf.subscribe(async (request, response) => {
    try {
        const dialog = await srf.createUAS(request, response, { headers: { Expires: request.get('Expires') } });
        stats.subscribe++;
        dialog.on('subscribe', (refresh, reply) => {
            reply.send(202, { headers: { Expires: refresh.get('Expires') } });
            stats.subscribe++;
            notify(dialog, refresh).catch(fail);
        });
        dialog.on('unsubscribe', refresh => { stats.subscribe++; notify(dialog, refresh).catch(fail); });
        await notify(dialog, request);
    } catch (error) { fail(error); }
});
srf.on('error', fail);
srf.on('connect', error => {
    if (error) return fail(error);
    http.createServer((request, response) => {
        response.setHeader('Content-Type', 'application/json');
        response.end(JSON.stringify({ ...stats, controllerRuntime: process.memoryUsage(), controllerCPU: process.cpuUsage() }));
    }).listen(8801, '0.0.0.0', () => console.log('fixture-application-ready'));
});
srf.connect({ host: 'core', port: 9022, secret: 'performance-only' });
