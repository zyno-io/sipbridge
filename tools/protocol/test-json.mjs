import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { mkdtempSync, rmSync, cpSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const toolRoot = dirname(fileURLToPath(import.meta.url));
const root = resolve(toolRoot, '../..');
const temporary = mkdtempSync(join(toolRoot, '.sipbridge-test-'));
try {
    const source = join(temporary, 'source');
    const output = join(temporary, 'output');
    mkdirSync(source);
    cpSync(join(root, 'generated/typescript/sipbridge-control.ts'), join(source, 'sipbridge-control.ts'));
    execFileSync(join(toolRoot, 'node_modules/.bin/tsc'), [
        '--strict', '--skipLibCheck', '--target', 'ES2022', '--module', 'Node16', '--moduleResolution', 'Node16',
        '--types', 'node', '--typeRoots', join(toolRoot, 'node_modules/@types'),
        '--rootDir', source, '--outDir', output, join(source, 'sipbridge-control.ts')
    ], { stdio: 'inherit' });
    const require = createRequire(import.meta.url);
    const { Envelope } = require(join(output, 'sipbridge-control.js'));
    const cases = [
        '{"correlationId":"case","hello":{"controller":{"instanceId":"controller","epoch":"epoch"},"connectionAttempt":"18446744073709551615","major":1,"bounds":{"maxEnvelopeBytes":4096}}}',
        '{"correlationId":"case","request":{"snapshot":{"revision":"9007199254740993"},"message":{"wire":"AP+AQQ=="}}}',
        '{"correlationId":"case","delivery":{"outcome":"UNKNOWN"}}',
        '{"correlationId":"case","command":{"sequence":"0","cancel":{}}}',
        '{"correlationId":"case","ingress":{"message":{"headers":[{"name":"X-Test","value":"ok"}],"addresses":{"to":{"values":[{"uri":"sip:a@test","params":{"tag":"abc","__proto__":"safe"}}]}}}}}'
    ];
    for (const fixture of cases) {
        const envelope = Envelope.fromJSON(JSON.parse(fixture));
        const json = Envelope.toJSON(envelope);
        const wire = Envelope.encode(envelope).finish();
        assert.deepEqual(Envelope.toJSON(Envelope.decode(wire)), json, 'protobuf and persistence round trip');
    }
    assert.throws(() => Envelope.decode(Uint8Array.of(255)), 'malformed protobuf must be rejected');
    console.log(`Standalone TypeScript protobuf and persistence contract: ${cases.length} cases passed`);
} finally {
    rmSync(temporary, { recursive: true, force: true });
}
