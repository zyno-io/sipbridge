import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const toolRoot = dirname(fileURLToPath(import.meta.url));
const root = resolve(toolRoot, '../..');
const temporary = mkdtempSync(join(tmpdir(), 'sipbridge-json-check-'));
try {
    execFileSync(join(toolRoot, 'node_modules/.bin/tsc'), [
        '--strict', '--skipLibCheck', '--target', 'ES2022', '--module', 'commonjs',
        '--types', 'node', '--typeRoots', join(toolRoot, 'node_modules/@types'),
        '--rootDir', join(root, 'generated/typescript'), '--outDir', temporary,
        join(root, 'generated/typescript/sipbridge-control.ts'),
        join(root, 'generated/typescript/sipbridge-json.ts'),
        join(root, 'generated/typescript/json-validation.ts')
    ], { stdio: 'inherit' });
    const require = createRequire(import.meta.url);
    const { parseSipbridgeJson, validateSipbridgeJson } = require(join(temporary, 'json-validation.js'));
    const { Envelope } = require(join(temporary, 'sipbridge-control.js'));
    const cases = JSON.parse(readFileSync(join(root, 'proto/sipbridge-control-json-cases.json'), 'utf8'));
    for (const fixture of cases) {
        let parsed;
        let valid = true;
        try {
            parsed = parseSipbridgeJson(fixture.json);
            validateSipbridgeJson(parsed);
        } catch {
            valid = false;
        }
        assert.equal(valid, fixture.valid, fixture.name);
        if (valid) {
            const envelope = Envelope.fromJSON(parsed);
            validateSipbridgeJson(Envelope.toJSON(envelope));
        }
    }
    console.log(`Standalone TypeScript strict JSON contract: ${cases.length} cases passed`);
} finally {
    rmSync(temporary, { recursive: true, force: true });
}
