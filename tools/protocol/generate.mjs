import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const toolRoot = dirname(fileURLToPath(import.meta.url));
const root = resolve(toolRoot, '../..');
const checking = process.argv.includes('--check');
const protoc = process.env.SIPBRIDGE_PROTOC ?? 'protoc';
const goPlugin = process.env.SIPBRIDGE_PROTOC_GEN_GO ?? 'protoc-gen-go';
const tsPackage = fileURLToPath(import.meta.resolve('ts-proto/package.json'));
const tsVersion = JSON.parse(readFileSync(tsPackage, 'utf8')).version;
if (tsVersion !== '2.18.3') throw new Error('review the ts-proto pin before regenerating sipbridge');
if (execFileSync(protoc, ['--version'], { encoding: 'utf8' }).trim() !== 'libprotoc 3.21.12') {
    throw new Error('sipbridge generation requires protoc 3.21.12');
}
if (execFileSync(goPlugin, ['--version'], { encoding: 'utf8' }).trim() !== 'protoc-gen-go v1.36.12') {
    throw new Error('sipbridge generation requires protoc-gen-go v1.36.12');
}
const temporary = mkdtempSync(join(root, '.sipbridge-gen-'));
const hash = buffer => createHash('sha256').update(buffer).digest('hex');
const schema = join(root, 'proto/sipbridge-control.proto');
const tsTarget = join(root, 'generated/typescript/sipbridge-control.ts');
const goTarget = join(root, 'internal/controlpb/sipbridge-control.pb.go');
const formatter = join(toolRoot, 'node_modules/.bin/oxfmt');
function format(path) {
    execFileSync(formatter, ['--config', join(toolRoot, '.oxfmtrc.json'), '--write', path], { cwd: toolRoot, stdio: 'pipe' });
}
function install(source, destination) {
    const data = readFileSync(source);
    if (checking) {
        if (!existsSync(destination) || !data.equals(readFileSync(destination))) throw new Error(`generated output drift: ${destination}`);
    } else {
        mkdirSync(dirname(destination), { recursive: true });
        writeFileSync(destination, data);
    }
    return hash(data);
}
try {
    execFileSync(
        protoc,
        [
            `--proto_path=${join(root, 'proto')}`,
            `--plugin=protoc-gen-ts_proto=${join(dirname(tsPackage), 'protoc-gen-ts_proto')}`,
            `--ts_proto_out=${temporary}`,
            '--ts_proto_opt=oneof=unions,useOptionals=messages,forceLong=bigint,outputEncodeMethods=true,outputJsonMethods=true,outputPartialMethods=false,outputClientImpl=false,exportCommonSymbols=false,env=node',
            schema
        ],
        { cwd: root }
    );
    const tsGenerated = join(temporary, 'sipbridge-control.ts');
    // Persistence JSON conversions retain explicit types; binary decoding
    // preserves map keys and applies the same bounded message nesting.
    const generated = readFileSync(tsGenerated, 'utf8');
    const safeJson = generated
        .replaceAll('(reader as any).__tsProtoDecodeDepth', '(reader as BinaryReader & { __tsProtoDecodeDepth?: number }).__tsProtoDecodeDepth')
        .replaceAll('previousRecursionDepth >= 100', 'previousRecursionDepth >= 32')
        .replaceAll('key: key as any', 'key')
        .replaceAll(' as any', '')
        .replaceAll(
            /message\.(\w+)\[(entry\d+)\.key\] = \2\.value;/g,
            'globalThis.Object.defineProperty(message.$1, $2.key, { value: $2.value, enumerable: true, configurable: true, writable: true });'
        )
        .replaceAll(/fromJSON\(object: any\): (\w+) \{/g, 'fromJSON(value: unknown): $1 {\nconst object = isObject(value) ? value : {};')
        .replaceAll('const obj: any = {};', 'const obj: Record<string, unknown> = {};')
        .replaceAll(': any', ': unknown')
        .replaceAll(', any', ', unknown')
        .replace('function isObject(value: unknown): boolean', 'function isObject(value: unknown): value is Record<string, unknown>')
        .replaceAll(/BigInt\((object\.\w+)\)/g, 'BigInt(globalThis.String($1))')
        .replaceAll(/bytesFromBase64\((object\.\w+)\)/g, 'bytesFromBase64(globalThis.String($1))')
        .replaceAll(/(?:globalThis\.)?Math.round\((message\.\w+)\)/g, '$1')
        // Preserve arbitrary map keys, including __proto__, on JSON output too.
        .replaceAll(
            /obj\.(\w+)\[k\] = ([^;]+);/g,
            'globalThis.Object.defineProperty(obj.$1 as Record<string, unknown>, k, { value: $2, enumerable: true, configurable: true, writable: true });'
        );
    if (/\bany\b/.test(safeJson)) throw new Error(`review untyped JSON generator output: ${safeJson.match(/^.*\bany\b.*$/m)?.[0]}`);
    writeFileSync(tsGenerated, safeJson);
    format(tsGenerated);
    const tsHash = install(tsGenerated, tsTarget);
    const schemaHash = install(schema, join(root, 'dependencies/sipbridge-control.proto'));
    const challengeCaseHash = install(join(root, 'proto/sipbridge-digest-challenge-case.json'), join(root, 'internal/engine/testdata/digest-challenge-case.json'));
    execFileSync(protoc, [
        `--proto_path=${join(root, 'proto')}`,
        `--plugin=protoc-gen-go=${goPlugin}`,
        `--go_out=${temporary}`,
        '--go_opt=paths=source_relative',
        schema
    ]);
    const goHash = install(join(temporary, 'sipbridge-control.pb.go'), goTarget);
    const pin = Buffer.from(JSON.stringify({
        repository: 'https://github.com/zyno-io/sipbridge',
        source: 'proto/sipbridge-control.proto',
        schema_sha256: schemaHash,
        protoc: '3.21.12',
        protoc_gen_go: '1.36.12',
        ts_proto: tsVersion,
        generated_go_sha256: goHash,
        generated_ts_sha256: tsHash,
        digest_challenge_case_sha256: challengeCaseHash,
        wire_encoding: 'protobuf',
        subprotocol: 'sipbridge.control.protobuf.v1',
        protocol_major: 1
    }, null, 2) + '\n');
    const pinTarget = join(root, 'dependencies/control-schema.json');
    if (checking) {
        if (!pin.equals(readFileSync(pinTarget))) throw new Error('control schema provenance drift');
    } else writeFileSync(pinTarget, pin);
    console.log(`sipbridge standalone protobuf v1 contract ${checking ? 'checked' : 'generated'}`);
} finally {
    rmSync(temporary, { recursive: true, force: true });
}
