import type { JsonField } from './sipbridge-json.js';

import { SIPBRIDGE_JSON_MESSAGES } from './sipbridge-json.js';

const uint64 = /^(0|[1-9][0-9]{0,19})$/;
const base64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const invalidUnicode = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** JSON.parse accepts duplicate keys. Reject them, including escaped aliases,
 * before conversion, and bound nesting even inside unknown optional fields. */
export function parseSipbridgeJson(data: Uint8Array | string): unknown {
    const text = typeof data === 'string' ? data : utf8.decode(data);
    const containers: (Set<string> | undefined)[] = [];
    for (let position = 0; position < text.length; position++) {
        const character = text[position];
        if (character === '{' || character === '[') {
            containers.push(character === '{' ? new Set() : undefined);
            if (containers.length > 32) throw new Error('sipbridge JSON nesting limit');
        } else if (character === '}' || character === ']') containers.pop();
        else if (character === '"') {
            const start = position;
            for (position++; position < text.length && text[position] !== '"'; position++) {
                if (text[position] === '\\') position++;
            }
            let next = position + 1;
            while (/\s/.test(text[next] ?? '') && next < text.length) next++;
            if (text[next] === ':') {
                const keys = containers.at(-1);
                const key: unknown = JSON.parse(text.slice(start, position + 1));
                if (!keys || typeof key !== 'string' || invalidUnicode.test(key) || keys.has(key))
                    throw new Error('duplicate or invalid sipbridge JSON key');
                keys.add(key);
            }
        }
    }
    const value: unknown = JSON.parse(text);
    return value;
}

function validateScalar(value: unknown, field: JsonField): void {
    switch (field.kind) {
        case 'message':
            if (!field.message) throw new Error('missing JSON message definition');
            validateSipbridgeJson(value, field.message);
            return;
        case 'uint64':
            if (typeof value === 'string' && uint64.exec(value)?.[0] === value && BigInt(value) <= 0xffffffffffffffffn) return;
            break;
        case 'uint32':
            if (typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= 0xffffffff) return;
            break;
        case 'bytes':
            if (typeof value === 'string' && base64.test(value) && Buffer.from(value, 'base64').toString('base64') === value) return;
            break;
        case 'enum':
            if (typeof value === 'string' && field.values?.includes(value)) return;
            break;
        case 'string':
            if (typeof value === 'string' && !invalidUnicode.test(value)) return;
            break;
        case 'boolean':
            if (typeof value === 'boolean') return;
            break;
    }
    throw new Error(`invalid sipbridge JSON ${field.kind} field`);
}

export function validateSipbridgeJson(value: unknown, name = '.sipbridge.control.v1.Envelope'): void {
    if (!isObject(value)) throw new Error('sipbridge JSON message must be an object');
    const message = SIPBRIDGE_JSON_MESSAGES[name];
    if (!message) throw new Error('unknown sipbridge JSON message');
    const oneofs = new Set<string>();
    const aliases = new Set(Object.values(message.fields).flatMap(field => (field.alias ? [field.alias] : [])));
    for (const [key, item] of Object.entries(value)) {
        const field = Object.hasOwn(message.fields, key) ? message.fields[key] : undefined;
        if (!field) {
            if (aliases.has(key)) throw new Error('noncanonical sipbridge JSON field name');
            continue;
        }
        if (field.oneof) {
            if (oneofs.has(field.oneof)) throw new Error('multiple sipbridge JSON variants');
            oneofs.add(field.oneof);
        }
        if (field.map) {
            if (!isObject(item)) throw new Error('sipbridge JSON map must be an object');
            for (const entry of Object.values(item)) validateScalar(entry, field);
        } else if (field.repeated) {
            if (!Array.isArray(item)) throw new Error('sipbridge JSON repeated field must be an array');
            for (const entry of item) validateScalar(entry, field);
        } else validateScalar(item, field);
    }
    if (message.oneofs.some(oneof => !oneofs.has(oneof))) throw new Error('missing sipbridge JSON variant');
}
