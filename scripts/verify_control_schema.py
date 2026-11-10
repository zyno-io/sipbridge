"""Verify the locally owned protocol sources and all distributed contracts."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--regenerate', action='store_true', help='also reproduce outputs with the pinned local generator')
options = parser.parse_args()
pin = json.loads((root / 'dependencies/control-schema.json').read_text())
if (pin['repository'] != 'https://github.com/zyno-io/sipbridge'
        or pin['source'] != 'proto/sipbridge-control.proto'
        or pin['protocol_major'] != 2
        or pin['subprotocol'] != 'sipbridge.control.json.v2'):
    raise SystemExit('unexpected public control contract provenance')
checks = [
    ('proto/sipbridge-control.proto', 'schema_sha256'),
    ('dependencies/sipbridge-control.proto', 'schema_sha256'),
    ('internal/controlpb/sipbridge-control.pb.go', 'generated_go_sha256'),
    ('generated/typescript/sipbridge-control.ts', 'generated_ts_sha256'),
    ('generated/typescript/sipbridge-json.ts', 'generated_json_validation_sha256'),
    ('tools/protocol/json-validation.ts', 'typescript_validator_sha256'),
    ('generated/typescript/json-validation.ts', 'typescript_validator_sha256'),
    ('proto/sipbridge-control-json-cases.json', 'json_cases_sha256'),
    ('internal/control/testdata/json-cases.json', 'json_cases_sha256'),
    ('proto/sipbridge-digest-challenge-case.json', 'digest_challenge_case_sha256'),
    ('internal/engine/testdata/digest-challenge-case.json', 'digest_challenge_case_sha256')
]
for relative, key in checks:
    if hashlib.sha256((root / relative).read_bytes()).hexdigest() != pin[key]:
        raise SystemExit(f'control schema/generated output drift: {relative}')
if options.regenerate:
    subprocess.run(['node', str(root / 'scripts/generate-control.mjs'), '--check'], cwd=root, check=True)
print('standalone control schema and generated contracts verified:', pin['schema_sha256'])
