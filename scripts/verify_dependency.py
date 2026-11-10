"""Verify the checked-in, immutable native dependency before building or testing."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
pin = json.loads((ROOT / 'dependencies/sipgo.json').read_text())
library = ROOT / pin['source_directory']
manifest = (ROOT / pin['manifest']).read_bytes()
if hashlib.sha256(manifest).hexdigest() != pin['manifest_sha256']:
    raise SystemExit('sipgo manifest does not match its reviewed dependency pin')
expected = set()
for line in manifest.decode().splitlines():
    digest, name = line.split('  ', 1)
    path = ROOT / name
    if not path.is_relative_to(library) or path.is_symlink() or not path.is_file():
        raise SystemExit(f'invalid or missing sipgo source: {name}')
    if name in expected or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise SystemExit(f'sipgo source drift: {name}')
    expected.add(name)
actual = set()
for path in library.rglob('*'):
    if path.is_symlink():
        raise SystemExit(f'sipgo source contains a symlink: {path.relative_to(ROOT)}')
    if path.is_file():
        actual.add(str(path.relative_to(ROOT)))
if actual != expected:
    raise SystemExit('sipgo source file set changed; update/review the consolidated dependency explicitly')
if hashlib.sha256((ROOT / pin['patch']).read_bytes()).hexdigest() != pin['patch_sha256']:
    raise SystemExit('sipgo upstream patch provenance changed')
print('sipgo dependency verified:', pin['consolidated_commit'])
