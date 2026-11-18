"""Export a reviewed sipgo revision and regenerate its immutable provenance.

Usage: python3 scripts/update_dependency.py SOURCE_CHECKOUT [--proposal NAME=BRANCH]
The source worktree must be clean; only committed Git objects are exported.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('--proposal', action='append', default=[], metavar='NAME=BRANCH')
    args = parser.parse_args()
    source = args.source.resolve()

    def git(*arguments):
        return subprocess.check_output(['git', '-C', str(source), *arguments])

    if git('status', '--porcelain').strip():
        raise SystemExit('source checkout must be clean before exporting reviewed commits')
    subprocess.run(['python3', str(ROOT / 'scripts/verify_dependency.py')], check=True)
    pin_file = ROOT / 'dependencies/sipgo.json'
    pin = json.loads(pin_file.read_text())
    pin['consolidated_commit'] = git('rev-parse', 'HEAD').decode().strip()
    branch = git('symbolic-ref', '--short', 'HEAD').decode().strip()
    if branch != pin['consolidated_branch']:
        raise SystemExit(f"expected consolidated branch {pin['consolidated_branch']}, got {branch}")
    for proposal in args.proposal:
        name, separator, ref = proposal.partition('=')
        if not separator or not name.isidentifier() or not ref:
            raise SystemExit('proposal must have the form NAME=BRANCH')
        pin[f'{name}_branch'] = ref
        pin[f'{name}_commit'] = git('rev-parse', '--verify', f'{ref}^{{commit}}').decode().strip()

    archive = git('archive', '--format=tar', pin['consolidated_commit'])
    patch = git('diff', '--binary', pin['upstream_commit'], pin['consolidated_commit'])
    library = ROOT / pin['source_directory']
    with tempfile.TemporaryDirectory(prefix='sipgo-export-') as directory:
        staging = Path(directory)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tree:
            for member in tree.getmembers():
                relative = Path(member.name)
                if relative.is_absolute() or '..' in relative.parts or not (member.isfile() or member.isdir()):
                    raise SystemExit(f'unsupported source archive entry: {member.name}')
            tree.extractall(staging, filter='data')
        shutil.rmtree(library)
        shutil.copytree(staging, library)

    manifest = ''.join(
        f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(ROOT)}\n'
        for path in sorted(library.rglob('*')) if path.is_file()
    ).encode()
    (ROOT / pin['manifest']).write_bytes(manifest)
    (ROOT / pin['patch']).write_bytes(patch)
    pin['manifest_sha256'] = hashlib.sha256(manifest).hexdigest()
    pin['patch_sha256'] = hashlib.sha256(patch).hexdigest()
    pin_file.write_text(json.dumps(pin, indent=2) + '\n')
    subprocess.run(['python3', str(ROOT / 'scripts/verify_dependency.py')], check=True)


if __name__ == '__main__':
    main()
