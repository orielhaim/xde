"""Build frozen engines without sharing Cargo artifacts across source identities."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess

spec = importlib.util.spec_from_file_location('record_build', Path(__file__).with_name('record-build.py'))
record_build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(record_build)


def bind_target(target, identity):
    target.mkdir(parents=True, exist_ok=True)
    binding = target / 'build-source.json'
    if binding.exists():
        if json.loads(binding.read_text(encoding='utf-8')) != identity:
            raise RuntimeError('Cargo target belongs to a different source/toolchain; use a new directory')
    else:
        if any(target.iterdir()):
            raise RuntimeError('Refusing an existing Cargo target without source provenance')
        with binding.open('x', encoding='utf-8') as output:
            json.dump(identity, output, indent=2)


def build(source, target, binary, output):
    manifest = source.parent / f'{source.name}-manifest.json'
    files = json.loads(manifest.read_text(encoding='utf-8'))
    for name, digest in files.items():
        if hashlib.sha256((source / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f'Frozen source changed: {name}')
    identity = {'source': str(source), 'manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
                'compiler': subprocess.check_output(['rustc', '-Vv'], text=True),
                'environment': {key: os.environ.get(key) for key in
                                ['RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER',
                                 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_TARGET']}}
    bind_target(target, identity)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.with_suffix('.log').open('w', encoding='utf-8') as log:
        subprocess.run(['cargo', 'build', '--release', '--locked', '-vv', '-p', 'xde-bench',
                        '--bin', binary, '--manifest-path', str(source / 'Cargo.toml'),
                        '--target-dir', str(target)], stdout=log, stderr=subprocess.STDOUT, check=True)
    executable = target / 'release' / (binary + ('.exe' if os.name == 'nt' else ''))
    result = record_build.record(source, target, executable)
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(result['build']['XDE_SOURCE_ID'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=Path)
    parser.add_argument('target', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--binary', default='xde-bench')
    args = parser.parse_args()
    build(args.source.resolve(), args.target.resolve(), args.binary, args.output.resolve())
