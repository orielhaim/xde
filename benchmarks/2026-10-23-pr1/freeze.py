"""Freeze both engines with one fixture, harness, toolchain and dependency lock."""
import argparse
import hashlib
import io
import json
import subprocess
import zipfile
from pathlib import Path


def source_file(path):
    return path.endswith(".rs") or path.endswith("Cargo.toml") or path in {
        "Cargo.lock", "rust-toolchain.toml", "README.md", "AGENTS.md", ".gitignore", "LICENSE"}


def freeze(root, destination, baseline):
    paths = sorted(set(subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root, text=True).splitlines()))
    candidate = {path: (root / path).read_bytes() for path in paths if source_file(path)}
    if any((root / path).read_bytes() != data for path, data in candidate.items()):
        raise RuntimeError("Source changed while freezing; use a new snapshot directory")
    reference = subprocess.check_output(["git", "rev-parse", baseline], cwd=root, text=True).strip()
    archive = subprocess.check_output(["git", "archive", "--format=zip", reference], cwd=root)
    with zipfile.ZipFile(io.BytesIO(archive)) as source:
        original = {path: source.read(path) for path in source.namelist() if source_file(path)}
    overlay = {path: data for path, data in candidate.items()
               if path.startswith(("crates/xde-bench/", "crates/xde-test/"))
               or path in {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"}}
    original.update(overlay)
    destination.mkdir(parents=True, exist_ok=False)
    for name, files in [("baseline", original), ("candidate", candidate)]:
        directory = destination / name
        directory.mkdir()
        with zipfile.ZipFile(destination / f"{name}-source.zip", "w", zipfile.ZIP_DEFLATED) as output:
            for path, data in files.items():
                target = directory / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                output.writestr(path, data)
        manifest = {path: hashlib.sha256(data).hexdigest() for path, data in sorted(files.items())}
        (destination / f"{name}-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        subprocess.run(["git", "init", "-q", str(directory)], check=True)
    (destination / "provenance.json").write_text(json.dumps({"baseline_engine": reference,
        "shared_overlay": sorted(overlay), "lock_sha256": hashlib.sha256(candidate["Cargo.lock"]).hexdigest()}, indent=2), encoding="utf-8")
    print(destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    parser.add_argument("--baseline", default="218a89f")
    args = parser.parse_args()
    freeze(Path.cwd(), args.destination.resolve(), args.baseline)
