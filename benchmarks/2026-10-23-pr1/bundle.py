"""Preserve selected raw cohorts, failed attempts and source/build provenance."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


COHORTS = ["pr1-baseline", "pr1-control", "pr1-effective", "pr1-final",
    "pr1-receive", "pr1-affinity", "pr1-natural", "pr1-profile",
    "pr1-coherent", "pr1-coherent-source", "pr1-delivery", "pr1-delivery-source"]
SUFFIXES = {".json", ".log", ".txt", ".patch", ".sha256", ".affinity", ".stderr", ".data"}


def bundle(root, output):
    if output.exists():
        raise FileExistsError(f"Refusing to replace evidence: {output}")
    files = {}
    for cohort in COHORTS:
        directory = root / "target" / cohort
        for path in sorted(directory.rglob("*")):
            relative = path.relative_to(directory)
            if not path.is_file() or any(part in {".git", "target", "baseline", "candidate"} for part in relative.parts[:-1]):
                continue
            if path.suffix in SUFFIXES or (path.parent == directory and path.suffix == ".zip"):
                files[path.relative_to(root).as_posix()] = path
    manifest = {}
    with zipfile.ZipFile(output, "x", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, path in sorted(files.items()):
            data = path.read_bytes()
            manifest[name] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            archive.writestr(name, data)
        archive.writestr("manifest.json", json.dumps({"schema_version": 1,
            "scope": "PR1 diagnostic and matched cohorts; preserve original labels",
            "files": manifest}, indent=2))
    with zipfile.ZipFile(output) as archive:
        for name, expected in manifest.items():
            if hashlib.sha256(archive.read(name)).hexdigest() != expected["sha256"]:
                raise RuntimeError(f"Archive verification failed: {name}")
    print(f"{len(files)} artifacts, {output.stat().st_size} bytes: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    bundle(Path.cwd(), args.output.resolve())
