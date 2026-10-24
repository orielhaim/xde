"""Bind an executable to the verified frozen source and Cargo build metadata."""
import argparse
import hashlib
import json
from pathlib import Path


def record(source, cargo_target, executable):
    manifest_path = source.parent / f"{source.name}-manifest.json"
    binding = json.loads((cargo_target / "build-source.json").read_text(encoding="utf-8"))
    if (binding["source"] != str(source)
            or binding["manifest_sha256"] != hashlib.sha256(manifest_path.read_bytes()).hexdigest()):
        raise RuntimeError("Cargo target/source binding mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name, expected in manifest.items():
        if hashlib.sha256((source / name).read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Frozen source changed: {name}")
    matches = []
    for path in (cargo_target / "release/build").glob("xde-bench-*/output"):
        lines = path.read_text(encoding="utf-8").splitlines()
        references = [line.removeprefix("cargo:rerun-if-changed=") for line in lines if line.startswith("cargo:rerun-if-changed=")]
        if not any(Path(name).resolve() == source / "Cargo.toml" for name in references):
            continue
        env = dict(line.removeprefix("cargo:rustc-env=").split("=", 1)
                   for line in lines if line.startswith("cargo:rustc-env="))
        matches.append(env)
    if len(matches) != 1:
        raise RuntimeError(f"Expected one matching release build output, found {len(matches)}")
    return {"source": str(source), "target_binding": binding, "executable": str(executable),
            "executable_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
            "lock_sha256": manifest["Cargo.lock"], "build": matches[0]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("cargo_target", type=Path)
    parser.add_argument("executable", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    data = record(args.source.resolve(), args.cargo_target.resolve(), args.executable.resolve())
    args.output.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(data["build"]["XDE_SOURCE_ID"])
