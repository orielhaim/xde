"""Compare fixture endpoint layouts, not engine implementations or physical limits."""
import argparse
import hashlib
import json
from pathlib import Path
from analyze import distribution, functional_reasons


def analyze(directory):
    records, pairs = [], {}
    for path in sorted(directory.glob("*.json")):
        if path.name == "analysis.json":
            continue
        raw = path.read_bytes()
        row = json.loads(raw.decode("utf-8-sig"))
        layout, trial = path.stem.rsplit("-", 1)
        reasons = functional_reasons(row, "h3")
        if row["shared_origin"] != (layout == "shared") or row["jobs"] != 2 or row["mode"] != "Single":
            reasons.append("fixture layout mismatch")
        identity = {key: row[key] for key in ["source_id", "compiler", "lock_blake3",
            "compio_revision", "build_profile", "build_target", "workload",
            "global_connections", "global_assignments"]}
        record = {"file": path.name, "sha256": hashlib.sha256(raw).hexdigest(),
            "layout": layout, "trial": int(trial), "eligible": not reasons,
            "reasons": reasons, "identity": identity, "mib_s": row["aggregate_mib_s"],
            "load": row["load"]}
        records.append(record)
        pairs.setdefault(int(trial), {})[layout] = record
    ratios = []
    for trial, arms in sorted(pairs.items()):
        if set(arms) != {"shared", "independent"}:
            continue
        a, b = arms["shared"], arms["independent"]
        if a["eligible"] and b["eligible"] and a["identity"] == b["identity"]:
            ratios.append({"trial": trial, "ratio": b["mib_s"] / a["mib_s"]})
    return {"schema_version": 1, "diagnostic_only": True, "records": records,
        "matched_pairs": ratios,
        "independent_over_shared": distribution([r["ratio"] for r in ratios]) if ratios else None}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = analyze(args.directory)
    (args.directory / "analysis.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(result["independent_over_shared"])
