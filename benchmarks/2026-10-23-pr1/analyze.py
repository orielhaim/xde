"""Keep all paired observations; CPU attribution is descriptive metadata."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def distribution(values):
    ordered = sorted(values)
    def percentile(p):
        i = (len(ordered) - 1) * p
        lo = int(i)
        return ordered[lo] + (ordered[min(lo + 1, len(ordered) - 1)] - ordered[lo]) * (i - lo)
    return {"values": values, "min": min(values), "p10": percentile(.1),
            "median": statistics.median(values), "p90": percentile(.9), "max": max(values)}


def functional_reasons(row, label=""):
    t = row["transport"]
    reasons = []
    expected = row["workload"]["bytes"] * row["jobs"]
    if row["published_bytes"] != expected or t["response_validated_bytes"] != expected:
        reasons.append("payload/publication byte mismatch")
    if row["engine_memory_used_after_jobs"] != 0:
        reasons.append("retained transfer memory")
    if t["assignments_started"] != t["assignments_finished"]:
        reasons.append("unsettled assignments")
    protocol = {"h1": "http/1.1", "h2": "h2", "h3": "h3"}[row["workload"]["protocol"]]
    claimed_protocol = label.split("-", 1)[0]
    if claimed_protocol in ["h1", "h2", "h3"] and row["workload"]["protocol"] != claimed_protocol:
        reasons.append("case protocol label mismatch")
    if t["missing_protocol_evidence"] or t["payload_protocols"] != [protocol]:
        reasons.append("payload protocol mismatch")
    if t["peak_reserved_connections"] > row["global_connections"] or t["peak_assignments"] > row["global_assignments"]:
        reasons.append("global capacity exceeded")
    if row["mode"] == "Single" and (t["peak_streams_per_connection"] != 1
            or t["peak_payload_connections"] > row["jobs"] or t["peak_assignments"] > row["jobs"]):
        reasons.append("single topology mismatch")
    return reasons


def analyze(directory, expected_sources=None):
    records = []
    grouped = {}
    for path in sorted(directory.glob("*.json")):
        if path.name == "analysis.json":
            continue
        raw = path.read_bytes()
        row = json.loads(raw.decode("utf-8-sig"))
        case, trial, arm = path.stem.rsplit("-", 2)
        reasons = functional_reasons(row, case)
        functional_eligible = not reasons
        if expected_sources and row["source_id"] != expected_sources[arm]:
            reasons.append("unexpected source artifact")
        identity = {key: row[key] for key in ["workload", "jobs", "mode", "shared_origin", "weighted", "queue", "global_connections", "global_assignments", "compiler", "compio_revision", "lock_blake3", "build_profile", "build_target", "os"]}
        affinity_path = Path(str(path) + ".affinity")
        affinity = json.loads(affinity_path.read_text(encoding="utf-8-sig")) if affinity_path.exists() else None
        identity["processor_affinity"] = affinity["actual_affinity"] if affinity else None
        if affinity and affinity["actual_affinity"] != affinity["requested_affinity"]:
            reasons.append("processor affinity readback mismatch")
        record = {"file": path.name, "sha256": hashlib.sha256(raw).hexdigest(), "case": case,
                  "trial": int(trial), "arm": arm, "functional_eligible": functional_eligible, "eligible": not reasons, "reasons": reasons,
                  "identity": identity, "source_id": row["source_id"], "throughput": row["aggregate_mib_s"],
                  "controller_seconds": row["controller_seconds"], "allocation_passes": row["allocation_passes"],
                  "completion_seconds": row["completion_seconds"], "load": row["load"]}
        records.append(record)
        grouped.setdefault(case, {}).setdefault(int(trial), {})[arm] = record
    for arm in {r["arm"] for r in records}:
        if len({r["source_id"] for r in records if r["arm"] == arm}) != 1:
            for record in records:
                if record["arm"] == arm:
                    record["eligible"] = False
                    record["reasons"].append("mixed source artifacts within arm")
    comparisons, pair_diagnostics = {}, []
    for case, trials in grouped.items():
        pairs = []
        for trial, arms in sorted(trials.items()):
            if set(arms) != {"baseline", "candidate"}:
                continue
            a, b = arms["baseline"], arms["candidate"]
            differences = {key: [a["identity"][key], b["identity"][key]] for key in a["identity"]
                           if a["identity"][key] != b["identity"][key]}
            matched = a["eligible"] and b["eligible"] and not differences
            pair_diagnostics.append({"case": case, "trial": trial, "eligible": matched,
                                     "identity_differences": differences,
                                     "record_reasons": a["reasons"] + b["reasons"]})
            if matched:
                pairs.append((trial, a, b))
        if pairs:
            comparisons[case] = {"paired_trials": [p[0] for p in pairs],
                "ratios": distribution([b["throughput"] / a["throughput"] for _, a, b in pairs]),
                **{arm: {key: distribution([pair[index][key] for pair in pairs])
                         for key in ["throughput", "controller_seconds", "allocation_passes"]}
                   for arm, index in [("baseline", 1), ("candidate", 2)]}}
    return {"schema_version": 2, "methodology": "workstation_v1", "records": records,
            "pair_diagnostics": pair_diagnostics, "comparisons": comparisons}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--expected-sources", type=Path)
    args = parser.parse_args()
    expected = json.loads(args.expected_sources.read_text(encoding="utf-8-sig")) if args.expected_sources else None
    result = analyze(args.directory, expected)
    (args.directory / "analysis.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"{len(result['records'])} records, {sum(r['eligible'] for r in result['records'])} eligible")
    print(f"{sum(p['eligible'] for p in result['pair_diagnostics'])} matched engine-comparison pairs")
    for case, comparison in result["comparisons"].items():
        print(case, comparison["ratios"])
