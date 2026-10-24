"""Lower-layer references have different work; never pool them as paired downloads."""
import argparse
import hashlib
import json
from pathlib import Path
from analyze import distribution


def analyze(directory):
    records, groups = [], {}
    for path in sorted(directory.glob("*.json")):
        if path.name == "analysis.json":
            continue
        raw = path.read_bytes()
        row = json.loads(raw.decode("utf-8-sig"))
        case, trial = path.stem.rsplit("-", 1)
        reasons = []
        if case.startswith("engine-"):
            trial_row = row["trials"][0]
            if trial_row["validity"]["status"] != "valid":
                reasons.append(trial_row["validity"])
            arm = trial_row["xde"].get("Ok")
            if arm is None:
                reasons.append(trial_row["xde"])
                records.append({"file": path.name, "eligible": False, "reasons": reasons})
                continue
            t = arm["transport"]
            protocol = {"h1": "http/1.1", "h2": "h2", "h3": "h3"}[row["workload"]["protocol"]]
            if t["payload_protocols"] != [protocol] or t["response_validated_bytes"] != arm["bytes"]:
                reasons.append("protocol or response validation mismatch")
            if t["peak_payload_connections"] != 1 or t["peak_streams_per_connection"] != 1 or t["payload_connections"] != 1:
                reasons.append("single-connection single-stream topology mismatch")
            if case == "engine-e" and arm["artifact_verified_bytes"] != arm["bytes"]:
                reasons.append("missing artifact readback")
            metrics = {"mib_s": arm["job_mib_s"], "payload_mib_s": arm["payload_mib_s"],
                       "chunks_per_mib": t["receive_chunks"] / (arm["bytes"] / (1 << 20)),
                       "writes_per_mib": t["destination_operations"] / (arm["bytes"] / (1 << 20)),
                       "file_write_await_seconds": t["file_write_await_seconds"],
                       "memory_wait_seconds": t["memory_wait_seconds"]}
            load = arm["load"]
            size = arm["bytes"]
        else:
            size = row["bytes"]
            if row["verified_bytes"] != size:
                reasons.append("payload verification mismatch")
            mib = size / (1 << 20)
            metrics = {"mib_s": row["mib_s"], "payload_mib_s": mib / row["payload_seconds"],
                       "chunks_per_mib": row["returned_chunks"] / mib,
                       "successful_receive_returns_per_mib": row["receive_calls"] / mib,
                       "peak_process_working_set_bytes": row.get("peak_process_working_set_bytes")}
            for side in ["client", "server"]:
                cpu = row[side]
                if cpu:
                    metrics.update({f"{side}_cpu_fraction": cpu["seconds"] / row["seconds"],
                                    f"{side}_cycles_per_byte": cpu["cycles"] / size})
            load = row["load"]
        record = {"file": path.name, "sha256": hashlib.sha256(raw).hexdigest(),
                  "case": case, "trial": int(trial), "bytes": size,
                  "source_id": row["source_id"], "eligible": not reasons, "reasons": reasons,
                  "metrics": metrics, "load": load}
        records.append(record)
        if record["eligible"]:
            groups.setdefault(case, []).append(record)
    summary = {}
    for case, rows in groups.items():
        keys = set.intersection(*(set(r["metrics"]) for r in rows))
        summary[case] = {key: distribution([r["metrics"][key] for r in rows])
                         for key in sorted(keys) if all(r["metrics"][key] is not None for r in rows)}
    return {"schema_version": 1, "records": records, "distributions": summary,
            "limitations": ["raw QUIC is not equivalent H3 work", "thread CPU windows differ", "verification is in the lower-layer receive interval", "no CPU utilization rejection"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = analyze(args.directory)
    (args.directory / "analysis.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(len(result["records"]), "records", sum(r["eligible"] for r in result["records"]), "eligible")
    for case, metrics in result["distributions"].items():
        print(case, metrics["mib_s"])
