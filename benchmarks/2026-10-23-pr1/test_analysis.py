import copy
import json
import tempfile
import unittest
from pathlib import Path
from analyze import analyze


def row(arm, speed):
    return {"workload": {"protocol": "h2", "bytes": 1048576}, "jobs": 1, "mode": "Single",
            "shared_origin": False, "weighted": False, "queue": None,
            "global_connections": 1, "global_assignments": 1, "compiler": "same compiler",
            "compio_revision": "same pinned revision", "lock_blake3": "same lock",
            "build_profile": "release", "build_target": "same target", "os": "same OS",
            "source_id": arm, "published_bytes": 1048576, "engine_memory_used_after_jobs": 0,
            "aggregate_mib_s": speed, "controller_seconds": .001, "allocation_passes": 1,
            "completion_seconds": [1 / speed], "load": {"unattributed_percent": 99},
            "transport": {"response_validated_bytes": 1048576, "assignments_started": 1,
                          "assignments_finished": 1, "missing_protocol_evidence": False,
                          "payload_protocols": ["h2"], "peak_reserved_connections": 1,
                          "peak_assignments": 1, "peak_payload_connections": 1,
                          "peak_streams_per_connection": 1}}


class AnalysisTests(unittest.TestCase):
    def run_rows(self, rows, expected=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            for name, record in rows.items():
                (path / name).write_text(json.dumps(record), encoding="utf-8")
            return analyze(path, expected)

    def test_slow_and_high_cpu_observations_remain_in_paired_distribution(self):
        result = self.run_rows({"h2-case-0-baseline.json": row("base", 100),
                                "h2-case-0-candidate.json": row("candidate", 20),
                                "h2-case-1-baseline.json": row("base", 100),
                                "h2-case-1-candidate.json": row("candidate", 110)})
        self.assertEqual(result["comparisons"]["h2-case"]["ratios"]["values"], [.2, 1.1])
        self.assertTrue(all(r["eligible"] for r in result["records"]))

    def test_unmatched_builds_reject_pair_without_fabricating_functional_failure(self):
        for field in ["build_profile", "build_target", "os", "lock_blake3"]:
            with self.subTest(field=field):
                candidate = row("candidate", 100)
                candidate[field] = "different"
                result = self.run_rows({"h2-case-0-baseline.json": row("base", 100),
                                        "h2-case-0-candidate.json": candidate})
                self.assertEqual(result["comparisons"], {})
                self.assertTrue(all(r["functional_eligible"] for r in result["records"]))
                self.assertEqual(set(result["pair_diagnostics"][0]["identity_differences"]), {field})

    def test_unexpected_artifact_and_topology_are_rejected(self):
        records = {"h2-case-0-baseline.json": row("base", 100),
                   "h2-case-0-candidate.json": row("wrong-source", 100)}
        result = self.run_rows(records, {"baseline": "base", "candidate": "expected"})
        self.assertEqual(result["comparisons"], {})
        candidate = copy.deepcopy(records["h2-case-0-candidate.json"])
        candidate["transport"]["peak_streams_per_connection"] = 2
        result = self.run_rows({"h2-case-0-candidate.json": candidate})
        self.assertFalse(result["records"][0]["functional_eligible"])


if __name__ == "__main__":
    unittest.main()
