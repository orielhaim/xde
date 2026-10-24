# PR #1 measurements

See [the performance report](../../docs/pr1-performance.md) for findings and
limitations. CPU attribution is diagnostic metadata. Keep slow observations;
functional correctness and matched build/workload identity determine eligibility.
The checked-in `raw.zip` contains 422 artifacts with per-file SHA-256 records.
It includes original labels, failed attempts and the immutable measured source
snapshots. `bundle.py` verifies archive contents and refuses to replace evidence.

`freeze.py` creates immutable baseline/candidate source trees with an identical
benchmark harness, crate manifests and lockfile. Use a separate, source-bound Cargo
target for each tree. Sharing a target across frozen paths has reused an engine
library from another checkout while rebuilding the benchmark executable.
`XDE_SOURCE_ID` alone cannot detect that mistake.

```powershell
python benchmarks/2026-10-23-pr1/build.py target/frozen/baseline target/build-baseline target/build-baseline.json
python benchmarks/2026-10-23-pr1/build.py target/frozen/candidate target/build-candidate target/build-candidate.json
```

`build.py` verifies the frozen manifest and binds each target to its source path,
manifest hash, compiler and build environment. It rejects existing unbound targets
and mismatched bindings. `record-build.py` requires that binding and records the
executable hash. Preserve both records with the trials.

`run-control.ps1` alternates the scheduler-probe binaries. Select a small set with
`-CaseName`, and supply the frozen `-Baseline` and `-Candidate` paths. Run without
concurrent builds, profilers or other benchmark sessions. Analyze with:

```powershell
python benchmarks/2026-10-23-pr1/analyze.py target/pr1-coherent/pilot --expected-sources target/pr1-coherent/expected-sources.json
python -m unittest discover -s benchmarks/2026-10-23-pr1 -p 'test_*.py'
```

The expected-sources JSON maps `baseline` and `candidate` to their recorded
`XDE_SOURCE_ID`. Analysis retains per-record eligibility, mismatched pair fields
and complete paired ratio distributions. It never promotes an old label in place.

`run-receive.ps1` compares lower-layer receive references with the existing
engine A/E paths. `-EngineOnly -Protocol h2` runs only the H2 engine comparison.
Raw QUIC is an isolation reference, not an equivalent HTTP workload.
`analyze-receive.py` checks byte verification, actual protocol and topology.
`run-fixture-scaling.ps1` and `analyze-fixture.py` compare one shared endpoint
against independent endpoints for two H3 jobs. This changes fixture layout and
origin scheduling, not engine implementation.

`run-affinity.ps1` applies temporary process CPU affinity and records readback.
It changes no permanent machine settings. `cpu_sets.py` inventories Windows CPU
classes. Affinity results remain diagnostic unless both arms also have matching
workload and build identity.

`netem.sh` runs in a new Linux network namespace, validates loopback RTT with ping
and preserves qdisc counters before and after every transfer. It requires root,
iproute2, ping and GNU time. It changes no host network interfaces. This is actual
packet delay; fixture response sleeps are not RTT emulation.

Profiling runs are diagnostic and separate from timing comparisons. Preserve
failed profiling attempts too. API read counts, UDP library counters, task polls
and kernel socket operations are different measurements.
