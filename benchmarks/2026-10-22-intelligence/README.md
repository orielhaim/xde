# Adaptive intelligence measurements

`run.ps1` uses the existing scheduler probe and process runner. Pilot and Matrix
are disjoint cohorts. Three trials alternate arm order and reverse case order.
The baseline engine is the previous scheduler, rebuilt with identical fixture and
benchmark sources, compiler and lock. Preserve source snapshots and binary hashes.
Reference builds must use a separate Cargo target directory.

```powershell
benchmarks/2026-10-22-intelligence/run.ps1 -Section Pilot
benchmarks/2026-10-22-intelligence/run.ps1 -Section Matrix
```

The fixed reference uses four H1 connections for capped cases and one connection /
stream for H2/H3. It is a manual-policy reference, not another implementation of
adaptation. Matched baseline/current comparisons use identical user ceilings.

Shared egress paces application payload across independent fixture origins.
It models a controlled shared server bottleneck. It does not emulate network RTT,
packet loss, TCP/QUIC congestion or a last-mile link. Independent source caps model
heterogeneous servers. Raw CPU counters, all slow observations, actual protocols,
topologies, request lifecycles and read-back publication checks are retained.
The egress pacer reserves at most 1 MiB per request, then delivers normal protocol
chunks without zero-duration sleeps. This amortizes Windows timer granularity.
Cancelled requests can waste at most their outstanding reservation. Report this
bounded burst behavior when interpreting short shared-egress runs.


After a complete selected cohort:

```powershell
python benchmarks/2026-10-22-intelligence/analyze.py target/intelligence/selected
python benchmarks/2026-10-22-intelligence/report.py
python benchmarks/2026-10-22-intelligence/bundle.py
```

`report.py` requires all 90 observations. `bundle.py` preserves each earlier
cohort separately with its classification and SHA-256 manifest. Historical
records are not pooled into the selected policy's paired ratios. Raw records
contain controlled and residual CPU counters; residual CPU is not assumed to
be wholly external interference.


The matched reference combines the engine in `baseline/source.zip` with
`crates/xde-bench`, `crates/xde-test` and Cargo.lock from
`selected-binaries/source.zip`. Extract to a separate source directory, initialize
its Git metadata for build provenance, and build the release scheduler probe with
its own `CARGO_TARGET_DIR`. The selected engine is built directly from the latter
snapshot. Git initialization does not require a commit or remote operation.
Compiled source IDs distinguish the resulting engines; matching compiler, lock,
Compio revision, fixture and workload are checked independently.
