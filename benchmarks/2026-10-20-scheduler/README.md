# Scheduler measurements

Run from the repository root on Windows:

```powershell
cargo build -p xde-bench --release
benchmarks/2026-10-20-scheduler/run.ps1 -Section Adaptive -OutputDirectory target/scheduler-adaptive
benchmarks/2026-10-20-scheduler/run.ps1 -Section Long -OutputDirectory target/scheduler-long
benchmarks/2026-10-20-scheduler/run.ps1 -Section Multi -OutputDirectory target/scheduler-multi
benchmarks/2026-10-20-scheduler/run.ps1 -Section Scaling -OutputDirectory target/scheduler-scaling
python benchmarks/2026-10-20-scheduler/analyze.py target/scheduler-adaptive
```

Each section retains three trials by default. Competing arms alternate order;
case order reverses each round. Existing files are never overwritten. A failed
process keeps stdout/stderr and stops the section. No CPU utilization gate selects
samples. Inspect exceptional interference explicitly; keep slow observations.

`Fixed` compares a separately preserved binary against the current build.
`Matched` compares a separately built foundation reference with the current lock
file. Supply `-BaselineExecutable` or `-MatchedBaselineExecutable` as appropriate.
Use a separate Cargo target directory for each reference source tree. Executable
paths can also be supplied with `-CurrentExecutable` and `-SchedulerExecutable`.

The analyzer checks publication, payload protocol, actual fixed topology, including opening/closing reservation peaks,
completed request counts, workload equality and same-arm build/config identity.
Matched comparisons additionally require identical compiler, Compio, lock,
target and profile metadata. It preserves raw-file hashes, historical validity
labels, individual observations and paired ratios. It reports interpolated
p10/median/p90 plus min/max; with three pairs these are descriptive summaries.

CPU records include the fixture because client and fixture run in one process.
Unattributed CPU is a diagnostic residual, not proven external interference.
The first-completion service measure counts only requests wholly consumed before
the earliest run-relative completion. It is a lower bound: a partly completed
large request is not interpolated. Use it for bounded-quantum service, not to infer
instantaneous bandwidth from long requests.

These workloads measure buffered file writes and read-back-verified publication
on loopback. They do not measure durable device throughput or genuine network RTT.
