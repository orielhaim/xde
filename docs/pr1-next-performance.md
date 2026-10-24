# PR #1: pressure validity, scoped reclamation, and probe draining

## Confirmed issues

The Windows `write-probe` test coupled file correctness to scheduler interleaving.
Pressure tasks already acknowledged startup, but the test also required another
pressure poll during a short write batch. Ready writes can finish within one
executor turn. The original test passed locally; hosted CI supplied the failing
interleaving. This is not evidence of another IOCP progress defect.

The byte/region test still runs P, R and B with shared and separate files.
A separate test runs the same pressure future while controlled work remains
pending, waits for every pressure task to start, then requires additional polls
before releasing the work. It has no wall-clock pressure deadline. Production
measurements retain the no-overlap and expired-pressure labels. The pressure
expiry clock now starts at the measured write interval, after setup.

`Controller::reconcile` previously truncated every range when any job was
constrained. A deterministic regression creates contention at one origin and
observes an unrelated, fully granted H2 job's 32 MiB claim cut to 1 KiB.
Reclamation now requires that the affected job actually holds connections or
assignments above its grant. Replacement claims apply the fairness byte cap
only when that job is globally constrained. Queue behavior and acknowledged
range ownership remain unchanged. Existing connection counts are reused.

`WireBody::discard` suppressed transport errors. A real TCP server that sends
206 headers with `Content-Length: 1` and closes without a body reproduced a
successful probe. Discard now returns `Result`, and both probe drain call sites
propagate it through the existing deadline/failure path. TCP regressions cover
truncated 206 and 302 bodies. This prevents reporting an incomplete response as
reusable; it does not implement probe promotion.

## Matched H2 contention pilot

Hypothesis: scoping fairness turnover reduces request and connection churn when
some, but not all, jobs need to surrender resources.

Two jobs each download 1 GiB from independent local H2 fixtures. Each requests
fixed 2 connections x 1 stream; global limits are 3 connections and 3 assignments.
The fairness quantum remains 8 MiB. All six runs verify published bytes, H2 payload,
and observed peaks of 3 physical connections/assignments and one stream per
connection. This is buffered file publication, not durable storage throughput.
No network delay, loss, or shared-link bottleneck is emulated.

Three pairs alternate baseline/candidate order. All observations are retained.
CPU is diagnostic, with no percentage rejection gate. No builds, tests, or
profilers run during timing. The workload lasts approximately 1.7–1.9 seconds;
this is a focused pilot, not a broad performance distribution.

| Trial | Baseline MiB/s | Candidate MiB/s | Paired ratio | Requests B/C | Distinct payload connection generations B/C |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1084.64 | 1168.55 | 1.07736 | 256 / 94 | 194 / 13 |
| 1 | 1060.72 | 1181.76 | 1.11411 | 256 / 95 | 213 / 12 |
| 2 | 1073.62 | 1170.07 | 1.08984 | 256 / 86 | 198 / 12 |

The paired ratios have p10 1.07986, median 1.08984, and p90 1.10926.
These are descriptive interpolated percentiles of three observations.
Controller time falls from 6.62–6.90 ms to 1.75–2.05 ms.
Both jobs finish within 0.069 seconds of each other in every candidate trial;
this does not establish starvation bounds or general priority fairness.

There is a resource tradeoff. Process CPU includes client and fixtures.
Baseline process CPU seconds are [3.09375, 3.40625, 3.046875], versus candidate
[3.578125, 3.484375, 3.65625]. Thus this pilot improves completion time but does
not establish CPU efficiency. Process lifetime peak RSS is [46.3, 38.8, 40.7] MiB
versus [53.9, 47.0, 45.5] MiB. Peak RSS is not the engine memory-budget high-water.
Controlled aggregate CPU is 6.6–7.3% versus 8.4–8.7%; unattributed CPU is
7.8–8.7% versus 13.3–15.3%. Unattributed CPU is not proven external interference.
Raw idle, interval and final CPU counters remain in every record.

## Provenance and reproduction

Baseline engine commit: `053b46c`. Both frozen engines use identical benchmark,
fixture, dependency lock and toolchain files. The measured candidate includes
scoped fairness reclamation and the pressure fix, but predates the probe-drain
correction. Current final source and correctness logs are archived separately.
The unrelated staged CI edit is preserved.

Baseline source ID:
`11976997e965d4a5484fc91e218346319a9ec0396f932a33d4a1411741c35275`.
Candidate source ID:
`993b52d260ee9bb7ddfbf7d89c9542dd1f33b794c4b69d793dc0376d63de7745`.
The build records bind source manifests, lock hashes, compiler and executable SHA.

The host is Windows 11 Pro, Core Ultra 9 285K, 24 logical processors, approximately
64 GiB RAM, D: NTFS on Samsung 990 PRO. Rust is 1.99.1 MSVC. Compio remains pinned
to `3d9933787fc3e6c9b0e8ce9241b568cd40c862f6`. These hardware details describe
this workstation and do not imply a physical throughput ceiling.

Use `run-control.ps1 -CaseName h2-contended` with the two frozen executables,
then `analyze.py --expected-sources` with their recorded source IDs. The new case
is opt-in and does not enlarge the runner's default matrix.

[Raw evidence](../benchmarks/2026-10-24-next/raw.zip) contains 41 artifacts,
including raw trials, complete analysis, source ZIPs/manifests, build provenance,
red/green failures, final patch and quality logs. Archive SHA-256:
`55DD00C3AB5C1B40D57234F2FD236BE4C1F7CCC5F5688B57836C14FDC1CCDDC9`.
The earlier evidence archive remains unchanged.

## Validation and remaining work

Windows and WSL Linux each pass 168 workspace/all-feature tests, with one
intentionally ignored performance microbenchmark. All-target/all-feature Clippy,
doctests, formatting and whitespace checks pass. Hosted Windows CI has not been
rerun with these unpublished changes; macOS validation remains hosted-only.

This does not explain the earlier single-job adaptive H2 regression: without
contention, this fairness path does not run. Lifecycle timeline attribution is
still required. H1/H3 throughput and short-job latency were not remeasured here.

Reclamation still truncates all assignments of an over-grant job, and still uses
a byte quantum. Selective excess-resource draining and duration-based economics
remain open. The existing globally-constrained lookup also scans demand and
reservations; many-job cost of this newly qualified claim path is not measured.

Performance-aware transport selection, promotable probing, Windows kernel/socket
attribution, busy-connection sharing and load-aware placement remain separate
investigations. No executor, storage runtime, transport stack, offload policy,
security setting or destination batching change was made. Nothing was pushed or
merged; `archive/old-stage2` remains unchanged.
