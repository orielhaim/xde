# Checkpoint correctness and H2 performance

## Confirmed defects

Periodic recovery checkpointing scheduled a timer for every completed range and
performed synchronous journal persistence on the controller thread. It could mark
completed bytes durable without flushing the payload file. Consequently, the file
A/E ladder included recovery metadata I/O, not only buffered positional writes.

Fresh downloads also carried empty resume evidence. A normal server validator
could be interpreted as a mismatch, triggering an unnecessary empty journal write
before useful transfer. Fresh jobs now have no resume evidence. Existing invalid
recovery metadata must be reset before any replacement writes.

A late credential callback could restart a destination already draining after
failure. Such callbacks are now accepted only during probing or transfer.

## Ownership and ordering

Each job owns one checkpoint transaction with a revision and operation identity.
Repeated progress coalesces behind one timer or running operation. The operation
captures an immutable verified-range snapshot, flushes payload data, then persists
that exact snapshot. Later verified ranges cannot enter its durable set.

File checkpoints use Compio's existing blocking pool. Custom destinations use
`flush(Data)`. Neither blocks the controller. Commit, reset and terminal drain wait
for checkpoint acknowledgement. The destination lease remains held throughout
failure cleanup. Completion from an old job or operation cannot mutate a new one.
Periodic completion forces a final dirty checkpoint. Public success still has only
the destination commit acknowledgement as its producer.

Reset acknowledgement activates the already prepared replacement plan, preserving
range capability when a resume mismatch does not require full-body fallback.
Version 3 journals reject older records that lack the payload-flush guarantee.

Recovery metadata is removed while the destination lock is held, before artifact
rename. Failure to remove it prevents publication. If the subsequent rename fails,
the partial data remains but its recovery map is gone, requiring redownload.

## Measurement provenance

The matched baseline is committed engine `661f48e` with the same benchmark harness,
diagnostics, manifests and lockfile as the candidate. Build records bind separate
Cargo targets to immutable source manifests, compiler and environment. A shared
Cargo target previously reused an engine library across frozen checkout paths.
The first new A/E diagnostic cohort is therefore provisional for engine attribution.
Historical raw labels have not been rewritten. The isolated follow-up and this
checkpoint pilot use separately bound targets.

The pilot uses Windows, Rust 1.99.1 MSVC, a Core Ultra 9 285K with 24 logical CPUs,
and the pinned Compio revision `3d9933787fc3e6c9b0e8ce9241b568cd40c862f6`.
The 4 GiB file is on C: NTFS, Samsung 990 PRO with Heatsink, not project drive D:.
The fixture uses H2 prior knowledge on loopback, one physical connection and one
stream. Each arm has three alternating observations per durability mode, full
publication readback and protocol/topology validation. No profiler or builds run
during trials. CPU attribution remains diagnostic; slow eligible runs are retained.
This is not a durable-device bandwidth test or a network RTT experiment.

The [evidence archive](../benchmarks/2026-10-24-next/checkpoint-evidence.zip)
contains 121 artifacts, including raw trials, analyses, source snapshots, build
bindings and validation logs. SHA-256:
`c56650c5afa650f5eb716ac892bf3270987ab3a265cfc5d1869a45ff131e50c1`.

## Matched pilot results

All twelve observations pass matched identity, actual H2 1c x 1s and published
4 GiB readback checks. Each has sixteen 256 MiB requests, 16 receive units and
16 file operations per MiB, no measured memory/capacity waiting and no batch copy.

| Mode | Baseline MiB/s, trial order | Candidate MiB/s, trial order | Paired ratios | Ratio p10 / median / p90 |
| --- | --- | --- | --- | --- |
| Relaxed | 734.0, 802.9, 741.3 | 786.3, 858.7, 868.8 | 1.071, 1.069, 1.172 | 1.070 / 1.071 / 1.152 |
| Periodic | 910.4, 871.7, 768.7 | 769.8, 829.6, 708.7 | 0.846, 0.952, 0.922 | 0.861 / 0.922 / 0.946 |

Relaxed improves in all three pairs, but three pairs cannot establish a broad
speedup. Candidate first-useful-payload times are 6.1-10.6 ms versus baseline
11.5-13.6 ms. The removed fresh-job journal reset is a confirmed mechanism; it
cannot alone explain the entire throughput difference.

Periodic is slower in all three pairs. The candidate now flushes the payload
before recovery publication and waits for the final checkpoint. Time after the
last sink completion is 125/395/474 ms versus baseline 7/7/5 ms. Payload-rate
ratios are 0.863/1.031/1.003. Thus the added completion tail explains a substantial
part of the job-rate difference, while the first pair also has a slower payload
interval. These are different correctness guarantees, not an equivalent durability
speed comparison. No slow observation was discarded.

Across all runs, file-write awaits total 2.77-3.54 seconds. Inter-request gaps
inside the payload spans total 23-36 ms, too small to explain broad historical
H2 variance. These spans include sink waits and are not network-active intervals.
All 65,536 writes per run are first-poll Pending. First-poll totals are
1.47-1.66 seconds. Neither counter identifies kernel-only service time.

Total system CPU ranges 18.6-32.4%, controlled process CPU 4.3-6.0% and
unattributed residual 13.8-26.8% across these trials. The residual is not
automatically unrelated interference. CPU summaries and interval counters remain
in the raw files. Client and fixture
share the controlled process; separate CPU attribution is unavailable in this
pilot. Peak RSS and checkpoint operation latency were not independently measured.
Checkpoint control state and pending I/O are bounded per job; this is not a
measured process-memory claim.

## Validation

Windows and WSL Linux each pass 178 tests, with one intentionally skipped
microbenchmark. Workspace all-target/all-feature Clippy and doctests pass on both.
Windows MSRV 1.98, diagnostics-disabled compilation and formatting pass. Hosted CI
has not been rerun. Regressions cover checkpoint flush failure, immutable snapshots,
coalescing, pending-checkpoint cancellation and commit, destination lease retention,
invalid resume reset, stale acknowledgements and late credential callbacks.

The old-engine regression fails waiting for a periodic destination flush; its old
teardown then aborts. The current regression completes normally. This is evidence
of the missing flush contract, not a clean old-engine test exit.

## Remaining attribution limits

Isolated 4 GiB write-only P/R trials achieved roughly 3.3 GiB/s with mostly
microsecond latencies. Large H2 file-write await tails were not reproduced there.
A pending write's await includes I/O completion and task resumption; the current
instrumentation cannot separate those costs. First-poll time also includes
submission and possible CPU preemption. These observations do not establish a
physical ceiling or prove that all H2 variance came from checkpointing.

The next transport investigation should use the corrected checkpoint path and
source-bound binaries. Keep protocol receive, buffered writes and recovery
publication costs separate before changing transport or runtime architecture.
