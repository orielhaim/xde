# Scheduler implementation and measurements

This report records the 2026-10-21 scheduler source identified below. The current
policy is described in [scheduling.md](scheduling.md); its subsequent optimization
results are recorded separately in [adaptive intelligence results](adaptive-intelligence-results.md).

XDE now has two decision authorities: each job requests a topology, and the
engine allocator grants shared resources. User policy, desired topology, grants
and actual connections/assignments remain distinct. See [contracts and API](scheduling.md).

## Implementation

The job planner uses bounded, one-dimension experiments with aggregate useful-job
goodput. Three qualified windows establish a baseline and three evaluate a probe.
The full observed range qualifies the median comparison; a gain must also repay
setup cost before completion. Global grant changes invalidate comparisons rather
than triggering expansion. Local pressure, unsettled resources and short remaining
transfers suspend exploration. Fixed jobs never adapt.

FairShare uses discrete weighted max-min resource opportunities and normalized
useful-byte service to rotate indivisible resources. Pending range work counts as
service. Contended ranges yield at coarse byte quanta; unused demand is available
to other jobs. Queue independently limits active jobs, selects priority then FIFO,
and retains activation through publication. It does not manage bandwidth.

Origin-wide adaptive targets, their window history and the competing rebalance
controller were removed. Origins retain route/capability/backoff evidence. One
physical-open funnel owns acquisition; full generational reservation keys remain
charged until close acknowledgement. Busy connections belong to one job; idle
compatible connections can change owners.

Two integration defects were reproduced and corrected before final measurements:

- Whole-file claims hid stealable work from admission. Admission now requests an
  acknowledged yield. The old request owns its entire range until sink draining
  and terminal verification establish the actual written boundary, including an
  in-flight write that exceeds the requested cut. Only then can a successor claim
  the unwritten suffix.
- An arena-indexed reservation map could replace a still-closing generation with
  a new connection. Full generational keys preserve both physical reservations.
  A regression fails with one counted lease instead of two under the old map.

Success still requires acknowledged destination publication. Cancellation, pause,
stale work, range downgrade, digest verification and commit-failure regressions
remain in the validation suite. The destination batching and I/O runtime designs
were retained.

## Measurement method

These are loopback, buffered-file and read-back-verified publication workloads on
Windows 11 Pro build 26300.9711, Intel Core Ultra 9 285K (24 logical processors),
67,939,172,352 bytes RAM and a Samsung SSD 990 PRO 2TB NVMe (D: NTFS).
Final measurements were collected on 2026-10-21. Release compiler:
`rustc 1.99.1 (58cf8f9c3 2026-10-13)`, MSVC target, LLVM 23.1.1.
Compio is pinned to `3d9933787fc3e6c9b0e8ce9241b568cd40c862f6` on both platforms.

The final source identity is
`95685f275ed419039bd06b53ab263ed56e0f08597ed6bc14ad1f5186a8f03b3f`;
the lock BLAKE3 is
`48b2baffe979e8f3ebb2e8db802e30614b323902bc4abf1ce9221b0d94a236f5`.
Matched reference builds use the staged foundation snapshot with this same lock,
compiler, target and profile. This is a comparison of the complete scheduler
change, not an isolated estimate for an individual algorithm.

Competing arms alternate order across three trials; scaling/multi-job case order
also reverses. No builds, test suites or ETW profiling run alongside deliberate
timing trials. CPU and the unattributed residual are metadata, never selection
thresholds. Every functionally eligible slow observation remains included.
Three pairs are descriptive evidence; interpolated p10/p90 are not confidence
intervals. Job timing includes public start through committed completion; fixture
and client CPU share the controlled process. This does not measure durable device
throughput, genuine network RTT, packet loss or exact inbound bandwidth fairness.

## Final paired comparisons

All rates below are job/aggregate useful MiB/s in trial order. Each arm has
three observations; the ratio is calculated within each alternating pair.

| Workload | Reference rates | Current/adaptive rates | Paired ratios | Ratio p10 / median / p90 |
| --- | --- | --- | --- | --- |
| Adaptive/capped-h1 | 7.990, 7.989, 7.990 | 13.196, 13.204, 13.192 | 1.652, 1.653, 1.651 | 1.651 / 1.652 / 1.653 |
| Adaptive/h3 | 208.418, 213.108, 208.651 | 210.068, 203.718, 214.196 | 1.008, 0.956, 1.027 | 0.966 / 1.008 / 1.023 |
| Long/capped-h1 | 7.997, 7.997, 7.996 | 20.681, 20.761, 20.684 | 2.586, 2.596, 2.587 | 2.586 / 2.587 / 2.594 |
| Matched/h1 | 1006.210, 948.260, 835.001 | 894.685, 1015.491, 810.575 | 0.889, 1.071, 0.971 | 0.905 / 0.971 / 1.051 |
| Matched/h2 | 677.105, 797.970, 750.803 | 765.102, 590.267, 725.583 | 1.130, 0.740, 0.966 | 0.785 / 0.966 / 1.097 |

Matched H1/H2 use 4 GiB per transfer. Adaptive H3 also uses 4 GiB;
capped H1 uses 128 MiB (Adaptive) or 512 MiB (Long), with 8 MiB/s per
connection and a four-connection ceiling. The longer adaptive trials reach
four concurrent payload connections. Their completion times fall from about
64 seconds to about 24.7 seconds. Setup and exploration remain included.

The capped-source gain repeats in these three pairs. H3 shows no compelling
gain. The broad matched H1/H2 distributions include losses and gains; they
do not establish baseline equivalence or a consistent regression. In particular
the slow H2 observation remains included. H2 variance remains unresolved.
There is no matched-lock pre-change H3 baseline comparison in this final cohort.

## Fixed-topology scaling

Each trial transfers and verifies 4 GiB. Concurrent payload connections,
streams per connection and opening/closing reservation peaks are checked.
Connection identity counts may exceed concurrent counts after replacement.

| Topology | Individual job MiB/s | p10 / median / p90 |
| --- | --- | --- |
| h1-2c | 811.729, 879.044, 862.992 | 821.982 / 862.992 / 875.834 |
| h1-4c | 838.754, 1010.440, 724.362 | 747.240 / 838.754 / 976.102 |
| h1-8c | 596.346, 635.990, 778.939 | 604.275 / 635.990 / 750.349 |
| h2-2c | 746.029, 899.870, 780.301 | 752.884 / 780.301 / 875.956 |
| h2-2s | 611.573, 605.891, 663.269 | 607.028 / 611.573 / 652.930 |
| h2-4c | 797.121, 703.776, 710.935 | 705.208 / 710.935 / 779.884 |
| h2-4s | 568.144, 578.427, 662.778 | 570.201 / 578.427 / 645.907 |
| h2-8s | 409.197, 404.510, 378.269 | 383.517 / 404.510 / 408.259 |
| h3-2c | 213.896, 220.890, 204.742 | 206.573 / 213.896 / 219.491 |
| h3-2s | 210.367, 212.576, 217.044 | 210.809 / 212.576 / 216.150 |
| h3-4c | 216.432, 214.420, 214.022 | 214.102 / 214.420 / 216.029 |
| h3-8s | 196.827, 206.515, 206.988 | 198.765 / 206.515 / 206.893 |

These absolute topology observations are not paired topology speedups.
More streams/connections do not monotonically improve throughput. The data
does not identify a new runtime, storage or transport bottleneck.

## Multiple jobs

Two 64 MiB H1 downloads use 8 MiB/s per-connection pacing. Shared cases
have one global connection/assignment; independent cases have two. Queue
permits one active job. Weighted service uses Critical:Normal = 4:1.

| Case | Aggregate MiB/s, trials 0/1/2 | Job completion seconds by trial | Completed-request MiB at first completion |
| --- | --- | --- | --- |
| fair-shared | 7.951, 7.953, 7.964 | 15.094 / 16.095; 15.090 / 16.093; 15.072 / 16.071 | 64.00 / 56.00; 64.00 / 56.00; 64.00 / 56.00 |
| weighted-shared | 7.966, 7.972, 7.965 | 10.068 / 16.067; 10.053 / 16.056; 10.067 / 16.070 | 64.00 / 16.00; 64.00 / 16.00; 64.00 / 16.00 |
| fair-independent | 15.939, 15.934, 15.941 | 8.030 / 8.023; 8.024 / 8.032; 8.030 / 8.023 | 32.00 / 64.00; 64.00 / 32.00; 32.00 / 64.00 |
| queue-independent | 7.976, 7.977, 7.976 | 8.027 / 16.046; 8.024 / 16.045; 8.023 / 16.047 | 64.00 / 0.00; 64.00 / 0.00; 64.00 / 0.00 |

Service counts only requests wholly consumed by the first run-relative
completion; it is a lower bound, not interpolation of partially received
requests. It is useful for the bounded shared cases. A zero for an independent
whole-file request does not mean that job received no service.

Equal-priority shared jobs receive comparable bounded opportunities. Weighted
admission favors the high-priority job while the other receives service.
Independent sources retain concurrent aggregate throughput; Queue serializes
activation. This is evidence for these admission policies, not exact weighted
inbound network bandwidth control.

## Resource and controller costs

| Final trials | Controlled process CPU % | Total system CPU % | Unattributed residual % |
| --- | --- | --- | --- |
| Matched | 2.6532–5.3329 | 12.6494–31.8411 | 9.4479–28.8608 |
| Adaptive | 0.0528–7.1358 | 11.9787–24.6402 | 11.9259–18.4694 |
| Long | 0.0478–0.2980 | 11.6403–16.2347 | 11.5722–16.1870 |
| Scaling | 1.9543–6.8746 | 11.9954–21.4089 | 8.5543–14.5632 |
| Multi | 0.0607–0.2678 | 11.8310–19.5305 | 11.7580–19.4697 |

Across adaptive/single/multi-job probe rows, controller handling occupies
0.0024–0.0088% of makespan; this is elapsed
handling time with diagnostics enabled, excluding idle time/status queries,
and cannot isolate controller CPU. Process CPU above includes the fixture.
The residual is not proven unrelated interference. Raw idle windows, interval
spikes and CPU counters remain in each record; no independent per-core
contention diagnosis was collected during this final timing pass.

Planner state is 112 bytes/job and assignment control is 24 bytes/assignment
(the existing shared allocation, not a new telemetry allocation). Every probe
row reports zero transfer-budget usage after all jobs. This is not peak RSS.
Workers publish counters at 64 KiB boundaries; controller observations and
planner evaluations do not scale with the H3 fragment count. Raw records retain
payload rates, fragments/MiB, writes/MiB, wait/copy/write-await costs and counts.

## Raw evidence and cohort classification

The [raw evidence bundle](../benchmarks/2026-10-20-scheduler/raw.zip) contains
all retained cohorts, SHA-256 manifest, build/environment provenance,
historical labels, analyses and validation logs. The bundle manifest classifies
superseded implementations separately. Only `scheduler-final` supports the
tables above: 78 eligible timing rows plus one functional eight-connection
pilot. No old observation is promoted into this final cohort.

The original different-lock baseline is exploratory. Whole-file admission,
immediate range cutting and pre-ledger-fix builds are superseded. Their slow
rows and the topology-assertion failure are preserved. A functional row from
an unsafe superseded implementation is not evidence of production correctness.
The [runner and analyzer](../benchmarks/2026-10-20-scheduler/README.md) reproduce
the workload and complete distributions without CPU-based selection.


## Validation and limits

The final source passed 136/136 tests on native Windows and 136/136 on WSL Ubuntu,
using `cargo nextest run --workspace --all-features`. Formatting and workspace
Clippy across all targets/features with `-D warnings` passed. WSL uses kernel
`6.18.33.2-microsoft-standard-WSL2` and Rust 1.99.0. Linux tests execute the Linux
I/O backend inside WSL; they do not establish native Linux performance.

The deterministic policy model proves weighted opportunity counts of 400:100:100
over 600 equal-sized service turns at weights 4:1:1, 150:150 rotation of an
indivisible surplus, single-job full allocation, arrivals and unused-demand
borrowing. Property tests check grant conservation. Synthetic planner observations
cover beneficial/no-gain probes, H3 stream plateaus, noise, fixed and near-complete
jobs, global/local/source constraints, peer grant changes and reduction after path
changes. Integration tests verify range coverage, cancellation/Queue activation,
pause/drain/resume and published bytes on both operating systems.

Fairness is admission and useful-byte opportunity fairness under engine resource
contention. Equal connections do not guarantee equal network rates. Busy H2/H3
connections are not shared across jobs. Non-range responses cannot yield a suffix
and can hold a scarce slot until completion/cancellation/timeout. Coarse origin
grants use the primary origin; actual mirror opens check the routed origin, so
tight per-origin limits can conservatively restrict mirror expansion. Protocol
and kernel receive buffers are outside the engine transfer-memory budget.

The real multi-job performance matrix here uses H1. H2/H3 multi-job fairness,
heterogeneous mirrors, peak process RSS and large-job-count allocator costs are
not established by this pass. The reported controller timer is elapsed handling
time, not isolated controller CPU consumption.

Next validation should add genuine controlled shared-link delay/loss, heterogeneous
source rates, larger job counts and competing destinations. Measure fairness and
completion time together before tuning the experiment windows or adding shared
busy-connection ownership. These measurements do not justify another executor,
storage runtime, destination pipeline or QUIC tuning change.
