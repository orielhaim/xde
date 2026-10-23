# Adaptive intelligence: validation and measurements

The current system retains two authorities: job-local desired topology and engine-wide grants. Transport, destination batching and publication ownership are unchanged. See [contracts](scheduling.md) and [design](adaptive-intelligence.md).

## Decision quality

The previous policy spent three 750 ms windows on each baseline and probe, grew one resource at a time, and lacked routine contraction. The replacement uses aggregate 250 ms observations, two to six samples per decision, bounded direct search, geometric productive growth and refined downward trials. Unknown H2/H3 endpoints begin with one stream; H1 range transfers of at least 16 MiB request two connections immediately. Reliable endpoint history can justify two multiplexed streams. Limits and grants remain authoritative.

A separate coarse progress meter sizes range work to about one second per lane. Optimization qualification cannot erase that sizing evidence. Initial pieces are 8 MiB for H1 and 32 MiB for H2/H3. Local pressure blocks expansion but permits contraction. A topology change is not a connection grant.

Growth must improve both useful job progress and priority-weighted engine progress beyond descriptive noise bands. This prevents a job from interpreting bandwidth stolen from a peer as newly created capacity. Contraction preserves aggregate progress while improving useful progress per resource. Rejected moves back off for 4–128 seconds; accepted moves suppress immediate reversal. Unknown growth requires six seconds of remaining work, recently productive growth one second, and contraction 1.5 seconds. Stream growth does not pay a fictitious connection handshake.

The allocator protects a first opportunity and normalized byte-service rotation. Surplus uses weighted diminishing utility with a bounded marginal-productivity factor, never raw speed. Unknown evidence remains exploratory. Fixed intent and Queue semantics remain manual. Grants, phase changes and peer membership qualify comparisons; the planner cannot respond to denied grants by expanding demand without evidence.

## Deterministic references

Each profile transfers 4096 MiB with a controllable clock and 25 ms connection-growth setup. The oracle knows the synthetic curve in advance. These references do not model real TCP/QUIC, filesystem cost or all wasted bytes.

| Model | Selected completion / oracle (s) | Ratio | Final topology |
| --- | ---: | ---: | --- |
| H1 linear, 8 MiB/s/connection, cap four | 129.250 / 128.000 | 1.010 | 4c×1s |
| Flat 32 MiB/s | 128.100 / 128.000 | 1.001 | 1c×1s |
| Nonlinear multiplexed, peak at three connections | 15.875 / 13.653 | 1.163 | 3c×1s |
| Flat H3 with stream overhead | 17.300 / 17.067 | 1.014 | 1c×1s |

Useful four-way H1 concurrency appears at 0.775 s in the linear model. Staggered shared-link arrivals converge to two total connections, matching that model's resource optimum; independent jobs can each reach useful four-way concurrency. Tests cover priorities, arrivals, constraints, starvation protection, Queue, Fixed, bounded evidence, path changes and resource conservation. These examples establish behavior, not universal optimality.

## Measurement method

Windows 11 Pro build 26300.9711, Intel Core Ultra 9 285K (24 logical processors), 67,939,172,352 bytes RAM, Samsung 990 PRO 2 TB, D: NTFS. Release Rust 1.99.1 MSVC / LLVM 23.1.1. Compio revision `3d9933787fc3e6c9b0e8ce9241b568cd40c862f6` includes the upstream IOCP progress fix. Both engine versions use the same current lock, fixture and harness; the preserved engine snapshot is rebuilt in a separate Cargo target directory.

Three matched trials alternate baseline/current/fixed order and reverse case order. All functionally eligible observations remain, including slow rows. CPU attribution is diagnostic metadata, without a utilization rejection threshold. Publication read-back, actual protocol, topology, resource ceilings and zero post-job engine memory are checked independently. Three pairs support descriptive comparisons, not confident population-level significance claims.

Shared-egress tests use application pacing across separate fixture origins, not a real shared network link. Each request reserves up to 1 MiB; cancellation can waste that credit. Fixture response delay is not network RTT. File results concern buffered writes and verified publication, not durable media throughput. Multi-job makespan and individual completion times must be considered together.

## Validation and limitations

Windows and WSL Ubuntu each pass 151 workspace tests with all features, plus all-target/all-feature Clippy with warnings denied. WSL uses Rust 1.99.0 and kernel 6.18.33.2-microsoft-standard-WSL2. Native Windows tests exercise IOCP; WSL tests exercise Linux. Linux performance has not been measured here.

Planner state is fixed-size; no new fragment timestamps, telemetry allocations or controller messages were added. Controller timing excludes sleep and is not process CPU. Busy connections remain job-exclusive; exact inbound bandwidth fairness, per-core saturation detection and automatic shared-bottleneck identification are not implemented. Primary measurements and overhead summaries follow below.


## Review limits

Joint engine progress prevents a simple bandwidth-theft feedback loop, but it is
not causal isolation: simultaneous peer topology experiments can contaminate an
aggregate comparison. The planner does not serialize all jobs' experiments.
Adaptive claim sizing divides by desired lanes; when grants are lower this can
produce smaller claims than the effective per-active-lane horizon. These are
remaining control-quality limits, not weakened publication or resource ownership
contracts. Neither is hidden behind an assertion of real-world optimality.

Early implementation cohorts are retained separately. The first pacer used
64 KiB timer sleeps, which imposed a Windows timer-granularity ceiling; that
fixture cannot support concurrency conclusions. Later cohorts exposed missing
first-claim grants, a staggered shared-link connection arms race, repeated small
requests after qualification resets, and cold multiplexed stream overhead.
Their raw records remain diagnostic or superseded-policy evidence. They are not
pooled with the selected source's matched comparisons.


The H2 selected trials use 23–25 requests versus 16 for the previous scheduler;
all deliver 16 receive fragments/MiB. Selected controller handling is about
1.1 ms for the entire transfer versus 0.4–0.6 ms previously. This establishes
extra request/control work but does not attribute the throughput difference to
those milliseconds. CPU attribution and paired fixed results remain in the raw
records; no high/low CPU or slow row was discarded. Identifying a persistent
H2 performance mode requires more evidence than these three comparisons.


Reproduction uses `benchmarks/2026-10-22-intelligence/run.ps1`, `analyze.py`,
`report.py` and `bundle.py`. Primary baseline executable SHA-256:
`956a97b2dcf2e9d2e5ae569e8412eb622266b3205abf57e652153a053e97ed90`.
Selected executable SHA-256:
`23af647ab2ce8238983d94ad136310f5cc5b25ab7c4ca2642ed69ddc58292a4c`.
Selected compiled source ID:
`f03cdecb74bf599a3c8dc17dddd43d887bccaf7fc0e0e08ad8234511821e3e2d`.
Lock BLAKE3:
`a5468deb3573e6af9100324c736bbe4ad5797ba3d4b6cbe558581a64b2d7f5e3`.
Source snapshots and raw-file SHA manifests accompany the measurements.


Fixed mode is a current-engine manual reference in this cohort, not a paired
old-Fixed/new-Fixed regression experiment. Its policy correctness is covered by
tests, but this dataset alone cannot establish zero fixed-path performance
regression. Likewise, priority and Queue behavior are deterministically validated;
these new real trials emphasize equal-priority two-job workloads rather than a
full real priority sweep. No test here validates a genuine high-RTT/loss path,
independent Internet bottleneck discovery, or per-core contention inference.


The planner grows from 112 to 384 bytes/job (+272 bytes); assignment control
remains 24 bytes. The policy does not retain per-fragment histories. An earlier
single-pair 128×1 MiB H1 diagnostic measured controller handling near 9% of
makespan in both versions. That superseded-policy observation is retained as a
warning against calling control overhead universally negligible; it is not a
selected-policy many-job performance result.

## Selected matched results

Aggregate useful MiB/s; all three observations are shown in trial order. Ratios are paired current/baseline throughput, not ratios of independent medians.

| Workload | Previous adaptive | Selected adaptive | Fixed reference | Paired ratios |
| --- | --- | --- | --- | --- |
| h1-cold | 13.208, 13.228, 13.205 | 25.634, 26.632, 26.245 | 31.825, 31.829, 31.826 | 1.941, 2.013, 1.987 |
| h1-heterogeneous | 15.783, 15.742, 15.630 | 15.386, 15.440, 15.587 | 15.969, 15.969, 15.970 | 0.975, 0.981, 0.997 |
| h1-long | 20.833, 20.707, 20.782 | 30.132, 29.775, 30.005 | 31.952, 31.953, 31.957 | 1.446, 1.438, 1.444 |
| h1-small | 61.093, 61.307, 63.614 | 30.961, 58.975, 61.228 | 64.348, 56.809, 67.079 | 0.507, 0.962, 0.962 |
| h2-heterogeneous | 14.355, 14.462, 14.897 | 14.537, 14.419, 14.331 | 14.328, 14.371, 14.431 | 1.013, 0.997, 0.962 |
| h2-loopback | 880.415, 858.232, 969.247 | 841.360, 841.605, 840.249 | 759.848, 839.864, 888.882 | 0.956, 0.981, 0.867 |
| h2-shared | 31.380, 31.333, 31.336 | 30.983, 30.868, 31.124 | 31.875, 31.892, 31.830 | 0.987, 0.985, 0.993 |
| h3-heterogeneous | 14.696, 14.946, 14.825 | 14.314, 14.491, 14.700 | 15.005, 15.068, 14.673 | 0.974, 0.970, 0.992 |
| h3-loopback | 211.881, 218.561, 215.964 | 214.814, 208.384, 213.372 | 216.347, 214.018, 218.483 | 1.014, 0.953, 0.988 |
| h3-shared | 15.854, 15.836, 15.349 | 15.046, 15.040, 15.117 | 15.710, 15.920, 15.932 | 0.949, 0.950, 0.985 |

Full p10/median/p90/min/max, individual completion times, payload intervals, CPU attribution and request records are in `selected/analysis.json` and raw JSON. Three observations are not enough to distinguish small changes confidently.

## Observed control cost

| Workload | Controller handling % (all selected trials) | Planner evaluations | Topology changes |
| --- | --- | --- | --- |
| h1-cold | 0.021, 0.022, 0.019 | 19, 18, 18 | 3, 1, 1 |
| h1-heterogeneous | 0.025, 0.019, 0.019 | 96, 104, 95 | 8, 6, 8 |
| h1-long | 0.020, 0.020, 0.022 | 63, 67, 66 | 3, 5, 5 |
| h1-small | 0.526, 0.779, 0.709 | 0, 0, 0 | 0, 0, 0 |
| h2-heterogeneous | 0.014, 0.014, 0.014 | 45, 45, 45 | 4, 4, 4 |
| h2-loopback | 0.022, 0.022, 0.023 | 18, 17, 18 | 0, 0, 0 |
| h2-shared | 0.029, 0.025, 0.026 | 62, 64, 62 | 8, 8, 8 |
| h3-heterogeneous | 0.015, 0.014, 0.013 | 43, 42, 42 | 4, 4, 4 |
| h3-loopback | 0.013, 0.013, 0.013 | 73, 75, 73 | 10, 8, 10 |
| h3-shared | 0.019, 0.024, 0.016 | 62, 62, 64 | 9, 10, 8 |

Planner state: 384 bytes/job; assignment control: 24 bytes/assignment. These are structure sizes, not total job/transport RSS.


## Interpretation and next stage

The capped H1 cold-start paired median is 1.987× and the 512 MiB case is
1.444×. Fixed four-connection mode remains faster. The selected policy therefore
improves discovery substantially without matching an oracle that already knows
four connections will help. The 1 MiB H1 ratios are 0.507, 0.962, 0.962; the first
selected trial spends about 16.1 ms in body-next pending time versus 0.57 ms for
its baseline, while payload consumption intervals are 1.35 versus 1.26 ms. There
are no planner evaluations or topology changes. The delay is observed, but its
kernel/runtime cause is not established; it is retained, not discarded as noise.

H2 loopback paired ratios are 0.956, 0.981, 0.867 (median 0.956). Shared H2 is
0.985–0.993. H3 loopback is mixed (0.953–1.014, median 0.988), while shared H3
is consistently lower (0.949–0.985, median 0.950). H3 heterogeneous ratios are
0.970–0.992. There is no evidence for a universal speedup or for more H3 streams
being beneficial. Repeated exploration and bounded drain/pacer reservations have
real costs; this dataset does not isolate each cost causally. The selected
algorithm improves important H1 workloads but still explores too expensively on
these flat multiplexed profiles. Small differences with only three pairs should
not be presented as precisely estimated long-run effects.

The heterogeneous H1 selected faster-source completions are 8.683, 10.922 and
8.692 seconds versus 9.555, 9.549 and 16.022 seconds previously (consult raw
records for exact values). The faster-source job improves in two pairs and regresses in one.
Slow-source completion still determines makespan;
aggregate ratios remain 0.975, 0.981, 0.997. This is a completion-time tradeoff,
not an aggregate throughput improvement or precise bandwidth fairness result.

The next optimization should target lower-cost evidence on flat H2/H3 paths,
request-size economics under constraints, and peer-experiment qualification.
A matched old/new Fixed comparison and a larger short-job cohort should precede
claims of baseline parity. Extend real priority/many-job testing and genuine
network-delay/loss experiments separately. These results do not justify a storage
runtime, transport rewrite, new QUIC tuning or another scheduler authority.

All 90 selected trials are functionally eligible and retained. Earlier cohorts
remain separately classified in the raw archive. No historical CPU rejection label
was retroactively changed. Archive branch `archive/old-stage2` remains at
`e4d1137b7072e71c64f7ea63c4f5ac914ff8fa8e`; nothing was pushed.

Selected during-trial `total_percent`: 8.96–43.59% of all logical processors; median 13.94%.

Selected during-trial `controlled_percent`: 0.12–8.33% of all logical processors; median 0.71%.

Selected during-trial `background_percent`: 8.04–38.46% of all logical processors; median 12.34%.

Residual CPU is an attribution remainder, not automatically external interference. No per-core contention or total process RSS conclusion is available from these counters.
