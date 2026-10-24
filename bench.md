# Performance measurement

Use the current local source, locked dependencies and release binaries. The
[scheduler report](docs/scheduling-results.md) identifies measured builds and
retains complete raw cohorts. The [scheduler runner](benchmarks/2026-10-20-scheduler/README.md)
reproduces alternating comparisons without an environmental CPU rejection gate.
The [PR #1 investigation](docs/pr1-performance.md) records control-plane scaling,
effective capacity/range corrections, matched regression pilots, H3 sender CPU
attribution and controlled packet-delay measurements. Its [raw evidence](benchmarks/2026-10-23-pr1/raw.zip)
includes source/build manifests and failed diagnostic attempts.

## Timing and eligibility

- XDE job timing runs from public `start()` through committed completion.
  Engine construction and shutdown are separate measurements.
- XDE payload timing spans the first non-empty received payload through final
  sink consumption. Dispatch timestamps are not payload timestamps.
- Curl job timing uses libcurl `time_total`, excluding process creation.
  `time_total - time_starttransfer` is only an approximate payload interval.
- Alternate competing arms. Report individual observations and paired ratios,
  including p10, median, p90 and min/max. Ratios of independent medians are not
  paired speedups. Three pairs provide descriptive evidence, not strong inference.
- Establish functional correctness separately from timing eligibility. Validate
  payload protocol, actual topology, range responses, bytes and published content.
- Reject correctness failures, mismatched workloads or explicitly identified
  exceptional interference. Preserve reasons. Retain slow observations; utilization
  percentages and unfavorable throughput do not disqualify a trial.
- Preserve historical validity labels. A new analysis is a separate artifact with
  raw-file hashes and an explicit policy; it does not rewrite the original rows.

## CPU attribution

Record before/after idle windows, total system CPU during the transfer, controlled
process CPU and the unattributed residual. Keep interval samples and raw counters.
On Windows the recorder combines `GetSystemTimes` and `GetProcessTimes`, using
consistent sample boundaries and processor-normalized deltas. The fixture runs
in the client process; registered curl CPU is included when applicable.

Unattributed CPU is not automatically unrelated interference. Interrupts, kernel
work, process attribution and sample alignment can affect the residual. Independent
process or ETW observations can explain it. CPU and spikes are diagnostic metadata;
there is no arbitrary 5% or other utilization rejection threshold. Low aggregate
CPU also cannot prove absence of contention on a critical core.

Finish builds, tests and diagnostic profiling before deliberate timing trials.
Use profiling to investigate unexplained variance, then measure without it. Report
uncertainty when the machine remains variable.

## Protocol and topology evidence

H3 payload requires explicit H3 policy and payload assertions; an H1 Alt-Svc probe
is discovery traffic. Same-connection multiplexing requires overlapping active
request streams, independently checked by the fixture. Request count is insufficient.

Connection identities over a transfer's lifetime are distinct from concurrent
physical connections. H1 truncation may require replacement connections. Record
lifetime identities, concurrent payload connections and the reservation peak,
including opening/closing connections. Apply hard engine connection limits during
fixed-topology runs. Preserve per-request protocol and lifecycle evidence.

Fixture delay postpones response handling. It is not network RTT and cannot
support BDP or congestion-control claims. H3 curl discovery/routing is not an
equivalent workload; the runner does not pair it with XDE.

## Existing diagnostics

`xde-bench --pipeline a|b|c|d|e` uses the production controller and HTTP lifecycle:

| Stage | Added work |
| --- | --- |
| A | Receive, response/range validation, discard |
| B | Payload ownership and memory budgeting |
| C | Destination capacity acquisition/release |
| D | Batching, copies and permit retention without OS writes |
| E | Production positional file writes and publication |

A-D are diagnostic discard workloads. E requires read-back verification of the
published artifact. Diagnostic phase timers do not replace job throughput.

`write-probe --mode proactor|runtime|busy` compares direct Proactor, idle Runtime
and controlled runnable-task pressure. Use matching 64 KiB operations at depths
1/2/4/8 for shared disjoint positional regions and separate files. Preserve output
verification, latency distributions, ready/pending behavior, driver counts,
pressure activity and effective in-flight operations. A service-rate plateau does
not prove serialization.

The pinned Compio revision is `3d9933787fc3e6c9b0e8ce9241b568cd40c862f6`, including
the [IOCP progress correction](https://github.com/compio-rs/compio/commit/16d81e265c4806483c5677ba87d36002808da067).
A wake must not suppress polling pending completions. Keep the regression; that
fix does not explain every historical latency observation.

## Reporting

Record workload size/duration, actual protocol and topology, hardware/OS,
compiler, locked dependency/source identities, file setup and durability, CPU
attribution, resource counters, all observations, paired distributions and
correctness impact. Keep builds with different source/configuration identities in
separate cohorts. Use separate Cargo target directories for reference source trees.

Buffered file writes, cache throughput, verified publication and durable device
throughput are different metrics. Loopback is useful for isolation; genuine network
latency, loss and shared-link behavior require independently validated experiments.

[Adaptive intelligence results](docs/adaptive-intelligence-results.md) records the current policy validation and matched measurements.

The next focused findings and preserved paired evidence are in [PR #1 pressure and scoped reclamation](docs/pr1-next-performance.md).
