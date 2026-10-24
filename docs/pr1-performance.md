# PR #1 performance investigation

See the [checkpoint and provenance follow-up](checkpoint-performance.md) before
interpreting file-path results. Periodic recovery work was part of those paths.
A new diagnostic cohort also exposed unsafe shared-target build provenance.
Historical raw labels are preserved; the follow-up qualifies attribution separately.

## Scope and baseline

The comparison baseline is PR head `218a89f`. Its source archive, release
`scheduler-probe` executable and SHA-256 are preserved before production changes.
`archive/old-stage2` remains at
`e4d1137b7072e71c64f7ea63c4f5ac914ff8fa8e`.

This is an ongoing staged investigation. The control allocator and effective
capacity corrections are implemented. Focused transport profiling and controlled
packet-delay pilots are recorded below. Short-job attribution and busy-connection
sharing still require separate evidence.

## Control allocation

The old allocator recomputed an origin's connection sum by scanning every job
for each candidate and each resource. Grant refresh also searched vectors for
each job and scanned all connections separately for each job. Every observation
performed multiple refreshes, including refreshes with identical allocation
inputs. The pump searched all jobs despite exclusive connection ownership.

The replacement retains the same opportunity ordering and bounded productivity
weights. One temporary origin index tracks allocation-pass connection totals.
A local heap updates only the selected candidate after each grant. Its keys
cannot become stale across events because the heap does not survive the pass.
A complete input snapshot avoids repeating allocation for identical demand;
changing engine limits invalidates it. Snapshot construction still scans current
jobs, connections and assignments. This is not a persistent queue of scheduler
decisions or another resource ledger.

Connection selection uses its authoritative owner. Global admission is checked
by the pump, rather than rescanning probe occupancy for every candidate.
Each pump projects grants onto concrete ready connections, preferring their actual
stream capacity. Busy excess connections drain without receiving new assignments.
This removes the per-check distinct-connection set. Range dispatch occurs once,
after the event's state transition and any capacity-changing idle handoff.

Persistent priority queues and lifecycle-maintained origin/job caches were
considered. They require invalidation on progress, ownership changes, phase,
priority and connection transitions. The measured allocator reduction does not
justify those additional invariants yet.

Release microbenchmark, five batches of eight forced allocation calls, four
ready H2 connections/job, eight requested assignments/job, no active assignments:

| Jobs | Baseline independent/shared origins, µs | Indexed scans, µs | Local heap, µs |
| ---: | ---: | ---: | ---: |
| 1 | 0.11 / 0.10 | 0.38 / 0.34 | 0.23 / 0.23 |
| 8 | 3.70 / 4.01 | 3.69 / 3.60 | 2.41 / 2.33 |
| 32 | 76.76 / 101.49 | 55.39 / 54.85 | 9.72 / 9.38 |
| 128 | 2601.95 / 4290.55 | 900.25 / 890.61 | 47.73 / 47.24 |
| 512 | 253728.73 / 343086.64 | 14058.04 / 14084.75 | 241.11 / 241.71 |

These are wall times from sequential implementation cohorts, not paired CPU-time
measurements. The harness disables cache reuse for these calls. Zero-connection
cases and selection measurements are also preserved. This does not measure
full reconciliation, dispatch latency or allocator allocation counts.

The first matched engine cohort contains 30 eligible trials. At 128 jobs,
controller time falls from 89–106 ms to 12–18 ms. Allocation passes fall from
2118–2142 to 752–948. Throughput ratios are 0.953, 1.036, 1.793. This establishes
lower control cost, not a uniform throughput gain. At eight jobs the ratios are
0.979, 0.350, 0.669 despite similar sub-millisecond controller time. Those slow
observations remain included. Their payload intervals are also longer; the
controller's measured work does not account for the delay.

## Effective capacity and range sizing

Assignment demand now distinguishes discovery admission from payload capacity.
Ready H1 connections each provide one slot. Ready H2/H3 connections each provide
the requested stream ceiling. Allocation bounds those slots by the physical
grant, policy and destination ceilings. Opening connections do not provide
multiplexed payload capacity. Discovery retains a control slot so it can make
progress before any payload connection is ready.

For three H1 connections and one four-stream H2/H3 connection, the full capacity
is seven slots. A physical grant of two permits at most five; a grant of one
permits at most four. Pump ties prefer usable multiplexed capacity. Mixed
fleets suspend topology comparisons instead of qualifying a uniform experiment.

Range quantum uses aggregate sizing progress divided by usable granted lanes,
not desired lanes. Fixed topology retains its normal sizing. Work partitioning
can leave gaps for granted connections still opening; those prospective lanes
do not enter the throughput divisor or admission capacity. Acknowledged yielding
still retains the incumbent claim through the actual written boundary.

The second matched cohort has 36 eligible trials, three pairs/workload:

| Workload | Candidate/baseline ratios, in trial order |
| --- | --- |
| Capped H1, adaptive, 128 MiB | 0.958, 1.095, 1.148 |
| H2 fixed 1c×1s, 4 GiB | 1.057, 0.856, 1.048 |
| H3 fixed 1c×1s, 4 GiB | 1.011, 1.002, 1.014 |
| Eight H1 jobs, 8 MiB/job | 1.007, 0.990, 0.989 |
| H2 adaptive, 4 GiB | 1.002, 1.000, 1.060 |
| H3 adaptive, 4 GiB | 1.010, 0.996, 1.028 |

The large eight-job slowdown did not recur in this cohort. Its cause is not
resolved. H2 adaptive request counts are 27/24/24 before and 25/24/24 after;
this is not evidence of a general request-count improvement. Summed H2 adaptive
TTFB is 32–63 ms over roughly five-second transfers. Request setup alone does
not explain all historical H2 variation. H3 adaptive topology changes remain
frequent, eight to twelve per trial, with no demonstrated scaling benefit.

These binaries precede the final removal of duplicate pump occupancy checks.
They are separate frozen checkpoints, not measurements of every later edit.

## Transport investigation

The pinned Compio source exposes QUIC `read_chunk` and `read_chunks`. H3's QUIC
trait exposes `poll_data`; its private request stream owns DATA framing. Calling
QUIC reads underneath an active H3 parser would violate that ownership.

The focused `receive-probe` compares raw ordered QUIC chunks, up-to-32-chunk
reads, minimal H3 and minimal H3 consumption against the existing fixture.
It verifies every byte. Raw QUIC is a lower-layer diagnostic, not equivalent
HTTP/3 work. The existing full-engine A–E facility remains the engine reference.

Probe receive time begins after QUIC establishment. The separate payload interval
starts after the first nonempty API return and includes byte verification through
EOF. Client thread CPU covers receive time; minimal-server CPU starts at sending
and ends after close processing. These CPU intervals are not identical.
Process load covers setup through teardown. Peak working set is the kernel's
process-lifetime high-water mark, including the in-process fixture. Neither is
a transfer-memory-budget high-water measurement. API receive calls are not task
polls, wakeups, UDP operations or HTTP DATA frame counts.

Windows WPR CPU capture failed with `0xc5585011`: "Failed to enable the policy
to profile system performance." No security or machine policy was changed.
WSL supports software task-clock measurements, but hardware cycle/instruction
events are unsupported. No physical throughput ceiling is established.

The three 4 GiB receive trials produced the following complete throughput
sequences in MiB/s, in trial order:

* Raw QUIC chunks: 222.380, 229.115, 232.012.
* Raw QUIC reads of up to 32 chunks: 214.249, 226.704, 233.830.
* Minimal H3: 165.325, 209.203, 208.971.
* Minimal client with the existing H3 fixture: 207.477, 213.365, 216.321.
* Engine receive/validation A: 220.727, 226.267, 219.514.
* Production destination E: 197.931, 193.439, 216.901.

All eighteen transfers verified their bytes and advertised protocol/topology.
A and E each made sixteen requests. H3 delivered approximately 914 chunks/MiB;
E performed sixteen destination writes/MiB. Paired E/A throughput ratios were
approximately 0.897, 0.855 and 0.988. Summed file-write await time was
5.112, 5.144 and 3.369 seconds, which can overlap transport work and is not an
additive critical-path decomposition.

Fewer successful QUIC API returns did not demonstrate a throughput benefit.
The minimal H3 sender used 92–98% of one CPU over its measured sending interval;
the client used approximately half a CPU. This motivates sender profiling.
It does not identify a specific cryptographic, socket or runtime cause, and the
existing fixture's server-thread CPU was not separately measured in this cohort.
The slow minimal-H3 observation remains in the distribution.

The subsequent CPU-class affinity cohort is diagnostic only for engine
comparisons: its baseline and candidate lockfiles differed. All twelve transfers
passed functional checks, but none of the six A/B pairs had matching build
identity. Within each binary, H2 still varied with affinity restricted to the
same CPU class. CPU class alone therefore does not explain the observed variance.
New comparison builds use immutable source snapshots, an identical lockfile and
benchmark harness, and executable/source manifests. Historical labels remain
unchanged.

## Matched consolidation pilot

The coherent source snapshot includes concrete connection projection, natural
draining and consolidated dispatch. It precedes terminal metadata reclamation.
Its eighteen Windows transfers pass functional checks and all nine pairs have
matching harness, compiler, dependency and workload identity. Candidate/baseline
throughput ratios, in trial order:

* Capped H1 cold start: 1.011, 1.044, 0.951.
* Adaptive H2: 0.883, 0.981, 0.832.
* Adaptive H3: 1.032, 0.934, 1.017.

H2 is a regression signal, not a successful optimization. A second cohort using
the existing intelligence runner compares old adaptive, candidate adaptive and
candidate fixed 1c×1s. Candidate/old adaptive ratios are 0.957, 0.977, 0.948;
fixed/old adaptive ratios are 0.959, 1.065, 0.995. Requests remain approximately
23–26 per adaptive job. Measured controller time is approximately one millisecond
over five-second transfers, and summed request TTFB is approximately 40–64 ms in
the first cohort. Neither counter explains the entire loss. Their small absolute
cost does not rule out scheduling effects outside measured controller execution.

The candidate fixed H2 A/E ladder yields A throughput 865.240, 858.151, 877.631
MiB/s and E throughput 835.534, 883.225, 907.855 MiB/s. All six transfers use
one payload connection, one stream and sixteen requests. Destination processing
does not impose a consistent penalty in this cohort. A separate matched fixed
H2 comparison restricted to CPU class 1 gives ratios 0.978, 1.187, 0.866. Variation
persists within that class. Three trials are descriptive and cannot establish
distinct fast/slow modes or an engine-wide speedup.

## Windows H3 sender cost

The subsequent 1 GiB lower-layer diagnostic records separate Windows thread
user/kernel CPU counters. Minimal H3 throughput is 245.599, 241.179, 240.204 MiB/s.
Over 4.169, 4.246, 4.263 seconds elapsed, sender kernel CPU is 3.609, 3.609,
3.406 seconds and user CPU is 0.531, 0.594, 0.828 seconds. Receiver CPU is
2.188, 2.125, 2.313 seconds. Raw QUIC shows the same predominantly kernel-cost
sender pattern. These are unprofiled diagnostics, not accepted speedup comparisons
against the earlier 4 GiB cohort.

Client QUIC accounting reports approximately 898 UDP receive I/Os/MiB; sender
accounting reports approximately 11–13 transmit I/Os/MiB for minimal H3. These are
library counters, not an independent syscall trace. The pinned Windows backend
supports UDP segmentation through `UDP_SEND_MSG_SIZE` and Compio's `WSASendMsg`
path. Identifying the kernel functions responsible still requires an authorized
Windows profile capture. Kernel CPU attributed to this sender is workload cost,
not automatically unrelated environmental interference.

Two simultaneous 1c×1s H3 jobs, 1 GiB each, provide a separate fixture discriminator.
One shared endpoint produces 253.452, 211.082, 219.278 MiB/s aggregate; independent
endpoints produce 316.445, 295.509, 284.556 MiB/s. The paired ratios are
1.249, 1.400, 1.298. Both layouts verify publication and protocol/topology.
Independent fixtures add server threads and alter origin scheduling, so this
does not isolate one server function. It does demonstrate that fixture layout
materially affects the observed ceiling. Busy connection sharing remains untested.

The longer Linux H3 software profile records 2,449 samples with none lost.
Leading self costs include AES-GCM, copies, QUIC packet processing and kernel UDP
receive work. The same 8 GiB diagnostic runs around 1.26 GiB/s while profiling.
Its backend, compiler and profiling conditions differ from Windows; this is not
a cross-platform speedup comparison or a Windows kernel attribution.

## Controlled packet delay

The coherent Linux pair ran in a new network namespace with loopback netem
10 ms delay, no configured loss and a 10,000-packet queue. Ping measured
20.095/20.170/20.239 ms minimum/mean/maximum RTT. Per-trial qdisc counters are
preserved. The eighteen 128 MiB transfers pass functional checks and all nine
pairs match build/workload identity. Candidate/baseline ratios are:

* Capped adaptive H1: 1.000, 1.000, 0.994.
* Fixed H2 1c×1s: 1.066, 0.996, 1.071.
* Fixed H3 1c×1s: 1.257, 1.042, 0.623.

The slow H3 result is retained. This pilot validates actual packet delay rather
than fixture response sleeps. It does not establish H3 stability, loss response,
BDP-optimal settings or real remote-server behavior.

## Terminal ownership

Terminal jobs previously left source URLs/headers, unused artifact records and
destination keys resident indefinitely. Source reclamation has a failing-before,
passing-after regression. The unused artifact-ID layer and destination-ID/index
indirection are removed. Jobs own source records and destination keys; the active
lease map maps a key directly to its generational job owner. Terminal removal
releases those records while retaining reusable origin/endpoint evidence.

A second regression reproduces a live primary probe being handed to another job
under a one-connection limit after cancellation. Failure handling now captures
and closes pending control operations before changing the job phase. Transfer
writers still drain before terminal removal; publication still requires commit
acknowledgement. Late control results and stale cancellation cannot mutate a new
job or release its lease. Paused jobs retain their metadata and exclusive lease.
This cleanup has correctness validation, but no claimed RSS or throughput gain.
Origin/endpoint history remains a separate cache-lifetime question.

An isolated WSL network namespace with `netem delay 10ms` on loopback measured
20.068/20.141/20.201 ms minimum/mean/maximum ping RTT. The namespace disappeared
after the check. This validates the mechanism; it is not yet a download result.

## Connection sharing boundary

Busy connection sharing would require job-owned stream leases and a separately
owned physical reservation. Current connection rate, in-flight accounting and
snapshots assume one job owner. Request representation fingerprints deliberately
exclude credentials and cannot serve as connection-sharing security keys.

A conservative sharing key must include exact authority, negotiated protocol,
endpoint/Alt-Svc route, TLS identity, credential scope and network context.
Cross-origin coalescing is a separate feature. Both H2 and QUIC have shared
connection flow control as well as stream flow control; sharing can change
interference and attribution. HTTP priority is advisory.
[RFC 9113 §5.2](https://www.rfc-editor.org/rfc/rfc9113.html#section-5.2),
[RFC 9000 §4.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-4.1),
[RFC 9218 §10](https://www.rfc-editor.org/rfc/rfc9218.html#section-10).

No busy-sharing implementation is justified by the current measurements.
Its comparative performance remains unmeasured.

## CI and validation

Hosted CI logs show missing toolchain-specific fmt/Clippy, rather than a Rust
code error at those failing steps. Installation now names components on the
matrix toolchain itself. The pre-existing 1.99 OS matrix remains; a separate
Ubuntu 1.98 job retains the declared MSRV check. All Cargo gates use the selected
toolchain explicitly, all features and the lockfile.

Windows and WSL passed 164 correctness tests after terminal reclamation and
probe-cancellation correction. One isolated performance microbenchmark remains
intentionally ignored. All-feature/all-target Clippy and doctests passed on both
platforms. Formatting, three benchmark-analysis tests and Windows Rust 1.98
all-feature/all-target checking passed. Regressions cover natural multiplexed
connection draining, idle handoff capacity, terminal source/lease reclamation,
stale results after ID reuse and cancellation of a live primary probe.
Hosted Windows/Ubuntu/macOS checks cannot validate an unpublished workflow edit.
Nothing has been pushed or merged.
