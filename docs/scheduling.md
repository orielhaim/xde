# Job planning and resource allocation

XDE separates transfer intent from engine resource allocation. The pure controller
coordinates lifecycle effects; it does not own another topology policy.

## Ownership

| Fact | Owner |
| --- | --- |
| User limits and fixed/adaptive choice | `TransferPolicy` |
| Desired connections and streams, topology experiments | `JobPlanner` in each `JobNode` |
| FairShare/Queue admission and resource grants | `allocator::allocate` |
| Actual connection reservations, including opening and closing | Controller reservation ledger |
| Actual assignments and generation identity | `SegmentPlan` |
| Socket, HTTP sessions and I/O futures | Addressed Compio shard |
| Protocol discovery, endpoints, backoff and reachability | Origin/endpoint records |
| Transfer memory and destination credits | Existing memory budget and destination capacity registry |

Engine limits never overwrite the user's job policy. Snapshots expose desired
topology, grants, opening connections, active assignments and global constraints.
A fixed target is a request, not a reservation guarantee.

All physical opens pass through one grant-checked function. A reservation consumes
engine, origin and job connection capacity before dispatch. Failed dispatch or a
failed handshake returns it. A close retains the reservation until the owning
shard acknowledges closure. A close arriving during handshake is remembered and
the eventual connection is closed before becoming usable. Generational IDs keep
late acknowledgements from returning someone else's reservation. The ledger uses
full generational keys, so a closing connection and a new occupant of the same
arena slot can coexist.

Connections have one job owner at a time. This makes connection experiments and
per-job physical limits attributable. Idle compatible same-origin connections can
change owners. Busy connections never change owners, including multiplexed ones.
Consequently a single shared H2/H3 connection is time-shared between jobs rather
than multiplexing requests from several jobs simultaneously. This is a deliberate
cost of the simpler ownership model, and a limit to revisit if measured workloads
justify shared connection leases.

## Configuration

```rust
let engine = xde::Engine::builder()
    .limits(xde::EngineLimits {
        scheduling: xde::SchedulingPolicy::FairShare,
        max_physical_connections: 16,
        max_active_assignments: 32,
        ..Default::default()
    })
    .build()?;

let fixed = xde::TransferPolicy {
    topology: xde::TopologyPolicy::Fixed(xde::Topology::new(2, 4)),
    ..Default::default()
};

let queued = xde::EngineLimits {
    scheduling: xde::SchedulingPolicy::Queue {
        max_active_jobs: 2.try_into().unwrap(),
    },
    ..Default::default()
};
```

Adaptive is the default job policy. Range-capable jobs with at least 16 MiB
remaining can start with two H1 connections. Unknown H2/H3 endpoints start with
one stream; at least three endpoint samples predicting more than six seconds
plus setup can justify two streams. Smaller/unknown transfers stay at one lane.
Fresh endpoint receive evidence can suppress a trial that cannot repay setup.
Global/origin headroom, destination hints and user ceilings bound this request.
Fixed targets remain subject to job ceilings, real protocol/source capabilities,
destination capabilities and engine grants. H1 always has one active request per
connection. An active assignment currently uses one HTTP request stream. Probe
requests also consume global admission capacity.

Queue selects waiting jobs by priority, then monotonically allocated admission
order. Activated jobs retain their activation through publication. Waiting jobs
do not cause range preemption or topology changes in running jobs. Queue admission
does not implement bandwidth management. Destination preparation and leases are
established at submission; the queue controls transfer activation.

`Job::pause` stops admission and cancels active requests. It acknowledges the
request before the operations finish draining. Wait for `JobPhase::Paused` before
calling `resume`. Verified ranges survive; incomplete responses must be fetched
again. Pause is rejected during destination preparation, initial probing,
publication and teardown. Cancellation remains available in those phases.

## FairShare

The allocator distributes connection and assignment opportunities using discrete
weighted max-min allocation. It satisfies one opportunity per runnable job before
increasing a job's parallelism. When a resource is indivisible, normalized useful
byte service breaks ties. Admission reserves pending range work in the service
score, preventing a fleet of uncompleted assignments from appearing free.
Terminal observations settle service, including coarse consumed bytes from failed
requests. New/resumed jobs start at the active virtual-service floor.

Priority weights are 1, 2, 4, 8 and 16 for Background through Critical. These weights
govern contended opportunities. They do not promise those ratios of network
bandwidth. A limited-demand job leaves surplus grants for other jobs. A single
job receives all requested, usable capacity within the limits.

After the first opportunity, surplus uses weighted diminishing opportunity utility
with a marginal-productivity factor from 1 to 2. The score corresponds to the
derivative of `weight * log(resources * goodput(resources))`. Productivity is a
qualified fractional gain per fractional resource increase, not total speed.
Taking bandwidth from peers earns no productivity boost. The positive resource
term protects service when marginal productivity is zero. Unknown evidence remains
exploratory; Queue does not use this factor.

Only when FairShare demand exceeds a grant, new ranges are bounded by `fair_quantum_bytes`,
8 MiB by default, plus range alignment. Existing large requests receive a lower
dynamic end boundary so they return to admission after approximately another
quantum. The old assignment keeps its complete range reservation until its
terminal observation; another worker cannot claim that tail early. Already
consumed transport data and one in-progress sink operation can exceed the sampled
boundary. Fixed jobs with full grants keep their normal ranges. Adaptive jobs
also bound each claim to about one second of observed per-lane goodput, initially
8 MiB for H1 and 32 MiB for multiplexed protocols. A separate aggregate progress
meter retains sizing information through constrained optimization windows.
This leaves claimable work for expansion and bounds experiment draining.
The fair and adaptive byte bounds combine by taking the smaller. This bounds
work opportunities in bytes, not in time.

Full-body sources cannot yield by issuing a new range. Such requests hold their
actual assignment until completion, failure or cancellation. A stalled request can
therefore occupy a scarce slot until its timeout. With spare engine capacity it
does not prevent unrelated jobs from running.

When all bytes are already claimed, admission can request an incumbent yield.
The incumbent retains its original range until its terminal observation, including
any destination operation already in flight. Only acknowledged unwritten bytes
become claimable. An operation may finish beyond the requested end; the next
claim starts after the actual verified boundary. H1 must replace a connection
whose response was truncated; multiplexed transports retire the stream.

## Adaptive experiments

The planner samples aggregate progress on a 250 ms decision clock. Evidence
intervals span at least twice observed connection setup and need 64 KiB, except
zero progress remains observable. Two samples can decide a clear change; uncertain
trials accumulate up to six. Online mean/variance retains all windows. Its two
standard-error noise band is a decision rule, not a statistical confidence claim
about independent samples. Control constants are evaluated against deterministic
profiles and matched real workloads, not universal optimums.

Workers publish consumed bytes and accumulated memory/destination waiting at
64 KiB boundaries and request completion. There is one writer per assignment
record. No fragment creates a controller message, telemetry allocation or new
high-resolution timestamp. The controller reads those counters only at policy
decisions and lifecycle events. Consumed bytes are explicitly separate from
HTTP-verified bytes and published artifacts.

Growth must clear both job and priority-weighted engine goodput noise bands,
then repay added physical connection setup before completion. Stream growth does
not pay a fictitious handshake. The aggregate signal is derived from existing
useful-byte counters at policy ticks. Taking a peer's bandwidth cannot appear to
create capacity. A contraction can preserve aggregate progress while improving
per-resource job efficiency, allowing peers to release redundant connections.
Unknown lengths, short tails, local pressure and denied global demand do not
justify expansion. Local pressure permits contraction without erasing range-sizing
evidence. Unknown growth needs six seconds of remaining work; recently productive
growth needs one second, and downward trials need 1.5 seconds. Opening/draining suspends sampling. Changes to a job's own
grant discard comparisons. Peer membership/priority changes reset the context;
peer grant counts alone do not. Current intent remains unqualified, preventing
rollback from triggering cascading peer-grant changes. Qualified marginal evidence
survives such resets but expires after 32 seconds; observed path changes clear it.
Available global/origin headroom is an offer to experiment, never a grant.

H1 cold trials first offer growth from two to four connections when headroom
exists. Unknown multiplexed endpoints establish a single-stream baseline. Bounded direct search considers halving connections/streams, increasing a useful
dimension geometrically and redistributing a fixed slot budget across more
connections with fewer streams. Failed increases revert and refine to unit steps.
Rejected moves back off from four to 128 seconds. Accepted changes suppress an
immediate reverse trial. Path changes require at least four observations before
resetting that backoff. Failed halving trials refine to a one-resource reduction. A stable reference survives uncertain transition windows, so averaging a
slowdown into the reference cannot hide it. Downward trials are retained when
joint progress is preserved and per-resource efficiency improves. Excess requests yield after a bounded piece,
retain their claims through acknowledgement, then idle connections close/reassign.

These experiments compare total useful-job progress, not request receive-active
rates. Endpoint receive samples remain routing evidence. The planner does not
infer congestion, RTT, per-core CPU contention or source throttling from a single
low rate. Setup economics use observed handshakes; DNS and cold-route costs for a
new mirror remain approximate.

## Backpressure and limits

This scheduler controls request admission and bounded range work. It does not
centrally schedule fragments or promise exact inbound bandwidth fairness.
Independent origins can use independent resources concurrently. Priority weights
apply when engine resource grants contend; they do not impose weighted network
rates on jobs whose grants are fully satisfied.

Coarse origin grants group a job under its primary origin. Every actual mirror
dial separately checks its route's origin ledger. Expansion across several mirrors
can therefore be conservative under tight per-origin ceilings.

H1 consumption eventually limits TCP receive progress. H2 consumption interacts
with stream and connection flow control; the pinned configuration advertises
8 MiB stream and 32 MiB connection windows with 64 KiB maximum frames. H3 uses
Quinn stream/connection flow control through Compio. Its default connection receive
window is not an engine-budget-sized aggregate bound. Protocol and kernel buffers
are separate from `MemoryBudget`; this change does not retune or replace them.
Stopping destination consumption does not instantly stop a server's transmissions.

The research references inform the separation of admission, measurements and
service accounting. [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits)
and [Envoy adaptive concurrency](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter.html)
target request admission and latency. They are not copied as download topology
algorithms. [BBR](https://github.com/google/bbr) controls transport in-flight data
using a path model. XDE does not have equivalent packet-level observations.
[RFC 8290](https://www.rfc-editor.org/rfc/rfc8290) motivates byte-denominated fair
service, but XDE uses coarse range admissions rather than packet queues or CoDel.

## Correctness and observability

Plans, assignment identities, retries, memory permits, destination leases and
publication barriers retain their existing ownership. Success still has one
producer after acknowledged destination commit. Pausing, cancelling, reducing
grants or abandoning an experiment cannot produce success. Stale assignment
observations and retry timers remain generation-checked.

`ControllerStats` counts observations, allocation passes and policy evaluations.
With diagnostics enabled it also measures total controller handling time. This
excludes sleeping and status-query handling and is not a process CPU measurement.
Decision counters separate global, local, source and unsettled constraints from
topology changes. Planner statistics and move backoffs have fixed size. Policy decisions add no
per-fragment histograms or per-job shared writer atomics.

The simulator covers weighted service, arrivals, unused demand, same/independent
origins, queue ordering, conservation, fixed jobs, beneficial/ineffective probes,
local/global/source constraints, noisy trials, near completion and reduction after
a path change. Integration tests assert actual range coverage, paused/resumed
publication, queue cancellation, physical lease acknowledgements and the existing
publication/digest/cancellation regressions.
