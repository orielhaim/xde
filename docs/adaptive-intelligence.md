# Adaptive intelligence design

The existing controller correctly separates job intent from engine grants, but
the policy loses seconds establishing repeated baselines, grows by one resource,
and only contracts after a detected path change. A rejected direction can remain
suppressed indefinitely. The allocator knows demand and byte service but cannot
distinguish productive surplus from a saturated job.

The implementation keeps the two authorities. The job planner performs bounded
direct search over topology. A small sequential observation block replaces fixed
three-window phases. H1 cold starts use a bounded two-way trial after valid range
evidence and sufficient remaining work. Unknown multiplexed endpoints start with
one stream; reliable long-duration endpoint evidence can justify two. Growth can double a previously productive
dimension; a failed step refines to a smaller step. Downward trials periodically
test whether fewer resources preserve goodput. Rejected directions back off and
can be reconsidered after changed conditions. Global changes invalidate evidence.

The alternative considered was an engine-wide optimizer selecting both experiments
and allocations. It could coordinate exploration but would merge the two decision
authorities and confound job-local topology evidence with allocation policy.
Luna independently recommended retaining job-local marginal experiments.

The allocator protects a first opportunity and byte-service rotation. Surplus
uses weighted diminishing resource utility plus bounded, measured fractional
goodput elasticity. Total observed speed never becomes a priority multiplier.
The fairness term remains positive when measured marginal productivity is zero.
Unknown productivity receives exploration opportunity, not an assertion of gain.
Queue and Fixed retain their manual semantics.

This is informed by [Kelly, Maulloo and Tan's network utility formulation](https://www.statslab.cam.ac.uk/~frank/rate.pdf).
XDE controls resource admission, not transport rates. Its discrete utility model
does not establish proportional inbound bandwidth fairness or identify shared
network bottlenecks. Simulated optima apply only to the stated model.

Integration contracts:

- `JobPlanner::start` consumes range/protocol/remaining/setup evidence before first
  assignment admission. It cannot open a connection or override resource limits.
- `JobPlanner::observe` consumes aggregate useful bytes and constraints on the
  coarse decision clock. It returns desired topology changes only.
- `JobPlanner::productivity` supplies bounded, qualified marginal evidence for
  surplus allocation. Grant changes discard comparisons, not current intent or
  previously qualified productivity. Evidence expires after 32 seconds or resets
  after observed path changes. This prevents the allocator from erasing the
  evidence that justified its own grant decision.
- Adaptive range claims have a one-second per-lane goodput horizon, initially
  8 MiB for H1 and 32 MiB for H2/H3. An independent aggregate sizing meter
  survives constrained optimization windows. Cold intent is granted before the first claim. An efficient topology
  cannot help when one early request has reserved the useful work for seconds.
- `allocator::allocate` is the sole grant authority. Existing full-generation
  physical reservations and acknowledged range yields enforce actual ownership.

Windows and WSL tests, synthetic completion-time/oracle comparisons and alternating
matched-source experiments validate the selected design before performance claims.
