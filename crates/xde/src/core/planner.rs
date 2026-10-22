//! Job-local bounded direct search. Grants and I/O belong to other owners.
use super::policy::{Topology, TopologyPolicy, TransportLimits};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Constraint {
    None,
    Global,
    Local,
    Source,
}

#[derive(Debug, Clone, Copy)]
pub struct Evidence {
    pub now: Instant,
    pub useful_bytes: u64,
    pub engine_bytes: u128,
    pub remaining: Option<u64>,
    pub constraint: Constraint,
    pub multiplexed: bool,
    pub setup: Duration,
    pub workload_context: u64,
}
/// Fractional gain per fractional resource increase, never total job speed.
#[derive(Debug, Clone, Copy)]
pub struct Productivity {
    pub connections: f64,
    pub assignments: f64,
}
impl Default for Productivity {
    fn default() -> Self {
        Self {
            connections: 1.0,
            assignments: 1.0,
        }
    }
}
#[derive(Debug, Clone, Copy)]
enum Move {
    FewerStreams,
    FewerConnections,
    MoreStreams,
    MoreConnections,
    Redistribute,
}
impl Move {
    const ALL: [Self; 5] = [
        Self::FewerStreams,
        Self::FewerConnections,
        Self::MoreStreams,
        Self::MoreConnections,
        Self::Redistribute,
    ];
    fn index(self) -> usize {
        self as usize
    }
    fn decrease(self) -> bool {
        matches!(self, Self::FewerStreams | Self::FewerConnections)
    }
}
#[derive(Debug, Clone, Copy, Default)]
struct Estimate {
    count: u8,
    mean: f64,
    m2: f64,
}
impl Estimate {
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / f64::from(self.count);
        self.m2 += delta * (value - self.mean);
    }
    fn radius(self) -> f64 {
        if self.count < 2 {
            return f64::INFINITY;
        }
        2.0 * (self.m2 / f64::from(self.count - 1) / f64::from(self.count)).sqrt()
    }
    fn lower(self) -> f64 {
        (self.mean - self.radius()).max(0.0)
    }
    fn upper(self) -> f64 {
        self.mean + self.radius()
    }
}
#[derive(Debug, Clone, Copy)]
enum Search {
    Baseline,
    Trial {
        previous: Topology,
        baseline: Estimate,
        engine_baseline: Estimate,
        movement: Move,
    },
    Hold {
        reference: Estimate,
    },
}
#[derive(Debug, Clone, Copy, Default)]
struct Backoff {
    failures: u8,
    until: Option<Instant>,
}
impl Backoff {
    fn reject(&mut self, now: Instant) {
        self.failures = (self.failures + 1).min(6);
        self.until = Some(now + JobPlanner::WINDOW * (8 << self.failures));
    }
    fn available(self, now: Instant) -> bool {
        self.until.is_none_or(|until| now >= until)
    }
}
#[derive(Debug, Default)]
struct Progress {
    previous: Option<(Instant, u64)>,
    rate: f64,
}
impl Progress {
    fn observe(&mut self, now: Instant, bytes: u64) {
        if let Some((at, previous)) = self.previous {
            let elapsed = now.saturating_duration_since(at).as_secs_f64();
            if bytes < previous {
                self.rate = 0.0;
            } else if elapsed > 0.0 {
                let rate = (bytes - previous) as f64 / elapsed;
                self.rate = if self.rate == 0.0 {
                    rate
                } else {
                    self.rate
                        + (rate - self.rate) * elapsed
                            / (elapsed + JobPlanner::WINDOW.as_secs_f64())
                };
            }
        }
        self.previous = Some((now, bytes));
    }
}

#[derive(Debug)]
pub struct JobPlanner {
    policy: TopologyPolicy,
    desired: Topology,
    started: bool,
    since: Option<(Instant, u64, u128)>,
    workload_context: Option<u64>,
    block: Estimate,
    engine_block: Estimate,
    progress: Progress,
    bootstrap_piece: u64,
    search: Search,
    cursor: usize,
    backoff: [Backoff; 5],
    connection_step: u8,
    stream_step: u16,
    productivity: Option<(Productivity, Instant)>,
}
impl JobPlanner {
    pub const WINDOW: Duration = Duration::from_millis(250);
    const MIN_BYTES: u64 = 64 * 1024;
    const MAX_SAMPLES: u8 = 6;
    // Two worst-case qualification blocks may consume at most half the remaining time.
    const EXPLORATION_WINDOWS: u32 = Self::MAX_SAMPLES as u32 * 4;
    pub const COLD_BYTES: u64 = 16 * 1024 * 1024;
    pub fn new(policy: TopologyPolicy, limits: TransportLimits) -> Self {
        Self {
            policy,
            desired: match policy {
                TopologyPolicy::Adaptive => Topology::new(1, 1),
                TopologyPolicy::Fixed(target) => target,
            }
            .clamped(limits),
            started: false,
            since: None,
            workload_context: None,
            block: Estimate::default(),
            engine_block: Estimate::default(),
            progress: Progress::default(),
            bootstrap_piece: Self::COLD_BYTES / 2,
            search: Search::Baseline,
            cursor: 0,
            backoff: [Backoff::default(); 5],
            connection_step: 1,
            stream_step: 1,
            productivity: None,
        }
    }
    pub fn desired(&self) -> Topology {
        self.desired
    }
    pub fn adaptive(&self) -> bool {
        matches!(self.policy, TopologyPolicy::Adaptive)
    }
    pub fn assignment_quantum(&self) -> Option<u64> {
        if !self.adaptive() {
            return None;
        }
        let rate = self.progress.rate;
        Some(if rate > 0.0 {
            (rate / f64::from(self.desired.slots()) * (Self::WINDOW * 4).as_secs_f64())
                .max((Self::MIN_BYTES * 4) as f64) as u64
        } else {
            self.bootstrap_piece
        })
    }
    pub fn productivity(&self, now: Instant) -> Productivity {
        self.measured_productivity(now).unwrap_or_default()
    }
    fn measured_productivity(&self, now: Instant) -> Option<Productivity> {
        self.productivity
            .filter(|(_, at)| now.saturating_duration_since(*at) <= Self::WINDOW * 128)
            .map(|(value, _)| value)
    }
    pub fn start(
        &mut self,
        remaining: Option<u64>,
        multiplexed: bool,
        setup: Duration,
        prior_bps: Option<f64>,
        limits: TransportLimits,
    ) {
        if self.started || !self.adaptive() {
            return;
        }
        self.started = true;
        self.bootstrap_piece = if multiplexed {
            Self::COLD_BYTES * 2
        } else {
            Self::COLD_BYTES / 2
        };
        let Some(bytes) = remaining.filter(|bytes| *bytes >= Self::COLD_BYTES) else {
            return;
        };
        if prior_bps
            .is_some_and(|rate| bytes as f64 / rate <= (setup + Self::WINDOW * 4).as_secs_f64())
        {
            return;
        }
        let target = if multiplexed {
            if prior_bps.is_some_and(|rate| {
                bytes as f64 / rate
                    > (Self::WINDOW * Self::EXPLORATION_WINDOWS + setup).as_secs_f64()
            }) {
                Topology::new(1, 2)
            } else {
                Topology::new(1, 1)
            }
        } else {
            Topology::new(2, 1)
        }
        .clamped(limits);
        if target.slots() <= u32::from(limits.max_active_assignments) {
            self.desired = target;
            if !multiplexed && target.connections > 1 {
                self.cursor = Move::MoreConnections.index();
                self.connection_step = target.connections;
            }
        }
    }
    pub fn suspend(&mut self) {
        self.since = None;
        self.block = Estimate::default();
        self.engine_block = Estimate::default();
    }
    pub fn invalidate(&mut self) {
        self.suspend();
        self.search = Search::Baseline;
        // Allocation changes invalidate evidence, not intent. A rollback would
        // itself change peer grants and propagate another round of invalidations.
        self.cursor = 0;
    }
    pub fn observe(&mut self, e: Evidence, limits: TransportLimits) -> bool {
        if !self.adaptive() {
            return false;
        }
        self.progress.observe(e.now, e.useful_bytes);
        let before = self.desired;
        if self
            .workload_context
            .is_some_and(|context| context != e.workload_context)
        {
            self.invalidate();
        }
        self.workload_context = Some(e.workload_context);
        if matches!(e.constraint, Constraint::Global | Constraint::Source) {
            self.invalidate();
            return before != self.desired;
        }
        if e.constraint == Constraint::Local
            && let Search::Trial {
                previous, movement, ..
            } = self.search
            && !movement.decrease()
        {
            self.desired = previous;
            self.invalidate();
            self.backoff[movement.index()].reject(e.now);
            return before != self.desired;
        }
        let Some((at, bytes, engine_bytes)) = self.since else {
            self.since = Some((e.now, e.useful_bytes, e.engine_bytes));
            return before != self.desired;
        };
        let elapsed = e.now.saturating_duration_since(at);
        let delta = e.useful_bytes.saturating_sub(bytes);
        if e.useful_bytes < bytes || e.engine_bytes < engine_bytes {
            self.invalidate();
            return before != self.desired;
        }
        if elapsed < Self::WINDOW.max(e.setup * 2) {
            return before != self.desired;
        }
        // Zero progress must remain visible; a byte threshold must not hide stalls.
        if delta < Self::MIN_BYTES && delta != 0 {
            return before != self.desired;
        }
        self.since = Some((e.now, e.useful_bytes, e.engine_bytes));
        self.block.add(delta as f64 / elapsed.as_secs_f64());
        self.engine_block
            .add((e.engine_bytes - engine_bytes) as f64 / elapsed.as_secs_f64());
        if self.block.count < 2 {
            return before != self.desired;
        }
        match self.search {
            Search::Trial {
                previous,
                baseline,
                engine_baseline,
                movement,
            } => {
                let rate = self.block;
                let savings = e.remaining.map_or(0.0, |left| {
                    left as f64 * (1.0 / baseline.mean - 1.0 / rate.mean)
                });
                let added = self
                    .desired
                    .connections
                    .saturating_sub(previous.connections);
                let keep = if movement.decrease() {
                    rate.mean > 0.0
                        && self.engine_block.lower() >= engine_baseline.lower()
                        && rate.lower() / f64::from(self.desired.slots())
                            >= baseline.lower() / f64::from(previous.slots())
                } else {
                    rate.lower() > baseline.upper()
                        && self.engine_block.lower() > engine_baseline.upper()
                        && savings > e.setup.as_secs_f64() * f64::from(added)
                };
                let reject = self.engine_block.upper() < engine_baseline.lower()
                    || (!movement.decrease()
                        && self.engine_block.upper() <= engine_baseline.lower())
                    || rate.upper() < baseline.lower()
                    || (rate.upper() <= baseline.lower() && !movement.decrease());
                if !keep && !reject && rate.count < Self::MAX_SAMPLES {
                    return before != self.desired;
                }
                let engine_gain = if movement.decrease() {
                    engine_baseline.lower() - self.engine_block.upper()
                } else {
                    self.engine_block.lower() - engine_baseline.upper()
                };
                self.learn(previous, baseline, rate, movement, e.now, engine_gain);
                let reference = if keep { rate } else { baseline };
                if !keep {
                    self.desired = previous;
                    self.backoff[movement.index()].reject(e.now);
                    match movement {
                        Move::MoreConnections => self.connection_step = 1,
                        Move::MoreStreams => self.stream_step = 1,
                        _ => {}
                    }
                } else {
                    self.backoff[movement.index()] = Backoff::default();
                    let opposite = match movement {
                        Move::FewerConnections => Some(Move::MoreConnections),
                        Move::FewerStreams => Some(Move::MoreStreams),
                        Move::MoreConnections => Some(Move::FewerConnections),
                        Move::MoreStreams => Some(Move::FewerStreams),
                        _ => None,
                    };
                    if let Some(opposite) = opposite {
                        self.backoff[opposite.index()].reject(e.now);
                    }
                }
                self.search = Search::Hold { reference };
                self.block = Estimate::default();
                self.engine_block = Estimate::default();
            }
            Search::Baseline | Search::Hold { .. } => {
                let rate = self.block;
                let reference = match self.search {
                    Search::Hold { reference } => reference,
                    _ => rate,
                };
                let changed = rate.lower() > reference.upper() || rate.upper() < reference.lower();
                if changed && rate.count < 4 {
                    return before != self.desired;
                }
                if !changed && rate.radius() > reference.radius() && rate.count < Self::MAX_SAMPLES
                {
                    return before != self.desired;
                }
                if changed {
                    self.backoff = [Backoff::default(); 5];
                    self.productivity = None;
                    self.cursor = 0;
                }
                self.block = Estimate::default();
                let engine_baseline = self.engine_block;
                self.engine_block = Estimate::default();
                let horizon = if self
                    .measured_productivity(e.now)
                    .is_some_and(|p| p.connections > 0.0 || p.assignments > 0.0)
                {
                    Self::WINDOW * 4
                } else {
                    Self::WINDOW * Self::EXPLORATION_WINDOWS
                };
                let Some(remaining) = e.remaining.filter(|_| rate.mean > 0.0) else {
                    self.search = Search::Hold { reference: rate };
                    return before != self.desired;
                };
                self.search = Search::Hold {
                    reference: if changed { rate } else { reference },
                };
                for _ in 0..Move::ALL.len() {
                    let movement = Move::ALL[self.cursor];
                    self.cursor = (self.cursor + 1) % Move::ALL.len();
                    if e.constraint == Constraint::Local && !movement.decrease() {
                        continue;
                    }
                    if !self.backoff[movement.index()].available(e.now) {
                        continue;
                    }
                    let required = if movement.decrease() {
                        Self::WINDOW * u32::from(Self::MAX_SAMPLES)
                    } else {
                        horizon + e.setup
                    };
                    if remaining as f64 / rate.mean <= required.as_secs_f64() {
                        continue;
                    }
                    if let Some(target) = self.candidate(movement, e.multiplexed, limits) {
                        self.search = Search::Trial {
                            previous: self.desired,
                            baseline: rate,
                            engine_baseline,
                            movement,
                        };
                        self.desired = target;
                        break;
                    }
                }
            }
        }
        before != self.desired
    }
    fn candidate(
        &self,
        movement: Move,
        multiplexed: bool,
        limits: TransportLimits,
    ) -> Option<Topology> {
        let mut t = self.desired;
        match movement {
            Move::FewerConnections if t.connections > 1 => {
                t.connections = if self.backoff[movement.index()].failures == 0 {
                    t.connections.div_ceil(2)
                } else {
                    t.connections - 1
                }
            }
            Move::FewerStreams if multiplexed && t.streams_per_connection > 1 => {
                t.streams_per_connection = if self.backoff[movement.index()].failures == 0 {
                    t.streams_per_connection.div_ceil(2)
                } else {
                    t.streams_per_connection - 1
                }
            }
            Move::MoreConnections => {
                t.connections = t
                    .connections
                    .saturating_add(self.connection_step)
                    .min(limits.max_physical_connections)
            }
            Move::MoreStreams if multiplexed => {
                t.streams_per_connection = t
                    .streams_per_connection
                    .saturating_add(self.stream_step)
                    .min(limits.max_streams_per_connection)
            }
            Move::Redistribute if multiplexed && t.streams_per_connection > 1 => {
                t.connections = t
                    .connections
                    .saturating_mul(2)
                    .min(limits.max_physical_connections);
                t.streams_per_connection = t.streams_per_connection.div_ceil(2);
            }
            _ => return None,
        }
        (t != self.desired
            && t.connections <= limits.max_physical_connections
            && t.slots() <= u32::from(limits.max_active_assignments))
        .then_some(t)
    }
    fn learn(
        &mut self,
        previous: Topology,
        baseline: Estimate,
        measured: Estimate,
        movement: Move,
        now: Instant,
        engine_gain: f64,
    ) {
        if matches!(movement, Move::Redistribute) {
            return;
        }
        let (low, high, a, b) = if movement.decrease() {
            (self.desired, previous, measured, baseline)
        } else {
            (previous, self.desired, baseline, measured)
        };
        let slots_delta = f64::from(high.slots() - low.slots());
        let gain = if engine_gain > 0.0 {
            (b.lower() - a.upper()).max(0.0)
        } else {
            0.0
        };
        let elasticity = if a.mean > 0.0 {
            (gain / a.mean * f64::from(low.slots()) / slots_delta).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let mut productivity = self.productivity(now);
        productivity.assignments = elasticity;
        if high.connections != low.connections {
            productivity.connections = elasticity;
            if elasticity > 0.0 {
                self.connection_step = high.connections;
            }
        } else if elasticity > 0.0 {
            self.stream_step = high.streams_per_connection;
        }
        self.productivity = Some((productivity, now));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    struct Model {
        planner: JobPlanner,
        now: Instant,
        bytes: u64,
        engine_bytes: u128,
        limits: TransportLimits,
    }
    impl Model {
        fn new(size: u64, multiplexed: bool) -> Self {
            let limits = TransportLimits::default();
            let mut planner = JobPlanner::new(TopologyPolicy::Adaptive, limits);
            planner.start(
                Some(size),
                multiplexed,
                Duration::from_millis(25),
                None,
                limits,
            );
            Self {
                planner,
                now: Instant::now(),
                bytes: 0,
                engine_bytes: 0,
                limits,
            }
        }
        fn step(
            &mut self,
            rate: f64,
            remaining: u64,
            multiplexed: bool,
            constraint: Constraint,
            context: u64,
        ) -> bool {
            self.step_total(rate, rate, remaining, multiplexed, constraint, context)
        }
        fn step_total(
            &mut self,
            rate: f64,
            total_rate: f64,
            remaining: u64,
            multiplexed: bool,
            constraint: Constraint,
            context: u64,
        ) -> bool {
            self.now += JobPlanner::WINDOW;
            self.bytes += (rate * 1048576.0 * JobPlanner::WINDOW.as_secs_f64()) as u64;
            self.engine_bytes +=
                (total_rate * 1048576.0 * JobPlanner::WINDOW.as_secs_f64()) as u128;
            self.planner.observe(
                Evidence {
                    now: self.now,
                    useful_bytes: self.bytes,
                    engine_bytes: self.engine_bytes,
                    remaining: Some(remaining),
                    constraint,
                    multiplexed,
                    setup: Duration::from_millis(25),
                    workload_context: context,
                },
                self.limits,
            )
        }
    }
    #[rstest]
    #[case(false, Topology::new(2, 1))]
    #[case(true, Topology::new(1, 1))]
    fn cold_start_is_bounded_protocol_aware_and_respects_manual_limits(
        #[case] mux: bool,
        #[case] expected: Topology,
    ) {
        assert_eq!(Model::new(128 << 20, mux).planner.desired(), expected);
        assert_eq!(
            Model::new(1 << 20, mux).planner.desired(),
            Topology::new(1, 1)
        );
        let limits = TransportLimits {
            max_active_assignments: 1,
            ..Default::default()
        };
        let mut p = JobPlanner::new(TopologyPolicy::Adaptive, limits);
        p.start(Some(1 << 30), mux, Duration::ZERO, None, limits);
        assert_eq!(p.desired(), Topology::new(1, 1));
        let fixed = Topology::new(3, 2);
        let mut p = JobPlanner::new(TopologyPolicy::Fixed(fixed), Default::default());
        p.start(Some(1 << 30), mux, Duration::ZERO, None, Default::default());
        assert_eq!(p.desired(), fixed);
        let mut p = JobPlanner::new(TopologyPolicy::Adaptive, Default::default());
        p.start(
            Some(1 << 30),
            true,
            Duration::ZERO,
            Some(8.0 * 1048576.0),
            Default::default(),
        );
        assert_eq!(p.desired(), Topology::new(1, 2));
    }
    #[rstest]
    #[case(Constraint::Global)]
    #[case(Constraint::Local)]
    #[case(Constraint::Source)]
    fn constraints_freeze_exploration_without_expanding(#[case] constraint: Constraint) {
        let mut m = Model::new(1 << 30, false);
        for _ in 0..50 {
            m.step(1.0, 1 << 30, false, constraint, 0);
        }
        assert!(m.planner.desired().connections <= 2);
    }
    #[test]
    fn peer_changes_and_failed_progress_invalidate_comparisons() {
        let mut m = Model::new(1 << 30, false);
        for _ in 0..3 {
            m.step(16.0, 1 << 30, false, Constraint::None, 0);
        }
        assert!(matches!(m.planner.search, Search::Trial { .. }));
        let unqualified = m.planner.desired();
        m.step(1.0, 1 << 30, false, Constraint::None, 1);
        assert_eq!(m.planner.desired(), unqualified);
        assert!(matches!(m.planner.search, Search::Baseline));
        m.bytes = 0;
        m.step(1.0, 1 << 30, false, Constraint::None, 1);
        assert!(matches!(m.planner.search, Search::Baseline));
    }
    #[test]
    fn near_completion_stops_new_experiments_and_stalls_are_observed() {
        let mut m = Model::new(1 << 20, false);
        for _ in 0..40 {
            m.step(10.0, 1024, false, Constraint::None, 0);
        }
        assert_eq!(m.planner.desired(), Topology::new(1, 1));
        for _ in 0..10 {
            m.step(0.0, 1 << 30, false, Constraint::None, 0);
        }
        assert_eq!(m.planner.desired(), Topology::new(1, 1));
    }
    #[test]
    fn local_pressure_preserves_sizing_rate_and_allows_contraction() {
        let mut m = Model::new(1 << 34, true);
        m.planner.desired = Topology::new(1, 2);
        for _ in 0..12 {
            m.step(512.0, 1 << 34, true, Constraint::Local, 0);
        }
        assert_eq!(m.planner.desired(), Topology::new(1, 1));
        assert!(m.planner.assignment_quantum().unwrap() >= 128 << 20);
    }

    fn curve(profile: usize, t: Topology) -> f64 {
        let c = f64::from(t.connections);
        let s = f64::from(t.streams_per_connection);
        match profile {
            0 => 8.0 * c.min(4.0),
            1 => 32.0,
            2 => (100.0 * c).min(300.0) / (1.0 + 0.2 * (s - 1.0)),
            3 => 240.0 / (1.0 + 0.03 * (s - 1.0)),
            _ => unreachable!(),
        }
    }
    #[test]
    fn changing_capacity_reoptimizes_without_unbounded_noise_churn() {
        let mut m = Model::new(1 << 34, false);
        let mut saw_four = false;
        let mut contracted = false;
        let mut recovered = false;
        for tick in 0..200 {
            let c = m.planner.desired().connections;
            let rate = if (60..120).contains(&tick) {
                8.0
            } else {
                8.0 * f64::from(c.min(4))
            };
            if tick < 60 {
                saw_four |= c >= 4;
            }
            if (80..120).contains(&tick) {
                contracted |= c == 1;
            }
            if tick > 140 {
                recovered |= c >= 4;
            }
            m.step(rate, 1 << 34, false, Constraint::None, 0);
        }
        assert!(
            saw_four && contracted && recovered,
            "initial={saw_four}, contracted={contracted}, recovered={recovered}"
        );
        let mut m = Model::new(1 << 34, true);
        let mut changes = 0;
        for tick in 0..240 {
            let noise = [0.8, 1.2, 0.9, 1.1][tick % 4];
            changes += usize::from(m.step(100.0 * noise, 1 << 34, true, Constraint::None, 0));
        }
        assert!(changes < 30, "noise caused {changes} changes");
        assert!(m.planner.desired().slots() <= 2);
    }

    #[test]
    fn qualified_productivity_survives_grant_feedback_but_expires() {
        let mut m = Model::new(1 << 30, false);
        for _ in 0..5 {
            let c = m.planner.desired().connections;
            m.step(8.0 * f64::from(c), 1 << 30, false, Constraint::None, 0);
        }
        let before = m.planner.productivity(m.now);
        m.planner.invalidate();
        assert_eq!(
            m.planner.productivity(m.now).connections,
            before.connections
        );
        assert_eq!(
            m.planner
                .productivity(m.now + Duration::from_secs(33))
                .connections,
            1.0
        );
    }
    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(3)]
    fn saturated_shared_link_does_not_create_a_connection_arms_race(#[case] stagger: usize) {
        let mut models = [Model::new(1 << 34, true), Model::new(1 << 34, true)];
        let mut late_connections = 0;
        for tick in 0..240 {
            let topologies = models.each_ref().map(|m| m.planner.desired());
            let total: f64 = topologies.iter().map(|t| f64::from(t.connections)).sum();
            let rates = topologies.map(|own| {
                16.0 * f64::from(own.connections)
                    / total
                    / (1.0 + 0.03 * f64::from(own.streams_per_connection - 1))
            });
            for i in 0..2 {
                if i == 1 && tick < stagger {
                    continue;
                }
                let own = topologies[i];
                models[i].step_total(
                    rates[i],
                    rates.iter().sum(),
                    1 << 34,
                    true,
                    Constraint::None,
                    0,
                );
                if tick >= 200 {
                    late_connections += usize::from(own.connections);
                }
            }
        }
        eprintln!(
            "SHARED mean_late_connections={} oracle=2",
            late_connections as f64 / 40.0
        );
        assert!(
            late_connections as f64 / 40.0 < 3.0,
            "saturated peers hoarded {late_connections} connection-ticks"
        );
    }
    #[test]
    fn independent_jobs_can_both_expand_when_joint_progress_improves() {
        let mut models = [Model::new(1 << 34, false), Model::new(1 << 34, false)];
        let mut useful = 0;
        for tick in 0..160 {
            let rates = models
                .each_ref()
                .map(|m| 8.0 * f64::from(m.planner.desired().connections.min(4)));
            for i in 0..2 {
                models[i].step_total(
                    rates[i],
                    rates.iter().sum(),
                    1 << 34,
                    false,
                    Constraint::None,
                    0,
                );
            }
            if tick >= 120 {
                useful += usize::from(rates.iter().all(|rate| *rate >= 32.0));
            }
        }
        assert!(
            useful >= 32,
            "independent jobs failed to retain useful parallelism: {useful}/40"
        );
    }
    #[test]
    fn bandwidth_taken_from_a_peer_is_not_productivity() {
        let mut m = Model::new(1 << 34, false);
        for _ in 0..3 {
            m.step_total(16.0, 16.0, 1 << 34, false, Constraint::None, 0);
        }
        assert_eq!(m.planner.desired().connections, 4);
        for _ in 0..2 {
            m.step_total(24.0, 16.0, 1 << 34, false, Constraint::None, 0);
        }
        assert_eq!(m.planner.desired().connections, 2);
        assert_eq!(m.planner.productivity(m.now).connections, 0.0);
    }
    #[rstest]
    #[case(1.0, 1)]
    #[case(4.0, 2)]
    fn contraction_respects_joint_progress_and_priority(#[case] weight: f64, #[case] expected: u8) {
        let mut m = Model::new(1 << 34, false);
        m.planner.cursor = Move::FewerConnections.index();
        for _ in 0..3 {
            m.step_total(
                32.0 / 3.0,
                weight * 32.0 / 3.0 + 16.0 / 3.0,
                1 << 34,
                false,
                Constraint::None,
                0,
            );
        }
        for _ in 0..2 {
            m.step_total(8.0, weight * 8.0 + 8.0, 1 << 34, false, Constraint::None, 0);
        }
        assert_eq!(m.planner.desired().connections, expected);
    }
    #[rstest]
    #[case(0, false)]
    #[case(1, false)]
    #[case(2, true)]
    #[case(3, true)]
    fn completion_quality_against_feasible_oracle(#[case] profile: usize, #[case] mux: bool) {
        let size = 4096u64;
        let mut m = Model::new(size << 20, mux);
        let mut left = size as f64;
        let mut elapsed = 0.0;
        let mut changes = 0;
        let mut first_four = None;
        let mut setup = 0.0;
        while left > 0.0 {
            let topology = m.planner.desired();
            if topology.connections >= 4 && first_four.is_none() {
                first_four = Some(elapsed);
            }
            let rate = curve(profile, topology);
            left = (left - rate * 0.25).max(0.0);
            elapsed += 0.25;
            if m.step(rate, (left * 1048576.0) as u64, mux, Constraint::None, 0) {
                changes += 1;
                let added = m
                    .planner
                    .desired()
                    .connections
                    .saturating_sub(topology.connections);
                if added > 0 {
                    setup += 0.025;
                    elapsed += 0.025;
                    m.planner.suspend();
                }
            }
            assert!(elapsed < 1000.0);
        }
        let best = (1..=8)
            .flat_map(|c| {
                (1..=if mux { 8 / c } else { 1 }).map(move |s| Topology::new(c, s.into()))
            })
            .map(|t| curve(profile, t))
            .fold(0.0, f64::max);
        let oracle = size as f64 / best;
        eprintln!(
            "MODEL profile={profile} completion={elapsed:.3} oracle={oracle:.3} ratio={:.3} changes={changes} setup={setup:.3} first_four={first_four:?}",
            elapsed / oracle
        );
        assert!(
            elapsed / oracle < 1.3,
            "profile={profile}, elapsed={elapsed}, oracle={oracle}"
        );
        if profile == 0 {
            assert!(first_four.is_some_and(|t| t <= 3.0));
        }
        if profile == 1 || profile == 3 {
            assert!(changes < 30, "unbounded plateau churn: {changes}");
        }
        assert_eq!(
            m.planner.desired(),
            Topology::new([4, 1, 3, 1][profile], 1),
            "must retire surplus resources, profile={profile}"
        );
    }
}
