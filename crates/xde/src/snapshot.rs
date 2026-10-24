//! Compact public projections of engine/job state.
//!
//! A snapshot is a read-only projection built from the control plane's
//! WorldModel - never the model itself. Fields are UI-shaped: verified
//! bytes, rates, active resource counts.

use crate::core::events::Protocol;

/// Current state of one job, projected for consumers/UIs.
#[derive(Debug, Clone)]
pub struct JobSnapshot {
    pub desired: crate::Topology,
    pub granted: crate::ResourceGrant,
    pub queued: bool,
    pub globally_constrained: bool,
    pub opening_connections: usize,
    /// Consumed payload, including active unverified ranges. Not publication evidence.
    pub consumed_bytes: u64,
    pub phase: JobPhase,
    /// Total artifact length, when known.
    pub total_length: Option<u64>,
    /// HTTP-verified (and destination-committed) bytes.
    pub verified_bytes: u64,
    /// Bytes seeded from a previous run's journal.
    pub resumed_bytes: u64,
    /// Median pure-receive rate across active endpoints, when measurable.
    pub receive_rate_bps: Option<f64>,
    pub active_connections: usize,
    /// Active assignments (streams) claimed by this job.
    pub active_streams: usize,
    /// Protocol(s) in use by the job's live connections.
    pub protocols: Vec<Protocol>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPhase {
    Created,
    Probing,
    Preparing,
    Transferring,
    Pausing,
    Paused,
    Committing,
    Restarting,
    Cancelling,
    Stopping,
}

impl From<&crate::core::world::JobPhase> for JobPhase {
    fn from(p: &crate::core::world::JobPhase) -> Self {
        use crate::core::world::JobPhase as P;
        match p {
            P::Created => JobPhase::Created,
            P::Probing => JobPhase::Probing,
            P::Preparing => JobPhase::Preparing,
            P::StoppingPreparation(crate::core::world::JobFailure::Cancelled)
            | P::StoppingDestination(crate::core::world::JobFailure::Cancelled) => {
                JobPhase::Cancelling
            }
            P::StoppingPreparation(_) | P::StoppingDestination(_) => JobPhase::Stopping,
            P::Transferring => JobPhase::Transferring,
            P::Pausing => JobPhase::Pausing,
            P::Paused => JobPhase::Paused,
            P::Committing => JobPhase::Committing,
            P::Restarting | P::Resetting => JobPhase::Restarting,
            P::Stopping(crate::core::world::JobFailure::Cancelled) => JobPhase::Cancelling,
            P::Stopping(_) => JobPhase::Stopping,
        }
    }
}

/// Compact immutable view of ENGINE-GLOBAL state. Cheap to take, safe for
/// UI polling; read-only projection, never the model itself.
#[derive(Debug, Clone)]
pub struct EngineSnapshot {
    pub controller: ControllerStats,
    pub queued_jobs: usize,
    /// Jobs currently admitted and not terminal.
    pub active_jobs: usize,
    /// Live physical connections across all shards/origins.
    pub physical_connections: usize,
    /// In-flight assignments across all jobs.
    pub active_streams: usize,
    /// Live connection count per negotiated protocol.
    pub protocol_counts: Vec<(Protocol, usize)>,
    /// Distinct origins with at least one live connection.
    pub active_origins: usize,
    /// Transfer-memory bytes currently reserved / total budget.
    pub memory_used: u64,
    pub memory_limit: u64,
}

/// Control-plane work counts. Active time is populated only with diagnostics enabled.
#[derive(Debug, Clone, Copy, Default)]
pub struct ControllerStats {
    pub planner_bytes_per_job: usize,
    pub assignment_control_bytes: usize,
    pub peak_reserved_connections: usize,
    pub observations: u64,
    pub allocation_passes: u64,
    pub planner_evaluations: u64,
    pub planner_global_limited: u64,
    pub planner_local_limited: u64,
    pub planner_source_limited: u64,
    pub planner_unsettled: u64,
    pub planner_topology_changes: u64,
    pub active_time: std::time::Duration,
}
