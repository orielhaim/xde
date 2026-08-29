//! Compact public projections of engine/job state.
//!
//! A snapshot is a read-only projection built from the control plane's
//! WorldModel - never the model itself. Fields are UI-shaped: verified
//! bytes, rates, active resource counts.

use crate::core::events::Protocol;

/// Current state of one job, projected for consumers/UIs.
#[derive(Debug, Clone)]
pub struct JobSnapshot {
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
    Transferring,
    Committing,
    Completed,
    Cancelling,
    Failed,
}

impl From<crate::core::world::JobPhase> for JobPhase {
    fn from(p: crate::core::world::JobPhase) -> Self {
        use crate::core::world::JobPhase as P;
        match p {
            P::Created => JobPhase::Created,
            P::Probing => JobPhase::Probing,
            P::Transferring => JobPhase::Transferring,
            P::Committing => JobPhase::Committing,
            P::Completed => JobPhase::Completed,
            P::Cancelling => JobPhase::Cancelling,
            P::Failing => JobPhase::Cancelling,
            P::Failed => JobPhase::Failed,
        }
    }
}

/// Compact immutable view of ENGINE-GLOBAL state. Cheap to take, safe for
/// UI polling; read-only projection, never the model itself.
#[derive(Debug, Clone)]
pub struct EngineSnapshot {
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
