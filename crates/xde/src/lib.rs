//! XDE public facade.
//!
//! `Engine` owns the runtime: one control-plane thread running the pure
//! controller, one resolver thread, and one resident shard service per Compio
//! shard. Jobs express intent (`engine.download(url).to(path).start()`); the
//! engine decides everything else - endpoints, connections, ranges, shards,
//! retries.
//!
//! Ownership in one paragraph: the controller's WorldModel allocates every
//! entity ID; shards hold the non-Send resources (sockets, files) keyed by
//! those IDs; the coordinator owns destination lifecycle (lock, prealloc,
//! commit); timers live on the control thread. Bytes flow socket → hyper →
//! sink lane → positional write on one shard, never through the controller.

#![allow(dead_code, unused_imports)]

mod control;
pub(crate) mod core;
mod engine;
pub(crate) mod http;
pub(crate) mod integrity;
mod job;
pub(crate) mod net;
mod netctx;
mod progress;
mod resolve;
pub(crate) mod runtime;
pub(crate) mod shard;
mod snapshot;
pub(crate) mod storage;

pub use control::JobOutcome;
pub use engine::{DownloadBuilder, Engine, EngineBuilder};
pub use job::Job;
pub use progress::DownloadProgress;

pub use crate::core::credentials::{RefreshRequest, RefreshedSource, SourceRefresher};
pub use crate::core::events::{Event, EventStream, Protocol};
pub use crate::core::policy::{
    EngineLimits, HttpVersionPolicy, SegmentationPolicy, TransferPolicy, TransportLimits,
};
pub use crate::core::spec::{
    Digest, DigestCheck, Durability, ExpectedDigest, HashKind, IntegritySpec, Priority,
    SourceRequest, Urgency,
};
pub use crate::core::{ByteRange, Error, RangeSet, Result, RuntimeError};
pub use crate::storage::{
    ArtifactMode, BeginArtifact, CommitOutcome, DestinationCaps, DestinationHints, DynDestination,
    FlushLevel, RandomAccessDestination, ReorderingDestination, SequentialDestination,
    TransferChunk, WriteCompletion,
};
pub use snapshot::{EngineSnapshot, JobPhase, JobSnapshot};
