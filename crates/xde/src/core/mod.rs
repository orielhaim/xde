//! Core domain model, control plane and scheduling policy for XDE.
//!
//! This crate contains **no I/O**. Everything here is pure logic over
//! observations, which is what makes the controller testable with proptest.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod context;
pub mod controller;
pub mod credentials;
pub mod disposition;
pub mod error;
pub mod events;
pub mod ewma;
pub mod ids;
pub mod metrics;
pub mod policy;
pub mod profile;
pub mod provenance;
pub mod ranges;
pub mod representation;
pub mod segment;
pub mod sink;
pub mod spec;
pub mod timers;
pub mod units;
pub mod world;

pub use disposition::Disposition;
pub use error::{Error, Result, RuntimeError};
pub use ids::{
    ArtifactId, AssignId, AssignmentRef, ConnectionId, DestinationId, EndpointId, JobId,
    NetworkContextId, OriginId, PathId, SessionId, SourceId, StreamId,
};
pub use provenance::ArtifactProvenance;
pub use ranges::{ByteRange, RangeSet};
pub use representation::{
    JournaledFingerprint, RemoteFingerprint, RepresentationLock, ValidatorMatch, ValidatorStrength,
    format_http_date, parse_http_date,
};
pub use units::{Bytes, Rate};
pub use world::WorldModel;

/// Re-exported for controller observation payloads.
pub use spec::{JobSpec, SourceRequest};
