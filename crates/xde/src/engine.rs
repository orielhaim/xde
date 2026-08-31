use std::{path::PathBuf, sync::Arc, thread::JoinHandle};

use crate::core::{
    Error, Result,
    policy::{EngineLimits, TransferPolicy},
    spec::{Durability, IntegritySpec, JobSpec, Priority, SourceRequest, Urgency},
};
use crate::net::Connector;
use crate::storage::MemoryBudget;
use url::Url;

use crate::{control::ControlCommand, control::ControlHandle, job::Job, netctx};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    limits: EngineLimits,
    shards: usize,
    memory_limit_bytes: u64,
    /// Expert knob: speak HTTP/2 with prior knowledge (h2c) to every
    /// plaintext origin. For benchmarks and origins explicitly configured
    /// for it; `HttpVersionPolicy::Auto` never guesses this.
    h2_prior_knowledge: bool,
    /// Test fixture escape hatch: skip certificate verification for HTTP/3
    /// against local self-signed QUIC servers.
    danger_accept_invalid_certs: bool,
    /// Persistent-learning storage. When set, transport evidence from
    /// earlier runs is loaded at startup and written back on shutdown.
    profile_path: Option<PathBuf>,
}

/// Threads that must be joined exactly once, whether teardown comes from an
/// explicit `Engine::shutdown()` or the last handle dropping.
struct RunningState {
    runtime: crate::runtime::Runtime,
    control_thread: JoinHandle<()>,
}

impl RunningState {
    /// Deterministic teardown. Dropping the runtime first closes every
    /// shard-side socket and lane; joining the control thread afterwards
    /// guarantees every job waiter has been answered before this returns.
    /// The loop itself terminates via the shutdown command (see
    /// `run_loop`): its own `obs_tx` clone keeps the observation channel
    /// connected until it exits, so mailbox disconnect alone cannot be the
    /// exit signal.
    fn join(self) {
        drop(self.runtime);
        let _ = self.control_thread.join();
    }
}

struct EngineInner {
    config: EngineConfig,
    control: ControlHandle,
    events: crate::core::events::EventStream,
    state: std::sync::Mutex<Option<RunningState>>,
}

impl EngineInner {
    /// Cancel every active job, stop the control loop, and join all
    /// threads. Idempotent; safe from both `shutdown()` and `Drop`.
    fn terminate(&self) {
        // Ask the loop to wind down. `Disconnected` means it already exited;
        // a timeout means it is wedged and joining would block indefinitely,
        // so teardown degrades to best-effort.
        let loop_gone = matches!(
            self.control.shutdown(),
            Err(Error::Runtime(crate::core::RuntimeError::EngineGone))
        );
        if let Ok(mut guard) = self.state.lock()
            && let Some(state) = guard.take()
            && !loop_gone
        {
            state.join();
        }
    }
}

impl std::fmt::Debug for EngineInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineInner")
            .field("config", &self.config)
            .finish()
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// The transfer engine.
///
/// Owns the Compio shard pool, the control-plane thread and the resolver
/// thread. Clones share one engine; everything shuts down when the last
/// handle drops. Jobs describe intent; the engine owns all resources and
/// makes every scheduling decision through its pure controller.
#[derive(Debug, Clone)]
pub struct Engine(Arc<EngineInner>);

#[derive(Debug, Clone)]
pub struct EngineBuilder(EngineConfig);

impl Default for EngineBuilder {
    fn default() -> Self {
        Self(EngineConfig {
            limits: EngineLimits::default(),
            shards: 2.min(std::thread::available_parallelism().map_or(1, usize::from)),
            memory_limit_bytes: 256 * 1024 * 1024,
            h2_prior_knowledge: false,
            danger_accept_invalid_certs: false,
            profile_path: None,
        })
    }
}

impl EngineBuilder {
    pub fn limits(mut self, limits: EngineLimits) -> Self {
        self.0.limits = limits;
        self
    }

    pub fn shards(mut self, count: usize) -> Self {
        self.0.shards = count.max(1);
        self
    }

    pub fn memory_limit(mut self, bytes: u64) -> Self {
        self.0.memory_limit_bytes = bytes.max(1);
        self
    }

    /// Speak HTTP/2 with prior knowledge to plaintext origins (h2c).
    pub fn h2_prior_knowledge(mut self, on: bool) -> Self {
        self.0.h2_prior_knowledge = on;
        self
    }

    /// Skip TLS certificate verification for HTTP/3 dials. For test fixtures
    /// against local self-signed QUIC servers only.
    pub fn danger_accept_invalid_certs(mut self, on: bool) -> Self {
        self.0.danger_accept_invalid_certs = on;
        self
    }

    /// Persist transport learning to `path`: evidence is loaded at engine
    /// startup and written back on shutdown, keyed semantically (origin +
    /// network context signature), never process-local IDs.
    pub fn profile_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.0.profile_path = Some(path.into());
        self
    }

    pub fn build(self) -> Result<Engine> {
        let runtime = crate::runtime::Runtime::builder()
            .shards(self.0.shards)
            .build()?;
        let memory = MemoryBudget::new(self.0.memory_limit_bytes);

        // Observation mailbox: shards and the resolver publish; the control
        // loop consumes.
        let (obs_tx, obs_rx) = flume::bounded(4096);

        let connector_config = crate::net::ConnectorConfig {
            prior_knowledge_http2: self.0.h2_prior_knowledge,
            danger_accept_invalid_certs: self.0.danger_accept_invalid_certs,
            ..Default::default()
        };
        let connector = Arc::new(Connector::new(connector_config)?);

        // Destination-capacity accounting shared by every shard's lanes: a
        // destination's ceilings hold globally, not per lane.
        let capacity = Arc::new(crate::storage::DestinationCapacityRegistry::new(
            512 * 1024 * 1024, // max inflight bytes
            64,                // max inflight operations
        ));

        // Resident shard service per Compio shard.
        let mut shard_txs = Vec::with_capacity(self.0.shards);
        for shard in 0..self.0.shards {
            let (tx, rx) = flume::bounded::<crate::shard::ShardCommand>(1024);
            shard_txs.push(tx);
            let service = crate::shard::ShardService::new(
                shard,
                rx,
                obs_tx.clone(),
                connector.clone(),
                memory.clone(),
                capacity.clone(),
            );
            runtime.handle().drive_local(shard, move || async move {
                service.run().await;
            })?;
        }

        let resolver = crate::resolve::ResolverHandle::spawn();
        // Public telemetry: one bounded channel, one logical consumer.
        // Terminal job state always travels through `Job`, never here.
        let (event_sink, event_stream) = crate::core::events::EventSink::channel();
        let network_context = netctx::detect();
        let (control, control_thread) = crate::control::ControlPlane::spawn_with_profile(
            self.0.shards,
            self.0.limits,
            obs_rx,
            obs_tx,
            shard_txs,
            resolver,
            Some(event_sink),
            Some(network_context),
            memory.clone(),
            self.0.profile_path.clone(),
        );

        Ok(Engine(Arc::new(EngineInner {
            config: self.0,
            control,
            events: event_stream,
            state: std::sync::Mutex::new(Some(RunningState {
                runtime,
                control_thread,
            })),
        })))
    }
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }

    /// Subscribe to engine telemetry. Clones share the same bounded stream;
    /// under pressure, progress-class events are dropped first by design.
    pub fn events(&self) -> crate::core::events::EventStream {
        self.0.events.clone()
    }

    /// Compact engine-global snapshot: active jobs, connections, streams,
    /// protocol distribution, memory pressure. Read-only projection, safe
    /// to poll from UI code.
    pub fn snapshot(&self) -> Result<crate::snapshot::EngineSnapshot> {
        self.0
            .control
            .engine_snapshot()
            .ok_or(Error::Runtime(crate::core::RuntimeError::EngineGone))
    }

    /// Graceful shutdown: cancel every active job (journals are checkpointed
    /// so interrupted transfers resume later), answer all waiters with
    /// `Error::Cancelled`, stop the control loop, and join the control and
    /// shard threads. Fully synchronous: when this returns, every resource
    /// the engine owned has been released. Dropping the final `Engine`
    /// handle runs the same path; this is the explicit form.
    pub fn shutdown(&self) -> Result<()> {
        self.0.control.shutdown()?;
        if let Ok(mut guard) = self.0.state.lock()
            && let Some(state) = guard.take()
        {
            state.join();
        }
        Ok(())
    }

    pub fn download(&self, url: impl AsRef<str>) -> DownloadBuilder {
        DownloadBuilder {
            engine: self.clone(),
            url: url.as_ref().to_owned(),
            mirrors: Vec::new(),
            destination: None,
            shared_destination: None,
            priority: Priority::Normal,
            urgency: Urgency::ThroughputSensitive,
            integrity: IntegritySpec::default(),
            durability: Durability::default(),
            timeout: None,
            policy: TransferPolicy::default(),
            refresher: None,
            progress: None,
        }
    }

    pub(crate) fn submit(
        &self,
        spec: JobSpec,
        final_path: Option<PathBuf>,
        shared_destination: Option<
            std::sync::Arc<dyn crate::storage::DynDestination + Send + Sync>,
        >,
        refresher: Option<std::sync::Arc<dyn crate::core::credentials::SourceRefresher>>,
        progress: Option<crate::progress::ProgressPublisher>,
    ) -> Result<Job> {
        let mut spec = spec;
        spec.policy = self.0.config.limits.clamp_policy(spec.policy);
        let destination_key = final_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("anon-{}-{}", std::process::id(), next_counter()));
        let (admit_tx, admit_rx) = flume::bounded(1);
        let (result_tx, result_rx) = flume::bounded(1);
        self.0.control.submit(ControlCommand {
            spec: Box::new(spec),
            destination_key,
            final_path,
            shared_destination,
            refresher,
            progress,
            shutdown: false,
            admit_tx,
            result_tx,
        })?;
        let job_id = admit_rx
            .recv()
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineGone))??;
        Ok(Job::new(job_id, result_rx, self.0.control.clone()))
    }
}

fn next_counter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

pub struct DownloadBuilder {
    engine: Engine,
    url: String,
    /// Additional mirrors for the same artifact, in preference order.
    mirrors: Vec<String>,
    destination: Option<PathBuf>,
    /// Shared custom destination (mutually exclusive with `destination`).
    shared_destination: Option<std::sync::Arc<dyn crate::storage::DynDestination + Send + Sync>>,
    priority: Priority,
    urgency: Urgency,
    integrity: IntegritySpec,
    durability: Durability,
    timeout: Option<std::time::Duration>,
    policy: TransferPolicy,
    refresher: Option<std::sync::Arc<dyn crate::core::credentials::SourceRefresher>>,
    progress: Option<std::sync::Arc<dyn Fn(crate::DownloadProgress) + Send + Sync>>,
}

impl DownloadBuilder {
    pub fn to(mut self, path: impl Into<PathBuf>) -> Self {
        self.destination = Some(path.into());
        self.shared_destination = None;
        self
    }

    /// Download into an application-provided random-access destination.
    ///
    /// The destination's [`crate::storage::DestinationCaps`] drive behavior
    /// honestly:
    /// - without `OUT_OF_ORDER` the job runs as a single ordered stream;
    /// - `max_parallel_writes` caps concurrent assignments;
    /// - expected-digest verification requires `READ_BACK` (the job fails
    ///   at admission otherwise - verification is never silently skipped);
    /// - resume journals do not apply; a failed non-`IDEMPOTENT_REWRITE`
    ///   destination fails the job rather than rewriting bytes.
    pub fn destination(
        mut self,
        dest: std::sync::Arc<dyn crate::storage::DynDestination + Send + Sync>,
    ) -> Self {
        self.destination = None;
        self.shared_destination = Some(dest);
        self
    }

    /// Download into an application-provided sequential sink. The engine
    /// inserts bounded reordering so segmented transfer still works, with
    /// memory bounded by the reorder buffer.
    pub fn sequential_destination<D>(mut self, dest: D) -> Self
    where
        D: crate::storage::SequentialDestination + Send + 'static,
    {
        let reordered = crate::storage::ReorderingDestination::new(dest, 8 * 1024 * 1024);
        self.destination = None;
        self.shared_destination = Some(std::sync::Arc::new(reordered));
        self
    }

    /// Add a mirror for the same artifact. Mirrors are used as failover by
    /// default; when the job carries an expected digest (strong artifact
    /// equivalence), healthy mirrors may serve ranges simultaneously and
    /// the brain distributes work by measured mirror speed.
    pub fn mirror(mut self, url: impl AsRef<str>) -> Self {
        self.mirrors.push(url.as_ref().to_owned());
        self
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn urgency(mut self, urgency: Urgency) -> Self {
        self.urgency = urgency;
        self
    }

    pub fn integrity(mut self, integrity: IntegritySpec) -> Self {
        self.integrity = integrity;
        self
    }

    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    pub fn policy(mut self, policy: TransferPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn on_progress(
        mut self,
        callback: impl Fn(crate::DownloadProgress) + Send + Sync + 'static,
    ) -> Self {
        self.progress = Some(std::sync::Arc::new(callback));
        self
    }

    pub fn refresher(
        mut self,
        refresher: std::sync::Arc<dyn crate::core::credentials::SourceRefresher>,
    ) -> Self {
        self.refresher = Some(refresher);
        self
    }

    pub fn start(self) -> Result<Job> {
        let url = Url::parse(&self.url).map_err(|e| Error::Config(e.to_string()))?;
        let mut sources = smallvec::SmallVec::new();
        sources.push(SourceRequest::new(url));
        for m in &self.mirrors {
            let mu = Url::parse(m).map_err(|e| Error::Config(e.to_string()))?;
            sources.push(SourceRequest::new(mu));
        }
        let spec = JobSpec {
            sources,
            integrity: self.integrity,
            priority: self.priority,
            urgency: self.urgency,
            deadline: self.timeout.map(|t| std::time::Instant::now() + t),
            persistence: self.durability,
            policy: self.policy,
            label: None,
        };
        let progress = self.progress.map(crate::progress::ProgressPublisher::spawn);
        self.engine.submit(
            spec,
            self.destination,
            self.shared_destination,
            self.refresher,
            progress,
        )
    }
}
