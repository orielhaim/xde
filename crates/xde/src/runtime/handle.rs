use std::{future::Future, pin::Pin, sync::Arc, thread};

use crate::core::{Error, Result};

enum Command {
    Run(Box<dyn FnOnce() + Send + 'static>),
    /// Install a future that is polled on the shard `block_on` root together
    /// with this command channel. Used for the resident shard service so H2
    /// I/O is never a Compio sibling of an idle `recv_async`.
    Drive(Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static>),
    Shutdown,
}

/// Handle used by the facade. Jobs enter a shard through the control plane;
/// their buffers never travel through this channel.
#[derive(Clone)]
pub struct RuntimeHandle {
    shards: Arc<[flume::Sender<Command>]>,
}

impl std::fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHandle")
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl RuntimeHandle {
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn spawn<F, T>(&self, shard: usize, future: F) -> Result<flume::Receiver<T>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = flume::bounded(1);
        let task = Box::new(move || {
            compio::runtime::spawn(async move {
                let output = future.await;
                let _ = tx.send(output);
            })
            .detach();
        });
        self.shards[shard % self.shards.len()]
            .send(Command::Run(task))
            .map_err(|_| Error::EngineGone)?;
        Ok(rx)
    }

    /// Build a non-Send future on its owning shard. Compio resources are
    /// intentionally local, so moving an already-built future across threads
    /// would violate the runtime model.
    pub fn spawn_local_with<M, F, T>(&self, shard: usize, make: M) -> Result<flume::Receiver<T>>
    where
        M: FnOnce() -> F + Send + 'static,
        F: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = flume::bounded(1);
        let task = Box::new(move || {
            compio::runtime::spawn(async move {
                let output = make().await;
                let _ = tx.send(output);
            })
            .detach();
        });
        self.shards[shard % self.shards.len()]
            .send(Command::Run(task))
            .map_err(|_| Error::EngineGone)?;
        Ok(rx)
    }

    /// Run a non-Send future on the shard `block_on` root, polled with the
    /// same waker as this shard's command mailbox.
    pub fn drive_local<M, F>(&self, shard: usize, make: M) -> Result<()>
    where
        M: FnOnce() -> F + Send + 'static,
        F: Future<Output = ()> + 'static,
    {
        let factory = Box::new(move || Box::pin(make()) as Pin<Box<dyn Future<Output = ()>>>);
        self.shards[shard % self.shards.len()]
            .send(Command::Drive(factory))
            .map_err(|_| Error::EngineGone)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Runtime {
    handle: RuntimeHandle,
    threads: Vec<thread::JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct RuntimeBuilder {
    shards: usize,
    thread_name: String,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self {
            shards: thread::available_parallelism().map_or(1, usize::from),
            thread_name: "xde-io".into(),
        }
    }
}

impl RuntimeBuilder {
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards.max(1);
        self
    }

    pub fn build(self) -> Result<Runtime> {
        let mut senders = Vec::with_capacity(self.shards);
        let mut threads = Vec::with_capacity(self.shards);
        let mut startup_receivers = Vec::with_capacity(self.shards);
        for index in 0..self.shards {
            // Bounded control mailbox: 1024 commands, explicit overload via send error
            let (tx, rx) = flume::bounded(1024);
            let (startup_tx, startup_rx) = flume::bounded(1);
            startup_receivers.push(startup_rx);
            let name = format!("{}-{index}", self.thread_name);
            let thread = thread::Builder::new()
                .name(name)
                .spawn(move || shard_main(rx, startup_tx))
                .map_err(|e| Error::Transport(crate::core::error::TransportError::Io(e)))?;
            senders.push(tx);
            threads.push(thread);
        }
        // Handshake: every shard must report successful Compio init
        for rx in startup_receivers {
            match rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    // Shutdown any already-started shards
                    for sender in &senders {
                        let _ = sender.send(Command::Shutdown);
                    }
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(Error::Transport(crate::core::error::TransportError::Io(e)));
                }
                Err(_) => {
                    for sender in &senders {
                        let _ = sender.send(Command::Shutdown);
                    }
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(Error::Config("shard failed to start".into()));
                }
            }
        }
        Ok(Runtime {
            handle: RuntimeHandle {
                shards: senders.into(),
            },
            threads,
        })
    }
}

fn shard_main(rx: flume::Receiver<Command>, startup: flume::Sender<Result<(), std::io::Error>>) {
    let runtime = match compio::runtime::Runtime::new() {
        Ok(runtime) => {
            let _ = startup.send(Ok(()));
            runtime
        }
        Err(error) => {
            let io_err = std::io::Error::other(error.to_string());
            let _ = startup.send(Err(io_err));
            tracing::error!("failed to start XDE runtime shard");
            return;
        }
    };
    runtime.block_on(async move {
        let mut drive: Option<Pin<Box<dyn Future<Output = ()>>>> = None;
        loop {
            let recv = rx.recv_async();
            let mut recv = std::pin::pin!(recv);
            let command = std::future::poll_fn(|cx| {
                if let Some(fut) = drive.as_mut()
                    && fut.as_mut().poll(cx).is_ready()
                {
                    drive = None;
                }
                recv.as_mut().poll(cx)
            })
            .await;
            match command {
                Ok(Command::Run(task)) => task(),
                Ok(Command::Drive(make)) => {
                    drive = Some(make());
                }
                Ok(Command::Shutdown) | Err(_) => break,
            }
        }
    });
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    pub fn handle(&self) -> RuntimeHandle {
        self.handle.clone()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        for shard in self.handle.shards.iter() {
            let _ = shard.send(Command::Shutdown);
        }
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
