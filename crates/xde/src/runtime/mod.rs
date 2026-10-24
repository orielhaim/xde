//! Runtime ownership and thread topology for XDE.

#![forbid(unsafe_code)]

use std::thread;

use crate::{
    core::{Error, Result},
    shard::{ShardCommand, ShardService},
};

/// Owns shard ingress and joins each resident service before its Compio
/// runtime is destroyed. There is one command queue per shard.
pub struct ShardPool {
    ingress: Vec<flume::Sender<ShardCommand>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl ShardPool {
    pub fn start(
        services: Vec<ShardService>,
        ingress: Vec<flume::Sender<ShardCommand>>,
    ) -> Result<Self> {
        assert_eq!(services.len(), ingress.len());
        let mut pool = Self {
            ingress,
            threads: Vec::new(),
        };
        let mut startup = Vec::new();
        for (index, service) in services.into_iter().enumerate() {
            let (ready, done) = flume::bounded(1);
            let thread = thread::Builder::new()
                .name(format!("xde-io-{index}"))
                .spawn(move || match compio::runtime::Runtime::new() {
                    Ok(runtime) => {
                        let _ = ready.send(Ok(()));
                        runtime.block_on(service.run());
                    }
                    Err(error) => {
                        let _ = ready.send(Err(std::io::Error::other(error.to_string())));
                    }
                })
                .map_err(|e| Error::Transport(crate::core::error::TransportError::Io(e)))?;
            pool.threads.push(thread);
            startup.push(done);
        }
        for ready in startup {
            ready
                .recv()
                .map_err(|_| Error::Config("shard startup failed".into()))?
                .map_err(|e| Error::Transport(crate::core::error::TransportError::Io(e)))?;
        }
        Ok(pool)
    }

    pub fn senders(&self) -> Vec<flume::Sender<ShardCommand>> {
        self.ingress.clone()
    }
}

impl Drop for ShardPool {
    fn drop(&mut self) {
        self.ingress.clear();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
