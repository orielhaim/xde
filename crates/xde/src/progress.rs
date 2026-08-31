use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

/// Aggregate progress for one download.
///
/// `downloaded_bytes` is derived from XDE's merged verified ranges. It never
/// double-counts overlapping pieces, retries, mirrors, or out-of-order work.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DownloadProgress {
    pub downloaded_bytes: u64,
    pub total_bytes: Option<u64>,
    pub fraction: Option<f64>,
    pub bytes_per_second: Option<f64>,
    pub eta: Option<Duration>,
}

impl DownloadProgress {
    pub(crate) fn new(done: u64, total: Option<u64>, rate: Option<f64>) -> Self {
        let fraction = total.map(|total| {
            if total == 0 {
                1.0
            } else {
                done as f64 / total as f64
            }
        });
        let eta = total
            .and_then(|total| total.checked_sub(done))
            .zip(rate.filter(|rate| *rate > 0.0))
            .map(|(remaining, rate)| Duration::from_secs_f64(remaining as f64 / rate));
        Self {
            downloaded_bytes: done,
            total_bytes: total,
            fraction,
            bytes_per_second: rate,
            eta,
        }
    }
}

struct State {
    latest: DownloadProgress,
    generation: u64,
    closed: bool,
}

pub(crate) struct ProgressPublisher {
    shared: Arc<(Mutex<State>, Condvar)>,
}

impl ProgressPublisher {
    pub(crate) fn spawn(callback: Arc<dyn Fn(DownloadProgress) + Send + Sync>) -> Self {
        let shared = Arc::new((
            Mutex::new(State {
                latest: DownloadProgress::new(0, None, None),
                generation: 1,
                closed: false,
            }),
            Condvar::new(),
        ));
        let worker = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("xde-progress".into())
            .spawn(move || run_callback(worker, callback))
            .expect("failed to start progress callback thread");
        Self { shared }
    }

    pub(crate) fn publish(&self, progress: DownloadProgress) {
        let (lock, wake) = &*self.shared;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.latest = progress;
        state.generation = state.generation.wrapping_add(1);
        wake.notify_one();
    }

    pub(crate) fn close(&self, progress: Option<DownloadProgress>) {
        let (lock, wake) = &*self.shared;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(progress) = progress {
            state.latest = progress;
            state.generation = state.generation.wrapping_add(1);
        }
        state.closed = true;
        wake.notify_one();
    }
}

impl Drop for ProgressPublisher {
    fn drop(&mut self) {
        self.close(None);
    }
}

fn run_callback(
    shared: Arc<(Mutex<State>, Condvar)>,
    callback: Arc<dyn Fn(DownloadProgress) + Send + Sync>,
) {
    let (lock, wake) = &*shared;
    let mut seen = 0;
    loop {
        let (progress, generation, closed) = {
            let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            while state.generation == seen && !state.closed {
                state = wake
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            (state.latest, state.generation, state.closed)
        };
        if generation != seen {
            seen = generation;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(progress)));
        }
        if closed {
            break;
        }
    }
}
