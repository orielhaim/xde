use std::time::Duration;

use crate::core::units::Rate;

/// Time-decayed EWMA with a running variance estimate.
/// Variance is what lets us call a worker a straggler instead of "slower than average".
#[derive(Clone, Copy, Debug, Default)]
pub struct EwmaWithVariance {
    mean: f64,
    var: f64,
    /// Time constant in seconds.
    tau: f64,
    samples: u32,
}

impl EwmaWithVariance {
    pub fn new(tau: Duration) -> Self {
        Self {
            mean: 0.0,
            var: 0.0,
            tau: tau.as_secs_f64().max(1e-3),
            samples: 0,
        }
    }

    pub fn observe(&mut self, value: f64, dt: Duration) {
        if !value.is_finite() {
            return;
        }
        if self.samples == 0 {
            self.mean = value;
            self.var = 0.0;
            self.samples = 1;
            return;
        }
        let alpha = 1.0 - (-dt.as_secs_f64() / self.tau).exp();
        let alpha = alpha.clamp(1e-4, 1.0);
        let delta = value - self.mean;
        self.mean += alpha * delta;
        self.var = (1.0 - alpha) * (self.var + alpha * delta * delta);
        self.samples = self.samples.saturating_add(1);
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        self.mean
    }
    #[inline]
    pub fn variance(&self) -> f64 {
        self.var
    }
    #[inline]
    pub fn stddev(&self) -> f64 {
        self.var.max(0.0).sqrt()
    }
    #[inline]
    pub fn samples(&self) -> u32 {
        self.samples
    }
    #[inline]
    pub fn is_warm(&self) -> bool {
        self.samples >= 3
    }
    #[inline]
    pub fn rate(&self) -> Rate {
        Rate::from_bps(self.mean)
    }

    /// How many stddevs below the mean `value` sits. Positive == worse.
    pub fn z_below(&self, value: f64) -> f64 {
        let sd = self.stddev();
        if sd < 1e-9 {
            return if value < self.mean * 0.5 { 4.0 } else { 0.0 };
        }
        (self.mean - value) / sd
    }
}

/// Plain decayed scalar, for signals with no useful variance (queue depth, loss).
#[derive(Clone, Copy, Debug, Default)]
pub struct Ewma {
    v: f64,
    tau: f64,
    init: bool,
}

impl Ewma {
    pub fn new(tau: Duration) -> Self {
        Self {
            v: 0.0,
            tau: tau.as_secs_f64().max(1e-3),
            init: false,
        }
    }
    pub fn observe(&mut self, value: f64, dt: Duration) {
        if !value.is_finite() {
            return;
        }
        if !self.init {
            self.v = value;
            self.init = true;
            return;
        }
        let a = (1.0 - (-dt.as_secs_f64() / self.tau).exp()).clamp(1e-4, 1.0);
        self.v += a * (value - self.v);
    }
    #[inline]
    pub fn get(&self) -> f64 {
        self.v
    }
    #[inline]
    pub fn is_init(&self) -> bool {
        self.init
    }
}

/// Windowed rate meter: bytes in, rate out, with an explicit sample window so a
/// 100ms hiccup does not read as a collapse.
#[derive(Debug)]
pub struct RateMeter {
    acc: u64,
    window_start: std::time::Instant,
    window: Duration,
    ewma: EwmaWithVariance,
    last_instant_rate: Rate,
}

impl RateMeter {
    pub fn new(window: Duration, tau: Duration) -> Self {
        Self {
            acc: 0,
            window_start: std::time::Instant::now(),
            window,
            ewma: EwmaWithVariance::new(tau),
            last_instant_rate: Rate::ZERO,
        }
    }

    pub fn add(&mut self, n: u64) {
        self.acc += n;
    }

    /// Roll the window if it has elapsed. Returns the instantaneous rate if rolled.
    pub fn tick(&mut self, now: std::time::Instant) -> Option<Rate> {
        let dt = now.saturating_duration_since(self.window_start);
        if dt < self.window {
            return None;
        }
        let r = Rate::from_bps(self.acc as f64 / dt.as_secs_f64());
        self.ewma.observe(r.bps(), dt);
        self.last_instant_rate = r;
        self.acc = 0;
        self.window_start = now;
        Some(r)
    }

    #[inline]
    pub fn smoothed(&self) -> Rate {
        self.ewma.rate()
    }
    #[inline]
    pub fn instant(&self) -> Rate {
        self.last_instant_rate
    }
    #[inline]
    pub fn stats(&self) -> EwmaWithVariance {
        self.ewma
    }
}
