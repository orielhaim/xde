use std::{fmt, ops, time::Duration};

/// Bytes per second. Kept as f64 because every consumer is doing ratio math.
#[derive(Copy, Clone, PartialEq, PartialOrd, Default)]
pub struct Rate(f64);

impl Rate {
    pub const ZERO: Rate = Rate(0.0);
    /// Floor used everywhere we divide by a rate, so a stalled worker never
    /// produces NaN or an infinite split point.
    pub const FLOOR: Rate = Rate(1024.0);
    /// Piece-sizing prior before a connection has a measured rate.
    /// FLOOR would emit hundreds of tiny Range requests on a fast path.
    pub const COLD_START: Rate = Rate(128.0 * 1024.0 * 1024.0);

    #[inline]
    pub fn from_bps(v: f64) -> Self {
        Self(if v.is_finite() && v > 0.0 { v } else { 0.0 })
    }
    #[inline]
    pub fn from_mbps(v: f64) -> Self {
        Self::from_bps(v * 125_000.0)
    }
    #[inline]
    pub fn bps(self) -> f64 {
        self.0
    }
    #[inline]
    pub fn nonzero(self) -> f64 {
        self.0.max(Self::FLOOR.0)
    }
    /// Bytes transferred in `d` at this rate.
    #[inline]
    pub fn bytes_in(self, d: Duration) -> u64 {
        (self.0 * d.as_secs_f64()).max(0.0) as u64
    }
    /// Time to move `n` bytes at this rate.
    #[inline]
    pub fn time_for(self, n: u64) -> Duration {
        Duration::from_secs_f64(n as f64 / self.nonzero())
    }
    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 <= 0.0
    }
    #[inline]
    pub fn min(self, o: Rate) -> Rate {
        Rate(self.0.min(o.0))
    }
    #[inline]
    pub fn max(self, o: Rate) -> Rate {
        Rate(self.0.max(o.0))
    }
}

impl ops::Add for Rate {
    type Output = Rate;
    fn add(self, o: Rate) -> Rate {
        Rate(self.0 + o.0)
    }
}
impl ops::Sub for Rate {
    type Output = Rate;
    fn sub(self, o: Rate) -> Rate {
        Rate((self.0 - o.0).max(0.0))
    }
}
impl ops::Mul<f64> for Rate {
    type Output = Rate;
    fn mul(self, k: f64) -> Rate {
        Rate::from_bps(self.0 * k)
    }
}
impl std::iter::Sum for Rate {
    fn sum<I: Iterator<Item = Rate>>(it: I) -> Rate {
        Rate(it.map(|r| r.0).sum())
    }
}

impl fmt::Debug for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = self.0;
        if v >= 125_000_000.0 {
            write!(f, "{:.2}Gbps", v / 125_000_000.0)
        } else if v >= 125_000.0 {
            write!(f, "{:.1}Mbps", v / 125_000.0)
        } else {
            write!(f, "{:.0}B/s", v)
        }
    }
}

/// Human-readable byte counts, used in events and logs only.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Bytes(pub u64);

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
        let mut v = self.0 as f64;
        let mut i = 0;
        while v >= 1024.0 && i < U.len() - 1 {
            v /= 1024.0;
            i += 1;
        }
        write!(f, "{v:.2}{}", U[i])
    }
}

pub const KIB: u64 = 1024;
pub const MIB: u64 = 1024 * KIB;
pub const GIB: u64 = 1024 * MIB;
