pub mod hash;
pub mod overlap;

pub use hash::{Hasher, StreamingDigest};
pub use overlap::{OverlapGuard, OverlapVerdict, RepresentationLock};
