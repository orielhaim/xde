//! Runtime ownership and thread topology for XDE.

#![forbid(unsafe_code)]

mod handle;

pub use handle::{Runtime, RuntimeBuilder, RuntimeHandle};
