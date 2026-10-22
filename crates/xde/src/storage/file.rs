use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct FileDestinationOptions {
    /// Final path. We always write to `<final>.part` and rename at the end.
    pub final_path: PathBuf,
    /// Real preallocation via `fallocate`, not just `set_len`. `set_len` can
    /// leave you with a logically sparse file that has no storage behind it.
    pub preallocate: bool,
    /// Tell the kernel we are streaming, so it stops trying to be clever.
    pub advise_sequential: bool,
    pub max_parallel_writes: u16,
    pub max_inflight_bytes: u64,
}

impl FileDestinationOptions {
    pub fn new(final_path: impl Into<PathBuf>) -> Self {
        Self {
            final_path: final_path.into(),
            preallocate: true,
            advise_sequential: false,
            max_parallel_writes: 16,
            max_inflight_bytes: 256 * 1024 * 1024,
        }
    }
}

pub fn part_path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".part");
    PathBuf::from(s)
}

pub fn journal_path_for(final_path: &Path) -> PathBuf {
    let mut s = part_path_for(final_path).into_os_string();
    s.push(".state");
    PathBuf::from(s)
}
