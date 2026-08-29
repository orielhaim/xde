//! Deterministic generated payload. Large fixtures never store the full
//! artifact; tests reconstruct expected bytes from the same generator.

use std::sync::OnceLock;

const BLOCK: usize = 1024 * 1024;

fn generate_tile() -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut buf = vec![0u8; BLOCK];
    for b in &mut buf {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *b = state as u8;
    }
    buf
}

/// Cached 1 MiB tile. Regenerating this on every 64 KiB socket write is what
/// collapsed loopback curl from hundreds of MiB/s to tens.
pub fn tile_ref() -> &'static [u8] {
    static TILE: OnceLock<Vec<u8>> = OnceLock::new();
    TILE.get_or_init(generate_tile).as_slice()
}

/// One MiB of xorshift output, used as a repeating tile.
pub fn tile() -> Vec<u8> {
    tile_ref().to_vec()
}

pub fn fill(dst: &mut [u8], start: u64) {
    let tile = tile_ref();
    let mut offset = start;
    let mut written = 0;
    while written < dst.len() {
        let in_block = (offset % BLOCK as u64) as usize;
        let take = (dst.len() - written).min(BLOCK - in_block);
        dst[written..written + take].copy_from_slice(&tile[in_block..in_block + take]);
        written += take;
        offset += take as u64;
    }
}

pub fn bytes(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    fill(&mut v, 0);
    v
}

/// Copy `[start, end_incl]` of the infinite tiled payload into `w`.
pub fn write_range(w: &mut impl std::io::Write, start: u64, end_incl: u64) -> std::io::Result<()> {
    if start > end_incl {
        return Ok(());
    }
    let tile = tile_ref();
    let mut offset = start;
    while offset <= end_incl {
        let in_block = (offset % BLOCK as u64) as usize;
        let remaining = (end_incl - offset + 1) as usize;
        let take = remaining.min(BLOCK - in_block);
        w.write_all(&tile[in_block..in_block + take])?;
        offset += take as u64;
    }
    Ok(())
}
