//! Example: everyday XDE usage.
//!
//! Run: cargo run --example basics -- <url> <output-path>
//!
//! Covers the common path (download with progress), integrity, resume and
//! multiple mirrors. Persistent learning is OFF by default; see the last
//! section for how to opt in.

use std::time::Duration;

fn main() -> Result<(), xde::Error> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://proof.ovh.net/files/10Mb.dat".into());
    let dest = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "artifact.bin".into());

    let engine = xde::Engine::builder().shards(4).build()?;

    // ---- Simple download + progress polling ----
    let job = engine.download(&url).to(&dest).start()?;

    while let Ok(snap) = job.snapshot() {
        let pct = snap
            .total_length
            .map(|t| format!("{:3.0}%", 100.0 * snap.verified_bytes as f64 / t as f64))
            .unwrap_or_else(|| "??%".into());
        println!(
            "{pct} verified={}B streams={} conns={}",
            snap.verified_bytes, snap.active_streams, snap.active_connections
        );
        if snap.verified_bytes == snap.total_length.unwrap_or(u64::MAX) {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let outcome = job.wait_blocking()?;
    println!(
        "done: {} bytes (resumed {})",
        outcome.bytes, outcome.resumed_bytes
    );

    // ---- Integrity: verify against a known digest ----
    // let digest = xde::ExpectedDigest::parse_hex(xde::HashKind::Blake3, "<hex>")?;
    // let job = engine.download(&url).to(&dest)
    //     .integrity(xde::IntegritySpec::strict(digest)).start()?;

    // ---- Mirrors ----
    // .mirror("https://mirror.example.org/same/file") - failover by default;
    // with an expected digest, healthy mirrors may serve ranges simultaneously.

    // ---- Persistent learning is DISABLED by default ----
    // Opt in explicitly to remember endpoint performance across runs:
    // let engine = Engine::builder().profile_path("xde-profile.json").build()?;

    engine.shutdown()?;
    Ok(())
}
