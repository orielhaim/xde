use std::{path::PathBuf, process::Command};
fn main() {
    let rustc = Command::new(std::env::var_os("RUSTC").unwrap())
        .arg("-vV")
        .output()
        .expect("compiler identity");
    assert!(rustc.status.success());
    println!(
        "cargo:rustc-env=XDE_COMPILER={}",
        String::from_utf8(rustc.stdout)
            .unwrap()
            .trim()
            .replace('\n', " | ")
            .replace('\r', "")
    );
    println!(
        "cargo:rustc-env=XDE_BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap()
    );
    println!(
        "cargo:rustc-env=XDE_BUILD_TARGET={}",
        std::env::var("TARGET").unwrap()
    );
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../..");
    let lock = std::fs::read_to_string(root.join("Cargo.lock")).expect("dependency lock");
    let compio_sources: std::collections::BTreeSet<_> = lock
        .lines()
        .filter(|line| {
            line.starts_with("source = ") && line.contains("github.com/compio-rs/compio?")
        })
        .collect();
    assert_eq!(
        compio_sources.len(),
        1,
        "one pinned Compio source is required"
    );
    let revision = compio_sources
        .first()
        .unwrap()
        .split('#')
        .next_back()
        .unwrap()
        .trim_end_matches('"');
    println!("cargo:rustc-env=XDE_COMPIO_REV={revision}");
    let output = Command::new("git")
        .args(["ls-files", "--cached", "--others", "--exclude-standard"])
        .current_dir(&root)
        .output()
        .expect("git source inventory");
    assert!(output.status.success(), "git source inventory failed");
    let text = String::from_utf8(output.stdout).expect("source paths");
    let mut paths: Vec<_> = text
        .lines()
        .filter(|p| {
            p.ends_with(".rs")
                || p.ends_with("Cargo.toml")
                || p.ends_with("Cargo.lock")
                || *p == "rust-toolchain.toml"
        })
        .collect();
    paths.sort_unstable();
    paths.dedup();
    let mut hasher = blake3::Hasher::new();
    for path in paths {
        let full = root.join(path);
        if let Ok(bytes) = std::fs::read(&full) {
            println!("cargo:rerun-if-changed={}", full.display());
            hasher.update(&(path.len() as u64).to_le_bytes());
            hasher.update(path.as_bytes());
            hasher.update(&(bytes.len() as u64).to_le_bytes());
            hasher.update(&bytes);
        }
    }
    println!(
        "cargo:rustc-env=XDE_SOURCE_ID={}",
        hasher.finalize().to_hex()
    );
}
