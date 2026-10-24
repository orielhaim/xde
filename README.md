# XDE

![badge](https://shieldcn.dev/badge/Status-In%20development.svg?theme=amber&split=true)
[![badge](https://shieldcn.dev/crates/xde.svg)](https://crates.io/crates/xde)
[![badge](https://shieldcn.dev/badge/Join%20Discord.svg?brand=discord)](https://discord.gg/y5bNc3MYKz)

XDE is built to get as much download throughput as possible without requiring the caller to tune connection counts, chunk sizes, or protocol-specific behavior.

## Why

Download managers such as IDM and XDM are useful because they often make downloads faster than a single straightforward HTTP transfer.

They do this by using multiple connections or streams, splitting files into ranges, redistributing unfinished work, recovering interrupted transfers, and adapting to server behavior while the download is running.

XDE provides the same kind of transfer engine as an embeddable Rust library.

The caller provides a URL and destination. XDE handles the transfer strategy.

## Usage

```rust
let engine = xde::Engine::builder().build()?;

let job = engine
    .download("https://example.com/file.bin")
    .to("file.bin")
    .on_progress(|progress| {
        if let Some(fraction) = progress.fraction {
            println!("{:.1}%", fraction * 100.0);
        }
    })
    .start()?;

let result = job.wait_blocking()?;

println!("downloaded {} bytes", result.bytes);

engine.shutdown()?;
```

XDE starts conservatively and adjusts concurrency while the transfer is running.

It can:

* open additional physical connections
* add streams on multiplexed protocols
* split files into byte ranges
* redistribute remaining ranges between workers
* split slow tail ranges
* resume interrupted downloads
* reduce or stop scaling when additional concurrency is no longer useful

The application does not need to choose a fixed connection count.

To impose a limit:

```rust
let policy = xde::TransferPolicy::builder()
    .max_physical_connections(4)
    .build();

let job = engine
    .download("https://example.com/file.bin")
    .policy(policy)
    .to("file.bin")
    .start()?;
```

`max_physical_connections(4)` is a ceiling, not a requested connection count.

## Mirrors

```rust
let job = engine
    .download("https://origin.example/file.bin")
    .mirror("https://mirror-1.example/file.bin")
    .mirror("https://mirror-2.example/file.bin")
    .to("file.bin")
    .start()?;
```

## Integrity

```rust
let digest = xde::ExpectedDigest::parse_hex(
    xde::HashKind::Blake3,
    "<expected digest>",
)?;

let job = engine
    .download("https://example.com/file.bin")
    .integrity(xde::IntegritySpec::strict(digest))
    .to("file.bin")
    .start()?;
```

BLAKE3 and SHA-256 are currently supported.

## Custom destinations

```rust
engine
    .download(url)
    .destination(destination)
    .start()?;
```

This allows XDE to be used with storage backends other than a normal file.

## Features

* HTTP/1.1, HTTP/2 and HTTP/3
* adaptive transfer concurrency
* resumable downloads
* byte-range transfers
* mirrors
* BLAKE3 and SHA-256 verification
* concurrent job priorities
* custom destinations
* bounded memory usage

## Performance validation

Per-job planners request topology; the engine grants shared resources through
FairShare or Queue scheduling. See [scheduling and configuration](docs/scheduling.md)
and [measured results and limitations](docs/scheduling-results.md).

Benchmarks retain complete paired distributions. CPU activity is diagnostic
metadata, not a timing rejection threshold. See [benchmark requirements](bench.md).
Earlier reports retain their historical labels.

## Status

XDE is under active development.

The public API may still change. Current work is focused on transfer scheduling, adaptive concurrency, protocol behavior and performance across real network conditions.

Bug reports, benchmarks and patches are welcome.

## License

[Apache-2.0](LICENSE)

[Adaptive intelligence results](docs/adaptive-intelligence-results.md) records the current policy validation and matched measurements.
