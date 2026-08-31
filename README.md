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

## Adaptive concurrency

XDE does not assume that more connections are always faster.

It starts with low concurrency, measures transfer throughput, and increases concurrency when doing so produces a useful gain. Scaling stops when additional workers stop improving the transfer enough to justify them.

The following benchmark uses a server that limits each TCP connection to `4 MiB/s`.

With a single connection, throughput is therefore limited to approximately `4 MiB/s`.

No connection count was configured for XDE:

|       | Throughput | Connections opened |
| ----- | ---------: | -----------------: |
| Run 1 | 10.8 MiB/s |                  7 |
| Run 2 | 10.8 MiB/s |                  7 |
| Run 3 | 10.8 MiB/s |                  7 |

The engine detected that additional connections increased aggregate throughput and scaled the transfer automatically.

Seven connections is not a target or a preset. It is the result of the runtime scaling policy for this benchmark.

## Performance

On a Windows loopback HTTP/1.1 benchmark:

|      |          16 MiB |          256 MiB |
| ---- | --------------: | ---------------: |
| XDE  | **569.5 MiB/s** | **1516.7 MiB/s** |
| curl |     447.5 MiB/s |     1123.3 MiB/s |

A separate fixed-concurrency benchmark limits the server to `100 MiB/s` per connection:

| Connections |         XDE |
| ----------: | ----------: |
|           1 |  99.4 MiB/s |
|           2 | 197.1 MiB/s |
|           3 | 293.1 MiB/s |
|           4 | 388.9 MiB/s |

This benchmark disables the adaptive decision-making and measures how well the transfer engine scales once concurrency is available.

HTTP/2 performance on Windows is currently slightly below curl.

Full methodology and results are available in [`bench.md`](bench.md).

## Status

XDE is under active development.

The public API may still change. Current work is focused on transfer scheduling, adaptive concurrency, protocol behavior and performance across real network conditions.

Bug reports, benchmarks and patches are welcome.

## License

[Apache-2.0](LICENSE)
