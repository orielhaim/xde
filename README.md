# XDE

![badge](https://shieldcn.dev/badge/Status-In%20development.svg?theme=amber&split=true)
[![badge](https://shieldcn.dev/badge/Join%20Discord.svg?brand=discord)](https://discord.gg/y5bNc3MYKz)

eXtreme download engine you can embed anywhere.

## Why

I've used IDM and XDM for years. They do something annoyingly useful. You give them a file and they figure out how to download it properly. Split it, open more connections when that helps, back off when it doesn't, resume after something breaks, and keep going.

At some point I wanted that inside one of my own projects.

The options were basically:

1. ship a download manager next to my app
2. call some CLI and parse its output
3. write the whole thing myself

So naturally I picked the reasonable option and wrote the whole thing myself.

(The name is a small nod to XDM)

## The point

XDE is not `curl` with a connection-count option.

You don't tell it:

```text
use 8 connections
split this into 32 chunks
```

You tell it:

```text
download this
```

XDE starts conservatively and figures the rest out while the download is running.

It can add physical connections, add streams on multiplexed protocols, redistribute ranges between workers, cut slow tails and stop scaling when extra concurrency isn't buying anything.

## Usage

```toml
[dependencies]
xde = "1.0.0"
```

And then:

```rust
let engine = xde::Engine::builder().build()?;

let job = engine
    .download("https://example.com/file.bin")
    .to("file.bin")
    .start()?;

let result = job.wait_blocking()?;

println!("downloaded {} bytes", result.bytes);

engine.shutdown()?;
```

That's the normal path.

XDE starts with one connection and adapts from there.

If you want to put a ceiling on it:

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

That means "use at most 4", not "use 4".

## More stuff

Mirrors:

```rust
let job = engine
    .download("https://origin.example/file.bin")
    .mirror("https://mirror-1.example/file.bin")
    .mirror("https://mirror-2.example/file.bin")
    .to("file.bin")
    .start()?;
```

Integrity:

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

Custom destination:

```rust
engine
    .download(url)
    .destination(destination)
    .start()?;
```

XDE currently supports HTTP/1.1, HTTP/2 and HTTP/3, resumable file downloads, mirrors, BLAKE3/SHA-256 verification, concurrent job priorities, custom destinations and bounded memory.

## Autoscaling

In the benchmark below, the server limits every TCP connection to `4 MiB/s`.

A single connection cannot magically go faster than that. XDE has to discover that more connections actually increase throughput and scale the download itself.

Three independent runs:

|       | Throughput | Connections opened |
| ----- | ---------: | -----------------: |
| Run 1 | 10.8 MiB/s |                  7 |
| Run 2 | 10.8 MiB/s |                  7 |
| Run 3 | 10.8 MiB/s |                  7 |

No fixed connection count was passed to the download.

There's still plenty to improve here, but this is what XDE is built for.

## Raw performance

On my Windows loopback benchmark, HTTP/1.1 currently does:

|      |          16 MiB |          256 MiB |
| ---- | --------------: | ---------------: |
| XDE  | **569.5 MiB/s** | **1516.7 MiB/s** |
| curl |     447.5 MiB/s |     1123.3 MiB/s |

With concurrency fixed for the scaling test and the server capped at `100 MiB/s` per connection:

| Connections |         XDE |
| ----------: | ----------: |
|           1 |  99.4 MiB/s |
|           2 | 197.1 MiB/s |
|           3 | 293.1 MiB/s |
|           4 | 388.9 MiB/s |

So the engine can use the extra connections when they exist. The interesting problem is deciding when they should exist in the first place.

HTTP/2 on Windows is currently slower than curl a bit.

Full numbers and methodology are in [`bench.md`](bench.md).

## Status

Early release, still in development

There are probably decisions in here that seemed brilliant at 2 AM and will look ridiculous to someone else

PRs are very welcome obviously (especially performance ones)

## License

[Apache-2.0](LICENSE)
