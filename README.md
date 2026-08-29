# XDE

eXtreme download engine you can embed anywhere.

## Why

I've used IDM and XDM for years. They do something annoyingly useful: take a download, split the work, open more connections when it helps, resume when things break, and generally download files better than the usual "open one HTTP request and hope for the best"

At some point I wanted that inside one of my own projects.

Turns out the options were basically:

1. ship a download manager next to my app
2. call some CLI and parse its output
3. write the whole thing myself

So naturally I picked the reasonable option and wrote the whole thing myself.

(The name is also a small nod to XDM)

## What it does

XDE is a Rust library, not another download manager UI.

Give it a URL and a destination. It handles the ugly parts.

* HTTP/1.1, HTTP/2 and HTTP/3
* segmented downloads
* multiple connections when they actually help
* mirrors
* resume after interruption
* BLAKE3 and SHA-256 verification
* priorities across concurrent downloads
* custom and sequential destinations
* bounded memory usage
* runtime progress and events

The engine decides how to download. Your application doesn't need to babysit it.

## Usage

```toml
[dependencies]
xde = "1.0.0"
```

Basic download:

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

Add mirrors:

```rust
let job = engine
    .download("https://origin.example/file.bin")
    .mirror("https://mirror-1.example/file.bin")
    .mirror("https://mirror-2.example/file.bin")
    .to("file.bin")
    .start()?;
```

Verify the result:

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

You can also provide your own destination instead of a file:

```rust
engine
    .download(url)
    .destination(destination)
    .start()?;
```

## Performance

On my Windows loopback benchmark, HTTP/1.1 currently does:

|      |          16 MiB |          256 MiB |
| ---- | --------------: | ---------------: |
| XDE  | **569.5 MiB/s** | **1516.7 MiB/s** |
| curl |     447.5 MiB/s |     1123.3 MiB/s |

With a server capped at 100 MiB/s per connection:

| Connections |         XDE |
| ----------: | ----------: |
|           1 |  99.4 MiB/s |
|           2 | 197.1 MiB/s |
|           3 | 293.1 MiB/s |
|           4 | 388.9 MiB/s |

HTTP/2 on Windows is currently slower than curl. I'm not going to hide the ugly number in a footnote.

Full results and methodology are in [`bench.md`](bench.md).

## Status

Very pre-1.0.

Things will break. APIs will change. Some parts are probably much smarter than they need to be and some parts are definitely dumber than I think they are.

Contributions are welcome, especially if you enjoy profiling networking code more than is probably healthy.

## License

Apache-2.0.
