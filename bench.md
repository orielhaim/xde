# XDE Benchmarks

Loopback benchmarks for XDE against curl.

These numbers characterize the current pre-release implementation. They are not intended to represent internet download speeds.

## Environment

* Windows 11 pro
* Intel Core Ultra 9 285K
* Rust 1.98
* release profile: `opt-level = 3`, ThinLTO, `codegen-units = 1`
* curl 8.21 with nghttp2
* file destination
* loopback HTTP server
* throughput measured until the download job completes
* engine shutdown is excluded from throughput

XDE and curl runs are alternated within paired benchmarks to reduce ordering bias.

## Single connection

One connection, one stream.

### 16 MiB

| Protocol |      XDE median |     curl median | Difference |
| -------- | --------------: | --------------: | ---------: |
| HTTP/1.1 | **569.5 MiB/s** |     447.5 MiB/s | **+27.3%** |
| HTTP/2   |     250.0 MiB/s | **386.3 MiB/s** |     -35.3% |

11 measured trials after 2 warmups.

### 256 MiB

| Protocol |       XDE median |     curl median | Difference |
| -------- | ---------------: | --------------: | ---------: |
| HTTP/1.1 | **1516.7 MiB/s** |    1123.3 MiB/s | **+35.0%** |
| HTTP/2   |      551.1 MiB/s | **803.8 MiB/s** |     -31.4% |

7 measured trials after 1 warmup.

HTTP/1.1 currently performs very well on loopback.

HTTP/2 remains a known performance gap. Correctness is not affected; the current issue is throughput and variance on the Windows receive path.

HTTP/3 is supported by XDE but is not included here because the curl build used for these measurements does not support HTTP/3.

## Multi-connection scaling

256 MiB HTTP/1.1 download with the fixture capped at 100 MiB/s per physical connection.

| Connections |  Throughput | Scaling |
| ----------: | ----------: | ------: |
|           1 |  99.4 MiB/s |   1.00× |
|           2 | 197.1 MiB/s |   1.98× |
|           3 | 293.1 MiB/s |   2.95× |
|           4 | 388.9 MiB/s |   3.91× |

XDE reaches near-linear scaling through four physical connections under an independent per-connection bottleneck.

## Adaptive mode

The adaptive benchmark uses a 256 MiB artifact with each connection capped at 4 MiB/s.

Three independent runs:

| Run | Throughput | Connections accepted |
| --: | ---------: | -------------------: |
|   1 | 10.8 MiB/s |                    7 |
|   2 | 10.8 MiB/s |                    7 |
|   3 | 10.8 MiB/s |                    7 |

This benchmark exercises automatic connection exploration rather than a fixed connection count.

## Reproducing

Single-flow comparison:

```bash
cargo run --release -p xde-bench -- \
  --suite single \
  --size-mib 256 \
  --trials 7 \
  --warmup 1
```

Scaling:

```bash
cargo run --release -p xde-bench -- \
  --suite scaling \
  --size-mib 256 \
  --trials 5 \
  --warmup 1
```

Adaptive mode:

```bash
cargo run --release -p xde-bench -- \
  --suite adaptive \
  --size-mib 256 \
  --trials 1 \
  --warmup 0
```

## Interpretation

These are local system benchmarks, primarily useful for comparing implementations under controlled conditions.

The current results show:

* strong HTTP/1.1 single-flow throughput
* near-linear multi-connection scaling
* stable adaptive behavior in the capped fixture
* a remaining HTTP/2 performance gap on Windows

See the benchmark harness in `crates/xde-bench` for the implementation and fixtures.
