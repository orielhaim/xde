//! Lower-layer receive capacity, not an equivalent production download benchmark.
use bytes::{Buf, Bytes};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::{net::SocketAddr, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
enum Mode {
    QuicChunk,
    QuicBatch,
    H3,
    FixtureH3,
}
impl Mode {
    fn http3(self) -> bool {
        matches!(self, Self::H3 | Self::FixtureH3)
    }
}
#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, default_value_t = 4096)]
    size_mib: u64,
}
#[derive(Default, Clone, Copy, Serialize)]
struct Counters {
    seconds: f64,
    user_seconds: f64,
    kernel_seconds: f64,
    cycles: u64,
}
#[derive(Serialize)]
struct QuicStats {
    rx_datagrams: u64,
    tx_datagrams: u64,
    rx_ios: u64,
    tx_ios: u64,
    rx_bytes: u64,
    tx_bytes: u64,
    lost_packets: u64,
    lost_bytes: u64,
    sent_packets: u64,
    congestion_events: u64,
    stream_frames_rx: u64,
    stream_frames_tx: u64,
    stream_data_blocked_tx: u64,
    data_blocked_tx: u64,
    rtt_ns: u128,
    cwnd: u64,
    mtu: u16,
}
impl QuicStats {
    fn read(connection: &compio_quic::Connection) -> Self {
        let s = connection.stats();
        Self {
            rx_datagrams: s.udp_rx.datagrams,
            tx_datagrams: s.udp_tx.datagrams,
            rx_ios: s.udp_rx.ios,
            tx_ios: s.udp_tx.ios,
            rx_bytes: s.udp_rx.bytes,
            tx_bytes: s.udp_tx.bytes,
            lost_packets: s.path.lost_packets,
            lost_bytes: s.path.lost_bytes,
            sent_packets: s.path.sent_packets,
            congestion_events: s.path.congestion_events,
            stream_frames_rx: s.frame_rx.stream,
            stream_frames_tx: s.frame_tx.stream,
            stream_data_blocked_tx: s.frame_tx.stream_data_blocked,
            data_blocked_tx: s.frame_tx.data_blocked,
            rtt_ns: s.path.rtt.as_nanos(),
            cwnd: s.path.cwnd,
            mtu: s.path.current_mtu,
        }
    }
}
#[cfg(windows)]
fn thread_counters() -> Option<Counters> {
    use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};
    use windows_sys::Win32::System::WindowsProgramming::QueryThreadCycleTime;
    let (mut created, mut exited, mut kernel, mut user) = unsafe { std::mem::zeroed() };
    let mut cycles = 0;
    // Pseudo handle is valid for this thread; all outputs are live stack storage.
    if unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &mut created,
            &mut exited,
            &mut kernel,
            &mut user,
        )
    } == 0
        || unsafe { QueryThreadCycleTime(GetCurrentThread(), &mut cycles) } == 0
    {
        return None;
    }
    let ticks = |t: windows_sys::Win32::Foundation::FILETIME| {
        (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime)
    };
    Some(Counters {
        seconds: (ticks(kernel) + ticks(user)) as f64 / 1e7,
        user_seconds: ticks(user) as f64 / 1e7,
        kernel_seconds: ticks(kernel) as f64 / 1e7,
        cycles,
    })
}
#[cfg(not(windows))]
fn thread_counters() -> Option<Counters> {
    None
}
fn delta(start: Option<Counters>) -> Option<Counters> {
    let (a, b) = (start?, thread_counters()?);
    Some(Counters {
        seconds: b.seconds - a.seconds,
        user_seconds: b.user_seconds - a.user_seconds,
        kernel_seconds: b.kernel_seconds - a.kernel_seconds,
        cycles: b.cycles.saturating_sub(a.cycles),
    })
}
#[derive(Serialize)]
struct Report {
    schema_version: u8,
    diagnostic_only: bool,
    mode: Mode,
    bytes: u64,
    source_id: &'static str,
    compiler: &'static str,
    compio_revision: &'static str,
    build_target: &'static str,
    load: Option<xde_bench::load::LoadObservation>,
    peak_process_working_set_bytes: Option<u64>,
    seconds: f64,
    payload_seconds: Option<f64>,
    mib_s: f64,
    returned_chunks: u64,
    receive_calls: u64,
    client: Option<Counters>,
    server: Option<Counters>,
    client_transport: QuicStats,
    server_transport: Option<QuicStats>,
    verified_bytes: u64,
}
fn verify(mut data: &[u8], offset: &mut u64) {
    let tile = xde_test::payload::tile_ref();
    while !data.is_empty() {
        let start = *offset as usize % tile.len();
        let n = data.len().min(tile.len() - start);
        assert_eq!(
            &data[..n],
            &tile[start..start + n],
            "receive corruption at {offset}"
        );
        *offset += n as u64;
        data = &data[n..];
    }
}
fn block(tile: &Bytes, offset: u64, size: u64) -> Bytes {
    let start = offset as usize % tile.len();
    let n = (size - offset)
        .min(64 * 1024)
        .min((tile.len() - start) as u64) as usize;
    tile.slice(start..start + n)
}

enum Server {
    Minimal(std::thread::JoinHandle<(Option<Counters>, QuicStats)>),
    Fixture(xde_test::DualH3),
}
impl Server {
    fn finish(self) -> (Option<Counters>, Option<QuicStats>) {
        match self {
            Self::Minimal(thread) => {
                let (cpu, transport) = thread.join().unwrap();
                (cpu, Some(transport))
            }
            Self::Fixture(server) => {
                use std::sync::atomic::Ordering;
                assert_eq!(server.stats.h3_connections.load(Ordering::Acquire), 1);
                assert_eq!(server.stats.h3_requests.load(Ordering::Acquire), 1);
                server.shutdown();
                (None, None)
            }
        }
    }
}
fn server(mode: Mode, size: u64) -> (SocketAddr, Server) {
    if mode == Mode::FixtureH3 {
        let server = xde_test::spawn_h1h3(xde_test::FixtureSpec {
            size,
            ..Default::default()
        });
        return (
            (server.addr.ip(), server.h3_port).into(),
            Server::Fixture(server),
        );
    }
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let config =
        compio_quic::ServerBuilder::new_with_single_cert(vec![cert.der().clone()], key.into())
            .unwrap()
            .with_alpn_protocols(&[if mode.http3() { "h3" } else { "xde-probe" }])
            .build();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("receive-server".into())
        .spawn(move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let endpoint = compio_quic::Endpoint::server(("127.0.0.1", 0), config)
                        .await
                        .unwrap();
                    tx.send(endpoint.local_addr().unwrap()).unwrap();
                    let connection = endpoint
                        .wait_incoming()
                        .await
                        .unwrap()
                        .accept()
                        .unwrap()
                        .await
                        .unwrap();
                    let tile = Bytes::from_static(xde_test::payload::tile_ref());
                    let cpu = if mode.http3() {
                        let mut h3 = h3::server::builder()
                            .build::<_, Bytes>(connection.clone())
                            .await
                            .unwrap();
                        let resolver = h3.accept().await.unwrap().unwrap();
                        let (request, mut stream) = resolver.resolve_request().await.unwrap();
                        assert_eq!(request.headers()["range"], format!("bytes=0-{}", size - 1));
                        while let Some(data) = stream.recv_data().await.unwrap() {
                            assert_eq!(data.remaining(), 0);
                        }
                        let response = http::Response::builder()
                            .status(206)
                            .header("content-length", size.to_string())
                            .header("content-range", format!("bytes 0-{}/{size}", size - 1))
                            .body(())
                            .unwrap();
                        stream.send_response(response).await.unwrap();
                        let started = thread_counters();
                        let mut offset = 0;
                        while offset < size {
                            let data = block(&tile, offset, size);
                            offset += data.len() as u64;
                            stream.send_data(data).await.unwrap();
                        }
                        stream.finish().await.unwrap();
                        // Keep H3 control streams alive until the client consumes the FIN.
                        let _ = h3.accept().await;
                        delta(started)
                    } else {
                        let mut stream = connection.open_uni_wait().await.unwrap();
                        let started = thread_counters();
                        let mut offset = 0;
                        while offset < size {
                            let data = block(&tile, offset, size);
                            offset += data.len() as u64;
                            stream.write_all_chunks(&mut [data]).await.unwrap();
                        }
                        stream.finish().unwrap();
                        let _ = connection.closed().await;
                        delta(started)
                    };
                    endpoint.close(0u32.into(), b"done");
                    (cpu, QuicStats::read(&connection))
                })
        })
        .unwrap();
    (rx.recv().unwrap(), Server::Minimal(handle))
}

fn run(args: Args) -> Report {
    let size = args
        .size_mib
        .checked_mul(1 << 20)
        .filter(|n| *n > 0)
        .unwrap();
    let (address, server) = server(args.mode, size);
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let endpoint = compio_quic::Endpoint::client((address.ip(), 0))
            .await
            .unwrap();
        let config = compio_quic::ClientBuilder::new_with_no_server_verification()
            .with_alpn_protocols(&[if args.mode.http3() { "h3" } else { "xde-probe" }])
            .build();
        let connection = endpoint
            .connect(address, "localhost", Some(config))
            .unwrap()
            .await
            .unwrap();
        let (mut bytes, mut chunks, mut calls) = (0, 0, 0);
        let started = Instant::now();
        let cpu = thread_counters();
        let mut first_payload = None;
        let mut drive = None;
        if args.mode.http3() {
            let (mut driver, mut sender) = h3::client::builder()
                .build::<_, compio_quic::h3::OpenStreams, Bytes>(connection.clone())
                .await
                .unwrap();
            drive = Some(compio::runtime::spawn(async move {
                let _ = driver.wait_idle().await;
            }));
            let mut stream = sender
                .send_request(
                    http::Request::builder()
                        .uri("https://localhost/artifact.bin")
                        .header("range", format!("bytes=0-{}", size - 1))
                        .body(())
                        .unwrap(),
                )
                .await
                .unwrap();
            stream.finish().await.unwrap();
            let response = stream.recv_response().await.unwrap();
            assert_eq!(response.status(), 206);
            assert_eq!(
                response.headers()["content-range"],
                format!("bytes 0-{}/{size}", size - 1)
            );
            while let Some(mut data) = stream.recv_data().await.unwrap() {
                calls += 1;
                chunks += 1;
                while data.has_remaining() {
                    if bytes == 0 {
                        first_payload = Some(Instant::now());
                    }
                    let n = data.chunk().len();
                    verify(data.chunk(), &mut bytes);
                    data.advance(n);
                }
            }
        } else {
            let mut stream = connection.accept_uni().await.unwrap();
            if args.mode == Mode::QuicBatch {
                let mut buffers = std::array::from_fn::<_, 32, _>(|_| Bytes::new());
                while let Some(count) = stream.read_chunks(&mut buffers).await.unwrap() {
                    calls += 1;
                    chunks += count as u64;
                    for data in &mut buffers[..count] {
                        if bytes == 0 && !data.is_empty() {
                            first_payload = Some(Instant::now());
                        }
                        verify(data, &mut bytes);
                        *data = Bytes::new();
                    }
                }
            } else {
                while let Some(chunk) = stream.read_chunk(usize::MAX, true).await.unwrap() {
                    if bytes == 0 && !chunk.bytes.is_empty() {
                        first_payload = Some(Instant::now());
                    }
                    calls += 1;
                    chunks += 1;
                    verify(&chunk.bytes, &mut bytes);
                }
            }
        }
        assert_eq!(bytes, size);
        let seconds = started.elapsed().as_secs_f64();
        let payload_seconds = first_payload.map(|at| at.elapsed().as_secs_f64());
        let client = delta(cpu);
        let client_transport = QuicStats::read(&connection);
        connection.close(0u32.into(), b"consumed");
        if let Some(drive) = drive {
            let _ = drive.cancel().await;
        }
        let mut report = Report {
            schema_version: 2,
            diagnostic_only: true,
            mode: args.mode,
            bytes,
            source_id: env!("XDE_SOURCE_ID"),
            compiler: env!("XDE_COMPILER"),
            compio_revision: env!("XDE_COMPIO_REV"),
            build_target: env!("XDE_BUILD_TARGET"),
            load: None,
            peak_process_working_set_bytes: None,
            seconds,
            payload_seconds,
            mib_s: bytes as f64 / (1 << 20) as f64 / seconds,
            returned_chunks: chunks,
            receive_calls: calls,
            client,
            server: None,
            client_transport,
            server_transport: None,
            verified_bytes: bytes,
        };
        // Keep the client proactor alive while its close reaches the server.
        let (server, server_transport) = compio::runtime::spawn_blocking(move || server.finish())
            .await
            .unwrap();
        report.server = server;
        report.server_transport = server_transport;
        report
    })
}
fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args = Args::parse();
    let load = xde_bench::load::Monitor::start();
    let mut report = run(args);
    report.load = Some(load.finish());
    report.peak_process_working_set_bytes = xde_bench::load::peak_working_set_bytes();
    println!("{}", serde_json::to_string(&report).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lower_layer_modes_verify_the_same_payload_and_finish_both_runtimes() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        for mode in [Mode::QuicChunk, Mode::QuicBatch, Mode::H3, Mode::FixtureH3] {
            let report = run(Args { mode, size_mib: 1 });
            assert_eq!(report.verified_bytes, 1 << 20);
            assert_eq!(report.bytes, 1 << 20);
            assert!(report.returned_chunks > 0);
            assert!(report.receive_calls <= report.returned_chunks);
        }
    }
}
