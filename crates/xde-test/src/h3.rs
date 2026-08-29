//! HTTP/1.1 TCP listener + HTTP/3 QUIC listener. The H1 responses advertise
//! `Alt-Svc` so the engine discovers H3 the same way production origins do.

use std::{
    io::{BufReader, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use bytes::Bytes;

use crate::{
    h1, payload,
    spec::{FixtureSpec, FixtureStats, SharedStats},
};

pub struct DualH3 {
    pub addr: std::net::SocketAddr,
    pub h3_port: u16,
    pub stats: SharedStats,
    stop: Arc<AtomicBool>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl DualH3 {
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::Release);
        for t in self.threads {
            let _ = t.join();
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}/artifact.bin", self.addr)
    }
}

pub fn spawn_h1h3(spec: FixtureSpec) -> DualH3 {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let stats: SharedStats = Arc::new(FixtureStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let payload_h3: Bytes = {
        let mut v = vec![0u8; spec.size.min(1024 * 1024) as usize];
        if v.is_empty() {
            v = vec![0u8; 1];
        }
        payload::fill(&mut v, 0);
        // H3 fixture streams from the repeating tile via payload::fill on
        // each chunk; the Bytes here is only a 1 MiB tile.
        Bytes::from(payload::tile())
    };

    let params = rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("rcgen params");
    let signing_key = rcgen::KeyPair::generate().expect("rcgen key");
    let cert = params.self_signed(&signing_key).expect("rcgen cert");
    let cert_der = cert.der().to_vec();
    let key_der = rustls::pki_types::PrivateKeyDer::from(signing_key);
    let (h3_addr_tx, h3_addr_rx) = std::sync::mpsc::channel();
    let h3_thread = {
        let stop = stop.clone();
        let payload = payload_h3.clone();
        let size = spec.size;
        thread::spawn(move || {
            let rt = compio::runtime::Runtime::new().expect("h3 runtime");
            rt.block_on(async move {
                let cert_chain = vec![cert_der.into()];
                let server_crypto =
                    compio_quic::ServerBuilder::new_with_single_cert(cert_chain, key_der)
                        .expect("server cert")
                        .with_alpn_protocols(&["h3"])
                        .build();
                let endpoint =
                    compio_quic::Endpoint::server(crate::fixture_bind_addr(), server_crypto)
                        .await
                        .expect("quic bind");
                let _ = h3_addr_tx.send(endpoint.local_addr().expect("local"));
                {
                    let stop = stop.clone();
                    let endpoint = endpoint.clone();
                    compio::runtime::spawn(async move {
                        loop {
                            if stop.load(Ordering::Acquire) {
                                endpoint.close(0u32.into(), b"shutdown");
                                return;
                            }
                            compio::time::sleep(Duration::from_millis(50)).await;
                        }
                    })
                    .detach();
                }
                while let Some(incoming) = endpoint.wait_incoming().await {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    match incoming.accept() {
                        Ok(connecting) => {
                            let payload = payload.clone();
                            let stop = stop.clone();
                            compio::runtime::spawn(async move {
                                if let Ok(conn) = connecting.await
                                    && let Ok(mut h3_conn) =
                                        h3::server::builder().build::<_, Bytes>(conn).await
                                {
                                    while let Ok(Some(resolver)) = h3_conn.accept().await {
                                        if stop.load(Ordering::Acquire) {
                                            break;
                                        }
                                        let Ok((request, mut stream)) =
                                            resolver.resolve_request().await
                                        else {
                                            break;
                                        };
                                        serve_h3(
                                            &mut stream,
                                            &payload,
                                            size,
                                            request,
                                            stop.clone(),
                                        )
                                        .await;
                                    }
                                }
                            })
                            .detach();
                        }
                        Err(_) => break,
                    }
                }
            });
        })
    };
    let h3_port = h3_addr_rx.recv().expect("h3 address").port();

    let listener = TcpListener::bind(crate::fixture_bind_addr()).unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let h1_spec = FixtureSpec {
        alt_svc_h3: Some(h3_port),
        ..spec
    };
    let h1_thread = {
        let stop = stop.clone();
        let stats = stats.clone();
        thread::spawn(move || {
            let mut handlers = Vec::new();
            while !stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        stats.accepts.fetch_add(1, Ordering::AcqRel);
                        let spec = h1_spec.clone();
                        let stop = stop.clone();
                        let stats = stats.clone();
                        handlers.push(thread::spawn(move || {
                            serve_h1_advertise(stream, spec, stop, stats);
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
            for h in handlers {
                let _ = h.join();
            }
        })
    };

    DualH3 {
        addr,
        h3_port,
        stats,
        stop,
        threads: vec![h1_thread, h3_thread],
    }
}

fn serve_h1_advertise(
    mut stream: std::net::TcpStream,
    spec: FixtureSpec,
    stop: Arc<AtomicBool>,
    stats: SharedStats,
) {
    let reader_stream = stream.try_clone().unwrap();
    reader_stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .ok();
    let mut reader = BufReader::new(reader_stream);
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }
        let Some(head) = read_head(&mut reader) else {
            return;
        };
        stats.requests.fetch_add(1, Ordering::AcqRel);
        let range = h1::parse_range(&head, spec.size);
        let (start, end_incl) = range.unwrap_or((0, spec.size.saturating_sub(1)));
        let len = end_incl - start + 1;
        let alt = spec.alt_svc_h3.unwrap();
        let head = if range.is_some() {
            format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {len}\r\nContent-Range: bytes {start}-{end_incl}/{}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nAlt-Svc: h3=\":{alt}\"\r\n\r\n",
                spec.size, spec.etag
            )
        } else {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: {}\r\nAlt-Svc: h3=\":{alt}\"\r\n\r\n",
                spec.size, spec.etag
            )
        };
        if write!(stream, "{head}").is_err() {
            return;
        }
        let mut offset = start;
        let block = payload::tile_ref();
        while offset <= end_incl {
            let in_block = (offset % block.len() as u64) as usize;
            let take = ((end_incl - offset + 1) as usize).min(block.len() - in_block);
            if stream.write_all(&block[in_block..in_block + take]).is_err() {
                return;
            }
            stats.bytes_sent.fetch_add(take as u64, Ordering::AcqRel);
            offset += take as u64;
        }
        if !spec.keep_alive {
            return;
        }
    }
}

fn read_head(reader: &mut BufReader<std::net::TcpStream>) -> Option<String> {
    use std::io::BufRead;
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) | Err(_) => return None,
        Ok(_) => {}
    }
    loop {
        let mut hdr = String::new();
        match reader.read_line(&mut hdr) {
            Ok(0) | Err(_) => return Some(line),
            Ok(_) if hdr == "\r\n" => return Some(line),
            Ok(_) => line.push_str(&hdr),
        }
    }
}

async fn serve_h3(
    stream: &mut h3::server::RequestStream<
        <compio_quic::h3::OpenStreams as h3::quic::OpenStreams<Bytes>>::BidiStream,
        Bytes,
    >,
    tile: &Bytes,
    total: u64,
    request: http::Request<()>,
    stop: Arc<AtomicBool>,
) {
    let range = request.headers().get("range").and_then(|v| {
        let v = v.to_str().ok()?;
        let v = v.strip_prefix("bytes=")?;
        let (s, e) = v.split_once('-')?;
        let start: u64 = s.parse().ok()?;
        let end: u64 = if e.is_empty() {
            total.saturating_sub(1)
        } else {
            e.parse().ok()?
        };
        Some((start, end.min(total.saturating_sub(1))))
    });
    let resp_head = match range {
        Some((start, end)) => {
            let end = end.min(total.saturating_sub(1));
            http::Response::builder()
                .status(206)
                .header("content-length", format!("{}", end - start + 1))
                .header("content-range", format!("bytes {start}-{end}/{total}"))
                .header("accept-ranges", "bytes")
                .body(())
                .unwrap()
        }
        None => http::Response::builder()
            .status(200)
            .header("content-length", format!("{total}"))
            .header("accept-ranges", "bytes")
            .body(())
            .unwrap(),
    };
    if stream.send_response(resp_head).await.is_err() {
        return;
    }
    let (start, end) = range.map_or((0, total.saturating_sub(1)), |(s, e)| {
        (s, e.min(total.saturating_sub(1)))
    });
    let mut offset = start;
    while offset <= end && !stop.load(Ordering::Acquire) {
        let block_len = tile.len();
        let in_block = (offset % block_len as u64) as usize;
        let take = ((end - offset + 1) as usize)
            .min(64 * 1024)
            .min(block_len - in_block);
        if stream
            .send_data(tile.slice(in_block..in_block + take))
            .await
            .is_err()
        {
            return;
        }
        offset += take as u64;
    }
    let _ = stream.finish().await;
}
