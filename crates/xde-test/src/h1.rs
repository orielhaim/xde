use std::{
    io::{BufRead, BufReader, ErrorKind, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    payload,
    spec::{FixtureSpec, FixtureStats, SharedStats},
};

pub struct H1Server {
    pub addr: std::net::SocketAddr,
    pub stats: SharedStats,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl H1Server {
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}/artifact.bin", self.addr)
    }
}

pub fn spawn_h1(spec: FixtureSpec) -> H1Server {
    let listener = TcpListener::bind(crate::fixture_bind_addr()).unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stats: SharedStats = Arc::new(FixtureStats::default());
    let global_requests = Arc::new(AtomicU32::new(0));
    let handle = {
        let stop = stop.clone();
        let stats = stats.clone();
        thread::spawn(move || {
            let mut handlers = Vec::new();
            while !stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        stats.accepts.fetch_add(1, Ordering::AcqRel);
                        let spec = spec.clone();
                        let stop = stop.clone();
                        let stats = stats.clone();
                        let global_requests = global_requests.clone();
                        handlers.push(thread::spawn(move || {
                            serve_conn(stream, spec, stop, stats, global_requests);
                        }));
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
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
    H1Server {
        addr,
        stats,
        stop,
        handle: Some(handle),
    }
}

fn serve_conn(
    mut stream: TcpStream,
    spec: FixtureSpec,
    stop: Arc<AtomicBool>,
    stats: SharedStats,
    global_requests: Arc<AtomicU32>,
) {
    let reader_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    reader_stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .ok();
    let mut reader = BufReader::new(reader_stream);
    let mut conn_bytes = 0u64;
    let mut local_n = 0u32;
    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let Some(head) = read_head(&mut reader) else {
            break;
        };
        local_n += 1;
        let nth = global_requests.fetch_add(1, Ordering::AcqRel) + 1;
        stats.requests.fetch_add(1, Ordering::AcqRel);

        if spec.latency > Duration::ZERO {
            thread::sleep(spec.latency);
        }

        if spec.reset_nth == Some(nth) {
            let _ = stream.shutdown(Shutdown::Both);
            break;
        }

        if let Some(redir) = &spec.redirect {
            let _ = write!(
                stream,
                "HTTP/1.1 {} Redirect\r\nLocation: {}\r\nContent-Length: 0\r\n\r\n",
                redir.status, redir.location
            );
            if !spec.keep_alive {
                break;
            }
            continue;
        }

        if let Some(status) = spec.status {
            let retry = spec
                .retry_after
                .map(|d| format!("Retry-After: {}\r\n", d.as_secs()))
                .unwrap_or_default();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} Error\r\n{retry}Content-Length: 0\r\nConnection: close\r\n\r\n"
            );
            break;
        }

        let range = parse_range(&head, spec.size);
        let (start, end_incl) = range.unwrap_or((0, spec.size.saturating_sub(1)));
        let len = end_incl.saturating_sub(start).saturating_add(1);
        let alt = spec
            .alt_svc_h3
            .map(|p| format!("Alt-Svc: h3=\":{p}\"\r\n"))
            .unwrap_or_default();
        let status_line = if range.is_some() {
            format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end_incl}/{}\r\n",
                spec.size
            )
        } else {
            "HTTP/1.1 200 OK\r\n".into()
        };
        let close = spec.close_after_requests.is_some_and(|n| local_n >= n) || !spec.keep_alive;
        let conn_hdr = if close { "Connection: close\r\n" } else { "" };
        if write!(
            stream,
            "{status_line}Content-Length: {len}\r\nAccept-Ranges: bytes\r\nETag: {}\r\n{alt}{conn_hdr}\r\n",
            spec.etag
        )
        .is_err()
        {
            break;
        }

        let mut send_end = end_incl;
        if let Some(t) = spec.truncate
            && t.nth_request == nth
        {
            send_end = start
                .saturating_add(t.after_bytes)
                .saturating_sub(1)
                .min(end_incl);
        }

        if write_body(
            &mut stream,
            start,
            send_end,
            spec.per_connection_bps,
            spec.corrupt_from,
            &stats,
            &stop,
        )
        .is_err()
        {
            break;
        }
        conn_bytes += send_end.saturating_sub(start).saturating_add(1);

        if spec.truncate.is_some_and(|t| t.nth_request == nth) {
            let _ = stream.shutdown(Shutdown::Both);
            break;
        }
        if close {
            break;
        }
    }
    stats.record_conn_bytes(conn_bytes);
}

fn write_body(
    stream: &mut TcpStream,
    start: u64,
    end_incl: u64,
    bps: Option<u64>,
    corrupt_from: Option<u64>,
    stats: &FixtureStats,
    stop: &AtomicBool,
) -> std::io::Result<()> {
    if start > end_incl {
        return Ok(());
    }
    let block = payload::tile_ref();
    let mut offset = start;
    let origin = Instant::now();
    let mut sent_on_conn = 0u64;
    while offset <= end_incl && !stop.load(Ordering::Acquire) {
        let in_block = (offset % block.len() as u64) as usize;
        let remaining = (end_incl - offset + 1) as usize;
        let take = remaining.min(block.len() - in_block);
        if let Some(limit) = bps {
            let due = Duration::from_secs_f64((sent_on_conn + take as u64) as f64 / limit as f64);
            let wait = due.saturating_sub(origin.elapsed());
            if !wait.is_zero() {
                thread::sleep(wait);
            }
        }
        if let Some(from) = corrupt_from {
            let mut scratch = block[in_block..in_block + take].to_vec();
            for (i, b) in scratch.iter_mut().enumerate() {
                if offset + i as u64 >= from {
                    *b ^= 0x5A;
                }
            }
            stream.write_all(&scratch)?;
        } else {
            stream.write_all(&block[in_block..in_block + take])?;
        }
        stats.bytes_sent.fetch_add(take as u64, Ordering::AcqRel);
        sent_on_conn += take as u64;
        offset += take as u64;
    }
    stream.flush()
}

fn read_head(reader: &mut BufReader<TcpStream>) -> Option<String> {
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

pub(crate) fn parse_range(head: &str, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    for line in head.lines() {
        if let Some(v) = line
            .strip_prefix("range: bytes=")
            .or_else(|| line.strip_prefix("Range: bytes="))
        {
            let v = v.trim();
            let (s, e) = v.split_once('-')?;
            let s: u64 = s.parse().ok()?;
            let e: u64 = if e.is_empty() {
                total.saturating_sub(1)
            } else {
                e.parse::<u64>().ok()?.min(total.saturating_sub(1))
            };
            return Some((s, e));
        }
    }
    None
}
