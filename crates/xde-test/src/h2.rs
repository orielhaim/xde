use std::{
    convert::Infallible,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http_body::Frame;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};

use crate::{
    payload,
    spec::{FixtureSpec, FixtureStats, SharedStats},
};

pub struct H2Server {
    pub addr: std::net::SocketAddr,
    pub stats: SharedStats,
    stop: Arc<AtomicBool>,
    _rt: tokio::runtime::Runtime,
}

impl H2Server {
    pub fn shutdown(self) {
        self.stop.store(true, Ordering::Release);
    }

    pub fn url(&self) -> String {
        format!("http://{}/artifact.bin", self.addr)
    }
}

pub fn spawn_h2c(spec: FixtureSpec) -> H2Server {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let stats: SharedStats = Arc::new(FixtureStats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let global_requests = Arc::new(AtomicU32::new(0));
    let addr = rt.block_on({
        let stats = stats.clone();
        let stop = stop.clone();
        async move {
            let listener = tokio::net::TcpListener::bind(crate::fixture_bind_addr())
                .await
                .unwrap();
            let local = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    stats.accepts.fetch_add(1, Ordering::AcqRel);
                    let spec = spec.clone();
                    let stats = stats.clone();
                    let stop = stop.clone();
                    let global_requests = global_requests.clone();
                    tokio::spawn(async move {
                        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                            .max_frame_size(1024 * 1024)
                            .initial_stream_window_size(64 * 1024 * 1024)
                            .initial_connection_window_size(64 * 1024 * 1024)
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                                    let spec = spec.clone();
                                    let stats = stats.clone();
                                    let stop = stop.clone();
                                    let global_requests = global_requests.clone();
                                    async move {
                                        Ok::<_, Infallible>(respond(
                                            req,
                                            spec,
                                            stats,
                                            stop,
                                            global_requests,
                                        ))
                                    }
                                }),
                            )
                            .await;
                    });
                }
            });
            local
        }
    });
    H2Server {
        addr,
        stats,
        stop,
        _rt: rt,
    }
}

fn respond(
    req: hyper::Request<hyper::body::Incoming>,
    spec: FixtureSpec,
    stats: SharedStats,
    stop: Arc<AtomicBool>,
    global_requests: Arc<AtomicU32>,
) -> http::Response<PayloadBody> {
    stats.requests.fetch_add(1, Ordering::AcqRel);
    let nth = global_requests.fetch_add(1, Ordering::AcqRel) + 1;
    if spec.latency > Duration::ZERO {
        std::thread::sleep(spec.latency);
    }
    if let Some(redir) = &spec.redirect {
        return http::Response::builder()
            .status(redir.status)
            .header("location", &redir.location)
            .body(PayloadBody::empty())
            .unwrap();
    }
    if let Some(status) = spec.status {
        let mut b = http::Response::builder().status(status);
        if let Some(d) = spec.retry_after {
            b = b.header("retry-after", d.as_secs().to_string());
        }
        return b.body(PayloadBody::empty()).unwrap();
    }

    let range = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_http_range(v, spec.size));
    let (start, end_incl) = range.unwrap_or((0, spec.size.saturating_sub(1)));
    let mut reset_after: Option<u64> = None;
    if spec.reset_nth == Some(nth) {
        reset_after = Some(0);
    }
    if let Some(t) = spec.truncate
        && t.nth_request == nth
    {
        reset_after = Some(t.after_bytes);
    }
    let mut b = http::Response::builder()
        .header("accept-ranges", "bytes")
        .header("etag", &spec.etag);
    if range.is_some() {
        b = b
            .status(206)
            .header(
                "content-range",
                format!("bytes {start}-{end_incl}/{}", spec.size),
            )
            .header("content-length", (end_incl - start + 1).to_string());
    } else {
        b = b
            .status(200)
            .header("content-length", spec.size.to_string());
    }
    b.body(PayloadBody {
        start,
        end_incl,
        sent: 0,
        stats,
        stop,
        corrupt_from: spec.corrupt_from,
        reset_after,
    })
    .unwrap()
}

fn parse_http_range(v: &str, total: u64) -> Option<(u64, u64)> {
    let v = v.strip_prefix("bytes=")?;
    let (s, e) = v.split_once('-')?;
    let s: u64 = s.parse().ok()?;
    let e: u64 = if e.is_empty() {
        total.saturating_sub(1)
    } else {
        e.parse().ok()?
    };
    Some((s, e.min(total.saturating_sub(1))))
}

pub(crate) struct PayloadBody {
    start: u64,
    end_incl: u64,
    sent: u64,
    stats: SharedStats,
    stop: Arc<AtomicBool>,
    corrupt_from: Option<u64>,
    reset_after: Option<u64>,
}

#[derive(Debug)]
pub struct StreamReset;

impl std::fmt::Display for StreamReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("truncated stream")
    }
}

impl std::error::Error for StreamReset {}

impl PayloadBody {
    fn empty() -> Self {
        Self {
            start: 0,
            end_incl: 0,
            sent: 1,
            stats: Arc::new(FixtureStats::default()),
            stop: Arc::new(AtomicBool::new(false)),
            corrupt_from: None,
            reset_after: None,
        }
    }
}

impl http_body::Body for PayloadBody {
    type Data = Bytes;
    type Error = StreamReset;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        if this.stop.load(Ordering::Acquire) {
            return Poll::Ready(None);
        }
        let total = this.end_incl.saturating_sub(this.start).saturating_add(1);
        if this.sent >= total {
            return Poll::Ready(None);
        }
        if this.reset_after.is_some_and(|n| this.sent >= n) {
            return Poll::Ready(Some(Err(StreamReset)));
        }
        let offset = this.start + this.sent;
        let block = payload::tile_ref();
        let in_block = (offset % block.len() as u64) as usize;
        let mut take = ((total - this.sent) as usize).min(block.len() - in_block);
        if let Some(n) = this.reset_after {
            take = take.min(n.saturating_sub(this.sent) as usize);
        }
        if take == 0 {
            return Poll::Ready(Some(Err(StreamReset)));
        }
        let mut buf = block[in_block..in_block + take].to_vec();
        if let Some(from) = this.corrupt_from {
            for (i, b) in buf.iter_mut().enumerate() {
                if offset + i as u64 >= from {
                    *b ^= 0x5A;
                }
            }
        }
        this.sent += take as u64;
        this.stats
            .bytes_sent
            .fetch_add(take as u64, Ordering::AcqRel);
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(buf)))))
    }
}
