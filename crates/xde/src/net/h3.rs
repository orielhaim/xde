//! HTTP/3 transport over compio-quic.
//!
//! Ownership model (docs/h3-backend-decision.md): a shard-local
//! [`compio_quic::Endpoint`] manages UDP sockets; one
//! [`compio_quic::Connection`] maps to exactly one XDE ConnectionId. The
//! engine never treats an Endpoint as a connection.

use std::{
    pin::Pin,
    time::{Duration, Instant},
};

use crate::core::{Error, Result, error::TransportError};
use bytes::{Buf as _, Bytes};

/// Cloneable handle for opening HTTP/3 request streams on one QUIC
/// connection.
pub type H3SendRequest = h3::client::SendRequest<compio_quic::h3::OpenStreams, Bytes>;
pub type H3BidiStream = <compio_quic::h3::OpenStreams as h3::quic::OpenStreams<Bytes>>::BidiStream;
type H3RequestStream = h3::client::RequestStream<H3BidiStream, Bytes>;

/// Client TLS configuration for HTTP/3: platform certificate verification
/// (matching the rustls setup used for TCP), or no verification for test
/// fixtures against local self-signed servers.
pub fn client_config(accept_invalid_certs: bool) -> Result<compio_quic::ClientConfig> {
    let builder = if accept_invalid_certs {
        compio_quic::ClientBuilder::new_with_no_server_verification()
    } else {
        compio_quic::ClientBuilder::new_with_platform_verifier()
            .map_err(|e| Error::Transport(TransportError::Tls(e.to_string())))?
    };
    let builder = builder.with_alpn_protocols(&["h3"]);
    Ok(builder.build())
}

/// Establish one QUIC connection and its HTTP/3 client layer. The endpoint
/// is shard-owned; this function must run on that shard's thread.
///
/// Returns the cloned stream opener plus the QUIC connection (kept so the
/// owner can close it deterministically).
pub async fn open_h3(
    endpoint: &compio_quic::Endpoint,
    remote: std::net::SocketAddr,
    server_name: &str,
    cfg: Option<compio_quic::ClientConfig>,
) -> Result<(H3SendRequest, compio_quic::Connection)> {
    let connecting = endpoint
        .connect(remote, server_name, cfg)
        .map_err(|e| Error::Transport(TransportError::Tls(format!("quic connect: {e}"))))?;
    // Bounded handshake: a silent peer (blocked UDP, stale advertisement)
    // must surface as a retriable failure, never wedge a shard's command
    // loop.
    let conn = match compio::time::timeout(std::time::Duration::from_secs(5), connecting).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => {
            return Err(Error::Transport(TransportError::Tls(format!(
                "quic handshake: {e}"
            ))));
        }
        Err(_) => {
            return Err(Error::Transport(TransportError::ConnectTimeout));
        }
    };
    let (mut driver, send_request) = h3::client::builder()
        .build::<compio_quic::Connection, compio_quic::h3::OpenStreams, Bytes>(conn.clone())
        .await
        .map_err(|e| {
            Error::Transport(TransportError::ConnectionRetired {
                reason: format!("h3 handshake: {e}"),
            })
        })?;
    // Drive control streams until the connection dies.
    compio::runtime::spawn(async move {
        let _ = driver.wait_idle().await;
    })
    .detach();
    Ok((send_request, conn))
}

/// A response head plus a protocol-agnostic streaming DATA source.
pub struct WireResponse {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: WireBody,
    pub send_ready: Duration,
    pub header_wait: Duration,
}

/// DATA-frame accounting for the H1/H2 vs H2 gap. Reset per response.
#[derive(Debug, Default, Clone)]
pub struct BodyWireStats {
    pub data_frames: u64,
    pub copy_count: u64,
    pub copied_bytes: u64,
    pub pending: Duration,
    pub max_gap: Duration,
    frame_sizes: Vec<u32>,
    pending_since: Option<Instant>,
    last_frame: Option<Instant>,
}

impl BodyWireStats {
    fn note_frame(&mut self, len: usize) {
        if let Some(since) = self.pending_since.take() {
            self.pending += since.elapsed();
        }
        if let Some(last) = self.last_frame {
            self.max_gap = self.max_gap.max(last.elapsed());
        }
        self.last_frame = Some(Instant::now());
        self.data_frames += 1;
        self.frame_sizes
            .push(u32::try_from(len).unwrap_or(u32::MAX));
    }

    fn note_pending(&mut self) {
        if self.pending_since.is_none() {
            self.pending_since = Some(Instant::now());
        }
    }

    pub fn frame_percentile(&self, p: f64) -> u32 {
        if self.frame_sizes.is_empty() {
            return 0;
        }
        let mut v = self.frame_sizes.clone();
        v.sort_unstable();
        let i = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
        v[i.min(v.len() - 1)]
    }

    pub fn avg_frame(&self) -> u32 {
        if self.data_frames == 0 {
            return 0;
        }
        let sum: u64 = self.frame_sizes.iter().map(|s| u64::from(*s)).sum();
        (sum / self.data_frames) as u32
    }
}

enum WireBodyInner {
    Hyper(hyper::body::Incoming),
    H3(Box<H3RequestStream>),
}

pub struct WireBody {
    inner: WireBodyInner,
    pub stats: BodyWireStats,
}

impl WireBody {
    fn hyper(body: hyper::body::Incoming) -> Self {
        Self {
            inner: WireBodyInner::Hyper(body),
            stats: BodyWireStats::default(),
        }
    }

    fn h3(stream: Box<H3RequestStream>) -> Self {
        Self {
            inner: WireBodyInner::H3(stream),
            stats: BodyWireStats::default(),
        }
    }

    /// Next payload chunk, or None at end of body.
    ///
    /// Hyper yields the DATA `Bytes` from h2. That allocation is Hyper/h2's;
    /// XDE does not memcpy it again on the H1/H2 path.
    pub async fn next_data(&mut self) -> Result<Option<Bytes>> {
        match &mut self.inner {
            WireBodyInner::Hyper(body) => {
                std::future::poll_fn(|cx| poll_hyper_frame(body, &mut self.stats, cx)).await
            }
            WireBodyInner::H3(stream) => match stream.recv_data().await {
                Ok(Some(mut buf)) => {
                    let n = buf.remaining();
                    self.stats.note_frame(n);
                    // h3 `Buf` is not `Bytes`; copy_to_bytes is the protocol
                    // boundary until quic/h3 yields owned Bytes.
                    self.stats.copy_count += 1;
                    self.stats.copied_bytes += n as u64;
                    Ok(Some(buf.copy_to_bytes(n)))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(Error::Transport(TransportError::ConnectionRetired {
                    reason: format!("h3 body error: {e}"),
                })),
            },
        }
    }

    /// Drain any remaining body so the underlying stream can be reused.
    pub async fn discard(&mut self) {
        loop {
            match self.next_data().await {
                Ok(Some(d)) if !d.is_empty() => continue,
                _ => break,
            }
        }
    }
}

fn poll_hyper_frame(
    body: &mut hyper::body::Incoming,
    stats: &mut BodyWireStats,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<Result<Option<Bytes>>> {
    use http_body::Body as _;
    use std::task::Poll;

    loop {
        match Pin::new(&mut *body).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                if data.is_empty() {
                    continue;
                }
                stats.note_frame(data.len());
                return Poll::Ready(Ok(Some(data)));
            }
            Poll::Ready(Some(Err(e))) => {
                return Poll::Ready(Err(Error::Transport(TransportError::ConnectionRetired {
                    reason: format!("body error: {e}"),
                })));
            }
            Poll::Ready(None) => return Poll::Ready(Ok(None)),
            Poll::Pending => {
                stats.note_pending();
                return Poll::Pending;
            }
        }
    }
}

/// Issue a GET through whichever session variant this is, read the response
/// head, and return it with the body attached. This is the single wire entry
/// point shared by probe, range fetch and full-body fetch across H1/H2/H3.
pub async fn send_get(
    session: &mut crate::net::HttpSession,
    mut request: http::Request<()>,
) -> Result<WireResponse> {
    use crate::net::HttpSession as S;
    let (status, headers, body, send_ready, header_wait) = match session {
        S::Http1(sender) => {
            let (parts, _) = request.into_parts();
            let req = http::Request::from_parts(parts, crate::net::body::EngineBody::empty());
            let t0 = Instant::now();
            sender.ready().await.map_err(map_send_error)?;
            let send_ready = t0.elapsed();
            let t1 = Instant::now();
            let resp = sender.send_request(req).await.map_err(map_send_error)?;
            let header_wait = t1.elapsed();
            let (head, body) = resp.into_parts();
            (
                head.status,
                head.headers,
                WireBody::hyper(body),
                send_ready,
                header_wait,
            )
        }
        S::Http2(sender) => {
            let (parts, _) = request.into_parts();
            let req = http::Request::from_parts(parts, crate::net::body::EngineBody::empty());
            let t0 = Instant::now();
            sender.ready().await.map_err(map_send_error)?;
            let send_ready = t0.elapsed();
            let t1 = Instant::now();
            let resp = sender.send_request(req).await.map_err(map_send_error)?;
            let header_wait = t1.elapsed();
            let (head, body) = resp.into_parts();
            (
                head.status,
                head.headers,
                WireBody::hyper(body),
                send_ready,
                header_wait,
            )
        }
        S::Http3(sender) => {
            let _ = &mut request;
            let t0 = Instant::now();
            let stream = sender.send_request(request).await.map_err(|e| {
                Error::Transport(TransportError::ConnectionRetired {
                    reason: format!("h3 send_request: {e}"),
                })
            })?;
            let send_ready = t0.elapsed();
            let mut stream = Box::pin(stream);
            let t1 = Instant::now();
            let resp = stream.recv_response().await.map_err(|e| {
                Error::Transport(TransportError::ConnectionRetired {
                    reason: format!("h3 recv_response: {e}"),
                })
            })?;
            let header_wait = t1.elapsed();
            let (head, ()) = resp.into_parts();
            let stream = *Pin::into_inner(stream);
            (
                head.status,
                head.headers,
                WireBody::h3(Box::new(stream)),
                send_ready,
                header_wait,
            )
        }
    };
    Ok(WireResponse {
        status,
        headers,
        body,
        send_ready,
        header_wait,
    })
}

fn map_send_error(e: impl std::fmt::Display) -> Error {
    Error::Transport(TransportError::ConnectionRetired {
        reason: format!("request failed: {e}"),
    })
}
