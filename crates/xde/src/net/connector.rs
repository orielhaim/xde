use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::core::{
    error::{Error, Result},
    events::Protocol,
    ids::TransportOriginKey,
};
use compio::net::TcpStream;
use compio::tls::TlsStream;
use hyper::client::conn::{http1, http2};

use crate::net::{h2::H2Handle, hyper_io::CompioIo, tcp::TcpTuning, tls::TlsSetup};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireProtocol {
    Http1,
    Http2,
    Http3,
}

impl From<WireProtocol> for Protocol {
    fn from(w: WireProtocol) -> Self {
        match w {
            WireProtocol::Http1 => Protocol::Http1_1,
            WireProtocol::Http2 => Protocol::Http2,
            WireProtocol::Http3 => Protocol::Http3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectTarget {
    pub key: TransportOriginKey,
    pub addr: SocketAddr,
    pub sni: String,
    pub tls: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub tcp: TcpTuning,
    pub connect_timeout: Duration,
    pub h2_initial_stream_window: u32,
    pub h2_initial_conn_window: u32,
    pub h2_adaptive_window: bool,
    /// HTTP/2 max frame size advertised to the peer. Larger frames reduce
    /// per-frame processing overhead on high-throughput streams.
    pub h2_max_frame_size: u32,
    pub h1_read_buf: usize,
    pub h2_read_buf: usize,
    pub h1_writev: bool,
    /// Force H1 even if the origin offers h2. Used when the profile says H2 is
    /// unreliable here, and by the H1-only fallback path.
    pub force_http1: bool,
    pub prior_knowledge_http2: bool,
    /// Test fixture escape hatch: skip TLS certificate verification for
    /// HTTP/3 against local self-signed QUIC servers.
    pub danger_accept_invalid_certs: bool,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            tcp: TcpTuning::default(),
            connect_timeout: Duration::from_secs(15),
            // Large windows on purpose: the default 64KB stream window caps a
            // single H2 stream at roughly (window / RTT), which on a 100ms path
            // is about 5Mbps. That single default is why "H2 is slow for
            // downloads" is folklore. 64 MiB windows were tried against the
            // H2 1c1s stall and made median throughput worse; 8/32 MiB is
            // enough for loopback 1 MiB frames without holding extra credit.
            h2_initial_stream_window: 8 * 1024 * 1024,
            h2_initial_conn_window: 32 * 1024 * 1024,
            // hyper's adaptive_window(true) resets both windows to 64 KiB.
            h2_adaptive_window: false,
            h2_max_frame_size: 1024 * 1024, // 1 MiB - fewer DATA frames
            h1_read_buf: 256 * 1024,
            h2_read_buf: 1024 * 1024,
            h1_writev: true,
            force_http1: false,
            prior_knowledge_http2: false,
            danger_accept_invalid_certs: false,
        }
    }
}

/// One established connection plus the hyper sender for it.
///
/// hyper enters at `client::conn` only, never `hyper_util::Client`: at this
/// level there is no DNS, no connection establishment and no pooling, which is
/// exactly what we want, because all three are decisions the controller must own.
pub enum PhysicalConnection {
    /// One QUIC connection speaking HTTP/3.
    Http3 {
        sender: crate::net::h3::H3SendRequest,
        conn: compio_quic::Connection,
    },
    Http1 {
        sender: Option<http1::SendRequest<crate::net::body::EngineBody>>,
        driver: compio::runtime::JoinHandle<()>,
    },
    Http2 {
        sender: http2::SendRequest<crate::net::body::EngineBody>,
        handle: H2Handle,
    },
}

pub enum HttpSession {
    Http1(http1::SendRequest<crate::net::body::EngineBody>),
    Http2(http2::SendRequest<crate::net::body::EngineBody>),
    /// A clone of the H3 stream opener; one QUIC connection multiplexes
    /// many request streams.
    Http3(crate::net::h3::H3SendRequest),
}

impl std::fmt::Debug for PhysicalConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalConnection::Http1 { .. } => f.write_str("PhysicalConnection::Http1"),
            PhysicalConnection::Http2 { .. } => f.write_str("PhysicalConnection::Http2"),
            PhysicalConnection::Http3 { .. } => f.write_str("PhysicalConnection::Http3"),
        }
    }
}

impl std::fmt::Debug for HttpSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpSession::Http1(_) => f.write_str("HttpSession::Http1"),
            HttpSession::Http2(_) => f.write_str("HttpSession::Http2"),
            HttpSession::Http3(_) => f.write_str("HttpSession::Http3"),
        }
    }
}

impl PhysicalConnection {
    pub fn protocol(&self) -> WireProtocol {
        match self {
            PhysicalConnection::Http1 { .. } => WireProtocol::Http1,
            PhysicalConnection::Http2 { .. } => WireProtocol::Http2,
            PhysicalConnection::Http3 { .. } => WireProtocol::Http3,
        }
    }

    pub fn is_ready(&self) -> bool {
        match self {
            PhysicalConnection::Http1 { sender, .. } => {
                sender.as_ref().is_some_and(http1::SendRequest::is_ready)
            }
            PhysicalConnection::Http2 { sender, .. } => sender.is_ready(),
            // H3 openers are optimistic: failures surface on the next
            // request as ConnectionRetired.
            PhysicalConnection::Http3 { .. } => true,
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            PhysicalConnection::Http1 { sender, .. } => {
                sender.as_ref().is_none_or(http1::SendRequest::is_closed)
            }
            PhysicalConnection::Http2 { sender, .. } => sender.is_closed(),
            // QUIC connections report liveness through their stats; treat
            // them as open until an operation fails or close() was called.
            PhysicalConnection::Http3 { .. } => false,
        }
    }

    pub fn open_session(&mut self) -> Option<HttpSession> {
        match self {
            PhysicalConnection::Http1 { sender, .. } => sender.take().map(HttpSession::Http1),
            PhysicalConnection::Http2 { sender, .. } => Some(HttpSession::Http2(sender.clone())),
            PhysicalConnection::Http3 { sender, .. } => Some(HttpSession::Http3(sender.clone())),
        }
    }

    pub fn return_session(&mut self, stream: HttpSession) {
        if let (PhysicalConnection::Http1 { sender, .. }, HttpSession::Http1(returned)) =
            (self, stream)
        {
            debug_assert!(sender.is_none());
            *sender = Some(returned);
        }
    }

    /// Stop accepting request capacity and wait until the protocol driver has
    /// observed that every sender has gone away.
    pub async fn close(&mut self) {
        match self {
            PhysicalConnection::Http1 { sender, driver } => {
                *sender = None;
                let driver = std::mem::replace(driver, compio::runtime::spawn(async {}));
                let _ = driver.cancel().await;
            }
            PhysicalConnection::Http2 { sender, handle } => {
                let _ = sender;
                handle.shutdown();
                let handle = handle.clone();
                std::future::poll_fn(move |cx| handle.poll_task(cx)).await;
            }
            PhysicalConnection::Http3 { conn, .. } => {
                conn.close(0u32.into(), b"retired");
            }
        }
    }

    /// Clone the cooperative H2 task handle, if this is an H2 connection.
    pub fn h2_handle(&self) -> Option<crate::net::h2::H2Handle> {
        match self {
            PhysicalConnection::Http2 { handle, .. } => Some(handle.clone()),
            _ => None,
        }
    }

    /// Admit a stream future onto this physical H2 connection's cooperative
    /// task. Returns `false` for H1/H3 so the caller can spawn instead.
    pub fn admit_h2_stream(&self, fut: impl std::future::Future<Output = ()> + 'static) -> bool {
        match self.h2_handle() {
            Some(handle) => {
                handle.admit(fut);
                true
            }
            None => false,
        }
    }
}

impl HttpSession {
    pub fn protocol(&self) -> WireProtocol {
        match self {
            HttpSession::Http1(_) => WireProtocol::Http1,
            HttpSession::Http2(_) => WireProtocol::Http2,
            HttpSession::Http3(_) => WireProtocol::Http3,
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            HttpSession::Http1(sender) => sender.is_closed(),
            HttpSession::Http2(sender) => sender.is_closed(),
            // Optimistic, like PhysicalConnection: stream-openers have no
            // cheap liveness probe; failures surface per-request.
            HttpSession::Http3(_) => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectMetrics {
    pub tcp_handshake: Duration,
    pub tls_handshake: Duration,
    pub protocol: WireProtocol,
}

#[derive(Debug)]
pub struct Connector {
    cfg: ConnectorConfig,
    tls: TlsSetup,
    tls_h1_only: TlsSetup,
    /// Pre-built HTTP/3 client TLS configuration, or the reason it could
    /// not be built. Built lazily so engines that never use H3 pay nothing.
    h3_config: std::sync::OnceLock<Option<compio_quic::ClientConfig>>,
}

impl Connector {
    pub fn new(cfg: ConnectorConfig) -> Result<Self> {
        Ok(Self {
            cfg,
            tls: TlsSetup::default_client()?,
            tls_h1_only: TlsSetup::http1_only()?,
            h3_config: std::sync::OnceLock::new(),
        })
    }

    pub fn config(&self) -> &ConnectorConfig {
        &self.cfg
    }

    /// Establish one HTTP/3 connection over `endpoint` (a shard-owned UDP
    /// endpoint). Must run on the endpoint's shard thread.
    pub async fn connect_h3(
        &self,
        endpoint: &compio_quic::Endpoint,
        target: &ConnectTarget,
    ) -> Result<(PhysicalConnection, ConnectMetrics)> {
        let t0 = Instant::now();
        let cfg = self
            .h3_config
            .get_or_init(|| {
                crate::net::h3::client_config(self.cfg.danger_accept_invalid_certs).ok()
            })
            .clone();
        let Some(cfg) = cfg else {
            return Err(Error::Transport(crate::core::error::TransportError::Tls(
                "h3 client TLS configuration unavailable".into(),
            )));
        };
        let (sender, conn) =
            crate::net::h3::open_h3(endpoint, target.addr, &target.sni, Some(cfg)).await?;
        Ok((
            PhysicalConnection::Http3 { sender, conn },
            ConnectMetrics {
                tcp_handshake: t0.elapsed(),
                tls_handshake: Duration::ZERO,
                protocol: WireProtocol::Http3,
            },
        ))
    }

    pub async fn connect(
        &self,
        target: &ConnectTarget,
    ) -> Result<(PhysicalConnection, ConnectMetrics)> {
        let t0 = Instant::now();
        let connect = TcpStream::connect(target.addr);
        let timeout = compio::time::sleep(self.cfg.connect_timeout);
        futures_util::pin_mut!(connect, timeout);
        let tcp = match futures_util::future::select(connect, timeout).await {
            futures_util::future::Either::Left((result, _)) => {
                result.map_err(|e| Error::Transport(crate::core::error::TransportError::Io(e)))?
            }
            futures_util::future::Either::Right(((), _)) => return Err(Error::DeadlineExceeded),
        };
        let tcp_handshake = t0.elapsed();
        crate::net::tcp::apply_tuning(&tcp, &self.cfg.tcp);

        if !target.tls {
            let read_buf = if self.cfg.prior_knowledge_http2 {
                self.cfg.h2_read_buf
            } else {
                self.cfg.h1_read_buf
            };
            let io = CompioIo::from_split(tcp, read_buf);
            let (conn, protocol) = if self.cfg.prior_knowledge_http2 {
                (self.handshake_h2(io).await?, WireProtocol::Http2)
            } else {
                (self.handshake_h1(io).await?, WireProtocol::Http1)
            };
            return Ok((
                conn,
                ConnectMetrics {
                    tcp_handshake,
                    tls_handshake: Duration::ZERO,
                    protocol,
                },
            ));
        }

        let t1 = Instant::now();
        let setup = if self.cfg.force_http1 {
            &self.tls_h1_only
        } else {
            &self.tls
        };
        let tls_stream: TlsStream<TcpStream> = setup
            .connector()
            .connect(&target.sni, tcp)
            .await
            .map_err(|e| Error::Transport(crate::core::error::TransportError::Io(e)))?;
        let tls_handshake = t1.elapsed();

        let alpn_h2 = tls_stream
            .negotiated_alpn()
            .is_some_and(|p| p.as_ref() == b"h2");

        let io = CompioIo::new(tls_stream);
        let (conn, protocol) = if alpn_h2 && !self.cfg.force_http1 {
            (self.handshake_h2(io).await?, WireProtocol::Http2)
        } else {
            (self.handshake_h1(io).await?, WireProtocol::Http1)
        };

        Ok((
            conn,
            ConnectMetrics {
                tcp_handshake,
                tls_handshake,
                protocol,
            },
        ))
    }

    async fn handshake_h1<S>(&self, io: CompioIo<S>) -> Result<PhysicalConnection>
    where
        CompioIo<S>: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
        S: 'static,
    {
        let (sender, conn) = http1::Builder::new()
            .writev(self.cfg.h1_writev)
            .max_buf_size(self.cfg.h1_read_buf)
            // Bulk bodies; title-case rewriting is pure overhead.
            .preserve_header_case(false)
            .handshake(io)
            .await
            .map_err(|e| Error::protocol(format!("h1 handshake: {e}")))?;

        let driver = compio::runtime::spawn(async move {
            // Driver errors are expected on retired connections; the
            // controller learns about them via request failures.
            if let Err(e) = conn.await {
                tracing::debug!(target: "xde::net", error = %e, "h1 driver ended");
            }
        });
        Ok(PhysicalConnection::Http1 {
            sender: Some(sender),
            driver,
        })
    }

    async fn handshake_h2<S>(&self, io: CompioIo<S>) -> Result<PhysicalConnection>
    where
        CompioIo<S>: hyper::rt::Read + hyper::rt::Write + Unpin + 'static,
        S: 'static,
    {
        // Window setters disable adaptive flow control. Calling
        // adaptive_window(true) afterwards would throw away the 8/32 MiB
        // windows and pin the stream at the spec 64 KiB default.
        let handle = H2Handle::new();
        let mut builder = http2::Builder::new(handle.executor());
        builder.keep_alive_interval(None);
        builder.max_frame_size(self.cfg.h2_max_frame_size);
        if self.cfg.h2_adaptive_window {
            builder.adaptive_window(true);
        } else {
            builder.adaptive_window(false);
            builder.initial_stream_window_size(self.cfg.h2_initial_stream_window);
            builder.initial_connection_window_size(self.cfg.h2_initial_conn_window);
        }
        let (sender, conn) = builder
            .handshake(io)
            .await
            .map_err(|e| Error::protocol(format!("h2 handshake: {e}")))?;

        handle.set_dispatcher(async move {
            let _ = conn.await;
        });
        Ok(PhysicalConnection::Http2 { sender, handle })
    }
}
