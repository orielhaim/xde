use std::{
    cell::RefCell,
    io,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use compio::io::{AsyncRead, AsyncWrite, util::Splittable};
use compio::tls::TlsStream;
use compio_io::compat::AsyncStream;
use send_wrapper::SendWrapper;

/// Shard-local IOCP receive accounting for the Compio ↔ Hyper bridge.
#[derive(Debug, Clone, Copy, Default)]
pub struct IoReadCounters {
    pub submitted: u64,
    pub completed: u64,
    pub inflight: u32,
    pub zero_read: Duration,
    pub max_zero: Duration,
}

struct IoReadState {
    counters: IoReadCounters,
    zero_since: Option<Instant>,
}

thread_local! {
    static IO_READ: RefCell<IoReadState> = RefCell::new(IoReadState {
        counters: IoReadCounters::default(),
        zero_since: None,
    });
}

/// Snapshot receive-arm counters for the current Compio shard thread.
pub fn io_read_snapshot() -> IoReadCounters {
    IO_READ.with(|cell| {
        let mut state = cell.borrow_mut();
        if let Some(since) = state.zero_since {
            let d = since.elapsed();
            state.counters.zero_read += d;
            state.counters.max_zero = state.counters.max_zero.max(d);
            state.zero_since = Some(Instant::now());
        }
        state.counters
    })
}

fn note_read_submit() {
    IO_READ.with(|cell| {
        let mut state = cell.borrow_mut();
        if state.counters.inflight == 0
            && let Some(since) = state.zero_since.take()
        {
            let d = since.elapsed();
            state.counters.zero_read += d;
            state.counters.max_zero = state.counters.max_zero.max(d);
        }
        state.counters.submitted += 1;
        state.counters.inflight = 1;
    });
}

fn note_read_complete() {
    IO_READ.with(|cell| {
        let mut state = cell.borrow_mut();
        state.counters.completed += 1;
        state.counters.inflight = 0;
        state.zero_since = Some(Instant::now());
    });
}

/// Bridges a Compio stream into `hyper::rt::{Read, Write}`.
///
/// Hyper's `ReadBufCursor::initialize_unfilled` zeros the spare buffer.
/// Cleartext uses `poll_read_uninit` so that memset never happens. TLS cannot
/// split the same way, so those reads cap zero-fill at 64 KiB.
pub struct CompioIo<S> {
    inner: SendWrapper<Pin<Box<S>>>,
    read_armed: bool,
}

impl<S> CompioIo<S> {
    pub fn new(stream: S) -> Self {
        Self {
            inner: SendWrapper::new(Box::pin(stream)),
            read_armed: false,
        }
    }
}

impl<S: Splittable + 'static> CompioIo<AsyncStream<S>>
where
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    pub fn from_split(stream: S, read: usize) -> Self {
        Self::new(AsyncStream::with_capacity(read, stream))
    }
}

impl<S> std::fmt::Debug for CompioIo<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompioIo").finish_non_exhaustive()
    }
}

impl<S: Splittable + 'static> hyper::rt::Read for CompioIo<AsyncStream<S>>
where
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let dst: &mut [MaybeUninit<u8>] = unsafe { buf.as_mut() };
        match AsyncStream::<S>::poll_read_uninit(self.inner.as_mut().as_mut(), cx, dst) {
            Poll::Pending => {
                if !self.read_armed {
                    self.read_armed = true;
                    note_read_submit();
                }
                Poll::Pending
            }
            Poll::Ready(Err(e)) => {
                self.read_armed = false;
                Poll::Ready(Err(e))
            }
            Poll::Ready(Ok(n)) => {
                if self.read_armed {
                    self.read_armed = false;
                    note_read_complete();
                }
                if n > dst.len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stream reported more bytes than the read buffer can hold",
                    )));
                }
                unsafe { buf.advance(n) };
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<S: Splittable + 'static> hyper::rt::Write for CompioIo<AsyncStream<S>>
where
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures_util::AsyncWrite::poll_write(self.inner.as_mut().as_mut(), cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        futures_util::AsyncWrite::poll_write_vectored(self.inner.as_mut().as_mut(), cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_util::AsyncWrite::poll_flush(self.inner.as_mut().as_mut(), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_util::AsyncWrite::poll_close(self.inner.as_mut().as_mut(), cx)
    }
}

impl<S: futures_util::AsyncRead + futures_util::AsyncWrite + Unpin + 'static> hyper::rt::Read
    for CompioIo<TlsStream<S>>
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let want = buf.remaining().min(64 * 1024);
        let uninit = buf.initialize_unfilled_to(want);
        let cap = uninit.len();
        match futures_util::AsyncRead::poll_read(self.inner.as_mut().as_mut(), cx, uninit) {
            Poll::Pending => {
                if !self.read_armed {
                    self.read_armed = true;
                    note_read_submit();
                }
                Poll::Pending
            }
            Poll::Ready(Err(e)) => {
                self.read_armed = false;
                Poll::Ready(Err(e))
            }
            Poll::Ready(Ok(n)) => {
                if self.read_armed {
                    self.read_armed = false;
                    note_read_complete();
                }
                if n > cap {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "stream reported more bytes than the read buffer can hold",
                    )));
                }
                unsafe { buf.advance(n) };
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<S: futures_util::AsyncRead + futures_util::AsyncWrite + Unpin + 'static> hyper::rt::Write
    for CompioIo<TlsStream<S>>
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures_util::AsyncWrite::poll_write(self.inner.as_mut().as_mut(), cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        futures_util::AsyncWrite::poll_write_vectored(self.inner.as_mut().as_mut(), cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_util::AsyncWrite::poll_flush(self.inner.as_mut().as_mut(), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_util::AsyncWrite::poll_close(self.inner.as_mut().as_mut(), cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{body::EngineBody, exec::CompioExec};
    use bytes::Bytes;
    use http_body::Body;
    use std::{convert::Infallible, sync::Arc};

    #[rstest::rstest]
    #[case::h1(false)]
    #[case::h2(true)]
    fn tls_bridge_receives_the_complete_response(#[case] h2: bool) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let alpn = if h2 {
            b"h2".to_vec()
        } else {
            b"http/1.1".to_vec()
        };
        let mut server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::from(key),
            )
            .unwrap();
        server_config.alpn_protocols = vec![alpn.clone()];
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let mut client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_config.alpn_protocols = vec![alpn.clone()];
        let expected: Vec<_> = (0..128 * 1024).map(|n| (n % 251) as u8).collect();
        compio::runtime::Runtime::new().unwrap().block_on(async {
            compio::time::timeout(Duration::from_secs(10), async {
                let listener = compio::net::TcpListener::bind(("127.0.0.1", 0))
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                let payload = Bytes::from(expected.clone());
                let server = compio::runtime::spawn(async move {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let tls = compio::tls::TlsAcceptor::from(Arc::new(server_config))
                        .accept(Box::pin(AsyncStream::new(tcp)))
                        .await
                        .unwrap();
                    let service = hyper::service::service_fn(move |_| {
                        let body = payload.clone();
                        async move {
                            Ok::<_, Infallible>(http::Response::new(EngineBody::from_bytes(body)))
                        }
                    });
                    if h2 {
                        let _ = hyper::server::conn::http2::Builder::new(CompioExec)
                            .serve_connection(CompioIo::new(tls), service)
                            .await;
                    } else {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(CompioIo::new(tls), service)
                            .await;
                    }
                });
                let tcp = compio::net::TcpStream::connect(addr).await.unwrap();
                let tls = compio::tls::TlsConnector::from(Arc::new(client_config))
                    .connect("localhost", Box::pin(AsyncStream::new(tcp)))
                    .await
                    .unwrap();
                assert_eq!(tls.negotiated_alpn().unwrap(), alpn);
                let request = http::Request::builder()
                    .uri("https://localhost/artifact")
                    .body(EngineBody::empty())
                    .unwrap();
                let (response, driver) = if h2 {
                    let (mut sender, connection) =
                        hyper::client::conn::http2::Builder::new(CompioExec)
                            .handshake(CompioIo::new(tls))
                            .await
                            .unwrap();
                    let driver = compio::runtime::spawn(async move {
                        let _ = connection.await;
                    });
                    (sender.send_request(request).await.unwrap(), driver)
                } else {
                    let (mut sender, connection) =
                        hyper::client::conn::http1::handshake(CompioIo::new(tls))
                            .await
                            .unwrap();
                    let driver = compio::runtime::spawn(async move {
                        let _ = connection.await;
                    });
                    (sender.send_request(request).await.unwrap(), driver)
                };
                assert_eq!(response.status(), 200);
                let mut body = Box::pin(response.into_body());
                let mut received = Vec::new();
                while let Some(frame) =
                    std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await
                {
                    if let Ok(data) = frame.unwrap().into_data() {
                        received.extend_from_slice(&data);
                    }
                }
                assert_eq!(received, expected);
                let _ = driver.cancel().await;
                let _ = server.cancel().await;
            })
            .await
            .unwrap();
        });
    }
}
