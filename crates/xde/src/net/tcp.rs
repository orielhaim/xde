use std::time::Duration;

use compio::net::TcpStream;

/// socket2 knobs with no clean alternative. This is why socket2 is a hard
/// dependency rather than a nicety.
#[derive(Debug, Clone, Copy)]
pub struct TcpTuning {
    pub nodelay: bool,
    /// Linux: eager ACKs. Meaningful on high-BDP paths; the kernel may reset it.
    pub quickack: bool,
    pub send_buffer: Option<u32>,
    pub recv_buffer: Option<u32>,
    /// TCP_USER_TIMEOUT: how long unacknowledged data may sit before we give up.
    /// Far better than waiting out the default retransmit schedule on a dead path.
    pub user_timeout: Option<Duration>,
    /// TCP_NOTSENT_LOWAT: bound the bytes queued in the kernel so our own
    /// backpressure signal is not swallowed by the socket buffer.
    pub notsent_lowat: Option<u32>,
    pub keepalive: Option<Duration>,
}

impl Default for TcpTuning {
    fn default() -> Self {
        Self {
            // Bulk transfer: Nagle would batch our (rare, small) requests, and
            // the response side does not care.
            nodelay: true,
            quickack: true,
            send_buffer: None,
            // Let the kernel autotune unless a measurement says otherwise.
            recv_buffer: None,
            user_timeout: Some(Duration::from_secs(30)),
            notsent_lowat: Some(128 * 1024),
            keepalive: Some(Duration::from_secs(60)),
        }
    }
}

/// Applied post-connect. Failures are logged, never fatal: a missing knob on
/// one platform is not a reason to fail a transfer.
pub fn apply_tuning(stream: &TcpStream, t: &TcpTuning) {
    if let Err(e) = stream.set_nodelay(t.nodelay) {
        tracing::debug!(target: "xde::tcp", %e, "set_nodelay failed");
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    if t.quickack {
        let _ = stream.set_quickack(true);
    }

    let sock = socket2_ref(stream);

    if let Some(n) = t.send_buffer {
        let _ = sock.set_send_buffer_size(n as usize);
    }
    if let Some(n) = t.recv_buffer {
        let _ = sock.set_recv_buffer_size(n as usize);
    }
    if let Some(d) = t.keepalive {
        let ka = socket2::TcpKeepalive::new().with_time(d);
        let _ = sock.set_tcp_keepalive(&ka);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if let Some(d) = t.user_timeout {
            let _ = sock.set_tcp_user_timeout(Some(d));
        }
        if let Some(v) = t.notsent_lowat {
            let _ = sock.set_tcp_notsent_lowat(v);
        }
    }
}

fn socket2_ref(stream: &TcpStream) -> socket2::SockRef<'_> {
    #[cfg(unix)]
    {
        socket2::SockRef::from(stream)
    }
    #[cfg(windows)]
    {
        socket2::SockRef::from(stream)
    }
}
