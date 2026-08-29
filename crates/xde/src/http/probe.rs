use std::time::{Duration, Instant};

use crate::core::{
    error::{Error, Result},
    ranges::ByteRange,
    representation::RemoteFingerprint,
};
use http::{Method, Request};
use url::Url;

use crate::http::{
    apply_request_defaults, host_header, range::parse_content_range, request_target,
};
use crate::net::connector::HttpSession;

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub fingerprint: RemoteFingerprint,
    /// Proven by an actual 206 with a sane Content-Range, not inferred from a header.
    pub supports_ranges: bool,
    pub total_length: Option<u64>,
    pub status: u16,
    pub ttfb: Duration,
    /// The probe body was fully consumed, so the physical connection may carry
    /// the first transfer request without another handshake.
    pub connection_reusable: bool,
    /// Response was content-coded; resume semantics are invalid.
    pub compressed: bool,
    /// `Alt-Svc` advertised HTTP/3 on this UDP port (RFC 7838/9114).
    /// Discovery evidence only; the controller still dials before trusting.
    pub alt_svc_h3_port: Option<u16>,
}

#[derive(Debug, Clone)]
pub enum ProbeOutcome {
    Ready(Box<ProbeResult>),
    Redirect {
        status: u16,
        location: Url,
        /// The redirect response announced `Connection: close`; the physical
        /// connection cannot carry the follow-up request.
        connection_close: bool,
    },
}

/// Source detection never relies on `HEAD`.
///
/// The probe is a `GET` with `Range: bytes=0-0` and `Accept-Encoding: identity`.
/// A `206` carrying a well-formed `Content-Range` is the only real proof.
/// `Accept-Ranges: bytes` is a hint; RFC 9110 says you may try Range without it
/// and promises nothing about the next request.
pub async fn probe_source(
    conn: &mut HttpSession,
    url: &Url,
    extra_headers: &http::HeaderMap,
    allow_compressed: bool,
    deadline: Option<std::time::Instant>,
) -> Result<ProbeOutcome> {
    let started = Instant::now();
    let probe_range = ByteRange::new(0, 1);

    let mut headers = extra_headers.clone();
    headers.insert(
        http::header::HOST,
        http::HeaderValue::from_str(host_header(url)?)
            .map_err(|error| Error::protocol(error.to_string()))?,
    );
    headers.insert(
        http::header::RANGE,
        http::HeaderValue::from_str(&probe_range.to_http_range())
            .expect("byte range is a valid header"),
    );

    if !allow_compressed {
        // Range operates on representation bytes, so identity is the default
        // for any binary download. A compressed response is a state where
        // ordinary resume is simply not valid.
        headers.insert(
            http::header::ACCEPT_ENCODING,
            http::HeaderValue::from_static("identity"),
        );
    }
    apply_request_defaults(&mut headers);

    let mut req = Request::builder()
        .method(Method::GET)
        .uri(request_target(url));
    *req.headers_mut().expect("valid request builder") = headers;
    let req = req.body(()).map_err(|e| Error::protocol(e.to_string()))?;

    let mut resp = before_deadline(deadline, crate::net::h3::send_get(conn, req)).await?;
    let ttfb = started.elapsed();
    let status = resp.status;
    let headers = resp.headers.clone();
    let connection_close = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .chain(headers.get_all("proxy-connection").iter())
        .any(|v| {
            v.as_bytes()
                .split(|&b| b == b',')
                .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"close"))
        });

    if status.is_redirection() {
        let location = headers
            .get(http::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Error::protocol("redirect response had no valid Location"))?;
        let location = url
            .join(location)
            .map_err(|error| Error::protocol(format!("invalid redirect Location: {error}")))?;
        {
            let body = &mut resp.body;
            before_deadline(deadline, async {
                body.discard().await;
                Ok(())
            })
            .await?;
        }
        return Ok(ProbeOutcome::Redirect {
            status: status.as_u16(),
            location,
            connection_close,
        });
    }

    // A compliant 206 contains one byte. If the server ignored Range and sent
    // a full 200, do not accidentally download the whole object as a probe.
    // That body is abandoned unread; the connection is not reusable and the
    // controller retires it.
    if status != http::StatusCode::OK {
        {
            let body = &mut resp.body;
            before_deadline(deadline, async {
                body.discard().await;
                Ok(())
            })
            .await?;
        }
    }

    let mut fp = RemoteFingerprint::from_headers(url.clone(), &headers);

    let (supports_ranges, total_length) = match status {
        http::StatusCode::PARTIAL_CONTENT => {
            let cr = headers
                .get(http::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| Error::protocol("206 without Content-Range"))?;
            let cr = parse_content_range(cr)?;
            cr.validate_against(probe_range)?;
            (true, cr.complete_length)
        }
        http::StatusCode::OK => {
            // Server ignored the Range: single-stream, no resume.
            (false, fp.content_length)
        }
        http::StatusCode::RANGE_NOT_SATISFIABLE => {
            // Often means the object is zero-length or our idea of its size is stale.
            let total = headers
                .get(http::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.rsplit('/').next())
                .and_then(|t| t.trim().parse::<u64>().ok());
            (true, total)
        }
        s if s.is_success() => (false, fp.content_length),
        s => {
            // Classify through the shared disposition table so 401/403
            // become credential refreshes, 503+Retry-After becomes origin
            // backoff, and 4xx/5xx map to their policy actions.
            let retry_after = headers
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(crate::core::disposition::parse_retry_after);
            let disp = crate::core::disposition::classify_status(
                s.as_u16(),
                Some(probe_range),
                retry_after,
                false,
            );
            if matches!(disp, crate::core::Disposition::Accept) {
                return Err(Error::Http(
                    crate::core::error::HttpError::RangeUnsupported { status: s.as_u16() },
                ));
            }
            return Err(Error::Http(crate::core::error::HttpError::Dispositioned(
                Box::new(disp),
                s.as_u16(),
            )));
        }
    };

    if supports_ranges || fp.content_length.is_none() {
        fp.content_length = total_length;
    }

    let alt_svc_h3_port = parse_alt_svc_h3(&headers);

    Ok(ProbeOutcome::Ready(Box::new(ProbeResult {
        compressed: fp.is_compressed(),
        fingerprint: fp,
        supports_ranges,
        total_length,
        status: status.as_u16(),
        ttfb,
        // The probe body was drained (or never existed); the physical
        // connection is clean unless a side announced `Connection: close`
        // or ignored Range and left a full body unread.
        connection_reusable: status != http::StatusCode::OK && !connection_close,
        alt_svc_h3_port,
    })))
}

/// Parse `Alt-Svc: h3=":port"` (or a clear list) into the advertised UDP
/// port. Same-host authorities only; absolute hostnames are ignored.
fn parse_alt_svc_h3(headers: &http::HeaderMap) -> Option<u16> {
    let raw = headers.get(http::header::ALT_SVC)?.to_str().ok()?;
    for entry in raw.split(',') {
        let entry = entry.trim();
        let Some((proto, authority)) = entry.split_once('=') else {
            continue;
        };
        if !proto.trim().eq_ignore_ascii_case("h3") {
            continue;
        }
        let inner = authority.trim().trim_matches('"');
        let Some(port_str) = inner.strip_prefix(':') else {
            continue;
        };
        if let Ok(port) = port_str.trim().parse::<u16>() {
            return Some(port);
        }
    }
    None
}

pub(crate) async fn before_deadline<F, T>(
    deadline: Option<std::time::Instant>,
    future: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let Some(deadline) = deadline else {
        return future.await;
    };
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(Error::DeadlineExceeded);
    }
    let timeout = compio::time::sleep(remaining);
    futures_util::pin_mut!(future, timeout);
    match futures_util::future::select(future, timeout).await {
        futures_util::future::Either::Left((result, _)) => result,
        futures_util::future::Either::Right(((), _)) => Err(Error::DeadlineExceeded),
    }
}
