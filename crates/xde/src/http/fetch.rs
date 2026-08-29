use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::core::{
    context::JobContext,
    error::{Error, Result},
    metrics::TransferSample,
    ranges::ByteRange,
    representation::RepresentationLock,
};
use crate::net::connector::HttpSession;
use crate::storage::{MemoryBudget, TransferChunk};
use http::{Method, Request};
use url::Url;

use crate::http::{
    host_header, probe::before_deadline, range::parse_content_range, range_request_headers,
    request_target,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFetchCompletion {
    FullyConsumed,
    IntentionallyTruncated,
}

/// What a single range fetch produced.
#[derive(Debug)]
pub struct RangeFetchOutcome {
    /// Where the request's time actually went.
    pub sample: TransferSample,
    pub completion: RangeFetchCompletion,
    /// The scheduler shortened this range while the response was active.
    /// For H1 the unread response body makes the connection non-reusable.
    pub truncated: bool,
    /// The connection can carry another request (H1 body fully drained).
    pub connection_reusable: bool,
}

impl RangeFetchOutcome {
    pub fn bytes(&self) -> u64 {
        self.sample.bytes
    }
}

#[derive(Debug)]
pub struct FullBodyFetchOutcome {
    pub sample: TransferSample,
    pub connection_reusable: bool,
}

#[derive(Debug, Clone)]
pub struct SourceContext {
    pub url: Url,
    /// Strong validator (ETag) for If-Range. Only ever populated with a
    /// *strong* validator; weak ones must not be sent per RFC 9110.
    pub if_range: Option<String>,
    pub allow_compressed: bool,
    pub extra_headers: http::HeaderMap,
    pub deadline: Option<std::time::Instant>,
}

#[derive(Debug, Clone)]
pub struct RangeFetch {
    pub source: Arc<SourceContext>,
    /// Half-open byte interval this assignment owns.
    pub range: ByteRange,
    /// Bytes of overlap prefix requested before `range.start`, for boundary
    /// verification against already-verified data.
    pub overlap: u32,
    pub is_resume: bool,
}

#[derive(Debug, Clone)]
pub struct FullBodyFetch {
    pub source: Arc<SourceContext>,
}

/// Callback invoked with protocol-owned data and its absolute offset.
pub trait ChunkSink {
    fn accept(&mut self, chunk: TransferChunk) -> impl Future<Output = Result<()>>;
    fn finish(&mut self) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }

    /// Exclusive absolute offset currently wanted by the scheduler. A sink may
    /// lower this while the response is active to hand the tail to another
    /// worker. The fetcher never delivers bytes at or beyond this boundary.
    fn end_offset(&self) -> Option<u64> {
        None
    }

    fn max_chunk_size(&self) -> usize {
        usize::MAX
    }

    /// Forward-progress class of the chunk landing at `offset`. Frontier
    /// chunks may consume the memory budget's reserve; speculative ones
    /// must not.
    fn progress_class(&self, _offset: u64) -> crate::storage::ProgressClass {
        crate::storage::ProgressClass::Speculative
    }
}

fn observe_identity(
    lock: &mut RepresentationLock,
    headers: &http::HeaderMap,
    content_range_total: Option<u64>,
) -> Result<()> {
    let etag = headers
        .get(http::header::ETAG)
        .and_then(|v| v.to_str().ok());
    let last_modified = headers
        .get(http::header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .and_then(crate::core::representation::parse_http_date);
    lock.validate(etag, last_modified, content_range_total)
}

#[allow(clippy::too_many_arguments)]
async fn deliver_range_bytes<S: ChunkSink>(
    data: &bytes::Bytes,
    fetch: &RangeFetch,
    sink: &mut S,
    memory: &MemoryBudget,
    context: &JobContext,
    wire_range: ByteRange,
    offset: &mut u64,
    received: &mut u64,
    useful_received: &mut u64,
    truncated: &mut bool,
    memory_blocked: &mut Duration,
    destination_blocked: &mut Duration,
    dest_accepts: &mut u64,
) -> Result<()> {
    let mut consumed = 0;
    while consumed < data.len() {
        let dynamic_end = sink
            .end_offset()
            .unwrap_or(wire_range.end)
            .min(wire_range.end);
        if *offset >= dynamic_end {
            *truncated = dynamic_end < wire_range.end;
            break;
        }
        let remaining_wire = (wire_range.end - *offset) as usize;
        if data.len() - consumed > remaining_wire && sink.end_offset().is_none() {
            return Err(Error::Http(crate::core::error::HttpError::OversizedBody {
                received: *received + (data.len() - consumed) as u64,
                expected: wire_range.len(),
            }));
        }
        let amount = (data.len() - consumed)
            .min((dynamic_end - *offset) as usize)
            .min(memory.limit() as usize)
            .min(sink.max_chunk_size());
        let payload = data.slice(consumed..consumed + amount);
        let mem_started = Instant::now();
        let class = sink.progress_class(*offset);
        let chunk = TransferChunk::bytes_classed(*offset, payload, memory, class, context).await?;
        *memory_blocked += mem_started.elapsed();
        let dest_started = Instant::now();
        sink.accept(chunk).await?;
        *destination_blocked += dest_started.elapsed();
        *dest_accepts += 1;
        consumed += amount;
        *offset += amount as u64;
        *received += amount as u64;
        let owned_start = (*offset - amount as u64).max(fetch.range.start);
        let owned_end = (*offset).min(fetch.range.end);
        *useful_received = useful_received.saturating_add(owned_end.saturating_sub(owned_start));
    }
    Ok(())
}

/// Execute one range request and stream its body into the sink.
///
/// This is NOT full-body-with-flags: the request carries an exact Range, the
/// 206/Content-Range contract is validated, and any 200 answer is rejected as
/// a capability change for the controller to handle.
pub async fn fetch_range<S: ChunkSink>(
    conn: &mut HttpSession,
    memory: &MemoryBudget,
    fetch: &RangeFetch,
    sink: &mut S,
    context: &JobContext,
    rep_lock: &std::sync::Mutex<RepresentationLock>,
) -> Result<RangeFetchOutcome> {
    let wire_range = ByteRange::new(
        fetch.range.start.saturating_sub(u64::from(fetch.overlap)),
        fetch.range.end,
    );
    let mut headers = range_request_headers(
        wire_range,
        fetch.source.if_range.as_deref(),
        fetch.source.allow_compressed,
        &fetch.source.extra_headers,
    );
    if let Ok(host) = host_header(&fetch.source.url)
        && let Ok(value) = http::HeaderValue::from_str(host)
    {
        headers.insert(http::header::HOST, value);
    }
    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(request_target(&fetch.source.url));
    *builder.headers_mut().expect("valid builder") = headers;
    let request = builder
        .body(())
        .map_err(|error| Error::protocol(error.to_string()))?;

    let started = Instant::now();
    let mut response = before_deadline(
        fetch.source.deadline,
        crate::net::h3::send_get(conn, request),
    )
    .await?;
    let ttfb = started.elapsed();
    let send_ready = response.send_ready;
    let header_wait = response.header_wait;
    let status = response.status;
    let connection_close = has_connection_close(&response.headers);
    let retry_after = response
        .headers
        .get(http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(crate::core::disposition::parse_retry_after);
    let disposition = crate::core::disposition::classify_status(
        status.as_u16(),
        Some(wire_range),
        retry_after,
        fetch.is_resume,
    );
    // Non-accept dispositions travel back to the controller untouched; the
    // fetcher never invents retry decisions.
    if !matches!(disposition, crate::core::Disposition::Accept) {
        return Err(Error::Http(crate::core::error::HttpError::Dispositioned(
            Box::new(disposition),
            status.as_u16(),
        )));
    }

    let observed_total = if status == http::StatusCode::PARTIAL_CONTENT {
        let value = response
            .headers
            .get(http::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Error::protocol("206 without Content-Range"))?;
        let content_range = parse_content_range(value)?;
        content_range.validate_against(wire_range)?;
        content_range.complete_length
    } else {
        // A 200 to an explicitly ranged GET means the server ignored Range
        // and sent the full representation. Never consume it here: the
        // controller must downgrade the whole job to full-body mode so byte
        // offsets stay correct.
        return Err(Error::Http(
            crate::core::error::HttpError::RangeCapabilityLost,
        ));
    };

    if !fetch.source.allow_compressed
        && response
            .headers
            .get(http::header::CONTENT_ENCODING)
            .is_some_and(|value| value.as_bytes() != b"identity")
    {
        return Err(Error::Http(
            crate::core::error::HttpError::CompressedPayload,
        ));
    }

    {
        let mut lock = rep_lock
            .lock()
            .map_err(|_| Error::protocol("representation lock poisoned"))?;
        observe_identity(&mut lock, &response.headers, observed_total)?;
    }

    let expected = if status == http::StatusCode::PARTIAL_CONTENT {
        Some(wire_range.len())
    } else {
        observed_total
    };

    let mut offset = wire_range.start;
    let mut received = 0_u64;
    let mut useful_received = 0_u64;
    let mut truncated = false;
    let mut receive_active = Duration::ZERO;
    let mut memory_blocked = Duration::ZERO;
    let mut destination_blocked = Duration::ZERO;
    let mut dest_accepts = 0_u64;
    let body = &mut response.body;
    let io0 = crate::net::hyper_io::io_read_snapshot();
    loop {
        let recv_started = Instant::now();
        let data = before_deadline(fetch.source.deadline, body.next_data()).await?;
        receive_active += recv_started.elapsed();
        let Some(data) = data else { break };
        deliver_range_bytes(
            &data,
            fetch,
            sink,
            memory,
            context,
            wire_range,
            &mut offset,
            &mut received,
            &mut useful_received,
            &mut truncated,
            &mut memory_blocked,
            &mut destination_blocked,
            &mut dest_accepts,
        )
        .await?;
        if truncated {
            break;
        }
    }

    sink.finish().await?;
    let response_wall = started.elapsed().max(Duration::from_nanos(1));

    if !truncated
        && let Some(expected) = expected
        && received != expected
    {
        if received > expected {
            return Err(Error::Http(crate::core::error::HttpError::OversizedBody {
                received,
                expected,
            }));
        }
        return Err(Error::Http(crate::core::error::HttpError::PrematureEof {
            received,
            expected,
        }));
    }
    let completion = if truncated {
        RangeFetchCompletion::IntentionallyTruncated
    } else {
        RangeFetchCompletion::FullyConsumed
    };
    // A physical connection may carry another request only when the response
    // ended naturally: every body byte was consumed, nothing was truncated,
    // and neither side announced `Connection: close`. An intentional
    // truncation leaves unread body bytes on an H1 socket, which poisons it.
    // H2 senders are clones; a cleanly finished stream never poisons the
    // physical connection (GOAWAY surfaces as a transport error instead).
    let connection_reusable = !connection_close
        && match conn {
            HttpSession::Http1(_) => completion == RangeFetchCompletion::FullyConsumed,
            HttpSession::Http2(sender) => sender.is_ready() && !sender.is_closed(),
            HttpSession::Http3(_) => true,
        };
    let io = io_delta(io0);
    Ok(RangeFetchOutcome {
        sample: TransferSample {
            bytes: useful_received,
            ttfb,
            response_wall,
            receive_active: body.stats.pending,
            memory_blocked,
            destination_blocked,
            next_pending: body.stats.pending,
            max_frame_gap: body.stats.max_gap,
            send_ready,
            headers: header_wait,
            data_frames: body.stats.data_frames,
            dest_accepts,
            copy_count: body.stats.copy_count,
            copied_bytes: body.stats.copied_bytes,
            frame_p50: body.stats.frame_percentile(50.0),
            frame_p90: body.stats.frame_percentile(90.0),
            avg_frame: body.stats.avg_frame(),
            io_reads_submitted: io.submitted,
            io_reads_completed: io.completed,
            zero_read: io.zero_read,
            max_zero_read: io.max_zero,
        },
        completion,
        truncated,
        connection_reusable,
    })
}

fn io_delta(start: crate::net::hyper_io::IoReadCounters) -> crate::net::hyper_io::IoReadCounters {
    let end = crate::net::hyper_io::io_read_snapshot();
    crate::net::hyper_io::IoReadCounters {
        submitted: end.submitted.saturating_sub(start.submitted),
        completed: end.completed.saturating_sub(start.completed),
        inflight: end.inflight,
        zero_read: end.zero_read.saturating_sub(start.zero_read),
        max_zero: end.max_zero,
    }
}

/// RFC 9110 `Connection`: a listed token of `close` ends the physical
/// connection after this message. `Proxy-Connection` is checked too because
/// intermediaries still emit it.
fn has_connection_close(headers: &http::HeaderMap) -> bool {
    fn lists_close(value: &[u8]) -> bool {
        value
            .split(|&b| b == b',')
            .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"close"))
    }
    headers
        .get_all(http::header::CONNECTION)
        .iter()
        .chain(headers.get_all("proxy-connection").iter())
        .any(|v| lists_close(v.as_bytes()))
}

/// Fetch the entire body in one HTTP request.
///
/// Deliberately separate from `fetch_range`: no Range header, no If-Range, no
/// Content-Range validation; completion is validated EOF/content-length.
pub async fn fetch_full_body<S: ChunkSink>(
    conn: &mut HttpSession,
    memory: &MemoryBudget,
    fetch: &FullBodyFetch,
    sink: &mut S,
    context: &JobContext,
    rep_lock: &std::sync::Mutex<RepresentationLock>,
) -> Result<FullBodyFetchOutcome> {
    let mut headers = fetch.source.extra_headers.clone();
    crate::http::apply_request_defaults(&mut headers);
    if !fetch.source.allow_compressed {
        headers.insert(
            http::header::ACCEPT_ENCODING,
            http::HeaderValue::from_static("identity"),
        );
    }
    if let Ok(host) = host_header(&fetch.source.url)
        && let Ok(value) = http::HeaderValue::from_str(host)
    {
        headers.insert(http::header::HOST, value);
    }
    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(request_target(&fetch.source.url));
    *builder.headers_mut().expect("valid builder") = headers;
    let request = builder
        .body(())
        .map_err(|error| Error::protocol(error.to_string()))?;

    let started = Instant::now();
    let mut response = before_deadline(
        fetch.source.deadline,
        crate::net::h3::send_get(conn, request),
    )
    .await?;
    let ttfb = started.elapsed();
    let send_ready = response.send_ready;
    let header_wait = response.header_wait;
    let status = response.status;
    let connection_close = has_connection_close(&response.headers);
    if status == http::StatusCode::PARTIAL_CONTENT {
        return Err(Error::Http(
            crate::core::error::HttpError::UnexpectedPartialContent,
        ));
    }
    let retry_after = response
        .headers
        .get(http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(crate::core::disposition::parse_retry_after);
    let disposition =
        crate::core::disposition::classify_status(status.as_u16(), None, retry_after, false);
    if !matches!(disposition, crate::core::Disposition::Accept) {
        return Err(Error::Http(crate::core::error::HttpError::Dispositioned(
            Box::new(disposition),
            status.as_u16(),
        )));
    }
    if !fetch.source.allow_compressed
        && response
            .headers
            .get(http::header::CONTENT_ENCODING)
            .is_some_and(|value| value.as_bytes() != b"identity")
    {
        return Err(Error::Http(
            crate::core::error::HttpError::CompressedPayload,
        ));
    }
    let expected = response
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    {
        let mut lock = rep_lock
            .lock()
            .map_err(|_| Error::protocol("representation lock poisoned"))?;
        observe_identity(&mut lock, &response.headers, expected)?;
    }

    let body = &mut response.body;
    let mut offset = 0_u64;
    let mut receive_active = Duration::ZERO;
    let mut memory_blocked = Duration::ZERO;
    let mut destination_blocked = Duration::ZERO;
    let mut dest_accepts = 0_u64;
    let io0 = crate::net::hyper_io::io_read_snapshot();
    loop {
        let recv_started = Instant::now();
        let data = before_deadline(fetch.source.deadline, body.next_data()).await?;
        receive_active += recv_started.elapsed();
        let Some(data) = data else { break };
        let mut consumed = 0;
        while consumed < data.len() {
            let amount = (data.len() - consumed)
                .min(memory.limit() as usize)
                .min(sink.max_chunk_size());
            let mem_started = Instant::now();
            let class = sink.progress_class(offset);
            let chunk = TransferChunk::bytes_classed(
                offset,
                data.slice(consumed..consumed + amount),
                memory,
                class,
                context,
            )
            .await?;
            memory_blocked += mem_started.elapsed();
            let dest_started = Instant::now();
            sink.accept(chunk).await?;
            destination_blocked += dest_started.elapsed();
            dest_accepts += 1;
            consumed += amount;
            offset += amount as u64;
        }
    }
    sink.finish().await?;
    let io = io_delta(io0);
    let response_wall = started.elapsed().max(Duration::from_nanos(1));
    if let Some(expected) = expected
        && offset != expected
    {
        if offset > expected {
            return Err(Error::Http(crate::core::error::HttpError::OversizedBody {
                received: offset,
                expected,
            }));
        }
        return Err(Error::Http(crate::core::error::HttpError::PrematureEof {
            received: offset,
            expected,
        }));
    }
    Ok(FullBodyFetchOutcome {
        sample: TransferSample {
            bytes: offset,
            ttfb,
            response_wall,
            receive_active: body.stats.pending,
            memory_blocked,
            destination_blocked,
            next_pending: body.stats.pending,
            max_frame_gap: body.stats.max_gap,
            send_ready,
            headers: header_wait,
            data_frames: body.stats.data_frames,
            dest_accepts,
            copy_count: body.stats.copy_count,
            copied_bytes: body.stats.copied_bytes,
            frame_p50: body.stats.frame_percentile(50.0),
            frame_p90: body.stats.frame_percentile(90.0),
            avg_frame: body.stats.avg_frame(),
            io_reads_submitted: io.submitted,
            io_reads_completed: io.completed,
            zero_read: io.zero_read,
            max_zero_read: io.max_zero,
        },
        connection_reusable: !connection_close
            && match conn {
                HttpSession::Http1(_) => true,
                HttpSession::Http2(sender) => sender.is_ready() && !sender.is_closed(),
                // H3 openers have no cheap liveness probe; a dead QUIC connection
                // surfaces as ConnectionRetired on the next request.
                HttpSession::Http3(_) => true,
            },
    })
}
