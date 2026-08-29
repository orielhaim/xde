//! HTTP semantics: probing, range/full-body fetches, redirects, validators.
//!
//! This module owns HTTP-specific behavior only. Identity (fingerprints,
//! validators) and policy (dispositions) are canonical in the core module; this
//! layer produces the evidence the controller consumes.

pub mod fetch;
pub mod probe;
pub mod range;

pub use fetch::{
    ChunkSink, FullBodyFetch, FullBodyFetchOutcome, RangeFetch, RangeFetchOutcome, SourceContext,
    fetch_full_body,
};
pub use probe::{ProbeOutcome, ProbeResult, probe_source};
pub use range::{ContentRange, parse_content_range};

use crate::core::{
    error::{Error, Result},
    ranges::ByteRange,
};
use http::{HeaderMap, HeaderValue};
use url::Url;

/// Header map for an ordinary range request. Kept in one place so the resume
/// path and the fresh path cannot drift apart.
pub fn range_request_headers(
    range: ByteRange,
    if_range: Option<&str>,
    allow_compressed: bool,
    extra: &HeaderMap,
) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        http::header::RANGE,
        http::HeaderValue::from_str(&range.to_http_range()).expect("ascii range"),
    );
    if !allow_compressed {
        h.insert(
            http::header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
    }
    if let Some(v) = if_range
        && let Ok(val) = http::HeaderValue::from_str(v)
    {
        h.insert(http::header::IF_RANGE, val);
    }
    for (name, value) in extra {
        h.append(name, value.clone());
    }
    apply_request_defaults(&mut h);
    h
}

pub(crate) fn apply_request_defaults(headers: &mut HeaderMap) {
    if !headers.contains_key(http::header::USER_AGENT) {
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static(concat!("xde/", env!("CARGO_PKG_VERSION"))),
        );
    }
    if !headers.contains_key(http::header::ACCEPT) {
        headers.insert(http::header::ACCEPT, HeaderValue::from_static("*/*"));
    }
}

pub(crate) fn request_target(url: &Url) -> &str {
    &url[url::Position::BeforePath..url::Position::AfterQuery]
}

pub(crate) fn host_header(url: &Url) -> Result<&str> {
    if url.host().is_none() {
        return Err(Error::protocol("request URL has no host"));
    }
    Ok(&url[url::Position::BeforeHost..url::Position::AfterPort])
}
