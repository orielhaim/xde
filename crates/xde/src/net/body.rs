use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

/// Request bodies only. We are a downloader: every request we send is empty.
/// Keeping a dedicated type rather than `Empty<Bytes>` means adding an upload
/// path later does not change every signature in the crate.
#[derive(Debug, Default, Clone)]
pub struct EngineBody {
    inner: Option<Bytes>,
}

impl EngineBody {
    pub fn empty() -> Self {
        Self { inner: None }
    }
    pub fn from_bytes(b: Bytes) -> Self {
        Self { inner: Some(b) }
    }
}

impl Body for EngineBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.inner.take().map(|b| Ok(Frame::data(b))))
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.inner.as_ref().map_or(0, |b| b.len() as u64))
    }
}
