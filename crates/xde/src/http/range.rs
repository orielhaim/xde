use crate::core::{
    error::{Error, Result},
    ranges::ByteRange,
};

/// Parsed `Content-Range: bytes first-last/complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    pub first: u64,
    /// Inclusive, as on the wire.
    pub last: u64,
    pub complete_length: Option<u64>,
}

impl ContentRange {
    pub fn len(&self) -> u64 {
        self.last
            .checked_sub(self.first)
            .and_then(|diff| diff.checked_add(1))
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.first > self.last
    }

    /// Half-open, as used everywhere inside the engine.
    pub fn as_range(&self) -> Result<ByteRange> {
        let end = self
            .last
            .checked_add(1)
            .ok_or_else(|| Error::protocol("Content-Range last=u64::MAX overflow"))?;
        Ok(ByteRange::new(self.first, end))
    }

    /// Every 206 is checked against what we actually asked for, not just against
    /// the status code. A server that answers a different range than requested
    /// and gets believed is how a corrupt 40GB file happens.
    pub fn validate_against(&self, requested: ByteRange) -> Result<()> {
        if self.first != requested.start {
            return Err(Error::protocol(format!(
                "Content-Range starts at {} but we asked for {}",
                self.first, requested.start
            )));
        }
        let end = self
            .last
            .checked_add(1)
            .ok_or_else(|| Error::protocol("Content-Range last overflow"))?;
        if end > requested.end {
            return Err(Error::protocol(format!(
                "Content-Range ends at {} beyond requested {}",
                end, requested.end
            )));
        }
        if self.last < self.first {
            return Err(Error::protocol("Content-Range last < first"));
        }
        if let Some(total) = self.complete_length
            && self.last >= total
        {
            return Err(Error::protocol("Content-Range last >= complete length"));
        }
        Ok(())
    }
}

/// `bytes 0-1023/146515` or `bytes 0-1023/*`. Anything else is rejected.
pub fn parse_content_range(v: &str) -> Result<ContentRange> {
    let v = v.trim();
    let rest = v
        .strip_prefix("bytes ")
        .or_else(|| v.strip_prefix("bytes="))
        .ok_or_else(|| Error::protocol(format!("unsupported Content-Range unit: {v:?}")))?;

    let (span, total) = rest
        .split_once('/')
        .ok_or_else(|| Error::protocol(format!("malformed Content-Range: {v:?}")))?;

    // `bytes */146515` is the unsatisfied form; it carries no span.
    if span.trim() == "*" {
        let complete = total.trim().parse::<u64>().ok();
        return Err(Error::protocol(format!(
            "unsatisfied Content-Range (complete length {complete:?})"
        )));
    }

    let (a, b) = span
        .trim()
        .split_once('-')
        .ok_or_else(|| Error::protocol(format!("malformed Content-Range span: {span:?}")))?;

    let first = a
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::protocol("bad range start"))?;
    let last = b
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::protocol("bad range end"))?;
    if last < first {
        return Err(Error::protocol("Content-Range last < first"));
    }
    if last == u64::MAX {
        return Err(Error::protocol("Content-Range last=u64::MAX is invalid"));
    }
    let complete_length = match total.trim() {
        "*" => None,
        t => {
            let cl = t
                .parse::<u64>()
                .map_err(|_| Error::protocol("bad complete length"))?;
            if cl == 0 {
                return Err(Error::protocol("Content-Range complete length cannot be 0"));
            }
            Some(cl)
        }
    };

    Ok(ContentRange {
        first,
        last,
        complete_length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_normal() {
        let c = parse_content_range("bytes 0-1023/146515").unwrap();
        assert_eq!(
            (c.first, c.last, c.complete_length),
            (0, 1023, Some(146515))
        );
        assert_eq!(c.len(), 1024);
        assert_eq!(c.as_range().unwrap(), ByteRange::new(0, 1024));
    }

    #[test]
    fn parses_unknown_total() {
        let c = parse_content_range("bytes 100-199/*").unwrap();
        assert_eq!(c.complete_length, None);
    }

    #[test]
    fn rejects_unsatisfied_and_junk() {
        assert!(parse_content_range("bytes */146515").is_err());
        assert!(parse_content_range("items 0-1/2").is_err());
        assert!(parse_content_range("bytes 100-50/200").is_err());
    }

    #[test]
    fn rejects_mismatched_range() {
        let c = parse_content_range("bytes 500-999/10000").unwrap();
        assert!(c.validate_against(ByteRange::new(0, 1000)).is_err());
        assert!(c.validate_against(ByteRange::new(500, 1000)).is_ok());
    }

    #[test]
    fn rejects_u64_max_boundary() {
        let max_str = format!("bytes 0-{}/1000", u64::MAX);
        assert!(parse_content_range(&max_str).is_err());
    }
}
