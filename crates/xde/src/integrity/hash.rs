use crate::core::{
    error::{Error, Result},
    spec::{ExpectedDigest, HashKind},
};

/// BLAKE3 internally (SIMD + multithreading in the official implementation),
/// SHA-256 exdernally because that is how the world publishes checksums -
/// not because it is better for us.
#[derive(Debug)]
pub enum Hasher {
    Blake3(Box<blake3::Hasher>),
    Sha256(sha2::Sha256),
}

impl Hasher {
    pub fn new(kind: HashKind) -> Self {
        match kind {
            HashKind::Blake3 => Hasher::Blake3(Box::new(blake3::Hasher::new())),
            HashKind::Sha256 => {
                use sha2::Digest;
                Hasher::Sha256(sha2::Sha256::new())
            }
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Blake3(h) => {
                h.update(data);
            }
            Hasher::Sha256(h) => {
                use sha2::Digest;
                h.update(data);
            }
        }
    }

    /// BLAKE3 only, and only worth it above a few hundred KB.
    pub fn update_rayon(&mut self, data: &[u8]) {
        match self {
            #[cfg(feature = "blake3-rayon")]
            Hasher::Blake3(h) => {
                h.update_rayon(data);
            }
            _ => self.update(data),
        }
    }

    pub fn finalize(self) -> [u8; 32] {
        match self {
            Hasher::Blake3(h) => *h.finalize().as_bytes(),
            Hasher::Sha256(h) => {
                use sha2::Digest;
                h.finalize().into()
            }
        }
    }

    pub fn kind(&self) -> HashKind {
        match self {
            Hasher::Blake3(_) => HashKind::Blake3,
            Hasher::Sha256(_) => HashKind::Sha256,
        }
    }
}

/// Sequential digest over the finished artifact. Ranges arrive out of order, so
/// the whole-file hash is a separate verification pass over the `.part` after
/// all ranges land - not something we can do incrementally in arrival order.
#[derive(Debug)]
pub struct StreamingDigest {
    hasher: Hasher,
    consumed: u64,
}

impl StreamingDigest {
    pub fn new(kind: HashKind) -> Self {
        Self {
            hasher: Hasher::new(kind),
            consumed: 0,
        }
    }
    pub fn feed(&mut self, data: &[u8]) {
        self.hasher.update(data);
        self.consumed += data.len() as u64;
    }
    pub fn consumed(&self) -> u64 {
        self.consumed
    }
    pub fn finish(self) -> [u8; 32] {
        self.hasher.finalize()
    }

    pub fn verify(self, expected: &ExpectedDigest) -> Result<[u8; 32]> {
        let got = self.hasher.finalize();
        if &got == expected.bytes() {
            Ok(got)
        } else {
            Err(Error::Integrity(format!(
                "digest mismatch: expected {}, got {}",
                hex(expected.bytes()),
                hex(&got)
            )))
        }
    }
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(char::from_digit((x >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((x & 0xf) as u32, 16).unwrap());
    }
    s
}
