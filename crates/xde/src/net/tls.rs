use std::sync::Arc;

use crate::core::error::Result;
use compio::tls::TlsConnector;
use rustls::ClientConfig;
use rustls_platform_verifier::ConfigVerifierExt;

/// rustls + aws-lc-rs, with the OS doing certificate verification.
/// aws-lc-rs also gives us the hybrid post-quantum key exchange for free.
#[derive(Debug, Clone)]
pub struct TlsSetup {
    config: Arc<ClientConfig>,
}

impl TlsSetup {
    pub fn new(alpn: &[&[u8]]) -> Result<Self> {
        // Install the process-wide provider once; a second call is not an error.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut config = ClientConfig::with_platform_verifier()
            .map_err(|error| crate::core::Error::protocol(format!("platform verifier: {error}")))?;

        config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        // Session resumption matters: many small files against one CDN pay the
        // full handshake otherwise.
        config.resumption = rustls::client::Resumption::in_memory_sessions(256);
        config.enable_early_data = false;

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// Default ALPN order. H2 first, H1 as the fallback - the inverse of a
    /// stack built around H1 that bolted on the rest later. H3 slots in above
    /// both once the QUIC backend lands, without touching this ordering logic.
    pub fn default_client() -> Result<Self> {
        Self::new(&[b"h2", b"http/1.1"])
    }

    pub fn http1_only() -> Result<Self> {
        Self::new(&[b"http/1.1"])
    }

    pub fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.config.clone())
    }

    pub fn config(&self) -> &Arc<ClientConfig> {
        &self.config
    }
}
