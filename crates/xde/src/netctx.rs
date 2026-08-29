//! Network context detection.
//!
//! Uses getifs (maintained cross-platform interface enumeration) to build a
//! coarse, stable [`NetworkContextKey`]: which interface the default route
//! prefers, its family/MTU, and the gateway. Evidence and paths are scoped
//! by this so Wi-Fi learning never masquerades as Ethernet truth.

use crate::core::world::{AddressFamily, NetworkContextKey};

/// Detect the current network environment. Falls back to an empty key when
/// the platform cannot answer; transfers still work, evidence is just less
/// precisely scoped.
pub fn detect() -> NetworkContextKey {
    let mut key = NetworkContextKey::default();

    // Preferred local addresses tell us the default egress interface.
    if let Ok(v4) = getifs::local_ipv4_addrs()
        && let Some(first) = v4.first()
    {
        key.family = Some(AddressFamily::V4);
        key.iface_index = Some(first.index());
        if let Ok(name) = first.name() {
            key.iface_name = Some(name.to_string());
        }
        if let Ok(interfaces) = getifs::interfaces()
            && let Some(iface) = interfaces.iter().find(|i| i.index() == first.index())
        {
            key.mtu = Some(iface.mtu());
        }
    } else if let Ok(v6) = getifs::local_ipv6_addrs()
        && let Some(first) = v6.first()
    {
        key.family = Some(AddressFamily::V6);
        key.iface_index = Some(first.index());
    }

    if let Ok(gw) = getifs::gateway_ipv4_addrs()
        && let Some(g) = gw.first()
    {
        key.gateway = Some(std::net::IpAddr::V4(g.addr()));
    } else if let Ok(gw) = getifs::gateway_ipv6_addrs()
        && let Some(g) = gw.first()
    {
        key.gateway = Some(std::net::IpAddr::V6(g.addr()));
    }

    key
}
