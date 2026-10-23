//! Network context detection.
//!
//! Uses getifs (maintained cross-platform interface enumeration) to build a
//! coarse, stable [`NetworkContextKey`]: which interface the default route
//! prefers, its family/MTU, and the gateway. Evidence and paths are scoped
//! by this so Wi-Fi learning never masquerades as Ethernet truth.

use std::net::IpAddr;

use getifs::{IfAddr, IfNet, best_local_addrs, gateway_addrs, get_interface_mtu, local_addrs};

use crate::core::world::{AddressFamily, NetworkContextKey};

/// Detect the current network environment. Falls back to an empty key when
/// the platform cannot answer; transfers still work, evidence is just less
/// precisely scoped.
pub fn detect() -> NetworkContextKey {
    let mut key = NetworkContextKey::default();

    let addrs = match best_local_addrs() {
        Ok(addrs) if !addrs.is_empty() => addrs,
        // No default route (offline, isolated link): any configured address
        // still scopes evidence better than an empty key.
        _ => local_addrs().unwrap_or_default(),
    };

    if let Some(net) = preferred_egress(&addrs) {
        key.family = Some(family_of(net.addr()));
        key.iface_index = Some(net.index());
        key.iface_name = net.name().ok().map(|name| name.to_string());
        key.mtu = get_interface_mtu(net.index()).ok();
    }

    if let Ok(gateways) = gateway_addrs() {
        key.gateway = select_gateway(&gateways, key.iface_index, key.family);
    }

    key
}

/// Pick the egress address. IPv4 is preferred when the host has both
/// families, matching the previous selection order.
fn preferred_egress(addrs: &[IfNet]) -> Option<&IfNet> {
    addrs
        .iter()
        .find(|net| matches!(net.addr(), IpAddr::V4(_)))
        .or_else(|| addrs.first())
}

/// Pick the gateway most likely used by the egress interface. Gateways come
/// in unspecified order, so prefer one reached through that interface, then
/// one of the egress address family, then whatever is available.
fn select_gateway(
    gateways: &[IfAddr],
    iface_index: Option<u32>,
    family: Option<AddressFamily>,
) -> Option<IpAddr> {
    gateways
        .iter()
        .min_by_key(|gateway| gateway_rank(gateway, iface_index, family))
        .map(IfAddr::addr)
}

fn gateway_rank(gateway: &IfAddr, iface_index: Option<u32>, family: Option<AddressFamily>) -> u8 {
    if iface_index == Some(gateway.index()) {
        0
    } else if family == Some(family_of(gateway.addr())) {
        1
    } else {
        2
    }
}

const fn family_of(addr: IpAddr) -> AddressFamily {
    match addr {
        IpAddr::V4(_) => AddressFamily::V4,
        IpAddr::V6(_) => AddressFamily::V6,
    }
}
