//! Resolver service.
//!
//! Discovery lives here; endpoint *intelligence* stays in the controller's
//! WorldModel. DNS runs on its own thread backed by an isolated
//! single-threaded Tokio runtime that exists only for hickory-resolver
//! (Option B): the compio data plane never touches it. Results arrive as
//! `Observation::Resolved`, keeping the controller pure.
//!
//! Hickory handles TTL caching, negative caching, concurrent request
//! coalescing and dual-stack racing internally - no manual cache is layered
//! on top (it would only discard HTTPS/SVCB metadata on hit). The resolver
//! thread dispatches lookups concurrently via `tokio::spawn` instead of
//! `recv → block_on lookup_ip → block_on HTTPS → next`.

use std::{
    net::{IpAddr, SocketAddr},
    thread,
    time::Duration,
};

use crate::core::{
    controller::{HttpsRecordInfo, Observation},
    ids::OriginId,
};
use flume::Sender;
use hickory_resolver::{TokioResolver, proto::rr::RecordType, system_conf::read_system_conf};

#[derive(Clone, Debug)]
pub struct ResolverHandle {
    tx: flume::Sender<Request>,
}

struct Request {
    origin: OriginId,
    host: String,
    port: u16,
    reply: Sender<Observation>,
}

pub(crate) fn spawn() -> ResolverHandle {
    let (tx, rx) = flume::bounded::<Request>(256);
    thread::Builder::new()
        .name("xde-resolver".into())
        .spawn(move || run(rx))
        .expect("resolver thread spawn");
    ResolverHandle { tx }
}

impl ResolverHandle {
    pub fn resolve(&self, origin: OriginId, host: String, port: u16, reply: Sender<Observation>) {
        let _ = self.tx.try_send(Request {
            origin,
            host,
            port,
            reply,
        });
    }
}

impl ResolverHandle {
    pub(crate) fn spawn() -> Self {
        spawn()
    }
}

fn run(rx: flume::Receiver<Request>) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return,
    };

    // A current-thread runtime only drives tasks while someone is blocked
    // inside `rt.block_on`. The previous code called `rx.recv()` from the
    // runtime-driving thread, which left every `rt.spawn(...)` future
    // permanently pending and deadlocked the engine on first DNS lookup.
    // Drive the runtime AND the request channel from a single `block_on` so
    // spawned tasks actually run between channel receives.
    let local = tokio::task::LocalSet::new();
    let resolver = rt.block_on(async {
        let mut builder = match read_system_conf() {
            Ok((config, opts)) => {
                let mut b = TokioResolver::builder_with_config(
                    config,
                    hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
                );
                *b.options_mut() = opts;
                b
            }
            Err(_) => TokioResolver::builder_with_config(
                default_config(),
                hickory_resolver::net::runtime::TokioRuntimeProvider::default(),
            ),
        };
        builder.options_mut().positive_min_ttl = Some(Duration::from_secs(30));
        builder.options_mut().negative_max_ttl = Some(Duration::from_secs(60));
        builder.build()
    });
    let Ok(resolver) = resolver else { return };

    let send_resolved = |origin: OriginId,
                         endpoints: Vec<std::net::SocketAddr>,
                         https_records: Vec<HttpsRecordInfo>,
                         reply: Sender<Observation>| {
        let failed = endpoints.is_empty();
        let _ = reply.send(Observation::Resolved {
            origin,
            endpoints,
            failed,
            https_records,
            from_cache: false,
        });
    };

    // The whole loop is one future the runtime drives. spawn_local works
    // because we are calling it from within the run_until future itself,
    // so the LocalSet is not moved.
    rt.block_on(local.run_until(async move {
        let resolver = resolver;
        loop {
            let req = match rx.recv_async().await {
                Ok(r) => r,
                Err(_) => break,
            };
            let resolver = resolver.clone();
            tokio::task::spawn_local(async move {
                let answer = resolve_host(&resolver, &req.host, req.port).await;
                send_resolved(req.origin, answer.addrs, answer.https_records, req.reply);
            });
        }
    }));
}

struct Answer {
    addrs: Vec<SocketAddr>,
    https_records: Vec<HttpsRecordInfo>,
}

async fn resolve_host(resolver: &TokioResolver, host: &str, port: u16) -> Answer {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Answer {
            addrs: vec![SocketAddr::new(ip, port)],
            https_records: Vec::new(),
        };
    }

    let ip_fut = resolver.lookup_ip(host.to_string());
    let https_fut = resolver.lookup(host.to_string(), RecordType::HTTPS);
    let (lookup, https) = tokio::join!(ip_fut, https_fut);

    let ips: Vec<IpAddr> = match &lookup {
        Ok(l) => l.iter().collect(),
        Err(_) => Vec::new(),
    };
    if ips.is_empty() {
        return Answer {
            addrs: Vec::new(),
            https_records: Vec::new(),
        };
    }

    let addrs: Vec<SocketAddr> = ips
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect();

    let records = match https {
        Ok(l) => parse_https_records(&l),
        Err(_) => Vec::new(),
    };

    Answer {
        addrs,
        https_records: records,
    }
}

fn parse_https_records(lookup: &hickory_resolver::lookup::Lookup) -> Vec<HttpsRecordInfo> {
    use hickory_resolver::proto::rr::{
        RData,
        rdata::svcb::{SvcParamKey, SvcParamValue},
    };
    let mut out = Vec::new();
    let ttl = Duration::from_secs(
        lookup
            .answers()
            .first()
            .map(|r| u64::from(r.ttl))
            .unwrap_or(60),
    );

    for record in lookup.answers() {
        let RData::HTTPS(ref https) = record.data else {
            continue;
        };
        let svc = &https.0;
        let mut alpn = Vec::new();
        let mut port = None;
        let mut ipv4_hint = Vec::new();
        let mut ipv6_hint = Vec::new();
        let mut ech = false;
        for (key, value) in &svc.svc_params {
            match (key, value) {
                (SvcParamKey::Alpn, SvcParamValue::Alpn(alpns)) => {
                    alpn.extend(alpns.0.iter().cloned());
                }
                (SvcParamKey::Port, SvcParamValue::Port(p)) => port = Some(*p),
                (SvcParamKey::Ipv4Hint, SvcParamValue::Ipv4Hint(hint)) => {
                    ipv4_hint.extend(hint.0.iter().map(|i| IpAddr::V4(i.0)));
                }
                (SvcParamKey::Ipv6Hint, SvcParamValue::Ipv6Hint(hint)) => {
                    ipv6_hint.extend(hint.0.iter().map(|i| IpAddr::V6(i.0)));
                }
                (SvcParamKey::EchConfigList, _) => ech = true,
                _ => {}
            }
        }
        out.push(HttpsRecordInfo {
            priority: svc.svc_priority,
            target: svc.target_name.to_string(),
            alpn,
            port,
            ipv4_hint,
            ipv6_hint,
            ech,
            ttl,
        });
        if out.len() >= 8 {
            break;
        }
    }
    out
}

fn default_config() -> hickory_resolver::config::ResolverConfig {
    use hickory_resolver::config::*;
    let mut cfg = ResolverConfig::default();
    cfg.name_servers = vec![
        NameServerConfig::udp_and_tcp("1.1.1.1".parse().unwrap()),
        NameServerConfig::udp_and_tcp("8.8.8.8".parse().unwrap()),
    ];
    cfg
}
