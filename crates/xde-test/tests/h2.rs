use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{sync::atomic::Ordering, time::Duration};
use xde_test::{FixtureSpec, payload, spawn_h2c};

#[test]
fn ranges_overlap_on_one_h2_connection() {
    let server = spawn_h2c(FixtureSpec {
        egress: Some(xde_test::spec::SharedEgress::new(
            (512 * 1024).try_into().unwrap(),
        )),
        size: 128 * 1024,
        latency: Duration::from_millis(30),
        ..Default::default()
    });
    let started = std::time::Instant::now();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let socket = tokio::net::TcpStream::connect(server.addr).await.unwrap();
            let (sender, connection) = hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
                TokioExecutor::new(),
                TokioIo::new(socket),
            )
            .await
            .unwrap();
            let driver = tokio::spawn(connection);
            let requests = (0..2).map(|n| {
                let mut sender = sender.clone();
                let url = server.url();
                async move {
                    let start = n * 64 * 1024;
                    let end = start + 64 * 1024 - 1;
                    let response = sender
                        .send_request(
                            http::Request::builder()
                                .uri(url)
                                .header("range", format!("bytes={start}-{end}"))
                                .body(Empty::<Bytes>::new())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.status(), 206);
                    assert_eq!(
                        response.headers()["content-range"],
                        format!("bytes {start}-{end}/131072")
                    );
                    let received = response.into_body().collect().await.unwrap().to_bytes();
                    let mut expected = vec![0; 64 * 1024];
                    payload::fill(&mut expected, start);
                    assert_eq!(received.as_ref(), expected);
                }
            });
            tokio::time::timeout(
                Duration::from_secs(10),
                futures_util::future::join_all(requests),
            )
            .await
            .unwrap();
            driver.abort();
            let _ = driver.await;
        });
    assert_eq!(server.stats.accepts(), 1);
    assert_eq!(server.stats.requests(), 2);
    assert!(
        server
            .stats
            .h2_peak_streams_per_connection
            .load(Ordering::Acquire)
            >= 2
    );
    assert_eq!(server.stats.bytes_sent(), 128 * 1024);
    assert!(started.elapsed() >= Duration::from_millis(250));
    server.shutdown();
}
