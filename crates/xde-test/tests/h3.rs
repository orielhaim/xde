use std::{sync::atomic::Ordering, time::Duration};

use bytes::{Buf, Bytes};
use xde_test::{FixtureSpec, payload, spawn_h1h3};

#[test]
fn ranges_overlap_on_one_quic_connection() {
    let server = spawn_h1h3(FixtureSpec {
        egress: Some(xde_test::spec::SharedEgress::new(
            (512 * 1024).try_into().unwrap(),
        )),
        size: 128 * 1024,
        latency: Duration::from_millis(30),
        ..Default::default()
    });
    let started = std::time::Instant::now();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let endpoint = compio_quic::Endpoint::client(("127.0.0.1", 0))
            .await
            .unwrap();
        let config = compio_quic::ClientBuilder::new_with_no_server_verification()
            .with_alpn_protocols(&["h3"])
            .build();
        let connection = endpoint
            .connect(
                (server.addr.ip(), server.h3_port).into(),
                "localhost",
                Some(config),
            )
            .unwrap()
            .await
            .unwrap();
        let (mut driver, sender) = h3::client::builder()
            .build::<_, compio_quic::h3::OpenStreams, Bytes>(connection.clone())
            .await
            .unwrap();
        let drive = compio::runtime::spawn(async move {
            let _ = driver.wait_idle().await;
        });
        let requests = (0..2).map(|n| {
            let mut sender = sender.clone();
            async move {
                let start = n * 64 * 1024;
                let end = start + 64 * 1024 - 1;
                let request = http::Request::builder()
                    .uri("https://localhost/artifact.bin")
                    .header("range", format!("bytes={start}-{end}"))
                    .body(())
                    .unwrap();
                let mut stream = sender.send_request(request).await.unwrap();
                stream.finish().await.unwrap();
                let response = stream.recv_response().await.unwrap();
                assert_eq!(response.status(), 206);
                assert_eq!(
                    response.headers()["content-range"],
                    format!("bytes {start}-{end}/131072")
                );
                let mut received = Vec::new();
                while let Some(mut chunk) = stream.recv_data().await.unwrap() {
                    let len = chunk.remaining();
                    received.extend_from_slice(&chunk.copy_to_bytes(len));
                }
                let mut expected = vec![0; 64 * 1024];
                payload::fill(&mut expected, start);
                assert_eq!(received, expected);
            }
        });
        compio::time::timeout(
            Duration::from_secs(10),
            futures_util::future::join_all(requests),
        )
        .await
        .unwrap();
        connection.close(0u32.into(), b"done");
        let _ = drive.cancel().await;
    });
    assert_eq!(server.stats.h3_connections.load(Ordering::Acquire), 1);
    assert_eq!(server.stats.h3_requests.load(Ordering::Acquire), 2);
    assert!(
        server
            .stats
            .h3_peak_streams_per_connection
            .load(Ordering::Acquire)
            >= 2
    );
    assert_eq!(
        server.stats.h3_payload_bytes.load(Ordering::Acquire),
        128 * 1024
    );
    assert!(started.elapsed() >= Duration::from_millis(250));
    server.shutdown();
}
