use std::{
    io::{Read, Write},
    time::{Duration, Instant},
};
use xde_test::{FixtureSpec, spawn_h1, spec::SharedEgress};

#[test]
fn independent_origins_share_one_application_egress_budget() {
    let egress = SharedEgress::new((2 * 1024 * 1024).try_into().unwrap());
    let servers: Vec<_> = (0..2)
        .map(|_| {
            spawn_h1(FixtureSpec {
                size: 256 * 1024,
                egress: Some(egress.clone()),
                ..Default::default()
            })
        })
        .collect();
    let start = Instant::now();
    std::thread::scope(|scope| {
        let readers:Vec<_>=servers.iter().map(|server|scope.spawn(move || {
            let mut socket=std::net::TcpStream::connect(server.addr).unwrap();
            socket.write_all(b"GET /artifact.bin HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
            let mut response=Vec::new();
            socket.read_to_end(&mut response).unwrap();
            let body=response.windows(4).position(|b|b==b"\r\n\r\n").unwrap()+4;
            let mut expected=vec![0;256*1024];
            xde_test::payload::fill(&mut expected,0);
            assert_eq!(&response[body..],expected);
        })).collect();
        for reader in readers {
            reader.join().unwrap();
        }
    });
    assert!(start.elapsed() >= Duration::from_millis(250));
    for server in servers {
        server.shutdown();
    }
}
