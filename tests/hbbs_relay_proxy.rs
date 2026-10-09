// Own test binary: proxy variables are process-wide.
use hbb_common::tokio::{self, io::{AsyncReadExt, AsyncWriteExt}, time::timeout};
use std::time::Duration;

async fn admin(port: u16, cmd: &str) -> String {
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(cmd.as_bytes()).await.unwrap();
    let mut out = String::new();
    timeout(Duration::from_secs(3), s.read_to_string(&mut out)).await.unwrap().unwrap();
    out
}

async fn http_stub(status: u16) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf).await;
            let resp = format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            let _ = s.write_all(resp.as_bytes()).await;
        }
    });
    format!("http://{addr}")
}

// Checks address in-cluster services directly; an egress proxy from the environment cannot reach them.
#[tokio::test]
async fn test_relay_checks_ignore_proxy_env() {
    for var in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"] {
        std::env::set_var(var, "http://127.0.0.1:1");
    }
    let up = http_stub(200).await;
    let down = http_stub(503).await;
    let server = hbbs::RendezvousServer::start_test_with(hbbs::TestOptions {
        relay_urls: vec!["wss://h/ws/relay/0".to_owned(), "wss://h/ws/relay/1".to_owned()],
        relay_checks: vec![format!("{up}/readyz"), format!("{down}/readyz")],
        ..Default::default()
    })
    .await
    .expect("start test server");
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for _ in 0..20 {
        assert_eq!(admin(server.nat_port, "tg 1.1.1.1 2.2.2.2").await, "\"wss://h/ws/relay/0\"");
    }
    server.shutdown();
}
