// Own test binary: the keep-alive interval is read once per process.
use hbb_common::{
    futures_util::{SinkExt, StreamExt},
    protobuf::Message as _,
    rendezvous_proto::*,
    tcp::FramedStream,
    tokio::{self, time::{sleep, timeout}},
};
use std::time::Duration;

const INTERVAL_MS: &str = "500";

fn register_pk(id: &str) -> RendezvousMessage {
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: id.to_owned(),
        uuid: vec![3; 16].into(),
        pk: vec![4; 32].into(),
        ..Default::default()
    });
    msg
}

#[tokio::test]
async fn test_ws_registered_peer_gets_keep_alive() {
    std::env::set_var("WS-HEARTBEAT-INTERVAL", INTERVAL_MS);
    let server = hbbs::RendezvousServer::start_test("").await.expect("start test server");
    let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", server.ws_port))
        .await
        .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{}", server.ws_port), tcp)
        .await
        .unwrap();
    let (mut write, mut read) = ws.split();
    write
        .send(tungstenite::Message::Binary(register_pk("ws_keepalive").write_to_bytes().unwrap()))
        .await
        .unwrap();

    let first = timeout(Duration::from_secs(3), read.next()).await.unwrap().unwrap().unwrap();
    let resp = RendezvousMessage::parse_from_bytes(&first.into_data()).unwrap();
    assert!(resp.has_register_pk_response());

    // The client drops the connection after 90 s without a frame; the server must send one.
    let frame = timeout(Duration::from_secs(3), read.next()).await;
    match frame {
        Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => assert!(bytes.is_empty()),
        other => panic!("expected an empty keep-alive frame, got {other:?}"),
    }
    server.shutdown();
}

#[tokio::test]
async fn test_tcp_registered_peer_gets_keep_alive() {
    std::env::set_var("WS-HEARTBEAT-INTERVAL", INTERVAL_MS);
    let server = hbbs::RendezvousServer::start_test("").await.expect("start test server");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", server.port).parse().unwrap();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut stream = FramedStream::from(tcp, addr);
    stream.send(&register_pk("tcp_keepalive")).await.unwrap();

    let first = stream.next_timeout(3_000).await.unwrap().unwrap();
    let resp = RendezvousMessage::parse_from_bytes(&first).unwrap();
    assert!(resp.has_register_pk_response());

    let frame = stream.next_timeout(3_000).await;
    match frame {
        Some(Ok(bytes)) => assert!(bytes.is_empty()),
        other => panic!("expected an empty keep-alive frame, got {other:?}"),
    }
    server.shutdown();
}

async fn start_with_keep_alive_1s() -> hbbs::TestServer {
    std::env::set_var("WS-HEARTBEAT-INTERVAL", INTERVAL_MS);
    hbbs::RendezvousServer::start_test_with(hbbs::TestOptions { keep_alive_secs: 1, ..Default::default() })
        .await
        .expect("start test server")
}

async fn presence_rows(db_url: &str, id: &str) -> i64 {
    let pool = sqlx::PgPool::connect(db_url).await.unwrap();
    sqlx::query_scalar("SELECT count(*) FROM peer_presence WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
}

async fn tcp_device(port: u16, id: &str) -> FramedStream {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = FramedStream::from(tokio::net::TcpStream::connect(addr).await.unwrap(), addr);
    stream.send(&register_pk(id)).await.unwrap();
    let first = stream.next_timeout(3_000).await.unwrap().unwrap();
    assert!(RendezvousMessage::parse_from_bytes(&first).unwrap().has_register_pk_response());
    stream
}

// A device that stopped answering may have moved to another pod; keeping it would misroute viewers.
#[tokio::test]
async fn test_silent_tcp_device_is_dropped_after_one_and_a_half_keep_alives() {
    let server = start_with_keep_alive_1s().await;
    let mut stream = tcp_device(server.port, "tcp_silent").await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(presence_rows(&server.db_url, "tcp_silent").await, 1);
    let started = std::time::Instant::now();
    let closed = timeout(Duration::from_secs(4), async {
        while let Some(Ok(bytes)) = stream.next().await {
            assert!(bytes.is_empty(), "only keep-alives expected");
        }
    })
    .await;
    assert!(closed.is_ok(), "server kept a silent device connected");
    assert!(started.elapsed() >= Duration::from_millis(1_000));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(presence_rows(&server.db_url, "tcp_silent").await, 0);
    server.shutdown();
}

#[tokio::test]
async fn test_silent_ws_device_is_dropped_after_one_and_a_half_keep_alives() {
    let server = start_with_keep_alive_1s().await;
    let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", server.ws_port)).await.unwrap();
    let (ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{}", server.ws_port), tcp)
        .await
        .unwrap();
    let (mut write, mut read) = ws.split();
    write
        .send(tungstenite::Message::Binary(register_pk("ws_silent").write_to_bytes().unwrap()))
        .await
        .unwrap();
    let first = timeout(Duration::from_secs(3), read.next()).await.unwrap().unwrap().unwrap();
    assert!(RendezvousMessage::parse_from_bytes(&first.into_data()).unwrap().has_register_pk_response());
    let closed = timeout(Duration::from_secs(4), async {
        while let Some(Ok(tungstenite::Message::Binary(bytes))) = read.next().await {
            assert!(bytes.is_empty(), "only keep-alives expected");
        }
    })
    .await;
    assert!(closed.is_ok(), "server kept a silent device connected");
    sleep(Duration::from_millis(300)).await;
    assert_eq!(presence_rows(&server.db_url, "ws_silent").await, 0);
    server.shutdown();
}

#[tokio::test]
async fn test_device_echoing_keep_alives_stays_connected() {
    let server = start_with_keep_alive_1s().await;
    let mut stream = tcp_device(server.port, "tcp_echo").await;
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        let frame = stream.next_timeout(2_000).await;
        match frame {
            Some(Ok(bytes)) if bytes.is_empty() => stream.send_bytes(bytes.freeze()).await.unwrap(),
            other => panic!("expected a keep-alive, got {other:?}"),
        }
    }
    assert_eq!(presence_rows(&server.db_url, "tcp_echo").await, 1);
    server.shutdown();
}
