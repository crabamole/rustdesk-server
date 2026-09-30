// Own test binary: the keep-alive interval is read once per process.
use hbb_common::{
    futures_util::{SinkExt, StreamExt},
    protobuf::Message as _,
    rendezvous_proto::*,
    tcp::FramedStream,
    tokio::{self, time::timeout},
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
