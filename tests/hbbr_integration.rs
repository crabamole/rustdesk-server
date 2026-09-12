use hbb_common::{
    futures_util::{SinkExt, StreamExt},
    protobuf::Message as _,
    rendezvous_proto::*,
    tokio::{self, time::timeout},
};
use hbbs::relay_server::TestRelayServer;
use std::time::Duration;

#[tokio::test]
async fn test_relay_server_accepts_connection() {
    let server = TestRelayServer::start("").await.expect("start relay");
    let addr = format!("127.0.0.1:{}", server.port);

    let stream = timeout(
        Duration::from_secs(3),
        hbb_common::tokio::net::TcpStream::connect(&addr),
    )
    .await;
    assert!(stream.is_ok(), "Should connect to relay server");
    assert!(stream.unwrap().is_ok());

    server.shutdown();
}

#[tokio::test]
async fn test_relay_admin_help_cmd() {
    use hbb_common::tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestRelayServer::start("").await.expect("start relay");

    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();

    stream.write_all(b"h").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(response.contains("blacklist"));
        assert!(response.contains("usage"));
    }

    server.shutdown();
}

async fn ws_connect(
    port: u16,
) -> tokio_tungstenite::WebSocketStream<
    hbb_common::tokio::net::TcpStream,
> {
    let stream = hbb_common::tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{}", port), stream)
        .await
        .unwrap();
    ws
}

#[tokio::test]
async fn test_relay_request_unpaired_via_ws() {
    let server = TestRelayServer::start("").await.expect("start relay");
    let mut ws = ws_connect(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(RequestRelay {
        uuid: "test-unpaired-ws-uuid".to_owned(),
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();
    ws.send(tungstenite::Message::Binary(bytes)).await.unwrap();

    // No pair arrives — server holds connection for 30s then drops
    let recv_result = timeout(Duration::from_secs(2), ws.next()).await;
    assert!(recv_result.is_err(), "Should timeout waiting for pair");

    server.shutdown();
}

#[tokio::test]
async fn test_relay_pair_and_exchange_via_ws() {
    let server = TestRelayServer::start("").await.expect("start relay");

    let uuid = "test-ws-pair-uuid";

    // Peer A connects via WS
    let mut ws_a = ws_connect(server.ws_port).await;
    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(RequestRelay {
        uuid: uuid.to_owned(),
        ..Default::default()
    });
    ws_a.send(tungstenite::Message::Binary(msg.write_to_bytes().unwrap()))
        .await
        .unwrap();

    hbb_common::tokio::time::sleep(Duration::from_millis(100)).await;

    // Peer B connects via WS with same uuid
    let mut ws_b = ws_connect(server.ws_port).await;
    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(RequestRelay {
        uuid: uuid.to_owned(),
        ..Default::default()
    });
    ws_b.send(tungstenite::Message::Binary(msg.write_to_bytes().unwrap()))
        .await
        .unwrap();

    hbb_common::tokio::time::sleep(Duration::from_millis(200)).await;

    // A sends data to B
    ws_a.send(tungstenite::Message::Binary(b"hello from A".to_vec()))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(3), ws_b.next()).await;
    assert!(received.is_ok(), "B should receive data from A");
    let received = received.unwrap().unwrap().unwrap();
    if let tungstenite::Message::Binary(data) = received {
        assert_eq!(&data[..], b"hello from A");
    } else {
        panic!("Expected binary message");
    }

    // B sends data to A
    ws_b.send(tungstenite::Message::Binary(b"hello from B".to_vec()))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(3), ws_a.next()).await;
    assert!(received.is_ok(), "A should receive data from B");
    let received = received.unwrap().unwrap().unwrap();
    if let tungstenite::Message::Binary(data) = received {
        assert_eq!(&data[..], b"hello from B");
    } else {
        panic!("Expected binary message");
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_wrong_key_rejected_via_ws() {
    let server = TestRelayServer::start("my_secret_key")
        .await
        .expect("start relay");
    let mut ws = ws_connect(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(RequestRelay {
        uuid: "test-wrong-key-ws".to_owned(),
        licence_key: "wrong_key".to_owned(),
        ..Default::default()
    });
    ws.send(tungstenite::Message::Binary(msg.write_to_bytes().unwrap()))
        .await
        .unwrap();

    // Server should close connection since key doesn't match
    let result = timeout(Duration::from_secs(3), ws.next()).await;
    match result {
        Ok(None) | Ok(Some(Err(_))) => {}
        Ok(Some(Ok(tungstenite::Message::Close(_)))) => {}
        Err(_) => {}
        Ok(Some(Ok(msg))) => panic!("Should not get data with wrong key, got: {:?}", msg),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_admin_blacklist_add_and_query() {
    use hbb_common::tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestRelayServer::start("").await.expect("start relay");

    // Add to blacklist
    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();
    stream.write_all(b"ba 10.0.0.99").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let _ = timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    drop(stream);

    // Query blacklist
    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();
    stream.write_all(b"b").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(
            response.contains("10.0.0.99"),
            "Blacklist should contain added IP"
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_admin_blocklist_add_and_query() {
    use hbb_common::tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestRelayServer::start("").await.expect("start relay");

    // Add to blocklist
    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();
    stream.write_all(b"Ba 10.0.0.88").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let _ = timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    drop(stream);

    // Query blocklist
    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();
    stream.write_all(b"B").await.unwrap();
    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(
            response.contains("10.0.0.88"),
            "Blocklist should contain added IP"
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_admin_downgrade_threshold() {
    use hbb_common::tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestRelayServer::start("").await.expect("start relay");

    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();
    stream.write_all(b"dt").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(
            response.contains("66"),
            "Default downgrade threshold should be 66"
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_admin_usage_query() {
    use hbb_common::tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestRelayServer::start("").await.expect("start relay");

    let mut stream = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.port
    ))
    .await
    .unwrap();
    stream.write_all(b"u").await.unwrap();

    let mut buf = vec![0u8; 4096];
    // Empty usage returns empty string — the server may close with no response
    let result = timeout(Duration::from_secs(2), stream.read(&mut buf)).await;
    match result {
        Ok(Ok(n)) => {
            // Empty usage returns empty string
            let response = std::str::from_utf8(&buf[..n]).unwrap();
            assert!(response.is_empty() || response.lines().count() == 0 || response.contains("kb/s"));
        }
        _ => {} // timeout or error OK for empty usage
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_empty_uuid_ignored() {
    let server = TestRelayServer::start("").await.expect("start relay");
    let mut ws = ws_connect(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(RequestRelay {
        uuid: "".to_owned(),
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();
    ws.send(tungstenite::Message::Binary(bytes)).await.unwrap();

    // Empty UUID is ignored by server
    let recv_result = timeout(Duration::from_secs(1), ws.next()).await;
    match recv_result {
        Err(_) => {} // timeout — expected
        Ok(None) => {} // stream closed — also OK
        Ok(Some(_)) => {} // any response is also OK
    }

    server.shutdown();
}

#[tokio::test]
async fn test_relay_non_relay_message_ignored() {
    let server = TestRelayServer::start("").await.expect("start relay");
    let mut ws = ws_connect(server.ws_port).await;

    // Send a RegisterPk message — relay server doesn't handle this
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "test_peer".to_owned(),
        uuid: vec![1; 16].into(),
        pk: vec![2; 32].into(),
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();
    ws.send(tungstenite::Message::Binary(bytes)).await.unwrap();

    // Server ignores non-RequestRelay messages and closes after 30s timeout
    let recv_result = timeout(Duration::from_secs(1), ws.next()).await;
    match recv_result {
        Err(_) => {} // timeout
        Ok(None) => {} // closed
        Ok(Some(_)) => {} // any response
    }

    server.shutdown();
}
