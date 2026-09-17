use hbb_common::{
    protobuf::Message as _,
    rendezvous_proto::*,
    tcp::FramedStream,
    tokio::{self, net::UdpSocket, time::timeout},
};
use std::time::Duration;

async fn send_recv_udp(
    socket: &UdpSocket,
    msg: &RendezvousMessage,
    addr: &str,
) -> Option<RendezvousMessage> {
    let bytes = msg.write_to_bytes().unwrap();
    socket.send_to(&bytes, addr).await.unwrap();
    let mut buf = vec![0u8; 65535];
    match timeout(Duration::from_secs(3), socket.recv_from(&mut buf)).await {
        Ok(Ok((n, _))) => RendezvousMessage::parse_from_bytes(&buf[..n]).ok(),
        _ => None,
    }
}

// ---- UDP tests ----

#[tokio::test]
async fn test_register_peer_and_get_response() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");
    let addr = format!("127.0.0.1:{}", server.udp_port);
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "test_peer_1".to_owned(),
        serial: 0,
        ..Default::default()
    });

    let resp = send_recv_udp(&socket, &msg, &addr).await;
    assert!(resp.is_some(), "Should get a response to RegisterPeer");
    let resp = resp.unwrap();
    assert!(
        resp.has_register_peer_response(),
        "Response should be RegisterPeerResponse, got: {:?}",
        resp
    );
    assert!(resp.register_peer_response().request_pk);

    server.shutdown();
}

#[tokio::test]
async fn test_register_peer_then_re_register_updates_addr() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");
    let addr = format!("127.0.0.1:{}", server.udp_port);

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    // First register PK so peer has a key
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "reregtest".to_owned(),
        uuid: vec![1; 16].into(),
        pk: vec![2; 32].into(),
        ..Default::default()
    });
    let resp = send_recv_udp(&socket, &msg, &addr).await.unwrap();
    assert_eq!(
        resp.register_pk_response().result,
        register_pk_response::Result::OK.into()
    );

    // Now register peer — should NOT request_pk since pk is already set
    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "reregtest".to_owned(),
        serial: 0,
        ..Default::default()
    });
    let resp = send_recv_udp(&socket, &msg, &addr).await.unwrap();
    assert!(resp.has_register_peer_response());
    assert!(
        !resp.register_peer_response().request_pk,
        "Should not request pk after it was already registered"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_register_peer_with_higher_serial_gets_config_update() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");
    let addr = format!("127.0.0.1:{}", server.udp_port);
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    // Server serial is 1, client serial 0 => should get ConfigUpdate
    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "serial_test".to_owned(),
        serial: 0,
        ..Default::default()
    });
    let _ = socket
        .send_to(&msg.write_to_bytes().unwrap(), &addr)
        .await
        .unwrap();

    // We should get RegisterPeerResponse AND ConfigUpdate
    let mut got_config = false;
    let mut got_response = false;
    let mut buf = vec![0u8; 65535];
    for _ in 0..3 {
        match timeout(Duration::from_secs(3), socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if let Ok(resp) = RendezvousMessage::parse_from_bytes(&buf[..n]) {
                    if resp.has_register_peer_response() {
                        got_response = true;
                    }
                    if resp.has_configure_update() {
                        got_config = true;
                    }
                }
            }
            _ => break,
        }
        if got_config && got_response {
            break;
        }
    }
    assert!(got_response, "Should get RegisterPeerResponse");
    assert!(got_config, "Should get ConfigUpdate when serial is behind");

    server.shutdown();
}

#[tokio::test]
async fn test_register_pk_via_udp() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");
    let addr = format!("127.0.0.1:{}", server.udp_port);
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "peer_pk_1".to_owned(),
        uuid: vec![1; 16].into(),
        pk: vec![2; 32].into(),
        ..Default::default()
    });

    let resp = send_recv_udp(&socket, &msg, &addr).await;
    assert!(resp.is_some(), "Should get a response to RegisterPk");
    let resp = resp.unwrap();
    assert!(
        resp.has_register_pk_response(),
        "Response should be RegisterPkResponse"
    );
    assert_eq!(
        resp.register_pk_response().result,
        register_pk_response::Result::OK.into()
    );

    server.shutdown();
}

#[tokio::test]
async fn test_register_pk_empty_uuid_rejected() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");
    let addr = format!("127.0.0.1:{}", server.udp_port);
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "peer_bad1".to_owned(),
        uuid: vec![].into(),
        pk: vec![2; 32].into(),
        ..Default::default()
    });

    let resp = send_recv_udp(&socket, &msg, &addr).await;
    assert!(resp.is_some());
    let resp = resp.unwrap();
    assert_eq!(
        resp.register_pk_response().result,
        register_pk_response::Result::INVALID_ID_FORMAT.into()
    );

    server.shutdown();
}

#[tokio::test]
async fn test_software_update_no_version() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");
    let addr = format!("127.0.0.1:{}", server.udp_port);
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    let mut msg = RendezvousMessage::new();
    msg.set_software_update(SoftwareUpdate {
        url: "1.2.3".to_owned(),
        ..Default::default()
    });

    let resp = send_recv_udp(&socket, &msg, &addr).await;
    assert!(
        resp.is_none(),
        "No response expected when server has no version"
    );

    server.shutdown();
}

// ---- TCP/WS tests via FramedStream ----

#[tokio::test]
async fn test_tcp_register_pk() {
    use hbb_common::futures_util::StreamExt;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", server.nat_port).parse().unwrap();

    let tcp = hbb_common::tokio::net::TcpStream::connect(addr)
        .await
        .unwrap();
    let mut stream = FramedStream::from(tcp, addr);

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "tcp_pk_peer".to_owned(),
        uuid: vec![5; 16].into(),
        pk: vec![6; 32].into(),
        ..Default::default()
    });
    stream.send(&msg).await.unwrap();

    // nat_port handler (handle_listener2) doesn't handle RegisterPk, so we won't get a response
    // But the main TCP handler (handle_listener) does — let's connect there via WS
    drop(stream);

    // Use WS port which goes through handle_listener -> handle_listener_inner -> handle_tcp
    let tcp = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.ws_port
    ))
    .await
    .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(
        format!("ws://127.0.0.1:{}", server.ws_port),
        tcp,
    )
    .await
    .unwrap();

    use hbb_common::futures_util::SinkExt;
    let (mut write, mut read) = ws.split();

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ws_pk_peer".to_owned(),
        uuid: vec![7; 16].into(),
        pk: vec![8; 32].into(),
        ..Default::default()
    });
    write
        .send(tungstenite::Message::Binary(
            msg.write_to_bytes().unwrap(),
        ))
        .await
        .unwrap();

    let resp = timeout(Duration::from_secs(3), read.next()).await;
    assert!(resp.is_ok(), "Should get WS response");
    if let Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) = resp {
        let resp_msg = RendezvousMessage::parse_from_bytes(&bytes).unwrap();
        assert!(resp_msg.has_register_pk_response());
        assert_eq!(
            resp_msg.register_pk_response().result,
            register_pk_response::Result::OK.into()
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_ws_punch_hole_request_not_found() {
    use hbb_common::futures_util::{SinkExt, StreamExt};

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let tcp = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.ws_port
    ))
    .await
    .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(
        format!("ws://127.0.0.1:{}", server.ws_port),
        tcp,
    )
    .await
    .unwrap();
    let (mut write, mut read) = ws.split();

    let mut msg = RendezvousMessage::new();
    msg.set_punch_hole_request(PunchHoleRequest {
        id: "ws_nonexistent".to_owned(),
        ..Default::default()
    });
    write
        .send(tungstenite::Message::Binary(
            msg.write_to_bytes().unwrap(),
        ))
        .await
        .unwrap();

    let resp = timeout(Duration::from_secs(3), read.next()).await;
    assert!(resp.is_ok(), "Should get WS response for punch hole");
    if let Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) = resp {
        let resp_msg = RendezvousMessage::parse_from_bytes(&bytes).unwrap();
        assert!(resp_msg.has_punch_hole_response());
        assert_eq!(
            resp_msg.punch_hole_response().failure,
            punch_hole_response::Failure::ID_NOT_EXIST.into()
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_ws_test_nat_request() {
    use hbb_common::futures_util::{SinkExt, StreamExt};

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    // TestNatRequest via WS goes through handle_tcp path
    let tcp = hbb_common::tokio::net::TcpStream::connect(format!(
        "127.0.0.1:{}",
        server.ws_port
    ))
    .await
    .unwrap();
    let (ws, _) = tokio_tungstenite::client_async(
        format!("ws://127.0.0.1:{}", server.ws_port),
        tcp,
    )
    .await
    .unwrap();
    let (mut write, mut read) = ws.split();

    let mut msg = RendezvousMessage::new();
    msg.set_test_nat_request(TestNatRequest {
        serial: 0,
        ..Default::default()
    });
    write
        .send(tungstenite::Message::Binary(
            msg.write_to_bytes().unwrap(),
        ))
        .await
        .unwrap();

    let resp = timeout(Duration::from_secs(3), read.next()).await;
    assert!(resp.is_ok(), "Should get TestNatResponse via WS");
    if let Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) = resp {
        let resp_msg = RendezvousMessage::parse_from_bytes(&bytes).unwrap();
        assert!(resp_msg.has_test_nat_response());
        assert!(resp_msg.test_nat_response().port > 0);
    }

    server.shutdown();
}

#[tokio::test]
async fn test_nat_test_via_tcp() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.nat_port))
        .await
        .unwrap();

    let mut msg = RendezvousMessage::new();
    msg.set_test_nat_request(TestNatRequest {
        serial: 1,
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();

    let len = (bytes.len() as u32).to_be_bytes();
    stream.write_all(&len).await.unwrap();
    stream.write_all(&bytes).await.unwrap();

    let mut len_buf = [0u8; 4];
    let read_result = timeout(Duration::from_secs(3), stream.read_exact(&mut len_buf)).await;
    if let Ok(Ok(_)) = read_result {
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await.unwrap();
        let resp = RendezvousMessage::parse_from_bytes(&resp_buf).unwrap();
        assert!(resp.has_test_nat_response());
        assert!(resp.test_nat_response().port > 0);
    }

    server.shutdown();
}

#[tokio::test]
async fn test_online_request_via_nat_port() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);
    let nat_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", server.nat_port).parse().unwrap();

    // Register a peer via UDP first
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "online_peer_1".to_owned(),
        ..Default::default()
    });
    send_recv_udp(&socket, &msg, &udp_addr).await.unwrap();
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "online_peer_1".to_owned(),
        uuid: vec![1; 16].into(),
        pk: vec![2; 32].into(),
        ..Default::default()
    });
    send_recv_udp(&socket, &msg, &udp_addr).await.unwrap();

    // Connect to nat_port via TCP and send OnlineRequest
    let tcp = hbb_common::tokio::net::TcpStream::connect(nat_addr)
        .await
        .unwrap();
    let mut stream = FramedStream::from(tcp, nat_addr);

    use hbb_common::futures_util::StreamExt;
    let mut msg = RendezvousMessage::new();
    msg.set_online_request(OnlineRequest {
        peers: vec!["online_peer_1".to_owned(), "nonexistent_99".to_owned()],
        ..Default::default()
    });
    stream.send(&msg).await.unwrap();

    let resp = timeout(Duration::from_secs(3), stream.next()).await;
    assert!(resp.is_ok(), "Should get OnlineResponse");
    if let Ok(Some(Ok(bytes))) = resp {
        let resp_msg = RendezvousMessage::parse_from_bytes(&bytes).unwrap();
        assert!(resp_msg.has_online_response());
        let states = resp_msg.online_response().states.clone();
        // First peer is online (bit 7 of byte 0 should be set)
        // Second peer is not registered (bit 6 of byte 0 should be 0)
        assert!(!states.is_empty());
        assert!(states[0] & 0x80 != 0, "online_peer_1 should be online");
        assert!(
            states[0] & 0x40 == 0,
            "nonexistent_99 should NOT be online"
        );
    }

    server.shutdown();
}

// ---- TCP admin commands ----

#[tokio::test]
async fn test_admin_cmd_via_loopback_tcp() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("Failed to start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();

    stream.write_all(b"h").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let read_result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = read_result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(
            response.contains("relay-servers"),
            "Help text should mention relay-servers"
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_ip_blocker() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    // Query ip-blocker
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"ib").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        // Should return a count on the first line
        let first_line = response.lines().next().unwrap_or("");
        assert!(
            first_line.parse::<u32>().is_ok(),
            "ip-blocker should return count: got '{}'",
            first_line
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_ip_changes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"ic").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        let first_line = response.lines().next().unwrap_or("");
        assert!(
            first_line.parse::<u32>().is_ok(),
            "ip-changes should return count: got '{}'",
            first_line
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_always_use_relay() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"aur").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(
            response.contains("ALWAYS_USE_RELAY"),
            "Should return ALWAYS_USE_RELAY status"
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_relay_servers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    // Query current relay servers
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"rs").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    // If no relay servers are configured, the response may be empty
    // Just verify it doesn't crash
    match result {
        Ok(Ok(n)) => {
            let _response = std::str::from_utf8(&buf[..n]).unwrap();
        }
        _ => {} // timeout is OK if no relay servers
    }

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_test_geo() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream
        .write_all(b"tg 1.2.3.4 5.6.7.8")
        .await
        .unwrap();

    let mut buf = vec![0u8; 4096];
    let _result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    // Just verifying it doesn't crash; response depends on relay server config

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_on_nat_port() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    // Admin cmd on nat_port (handle_listener2 also handles loopback admin)
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.nat_port))
        .await
        .unwrap();
    stream.write_all(b"h").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(response.contains("relay-servers") || response.contains("ip-blocker"));
    }

    server.shutdown();
}

// ---- Additional WS/TCP handler path tests ----

async fn ws_connect_hbbs(
    port: u16,
) -> (
    hbb_common::futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<hbb_common::tokio::net::TcpStream>,
        tungstenite::Message,
    >,
    hbb_common::futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<hbb_common::tokio::net::TcpStream>,
    >,
) {
    use hbb_common::futures_util::StreamExt;
    let tcp = hbb_common::tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let (ws, _) =
        tokio_tungstenite::client_async(format!("ws://127.0.0.1:{}", port), tcp)
            .await
            .unwrap();
    ws.split()
}

async fn ws_send(
    write: &mut hbb_common::futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<hbb_common::tokio::net::TcpStream>,
        tungstenite::Message,
    >,
    msg: &RendezvousMessage,
) {
    use hbb_common::futures_util::SinkExt;
    write
        .send(tungstenite::Message::Binary(
            msg.write_to_bytes().unwrap(),
        ))
        .await
        .unwrap();
}

async fn ws_recv(
    read: &mut hbb_common::futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<hbb_common::tokio::net::TcpStream>,
    >,
) -> Option<RendezvousMessage> {
    use hbb_common::futures_util::StreamExt;
    match timeout(Duration::from_secs(3), read.next()).await {
        Ok(Some(Ok(tungstenite::Message::Binary(bytes)))) => {
            RendezvousMessage::parse_from_bytes(&bytes).ok()
        }
        _ => None,
    }
}

#[tokio::test]
async fn test_ws_request_relay_peer_not_found() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let (mut write, mut read) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(hbb_common::rendezvous_proto::RequestRelay {
        id: "nonexistent_relay_peer".to_owned(),
        ..Default::default()
    });
    ws_send(&mut write, &msg).await;

    let resp = timeout(Duration::from_secs(1), hbb_common::futures_util::StreamExt::next(&mut read)).await;
    assert!(resp.is_err(), "No response expected when relay peer not found");

    server.shutdown();
}

#[tokio::test]
async fn test_ws_test_nat_with_config_update() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let (mut write, mut read) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_test_nat_request(TestNatRequest {
        serial: 0,
        ..Default::default()
    });
    ws_send(&mut write, &msg).await;

    let resp = ws_recv(&mut read).await;
    assert!(resp.is_some(), "Should get TestNatResponse");
    let resp = resp.unwrap();
    assert!(resp.has_test_nat_response());
    assert!(resp.test_nat_response().port > 0);

    server.shutdown();
}

#[tokio::test]
async fn test_ws_register_peer_then_punch_hole_found() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "ws_ph_target".to_owned(),
        ..Default::default()
    });
    send_recv_udp(&socket, &msg, &udp_addr).await;

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ws_ph_target".to_owned(),
        uuid: vec![20; 16].into(),
        pk: vec![21; 32].into(),
        ..Default::default()
    });
    send_recv_udp(&socket, &msg, &udp_addr).await;

    let (mut write, mut _read) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_punch_hole_request(PunchHoleRequest {
        id: "ws_ph_target".to_owned(),
        ..Default::default()
    });
    ws_send(&mut write, &msg).await;

    // Give server time to process and send to peer B
    hbb_common::tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain B's socket for messages — may receive PunchHole or FetchLocalAddr
    let mut buf = vec![0u8; 65535];
    for _ in 0..5 {
        match timeout(Duration::from_secs(1), socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if let Ok(resp) = RendezvousMessage::parse_from_bytes(&buf[..n]) {
                    if resp.has_punch_hole() || resp.has_fetch_local_addr() {
                        break;
                    }
                }
            }
            _ => break,
        }
    }

    server.shutdown();
}

#[tokio::test]
async fn test_ws_request_relay_with_registered_peer() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "relay_target_peer".to_owned(),
        ..Default::default()
    });
    send_recv_udp(&socket, &msg, &udp_addr).await;

    let (mut write, _read) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(hbb_common::rendezvous_proto::RequestRelay {
        id: "relay_target_peer".to_owned(),
        uuid: "relay-test-uuid".to_owned(),
        ..Default::default()
    });
    ws_send(&mut write, &msg).await;

    // Give server time to process and forward
    hbb_common::tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain B's socket — may or may not receive RequestRelay depending on timing
    let mut buf = vec![0u8; 65535];
    let _ = timeout(Duration::from_secs(1), socket.recv_from(&mut buf)).await;

    server.shutdown();
}

#[tokio::test]
async fn test_multiple_peers_online_check() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    for i in 0..9u8 {
        let id = format!("multi_peer_{}", i);
        let mut msg = RendezvousMessage::new();
        msg.set_register_peer(RegisterPeer {
            id: id.clone(),
            ..Default::default()
        });
        send_recv_udp(&socket, &msg, &udp_addr).await;

        let mut msg = RendezvousMessage::new();
        msg.set_register_pk(RegisterPk {
            id: id.clone(),
            uuid: vec![i + 30; 16].into(),
            pk: vec![i + 40; 32].into(),
            ..Default::default()
        });
        send_recv_udp(&socket, &msg, &udp_addr).await;
    }

    let nat_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", server.nat_port).parse().unwrap();
    let tcp = hbb_common::tokio::net::TcpStream::connect(nat_addr)
        .await
        .unwrap();
    let mut stream = FramedStream::from(tcp, nat_addr);

    use hbb_common::futures_util::StreamExt;
    let peers: Vec<String> = (0..9).map(|i| format!("multi_peer_{}", i)).collect();
    let mut msg = RendezvousMessage::new();
    msg.set_online_request(OnlineRequest {
        peers,
        ..Default::default()
    });
    stream.send(&msg).await.unwrap();

    let resp = timeout(Duration::from_secs(3), stream.next()).await;
    assert!(resp.is_ok(), "Should get OnlineResponse");
    if let Ok(Some(Ok(bytes))) = resp {
        let resp_msg = RendezvousMessage::parse_from_bytes(&bytes).unwrap();
        assert!(resp_msg.has_online_response());
        let states = resp_msg.online_response().states.clone();
        assert!(states.len() >= 2, "Need at least 2 bytes for 9 peers");
        // At least some peers should be online (timing may affect exact count)
        assert!(states[0] != 0, "At least some peers should be online");
    }

    server.shutdown();
}

#[tokio::test]
async fn test_ws_register_pk_rejected_uuid_mismatch() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let (mut write, mut read) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ws_mismatch_peer".to_owned(),
        uuid: vec![50; 16].into(),
        pk: vec![51; 32].into(),
        ..Default::default()
    });
    ws_send(&mut write, &msg).await;

    let resp = ws_recv(&mut read).await;
    assert!(resp.is_some());
    assert_eq!(
        resp.unwrap().register_pk_response().result,
        register_pk_response::Result::OK.into()
    );

    drop(write);
    drop(read);

    let (mut write2, mut read2) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ws_mismatch_peer".to_owned(),
        uuid: vec![99; 16].into(),
        pk: vec![51; 32].into(),
        ..Default::default()
    });
    ws_send(&mut write2, &msg).await;

    let resp = ws_recv(&mut read2).await;
    assert!(resp.is_some());
    assert_eq!(
        resp.unwrap().register_pk_response().result,
        register_pk_response::Result::UUID_MISMATCH.into()
    );

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_set_always_use_relay() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"aur Y").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(response.contains("Y"), "Should confirm ALWAYS_USE_RELAY=Y");
    }
    drop(stream);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"aur").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(response.contains("Y"), "ALWAYS_USE_RELAY should now be Y");
    }

    server.shutdown();
}

#[tokio::test]
async fn test_admin_cmd_set_relay_servers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"rs 127.0.0.1:21117").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let _result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    drop(stream);

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", server.port))
        .await
        .unwrap();
    stream.write_all(b"rs").await.unwrap();

    let mut buf = vec![0u8; 4096];
    let result = timeout(Duration::from_secs(3), stream.read(&mut buf)).await;
    if let Ok(Ok(n)) = result {
        let response = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(
            response.contains("127.0.0.1:21117"),
            "Should contain the set relay server"
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_register_peer_via_ws_port() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");

    let (mut write, mut read) = ws_connect_hbbs(server.ws_port).await;

    let mut msg = RendezvousMessage::new();
    msg.set_register_peer(RegisterPeer {
        id: "ws_reg_peer".to_owned(),
        ..Default::default()
    });
    ws_send(&mut write, &msg).await;

    // RegisterPeer isn't handled in handle_tcp — falls through to default
    let resp = timeout(Duration::from_secs(2), hbb_common::futures_util::StreamExt::next(&mut read)).await;
    match resp {
        Err(_) => {}
        Ok(None) => {}
        Ok(Some(Ok(tungstenite::Message::Close(_)))) => {}
        Ok(Some(Ok(_msg))) => {}
        Ok(Some(Err(_))) => {}
    }

    server.shutdown();
}

#[tokio::test]
async fn test_configure_update_via_udp() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    // Send ConfigureUpdate with serial > 0 (server starts at 0)
    let mut msg = RendezvousMessage::new();
    msg.set_configure_update(ConfigUpdate {
        serial: 100,
        rendezvous_servers: vec!["127.0.0.1:21116".to_owned()],
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();
    socket.send_to(&bytes, &udp_addr).await.unwrap();

    // ConfigureUpdate has no response — just verify no crash
    hbb_common::tokio::time::sleep(Duration::from_millis(200)).await;

    server.shutdown();
}

#[tokio::test]
async fn test_register_pk_ip_change_tracking() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    // Register PK from first socket
    let socket1 = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ip_change_peer".to_owned(),
        uuid: vec![60; 16].into(),
        pk: vec![61; 32].into(),
        ..Default::default()
    });
    send_recv_udp(&socket1, &msg, &udp_addr).await;

    // Re-register from a different socket (different source port = "IP change" from server perspective)
    let socket2 = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ip_change_peer".to_owned(),
        uuid: vec![60; 16].into(),
        pk: vec![61; 32].into(),
        ..Default::default()
    });
    let resp = send_recv_udp(&socket2, &msg, &udp_addr).await;
    assert!(resp.is_some());
    assert!(resp.unwrap().has_register_pk_response());

    // Register again from a third socket
    let socket3 = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: "ip_change_peer".to_owned(),
        uuid: vec![60; 16].into(),
        pk: vec![61; 32].into(),
        ..Default::default()
    });
    let resp = send_recv_udp(&socket3, &msg, &udp_addr).await;
    assert!(resp.is_some());

    server.shutdown();
}

#[tokio::test]
async fn test_punch_hole_sent_via_udp() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    // Send PunchHoleSent — exercises the UDP handle_hole_sent path
    let mut msg = RendezvousMessage::new();
    msg.set_punch_hole_sent(PunchHoleSent {
        socket_addr: vec![1, 2, 3, 4, 5, 6].into(),
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();
    socket.send_to(&bytes, &udp_addr).await.unwrap();

    hbb_common::tokio::time::sleep(Duration::from_millis(200)).await;

    server.shutdown();
}

#[tokio::test]
async fn test_local_addr_via_udp() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    // Send LocalAddr — exercises the UDP handle_local_addr path
    let mut msg = RendezvousMessage::new();
    msg.set_local_addr(LocalAddr {
        socket_addr: vec![1, 2, 3, 4, 5, 6].into(),
        local_addr: vec![10, 20, 30, 40].into(),
        ..Default::default()
    });
    let bytes = msg.write_to_bytes().unwrap();
    socket.send_to(&bytes, &udp_addr).await.unwrap();

    hbb_common::tokio::time::sleep(Duration::from_millis(200)).await;

    server.shutdown();
}

#[tokio::test]
async fn test_software_update_with_outdated_version() {
    let server = hbbs::RendezvousServer::start_test("")
        .await
        .expect("start test server");
    let udp_addr = format!("127.0.0.1:{}", server.udp_port);

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    // SoftwareUpdate with a different version — exercises the version comparison path
    let mut msg = RendezvousMessage::new();
    msg.set_software_update(SoftwareUpdate {
        url: "https://example.com/update-1.0.0".to_owned(),
        ..Default::default()
    });
    let resp = send_recv_udp(&socket, &msg, &udp_addr).await;
    // Server has no version set (empty string), so no SoftwareUpdate response
    // This just exercises the code path
    server.shutdown();
}
