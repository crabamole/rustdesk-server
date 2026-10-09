//! Several hbbs instances on one database, standing in for the pods of a StatefulSet.
use hbb_common::{
    futures_util::{
        stream::{SplitSink, SplitStream},
        SinkExt, StreamExt,
    },
    protobuf::Message as _,
    rendezvous_proto::*,
    tokio::{self, net::TcpStream, time::{sleep, timeout}},
};
use hbbs::{RendezvousServer, TestOptions, TestServer};
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message as WsMessage;

type Ws = WebSocketStream<TcpStream>;

const RELAY: &str = "wss://relay.test/ws/relay/0";

/// Test-only ed25519 secret key: pods share one keypair, as in a deployment.
const KEY: &str = "8xHdtm4mhNywDf8xTsEsrkHHTwOUlApFXKxNFJXnQeEXoj+9RmSytxmlyGz0RCBtYhwQ45x77o3jy8yM77rQCg==";
/// Public half of `KEY`, which clients send as `licence_key`.
const PK: &str = "F6I/vUZksrcZpchs9EQgbWIcEOOce+6N48vMjO+60Ao=";

async fn start(pod: &str, db_url: Option<String>) -> TestServer {
    RendezvousServer::start_test_with(TestOptions {
        key: KEY.to_owned(),
        pod: pod.to_owned(),
        db_url,
        relay_urls: vec![RELAY.to_owned()],
        ..Default::default()
    })
    .await
    .expect("start test server")
}

async fn connect(port: u16) -> (SplitSink<Ws, WsMessage>, SplitStream<Ws>) {
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let (ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{port}"), tcp).await.unwrap();
    ws.split()
}

async fn send(w: &mut SplitSink<Ws, WsMessage>, msg: RendezvousMessage) {
    w.send(WsMessage::Binary(msg.write_to_bytes().unwrap())).await.unwrap();
}

/// The next message, skipping keep-alive frames.
async fn recv(r: &mut SplitStream<Ws>) -> RendezvousMessage {
    loop {
        match timeout(Duration::from_secs(5), r.next()).await {
            Ok(Some(Ok(WsMessage::Binary(b)))) if !b.is_empty() => {
                return RendezvousMessage::parse_from_bytes(&b).unwrap()
            }
            Ok(Some(Ok(_))) => continue,
            other => panic!("no message: {other:?}"),
        }
    }
}

/// A device registered over `/ws/id`, with its open connection.
async fn device(port: u16, id: &str) -> (SplitSink<Ws, WsMessage>, SplitStream<Ws>, RegisterPkResponse) {
    let (mut w, mut r) = connect(port).await;
    let mut msg = RendezvousMessage::new();
    msg.set_register_pk(RegisterPk {
        id: id.to_owned(),
        uuid: id.as_bytes().to_vec().into(),
        pk: vec![9; 32].into(),
        ..Default::default()
    });
    send(&mut w, msg).await;
    let resp = recv(&mut r).await.register_pk_response().clone();
    (w, r, resp)
}

/// The pod a device's presence row names, waiting for the write that follows registration.
async fn presence_pod(db_url: &str, id: &str) -> Option<String> {
    let pool = sqlx::PgPool::connect(db_url).await.unwrap();
    for _ in 0..60 {
        let pod: Option<String> = sqlx::query_scalar("SELECT pod FROM peer_presence WHERE id = $1")
            .bind(id)
            .fetch_optional(&pool)
            .await
            .unwrap();
        if pod.is_some() {
            return pod;
        }
        sleep(Duration::from_millis(50)).await;
    }
    None
}

async fn http_status(port: u16, path: &str) -> u16 {
    reqwest::get(format!("http://127.0.0.1:{port}{path}")).await.unwrap().status().as_u16()
}

#[tokio::test]
async fn registration_sets_keep_alive_and_presence() {
    let server = start("hbbs-0", None).await;
    let (_w, _r, resp) = device(server.ws_port, "clu_keep1").await;
    assert_eq!(resp.result, register_pk_response::Result::OK.into());
    assert_eq!(resp.keep_alive, 20);
    assert_eq!(presence_pod(&server.db_url, "clu_keep1").await.as_deref(), Some("hbbs-0"));
    server.shutdown();
}

#[tokio::test]
async fn disconnect_removes_presence() {
    let server = start("hbbs-0", None).await;
    let (w, r, _) = device(server.ws_port, "clu_bye1").await;
    assert!(presence_pod(&server.db_url, "clu_bye1").await.is_some());
    drop((w, r));
    let pool = sqlx::PgPool::connect(&server.db_url).await.unwrap();
    let mut rows = 1i64;
    for _ in 0..60 {
        rows = sqlx::query_scalar("SELECT count(*) FROM peer_presence WHERE id = 'clu_bye1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        if rows == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(rows, 0);
    server.shutdown();
}

#[tokio::test]
async fn health_endpoints_answer() {
    let server = start("hbbs-0", None).await;
    assert_eq!(http_status(server.health_port, "/livez").await, 200);
    assert_eq!(http_status(server.health_port, "/readyz").await, 200);
    server.shutdown();
}

async fn pair() -> (TestServer, TestServer) {
    let pod0 = start("hbbs-0", None).await;
    let pod1 = start("hbbs-1", Some(pod0.db_url.clone())).await;
    (pod0, pod1)
}

fn relay_answer(socket_addr: hbb_common::bytes::Bytes, uuid: &str, id: &str) -> RendezvousMessage {
    let mut rr = RelayResponse {
        socket_addr,
        uuid: uuid.to_owned(),
        relay_server: RELAY.to_owned(),
        ..Default::default()
    };
    rr.set_id(id.to_owned());
    let mut msg = RendezvousMessage::new();
    msg.set_relay_response(rr);
    msg
}

#[tokio::test]
async fn punch_hole_crosses_pods_both_ways() {
    let (pod0, pod1) = pair().await;
    let (_dw, mut dr, _) = device(pod0.ws_port, "clu_punch1").await;
    assert_eq!(presence_pod(&pod0.db_url, "clu_punch1").await.as_deref(), Some("hbbs-0"));

    // Viewer on hbbs-1 asks for a device held by hbbs-0.
    let (mut vw, mut vr) = connect(pod1.ws_port).await;
    let mut req = RendezvousMessage::new();
    req.set_punch_hole_request(PunchHoleRequest {
        id: "clu_punch1".to_owned(),
        licence_key: PK.to_owned(),
        ..Default::default()
    });
    send(&mut vw, req).await;

    let ph = recv(&mut dr).await;
    assert!(ph.has_punch_hole(), "{ph:?}");
    assert_eq!(ph.punch_hole().relay_server, RELAY);
    let viewer = hbb_common::AddrMangle::decode(&ph.punch_hole().socket_addr);
    assert_eq!(viewer.port() >> 11, 1, "the token names hbbs-1");

    // The device answers on a new connection to hbbs-0; hbbs-0 passes it back to hbbs-1.
    let (mut aw, _ar) = connect(pod0.ws_port).await;
    send(&mut aw, relay_answer(ph.punch_hole().socket_addr.clone(), "clu-uuid-1", "clu_punch1")).await;
    let got = recv(&mut vr).await;
    assert_eq!(got.relay_response().uuid, "clu-uuid-1");
    assert_eq!(got.relay_response().relay_server, RELAY);
    pod0.shutdown();
    pod1.shutdown();
}

#[tokio::test]
async fn request_relay_crosses_pods() {
    let (pod0, pod1) = pair().await;
    let (_dw, mut dr, _) = device(pod1.ws_port, "clu_relay1").await;
    assert_eq!(presence_pod(&pod0.db_url, "clu_relay1").await.as_deref(), Some("hbbs-1"));

    let (mut vw, mut vr) = connect(pod0.ws_port).await;
    let mut req = RendezvousMessage::new();
    req.set_request_relay(RequestRelay {
        id: "clu_relay1".to_owned(),
        uuid: "clu-uuid-2".to_owned(),
        relay_server: RELAY.to_owned(),
        ..Default::default()
    });
    send(&mut vw, req).await;

    let rq = recv(&mut dr).await;
    assert!(rq.has_request_relay(), "{rq:?}");
    assert_eq!(hbb_common::AddrMangle::decode(&rq.request_relay().socket_addr).port() >> 11, 0);

    let (mut aw, _ar) = connect(pod1.ws_port).await;
    send(&mut aw, relay_answer(rq.request_relay().socket_addr.clone(), "clu-uuid-2", "clu_relay1")).await;
    assert_eq!(recv(&mut vr).await.relay_response().uuid, "clu-uuid-2");
    pod0.shutdown();
    pod1.shutdown();
}

#[tokio::test]
async fn a_row_of_an_earlier_epoch_answers_offline() {
    let (pod0, pod1) = pair().await;
    let (_dw, _dr, _) = device(pod0.ws_port, "clu_off1").await;
    assert!(presence_pod(&pod0.db_url, "clu_off1").await.is_some());
    let pool = sqlx::PgPool::connect(&pod0.db_url).await.unwrap();
    sqlx::query("UPDATE peer_presence SET epoch = gen_random_uuid() WHERE id = 'clu_off1'")
        .execute(&pool)
        .await
        .unwrap();

    let (mut vw, mut vr) = connect(pod1.ws_port).await;
    let mut req = RendezvousMessage::new();
    req.set_punch_hole_request(PunchHoleRequest {
        id: "clu_off1".to_owned(),
        licence_key: PK.to_owned(),
        ..Default::default()
    });
    send(&mut vw, req).await;
    let resp = recv(&mut vr).await;
    assert_eq!(resp.punch_hole_response().failure, punch_hole_response::Failure::OFFLINE.into());
    pod0.shutdown();
    pod1.shutdown();
}

#[tokio::test]
async fn online_request_over_ws_sees_other_pods() {
    let (pod0, pod1) = pair().await;
    let (_dw, _dr, _) = device(pod0.ws_port, "clu_online1").await;
    assert!(presence_pod(&pod0.db_url, "clu_online1").await.is_some());

    let (mut w, mut r) = connect(pod1.ws_port).await;
    let mut req = RendezvousMessage::new();
    req.set_online_request(OnlineRequest {
        peers: vec!["clu_online1".to_owned(), "clu_nobody".to_owned()],
        ..Default::default()
    });
    send(&mut w, req).await;
    assert_eq!(&recv(&mut r).await.online_response().states[..], &[0b1000_0000]);
    pod0.shutdown();
    pod1.shutdown();
}

#[tokio::test]
async fn punch_hole_answer_reaches_a_viewer_on_the_same_pod() {
    let pod0 = start("hbbs-0", None).await;
    let (_dw, mut dr, _) = device(pod0.ws_port, "clu_local1").await;
    assert!(presence_pod(&pod0.db_url, "clu_local1").await.is_some());

    let (mut vw, mut vr) = connect(pod0.ws_port).await;
    let mut req = RendezvousMessage::new();
    req.set_punch_hole_request(PunchHoleRequest {
        id: "clu_local1".to_owned(),
        licence_key: PK.to_owned(),
        ..Default::default()
    });
    send(&mut vw, req).await;

    let ph = recv(&mut dr).await;
    assert!(ph.has_punch_hole(), "{ph:?}");
    let viewer = hbb_common::AddrMangle::decode(&ph.punch_hole().socket_addr);
    assert_eq!(viewer.port() >> 11, 0, "the token names hbbs-0");

    let (mut aw, _ar) = connect(pod0.ws_port).await;
    send(&mut aw, relay_answer(ph.punch_hole().socket_addr.clone(), "clu-uuid-4", "clu_local1")).await;
    assert_eq!(recv(&mut vr).await.relay_response().uuid, "clu-uuid-4");
    pod0.shutdown();
}

#[tokio::test]
async fn drain_moves_devices_and_fails_readiness() {
    let server = start("hbbs-0", None).await;
    let (_dw, mut dr, _) = device(server.ws_port, "clu_drain1").await;
    assert!(presence_pod(&server.db_url, "clu_drain1").await.is_some());

    server.drain().await;
    match timeout(Duration::from_secs(5), dr.next()).await {
        Ok(Some(Ok(WsMessage::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => {}
        other => panic!("device connection still open: {other:?}"),
    }
    let pool = sqlx::PgPool::connect(&server.db_url).await.unwrap();
    let rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM peer_presence WHERE pod = 'hbbs-0') + (SELECT count(*) FROM hbbs_pod WHERE pod = 'hbbs-0')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rows, 0);
    assert_eq!(http_status(server.health_port, "/readyz").await, 503);
    assert_eq!(http_status(server.health_port, "/livez").await, 200);
    server.shutdown();
}
