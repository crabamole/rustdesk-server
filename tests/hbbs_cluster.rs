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

/// Test-only ed25519 secret key: pods share one keypair, as in a deployment.
const KEY: &str = "8xHdtm4mhNywDf8xTsEsrkHHTwOUlApFXKxNFJXnQeEXoj+9RmSytxmlyGz0RCBtYhwQ45x77o3jy8yM77rQCg==";

async fn start(pod: &str, db_url: Option<String>) -> TestServer {
    RendezvousServer::start_test_with(TestOptions {
        key: KEY.to_owned(),
        pod: pod.to_owned(),
        db_url,
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
