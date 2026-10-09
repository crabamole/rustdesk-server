// Shared by the hbbr integration tests.
use hbb_common::{
    futures_util::{SinkExt, StreamExt},
    protobuf::Message as _,
    rendezvous_proto::*,
    tokio::{
        net::TcpStream,
        time::{sleep, timeout},
    },
};
use std::time::Duration;
use tokio_tungstenite::WebSocketStream;

pub type Ws = WebSocketStream<TcpStream>;

pub async fn ws_connect_path(port: u16, path: &str) -> Ws {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();
    let (ws, _) = tokio_tungstenite::client_async(format!("ws://127.0.0.1:{port}{path}"), stream)
        .await
        .unwrap();
    ws
}

pub async fn request_relay(ws: &mut Ws, uuid: &str) {
    let mut msg = RendezvousMessage::new();
    msg.set_request_relay(RequestRelay {
        uuid: uuid.to_owned(),
        ..Default::default()
    });
    ws.send(tungstenite::Message::Binary(msg.write_to_bytes().unwrap()))
        .await
        .unwrap();
}

/// Two halves paired on `uuid` (dialed on `path_a` / `path_b`), checked by one message from A to B.
pub async fn pair_on(ws_port: u16, uuid: &str, path_a: &str, path_b: &str) -> (Ws, Ws) {
    let mut a = ws_connect_path(ws_port, path_a).await;
    request_relay(&mut a, uuid).await;
    sleep(Duration::from_millis(100)).await;
    let mut b = ws_connect_path(ws_port, path_b).await;
    request_relay(&mut b, uuid).await;
    sleep(Duration::from_millis(200)).await;
    a.send(tungstenite::Message::Binary(uuid.as_bytes().to_vec()))
        .await
        .unwrap();
    let got = timeout(Duration::from_secs(3), b.next())
        .await
        .expect("paired halves exchange data")
        .unwrap()
        .unwrap();
    assert_eq!(got, tungstenite::Message::Binary(uuid.as_bytes().to_vec()));
    (a, b)
}

pub async fn pair(ws_port: u16, uuid: &str) -> (Ws, Ws) {
    pair_on(ws_port, uuid, "", "").await
}
