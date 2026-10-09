//! Pod-to-pod calls on INTERNAL_PORT, one per connection: a token frame, a call wrapped in an
//! `HttpProxyRequest`, answered by an `HttpProxyResponse` (200 delivered, 404 not here, 400 bad).
use hbb_common::{
    bail,
    bytes::BytesMut,
    log,
    protobuf::Message as _,
    rendezvous_proto::*,
    sha2::{Digest, Sha256},
    sodiumoxide::utils::memcmp,
    tcp::FramedStream,
    tokio::{self, net::TcpListener},
    ResultType,
};
use std::{future::Future, net::SocketAddr, time::Duration};

pub(crate) type Token = [u8; 32];
pub(crate) const TIMEOUT_MS: u64 = 2_000;
const MAX_CALL_BYTES: usize = 64 * 1024;
const DEVICE: &str = "/device/";
const VIEWER: &str = "/viewer/";

/// Only pods holding the shared server keypair can derive it.
pub(crate) fn token(secret_key: &[u8]) -> Token {
    let mut h = Sha256::new();
    h.update(b"hbbs-internal-v1");
    h.update(secret_key);
    h.finalize().into()
}

#[derive(Debug, PartialEq)]
pub(crate) enum Call {
    Device { id: String, msg: RendezvousMessage },
    Viewer { addr: SocketAddr, msg: RendezvousMessage },
}

impl Call {
    pub(crate) fn encode(&self) -> ResultType<RendezvousMessage> {
        let (path, msg) = match self {
            Call::Device { id, msg } => (format!("{DEVICE}{id}"), msg),
            Call::Viewer { addr, msg } => (format!("{VIEWER}{addr}"), msg),
        };
        let mut out = RendezvousMessage::new();
        out.set_http_proxy_request(HttpProxyRequest {
            method: "POST".to_owned(),
            path,
            body: msg.write_to_bytes()?.into(),
            ..Default::default()
        });
        Ok(out)
    }

    pub(crate) fn decode(msg: &RendezvousMessage) -> Option<Call> {
        let Some(rendezvous_message::Union::HttpProxyRequest(req)) = &msg.union else {
            return None;
        };
        let inner = RendezvousMessage::parse_from_bytes(&req.body).ok()?;
        if let Some(id) = req.path.strip_prefix(DEVICE) {
            return (!id.is_empty()).then(|| Call::Device { id: id.to_owned(), msg: inner });
        }
        let addr = req.path.strip_prefix(VIEWER)?.parse().ok()?;
        Some(Call::Viewer { addr, msg: inner })
    }
}

pub(crate) async fn call(addr: &str, token: &Token, call: &Call) -> ResultType<bool> {
    let mut stream = FramedStream::new(addr, None, TIMEOUT_MS).await?;
    stream.send_raw(token.to_vec()).await?;
    stream.send(&call.encode()?).await?;
    let answer = RendezvousMessage::parse_from_bytes(&next(&mut stream).await?)?;
    match answer.union {
        Some(rendezvous_message::Union::HttpProxyResponse(r)) => Ok(r.status == 200),
        _ => bail!("unexpected answer from {addr}"),
    }
}

pub(crate) async fn serve<H, F>(listener: TcpListener, token: Token, handle: H)
where
    H: Fn(Call) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = bool> + Send + 'static,
{
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                log::error!("internal accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let handle = handle.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_one(FramedStream::from(stream, addr), &token, handle).await {
                log::debug!("internal call from {addr}: {e}");
            }
        });
    }
}

async fn serve_one<H, F>(mut stream: FramedStream, token: &Token, handle: H) -> ResultType<()>
where
    H: Fn(Call) -> F,
    F: Future<Output = bool>,
{
    // Unauthenticated peers may only make us buffer a token-sized frame.
    stream.0.codec_mut().set_max_packet_length(token.len());
    if !memcmp(&next(&mut stream).await?, token) {
        bail!("bad token");
    }
    stream.0.codec_mut().set_max_packet_length(MAX_CALL_BYTES);
    let msg = RendezvousMessage::parse_from_bytes(&next(&mut stream).await?)?;
    let status = match Call::decode(&msg) {
        Some(call) => {
            let delivered = tokio::time::timeout(Duration::from_millis(TIMEOUT_MS), handle(call));
            if delivered.await.unwrap_or(false) {
                200
            } else {
                404
            }
        }
        None => 400,
    };
    let mut answer = RendezvousMessage::new();
    answer.set_http_proxy_response(HttpProxyResponse { status, ..Default::default() });
    stream.send(&answer).await
}

async fn next(stream: &mut FramedStream) -> ResultType<BytesMut> {
    match stream.next_timeout(TIMEOUT_MS).await {
        Some(Ok(frame)) => Ok(frame),
        Some(Err(e)) => Err(e.into()),
        None => bail!("connection closed or timed out"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn punch() -> RendezvousMessage {
        let mut m = RendezvousMessage::new();
        m.set_punch_hole(PunchHole {
            relay_server: "wss://r/ws/relay/0".to_owned(),
            ..Default::default()
        });
        m
    }

    fn device(id: &str) -> Call {
        Call::Device { id: id.to_owned(), msg: punch() }
    }

    async fn server(token: Token) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(listener, token, |call| async move {
            matches!(call, Call::Device { ref id, .. } if id.as_str() == "here")
        }));
        addr
    }

    #[test]
    fn token_binds_prefix_and_key() {
        let t = token(&[1; 64]);
        assert_eq!(t, token(&[1; 64]));
        assert_ne!(t, token(&[2; 64]));
        let mut h = Sha256::new();
        h.update(b"hbbs-internal-v1");
        h.update([1u8; 64]);
        assert_eq!(t.to_vec(), h.finalize().to_vec());
    }

    #[test]
    fn calls_round_trip() {
        for call in [
            device("123456789"),
            Call::Viewer { addr: "203.0.113.5:2049".parse().unwrap(), msg: punch() },
            Call::Viewer { addr: "[2001:db8::7]:65535".parse().unwrap(), msg: punch() },
        ] {
            assert_eq!(Call::decode(&call.encode().unwrap()), Some(call));
        }
    }

    #[test]
    fn malformed_calls_are_rejected() {
        let req = |path: &str| {
            let mut m = RendezvousMessage::new();
            m.set_http_proxy_request(HttpProxyRequest {
                path: path.to_owned(),
                body: punch().write_to_bytes().unwrap().into(),
                ..Default::default()
            });
            m
        };
        assert_eq!(Call::decode(&punch()), None);
        assert_eq!(Call::decode(&req("/device/")), None);
        assert_eq!(Call::decode(&req("/viewer/not-an-addr")), None);
        assert_eq!(Call::decode(&req("/other/x")), None);
    }

    #[tokio::test]
    async fn call_reports_delivery() {
        let addr = server([3; 32]).await;
        assert!(call(&addr, &[3; 32], &device("here")).await.unwrap());
        assert!(!call(&addr, &[3; 32], &device("gone")).await.unwrap());
    }

    #[tokio::test]
    async fn wrong_token_gets_no_answer() {
        let addr = server([3; 32]).await;
        assert!(call(&addr, &[4; 32], &device("here")).await.is_err());
    }

    #[tokio::test]
    async fn a_message_that_is_not_a_call_gets_400() {
        let addr = server([3; 32]).await;
        let mut s = FramedStream::new(addr.as_str(), None, TIMEOUT_MS).await.unwrap();
        s.send_raw(vec![3; 32]).await.unwrap();
        s.send(&punch()).await.unwrap();
        let frame = s.next_timeout(TIMEOUT_MS).await.unwrap().unwrap();
        let answer = RendezvousMessage::parse_from_bytes(&frame).unwrap();
        assert_eq!(answer.http_proxy_response().status, 400);
    }

    #[tokio::test]
    async fn an_oversized_header_before_the_token_is_dropped_at_once() {
        use tokio::io::AsyncReadExt;
        let addr = server([3; 32]).await;
        let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
        // 4-byte header declaring a 512 MiB frame; no payload follows.
        s.try_write(&(((1u32 << 29) << 2) | 3).to_le_bytes()).unwrap();
        let mut buf = [0u8; 8];
        let read = tokio::time::timeout(Duration::from_millis(TIMEOUT_MS / 2), s.read(&mut buf));
        assert!(matches!(read.await, Ok(Ok(0)) | Ok(Err(_))));
    }

    #[tokio::test]
    async fn a_hung_handler_answers_404() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(listener, [3; 32], |_| std::future::pending::<bool>()));
        let mut s = FramedStream::new(addr.as_str(), None, TIMEOUT_MS).await.unwrap();
        s.send_raw(vec![3; 32]).await.unwrap();
        s.send(&device("here").encode().unwrap()).await.unwrap();
        let frame = s.next_timeout(TIMEOUT_MS * 2).await.unwrap().unwrap();
        let answer = RendezvousMessage::parse_from_bytes(&frame).unwrap();
        assert_eq!(answer.http_proxy_response().status, 404);
    }

    #[tokio::test]
    async fn a_silent_client_is_closed_after_the_timeout() {
        let addr = server([3; 32]).await;
        let mut s = FramedStream::new(addr.as_str(), None, TIMEOUT_MS).await.unwrap();
        let start = std::time::Instant::now();
        assert!(s.next_timeout(TIMEOUT_MS * 2).await.is_none());
        let waited = start.elapsed();
        assert!(waited >= Duration::from_millis(TIMEOUT_MS - 200), "{waited:?}");
        assert!(waited < Duration::from_millis(TIMEOUT_MS * 2), "{waited:?}");
    }

    #[tokio::test]
    async fn an_unreachable_pod_is_an_error() {
        assert!(call("127.0.0.1:1", &[3; 32], &device("here")).await.is_err());
    }
}
