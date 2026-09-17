use async_speed_limit::Limiter;
use async_trait::async_trait;
use hbb_common::{
    allow_err, bail,
    bytes::{Bytes, BytesMut},
    futures_util::{sink::SinkExt, stream::StreamExt},
    log,
    protobuf::Message as _,
    rendezvous_proto::*,
    sleep,
    tcp::{listen_any, FramedStream},
    timeout,
    tokio::{
        self,
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Mutex, RwLock},
        time::{interval, Duration},
    },
    ResultType,
};
use sodiumoxide::crypto::sign;
use std::{
    collections::{HashMap, HashSet},
    io::prelude::*,
    io::Error,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

type Usage = (usize, usize, usize, usize);

lazy_static::lazy_static! {
    static ref PEERS: Mutex<HashMap<String, Box<dyn StreamTrait>>> = Default::default();
    static ref USAGE: RwLock<HashMap<String, Usage>> = Default::default();
    static ref BLACKLIST: RwLock<HashSet<String>> = Default::default();
    static ref BLOCKLIST: RwLock<HashSet<String>> = Default::default();
}

static DOWNGRADE_THRESHOLD_100: AtomicUsize = AtomicUsize::new(66); // 0.66
static DOWNGRADE_START_CHECK: AtomicUsize = AtomicUsize::new(1_800_000); // in ms
static LIMIT_SPEED: AtomicUsize = AtomicUsize::new(32 * 1024 * 1024); // in bit/s
static TOTAL_BANDWIDTH: AtomicUsize = AtomicUsize::new(1024 * 1024 * 1024); // in bit/s
static SINGLE_BANDWIDTH: AtomicUsize = AtomicUsize::new(128 * 1024 * 1024); // in bit/s
const BLACKLIST_FILE: &str = "blacklist.txt";
const BLOCKLIST_FILE: &str = "blocklist.txt";

#[tokio::main(flavor = "multi_thread")]
pub async fn start(port: &str, key: &str) -> ResultType<()> {
    let key = get_server_sk(key);
    if let Ok(mut file) = std::fs::File::open(BLACKLIST_FILE) {
        let mut contents = String::new();
        if file.read_to_string(&mut contents).is_ok() {
            for x in contents.split('\n') {
                if let Some(ip) = x.trim().split(' ').next() {
                    BLACKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
    }
    log::info!(
        "#blacklist({}): {}",
        BLACKLIST_FILE,
        BLACKLIST.read().await.len()
    );
    if let Ok(mut file) = std::fs::File::open(BLOCKLIST_FILE) {
        let mut contents = String::new();
        if file.read_to_string(&mut contents).is_ok() {
            for x in contents.split('\n') {
                if let Some(ip) = x.trim().split(' ').next() {
                    BLOCKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
    }
    log::info!(
        "#blocklist({}): {}",
        BLOCKLIST_FILE,
        BLOCKLIST.read().await.len()
    );
    let port: u16 = port.parse()?;
    log::info!("Listening on tcp :{}", port);
    let port2 = port + 2;
    log::info!("Listening on websocket :{}", port2);
    let main_task = async move {
        loop {
            log::info!("Start");
            io_loop(listen_any(port).await?, listen_any(port2).await?, &key).await;
        }
    };
    let listen_signal = crate::common::listen_signal();
    tokio::select!(
        res = main_task => res,
        res = listen_signal => res,
    )
}

fn check_params() {
    let tmp = std::env::var("DOWNGRADE_THRESHOLD")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        DOWNGRADE_THRESHOLD_100.store((tmp * 100.) as _, Ordering::SeqCst);
    }
    log::info!(
        "DOWNGRADE_THRESHOLD: {}",
        DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst) as f64 / 100.
    );
    let tmp = std::env::var("DOWNGRADE_START_CHECK")
        .map(|x| x.parse::<usize>().unwrap_or(0))
        .unwrap_or(0);
    if tmp > 0 {
        DOWNGRADE_START_CHECK.store(tmp * 1000, Ordering::SeqCst);
    }
    log::info!(
        "DOWNGRADE_START_CHECK: {}s",
        DOWNGRADE_START_CHECK.load(Ordering::SeqCst) / 1000
    );
    let tmp = std::env::var("LIMIT_SPEED")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        LIMIT_SPEED.store((tmp * 1024. * 1024.) as usize, Ordering::SeqCst);
    }
    log::info!(
        "LIMIT_SPEED: {}Mb/s",
        LIMIT_SPEED.load(Ordering::SeqCst) as f64 / 1024. / 1024.
    );
    let tmp = std::env::var("TOTAL_BANDWIDTH")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        TOTAL_BANDWIDTH.store((tmp * 1024. * 1024.) as usize, Ordering::SeqCst);
    }

    log::info!(
        "TOTAL_BANDWIDTH: {}Mb/s",
        TOTAL_BANDWIDTH.load(Ordering::SeqCst) as f64 / 1024. / 1024.
    );
    let tmp = std::env::var("SINGLE_BANDWIDTH")
        .map(|x| x.parse::<f64>().unwrap_or(0.))
        .unwrap_or(0.);
    if tmp > 0. {
        SINGLE_BANDWIDTH.store((tmp * 1024. * 1024.) as usize, Ordering::SeqCst);
    }
    log::info!(
        "SINGLE_BANDWIDTH: {}Mb/s",
        SINGLE_BANDWIDTH.load(Ordering::SeqCst) as f64 / 1024. / 1024.
    )
}

async fn check_cmd(cmd: &str, limiter: Limiter) -> String {
    use std::fmt::Write;

    let mut res = "".to_owned();
    let mut fds = cmd.trim().split(' ');
    match fds.next() {
        Some("h") => {
            res = format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
                "blacklist-add(ba) <ip>",
                "blacklist-remove(br) <ip>",
                "blacklist(b) <ip>",
                "blocklist-add(Ba) <ip>",
                "blocklist-remove(Br) <ip>",
                "blocklist(B) <ip>",
                "downgrade-threshold(dt) [value]",
                "downgrade-start-check(t) [value(second)]",
                "limit-speed(ls) [value(Mb/s)]",
                "total-bandwidth(tb) [value(Mb/s)]",
                "single-bandwidth(sb) [value(Mb/s)]",
                "usage(u)"
            )
        }
        Some("blacklist-add" | "ba") => {
            if let Some(ip) = fds.next() {
                for ip in ip.split('|') {
                    BLACKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
        Some("blacklist-remove" | "br") => {
            if let Some(ip) = fds.next() {
                if ip == "all" {
                    BLACKLIST.write().await.clear();
                } else {
                    for ip in ip.split('|') {
                        BLACKLIST.write().await.remove(ip);
                    }
                }
            }
        }
        Some("blacklist" | "b") => {
            if let Some(ip) = fds.next() {
                res = format!("{}\n", BLACKLIST.read().await.get(ip).is_some());
            } else {
                for ip in BLACKLIST.read().await.clone().into_iter() {
                    let _ = writeln!(res, "{ip}");
                }
            }
        }
        Some("blocklist-add" | "Ba") => {
            if let Some(ip) = fds.next() {
                for ip in ip.split('|') {
                    BLOCKLIST.write().await.insert(ip.to_owned());
                }
            }
        }
        Some("blocklist-remove" | "Br") => {
            if let Some(ip) = fds.next() {
                if ip == "all" {
                    BLOCKLIST.write().await.clear();
                } else {
                    for ip in ip.split('|') {
                        BLOCKLIST.write().await.remove(ip);
                    }
                }
            }
        }
        Some("blocklist" | "B") => {
            if let Some(ip) = fds.next() {
                res = format!("{}\n", BLOCKLIST.read().await.get(ip).is_some());
            } else {
                for ip in BLOCKLIST.read().await.clone().into_iter() {
                    let _ = writeln!(res, "{ip}");
                }
            }
        }
        Some("downgrade-threshold" | "dt") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v > 0. {
                        DOWNGRADE_THRESHOLD_100.store((v * 100.) as _, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!(
                    "{}\n",
                    DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst) as f64 / 100.
                );
            }
        }
        Some("downgrade-start-check" | "t") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<usize>() {
                    if v > 0 {
                        DOWNGRADE_START_CHECK.store(v * 1000, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!("{}s\n", DOWNGRADE_START_CHECK.load(Ordering::SeqCst) / 1000);
            }
        }
        Some("limit-speed" | "ls") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v > 0. {
                        LIMIT_SPEED.store((v * 1024. * 1024.) as _, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!(
                    "{}Mb/s\n",
                    LIMIT_SPEED.load(Ordering::SeqCst) as f64 / 1024. / 1024.
                );
            }
        }
        Some("total-bandwidth" | "tb") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v > 0. {
                        TOTAL_BANDWIDTH.store((v * 1024. * 1024.) as _, Ordering::SeqCst);
                        limiter.set_speed_limit(TOTAL_BANDWIDTH.load(Ordering::SeqCst) as _);
                    }
                }
            } else {
                res = format!(
                    "{}Mb/s\n",
                    TOTAL_BANDWIDTH.load(Ordering::SeqCst) as f64 / 1024. / 1024.
                );
            }
        }
        Some("single-bandwidth" | "sb") => {
            if let Some(v) = fds.next() {
                if let Ok(v) = v.parse::<f64>() {
                    if v > 0. {
                        SINGLE_BANDWIDTH.store((v * 1024. * 1024.) as _, Ordering::SeqCst);
                    }
                }
            } else {
                res = format!(
                    "{}Mb/s\n",
                    SINGLE_BANDWIDTH.load(Ordering::SeqCst) as f64 / 1024. / 1024.
                );
            }
        }
        Some("usage" | "u") => {
            let mut tmp: Vec<(String, Usage)> = USAGE
                .read()
                .await
                .iter()
                .map(|x| (x.0.clone(), *x.1))
                .collect();
            tmp.sort_by(|a, b| ((b.1).1).partial_cmp(&(a.1).1).unwrap());
            for (ip, (elapsed, total, highest, speed)) in tmp {
                if elapsed == 0 {
                    continue;
                }
                let _ = writeln!(
                    res,
                    "{}: {}s {:.2}MB {}kb/s {}kb/s {}kb/s",
                    ip,
                    elapsed / 1000,
                    total as f64 / 1024. / 1024. / 8.,
                    highest,
                    total / elapsed,
                    speed
                );
            }
        }
        _ => {}
    }
    res
}

async fn io_loop(listener: TcpListener, listener2: TcpListener, key: &str) {
    check_params();
    let limiter = <Limiter>::new(TOTAL_BANDWIDTH.load(Ordering::SeqCst) as _);
    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((stream, addr))  => {
                        stream.set_nodelay(true).ok();
                        handle_connection(stream, addr, &limiter, key, false).await;
                    }
                    Err(err) => {
                       log::error!("listener.accept failed: {}", err);
                       break;
                    }
                }
            }
            res = listener2.accept() => {
                match res {
                    Ok((stream, addr))  => {
                        stream.set_nodelay(true).ok();
                        handle_connection(stream, addr, &limiter, key, true).await;
                    }
                    Err(err) => {
                       log::error!("listener2.accept failed: {}", err);
                       break;
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    limiter: &Limiter,
    key: &str,
    ws: bool,
) {
    let ip = hbb_common::try_into_v4(addr).ip();
    if !ws && ip.is_loopback() {
        let limiter = limiter.clone();
        tokio::spawn(async move {
            let mut stream = stream;
            let mut buffer = [0; 1024];
            if let Ok(Ok(n)) = timeout(1000, stream.read(&mut buffer[..])).await {
                if let Ok(data) = std::str::from_utf8(&buffer[..n]) {
                    let res = check_cmd(data, limiter).await;
                    stream.write(res.as_bytes()).await.ok();
                }
            }
        });
        return;
    }
    let ip = ip.to_string();
    if BLOCKLIST.read().await.get(&ip).is_some() {
        log::info!("{} blocked", ip);
        return;
    }
    let key = key.to_owned();
    let limiter = limiter.clone();
    tokio::spawn(async move {
        allow_err!(make_pair(stream, addr, &key, limiter, ws).await);
    });
}

async fn make_pair(
    stream: TcpStream,
    mut addr: SocketAddr,
    key: &str,
    limiter: Limiter,
    ws: bool,
) -> ResultType<()> {
    if ws {
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
        let callback = |req: &Request, response: Response| {
            let headers = req.headers();
            let real_ip = headers
                .get("X-Real-IP")
                .or_else(|| headers.get("X-Forwarded-For"))
                .and_then(|header_value| header_value.to_str().ok());
            if let Some(ip) = real_ip {
                if ip.contains('.') {
                    addr = format!("{ip}:0").parse().unwrap_or(addr);
                } else {
                    addr = format!("[{ip}]:0").parse().unwrap_or(addr);
                }
            }
            Ok(response)
        };
        let ws_stream = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
        make_pair_(ws_stream, addr, key, limiter).await;
    } else {
        make_pair_(FramedStream::from(stream, addr), addr, key, limiter).await;
    }
    Ok(())
}

async fn make_pair_(stream: impl StreamTrait, addr: SocketAddr, key: &str, limiter: Limiter) {
    let mut stream = stream;
    if let Ok(Some(Ok(bytes))) = timeout(30_000, stream.recv()).await {
        if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
            if let Some(rendezvous_message::Union::RequestRelay(rf)) = msg_in.union {
                if !key.is_empty() && rf.licence_key != key {
                    log::warn!("Relay authentication failed from {} - invalid key", addr);
                    return;
                }
                if !rf.uuid.is_empty() {
                    let mut peer = PEERS.lock().await.remove(&rf.uuid);
                    if let Some(peer) = peer.as_mut() {
                        log::info!("Relayrequest {} from {} got paired", rf.uuid, addr);
                        let id = format!("{}:{}", addr.ip(), addr.port());
                        USAGE.write().await.insert(id.clone(), Default::default());
                        if !stream.is_ws() && !peer.is_ws() {
                            peer.set_raw();
                            stream.set_raw();
                            log::info!("Both are raw");
                        }
                        if let Err(err) = relay(addr, &mut stream, peer, limiter, id.clone()).await
                        {
                            log::info!("Relay of {} closed: {}", addr, err);
                        } else {
                            log::info!("Relay of {} closed", addr);
                        }
                        USAGE.write().await.remove(&id);
                    } else {
                        log::info!("New relay request {} from {}", rf.uuid, addr);
                        PEERS.lock().await.insert(rf.uuid.clone(), Box::new(stream));
                        sleep(30.).await;
                        PEERS.lock().await.remove(&rf.uuid);
                    }
                }
            }
        }
    }
}

async fn relay(
    addr: SocketAddr,
    stream: &mut impl StreamTrait,
    peer: &mut Box<dyn StreamTrait>,
    total_limiter: Limiter,
    id: String,
) -> ResultType<()> {
    let ip = addr.ip().to_string();
    let mut tm = std::time::Instant::now();
    let mut elapsed = 0;
    let mut total = 0;
    let mut total_s = 0;
    let mut highest_s = 0;
    let mut downgrade: bool = false;
    let mut blacked: bool = false;
    let sb = SINGLE_BANDWIDTH.load(Ordering::SeqCst) as f64;
    let limiter = <Limiter>::new(sb);
    let blacklist_limiter = <Limiter>::new(LIMIT_SPEED.load(Ordering::SeqCst) as _);
    let downgrade_threshold =
        (sb * DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst) as f64 / 100. / 1000.) as usize; // in bit/ms
    let mut timer = interval(Duration::from_secs(3));
    let mut last_recv_time = std::time::Instant::now();
    loop {
        tokio::select! {
            res = peer.recv() => {
                if let Some(Ok(bytes)) = res {
                    last_recv_time = std::time::Instant::now();
                    let nb = bytes.len() * 8;
                    if blacked || downgrade {
                        blacklist_limiter.consume(nb).await;
                    } else {
                        limiter.consume(nb).await;
                    }
                    total_limiter.consume(nb).await;
                    total += nb;
                    total_s += nb;
                    if !bytes.is_empty() {
                        stream.send_raw(bytes.into()).await?;
                    }
                } else {
                    break;
                }
            },
            res = stream.recv() => {
                if let Some(Ok(bytes)) = res {
                    last_recv_time = std::time::Instant::now();
                    let nb = bytes.len() * 8;
                    if blacked || downgrade {
                        blacklist_limiter.consume(nb).await;
                    } else {
                        limiter.consume(nb).await;
                    }
                    total_limiter.consume(nb).await;
                    total += nb;
                    total_s += nb;
                    if !bytes.is_empty() {
                        peer.send_raw(bytes.into()).await?;
                    }
                } else {
                    break;
                }
            },
            _ = timer.tick() => {
                if last_recv_time.elapsed().as_secs() > 30 {
                    bail!("Timeout");
                }
            }
        }

        let n = tm.elapsed().as_millis() as usize;
        if n >= 1_000 {
            if BLOCKLIST.read().await.get(&ip).is_some() {
                log::info!("{} blocked", ip);
                break;
            }
            blacked = BLACKLIST.read().await.get(&ip).is_some();
            tm = std::time::Instant::now();
            let speed = total_s / n;
            if speed > highest_s {
                highest_s = speed;
            }
            elapsed += n;
            USAGE.write().await.insert(
                id.clone(),
                (elapsed as _, total as _, highest_s as _, speed as _),
            );
            total_s = 0;
            if elapsed > DOWNGRADE_START_CHECK.load(Ordering::SeqCst)
                && !downgrade
                && total > elapsed * downgrade_threshold
            {
                downgrade = true;
                log::info!(
                    "Downgrade {}, exceed downgrade threshold {}bit/ms in {}ms",
                    id,
                    downgrade_threshold,
                    elapsed
                );
            }
        }
    }
    Ok(())
}

fn get_server_sk(key: &str) -> String {
    let mut key = key.to_owned();
    if let Ok(sk) = base64::decode(&key) {
        if sk.len() == sign::SECRETKEYBYTES {
            log::info!("The key is a crypto private key");
            key = base64::encode(&sk[(sign::SECRETKEYBYTES / 2)..]);
        }
    }

    if key == "-" || key == "_" {
        let (pk, _) = crate::common::gen_sk(300);
        key = pk;
    }

    if !key.is_empty() {
        log::info!("Key: {}", key);
    }

    key
}

#[async_trait]
trait StreamTrait: Send + Sync + 'static {
    async fn recv(&mut self) -> Option<Result<BytesMut, Error>>;
    async fn send_raw(&mut self, bytes: Bytes) -> ResultType<()>;
    fn is_ws(&self) -> bool;
    fn set_raw(&mut self);
}

#[async_trait]
impl StreamTrait for FramedStream {
    async fn recv(&mut self) -> Option<Result<BytesMut, Error>> {
        self.next().await
    }

    async fn send_raw(&mut self, bytes: Bytes) -> ResultType<()> {
        self.send_bytes(bytes).await
    }

    fn is_ws(&self) -> bool {
        false
    }

    fn set_raw(&mut self) {
        self.set_raw();
    }
}

#[async_trait]
impl StreamTrait for tokio_tungstenite::WebSocketStream<TcpStream> {
    async fn recv(&mut self) -> Option<Result<BytesMut, Error>> {
        if let Some(msg) = self.next().await {
            match msg {
                Ok(msg) => {
                    match msg {
                        tungstenite::Message::Binary(bytes) => {
                            Some(Ok(bytes[..].into())) // to-do: poor performance
                        }
                        _ => Some(Ok(BytesMut::new())),
                    }
                }
                Err(err) => Some(Err(Error::new(std::io::ErrorKind::Other, err.to_string()))),
            }
        } else {
            None
        }
    }

    async fn send_raw(&mut self, bytes: Bytes) -> ResultType<()> {
        Ok(self
            .send(tungstenite::Message::Binary(bytes.to_vec()))
            .await?) // to-do: poor performance
    }

    fn is_ws(&self) -> bool {
        true
    }

    fn set_raw(&mut self) {}
}

#[cfg(feature = "integration-test")]
pub struct TestRelayServer {
    pub port: u16,
    pub ws_port: u16,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

#[cfg(feature = "integration-test")]
impl TestRelayServer {
    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
    }

    pub async fn start(key: &str) -> hbb_common::ResultType<Self> {
        let key = get_server_sk(key);
        let listener = hbb_common::tcp::listen_any(0).await?;
        let port = listener.local_addr()?.port();
        let listener2 = hbb_common::tcp::listen_any(0).await?;
        let ws_port = listener2.local_addr()?.port();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let key = key.to_owned();

        tokio::spawn(async move {
            tokio::select! {
                _ = io_loop(listener, listener2, &key) => {}
                _ = &mut shutdown_rx => {}
            }
        });

        Ok(TestRelayServer {
            port,
            ws_port,
            shutdown: shutdown_tx,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_limiter() -> Limiter {
        <Limiter>::new(TOTAL_BANDWIDTH.load(Ordering::SeqCst) as _)
    }

    mod check_cmd_tests {
        use super::*;

        #[tokio::test]
        async fn test_help() {
            let result = check_cmd("h", default_limiter()).await;
            assert!(result.contains("blacklist"));
            assert!(result.contains("blocklist"));
            assert!(result.contains("usage"));
        }

        #[tokio::test]
        async fn test_blacklist_add_and_check() {
            check_cmd("ba 192.168.88.1", default_limiter()).await;
            let result = check_cmd("b 192.168.88.1", default_limiter()).await;
            assert!(result.contains("true"));
            check_cmd("br 192.168.88.1", default_limiter()).await;
        }

        #[tokio::test]
        async fn test_blacklist_check_missing() {
            let result = check_cmd("b 192.168.88.200", default_limiter()).await;
            assert!(result.contains("false"));
        }

        #[tokio::test]
        async fn test_blacklist_remove() {
            check_cmd("ba 192.168.88.2", default_limiter()).await;
            check_cmd("br 192.168.88.2", default_limiter()).await;
            let result = check_cmd("b 192.168.88.2", default_limiter()).await;
            assert!(result.contains("false"));
        }

        #[tokio::test]
        async fn test_blacklist_list_contains_added() {
            check_cmd("ba 10.99.88.1", default_limiter()).await;
            let result = check_cmd("b", default_limiter()).await;
            assert!(result.contains("10.99.88.1"));
            check_cmd("br 10.99.88.1", default_limiter()).await;
        }

        #[tokio::test]
        async fn test_blocklist_add_and_check() {
            // Use unique IPs to avoid races with parallel tests
            check_cmd("Ba 172.16.99.1", default_limiter()).await;
            let result = check_cmd("B 172.16.99.1", default_limiter()).await;
            assert!(result.contains("true"));
            // Cleanup
            check_cmd("Br 172.16.99.1", default_limiter()).await;
        }

        #[tokio::test]
        async fn test_blocklist_remove() {
            check_cmd("Ba 172.16.99.2", default_limiter()).await;
            check_cmd("Br 172.16.99.2", default_limiter()).await;
            let result = check_cmd("B 172.16.99.2", default_limiter()).await;
            assert!(result.contains("false"));
        }

        #[tokio::test]
        async fn test_blocklist_list_after_add() {
            check_cmd("Ba 10.98.99.1", default_limiter()).await;
            let result = check_cmd("B", default_limiter()).await;
            assert!(result.contains("10.98.99.1"));
            check_cmd("Br 10.98.99.1", default_limiter()).await;
        }

        #[tokio::test]
        async fn test_downgrade_threshold_query() {
            let result = check_cmd("dt", default_limiter()).await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_downgrade_threshold_set() {
            let old = DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst);
            let result = check_cmd("dt 0.8", default_limiter()).await;
            assert!(result.is_empty());
            DOWNGRADE_THRESHOLD_100.store(old, Ordering::SeqCst);
        }

        #[tokio::test]
        async fn test_downgrade_start_check_query() {
            let result = check_cmd("t", default_limiter()).await;
            assert!(result.contains("s"));
        }

        #[tokio::test]
        async fn test_downgrade_start_check_set() {
            let old = DOWNGRADE_START_CHECK.load(Ordering::SeqCst);
            check_cmd("t 3600", default_limiter()).await;
            assert_eq!(DOWNGRADE_START_CHECK.load(Ordering::SeqCst), 3_600_000);
            DOWNGRADE_START_CHECK.store(old, Ordering::SeqCst);
        }

        #[tokio::test]
        async fn test_limit_speed_query() {
            let result = check_cmd("ls", default_limiter()).await;
            assert!(result.contains("Mb/s"));
        }

        #[tokio::test]
        async fn test_limit_speed_set() {
            let old = LIMIT_SPEED.load(Ordering::SeqCst);
            check_cmd("ls 10.0", default_limiter()).await;
            assert_eq!(LIMIT_SPEED.load(Ordering::SeqCst), (10.0 * 1024. * 1024.) as usize);
            LIMIT_SPEED.store(old, Ordering::SeqCst);
        }

        #[tokio::test]
        async fn test_total_bandwidth_query() {
            let result = check_cmd("tb", default_limiter()).await;
            assert!(result.contains("Mb/s"));
        }

        #[tokio::test]
        async fn test_single_bandwidth_query() {
            let result = check_cmd("sb", default_limiter()).await;
            assert!(result.contains("Mb/s"));
        }

        #[tokio::test]
        async fn test_usage_with_data() {
            USAGE.write().await.insert("10.77.0.1".to_owned(), (5000, 8000, 100, 50));
            let result = check_cmd("u", default_limiter()).await;
            assert!(result.contains("10.77.0.1"));
            USAGE.write().await.remove("10.77.0.1");
        }

        #[tokio::test]
        async fn test_usage_skips_zero_elapsed() {
            USAGE.write().await.insert("10.77.0.2".to_owned(), (0, 8000, 100, 50));
            let result = check_cmd("u", default_limiter()).await;
            // elapsed=0 entries are skipped
            assert!(!result.contains("10.77.0.2"));
            USAGE.write().await.remove("10.77.0.2");
        }

        #[tokio::test]
        async fn test_unknown_command() {
            let result = check_cmd("xyz", default_limiter()).await;
            assert!(result.is_empty());
        }
    }

    mod get_server_sk_tests {
        use super::*;

        #[test]
        fn test_valid_secret_key() {
            let (pk, sk) = sign::gen_keypair();
            let sk_b64 = base64::encode(&sk);
            let key = get_server_sk(&sk_b64);
            assert_eq!(key, base64::encode(pk));
        }

        #[test]
        fn test_non_crypto_key_passthrough() {
            let key = get_server_sk("plain_text_key");
            assert_eq!(key, "plain_text_key");
        }

        #[test]
        fn test_short_base64_not_crypto() {
            let short = base64::encode(b"short");
            let key = get_server_sk(&short);
            assert_eq!(key, short);
        }
    }

    #[test]
    fn test_check_params_no_panic() {
        // check_params reads env vars and sets atomics
        // Just verify it doesn't panic with default env
        check_params();
    }

    #[test]
    fn test_check_params_with_env_vars() {
        let old_dt = DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst);
        let old_dsc = DOWNGRADE_START_CHECK.load(Ordering::SeqCst);
        let old_ls = LIMIT_SPEED.load(Ordering::SeqCst);
        let old_tb = TOTAL_BANDWIDTH.load(Ordering::SeqCst);
        let old_sb = SINGLE_BANDWIDTH.load(Ordering::SeqCst);

        std::env::set_var("DOWNGRADE_THRESHOLD", "0.75");
        std::env::set_var("DOWNGRADE_START_CHECK", "2400");
        std::env::set_var("LIMIT_SPEED", "8.0");
        std::env::set_var("TOTAL_BANDWIDTH", "512.0");
        std::env::set_var("SINGLE_BANDWIDTH", "32.0");

        check_params();

        assert_eq!(DOWNGRADE_THRESHOLD_100.load(Ordering::SeqCst), 75);
        assert_eq!(DOWNGRADE_START_CHECK.load(Ordering::SeqCst), 2_400_000);
        assert_eq!(LIMIT_SPEED.load(Ordering::SeqCst), (8.0 * 1024. * 1024.) as usize);
        assert_eq!(TOTAL_BANDWIDTH.load(Ordering::SeqCst), (512.0 * 1024. * 1024.) as usize);
        assert_eq!(SINGLE_BANDWIDTH.load(Ordering::SeqCst), (32.0 * 1024. * 1024.) as usize);

        // Restore
        DOWNGRADE_THRESHOLD_100.store(old_dt, Ordering::SeqCst);
        DOWNGRADE_START_CHECK.store(old_dsc, Ordering::SeqCst);
        LIMIT_SPEED.store(old_ls, Ordering::SeqCst);
        TOTAL_BANDWIDTH.store(old_tb, Ordering::SeqCst);
        SINGLE_BANDWIDTH.store(old_sb, Ordering::SeqCst);

        std::env::remove_var("DOWNGRADE_THRESHOLD");
        std::env::remove_var("DOWNGRADE_START_CHECK");
        std::env::remove_var("LIMIT_SPEED");
        std::env::remove_var("TOTAL_BANDWIDTH");
        std::env::remove_var("SINGLE_BANDWIDTH");
    }

    mod check_cmd_extra_tests {
        use super::*;

        #[tokio::test]
        async fn test_blacklist_add_multiple_pipe() {
            check_cmd("ba 10.195.0.1|10.195.0.2|10.195.0.3", default_limiter()).await;
            let r1 = check_cmd("b 10.195.0.1", default_limiter()).await;
            let r2 = check_cmd("b 10.195.0.2", default_limiter()).await;
            let r3 = check_cmd("b 10.195.0.3", default_limiter()).await;
            assert!(r1.contains("true"));
            assert!(r2.contains("true"));
            assert!(r3.contains("true"));
            check_cmd("br 10.195.0.1|10.195.0.2|10.195.0.3", default_limiter()).await;
        }

        #[tokio::test]
        async fn test_total_bandwidth_set() {
            let old = TOTAL_BANDWIDTH.load(Ordering::SeqCst);
            check_cmd("tb 256.0", default_limiter()).await;
            assert_eq!(TOTAL_BANDWIDTH.load(Ordering::SeqCst), (256.0 * 1024. * 1024.) as usize);
            TOTAL_BANDWIDTH.store(old, Ordering::SeqCst);
        }

        #[tokio::test]
        async fn test_single_bandwidth_set() {
            let old = SINGLE_BANDWIDTH.load(Ordering::SeqCst);
            check_cmd("sb 64.0", default_limiter()).await;
            assert_eq!(SINGLE_BANDWIDTH.load(Ordering::SeqCst), (64.0 * 1024. * 1024.) as usize);
            SINGLE_BANDWIDTH.store(old, Ordering::SeqCst);
        }

        #[tokio::test]
        async fn test_blocklist_add_multiple_pipe() {
            check_cmd("Ba 10.194.0.1|10.194.0.2", default_limiter()).await;
            let r1 = check_cmd("B 10.194.0.1", default_limiter()).await;
            let r2 = check_cmd("B 10.194.0.2", default_limiter()).await;
            assert!(r1.contains("true"));
            assert!(r2.contains("true"));
            check_cmd("Br 10.194.0.1|10.194.0.2", default_limiter()).await;
        }

        #[tokio::test]
        async fn test_blocklist_remove_multiple_pipe() {
            check_cmd("Ba 10.193.0.1|10.193.0.2", default_limiter()).await;
            check_cmd("Br 10.193.0.1|10.193.0.2", default_limiter()).await;
            let r1 = check_cmd("B 10.193.0.1", default_limiter()).await;
            assert!(r1.contains("false"));
        }

        #[tokio::test]
        async fn test_blacklist_remove_multiple_pipe() {
            check_cmd("ba 10.192.0.1|10.192.0.2", default_limiter()).await;
            check_cmd("br 10.192.0.1|10.192.0.2", default_limiter()).await;
            let r1 = check_cmd("b 10.192.0.1", default_limiter()).await;
            assert!(r1.contains("false"));
        }

        #[tokio::test]
        async fn test_limit_speed_set_specific() {
            let old = LIMIT_SPEED.load(Ordering::SeqCst);
            check_cmd("ls 2.5", default_limiter()).await;
            assert_eq!(LIMIT_SPEED.load(Ordering::SeqCst), (2.5 * 1024. * 1024.) as usize);
            LIMIT_SPEED.store(old, Ordering::SeqCst);
        }
    }
}
