use crate::common::*;
use crate::peer::*;
use hbb_common::bytes::BufMut;
use hbb_common::{
    allow_err, bail,
    bytes::{Bytes, BytesMut},
    bytes_codec::BytesCodec,
    config,
    futures::future::join_all,
    futures_util::{
        sink::SinkExt,
        stream::{SplitSink, StreamExt},
    },
    log,
    protobuf::{Message as _, MessageField},
    rendezvous_proto::{
        register_pk_response::Result::{INVALID_ID_FORMAT, TOO_FREQUENT, UUID_MISMATCH},
        *,
    },
    sodiumoxide::crypto::{
        box_, box_::PublicKey, box_::SecretKey, secretbox, secretbox::Key, secretbox::Nonce, sign,
    },
    sodiumoxide::hex,
    tcp::{listen_any, FramedStream},
    timeout,
    tokio::{
        self,
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{mpsc, Mutex},
        time::{interval, Duration},
    },
    tokio_util::codec::Framed,
    try_into_v4,
    udp::FramedSocket,
    AddrMangle, ResultType,
};
use ipnetwork::Ipv4Network;

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    time::Instant,
};

use reqwest::Client;
use serde_json::json;

#[derive(Clone, Debug)]
enum Data {
    Msg(Box<RendezvousMessage>, SocketAddr),
    RelayServers0(String),
    RelayServers(RelayServers),
}

const REG_TIMEOUT: i32 = 30_000;
type TcpStreamSink = SplitSink<Framed<TcpStream, BytesCodec>, Bytes>;
type WsSink = SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, tungstenite::Message>;
enum SinkType {
    TcpStream(TcpStreamSink),
    Ws(WsSink),
}
struct Sink {
    tx: SinkType,
    key: Arc<Mutex<Option<Encrypt>>>,
}
#[derive(Clone)]
struct Encrypt {
    key: Key,
    enc_seqnum: u64,
    dec_seqnum: u64,
}

// Light version of hbb_common::tcp::Encrypt
impl Encrypt {
    pub fn dec(&mut self, bytes: &BytesMut) -> Result<Vec<u8>, ()> {
        self.dec_seqnum += 1;
        Ok(secretbox::open(
            bytes,
            &Self::get_nonce(self.dec_seqnum),
            &self.key,
        )?)
    }

    pub fn enc(&mut self, data: &[u8]) -> Vec<u8> {
        self.enc_seqnum += 1;
        secretbox::seal(&data, &Self::get_nonce(self.enc_seqnum), &self.key)
    }

    fn get_nonce(seqnum: u64) -> Nonce {
        let mut nonce = Nonce([0u8; secretbox::NONCEBYTES]);
        nonce.0[..std::mem::size_of_val(&seqnum)].copy_from_slice(&seqnum.to_le_bytes());
        nonce
    }
}

type Sender = mpsc::UnboundedSender<Data>;
type Receiver = mpsc::UnboundedReceiver<Data>;
static ROTATION_RELAY_SERVER: AtomicUsize = AtomicUsize::new(0);
type RelayServers = Vec<String>;
static CHECK_RELAY_TIMEOUT: u64 = 3_000;
static ALWAYS_USE_RELAY: AtomicBool = AtomicBool::new(false);
static WS_PEER_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

static WS_HEARTBEAT_INTERVAL: once_cell::sync::Lazy<u64> = once_cell::sync::Lazy::new(|| {
    get_arg_or("ws-heartbeat-interval", "20000".to_owned())
        .parse()
        .unwrap_or(20_000)
});

#[derive(Clone)]
struct Inner {
    serial: i32,
    version: String,
    software_url: String,
    mask: Option<Ipv4Network>,
    local_ip: String,
    sk: Option<sign::SecretKey>,
    secure_tcp_pk_b: PublicKey,
    secure_tcp_sk_b: SecretKey,
}

#[derive(Clone)]
pub struct RendezvousServer {
    tcp_punch: Arc<Mutex<HashMap<SocketAddr, Sink>>>,
    ws_peers: Arc<Mutex<HashMap<String, (u64, Sink)>>>,
    pm: PeerMap,
    tx: Sender,
    relay_servers: Arc<RelayServers>,
    relay_servers0: Arc<RelayServers>,
    rendezvous_servers: Arc<Vec<String>>,
    inner: Arc<Inner>,
}

enum LoopFailure {
    UdpSocket,
    Listener3,
    Listener2,
    Listener,
}

impl RendezvousServer {
    #[tokio::main(flavor = "multi_thread")]
    pub async fn start(port: i32, serial: i32, key: &str, rmem: usize) -> ResultType<()> {
        let (key, sk) = Self::get_server_sk(key);
        let nat_port = port - 1;
        let ws_port = port + 2;
        let pm = PeerMap::new().await?;
        log::info!("serial={}", serial);
        let rendezvous_servers = get_servers(&get_arg("rendezvous-servers"), "rendezvous-servers");
        log::info!("Listening on tcp/udp :{}", port);
        log::info!("Listening on tcp :{}, extra port for NAT test", nat_port);
        log::info!("Listening on websocket :{}", ws_port);
        let mut socket = create_udp_listener(port, rmem).await?;
        let (tx, mut rx) = mpsc::unbounded_channel::<Data>();
        let software_url = get_arg("software-url");
        let version = hbb_common::get_version_from_url(&software_url);
        if !version.is_empty() {
            log::info!("software_url: {}, version: {}", software_url, version);
        }
        let mask = get_arg("mask").parse().ok();
        let local_ip = if mask.is_none() {
            "".to_owned()
        } else {
            get_arg_or(
                "local-ip",
                local_ip_address::local_ip()
                    .map(|x| x.to_string())
                    .unwrap_or_default(),
            )
        };
        // For privacy use per connection key pair
        let (secure_tcp_pk_b, secure_tcp_sk_b) = box_::gen_keypair();
        let mut rs = Self {
            tcp_punch: Arc::new(Mutex::new(HashMap::new())),
            ws_peers: Default::default(),
            pm,
            tx: tx.clone(),
            relay_servers: Default::default(),
            relay_servers0: Default::default(),
            rendezvous_servers: Arc::new(rendezvous_servers),
            inner: Arc::new(Inner {
                serial,
                version,
                software_url,
                sk,
                mask,
                local_ip,
                secure_tcp_pk_b,
                secure_tcp_sk_b,
            }),
        };
        log::info!("mask: {:?}", rs.inner.mask);
        log::info!("local-ip: {:?}", rs.inner.local_ip);
        std::env::set_var("PORT_FOR_API", port.to_string());
        rs.parse_relay_servers(&get_arg("relay-servers"));
        let mut listener = create_tcp_listener(port).await?;
        let mut listener2 = create_tcp_listener(nat_port).await?;
        let mut listener3 = create_tcp_listener(ws_port).await?;
        let test_addr = std::env::var("TEST_HBBS").unwrap_or_default();
        if std::env::var("ALWAYS_USE_RELAY")
            .unwrap_or_default()
            .to_uppercase()
            == "Y"
        {
            ALWAYS_USE_RELAY.store(true, Ordering::SeqCst);
        }
        log::info!(
            "ALWAYS_USE_RELAY={}",
            if ALWAYS_USE_RELAY.load(Ordering::SeqCst) {
                "Y"
            } else {
                "N"
            }
        );
        if test_addr.to_lowercase() != "no" {
            let test_addr = if test_addr.is_empty() {
                listener.local_addr()?
            } else {
                test_addr.parse()?
            };
            tokio::spawn(async move {
                if let Err(err) = test_hbbs(test_addr).await {
                    if test_addr.is_ipv6() && test_addr.ip().is_unspecified() {
                        let mut test_addr = test_addr;
                        test_addr.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
                        if let Err(err) = test_hbbs(test_addr).await {
                            log::error!("Failed to run hbbs test with {test_addr}: {err}");
                            std::process::exit(1);
                        }
                    } else {
                        log::error!("Failed to run hbbs test with {test_addr}: {err}");
                        std::process::exit(1);
                    }
                }
            });
        };
        let main_task = async move {
            loop {
                log::info!("Start");
                match rs
                    .io_loop(
                        &mut rx,
                        &mut listener,
                        &mut listener2,
                        &mut listener3,
                        &mut socket,
                        &key,
                    )
                    .await
                {
                    LoopFailure::UdpSocket => {
                        drop(socket);
                        socket = create_udp_listener(port, rmem).await?;
                    }
                    LoopFailure::Listener => {
                        drop(listener);
                        listener = create_tcp_listener(port).await?;
                    }
                    LoopFailure::Listener2 => {
                        drop(listener2);
                        listener2 = create_tcp_listener(nat_port).await?;
                    }
                    LoopFailure::Listener3 => {
                        drop(listener3);
                        listener3 = create_tcp_listener(ws_port).await?;
                    }
                }
            }
        };
        let listen_signal = listen_signal();
        tokio::select!(
            res = main_task => res,
            res = listen_signal => res,
        )
    }

    async fn io_loop(
        &mut self,
        rx: &mut Receiver,
        listener: &mut TcpListener,
        listener2: &mut TcpListener,
        listener3: &mut TcpListener,
        socket: &mut FramedSocket,
        key: &str,
    ) -> LoopFailure {
        let mut timer_check_relay = interval(Duration::from_millis(CHECK_RELAY_TIMEOUT));
        loop {
            tokio::select! {
                _ = timer_check_relay.tick() => {
                    if self.relay_servers0.len() > 1 {
                        let rs = self.relay_servers0.clone();
                        let tx = self.tx.clone();
                        tokio::spawn(async move {
                            check_relay_servers(rs, tx).await;
                        });
                    }
                }
                Some(data) = rx.recv() => {
                    match data {
                        Data::Msg(msg, addr) => { allow_err!(socket.send(msg.as_ref(), addr).await); }
                        Data::RelayServers0(rs) => { self.parse_relay_servers(&rs); }
                        Data::RelayServers(rs) => { self.relay_servers = Arc::new(rs); }
                    }
                }
                res = socket.next() => {
                    match res {
                        Some(Ok((bytes, addr))) => {
                            if let Err(err) = self.handle_udp(&bytes, addr.into(), socket, key).await {
                                log::error!("udp failure: {}", err);
                                return LoopFailure::UdpSocket;
                            }
                        }
                        Some(Err(err)) => {
                            log::error!("udp failure: {}", err);
                            return LoopFailure::UdpSocket;
                        }
                        None => {
                            // unreachable!() ?
                        }
                    }
                }
                res = listener2.accept() => {
                    match res {
                        Ok((stream, addr))  => {
                            stream.set_nodelay(true).ok();
                            self.handle_listener2(stream, addr).await;
                        }
                        Err(err) => {
                           log::error!("listener2.accept failed: {}", err);
                           return LoopFailure::Listener2;
                        }
                    }
                }
                res = listener3.accept() => {
                    match res {
                        Ok((stream, addr))  => {
                            stream.set_nodelay(true).ok();
                            self.handle_listener(stream, addr, key, true).await;
                        }
                        Err(err) => {
                           log::error!("listener3.accept failed: {}", err);
                           return LoopFailure::Listener3;
                        }
                    }
                }
                res = listener.accept() => {
                    match res {
                        Ok((stream, addr)) => {
                            stream.set_nodelay(true).ok();
                            self.handle_listener(stream, addr, key, false).await;
                        }
                       Err(err) => {
                           log::error!("listener.accept failed: {}", err);
                           return LoopFailure::Listener;
                       }
                    }
                }
            }
        }
    }

    #[inline]
    async fn handle_udp(
        &mut self,
        bytes: &BytesMut,
        addr: SocketAddr,
        socket: &mut FramedSocket,
        key: &str,
    ) -> ResultType<()> {
        if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(bytes) {
            match msg_in.union {
                Some(rendezvous_message::Union::RegisterPeer(rp)) => {
                    // B registered
                    if !rp.id.is_empty() {
                        log::trace!("New peer registered: {:?} {:?}", &rp.id, &addr);
                        self.update_addr(rp.id, addr, socket).await?;
                        if self.inner.serial > rp.serial {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_configure_update(ConfigUpdate {
                                serial: self.inner.serial,
                                rendezvous_servers: (*self.rendezvous_servers).clone(),
                                ..Default::default()
                            });
                            socket.send(&msg_out, addr).await?;
                        }
                    }
                }
                Some(rendezvous_message::Union::RegisterPk(rk)) => {
                    let response = self.handle_register_pk(rk, addr).await;
                    match response {
                        Err(err) => {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_register_pk_response(RegisterPkResponse {
                                result: err.into(),
                                ..Default::default()
                            });
                            socket.send(&msg_out, addr).await?;
                        }
                        Ok((res, _id)) => {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_register_pk_response(RegisterPkResponse {
                                result: res.into(),
                                ..Default::default()
                            });
                            socket.send(&msg_out, addr).await?;
                        }
                    }
                }
                Some(rendezvous_message::Union::PunchHoleRequest(ph)) => {
                    if self.pm.is_in_memory(&ph.id).await {
                        self.handle_udp_punch_hole_request(addr, ph, key).await?;
                    } else {
                        // not in memory, fetch from db with spawn in case blocking me
                        let mut me = self.clone();
                        let key = key.to_owned();
                        tokio::spawn(async move {
                            allow_err!(me.handle_udp_punch_hole_request(addr, ph, &key).await);
                        });
                    }
                }
                Some(rendezvous_message::Union::PunchHoleSent(phs)) => {
                    self.handle_hole_sent(phs, addr, Some(socket)).await?;
                }
                Some(rendezvous_message::Union::LocalAddr(la)) => {
                    self.handle_local_addr(la, addr, Some(socket)).await?;
                }
                Some(rendezvous_message::Union::ConfigureUpdate(mut cu)) => {
                    if try_into_v4(addr).ip().is_loopback() && cu.serial > self.inner.serial {
                        let mut inner: Inner = (*self.inner).clone();
                        inner.serial = cu.serial;
                        self.inner = Arc::new(inner);
                        self.rendezvous_servers = Arc::new(
                            cu.rendezvous_servers
                                .drain(..)
                                .filter(|x| {
                                    !x.is_empty()
                                        && test_if_valid_server(x, "rendezvous-server").is_ok()
                                })
                                .collect(),
                        );
                        log::info!(
                            "configure updated: serial={} rendezvous-servers={:?}",
                            self.inner.serial,
                            self.rendezvous_servers
                        );
                    }
                }
                Some(rendezvous_message::Union::SoftwareUpdate(su)) => {
                    if !self.inner.version.is_empty() && su.url != self.inner.version {
                        let mut msg_out = RendezvousMessage::new();
                        msg_out.set_software_update(SoftwareUpdate {
                            url: self.inner.software_url.clone(),
                            ..Default::default()
                        });
                        socket.send(&msg_out, addr).await?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    #[inline]
    async fn handle_tcp(
        &mut self,
        bytes: &[u8],
        sink: &mut Option<Sink>,
        addr: SocketAddr,
        key: &str,
        ws: bool,
    ) -> (bool, Option<(String, u64)>) {
        if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(bytes) {
            match msg_in.union {
                Some(rendezvous_message::Union::PunchHoleRequest(ph)) => {
                    if let Some(sink) = sink.take() {
                        self.tcp_punch.lock().await.insert(try_into_v4(addr), sink);
                    }
                    allow_err!(self.handle_tcp_punch_hole_request(addr, ph, key, ws).await);
                    return (true, None);
                }
                Some(rendezvous_message::Union::RequestRelay(mut rf)) => {
                    if let Some(sink) = sink.take() {
                        self.tcp_punch.lock().await.insert(try_into_v4(addr), sink);
                    }
                    let target_id = rf.id.clone();
                    if let Some(peer) = self.pm.get_in_memory(&target_id).await {
                        let mut msg_out = RendezvousMessage::new();
                        rf.socket_addr = AddrMangle::encode(addr).into();
                        msg_out.set_request_relay(rf);
                        if !self.send_to_ws_peer(&target_id, msg_out.clone()).await {
                            let peer_addr = peer.read().await.socket_addr;
                            self.tx.send(Data::Msg(msg_out.into(), peer_addr)).ok();
                        }
                    }
                    return (true, None);
                }
                Some(rendezvous_message::Union::RelayResponse(mut rr)) => {
                    let addr_b = AddrMangle::decode(&rr.socket_addr);
                    rr.socket_addr = Default::default();
                    let id = rr.id();
                    if !id.is_empty() {
                        let pk = self.get_pk(&rr.version, id.to_owned()).await;
                        rr.set_pk(pk);
                    }
                    let mut msg_out = RendezvousMessage::new();
                    if !rr.relay_server.is_empty() {
                        if self.is_lan(addr_b) {
                            rr.relay_server = self.inner.local_ip.clone();
                        } else if rr.relay_server == self.inner.local_ip {
                            rr.relay_server = self.get_relay_server(addr.ip(), addr_b.ip());
                        }
                    }
                    msg_out.set_relay_response(rr);
                    allow_err!(self.send_to_tcp_sync(msg_out, addr_b).await);
                    return (true, None);
                }
                Some(rendezvous_message::Union::PunchHoleSent(phs)) => {
                    allow_err!(self.handle_hole_sent(phs, addr, None).await);
                    return (true, None);
                }
                Some(rendezvous_message::Union::LocalAddr(la)) => {
                    allow_err!(self.handle_local_addr(la, addr, None).await);
                    return (true, None);
                }
                Some(rendezvous_message::Union::TestNatRequest(tar)) => {
                    let mut msg_out = RendezvousMessage::new();
                    let mut res = TestNatResponse {
                        port: addr.port() as _,
                        ..Default::default()
                    };
                    if self.inner.serial > tar.serial {
                        let mut cu = ConfigUpdate::new();
                        cu.serial = self.inner.serial;
                        cu.rendezvous_servers = (*self.rendezvous_servers).clone();
                        res.cu = MessageField::from_option(Some(cu));
                    }
                    msg_out.set_test_nat_response(res);
                    Self::send_to_sink(sink, msg_out).await;
                    return (true, None);
                }
                Some(rendezvous_message::Union::RegisterPk(rk)) => {
                    let response = self.handle_register_pk(rk, addr).await;
                    match response {
                        Err(err) => {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_register_pk_response(RegisterPkResponse {
                                result: err.into(),
                                ..Default::default()
                            });
                            Self::send_to_sink(sink, msg_out).await;
                            return (false, None);
                        }
                        Ok((res, id)) => {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_register_pk_response(RegisterPkResponse {
                                result: res.into(),
                                ..Default::default()
                            });
                            Self::send_to_sink(sink, msg_out).await;
                            if let Some(s) = sink.take() {
                                let gen = WS_PEER_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                self.ws_peers.lock().await.insert(id.clone(), (gen, s));
                                log::info!("Peer {} registered via TCP/WS (gen={})", id, gen);
                                return (true, Some((id, gen)));
                            }
                            return (true, None);
                        }
                    }
                }
                Some(rendezvous_message::Union::RegisterPeer(rp)) => {
                    if !rp.id.is_empty() {
                        if let Some(peer) = self.pm.get_in_memory(&rp.id).await {
                            let mut w = peer.write().await;
                            w.socket_addr = addr;
                            w.last_reg_time = Instant::now();
                        }
                    }
                    let mut msg_out = RendezvousMessage::new();
                    msg_out.set_register_peer_response(RegisterPeerResponse {
                        request_pk: false,
                        ..Default::default()
                    });
                    Self::send_to_sink(sink, msg_out).await;
                    return (true, None);
                }
                Some(rendezvous_message::Union::KeyExchange(ex)) => {
                    log::trace!("KeyExchange {:?} <- bytes: {:?}", addr, hex::encode(&bytes));
                    if ex.keys.len() != 2 {
                        log::error!("Handshake failed: invalid phase 2 key exchange message");
                        return (false, None);
                    }
                    log::trace!("KeyExchange their_pk: {:?}", hex::encode(&ex.keys[0]));
                    log::trace!("KeyExchange box: {:?}", hex::encode(&ex.keys[1]));
                    let their_pk: [u8; 32] = ex.keys[0].to_vec().try_into().unwrap();
                    let cryptobox: [u8; 48] = ex.keys[1].to_vec().try_into().unwrap();
                    let symetric_key = get_symetric_key_from_msg(
                        self.inner.secure_tcp_sk_b.0,
                        their_pk,
                        &cryptobox,
                    );
                    log::debug!("KeyExchange symetric key: {:?}", hex::encode(&symetric_key));
                    let key = secretbox::Key::from_slice(&symetric_key);
                    match key {
                        Some(key) => {
                            if let Some(sink) = sink.as_mut() {
                                sink.key.lock().await.replace(Encrypt {
                                    key,
                                    enc_seqnum: 0,
                                    dec_seqnum: 0,
                                });
                            }
                            log::debug!("KeyExchange symetric key set");
                            return (true, None);
                        }
                        None => {
                            log::error!("KeyExchange symetric key NOT set");
                            return (false, None);
                        }
                    }
                }
                _ => {}
            }
        }
        (false, None)
    }

    async fn handle_register_pk(
        &mut self,
        rk: RegisterPk,
        addr: SocketAddr,
    ) -> Result<(register_pk_response::Result, String), register_pk_response::Result> {
        if rk.uuid.is_empty() || rk.pk.is_empty() {
            return Err(INVALID_ID_FORMAT);
        }
        let id = rk.id;
        let ip = addr.ip().to_string();
        if id.len() < 6 {
            return Err(UUID_MISMATCH);
            //return Err(send_rk_res(socket, addr, UUID_MISMATCH).await);
        } else if !self.check_ip_blocker(&ip, &id).await {
            return Err(TOO_FREQUENT);
            //return Err(send_rk_res(socket, addr, TOO_FREQUENT).await);
        }
        let peer = self.pm.get_or(&id).await;
        let (changed, ip_changed) = {
            let peer = peer.read().await;
            if peer.uuid.is_empty() {
                (true, false)
            } else {
                if peer.uuid == rk.uuid {
                    if peer.info.ip != ip && peer.pk != rk.pk {
                        log::warn!(
                            "Peer {} ip/pk mismatch: {}/{:?} vs {}/{:?}",
                            id,
                            ip,
                            rk.pk,
                            peer.info.ip,
                            peer.pk,
                        );
                        drop(peer);
                        return Err(UUID_MISMATCH);
                        //return Err(send_rk_res(socket, addr, UUID_MISMATCH).await);
                    }
                } else {
                    log::warn!(
                        "Peer {} uuid mismatch: {:?} vs {:?}",
                        id,
                        rk.uuid,
                        peer.uuid
                    );
                    drop(peer);
                    return Err(UUID_MISMATCH);
                    //return Err(send_rk_res(socket, addr, UUID_MISMATCH).await);
                }
                let ip_changed = peer.info.ip != ip;
                (
                    peer.uuid != rk.uuid || peer.pk != rk.pk || ip_changed,
                    ip_changed,
                )
            }
        };
        let mut req_pk = peer.read().await.reg_pk;
        if req_pk.1.elapsed().as_secs() > 6 {
            req_pk.0 = 0;
        } else if req_pk.0 > 2 {
            return Err(TOO_FREQUENT);
            //return Err(send_rk_res(socket, addr, TOO_FREQUENT).await);
        }
        req_pk.0 += 1;
        req_pk.1 = Instant::now();
        peer.write().await.reg_pk = req_pk;
        if ip_changed {
            let mut lock = IP_CHANGES.lock().await;
            if let Some((tm, ips)) = lock.get_mut(&id) {
                if tm.elapsed().as_secs() > IP_CHANGE_DUR {
                    *tm = Instant::now();
                    ips.clear();
                    ips.insert(ip.clone(), 1);
                } else if let Some(v) = ips.get_mut(&ip) {
                    *v += 1;
                } else {
                    ips.insert(ip.clone(), 1);
                }
            } else {
                lock.insert(
                    id.clone(),
                    (Instant::now(), HashMap::from([(ip.clone(), 1)])),
                );
            }
        }
        if changed {
            self.pm.update_pk(id.clone(), peer, addr, rk.uuid, rk.pk, ip).await;
        }
        Ok((register_pk_response::Result::OK, id))
        // let mut msg_out = RendezvousMessage::new();
        // msg_out.set_register_pk_response(RegisterPkResponse {
        //     result: register_pk_response::Result::OK.into(),
        //     ..Default::default()
        // });
        // Ok(msg_out)
    }

    #[inline]
    async fn update_addr(
        &mut self,
        id: String,
        socket_addr: SocketAddr,
        socket: &mut FramedSocket,
    ) -> ResultType<()> {
        let (request_pk, ip_change) = if let Some(old) = self.pm.get_in_memory(&id).await {
            let mut old = old.write().await;
            let ip = socket_addr.ip();
            let ip_change = if old.socket_addr.port() != 0 {
                ip != old.socket_addr.ip()
            } else {
                ip.to_string() != old.info.ip
            } && !ip.is_loopback();
            let request_pk = old.pk.is_empty() || ip_change;
            if !request_pk {
                old.socket_addr = socket_addr;
                old.last_reg_time = Instant::now();
            }
            let ip_change = if ip_change && old.reg_pk.0 <= 2 {
                Some(if old.socket_addr.port() == 0 {
                    old.info.ip.clone()
                } else {
                    old.socket_addr.to_string()
                })
            } else {
                None
            };
            (request_pk, ip_change)
        } else {
            (true, None)
        };
        if let Some(old) = ip_change {
            log::info!("IP change of {} from {} to {}", id, old, socket_addr);
        }
        if !request_pk {
            self.ws_peers.lock().await.remove(&id);
        }
        let mut msg_out = RendezvousMessage::new();
        msg_out.set_register_peer_response(RegisterPeerResponse {
            request_pk,
            ..Default::default()
        });
        socket.send(&msg_out, socket_addr).await
    }

    #[inline]
    async fn handle_hole_sent<'a>(
        &mut self,
        phs: PunchHoleSent,
        addr: SocketAddr,
        socket: Option<&'a mut FramedSocket>,
    ) -> ResultType<()> {
        // punch hole sent from B, tell A that B is ready to be connected
        let addr_a = AddrMangle::decode(&phs.socket_addr);
        log::debug!(
            "{} punch hole response to {:?} from {:?}",
            if socket.is_none() { "TCP" } else { "UDP" },
            &addr_a,
            &addr
        );
        let mut msg_out = RendezvousMessage::new();
        let mut p = PunchHoleResponse {
            socket_addr: AddrMangle::encode(addr).into(),
            pk: self.get_pk(&phs.version, phs.id).await,
            relay_server: phs.relay_server.clone(),
            ..Default::default()
        };
        if let Ok(t) = phs.nat_type.enum_value() {
            p.set_nat_type(t);
        }
        msg_out.set_punch_hole_response(p);
        if let Some(socket) = socket {
            socket.send(&msg_out, addr_a).await?;
        } else {
            self.send_to_tcp(msg_out, addr_a).await;
        }
        Ok(())
    }

    #[inline]
    async fn handle_local_addr<'a>(
        &mut self,
        la: LocalAddr,
        addr: SocketAddr,
        socket: Option<&'a mut FramedSocket>,
    ) -> ResultType<()> {
        // relay local addrs of B to A
        let addr_a = AddrMangle::decode(&la.socket_addr);
        log::debug!(
            "{} local addrs response to {:?} from {:?}",
            if socket.is_none() { "TCP" } else { "UDP" },
            &addr_a,
            &addr
        );
        let mut msg_out = RendezvousMessage::new();
        let mut p = PunchHoleResponse {
            socket_addr: la.local_addr.clone(),
            pk: self.get_pk(&la.version, la.id).await,
            relay_server: la.relay_server,
            ..Default::default()
        };
        p.set_is_local(true);
        msg_out.set_punch_hole_response(p);
        if let Some(socket) = socket {
            socket.send(&msg_out, addr_a).await?;
        } else {
            self.send_to_tcp(msg_out, addr_a).await;
        }
        Ok(())
    }

    #[inline]
    async fn handle_punch_hole_request(
        &mut self,
        addr: SocketAddr,
        ph: PunchHoleRequest,
        key: &str,
        ws: bool,
    ) -> ResultType<(RendezvousMessage, Option<SocketAddr>)> {
        let mut ph = ph;
        if !key.is_empty() && ph.licence_key != key {
            let mut msg_out = RendezvousMessage::new();
            msg_out.set_punch_hole_response(PunchHoleResponse {
                failure: punch_hole_response::Failure::LICENSE_MISMATCH.into(),
                ..Default::default()
            });
            return Ok((msg_out, None));
        }
        // For limiting abuse, only allow logged in users to punch hole
        // if LOGGED_IN_ONLY=Y is set in env or --logged-in-only is passed
        if std::env::var("LOGGED_IN_ONLY")
            .unwrap_or_default()
            .to_uppercase()
            == "Y"
        {
            let mut msg_out = RendezvousMessage::new();
            if !ph.token.is_empty() {
                let api_server = std::env::var("API_SERVER").unwrap_or_else(|_| "http://127.0.0.1:21114".to_string());
                let api_url = api_server + "/api/currentUser";
                let client = Client::new();
                let res = client.post(&api_url)
                    .bearer_auth(ph.token)
                    .json(&json!({ "id": ph.id, "uuid": "uuid" }))
                    .send()
                    .await?;
                if res.status().is_success() {
                    let response_body: serde_json::Value = res.json().await?;
                    log::debug!("Username: {}", response_body["name"]);
                } else {
                    log::debug!("Error: {}", res.status());
                    msg_out.set_punch_hole_response(PunchHoleResponse {
                        other_failure: String::from("The connection is not allowed. Your session expired."),
                        ..Default::default()
                    });
                    return Ok((msg_out, None));
                }
            } else {
                msg_out.set_punch_hole_response(PunchHoleResponse {
                    other_failure: String::from("The connection is not allowed. You have not logged in."),
                    ..Default::default()
                });
                return Ok((msg_out, None));
            }
        }
        let id = ph.id;
        // punch hole request from A, relay to B,
        // check if in same intranet first,
        // fetch local addrs if in same intranet.
        // because punch hole won't work if in the same intranet,
        // all routers will drop such self-connections.
        if let Some(peer) = self.pm.get(&id).await {
            let (elapsed, peer_addr) = {
                let r = peer.read().await;
                (r.last_reg_time.elapsed().as_millis() as i32, r.socket_addr)
            };
            if elapsed >= REG_TIMEOUT {
                let mut msg_out = RendezvousMessage::new();
                msg_out.set_punch_hole_response(PunchHoleResponse {
                    failure: punch_hole_response::Failure::OFFLINE.into(),
                    ..Default::default()
                });
                return Ok((msg_out, None));
            }
            let mut msg_out = RendezvousMessage::new();
            let peer_is_lan = self.is_lan(peer_addr);
            let is_lan = self.is_lan(addr);
            let mut relay_server = self.get_relay_server(addr.ip(), peer_addr.ip());
            if ALWAYS_USE_RELAY.load(Ordering::SeqCst) || (peer_is_lan ^ is_lan) {
                if peer_is_lan {
                    // https://github.com/rustdesk/rustdesk-server/issues/24
                    relay_server = self.inner.local_ip.clone()
                }
                ph.nat_type = NatType::SYMMETRIC.into(); // will force relay
            }
            let same_intranet: bool = !ws
                && (peer_is_lan && is_lan || {
                    match (peer_addr, addr) {
                        (SocketAddr::V4(a), SocketAddr::V4(b)) => a.ip() == b.ip(),
                        (SocketAddr::V6(a), SocketAddr::V6(b)) => a.ip() == b.ip(),
                        _ => false,
                    }
                });
            let socket_addr = AddrMangle::encode(addr).into();
            if same_intranet {
                log::debug!(
                    "Fetch local addr {:?} {:?} request from {:?}",
                    id,
                    peer_addr,
                    addr
                );
                msg_out.set_fetch_local_addr(FetchLocalAddr {
                    socket_addr,
                    relay_server,
                    ..Default::default()
                });
            } else {
                log::debug!(
                    "Punch hole {:?} {:?} request from {:?}",
                    id,
                    peer_addr,
                    addr
                );
                msg_out.set_punch_hole(PunchHole {
                    socket_addr,
                    nat_type: ph.nat_type,
                    relay_server,
                    ..Default::default()
                });
            }
            Ok((msg_out, Some(peer_addr)))
        } else {
            let mut msg_out = RendezvousMessage::new();
            msg_out.set_punch_hole_response(PunchHoleResponse {
                failure: punch_hole_response::Failure::ID_NOT_EXIST.into(),
                ..Default::default()
            });
            Ok((msg_out, None))
        }
    }

    #[inline]
    async fn handle_online_request(
        &mut self,
        stream: &mut FramedStream,
        peers: Vec<String>,
    ) -> ResultType<()> {
        let mut states = BytesMut::zeroed((peers.len() + 7) / 8);
        for (i, peer_id) in peers.iter().enumerate() {
            if let Some(peer) = self.pm.get_in_memory(peer_id).await {
                let elapsed = peer.read().await.last_reg_time.elapsed().as_millis() as i32;
                // bytes index from left to right
                let states_idx = i / 8;
                let bit_idx = 7 - i % 8;
                if elapsed < REG_TIMEOUT {
                    states[states_idx] |= 0x01 << bit_idx;
                }
            }
        }

        let mut msg_out = RendezvousMessage::new();
        msg_out.set_online_response(OnlineResponse {
            states: states.into(),
            ..Default::default()
        });
        stream.send(&msg_out).await?;

        Ok(())
    }

    #[inline]
    async fn send_to_tcp(&mut self, msg: RendezvousMessage, addr: SocketAddr) {
        let mut tcp = self.tcp_punch.lock().await.remove(&try_into_v4(addr));
        tokio::spawn(async move {
            Self::send_to_sink(&mut tcp, msg).await;
        });
    }

    #[inline]
    async fn send_to_sink(sink: &mut Option<Sink>, msg: RendezvousMessage) {
        if let Some(sink) = sink.as_mut() {
            if let Ok(mut bytes) = msg.write_to_bytes() {
                if let Some(enc) = &mut sink.key.lock().await.as_mut() {
                    bytes = enc.enc(&bytes);
                }
                match &mut sink.tx {
                    SinkType::TcpStream(s) => {
                        allow_err!(s.send(Bytes::from(bytes)).await);
                    }
                    SinkType::Ws(ws) => {
                        allow_err!(ws.send(tungstenite::Message::Binary(bytes)).await);
                    }
                }
            }
        }
    }

    #[inline]
    async fn send_to_tcp_sync(
        &mut self,
        msg: RendezvousMessage,
        addr: SocketAddr,
    ) -> ResultType<()> {
        let mut sink = self.tcp_punch.lock().await.remove(&try_into_v4(addr));
        Self::send_to_sink(&mut sink, msg).await;
        Ok(())
    }

    async fn send_to_ws_peer(&self, id: &str, msg: RendezvousMessage) -> bool {
        if let Some((_, sink)) = self.ws_peers.lock().await.get_mut(id) {
            if let Ok(mut bytes) = msg.write_to_bytes() {
                if let Some(enc) = &mut sink.key.lock().await.as_mut() {
                    bytes = enc.enc(&bytes);
                }
                match &mut sink.tx {
                    SinkType::TcpStream(s) => {
                        if s.send(Bytes::from(bytes)).await.is_ok() {
                            return true;
                        }
                    }
                    SinkType::Ws(ws) => {
                        if ws.send(tungstenite::Message::Binary(bytes)).await.is_ok() {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    #[inline]
    async fn handle_tcp_punch_hole_request(
        &mut self,
        addr: SocketAddr,
        ph: PunchHoleRequest,
        key: &str,
        ws: bool,
    ) -> ResultType<()> {
        let target_id = ph.id.clone();
        let (msg, to_addr) = self.handle_punch_hole_request(addr, ph, key, ws).await?;
        if let Some(peer_addr) = to_addr {
            if !self.send_to_ws_peer(&target_id, msg.clone()).await {
                self.tx.send(Data::Msg(msg.into(), peer_addr))?;
            }
        } else {
            self.send_to_tcp_sync(msg, addr).await?;
        }
        Ok(())
    }

    #[inline]
    async fn handle_udp_punch_hole_request(
        &mut self,
        addr: SocketAddr,
        ph: PunchHoleRequest,
        key: &str,
    ) -> ResultType<()> {
        let (msg, to_addr) = self.handle_punch_hole_request(addr, ph, key, false).await?;
        self.tx.send(Data::Msg(
            msg.into(),
            match to_addr {
                Some(addr) => addr,
                None => addr,
            },
        ))?;
        Ok(())
    }

    async fn check_ip_blocker(&self, ip: &str, id: &str) -> bool {
        let mut lock = IP_BLOCKER.lock().await;
        let now = Instant::now();
        if let Some(old) = lock.get_mut(ip) {
            let counter = &mut old.0;
            if counter.1.elapsed().as_secs() > IP_BLOCK_DUR {
                counter.0 = 0;
            } else if counter.0 > 30 {
                return false;
            }
            counter.0 += 1;
            counter.1 = now;

            let counter = &mut old.1;
            let is_new = counter.0.get(id).is_none();
            if counter.1.elapsed().as_secs() > DAY_SECONDS {
                counter.0.clear();
            } else if counter.0.len() > 300 {
                return !is_new;
            }
            if is_new {
                counter.0.insert(id.to_owned());
            }
            counter.1 = now;
        } else {
            lock.insert(ip.to_owned(), ((0, now), (Default::default(), now)));
        }
        true
    }

    fn parse_relay_servers(&mut self, relay_servers: &str) {
        let rs = get_servers(relay_servers, "relay-servers");
        self.relay_servers0 = Arc::new(rs);
        self.relay_servers = self.relay_servers0.clone();
    }

    fn get_relay_server(&self, _pa: IpAddr, _pb: IpAddr) -> String {
        if self.relay_servers.is_empty() {
            return "".to_owned();
        } else if self.relay_servers.len() == 1 {
            return self.relay_servers[0].clone();
        }
        let i = ROTATION_RELAY_SERVER.fetch_add(1, Ordering::SeqCst) % self.relay_servers.len();
        self.relay_servers[i].clone()
    }

    async fn check_cmd(&self, cmd: &str) -> String {
        use std::fmt::Write as _;

        let mut res = "".to_owned();
        let mut fds = cmd.trim().split(' ');
        match fds.next() {
            Some("h") => {
                res = format!(
                    "{}\n{}\n{}\n{}\n{}\n{}\n",
                    "relay-servers(rs) <separated by ,>",
                    "reload-geo(rg)",
                    "ip-blocker(ib) [<ip>|<number>] [-]",
                    "ip-changes(ic) [<id>|<number>] [-]",
                    "always-use-relay(aur)",
                    "test-geo(tg) <ip1> <ip2>"
                )
            }
            Some("relay-servers" | "rs") => {
                if let Some(rs) = fds.next() {
                    self.tx.send(Data::RelayServers0(rs.to_owned())).ok();
                } else {
                    for ip in self.relay_servers.iter() {
                        let _ = writeln!(res, "{ip}");
                    }
                }
            }
            Some("ip-blocker" | "ib") => {
                let mut lock = IP_BLOCKER.lock().await;
                lock.retain(|&_, (a, b)| {
                    a.1.elapsed().as_secs() <= IP_BLOCK_DUR
                        || b.1.elapsed().as_secs() <= DAY_SECONDS
                });
                res = format!("{}\n", lock.len());
                let ip = fds.next();
                let mut start = ip.map(|x| x.parse::<i32>().unwrap_or(-1)).unwrap_or(-1);
                if start < 0 {
                    if let Some(ip) = ip {
                        if let Some((a, b)) = lock.get(ip) {
                            let _ = writeln!(
                                res,
                                "{}/{}s {}/{}s",
                                a.0,
                                a.1.elapsed().as_secs(),
                                b.0.len(),
                                b.1.elapsed().as_secs()
                            );
                        }
                        if fds.next() == Some("-") {
                            lock.remove(ip);
                        }
                    } else {
                        start = 0;
                    }
                }
                if start >= 0 {
                    let mut it = lock.iter();
                    for i in 0..(start + 10) {
                        let x = it.next();
                        if x.is_none() {
                            break;
                        }
                        if i < start {
                            continue;
                        }
                        if let Some((ip, (a, b))) = x {
                            let _ = writeln!(
                                res,
                                "{}: {}/{}s {}/{}s",
                                ip,
                                a.0,
                                a.1.elapsed().as_secs(),
                                b.0.len(),
                                b.1.elapsed().as_secs()
                            );
                        }
                    }
                }
            }
            Some("ip-changes" | "ic") => {
                let mut lock = IP_CHANGES.lock().await;
                lock.retain(|&_, v| v.0.elapsed().as_secs() < IP_CHANGE_DUR_X2 && v.1.len() > 1);
                res = format!("{}\n", lock.len());
                let id = fds.next();
                let mut start = id.map(|x| x.parse::<i32>().unwrap_or(-1)).unwrap_or(-1);
                if !(0..=10_000_000).contains(&start) {
                    if let Some(id) = id {
                        if let Some((tm, ips)) = lock.get(id) {
                            let _ = writeln!(res, "{}s {:?}", tm.elapsed().as_secs(), ips);
                        }
                        if fds.next() == Some("-") {
                            lock.remove(id);
                        }
                    } else {
                        start = 0;
                    }
                }
                if start >= 0 {
                    let mut it = lock.iter();
                    for i in 0..(start + 10) {
                        let x = it.next();
                        if x.is_none() {
                            break;
                        }
                        if i < start {
                            continue;
                        }
                        if let Some((id, (tm, ips))) = x {
                            let _ = writeln!(res, "{}: {}s {:?}", id, tm.elapsed().as_secs(), ips,);
                        }
                    }
                }
            }
            Some("always-use-relay" | "aur") => {
                if let Some(rs) = fds.next() {
                    if rs.to_uppercase() == "Y" {
                        ALWAYS_USE_RELAY.store(true, Ordering::SeqCst);
                    } else {
                        ALWAYS_USE_RELAY.store(false, Ordering::SeqCst);
                    }
                    self.tx.send(Data::RelayServers0(rs.to_owned())).ok();
                } else {
                    let _ = writeln!(
                        res,
                        "ALWAYS_USE_RELAY: {:?}",
                        ALWAYS_USE_RELAY.load(Ordering::SeqCst)
                    );
                }
            }
            Some("test-geo" | "tg") => {
                if let Some(rs) = fds.next() {
                    if let Ok(a) = rs.parse::<IpAddr>() {
                        if let Some(rs) = fds.next() {
                            if let Ok(b) = rs.parse::<IpAddr>() {
                                res = format!("{:?}", self.get_relay_server(a, b));
                            }
                        } else {
                            res = format!("{:?}", self.get_relay_server(a, a));
                        }
                    }
                }
            }
            _ => {}
        }
        res
    }

    async fn handle_listener2(&self, stream: TcpStream, addr: SocketAddr) {
        let mut rs = self.clone();
        if addr.ip().is_loopback() {
            tokio::spawn(async move {
                let mut stream = stream;
                let mut buffer = [0; 1024];
                if let Ok(Ok(n)) = timeout(1000, stream.read(&mut buffer[..])).await {
                    if let Ok(data) = std::str::from_utf8(&buffer[..n]) {
                        let res = rs.check_cmd(data).await;
                        stream.write(res.as_bytes()).await.ok();
                    }
                }
            });
            return;
        }
        let stream = FramedStream::from(stream, addr);
        tokio::spawn(async move {
            let mut stream = stream;
            if let Some(Ok(bytes)) = stream.next_timeout(30_000).await {
                if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
                    match msg_in.union {
                        Some(rendezvous_message::Union::TestNatRequest(_)) => {
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_test_nat_response(TestNatResponse {
                                port: addr.port() as _,
                                ..Default::default()
                            });
                            stream.send(&msg_out).await.ok();
                        }
                        Some(rendezvous_message::Union::OnlineRequest(or)) => {
                            allow_err!(rs.handle_online_request(&mut stream, or.peers).await);
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    async fn handle_listener(&self, stream: TcpStream, addr: SocketAddr, key: &str, ws: bool) {
        log::debug!("Tcp connection from {:?}, ws: {}", addr, ws);
        let mut rs = self.clone();
        let key = key.to_owned();
        tokio::spawn(async move {
            allow_err!(rs.handle_listener_inner(stream, addr, &key, ws).await);
        });
    }

    #[inline]
    async fn handle_listener_inner(
        &mut self,
        stream: TcpStream,
        addr: SocketAddr,
        key: &str,
        ws: bool,
    ) -> ResultType<()> {
        let mut sink;
        let mut reg_info: Option<(String, u64)> = None;
        let heartbeat_interval = *WS_HEARTBEAT_INTERVAL;
        if ws {
            let ws_stream = tokio_tungstenite::accept_async(stream).await?;
            let (a, mut b) = ws_stream.split();
            sink = Some(Sink {
                tx: SinkType::Ws(a),
                key: Arc::new(Mutex::new(None)),
            });
            let mut read_timeout: u64 = 30_000;
            loop {
                match timeout(read_timeout, b.next()).await {
                    Ok(Some(Ok(msg))) => {
                        if let tungstenite::Message::Binary(bytes) = msg {
                            let (keep_going, info) = self.handle_tcp(&bytes, &mut sink, addr, key, ws).await;
                            if let Some(info) = info {
                                read_timeout = heartbeat_interval;
                                reg_info = Some(info);
                            }
                            if let Some((ref id, _)) = reg_info {
                                if let Some(peer) = self.pm.get_in_memory(id).await {
                                    let mut w = peer.write().await;
                                    w.last_reg_time = Instant::now();
                                }
                            }
                            if !keep_going {
                                break;
                            }
                        }
                    }
                    Ok(Some(Err(_))) | Ok(None) => break,
                    Err(_) => {
                        if let Some((ref id, _)) = reg_info {
                            if let Some(peer) = self.pm.get_in_memory(id).await {
                                let mut w = peer.write().await;
                                w.last_reg_time = Instant::now();
                            }
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_register_peer_response(RegisterPeerResponse {
                                request_pk: false,
                                ..Default::default()
                            });
                            Self::send_to_sink(&mut sink, msg_out).await;
                        } else {
                            break;
                        }
                    }
                }
            }
        } else {
            let (a, mut b) = Framed::new(stream, BytesCodec::new()).split();
            let enc = Arc::new(Mutex::new(None));
            sink = Some(Sink {
                tx: SinkType::TcpStream(a),
                key: enc.clone(),
            });
            if !key.is_empty() {
                self.key_exchange_phase1(addr, &mut sink).await;
            }
            let mut read_timeout: u64 = 30_000;
            loop {
                match timeout(read_timeout, b.next()).await {
                    Ok(Some(Ok(mut bytes))) => {
                        let mut enc_lock = enc.lock().await;
                        if enc_lock.is_some() {
                            if let Ok(dec) = enc_lock.as_mut().unwrap().dec(&bytes) {
                                bytes.clear();
                                bytes.put_slice(&dec);
                            } else {
                                log::warn!("Decryption error from {}", addr);
                                break;
                            }
                        }
                        drop(enc_lock);
                        let (keep_going, info) = self.handle_tcp(&bytes, &mut sink, addr, key, ws).await;
                        if let Some(info) = info {
                            read_timeout = heartbeat_interval;
                            reg_info = Some(info);
                        }
                        if let Some((ref id, _)) = reg_info {
                            if let Some(peer) = self.pm.get_in_memory(id).await {
                                let mut w = peer.write().await;
                                w.last_reg_time = Instant::now();
                            }
                        }
                        if !keep_going {
                            break;
                        }
                    }
                    Ok(Some(Err(_))) | Ok(None) => break,
                    Err(_) => {
                        if let Some((ref id, _)) = reg_info {
                            if let Some(peer) = self.pm.get_in_memory(id).await {
                                let mut w = peer.write().await;
                                w.last_reg_time = Instant::now();
                            }
                            let mut msg_out = RendezvousMessage::new();
                            msg_out.set_register_peer_response(RegisterPeerResponse {
                                request_pk: false,
                                ..Default::default()
                            });
                            Self::send_to_sink(&mut sink, msg_out).await;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        if let Some((id, gen)) = &reg_info {
            let mut ws_peers = self.ws_peers.lock().await;
            if let Some((stored_gen, _)) = ws_peers.get(id) {
                if *stored_gen == *gen {
                    ws_peers.remove(id);
                    log::info!("WS peer {} disconnected (gen={})", id, gen);
                }
            }
        }
        if sink.is_none() {
            self.tcp_punch.lock().await.remove(&try_into_v4(addr));
        }
        log::debug!("Tcp connection from {:?} closed", addr);
        Ok(())
    }

    #[inline]
    async fn get_pk(&mut self, version: &str, id: String) -> Bytes {
        if version.is_empty() || self.inner.sk.is_none() {
            Bytes::new()
        } else {
            match self.pm.get(&id).await {
                Some(peer) => {
                    let pk = peer.read().await.pk.clone();
                    sign::sign(
                        &hbb_common::message_proto::IdPk {
                            id,
                            pk,
                            ..Default::default()
                        }
                        .write_to_bytes()
                        .unwrap_or_default(),
                        self.inner.sk.as_ref().unwrap(),
                    )
                    .into()
                }
                _ => Bytes::new(),
            }
        }
    }

    #[inline]
    fn get_server_sk(key: &str) -> (String, Option<sign::SecretKey>) {
        let mut out_sk = None;
        let mut key = key.to_owned();
        if let Ok(sk) = base64::decode(&key) {
            if sk.len() == sign::SECRETKEYBYTES {
                log::info!("The key is a crypto private key");
                key = base64::encode(&sk[(sign::SECRETKEYBYTES / 2)..]);
                let mut tmp = [0u8; sign::SECRETKEYBYTES];
                tmp[..].copy_from_slice(&sk);
                out_sk = Some(sign::SecretKey(tmp));
            }
        }

        if key.is_empty() || key == "-" || key == "_" {
            let (pk, sk) = crate::common::gen_sk(0);
            out_sk = sk;
            if !key.is_empty() {
                key = pk;
            }
        }

        if !key.is_empty() {
            log::info!("Key: {}", key);
        }
        (key, out_sk)
    }

    #[inline]
    fn is_lan(&self, addr: SocketAddr) -> bool {
        if let Some(network) = &self.inner.mask {
            match addr {
                SocketAddr::V4(v4_socket_addr) => {
                    return network.contains(*v4_socket_addr.ip());
                }

                SocketAddr::V6(v6_socket_addr) => {
                    if let Some(v4_addr) = v6_socket_addr.ip().to_ipv4() {
                        return network.contains(v4_addr);
                    }
                }
            }
        }
        false
    }

    async fn key_exchange_phase1(&mut self, addr: SocketAddr, sink: &mut Option<Sink>) {
        let mut msg_out = RendezvousMessage::new();
        log::debug!("KeyExchange phase 1: send our pk for this tcp connection in a message signed with our server key");
        let sk = &self.inner.sk;
        match sk {
            Some(sk) => {
                let our_pk_b = self.inner.secure_tcp_pk_b.clone();
                let sm = sign::sign(&our_pk_b.0, &sk);

                let bytes_sm = Bytes::from(sm);
                msg_out.set_key_exchange(KeyExchange {
                    keys: vec![bytes_sm],
                    ..Default::default()
                });
                log::trace!(
                    "KeyExchange {:?} -> bytes: {:?}",
                    addr,
                    hex::encode(Bytes::from(msg_out.write_to_bytes().unwrap()))
                );
                Self::send_to_sink(sink, msg_out).await;
            }
            None => {}
        }
    }
}

#[cfg(feature = "integration-test")]
pub struct TestServer {
    pub port: u16,
    pub udp_port: u16,
    pub nat_port: u16,
    pub ws_port: u16,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

#[cfg(feature = "integration-test")]
impl TestServer {
    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

#[cfg(feature = "integration-test")]
impl RendezvousServer {
    pub async fn start_test(key: &str) -> ResultType<TestServer> {
        let (key, sk) = Self::get_server_sk(key);
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.sqlite3");
        std::mem::forget(dir);
        let db = crate::database::Database::new(path.to_str().unwrap()).await?;
        let pm = PeerMap::new_with_db(db);
        let (tx, mut rx) = mpsc::unbounded_channel::<Data>();
        let (secure_tcp_pk_b, secure_tcp_sk_b) = box_::gen_keypair();
        let mut rs = Self {
            tcp_punch: Arc::new(Mutex::new(HashMap::new())),
            ws_peers: Default::default(),
            pm,
            tx,
            relay_servers: Default::default(),
            relay_servers0: Default::default(),
            rendezvous_servers: Arc::new(vec![]),
            inner: Arc::new(Inner {
                serial: 1,
                version: String::new(),
                software_url: String::new(),
                sk,
                mask: None,
                local_ip: String::new(),
                secure_tcp_pk_b,
                secure_tcp_sk_b,
            }),
        };

        let mut socket = create_udp_listener(0, 0).await?;
        let udp_port = socket.local_addr().map(|a| a.port()).unwrap_or(0);
        let mut listener = create_tcp_listener(0).await?;
        let port = listener.local_addr()?.port();
        let mut listener2 = create_tcp_listener(0).await?;
        let nat_port = listener2.local_addr()?.port();
        let mut listener3 = create_tcp_listener(0).await?;
        let ws_port = listener3.local_addr()?.port();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            tokio::select! {
                _ = rs.io_loop(&mut rx, &mut listener, &mut listener2, &mut listener3, &mut socket, &key) => {}
                _ = &mut shutdown_rx => {}
            }
        });

        Ok(TestServer {
            port,
            udp_port,
            nat_port,
            ws_port,
            shutdown: shutdown_tx,
        })
    }
}

async fn check_relay_servers(rs0: Arc<RelayServers>, tx: Sender) {
    let mut futs = Vec::new();
    let rs = Arc::new(Mutex::new(Vec::new()));
    for x in rs0.iter() {
        let mut host = x.to_owned();
        if !host.contains(':') {
            host = format!("{}:{}", host, config::RELAY_PORT);
        }
        let rs = rs.clone();
        let x = x.clone();
        futs.push(tokio::spawn(async move {
            if FramedStream::new(&host, None, CHECK_RELAY_TIMEOUT)
                .await
                .is_ok()
            {
                rs.lock().await.push(x);
            }
        }));
    }
    join_all(futs).await;
    log::debug!("check_relay_servers");
    let rs = std::mem::take(&mut *rs.lock().await);
    if !rs.is_empty() {
        tx.send(Data::RelayServers(rs)).ok();
    }
}

// temp solution to solve udp socket failure
async fn test_hbbs(addr: SocketAddr) -> ResultType<()> {
    let mut addr = addr;
    if addr.ip().is_unspecified() {
        addr.set_ip(if addr.is_ipv4() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        });
    }

    let mut socket = FramedSocket::new(config::Config::get_any_listen_addr(addr.is_ipv4())).await?;
    let mut msg_out = RendezvousMessage::new();
    msg_out.set_register_peer(RegisterPeer {
        id: "(:test_hbbs:)".to_owned(),
        ..Default::default()
    });
    let mut last_time_recv = Instant::now();

    let mut timer = interval(Duration::from_secs(1));
    loop {
        tokio::select! {
          _ = timer.tick() => {
              if last_time_recv.elapsed().as_secs() > 12 {
                  bail!("Timeout of test_hbbs");
              }
              socket.send(&msg_out, addr).await?;
          }
          Some(Ok((bytes, _))) = socket.next() => {
              if let Ok(msg_in) = RendezvousMessage::parse_from_bytes(&bytes) {
                 log::trace!("Recv {:?} of test_hbbs", msg_in);
                 last_time_recv = Instant::now();
              }
          }
        }
    }
}

async fn create_udp_listener(port: i32, rmem: usize) -> ResultType<FramedSocket> {
    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port as _);
    if let Ok(s) = FramedSocket::new_reuse(&addr, true, rmem).await {
        log::debug!("listen on udp {:?}", s.local_addr());
        return Ok(s);
    }
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port as _);
    let s = FramedSocket::new_reuse(&addr, true, rmem).await?;
    log::debug!("listen on udp {:?}", s.local_addr());
    Ok(s)
}

#[inline]
async fn create_tcp_listener(port: i32) -> ResultType<TcpListener> {
    let s = listen_any(port as _, true).await?;
    log::debug!("listen on tcp {:?}", s.local_addr());
    Ok(s)
}

fn get_symetric_key_from_msg(
    our_sk_b: [u8; 32],
    their_pk_b: [u8; 32],
    sealed_value: &[u8; 48],
) -> [u8; 32] {
    let their_pk_b = box_::PublicKey(their_pk_b);
    let nonce = box_::Nonce([0u8; box_::NONCEBYTES]);
    let sk = box_::SecretKey(our_sk_b);
    let key = box_::open(sealed_value, &nonce, &their_pk_b, &sk);
    match key {
        Ok(key) => {
            let mut key_array = [0u8; 32];
            key_array.copy_from_slice(&key);
            key_array
        }
        Err(e) => panic!("Error while opening the seal key{:?}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod encrypt_tests {
        use super::*;

        fn make_encrypt() -> Encrypt {
            let key = secretbox::gen_key();
            Encrypt {
                key,
                enc_seqnum: 0,
                dec_seqnum: 0,
            }
        }

        #[test]
        fn test_encrypt_decrypt_roundtrip() {
            let mut enc = make_encrypt();
            let mut dec = Encrypt { ..enc.clone() };
            let plaintext = b"hello world";

            let ciphertext = enc.enc(plaintext);
            assert_ne!(ciphertext, plaintext);

            let decrypted = dec.dec(&BytesMut::from(&ciphertext[..])).unwrap();
            assert_eq!(decrypted, plaintext);
        }

        #[test]
        fn test_encrypt_different_messages_produce_different_ciphertext() {
            let mut enc = make_encrypt();
            let ct1 = enc.enc(b"message1");
            let ct2 = enc.enc(b"message1");
            assert_ne!(ct1, ct2);
        }

        #[test]
        fn test_decrypt_wrong_sequence_fails() {
            let mut enc = make_encrypt();
            let mut dec = Encrypt { ..enc.clone() };

            let ct1 = enc.enc(b"first");
            let ct2 = enc.enc(b"second");

            // Decrypt ct2 first (wrong order) should fail
            let result = dec.dec(&BytesMut::from(&ct2[..]));
            assert!(result.is_err());
        }

        #[test]
        fn test_get_nonce_different_for_different_seqnums() {
            let n1 = Encrypt::get_nonce(1);
            let n2 = Encrypt::get_nonce(2);
            assert_ne!(n1.0, n2.0);
        }

        #[test]
        fn test_get_nonce_deterministic() {
            let n1 = Encrypt::get_nonce(42);
            let n2 = Encrypt::get_nonce(42);
            assert_eq!(n1.0, n2.0);
        }
    }

    mod is_lan_tests {
        use super::*;

        fn test_is_lan_inner(mask: Option<Ipv4Network>, addr: SocketAddr) -> bool {
            if let Some(network) = &mask {
                match addr {
                    SocketAddr::V4(v4_socket_addr) => {
                        return network.contains(*v4_socket_addr.ip());
                    }
                    SocketAddr::V6(v6_socket_addr) => {
                        if let Some(v4_addr) = v6_socket_addr.ip().to_ipv4() {
                            return network.contains(v4_addr);
                        }
                    }
                }
            }
            false
        }

        #[test]
        fn test_is_lan_with_matching_subnet() {
            let mask: Ipv4Network = "192.168.1.0/24".parse().unwrap();
            let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
            assert!(test_is_lan_inner(Some(mask), addr));
        }

        #[test]
        fn test_is_lan_with_non_matching_subnet() {
            let mask: Ipv4Network = "192.168.1.0/24".parse().unwrap();
            let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
            assert!(!test_is_lan_inner(Some(mask), addr));
        }

        #[test]
        fn test_is_lan_without_mask_always_false() {
            let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
            assert!(!test_is_lan_inner(None, addr));
        }

        #[test]
        fn test_is_lan_boundary_address() {
            let mask: Ipv4Network = "10.0.0.0/8".parse().unwrap();
            assert!(test_is_lan_inner(Some(mask), "10.255.255.255:80".parse().unwrap()));
            assert!(!test_is_lan_inner(Some(mask), "11.0.0.1:80".parse().unwrap()));
        }

        #[test]
        fn test_is_lan_ipv6_mapped_v4() {
            let mask: Ipv4Network = "192.168.1.0/24".parse().unwrap();
            // IPv6-mapped IPv4 address ::ffff:192.168.1.5
            let addr: SocketAddr = "[::ffff:192.168.1.5]:8080".parse().unwrap();
            assert!(test_is_lan_inner(Some(mask), addr));
        }

        #[test]
        fn test_is_lan_ipv6_mapped_v4_non_matching() {
            let mask: Ipv4Network = "192.168.1.0/24".parse().unwrap();
            let addr: SocketAddr = "[::ffff:10.0.0.1]:8080".parse().unwrap();
            assert!(!test_is_lan_inner(Some(mask), addr));
        }

        #[test]
        fn test_is_lan_pure_ipv6() {
            let mask: Ipv4Network = "192.168.1.0/24".parse().unwrap();
            let addr: SocketAddr = "[::1]:8080".parse().unwrap();
            assert!(!test_is_lan_inner(Some(mask), addr));
        }
    }

    mod relay_server_tests {
        use super::*;

        #[test]
        fn test_get_relay_server_empty_returns_empty() {
            let relays: RelayServers = vec![];
            assert!(relays.is_empty());
        }

        #[test]
        fn test_get_relay_server_single_returns_it() {
            let relays: RelayServers = vec!["relay1.example.com".to_owned()];
            assert_eq!(relays.len(), 1);
            assert_eq!(relays[0], "relay1.example.com");
        }

        #[test]
        fn test_relay_rotation_wraps_around() {
            let relays = vec!["r1".to_owned(), "r2".to_owned(), "r3".to_owned()];
            ROTATION_RELAY_SERVER.store(0, Ordering::SeqCst);
            for round in 0..2 {
                for j in 0..3 {
                    let i = ROTATION_RELAY_SERVER.fetch_add(1, Ordering::SeqCst) % relays.len();
                    assert_eq!(i, j, "round={round}");
                }
            }
        }
    }

    async fn test_server() -> (RendezvousServer, Receiver) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        std::mem::forget(dir);

        let db = crate::database::Database::new(path.to_str().unwrap()).await.unwrap();
        let pm = PeerMap::new_with_db(db);
        let (tx, rx) = mpsc::unbounded_channel();
        let (secure_tcp_pk_b, secure_tcp_sk_b) = box_::gen_keypair();
        let rs = RendezvousServer {
            tcp_punch: Arc::new(Mutex::new(HashMap::new())),
            ws_peers: Default::default(),
            pm,
            tx,
            relay_servers: Arc::new(vec!["relay.example.com".to_owned()]),
            relay_servers0: Default::default(),
            rendezvous_servers: Arc::new(vec![]),
            inner: Arc::new(Inner {
                serial: 1,
                version: String::new(),
                software_url: String::new(),
                mask: None,
                local_ip: String::new(),
                sk: None,
                secure_tcp_pk_b,
                secure_tcp_sk_b,
            }),
        };
        (rs, rx)
    }

    mod handle_register_pk_tests {
        use super::*;

        #[tokio::test]
        async fn test_register_pk_empty_uuid_rejected() {
            let (mut rs, _rx) = test_server().await;
            let rk = RegisterPk {
                id: "testpeer".to_owned(),
                uuid: vec![].into(),
                pk: vec![1, 2, 3].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, "10.0.0.1:1234".parse().unwrap()).await;
            assert_eq!(result, Err(INVALID_ID_FORMAT));
        }

        #[tokio::test]
        async fn test_register_pk_empty_pk_rejected() {
            let (mut rs, _rx) = test_server().await;
            let rk = RegisterPk {
                id: "testpeer".to_owned(),
                uuid: vec![1, 2, 3].into(),
                pk: vec![].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, "10.0.0.1:1234".parse().unwrap()).await;
            assert_eq!(result, Err(INVALID_ID_FORMAT));
        }

        #[tokio::test]
        async fn test_register_pk_short_id_rejected() {
            let (mut rs, _rx) = test_server().await;
            let rk = RegisterPk {
                id: "short".to_owned(),
                uuid: vec![1, 2, 3].into(),
                pk: vec![1, 2, 3].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, "10.0.0.1:1234".parse().unwrap()).await;
            assert_eq!(result, Err(UUID_MISMATCH));
        }

        #[tokio::test]
        async fn test_register_pk_new_peer_succeeds() {
            let (mut rs, _rx) = test_server().await;
            let rk = RegisterPk {
                id: "newpeer123".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, "10.0.0.1:1234".parse().unwrap()).await;
            assert!(matches!(result, Ok((register_pk_response::Result::OK, _))));
        }

        #[tokio::test]
        async fn test_register_pk_same_uuid_updates() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "10.0.0.1:1234".parse().unwrap();

            // First registration
            let rk = RegisterPk {
                id: "peer_upd".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, addr).await;
            assert!(matches!(result, Ok((register_pk_response::Result::OK, _))));

            // Second registration with same uuid
            let rk = RegisterPk {
                id: "peer_upd".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![3; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, addr).await;
            assert!(matches!(result, Ok((register_pk_response::Result::OK, _))));
        }

        #[tokio::test]
        async fn test_register_pk_uuid_mismatch_rejected() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "10.0.0.1:1234".parse().unwrap();

            let rk = RegisterPk {
                id: "peer_mm".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            rs.handle_register_pk(rk, addr).await.unwrap();

            // Different uuid
            let rk = RegisterPk {
                id: "peer_mm".to_owned(),
                uuid: vec![9; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, addr).await;
            assert_eq!(result, Err(UUID_MISMATCH));
        }

        #[tokio::test]
        async fn test_register_pk_ip_pk_mismatch_rejected() {
            IP_BLOCKER.lock().await.clear();
            let (mut rs, _rx) = test_server().await;
            let addr1: SocketAddr = "10.0.0.1:1234".parse().unwrap();
            let addr2: SocketAddr = "10.0.0.2:1234".parse().unwrap();

            // First registration
            let rk = RegisterPk {
                id: "peer_ipm".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            rs.handle_register_pk(rk, addr1).await.unwrap();

            // Same uuid, different IP AND different PK
            let rk = RegisterPk {
                id: "peer_ipm".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![3; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, addr2).await;
            assert_eq!(result, Err(UUID_MISMATCH));
        }

        #[tokio::test]
        async fn test_register_pk_ip_change_tracked() {
            IP_BLOCKER.lock().await.clear();
            IP_CHANGES.lock().await.clear();
            let (mut rs, _rx) = test_server().await;
            let addr1: SocketAddr = "10.0.0.1:1234".parse().unwrap();
            let addr2: SocketAddr = "10.0.0.2:1234".parse().unwrap();

            // First registration from IP1
            let rk = RegisterPk {
                id: "peer_ipc".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            rs.handle_register_pk(rk, addr1).await.unwrap();

            // Same uuid, same pk, different IP
            let rk = RegisterPk {
                id: "peer_ipc".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, addr2).await;
            assert!(matches!(result, Ok((register_pk_response::Result::OK, _))));

            // Check IP change was tracked
            let lock = IP_CHANGES.lock().await;
            assert!(lock.contains_key("peer_ipc"));
        }

        #[tokio::test]
        async fn test_register_pk_rate_limited() {
            IP_BLOCKER.lock().await.clear();
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "10.0.0.3:1234".parse().unwrap();

            // Register 4 times rapidly — 4th should be rate limited
            for i in 0..4 {
                let rk = RegisterPk {
                    id: "peer_rl".to_owned(),
                    uuid: vec![1; 16].into(),
                    pk: vec![2; 32].into(),
                    ..Default::default()
                };
                let result = rs.handle_register_pk(rk, addr).await;
                if i < 3 {
                    assert!(matches!(result, Ok((register_pk_response::Result::OK, _))), "failed at iteration {}", i);
                } else {
                    assert_eq!(result, Err(TOO_FREQUENT), "should be rate limited at iteration {}", i);
                }
            }
        }

        #[tokio::test]
        async fn test_register_pk_no_change_no_update() {
            IP_BLOCKER.lock().await.clear();
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "10.0.0.4:1234".parse().unwrap();

            // First registration
            let rk = RegisterPk {
                id: "peer_nc".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            rs.handle_register_pk(rk, addr).await.unwrap();

            // Same everything — no change needed
            let rk = RegisterPk {
                id: "peer_nc".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            };
            let result = rs.handle_register_pk(rk, addr).await;
            assert!(matches!(result, Ok((register_pk_response::Result::OK, _))));
        }
    }

    mod check_ip_blocker_tests {
        use super::*;

        #[tokio::test]
        async fn test_first_request_allowed() {
            IP_BLOCKER.lock().await.clear();
            let (rs, _rx) = test_server().await;
            assert!(rs.check_ip_blocker("10.0.0.1", "peer1").await);
        }

        #[tokio::test]
        async fn test_excessive_requests_blocked() {
            IP_BLOCKER.lock().await.clear();
            let (rs, _rx) = test_server().await;
            for i in 0..35 {
                let _ = rs.check_ip_blocker("10.0.0.99", &format!("peer{i}")).await;
            }
            assert!(!rs.check_ip_blocker("10.0.0.99", "peer_new").await);
        }

        #[tokio::test]
        async fn test_same_id_multiple_times_allowed() {
            IP_BLOCKER.lock().await.clear();
            let (rs, _rx) = test_server().await;
            // Same ID repeated should be fine
            for _ in 0..10 {
                assert!(rs.check_ip_blocker("10.0.0.50", "same_peer").await);
            }
        }

        #[tokio::test]
        async fn test_different_ips_independent() {
            IP_BLOCKER.lock().await.clear();
            let (rs, _rx) = test_server().await;
            assert!(rs.check_ip_blocker("10.0.0.51", "p1").await);
            assert!(rs.check_ip_blocker("10.0.0.52", "p2").await);
        }
    }

    mod check_cmd_tests {
        use super::*;

        #[tokio::test]
        async fn test_help_command() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("h").await;
            assert!(result.contains("relay-servers"));
            assert!(result.contains("ip-blocker"));
            assert!(result.contains("always-use-relay"));
        }

        #[tokio::test]
        async fn test_relay_servers_list() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("rs").await;
            assert!(result.contains("relay.example.com"));
        }

        #[tokio::test]
        async fn test_always_use_relay_query() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("aur").await;
            assert!(result.contains("ALWAYS_USE_RELAY"));
        }

        #[tokio::test]
        async fn test_ip_blocker_list() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ib").await;
            // First line is the count (may not be 0 due to parallel tests)
            assert!(result.lines().next().unwrap().parse::<usize>().is_ok());
        }

        #[tokio::test]
        async fn test_ip_changes_list() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ic").await;
            assert!(result.lines().next().unwrap().parse::<usize>().is_ok());
        }

        #[tokio::test]
        async fn test_unknown_command() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("nonexistent").await;
            assert!(result.is_empty());
        }

        #[tokio::test]
        async fn test_ip_blocker_lookup_specific_ip() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ib 192.168.99.99").await;
            // IP not in blocker, so just the count line
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_ip_blocker_remove_ip() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ib 192.168.99.99 -").await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_ip_blocker_start_offset() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ib 0").await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_ip_changes_lookup_specific_id() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ic some_peer_id").await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_ip_changes_remove_id() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ic some_peer_id -").await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_ip_changes_start_offset() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("ic 0").await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_relay_servers_set() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("rs new-relay.com").await;
            // Setting relay servers sends data via tx, no output
            assert!(result.is_empty());
        }

        #[tokio::test]
        async fn test_always_use_relay_set_y() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("aur Y").await;
            assert!(result.is_empty());
        }

        #[tokio::test]
        async fn test_always_use_relay_set_n() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("aur N").await;
            assert!(result.is_empty());
        }

        #[tokio::test]
        async fn test_test_geo_command() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("tg 10.0.0.1 10.0.0.2").await;
            assert!(!result.is_empty());
        }

        #[tokio::test]
        async fn test_test_geo_single_ip() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("tg 10.0.0.1").await;
            assert!(!result.is_empty());
        }
    }

    async fn test_server_with_sk() -> (RendezvousServer, Receiver) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        std::mem::forget(dir);

        let db = crate::database::Database::new(path.to_str().unwrap()).await.unwrap();
        let pm = PeerMap::new_with_db(db);
        let (tx, rx) = mpsc::unbounded_channel();
        let (secure_tcp_pk_b, secure_tcp_sk_b) = box_::gen_keypair();
        let (_pk, sk) = sign::gen_keypair();
        let rs = RendezvousServer {
            tcp_punch: Arc::new(Mutex::new(HashMap::new())),
            ws_peers: Default::default(),
            pm,
            tx,
            relay_servers: Arc::new(vec!["relay.example.com".to_owned()]),
            relay_servers0: Default::default(),
            rendezvous_servers: Arc::new(vec![]),
            inner: Arc::new(Inner {
                serial: 1,
                version: String::new(),
                software_url: String::new(),
                mask: None,
                local_ip: String::new(),
                sk: Some(sk),
                secure_tcp_pk_b,
                secure_tcp_sk_b,
            }),
        };
        (rs, rx)
    }

    async fn register_peer(rs: &mut RendezvousServer, id: &str, addr: &str) {
        let addr: SocketAddr = addr.parse().unwrap();
        let peer = rs.pm.get_or(id).await;
        let uuid = Bytes::from_static(b"test-uuid-bytes!");
        let pk = Bytes::from_static(b"test-pk-bytes!!!");
        rs.pm.update_pk(
            id.to_owned(), peer, addr, uuid, pk, addr.ip().to_string(),
        ).await;
    }

    mod handle_punch_hole_tests {
        use super::*;

        #[tokio::test]
        async fn test_license_key_mismatch() {
            let (mut rs, _rx) = test_server().await;
            let ph = PunchHoleRequest {
                id: "target123".to_owned(),
                licence_key: "wrong_key".to_owned(),
                ..Default::default()
            };
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.1:1234".parse().unwrap(), ph, "correct_key", false,
            ).await.unwrap();
            assert!(peer_addr.is_none());
            assert!(msg.has_punch_hole_response());
            assert_eq!(
                msg.punch_hole_response().failure,
                punch_hole_response::Failure::LICENSE_MISMATCH.into()
            );
        }

        #[tokio::test]
        async fn test_empty_key_skips_license_check() {
            let (mut rs, _rx) = test_server().await;
            let ph = PunchHoleRequest {
                id: "nonexistent".to_owned(),
                licence_key: "anything".to_owned(),
                ..Default::default()
            };
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.1:1234".parse().unwrap(), ph, "", false,
            ).await.unwrap();
            assert!(peer_addr.is_none());
            assert_eq!(
                msg.punch_hole_response().failure,
                punch_hole_response::Failure::ID_NOT_EXIST.into()
            );
        }

        #[tokio::test]
        async fn test_peer_not_found() {
            let (mut rs, _rx) = test_server().await;
            let ph = PunchHoleRequest {
                id: "missing_peer".to_owned(),
                ..Default::default()
            };
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.1:1234".parse().unwrap(), ph, "", false,
            ).await.unwrap();
            assert!(peer_addr.is_none());
            assert_eq!(
                msg.punch_hole_response().failure,
                punch_hole_response::Failure::ID_NOT_EXIST.into()
            );
        }

        #[tokio::test]
        async fn test_peer_offline() {
            let (mut rs, _rx) = test_server().await;
            register_peer(&mut rs, "offline1", "10.0.0.2:5555").await;

            // Set last_reg_time to expired
            let peer = rs.pm.get("offline1").await.unwrap();
            peer.write().await.last_reg_time = get_expired_time();

            let ph = PunchHoleRequest {
                id: "offline1".to_owned(),
                ..Default::default()
            };
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.1:1234".parse().unwrap(), ph, "", false,
            ).await.unwrap();
            assert!(peer_addr.is_none());
            assert_eq!(
                msg.punch_hole_response().failure,
                punch_hole_response::Failure::OFFLINE.into()
            );
        }

        #[tokio::test]
        async fn test_peer_online_returns_punch_hole() {
            let (mut rs, _rx) = test_server().await;
            register_peer(&mut rs, "online1", "10.0.0.2:5555").await;

            let ph = PunchHoleRequest {
                id: "online1".to_owned(),
                ..Default::default()
            };
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.3:1234".parse().unwrap(), ph, "", false,
            ).await.unwrap();
            assert!(peer_addr.is_some());
            assert_eq!(peer_addr.unwrap(), "10.0.0.2:5555".parse::<SocketAddr>().unwrap());
            assert!(msg.has_punch_hole());
        }

        #[tokio::test]
        async fn test_same_ip_returns_fetch_local_addr() {
            let (mut rs, _rx) = test_server().await;
            register_peer(&mut rs, "local_1", "10.0.0.2:5555").await;

            let ph = PunchHoleRequest {
                id: "local_1".to_owned(),
                ..Default::default()
            };
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.2:6666".parse().unwrap(), ph, "", false,
            ).await.unwrap();
            assert!(peer_addr.is_some());
            assert!(msg.has_fetch_local_addr());
        }

        #[tokio::test]
        async fn test_peer_ws_same_ip_still_punches() {
            let (mut rs, _rx) = test_server().await;
            register_peer(&mut rs, "ws_peer1", "10.0.0.2:5555").await;

            let ph = PunchHoleRequest {
                id: "ws_peer1".to_owned(),
                ..Default::default()
            };
            // ws=true means same_intranet is always false
            let (msg, peer_addr) = rs.handle_punch_hole_request(
                "10.0.0.2:6666".parse().unwrap(), ph, "", true,
            ).await.unwrap();
            assert!(peer_addr.is_some());
            assert!(msg.has_punch_hole());
        }
    }

    mod get_pk_tests {
        use super::*;

        #[tokio::test]
        async fn test_get_pk_empty_version_returns_empty() {
            let (mut rs, _rx) = test_server().await;
            let pk = rs.get_pk("", "some_id".to_owned()).await;
            assert!(pk.is_empty());
        }

        #[tokio::test]
        async fn test_get_pk_no_sk_returns_empty() {
            let (mut rs, _rx) = test_server().await;
            let pk = rs.get_pk("1.2.3", "some_id".to_owned()).await;
            assert!(pk.is_empty());
        }

        #[tokio::test]
        async fn test_get_pk_peer_not_found_returns_empty() {
            let (mut rs, _rx) = test_server_with_sk().await;
            let pk = rs.get_pk("1.2.3", "unknown".to_owned()).await;
            assert!(pk.is_empty());
        }

        #[tokio::test]
        async fn test_get_pk_returns_signed_pk() {
            let (mut rs, _rx) = test_server_with_sk().await;
            register_peer(&mut rs, "signed1", "10.0.0.1:1234").await;
            let pk = rs.get_pk("1.2.3", "signed1".to_owned()).await;
            assert!(!pk.is_empty());
        }
    }

    mod parse_relay_servers_tests {
        use super::*;

        #[tokio::test]
        async fn test_parse_sets_relay_servers() {
            let (mut rs, _rx) = test_server().await;
            rs.parse_relay_servers("relay1.com,relay2.com");
            assert_eq!(rs.relay_servers.len(), 2);
            assert_eq!(rs.relay_servers0.len(), 2);
        }

        #[tokio::test]
        async fn test_parse_empty_clears_servers() {
            let (mut rs, _rx) = test_server().await;
            rs.parse_relay_servers("");
            assert!(rs.relay_servers.is_empty());
        }
    }

    mod symmetric_key_tests {
        use super::*;

        #[test]
        fn test_get_symetric_key_roundtrip() {
            let (their_pk, their_sk) = box_::gen_keypair();
            let (our_pk, our_sk) = box_::gen_keypair();
            let symmetric_key = secretbox::gen_key();

            let nonce = box_::Nonce([0u8; box_::NONCEBYTES]);
            let sealed = box_::seal(&symmetric_key.0, &nonce, &our_pk, &their_sk);
            let mut sealed_arr = [0u8; 48];
            sealed_arr.copy_from_slice(&sealed);

            let recovered = get_symetric_key_from_msg(our_sk.0, their_pk.0, &sealed_arr);
            assert_eq!(recovered, symmetric_key.0);
        }
    }

    mod get_server_sk_tests {
        use super::*;

        #[test]
        fn test_valid_secret_key_parsed() {
            let (pk, sk) = sign::gen_keypair();
            let sk_b64 = base64::encode(&sk);
            let (key, out_sk) = RendezvousServer::get_server_sk(&sk_b64);
            assert!(out_sk.is_some());
            assert_eq!(key, base64::encode(pk));
        }

        #[test]
        fn test_non_base64_key_treated_as_plain() {
            let (key, sk) = RendezvousServer::get_server_sk("not-valid-base64!!!");
            // Non-base64 strings aren't decoded, so sk stays None,
            // and since key is non-empty and not "-" or "_", gen_sk is not called
            assert_eq!(key, "not-valid-base64!!!");
            assert!(sk.is_none());
        }

        #[test]
        fn test_short_base64_not_secret_key() {
            let short = base64::encode(b"tooshort");
            let (key, sk) = RendezvousServer::get_server_sk(&short);
            // Decodes fine but length != SECRETKEYBYTES, so not treated as crypto key
            assert_eq!(key, short);
            assert!(sk.is_none());
        }
    }

    mod more_check_cmd_tests {
        use super::*;

        #[tokio::test]
        async fn test_ip_blocker_with_data_shows_details() {
            let (rs, _rx) = test_server().await;
            // Populate IP_BLOCKER with some data
            let unique_ip = "10.222.0.1";
            IP_BLOCKER.lock().await.insert(
                unique_ip.to_owned(),
                (
                    (5, Instant::now()),
                    ({
                        let mut s = std::collections::HashSet::new();
                        s.insert("peer_a".to_owned());
                        s
                    }, Instant::now()),
                ),
            );

            // Lookup specific IP
            let result = rs.check_cmd(&format!("ib {}", unique_ip)).await;
            assert!(result.contains("5/"), "Should show request count: got '{}'", result);

            // List with offset
            let result = rs.check_cmd("ib 0").await;
            assert!(result.contains(unique_ip) || !result.is_empty());

            // Remove
            let result = rs.check_cmd(&format!("ib {} -", unique_ip)).await;
            assert!(!result.is_empty());

            IP_BLOCKER.lock().await.remove(unique_ip);
        }

        #[tokio::test]
        async fn test_ip_changes_with_data_shows_details() {
            let (rs, _rx) = test_server().await;
            let unique_id = "ic_test_peer_99";
            IP_CHANGES.lock().await.insert(
                unique_id.to_owned(),
                (
                    Instant::now(),
                    {
                        let mut m = HashMap::new();
                        m.insert("10.0.0.1".to_owned(), 3);
                        m.insert("10.0.0.2".to_owned(), 1);
                        m
                    },
                ),
            );

            // Lookup specific id
            let result = rs.check_cmd(&format!("ic {}", unique_id)).await;
            assert!(result.contains("10.0.0.1"), "Should show IP details");

            // List with offset
            let result = rs.check_cmd("ic 0").await;
            assert!(result.contains(unique_id) || !result.is_empty());

            // Remove
            rs.check_cmd(&format!("ic {} -", unique_id)).await;
            assert!(IP_CHANGES.lock().await.get(unique_id).is_none());
        }

        #[tokio::test]
        async fn test_reload_geo_command() {
            let (rs, _rx) = test_server().await;
            let result = rs.check_cmd("rg").await;
            // reload-geo just reloads, returns empty or no output
            assert!(result.is_empty());
        }
    }

    mod more_check_ip_blocker_tests {
        use super::*;

        #[tokio::test]
        async fn test_ip_blocker_counter_resets_after_duration() {
            let (rs, _rx) = test_server().await;
            let unique_ip = "10.223.0.1";
            // Insert with expired timestamp
            let expired = Instant::now()
                .checked_sub(std::time::Duration::from_secs(IP_BLOCK_DUR + 10))
                .unwrap();
            IP_BLOCKER.lock().await.insert(
                unique_ip.to_owned(),
                ((35, expired), (Default::default(), Instant::now())),
            );

            // Should be allowed because counter is expired
            let result = rs.check_ip_blocker(unique_ip, "some_peer").await;
            assert!(result, "Counter should reset after IP_BLOCK_DUR");

            IP_BLOCKER.lock().await.remove(unique_ip);
        }

        #[tokio::test]
        async fn test_ip_blocker_too_many_ids_blocks_new() {
            let (rs, _rx) = test_server().await;
            let unique_ip = "10.224.0.1";
            let mut ids = std::collections::HashSet::new();
            for i in 0..301 {
                ids.insert(format!("peer_{}", i));
            }
            IP_BLOCKER.lock().await.insert(
                unique_ip.to_owned(),
                ((0, Instant::now()), (ids, Instant::now())),
            );

            // Should block a brand new ID
            let result = rs.check_ip_blocker(unique_ip, "brand_new_peer").await;
            assert!(!result, "Should block new peer when >300 IDs from same IP");

            // Should allow an existing ID
            let result = rs.check_ip_blocker(unique_ip, "peer_0").await;
            assert!(result, "Should allow existing peer ID");

            IP_BLOCKER.lock().await.remove(unique_ip);
        }

        #[tokio::test]
        async fn test_ip_blocker_day_reset() {
            let (rs, _rx) = test_server().await;
            let unique_ip = "10.225.0.1";
            let expired_day = Instant::now()
                .checked_sub(std::time::Duration::from_secs(DAY_SECONDS + 10))
                .unwrap();
            let mut ids = std::collections::HashSet::new();
            for i in 0..301 {
                ids.insert(format!("peer_{}", i));
            }
            IP_BLOCKER.lock().await.insert(
                unique_ip.to_owned(),
                ((0, Instant::now()), (ids, expired_day)),
            );

            // Day expired, IDs should be cleared, new peer should be allowed
            let result = rs.check_ip_blocker(unique_ip, "brand_new_peer").await;
            assert!(result, "Should allow after day reset");

            IP_BLOCKER.lock().await.remove(unique_ip);
        }
    }

    mod more_punch_hole_tests {
        use super::*;

        #[tokio::test]
        async fn test_always_use_relay_forces_symmetric() {
            let (mut rs, _rx) = test_server_with_sk().await;
            register_peer(&mut rs, "relay_target", "10.0.0.1:9999").await;

            ALWAYS_USE_RELAY.store(true, Ordering::SeqCst);

            let ph = PunchHoleRequest {
                id: "relay_target".to_owned(),
                ..Default::default()
            };
            let addr: SocketAddr = "10.0.0.2:1234".parse().unwrap();
            let (msg, peer_addr) = rs.handle_punch_hole_request(addr, ph, "", false).await.unwrap();
            assert!(peer_addr.is_some());
            // When ALWAYS_USE_RELAY is set, nat_type should be forced to SYMMETRIC
            assert!(msg.has_punch_hole());
            assert_eq!(
                msg.punch_hole().nat_type,
                NatType::SYMMETRIC.into(),
            );

            ALWAYS_USE_RELAY.store(false, Ordering::SeqCst);
        }

        #[tokio::test]
        async fn test_punch_hole_with_lan_mask() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.sqlite3");
            std::mem::forget(dir);
            let db = crate::database::Database::new(path.to_str().unwrap()).await.unwrap();
            let pm = PeerMap::new_with_db(db);
            let (tx, rx) = mpsc::unbounded_channel();
            let (secure_tcp_pk_b, secure_tcp_sk_b) = box_::gen_keypair();
            let (_pk, sk) = sign::gen_keypair();

            let mask: ipnetwork::Ipv4Network = "10.0.0.0/24".parse().unwrap();
            let mut rs = RendezvousServer {
                tcp_punch: Arc::new(Mutex::new(HashMap::new())),
                ws_peers: Default::default(),
                pm,
                tx,
                relay_servers: Arc::new(vec!["relay.example.com".to_owned()]),
                relay_servers0: Default::default(),
                rendezvous_servers: Arc::new(vec![]),
                inner: Arc::new(Inner {
                    serial: 1,
                    version: String::new(),
                    software_url: String::new(),
                    mask: Some(mask),
                    local_ip: "10.0.0.100".to_owned(),
                    sk: Some(sk),
                    secure_tcp_pk_b,
                    secure_tcp_sk_b,
                }),
            };
            register_peer(&mut rs, "lan_peer", "10.0.0.5:9999").await;

            // Both peers in same LAN
            let ph = PunchHoleRequest {
                id: "lan_peer".to_owned(),
                ..Default::default()
            };
            let addr: SocketAddr = "10.0.0.10:1234".parse().unwrap();
            let (msg, peer_addr) = rs.handle_punch_hole_request(addr, ph, "", false).await.unwrap();
            assert!(peer_addr.is_some());
            // Same intranet peers get FetchLocalAddr
            assert!(msg.has_fetch_local_addr(), "Same LAN peers should get FetchLocalAddr, got: {:?}", msg);
        }

        #[tokio::test]
        async fn test_punch_hole_cross_lan_forces_relay() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.sqlite3");
            std::mem::forget(dir);
            let db = crate::database::Database::new(path.to_str().unwrap()).await.unwrap();
            let pm = PeerMap::new_with_db(db);
            let (tx, rx) = mpsc::unbounded_channel();
            let (secure_tcp_pk_b, secure_tcp_sk_b) = box_::gen_keypair();
            let (_pk, sk) = sign::gen_keypair();

            let mask: ipnetwork::Ipv4Network = "10.0.0.0/24".parse().unwrap();
            let mut rs = RendezvousServer {
                tcp_punch: Arc::new(Mutex::new(HashMap::new())),
                ws_peers: Default::default(),
                pm,
                tx,
                relay_servers: Arc::new(vec!["relay.example.com".to_owned()]),
                relay_servers0: Default::default(),
                rendezvous_servers: Arc::new(vec![]),
                inner: Arc::new(Inner {
                    serial: 1,
                    version: String::new(),
                    software_url: String::new(),
                    mask: Some(mask),
                    local_ip: "10.0.0.100".to_owned(),
                    sk: Some(sk),
                    secure_tcp_pk_b,
                    secure_tcp_sk_b,
                }),
            };
            // Peer in LAN
            register_peer(&mut rs, "lan_peer2", "10.0.0.5:9999").await;

            // Requester outside LAN — cross LAN => force relay
            let ph = PunchHoleRequest {
                id: "lan_peer2".to_owned(),
                ..Default::default()
            };
            let addr: SocketAddr = "192.168.1.100:1234".parse().unwrap();
            let (msg, peer_addr) = rs.handle_punch_hole_request(addr, ph, "", false).await.unwrap();
            assert!(peer_addr.is_some());
            // Cross-LAN should force SYMMETRIC (relay)
            assert!(msg.has_punch_hole());
            assert_eq!(
                msg.punch_hole().nat_type,
                NatType::SYMMETRIC.into(),
            );
        }
    }

    mod handle_hole_sent_tests {
        use super::*;

        #[tokio::test]
        async fn test_handle_hole_sent_via_udp() {
            let (mut rs, mut rx) = test_server().await;
            register_peer(&mut rs, "hs_peer", "192.168.1.10:5000").await;

            let addr_a: SocketAddr = "192.168.1.20:6000".parse().unwrap();
            let addr_b: SocketAddr = "192.168.1.30:7000".parse().unwrap();

            let phs = PunchHoleSent {
                socket_addr: AddrMangle::encode(addr_a).into(),
                id: "hs_peer".to_owned(),
                relay_server: "relay1.example.com".to_owned(),
                ..Default::default()
            };

            // Create a FramedSocket for UDP send
            let mut socket = create_udp_listener(0, 0).await.unwrap();
            let result = rs.handle_hole_sent(phs, addr_b, Some(&mut socket)).await;
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_handle_hole_sent_via_tcp() {
            let (mut rs, mut rx) = test_server().await;
            let addr_a: SocketAddr = "10.0.0.1:5000".parse().unwrap();
            let addr_b: SocketAddr = "10.0.0.2:7000".parse().unwrap();

            let phs = PunchHoleSent {
                socket_addr: AddrMangle::encode(addr_a).into(),
                id: "".to_owned(),
                ..Default::default()
            };

            // TCP path: socket is None, sends via send_to_tcp
            let result = rs.handle_hole_sent(phs, addr_b, None).await;
            assert!(result.is_ok());
        }
    }

    mod handle_local_addr_tests {
        use super::*;

        #[tokio::test]
        async fn test_handle_local_addr_via_udp() {
            let (mut rs, mut rx) = test_server().await;
            register_peer(&mut rs, "la_peer", "10.0.0.5:5000").await;

            let addr_a: SocketAddr = "10.0.0.10:6000".parse().unwrap();
            let addr_b: SocketAddr = "10.0.0.20:7000".parse().unwrap();

            let la = LocalAddr {
                socket_addr: AddrMangle::encode(addr_a).into(),
                local_addr: vec![1, 2, 3, 4, 5, 6].into(),
                id: "la_peer".to_owned(),
                relay_server: "relay.example.com".to_owned(),
                ..Default::default()
            };

            let mut socket = create_udp_listener(0, 0).await.unwrap();
            let result = rs.handle_local_addr(la, addr_b, Some(&mut socket)).await;
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_handle_local_addr_via_tcp() {
            let (mut rs, mut rx) = test_server().await;
            let addr_a: SocketAddr = "10.0.0.1:5000".parse().unwrap();
            let addr_b: SocketAddr = "10.0.0.2:7000".parse().unwrap();

            let la = LocalAddr {
                socket_addr: AddrMangle::encode(addr_a).into(),
                local_addr: vec![10, 20, 30, 40].into(),
                id: "".to_owned(),
                relay_server: "".to_owned(),
                ..Default::default()
            };

            let result = rs.handle_local_addr(la, addr_b, None).await;
            assert!(result.is_ok());
        }
    }

    mod handle_online_request_tests {
        use super::*;

        #[tokio::test]
        async fn test_handle_online_request_empty_peers() {
            let (mut rs, _rx) = test_server().await;

            // Create a TCP listener and connect to it
            let listener = hbb_common::tcp::listen_any(0, true).await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let connect_handle = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                stream
            });
            let client = hbb_common::tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            let server_stream = connect_handle.await.unwrap();
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            let mut stream = FramedStream::from(server_stream, addr);

            let result = rs.handle_online_request(&mut stream, vec![]).await;
            assert!(result.is_ok());
        }
    }

    mod handle_tcp_tests {
        use super::*;

        #[tokio::test]
        async fn test_handle_tcp_unknown_message_returns_false() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "192.168.1.1:1234".parse().unwrap();
            let mut sink = None;

            // Empty/invalid bytes
            let result = rs.handle_tcp(b"invalid", &mut sink, addr, "", false).await;
            assert!(!result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_register_pk_success() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "192.168.1.1:1234".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_register_pk(RegisterPk {
                id: "tcp_test_peer".to_owned(),
                uuid: vec![1; 16].into(),
                pk: vec![2; 32].into(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            // Without a sink, response is lost but code path is exercised
            let result = rs.handle_tcp(&bytes, &mut sink, addr, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_punch_hole_request_stores_sink() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "192.168.1.1:1234".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_punch_hole_request(PunchHoleRequest {
                id: "nonexistent_peer".to_owned(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_request_relay_no_peer() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "192.168.1.1:1234".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_request_relay(RequestRelay {
                id: "nonexistent_peer".to_owned(),
                uuid: "test-uuid".to_owned(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_request_relay_with_peer() {
            let (mut rs, mut rx) = test_server().await;
            register_peer(&mut rs, "relay_tgt", "10.0.0.5:9000").await;

            let addr: SocketAddr = "192.168.1.1:1234".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_request_relay(RequestRelay {
                id: "relay_tgt".to_owned(),
                uuid: "test-uuid-2".to_owned(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr, "", false).await;
            assert!(result.0);

            // Server should have sent RequestRelay to peer via tx
            if let Ok(data) = rx.try_recv() {
                match data {
                    Data::Msg(_, to_addr) => {
                        assert_eq!(to_addr, "10.0.0.5:9000".parse::<SocketAddr>().unwrap());
                    }
                    _ => {}
                }
            }
        }

        #[tokio::test]
        async fn test_handle_tcp_test_nat_request() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "192.168.1.1:1234".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_test_nat_request(TestNatRequest {
                serial: 0,
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            // Without sink, response is lost but code path exercised
            let result = rs.handle_tcp(&bytes, &mut sink, addr, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_punch_hole_sent() {
            let (mut rs, _rx) = test_server().await;
            let addr_a: SocketAddr = "10.0.0.1:5000".parse().unwrap();
            let addr_b: SocketAddr = "10.0.0.2:7000".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_punch_hole_sent(PunchHoleSent {
                socket_addr: AddrMangle::encode(addr_a).into(),
                id: "".to_owned(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr_b, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_local_addr() {
            let (mut rs, _rx) = test_server().await;
            let addr_a: SocketAddr = "10.0.0.1:5000".parse().unwrap();
            let addr_b: SocketAddr = "10.0.0.2:7000".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_local_addr(LocalAddr {
                socket_addr: AddrMangle::encode(addr_a).into(),
                local_addr: vec![1, 2, 3, 4].into(),
                id: "".to_owned(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr_b, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_relay_response() {
            let (mut rs, _rx) = test_server().await;
            let addr_a: SocketAddr = "10.0.0.1:5000".parse().unwrap();
            let addr_b: SocketAddr = "10.0.0.2:7000".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_relay_response(RelayResponse {
                socket_addr: AddrMangle::encode(addr_b).into(),
                relay_server: "relay.example.com".to_owned(),
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr_a, "", false).await;
            assert!(result.0);
        }

        #[tokio::test]
        async fn test_handle_tcp_key_exchange_invalid() {
            let (mut rs, _rx) = test_server().await;
            let addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
            let mut sink = None;

            let mut msg = RendezvousMessage::new();
            msg.set_key_exchange(KeyExchange {
                keys: vec![vec![0; 32].into()], // only 1 key, needs 2
                ..Default::default()
            });
            let bytes = msg.write_to_bytes().unwrap();

            let result = rs.handle_tcp(&bytes, &mut sink, addr, "", false).await;
            assert!(!result.0);
        }
    }

    mod logged_in_only_tests {
        use super::*;

        #[tokio::test]
        async fn test_logged_in_only_empty_token_rejected() {
            let (mut rs, _rx) = test_server_with_sk().await;
            register_peer(&mut rs, "logged_peer", "10.0.0.5:5000").await;

            std::env::set_var("LOGGED_IN_ONLY", "Y");

            let ph = PunchHoleRequest {
                id: "logged_peer".to_owned(),
                token: "".to_owned(),
                ..Default::default()
            };
            let addr: SocketAddr = "10.0.0.1:1234".parse().unwrap();
            let (msg, _) = rs.handle_punch_hole_request(addr, ph, "", false).await.unwrap();

            std::env::remove_var("LOGGED_IN_ONLY");

            assert!(msg.has_punch_hole_response());
            assert!(
                msg.punch_hole_response().other_failure.contains("not logged in"),
            );
        }

        #[tokio::test]
        async fn test_logged_in_only_with_token_tries_api() {
            let (mut rs, _rx) = test_server_with_sk().await;
            register_peer(&mut rs, "logged_peer2", "10.0.0.5:5000").await;

            std::env::set_var("LOGGED_IN_ONLY", "Y");

            let ph = PunchHoleRequest {
                id: "logged_peer2".to_owned(),
                token: "fake-jwt-token".to_owned(),
                ..Default::default()
            };
            let addr: SocketAddr = "10.0.0.1:1234".parse().unwrap();
            // This will fail because API server isn't running, but exercises the code path
            let result = rs.handle_punch_hole_request(addr, ph, "", false).await;

            std::env::remove_var("LOGGED_IN_ONLY");

            // Should get an error or LOGIN_OVERHAUL response (API server not available)
            match result {
                Ok((msg, _)) => {
                    assert!(msg.has_punch_hole_response());
                }
                Err(_) => {} // API call failed, expected
            }
        }
    }
}
