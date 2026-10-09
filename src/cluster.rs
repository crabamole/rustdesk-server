//! This process among hbbs replicas: identity, presence, routing tokens, forwarding, drain state.
use crate::{
    internal::{self, Call, Token},
    presence::{self, Presence},
    routing::{self, RoutingTokens},
};
use hbb_common::{
    anyhow::Context,
    bail, log,
    rendezvous_proto::RendezvousMessage,
    tokio::{self, sync::watch, task::JoinHandle, time::interval},
    ResultType,
};
use sqlx::PgPool;
use std::{
    collections::HashMap,
    future::Future,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

/// Longer than a healthy heartbeat tick; a hung database must not hold up shutdown.
const HEARTBEAT_STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClusterConfig {
    pub(crate) pod: String,
    pub(crate) ordinal: u16,
    pub(crate) pod_ip: IpAddr,
    pub(crate) internal_port: u16,
    pub(crate) health_port: u16,
    pub(crate) keep_alive_secs: i32,
}

impl ClusterConfig {
    pub(crate) fn from_env() -> ResultType<Self> {
        Self::parse(|name| std::env::var(name).ok().filter(|v| !v.is_empty()))
    }

    pub(crate) fn parse(get: impl Fn(&str) -> Option<String>) -> ResultType<Self> {
        let pod = get("POD_NAME").unwrap_or_else(|| "hbbs-0".to_owned());
        let ordinal = match presence::ordinal_of_pod(&pod) {
            Some(n) if n <= routing::MAX_ORDINAL => n,
            _ => bail!("POD_NAME {pod:?} must end in -<n> with n at most {}", routing::MAX_ORDINAL),
        };
        let port = |name: &str, default: u16| -> ResultType<u16> {
            match get(name) {
                Some(v) => v.parse().with_context(|| format!("invalid {name}")),
                None => Ok(default),
            }
        };
        let keep_alive_secs: i32 = match get("KEEP_ALIVE_SECS") {
            Some(v) => v.parse().context("invalid KEEP_ALIVE_SECS")?,
            None => 20,
        };
        if keep_alive_secs <= 0 {
            bail!("KEEP_ALIVE_SECS must be positive");
        }
        let pod_ip = match get("POD_IP") {
            Some(v) => v.parse().context("invalid POD_IP")?,
            None => IpAddr::from([127, 0, 0, 1]),
        };
        Ok(Self {
            pod,
            ordinal,
            pod_ip,
            internal_port: port("INTERNAL_PORT", 21120)?,
            health_port: port("HEALTH_PORT", 21121)?,
            keep_alive_secs,
        })
    }

    /// The address other pods dial, as stored in `hbbs_pod.addr`.
    pub(crate) fn internal_addr(&self, port: u16) -> String {
        SocketAddr::new(self.pod_ip, port).to_string()
    }
}

pub(crate) struct Cluster {
    pub(crate) presence: Presence,
    pub(crate) tokens: RoutingTokens,
    pub(crate) keep_alive_secs: i32,
    token: Token,
    draining: watch::Sender<bool>,
    heartbeat: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Cluster {
    pub(crate) fn new(presence: Presence, ordinal: u16, keep_alive_secs: i32, token: Token) -> Self {
        Self {
            presence,
            tokens: RoutingTokens::new(ordinal),
            keep_alive_secs,
            token,
            draining: watch::Sender::new(false),
            heartbeat: Default::default(),
        }
    }

    pub(crate) async fn start(cfg: &ClusterConfig, internal_port: u16, pool: PgPool, token: Token) -> ResultType<Self> {
        let presence = Presence::new(pool, cfg.pod.clone(), cfg.internal_addr(internal_port));
        presence.start().await?;
        log::info!(
            "pod {} (ordinal {}, epoch {}) reachable at {}",
            cfg.pod,
            cfg.ordinal,
            presence.epoch(),
            cfg.internal_addr(internal_port)
        );
        Ok(Self::new(presence, cfg.ordinal, cfg.keep_alive_secs, token))
    }

    pub(crate) fn token(&self) -> Token {
        self.token
    }

    pub(crate) fn is_draining(&self) -> bool {
        *self.draining.borrow()
    }

    pub(crate) fn start_draining(&self) {
        self.draining.send_replace(true);
    }

    /// Runs `tick` every `presence::HEARTBEAT_SECS` until draining starts.
    pub(crate) fn run_heartbeat<F, Fut>(&self, tick: F)
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let mut draining = self.draining.subscribe();
        let task = tokio::spawn(async move {
            let mut every = interval(Duration::from_secs(presence::HEARTBEAT_SECS));
            loop {
                let stop = tokio::select! {
                    biased;
                    _ = draining.wait_for(|d| *d) => true,
                    _ = every.tick() => false,
                };
                if stop {
                    break;
                }
                tick().await;
            }
        });
        if let Ok(mut heartbeat) = self.heartbeat.lock() {
            *heartbeat = Some(task);
        }
    }

    /// The heartbeat finishes first, or it would write the pod row of a draining pod back;
    /// a hung one is cut off, so the row delete is best effort (an in-flight write may still
    /// commit and then expires within the 30 s liveness window).
    pub(crate) async fn stop(&self) {
        self.start_draining();
        let task = self.heartbeat.lock().ok().and_then(|mut h| h.take());
        if let Some(mut task) = task {
            match tokio::time::timeout(HEARTBEAT_STOP_TIMEOUT, &mut task).await {
                Ok(Err(e)) => log::warn!("presence heartbeat ended badly: {e}"),
                Ok(Ok(())) => {}
                Err(_) => {
                    log::warn!("presence heartbeat still busy after {HEARTBEAT_STOP_TIMEOUT:?}, stopping it");
                    task.abort();
                }
            }
        }
        self.presence.stop().await;
    }

    /// Rewrites this pod's presence from `devices()`. Connections that came or went meanwhile
    /// may have been undone by the rewrite, so they are written again.
    pub(crate) async fn reassert<F, Fut>(&self, devices: F)
    where
        F: Fn() -> Fut,
        Fut: Future<Output = HashMap<String, u64>>,
    {
        let before = devices().await;
        let snapshot: Vec<(String, u64)> = before.iter().map(|(id, gen)| (id.clone(), *gen)).collect();
        self.presence.reassert(&snapshot).await;
        let after = devices().await;
        for (id, gen) in &before {
            if after.get(id) != Some(gen) {
                self.presence.unregister(id, *gen).await;
            }
        }
        let came: Vec<(String, u64)> = after
            .iter()
            .filter(|(id, gen)| before.get(*id) != Some(*gen))
            .map(|(id, gen)| (id.clone(), *gen))
            .collect();
        // `after` may be stale by now: a newer connection here or on a live pod must win.
        self.presence.claim(&came).await;
        // One that left before its row was written again has already deleted it.
        let now = devices().await;
        for (id, gen) in &came {
            if now.get(id) != Some(gen) {
                self.presence.unregister(id, *gen).await;
            }
        }
    }

    pub(crate) async fn deliver_to_device(&self, id: &str, msg: &RendezvousMessage) -> Option<bool> {
        let holder = self.presence.holder(id).await?;
        Some(self.call(&holder.addr, Call::Device { id: id.to_owned(), msg: msg.clone() }).await)
    }

    /// True whenever the viewer belongs to another pod, even if it could not be reached:
    /// this pod's own waiting viewers must not be touched.
    pub(crate) async fn deliver_to_viewer(&self, addr: SocketAddr, msg: &RendezvousMessage) -> bool {
        let Some(ordinal) = routing::ordinal_of(addr).filter(|n| *n != self.tokens.ordinal()) else {
            return false;
        };
        match self.presence.pod_by_ordinal(ordinal).await {
            Some(pod) => {
                if !self.call(&pod.addr, Call::Viewer { addr, msg: msg.clone() }).await {
                    log::debug!("viewer {addr} no longer waits on {}", pod.pod);
                }
            }
            None => log::warn!("no live hbbs pod with ordinal {ordinal} for viewer {addr}"),
        }
        true
    }

    async fn call(&self, addr: &str, call: Call) -> bool {
        match internal::call(addr, &self.token, &call).await {
            Ok(delivered) => delivered,
            Err(e) => {
                log::warn!("forwarding to {addr} failed: {e}");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio::{self, net::TcpListener, sync::mpsc};
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
    };

    fn cfg(vars: &[(&str, &str)]) -> ResultType<ClusterConfig> {
        let vars: HashMap<String, String> = vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        ClusterConfig::parse(|name| vars.get(name).cloned())
    }

    #[test]
    fn defaults_describe_a_single_pod() {
        let c = cfg(&[]).unwrap();
        assert_eq!(
            c,
            ClusterConfig {
                pod: "hbbs-0".to_owned(),
                ordinal: 0,
                pod_ip: IpAddr::from([127, 0, 0, 1]),
                internal_port: 21120,
                health_port: 21121,
                keep_alive_secs: 20,
            }
        );
        assert_eq!(c.internal_addr(21120), "127.0.0.1:21120");
    }

    #[test]
    fn reads_the_pod_identity() {
        let c = cfg(&[
            ("POD_NAME", "rustdesk-hbbs-3"),
            ("POD_IP", "fd00::5"),
            ("INTERNAL_PORT", "9000"),
            ("HEALTH_PORT", "9001"),
            ("KEEP_ALIVE_SECS", "15"),
        ])
        .unwrap();
        assert_eq!((c.ordinal, c.internal_port, c.health_port, c.keep_alive_secs), (3, 9000, 9001, 15));
        assert_eq!(c.internal_addr(9000), "[fd00::5]:9000");
    }

    #[test]
    fn rejects_bad_values() {
        for vars in [
            [("POD_NAME", "hbbs")],
            [("POD_NAME", "hbbs-32")],
            [("KEEP_ALIVE_SECS", "0")],
            [("INTERNAL_PORT", "x")],
            [("POD_IP", "nope")],
        ] {
            assert!(cfg(&vars).is_err(), "{vars:?}");
        }
    }

    async fn pool() -> PgPool {
        PgPool::connect(&crate::testing::fresh_peer_database_url().await).await.unwrap()
    }

    async fn cluster(pool: &PgPool, pod: &str, internal_port: u16) -> Cluster {
        let cfg = cfg(&[("POD_NAME", pod)]).unwrap();
        Cluster::start(&cfg, internal_port, pool.clone(), [5; 32]).await.unwrap()
    }

    /// An internal server that records every call and delivers it.
    async fn recorder() -> (u16, mpsc::UnboundedReceiver<Call>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(internal::serve(listener, [5; 32], move |call| {
            let tx = tx.clone();
            async move { tx.send(call).is_ok() }
        }));
        (port, rx)
    }

    #[tokio::test]
    async fn device_calls_go_to_the_holder() {
        let pool = pool().await;
        let (port, mut calls) = recorder().await;
        let c0 = cluster(&pool, "hbbs-0", 1).await;
        let c1 = cluster(&pool, "hbbs-1", port).await;
        let msg = RendezvousMessage::new();
        assert_eq!(c0.deliver_to_device("dev", &msg).await, None);
        c1.presence.register("dev", 1).await;
        assert_eq!(c0.deliver_to_device("dev", &msg).await, Some(true));
        assert_eq!(calls.recv().await, Some(Call::Device { id: "dev".to_owned(), msg }));
    }

    #[tokio::test]
    async fn viewer_answers_go_to_the_pod_in_the_token() {
        let pool = pool().await;
        let (port, mut calls) = recorder().await;
        let c0 = cluster(&pool, "hbbs-0", 1).await;
        let _c1 = cluster(&pool, "hbbs-1", port).await;
        let msg = RendezvousMessage::new();
        let ip: SocketAddr = "203.0.113.5:0".parse().unwrap();
        assert!(!c0.deliver_to_viewer(c0.tokens.tag(ip), &msg).await, "own viewers are answered here");
        assert!(!c0.deliver_to_viewer(ip, &msg).await, "port 0 is never a token");
        let theirs = RoutingTokens::new(1).tag(ip);
        assert!(c0.deliver_to_viewer(theirs, &msg).await);
        assert_eq!(calls.recv().await, Some(Call::Viewer { addr: theirs, msg: msg.clone() }));
        let gone = RoutingTokens::new(9).tag(ip);
        assert!(c0.deliver_to_viewer(gone, &msg).await, "answers for a gone pod are dropped, not handled here");
    }

    #[tokio::test]
    async fn draining_is_a_one_way_switch() {
        let pool = pool().await;
        let c0 = cluster(&pool, "hbbs-0", 1).await;
        assert!(!c0.is_draining());
        c0.start_draining();
        assert!(c0.is_draining());
    }

    type Held = Mutex<HashMap<String, u64>>;

    fn held(devices: &[(&str, u64)]) -> Held {
        Mutex::new(devices.iter().map(|(id, gen)| (id.to_string(), *gen)).collect())
    }

    fn snapshot(held: &Held) -> HashMap<String, u64> {
        held.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn reassert_keeps_a_registration_that_raced_the_snapshot() {
        let pool = pool().await;
        let (c0, c1) = (cluster(&pool, "hbbs-0", 1).await, cluster(&pool, "hbbs-1", 1).await);
        let (held, calls) = (&held(&[]), &AtomicUsize::new(0));
        let c = &c0;
        c0.reassert(move || async move {
            let devices = snapshot(held);
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                // Connects right after the snapshot; its write lands before the rewrite.
                held.lock().unwrap().insert("raced".to_owned(), 1);
                c.presence.register("raced", 1).await;
            }
            devices
        })
        .await;
        assert!(c1.presence.holder("raced").await.is_some());
    }

    #[tokio::test]
    async fn reassert_drops_a_device_that_left_after_the_snapshot() {
        let pool = pool().await;
        let (c0, c1) = (cluster(&pool, "hbbs-0", 1).await, cluster(&pool, "hbbs-1", 1).await);
        c0.presence.register("left", 1).await;
        let (held, calls) = (&held(&[("left", 1)]), &AtomicUsize::new(0));
        let c = &c0;
        c0.reassert(move || async move {
            let devices = snapshot(held);
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                held.lock().unwrap().remove("left");
                c.presence.unregister("left", 1).await;
            }
            devices
        })
        .await;
        assert!(c1.presence.holder("left").await.is_none());
    }

    #[tokio::test]
    async fn reassert_does_not_rewrite_a_device_that_left_while_it_was_written_again() {
        let pool = pool().await;
        let (c0, c1) = (cluster(&pool, "hbbs-0", 1).await, cluster(&pool, "hbbs-1", 1).await);
        let (held, calls) = (&held(&[]), &AtomicUsize::new(0));
        let c = &c0;
        c0.reassert(move || async move {
            let devices = snapshot(held);
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    held.lock().unwrap().insert("brief".to_owned(), 1);
                    c.presence.register("brief", 1).await;
                }
                1 => {
                    held.lock().unwrap().remove("brief");
                    c.presence.unregister("brief", 1).await;
                }
                _ => {}
            }
            devices
        })
        .await;
        assert!(c1.presence.holder("brief").await.is_none());
    }

    #[tokio::test]
    async fn reassert_replay_does_not_undo_a_newer_connection_here() {
        let pool = pool().await;
        let (c0, c1) = (cluster(&pool, "hbbs-0", 1).await, cluster(&pool, "hbbs-1", 1).await);
        let (held, calls) = (&held(&[]), &AtomicUsize::new(0));
        let c = &c0;
        c0.reassert(move || async move {
            let devices = snapshot(held);
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    held.lock().unwrap().insert("x".to_owned(), 2);
                    c.presence.register("x", 2).await;
                }
                1 => {
                    // Reconnects here before the replay of the older connection.
                    held.lock().unwrap().insert("x".to_owned(), 3);
                    c.presence.register("x", 3).await;
                }
                _ => {}
            }
            devices
        })
        .await;
        assert_eq!(c1.presence.holder("x").await.map(|r| r.pod).as_deref(), Some("hbbs-0"));
    }

    #[tokio::test]
    async fn reassert_replay_does_not_take_a_device_from_a_live_pod() {
        let pool = pool().await;
        let (c0, c1) = (cluster(&pool, "hbbs-0", 1).await, cluster(&pool, "hbbs-1", 1).await);
        let (held, calls) = (&held(&[]), &AtomicUsize::new(0));
        let (c, other) = (&c0, &c1);
        c0.reassert(move || async move {
            let devices = snapshot(held);
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    held.lock().unwrap().insert("x".to_owned(), 2);
                    c.presence.register("x", 2).await;
                }
                1 => {
                    // Moves to another pod before the replay.
                    held.lock().unwrap().remove("x");
                    c.presence.unregister("x", 2).await;
                    other.presence.register("x", 5).await;
                }
                _ => {}
            }
            devices
        })
        .await;
        assert_eq!(c0.presence.holder("x").await.map(|r| r.pod).as_deref(), Some("hbbs-1"));
    }

    #[tokio::test]
    async fn stop_does_not_wait_forever_for_a_hung_heartbeat() {
        let pool = pool().await;
        let c0 = cluster(&pool, "hbbs-0", 1).await;
        let (started_tx, mut started) = mpsc::unbounded_channel();
        c0.run_heartbeat(move || {
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).ok();
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        started.recv().await;
        let bound = HEARTBEAT_STOP_TIMEOUT + Duration::from_secs(2);
        assert!(tokio::time::timeout(bound, c0.stop()).await.is_ok(), "stop hung on the heartbeat");
        assert_eq!(pod_rows(&pool, "hbbs-0").await, 0);
    }

    async fn pod_rows(pool: &PgPool, pod: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM hbbs_pod WHERE pod = $1")
            .bind(pod)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn stop_waits_for_a_running_heartbeat() {
        let pool = pool().await;
        let c0 = Arc::new(cluster(&pool, "hbbs-0", 1).await);
        let (started_tx, mut started) = mpsc::unbounded_channel();
        let c = c0.clone();
        c0.run_heartbeat(move || {
            let (c, started_tx) = (c.clone(), started_tx.clone());
            async move {
                started_tx.send(()).ok();
                tokio::time::sleep(Duration::from_millis(300)).await;
                c.presence.heartbeat().await;
            }
        });
        started.recv().await;
        c0.stop().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(pod_rows(&pool, "hbbs-0").await, 0, "a late heartbeat wrote the pod row back");
    }

    #[tokio::test]
    async fn stop_ends_an_idle_heartbeat_at_once() {
        let pool = pool().await;
        let c0 = cluster(&pool, "hbbs-0", 1).await;
        let (ticks_tx, mut ticks) = mpsc::unbounded_channel();
        c0.run_heartbeat(move || {
            let ticks_tx = ticks_tx.clone();
            async move {
                ticks_tx.send(()).ok();
            }
        });
        ticks.recv().await;
        assert!(tokio::time::timeout(Duration::from_secs(1), c0.stop()).await.is_ok());
        assert!(c0.is_draining());
        assert_eq!(pod_rows(&pool, "hbbs-0").await, 0);
        assert!(ticks.recv().await.is_none(), "the heartbeat task ended");
    }
}
