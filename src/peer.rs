use crate::common::*;
use crate::database;
use hbb_common::{
    bytes::Bytes,
    log,
    rendezvous_proto::*,
    tokio::sync::{Mutex, RwLock},
    ResultType,
};
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, collections::HashSet, net::SocketAddr, sync::Arc, time::Instant};

type IpBlockMap = HashMap<String, ((u32, Instant), (HashSet<String>, Instant))>;
type UserStatusMap = HashMap<Vec<u8>, Arc<(Option<Vec<u8>>, bool)>>;
type IpChangesMap = HashMap<String, (Instant, HashMap<String, i32>)>;
lazy_static::lazy_static! {
    pub(crate) static ref IP_BLOCKER: Mutex<IpBlockMap> = Default::default();
    pub(crate) static ref USER_STATUS: RwLock<UserStatusMap> = Default::default();
    pub(crate) static ref IP_CHANGES: Mutex<IpChangesMap> = Default::default();
}
pub const IP_CHANGE_DUR: u64 = 180;
pub const IP_CHANGE_DUR_X2: u64 = IP_CHANGE_DUR * 2;
pub const DAY_SECONDS: u64 = 3600 * 24;
pub const IP_BLOCK_DUR: u64 = 60;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct PeerInfo {
    #[serde(default)]
    pub(crate) ip: String,
}

pub(crate) struct Peer {
    pub(crate) socket_addr: SocketAddr,
    pub(crate) last_reg_time: Instant,
    pub(crate) guid: Vec<u8>,
    pub(crate) uuid: Bytes,
    pub(crate) pk: Bytes,
    // pub(crate) user: Option<Vec<u8>>,
    pub(crate) info: PeerInfo,
    // pub(crate) disabled: bool,
    pub(crate) reg_pk: (u32, Instant), // how often register_pk
}

impl Default for Peer {
    fn default() -> Self {
        Self {
            socket_addr: "0.0.0.0:0".parse().unwrap(),
            last_reg_time: get_expired_time(),
            guid: Vec::new(),
            uuid: Bytes::new(),
            pk: Bytes::new(),
            info: Default::default(),
            // user: None,
            // disabled: false,
            reg_pk: (0, get_expired_time()),
        }
    }
}

pub(crate) type LockPeer = Arc<RwLock<Peer>>;

#[derive(Clone)]
pub(crate) struct PeerMap {
    map: Arc<RwLock<HashMap<String, LockPeer>>>,
    pub(crate) db: database::Database,
}

impl PeerMap {
    pub(crate) async fn new() -> ResultType<Self> {
        let db_url = std::env::var("DB_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                hbb_common::anyhow::anyhow!("DB_URL is required (postgres://user:pass@host:5432/db)")
            })?;
        Ok(Self {
            map: Default::default(),
            db: database::Database::connect_with_retry(&db_url).await,
        })
    }

    #[inline]
    pub(crate) async fn update_pk(
        &mut self,
        id: String,
        peer: LockPeer,
        addr: SocketAddr,
        uuid: Bytes,
        pk: Bytes,
        ip: String,
    ) -> register_pk_response::Result {
        log::info!("update_pk {} {:?} {:?} {:?}", id, addr, uuid, pk);
        let (info_str, guid) = {
            let mut w = peer.write().await;
            w.socket_addr = addr;
            w.uuid = uuid.clone();
            w.pk = pk.clone();
            w.last_reg_time = Instant::now();
            w.info.ip = ip;
            (
                serde_json::to_string(&w.info).unwrap_or_default(),
                w.guid.clone(),
            )
        };
        if guid.is_empty() {
            match self.db.insert_peer(&id, &uuid, &pk, &info_str).await {
                Err(err) => {
                    log::error!("db.insert_peer failed: {}", err);
                    return register_pk_response::Result::SERVER_ERROR;
                }
                Ok(guid) => {
                    peer.write().await.guid = guid;
                }
            }
        } else {
            if let Err(err) = self.db.update_pk(&guid, &id, &pk, &info_str).await {
                log::error!("db.update_pk failed: {}", err);
                return register_pk_response::Result::SERVER_ERROR;
            }
            log::info!("pk updated instead of insert");
        }
        register_pk_response::Result::OK
    }

    #[inline]
    pub(crate) async fn get(&self, id: &str) -> Option<LockPeer> {
        let p = self.map.read().await.get(id).cloned();
        if p.is_some() {
            return p;
        }
        let db_peer = match self.db.get_peer(id).await {
            Ok(v) => v,
            Err(err) => {
                log::error!("get_peer({id}) failed: {err}");
                return None;
            }
        };
        if let Some(v) = db_peer {
            let peer = Peer {
                guid: v.guid,
                uuid: v.uuid.into(),
                pk: v.pk.into(),
                // user: v.user,
                info: serde_json::from_str::<PeerInfo>(&v.info).unwrap_or_default(),
                // disabled: v.status == Some(0),
                ..Default::default()
            };
            let peer = Arc::new(RwLock::new(peer));
            self.map.write().await.insert(id.to_owned(), peer.clone());
            return Some(peer);
        }
        None
    }

    #[inline]
    pub(crate) async fn get_or(&self, id: &str) -> LockPeer {
        if let Some(p) = self.get(id).await {
            return p;
        }
        let mut w = self.map.write().await;
        if let Some(p) = w.get(id) {
            return p.clone();
        }
        let tmp = LockPeer::default();
        w.insert(id.to_owned(), tmp.clone());
        tmp
    }

    #[inline]
    pub(crate) async fn get_in_memory(&self, id: &str) -> Option<LockPeer> {
        self.map.read().await.get(id).cloned()
    }

    #[inline]
    pub(crate) async fn is_in_memory(&self, id: &str) -> bool {
        self.map.read().await.contains_key(id)
    }

    pub fn new_with_db(db: database::Database) -> Self {
        PeerMap {
            map: Default::default(),
            db,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn temp_peer_map() -> PeerMap {
        let db = database::Database::new(&crate::testing::fresh_peer_database_url().await)
            .await
            .unwrap();
        PeerMap::new_with_db(db)
    }

    #[tokio::test]
    async fn peer_map_requires_db_url() {
        std::env::remove_var("DB_URL");
        let err = PeerMap::new().await.err().expect("expected error without DB_URL");
        assert!(err.to_string().contains("DB_URL is required"), "{err}");
    }

    #[tokio::test]
    async fn test_get_returns_none_for_unknown_peer() {
        let pm = temp_peer_map().await;
        assert!(pm.get("unknown_peer").await.is_none());
    }

    #[tokio::test]
    async fn test_get_or_creates_default_peer() {
        let pm = temp_peer_map().await;
        let peer = pm.get_or("new_peer").await;
        let r = peer.read().await;
        assert!(r.uuid.is_empty());
        assert!(r.pk.is_empty());
        assert!(r.guid.is_empty());
    }

    #[tokio::test]
    async fn test_get_or_returns_same_peer_on_second_call() {
        let pm = temp_peer_map().await;
        let p1 = pm.get_or("peer1").await;
        let p2 = pm.get_or("peer1").await;
        assert!(Arc::ptr_eq(&p1, &p2));
    }

    #[tokio::test]
    async fn test_is_in_memory_after_get_or() {
        let pm = temp_peer_map().await;
        assert!(!pm.is_in_memory("peer2").await);
        let _ = pm.get_or("peer2").await;
        assert!(pm.is_in_memory("peer2").await);
    }

    #[tokio::test]
    async fn test_get_in_memory_returns_none_before_insert() {
        let pm = temp_peer_map().await;
        assert!(pm.get_in_memory("peer3").await.is_none());
    }

    #[tokio::test]
    async fn test_get_in_memory_returns_some_after_get_or() {
        let pm = temp_peer_map().await;
        let _ = pm.get_or("peer4").await;
        assert!(pm.get_in_memory("peer4").await.is_some());
    }

    #[tokio::test]
    async fn test_update_pk_inserts_new_peer_to_db() {
        let mut pm = temp_peer_map().await;
        let peer = pm.get_or("peer5").await;
        let uuid = Bytes::from_static(b"test-uuid-bytes!");
        let pk = Bytes::from_static(b"test-pk-bytes!!!");
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();

        let result = pm.update_pk(
            "peer5".to_owned(),
            peer.clone(),
            addr,
            uuid,
            pk,
            "10.0.0.1".to_owned(),
        ).await;
        assert_eq!(result, register_pk_response::Result::OK);

        let r = peer.read().await;
        assert!(!r.guid.is_empty());
        assert_eq!(r.socket_addr, addr);
        assert_eq!(r.info.ip, "10.0.0.1");
    }

    #[tokio::test]
    async fn test_update_pk_updates_existing_peer() {
        let mut pm = temp_peer_map().await;
        let peer = pm.get_or("peer6").await;
        let uuid = Bytes::from_static(b"test-uuid-bytes!");
        let pk1 = Bytes::from_static(b"test-pk1-bytes!!");
        let pk2 = Bytes::from_static(b"test-pk2-bytes!!");
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();

        // First insert
        let result = pm.update_pk(
            "peer6".to_owned(),
            peer.clone(),
            addr,
            uuid.clone(),
            pk1,
            "10.0.0.1".to_owned(),
        ).await;
        assert_eq!(result, register_pk_response::Result::OK);

        // Second update (guid is now set)
        let result = pm.update_pk(
            "peer6".to_owned(),
            peer.clone(),
            addr,
            uuid,
            pk2.clone(),
            "10.0.0.2".to_owned(),
        ).await;
        assert_eq!(result, register_pk_response::Result::OK);

        let r = peer.read().await;
        assert_eq!(r.pk, pk2);
        assert_eq!(r.info.ip, "10.0.0.2");
    }

    #[tokio::test]
    async fn test_get_loads_peer_from_db() {
        let mut pm = temp_peer_map().await;
        let peer = pm.get_or("db_peer").await;
        let uuid = Bytes::from_static(b"test-uuid-bytes!");
        let pk = Bytes::from_static(b"test-pk-bytes!!!");
        let addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();

        pm.update_pk(
            "db_peer".to_owned(),
            peer,
            addr,
            uuid.clone(),
            pk.clone(),
            "10.0.0.1".to_owned(),
        ).await;

        // Clear in-memory map to force DB lookup
        pm.map.write().await.clear();
        assert!(!pm.is_in_memory("db_peer").await);

        // get() should load from DB
        let loaded = pm.get("db_peer").await;
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        let r = loaded.read().await;
        assert_eq!(r.uuid, uuid);
        assert_eq!(r.pk, pk);
    }

    #[tokio::test]
    async fn test_peer_default() {
        let peer = Peer::default();
        assert_eq!(peer.socket_addr, "0.0.0.0:0".parse::<SocketAddr>().unwrap());
        assert!(peer.uuid.is_empty());
        assert!(peer.pk.is_empty());
        assert!(peer.guid.is_empty());
        assert!(peer.info.ip.is_empty());
    }

    #[tokio::test]
    async fn test_peer_info_serialization() {
        let info = PeerInfo { ip: "192.168.1.1".to_owned() };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("192.168.1.1"));

        let deserialized: PeerInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.ip, "192.168.1.1");
    }

    #[tokio::test]
    async fn test_peer_info_default() {
        let info = PeerInfo::default();
        assert!(info.ip.is_empty());

        let deserialized: PeerInfo = serde_json::from_str("{}").unwrap();
        assert!(deserialized.ip.is_empty());
    }
}
