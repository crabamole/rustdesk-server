//! Device admission (rustdesk docs/design-device-admission.md): with DEVICE_ADMISSION=Y only
//! admitted devices can be controlled. The api-server owns the state; hbbs reads it.
use crate::database::Database;
use hbb_common::tokio::sync::Mutex;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

pub(crate) const PENDING: i16 = 0;
pub(crate) const ADMITTED: i16 = 1;
/// How long a looked-up state is used before hbbs reads it again.
const CACHE_TTL: Duration = Duration::from_secs(5);
pub(crate) const NOT_ADMITTED: &str = "The connection is not allowed. This device is not admitted for remote control.";
pub(crate) const CHECK_FAILED: &str = "The connection is not allowed. The server cannot check this device now.";

#[derive(Clone, Default)]
pub(crate) struct DeviceAdmission {
    enabled: bool,
    /// Known IDs only, so the size is bounded by the device count.
    cache: Arc<Mutex<HashMap<String, (bool, Instant)>>>,
}

impl DeviceAdmission {
    pub(crate) fn from_env() -> Self {
        Self::new(std::env::var("DEVICE_ADMISSION").is_ok_and(|v| v.eq_ignore_ascii_case("Y")))
    }

    pub(crate) fn new(enabled: bool) -> Self {
        Self { enabled, cache: Default::default() }
    }

    /// The state hbbs gives a device it registers for the first time.
    pub(crate) fn new_device_state(&self) -> i16 {
        if self.enabled { PENDING } else { ADMITTED }
    }

    /// Why a viewer may not connect to device `id`, or None. Unknown IDs pass: the existing
    /// checks answer them. A database error refuses (fail closed).
    pub(crate) async fn refusal(&self, db: &Database, id: &str) -> Option<&'static str> {
        if !self.enabled {
            return None;
        }
        if let Some((admitted, at)) = self.cache.lock().await.get(id).copied() {
            if at.elapsed() < CACHE_TTL {
                return (!admitted).then_some(NOT_ADMITTED);
            }
        }
        match db.peer_admission(id).await {
            Ok(None) => None,
            Ok(Some(state)) => {
                let admitted = state == ADMITTED;
                self.cache.lock().await.insert(id.to_owned(), (admitted, Instant::now()));
                (!admitted).then_some(NOT_ADMITTED)
            }
            Err(e) => {
                hbb_common::throttled_log!(Duration::from_secs(10), error, "device admission check failed: {e}");
                Some(CHECK_FAILED)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn db_with(id: &str, admission: i16) -> Database {
        let db = Database::new(&crate::testing::fresh_peer_database_url().await).await.unwrap();
        db.insert_peer(id, b"u", b"pk", "{}", admission).await.unwrap();
        db
    }

    #[tokio::test]
    async fn off_allows_everything() {
        let db = db_with("d_off", PENDING).await;
        assert_eq!(DeviceAdmission::new(false).refusal(&db, "d_off").await, None);
        assert_eq!(DeviceAdmission::new(false).new_device_state(), ADMITTED);
    }

    #[tokio::test]
    async fn on_refuses_pending_and_rejected_only() {
        let db = db_with("d_pending", PENDING).await;
        db.insert_peer("d_rejected", b"u2", b"pk", "{}", 2).await.unwrap();
        db.insert_peer("d_admitted", b"u3", b"pk", "{}", ADMITTED).await.unwrap();
        let a = DeviceAdmission::new(true);
        assert_eq!(a.new_device_state(), PENDING);
        assert_eq!(a.refusal(&db, "d_pending").await, Some(NOT_ADMITTED));
        assert_eq!(a.refusal(&db, "d_rejected").await, Some(NOT_ADMITTED));
        assert_eq!(a.refusal(&db, "d_admitted").await, None);
        assert_eq!(a.refusal(&db, "d_unknown").await, None);
    }

    #[tokio::test]
    async fn admitting_takes_effect_after_the_cache_time() {
        let db = db_with("d_late", PENDING).await;
        let a = DeviceAdmission::new(true);
        assert_eq!(a.refusal(&db, "d_late").await, Some(NOT_ADMITTED));
        sqlx::query("UPDATE peer SET admission = 1 WHERE id = 'd_late'").execute(db.pool()).await.unwrap();
        assert_eq!(a.refusal(&db, "d_late").await, Some(NOT_ADMITTED), "cached");
        a.cache.lock().await.get_mut("d_late").unwrap().1 -= CACHE_TTL;
        assert_eq!(a.refusal(&db, "d_late").await, None);
    }

    #[tokio::test]
    async fn on_refuses_when_the_database_fails() {
        let db = db_with("d_db_down", ADMITTED).await;
        db.pool().close().await;
        assert_eq!(DeviceAdmission::new(true).refusal(&db, "d_db_down").await, Some(CHECK_FAILED));
    }
}
