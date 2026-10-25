use hbb_common::{bail, log, ResultType};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    Connection, PgConnection, PgPool, Row,
};
use std::time::Duration;

const SCHEMA_TABLES: [&str; 3] = ["peer", "hbbs_pod", "peer_presence"];

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

/// Marks a `new()` failure as "connected fine, but the api-server hasn't
/// created the schema yet", as opposed to any other failure (e.g. Postgres
/// unreachable). Kept as a distinct type instead of matching on the error
/// text so `connect_with_retry` can tell the two apart reliably.
#[derive(Debug)]
struct SchemaNotReady(sqlx::Error);

impl std::fmt::Display for SchemaNotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "schema not ready (tables peer, hbbs_pod and peer_presence are created by the api-server): {}",
            self.0
        )
    }
}

impl std::error::Error for SchemaNotReady {}

#[derive(Default)]
pub struct Peer {
    pub guid: Vec<u8>,
    pub id: String,
    pub uuid: Vec<u8>,
    pub pk: Vec<u8>,
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub status: i64,
}

impl Peer {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            guid: row.try_get::<Vec<u8>, _>("guid")?,
            id: row.try_get::<String, _>("id")?,
            uuid: row.try_get::<Vec<u8>, _>("uuid")?,
            pk: row.try_get::<Vec<u8>, _>("pk")?,
            user: row.try_get::<Option<Vec<u8>>, _>("user")?,
            info: row.try_get::<String, _>("info")?,
            status: row.try_get::<i16, _>("status")? as i64,
        })
    }
}

impl Database {
    /// One attempt: connect and verify the api-server has created the schema.
    /// hbbs never creates tables.
    pub async fn new(url: &str) -> ResultType<Database> {
        // A plain connection first surfaces the real error (e.g. connection refused);
        // the pool would hide it behind its acquire timeout.
        PgConnection::connect(url).await?.close().await?;
        let max_connections: u32 = std::env::var("MAX_DATABASE_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or((num_cpus::get() * 4) as u32);
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        for table in SCHEMA_TABLES {
            let sql = format!("SELECT 1 FROM {table} LIMIT 1");
            if let Err(e) = sqlx::query(&sql).fetch_optional(&pool).await {
                pool.close().await;
                return Err(SchemaNotReady(e).into());
            }
        }
        log::info!("Database ready, max_connections={max_connections}");
        Ok(Database { pool })
    }

    /// Connect and wait for the schema, retrying until it succeeds. Kubernetes'
    /// startupProbe decides when to give up and restart the container.
    pub async fn connect_with_retry(url: &str) -> Database {
        let mut backoff = crate::retry::Backoff::new();
        loop {
            match Self::new(url).await {
                Ok(db) => return db,
                Err(e) => {
                    let delay = backoff.next_delay();
                    log::warn!("{}", retry_log_message(&e, delay));
                    hbb_common::tokio::time::sleep(delay).await;
                }
            }
        }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        let row = sqlx::query(
            "SELECT guid, id, uuid, pk, \"user\", status, info FROM peer WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => Ok(Some(Peer::from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Inserts a new device, or takes the row another pod just inserted for the same id and uuid.
    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
        admission: i16,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "INSERT INTO peer(guid, id, uuid, pk, info, admission) VALUES($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET pk = EXCLUDED.pk, \
             info = (COALESCE(NULLIF(peer.info, ''), '{}')::jsonb \
                     || COALESCE(NULLIF(EXCLUDED.info, ''), '{}')::jsonb)::text \
             WHERE peer.uuid = EXCLUDED.uuid \
             RETURNING guid",
        )
        .bind(&guid)
        .bind(id)
        .bind(uuid)
        .bind(pk)
        .bind(info)
        .bind(admission)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((guid,)) => Ok(guid),
            None => bail!("peer {id} is registered with another uuid"),
        }
    }

    /// Device `id`'s admission state (rustdesk docs/design-device-admission.md); None for an unknown ID.
    pub async fn peer_admission(&self, id: &str) -> ResultType<Option<i16>> {
        Ok(sqlx::query_scalar("SELECT admission FROM peer WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    /// Stores `pk` as the key of device `guid` when its owner approved that key
    /// (`approved_pk`); the approval is used up.
    pub async fn take_approved_pk(&self, guid: &[u8], pk: &[u8]) -> ResultType<bool> {
        let r = sqlx::query("UPDATE peer SET pk = $2, approved_pk = NULL WHERE guid = $1 AND approved_pk = $2")
            .bind(guid)
            .bind(pk)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() == 1)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        // Merge hbbs's info (e.g. ip) into the stored JSON so the api-server's
        // system info (hostname, os, cpu, ...) survives an address change.
        sqlx::query(
            "UPDATE peer SET id=$1, pk=$2, \
             info = (COALESCE(NULLIF(info, ''), '{}')::jsonb || $3::jsonb)::text \
             WHERE guid=$4",
        )
            .bind(id)
            .bind(pk)
            .bind(info)
            .bind(guid)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// Log text for a `connect_with_retry` attempt that failed with `e`, about to
/// retry after `delay`. Distinguishes "connected fine, schema not created
/// yet" (`SchemaNotReady`, api-server's job) from any other failure (e.g.
/// Postgres unreachable) by error type rather than by matching on text.
fn retry_log_message(e: &hbb_common::anyhow::Error, delay: Duration) -> String {
    if e.downcast_ref::<SchemaNotReady>().is_some() {
        format!(
            "waiting for schema (created by api-server), retrying in {}s",
            delay.as_secs()
        )
    } else {
        format!("database not ready ({e}), retrying in {}s", delay.as_secs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn test_db() -> Database {
        Database::new(&crate::testing::fresh_peer_database_url().await)
            .await
            .unwrap()
    }

    macro_rules! db_test {
        ($name:ident, |$db:ident| $body:block) => {
            #[tokio::test]
            async fn $name() {
                let $db = test_db().await;
                $body
            }
        };
    }

    #[test]
    fn retry_log_message_distinguishes_schema_not_ready() {
        let schema_err: hbb_common::anyhow::Error = SchemaNotReady(sqlx::Error::RowNotFound).into();
        let msg = retry_log_message(&schema_err, Duration::from_secs(2));
        assert_eq!(msg, "waiting for schema (created by api-server), retrying in 2s");

        let other_err = hbb_common::anyhow::anyhow!("connection refused");
        let msg = retry_log_message(&other_err, Duration::from_secs(2));
        assert_eq!(msg, "database not ready (connection refused), retrying in 2s");
    }

    #[tokio::test]
    async fn new_fails_when_schema_missing() {
        // Empty database: hbbs must not create tables, it must report the schema is not ready.
        let url = crate::testing::fresh_database_url().await;
        let err = Database::new(&url).await.err().expect("expected error on empty database");
        assert!(err.to_string().contains("schema not ready"), "{err}");
        // And it must not have created the table.
        let db_check = sqlx::PgPool::connect(&url).await.unwrap();
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'peer')",
        )
        .fetch_one(&db_check)
        .await
        .unwrap();
        assert!(!exists.0, "hbbs created table peer");
    }

    db_test!(status_and_user_decode, |db| {
        let guid = db.insert_peer("status_peer", b"u", b"p", "{}", 1).await.unwrap();
        let peer = db.get_peer("status_peer").await.unwrap().unwrap();
        assert_eq!(peer.status, 1, "status smallint default 1 decoded wrong");
        assert_eq!(peer.user, None);
        assert_eq!(peer.guid, guid);
    });

    db_test!(insert_and_get_peer, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";
        let info = r#"{"ip":"10.0.0.1"}"#;

        let guid = db.insert_peer("peer123", uuid, pk, info, 1).await.unwrap();
        assert!(!guid.is_empty());

        let peer = db.get_peer("peer123").await.unwrap().unwrap();
        assert_eq!(peer.id, "peer123");
        assert_eq!(peer.uuid, uuid.to_vec());
        assert_eq!(peer.pk, pk.to_vec());
        assert_eq!(peer.info, info);
    });

    db_test!(get_peer_not_found, |db| {
        let result = db.get_peer("nonexistent").await.unwrap();
        assert!(result.is_none());
    });

    db_test!(update_pk, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";
        let info = r#"{"ip":"10.0.0.1"}"#;

        let guid = db.insert_peer("peer456", uuid, pk, info, 1).await.unwrap();

        let new_pk = b"new-pk-bytes!!!!!";
        let new_info = r#"{"ip":"10.0.0.2"}"#;
        db.update_pk(&guid, "peer456", new_pk, new_info).await.unwrap();

        let peer = db.get_peer("peer456").await.unwrap().unwrap();
        assert_eq!(peer.pk, new_pk.to_vec());
        // info now round-trips through jsonb (merge), so compare parsed value, not raw text.
        let got: serde_json::Value = serde_json::from_str(&peer.info).unwrap();
        let want: serde_json::Value = serde_json::from_str(new_info).unwrap();
        assert_eq!(got, want);
    });

    db_test!(update_pk_merges_info_keeping_existing_keys, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";
        let info = r#"{"hostname":"ws-1","ip":"10.0.0.1"}"#;

        let guid = db.insert_peer("peer789", uuid, pk, info, 1).await.unwrap();

        let new_pk = b"new-pk-bytes!!!!!";
        let new_info = r#"{"ip":"203.0.113.5"}"#;
        db.update_pk(&guid, "peer789", new_pk, new_info).await.unwrap();

        let peer = db.get_peer("peer789").await.unwrap().unwrap();
        let merged: serde_json::Value = serde_json::from_str(&peer.info).unwrap();
        assert_eq!(merged["hostname"], "ws-1");
        assert_eq!(merged["ip"], "203.0.113.5");
    });

    db_test!(update_pk_merges_info_when_existing_info_is_empty, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";

        let guid = db.insert_peer("peer_empty_info", uuid, pk, "", 1).await.unwrap();

        let new_pk = b"new-pk-bytes!!!!!";
        let new_info = r#"{"ip":"203.0.113.5"}"#;
        db.update_pk(&guid, "peer_empty_info", new_pk, new_info).await.unwrap();

        let peer = db.get_peer("peer_empty_info").await.unwrap().unwrap();
        let merged: serde_json::Value = serde_json::from_str(&peer.info).unwrap();
        assert_eq!(merged, serde_json::json!({"ip":"203.0.113.5"}));
    });

    #[tokio::test]
    async fn new_fails_without_presence_tables() {
        let url = crate::testing::fresh_database_url().await;
        let mut c = <sqlx::PgConnection as sqlx::Connection>::connect(&url).await.unwrap();
        sqlx::Executor::execute(&mut c, include_str!("../tests/fixtures/peer.sql")).await.unwrap();
        let err = Database::new(&url).await.err().expect("presence tables are missing");
        assert!(err.to_string().contains("schema not ready"), "{err}");
    }

    db_test!(insert_same_id_and_uuid_keeps_the_row, |db| {
        let first = db.insert_peer("dup_id", b"uuid-1", b"pk-1", r#"{"ip":"10.0.0.1"}"#, 1).await.unwrap();
        let second = db.insert_peer("dup_id", b"uuid-1", b"pk-2", r#"{"ip":"10.0.0.2"}"#, 1).await.unwrap();
        assert_eq!(first, second);
        let peer = db.get_peer("dup_id").await.unwrap().unwrap();
        assert_eq!(peer.pk, b"pk-2".to_vec());
        let info: serde_json::Value = serde_json::from_str(&peer.info).unwrap();
        assert_eq!(info["ip"], "10.0.0.2");
    });

    db_test!(insert_same_id_other_uuid_fails, |db| {
        db.insert_peer("dup_id2", b"uuid-1", b"pk", "{}", 1).await.unwrap();
        assert!(db.insert_peer("dup_id2", b"uuid-2", b"pk", "{}", 1).await.is_err());
        assert_eq!(db.get_peer("dup_id2").await.unwrap().unwrap().uuid, b"uuid-1".to_vec());
    });

    db_test!(insert_peer_sets_admission_once, |db| {
        db.insert_peer("adm_new", b"uuid-a", b"pk-1", "{}", 0).await.unwrap();
        assert_eq!(db.peer_admission("adm_new").await.unwrap(), Some(0));
        // Same device registering again (e.g. after an hbbs restart) keeps its state.
        db.insert_peer("adm_new", b"uuid-a", b"pk-1", "{}", 1).await.unwrap();
        assert_eq!(db.peer_admission("adm_new").await.unwrap(), Some(0));
        assert_eq!(db.peer_admission("nobody").await.unwrap(), None);
    });

    db_test!(racing_first_registrations_agree, |db| {
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let db = db.clone();
                tokio::spawn(async move { db.insert_peer("race_id", b"u", b"p", "{}", 1).await.unwrap() })
            })
            .collect();
        let guids: std::collections::HashSet<Vec<u8>> = hbb_common::futures::future::join_all(jobs)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(guids.len(), 1);
    });

    db_test!(update_pk_changes_id, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";

        let guid = db.insert_peer("old_id", uuid, pk, "{}", 1).await.unwrap();
        db.update_pk(&guid, "new_id", pk, "{}").await.unwrap();

        let old = db.get_peer("old_id").await.unwrap();
        assert!(old.is_none());

        let new = db.get_peer("new_id").await.unwrap();
        assert!(new.is_some());
    });

    #[tokio::test]
    async fn new_reports_real_cause_when_unreachable() {
        let err = hbb_common::tokio::time::timeout(
            std::time::Duration::from_secs(10),
            Database::new("postgres://postgres:postgres@127.0.0.1:1/none"),
        )
        .await
        .expect("Database::new did not return within 10s")
        .err()
        .expect("expected error");
        assert!(!err.to_string().contains("pool timed out"), "{err}");
    }

    #[tokio::test]
    async fn connect_with_retry_waits_for_schema() {
        let url = crate::testing::fresh_database_url().await;
        let url2 = url.clone();
        // Create the schema 2s after hbbs starts waiting, as the api-server would.
        hbb_common::tokio::spawn(async move {
            hbb_common::tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let mut c = <sqlx::PgConnection as sqlx::Connection>::connect(&url2).await.unwrap();
            sqlx::Executor::execute(&mut c, include_str!("../tests/fixtures/peer.sql")).await.unwrap();
            sqlx::Executor::execute(&mut c, include_str!("../tests/fixtures/hbbs_presence.sql")).await.unwrap();
        });
        let db = hbb_common::tokio::time::timeout(
            std::time::Duration::from_secs(20),
            Database::connect_with_retry(&url),
        )
        .await
        .expect("connect_with_retry did not return after the schema appeared");
        assert!(db.get_peer("nobody").await.unwrap().is_none());
    }

    db_test!(concurrent_insert_and_read, |db| {
        let mut jobs = vec![];
        for i in 0..100 {
            let cloned = db.clone();
            let id = i.to_string();
            jobs.push(tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned.insert_peer(&id, &empty_vec, &empty_vec, "", 1).await.unwrap();
            }));
        }
        for i in 0..100 {
            let cloned = db.clone();
            let id = i.to_string();
            jobs.push(tokio::spawn(async move {
                cloned.get_peer(&id).await.unwrap();
            }));
        }
        hbb_common::futures::future::join_all(jobs).await;
    });
}
