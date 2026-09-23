use hbb_common::{log, ResultType};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    Connection, PgConnection, PgPool, Row,
};
use std::time::Duration;

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

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
        if let Err(e) = sqlx::query("SELECT 1 FROM peer LIMIT 1").fetch_optional(&pool).await {
            pool.close().await;
            hbb_common::bail!("schema not ready (table peer not found; it is created by the api-server): {e}");
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
                    log::warn!("database not ready ({e}), retrying in {}s", delay.as_secs());
                    hbb_common::tokio::time::sleep(delay).await;
                }
            }
        }
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

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        sqlx::query(
            "INSERT INTO peer(guid, id, uuid, pk, info) VALUES($1, $2, $3, $4, $5)",
        )
        .bind(&guid)
        .bind(id)
        .bind(uuid)
        .bind(pk)
        .bind(info)
        .execute(&self.pool)
        .await?;
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        sqlx::query("UPDATE peer SET id=$1, pk=$2, info=$3 WHERE guid=$4")
            .bind(id)
            .bind(pk)
            .bind(info)
            .bind(guid)
            .execute(&self.pool)
            .await?;
        Ok(())
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
        let guid = db.insert_peer("status_peer", b"u", b"p", "{}").await.unwrap();
        let peer = db.get_peer("status_peer").await.unwrap().unwrap();
        assert_eq!(peer.status, 1, "status smallint default 1 decoded wrong");
        assert_eq!(peer.user, None);
        assert_eq!(peer.guid, guid);
    });

    db_test!(insert_and_get_peer, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";
        let info = r#"{"ip":"10.0.0.1"}"#;

        let guid = db.insert_peer("peer123", uuid, pk, info).await.unwrap();
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

        let guid = db.insert_peer("peer456", uuid, pk, info).await.unwrap();

        let new_pk = b"new-pk-bytes!!!!!";
        let new_info = r#"{"ip":"10.0.0.2"}"#;
        db.update_pk(&guid, "peer456", new_pk, new_info).await.unwrap();

        let peer = db.get_peer("peer456").await.unwrap().unwrap();
        assert_eq!(peer.pk, new_pk.to_vec());
        assert_eq!(peer.info, new_info);
    });

    db_test!(insert_duplicate_id_fails, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";

        db.insert_peer("dup_id", uuid, pk, "{}").await.unwrap();
        let result = db.insert_peer("dup_id", uuid, pk, "{}").await;
        assert!(result.is_err());
    });

    db_test!(update_pk_changes_id, |db| {
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";

        let guid = db.insert_peer("old_id", uuid, pk, "{}").await.unwrap();
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
                cloned.insert_peer(&id, &empty_vec, &empty_vec, "").await.unwrap();
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
