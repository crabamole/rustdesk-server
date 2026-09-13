use hbb_common::{log, ResultType};
use sqlx::{
    any::{install_default_drivers, AnyPoolOptions, AnyRow},
    AnyPool, Row,
};

const SCHEMA_SQLITE: &str = include_str!("../db_v2/create/db_sqlite.sql");
const SCHEMA_POSTGRES: &str = include_str!("../db_v2/create/db_postgres.sql");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Sqlite,
    Postgres,
}

#[derive(Clone)]
pub struct Database {
    pool: AnyPool,
    backend: Backend,
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
    fn from_row(row: &AnyRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            guid: row.try_get::<Vec<u8>, _>("guid")?,
            id: row.try_get::<String, _>("id")?,
            uuid: row.try_get::<Vec<u8>, _>("uuid")?,
            pk: row.try_get::<Vec<u8>, _>("pk")?,
            user: row.try_get::<Vec<u8>, _>("user").ok(),
            info: row.try_get::<String, _>("info")?,
            status: row.try_get::<i64, _>("status").unwrap_or(0),
        })
    }
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        install_default_drivers();

        let url = normalize_url(url);
        let backend = if url.starts_with("postgres") {
            Backend::Postgres
        } else {
            Backend::Sqlite
        };

        if backend == Backend::Sqlite {
            let path = url.strip_prefix("sqlite://").unwrap_or(&url);
            if !std::path::Path::new(path).exists() {
                std::fs::File::create(path).ok();
            }
        }

        let max_connections: u32 = std::env::var("MAX_DATABASE_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or((num_cpus::get() * 4) as u32);
        log::info!(
            "Database backend={:?}, max_connections={}",
            backend,
            max_connections
        );

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .connect(&url)
            .await?;

        let db = Database { pool, backend };
        db.create_tables().await?;
        Ok(db)
    }

    pub fn backend(&self) -> Backend {
        self.backend
    }

    async fn create_tables(&self) -> ResultType<()> {
        let schema = match self.backend {
            Backend::Sqlite => SCHEMA_SQLITE,
            Backend::Postgres => SCHEMA_POSTGRES,
        };
        for statement in split_sql(schema) {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
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

/// Normalize a database URL for sqlx::Any.
/// Bare file paths (legacy) are converted to sqlite:// URLs.
fn normalize_url(url: &str) -> String {
    if url.starts_with("sqlite://") || url.starts_with("postgres://") || url.starts_with("postgresql://") {
        url.to_string()
    } else {
        format!("sqlite://{}", url)
    }
}

/// Split a SQL script into individual statements, skipping empty lines and comments.
fn split_sql(sql: &str) -> Vec<&str> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn test_db_sqlite() -> Database {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let url = format!("sqlite://{}", path.display());
        let db = Database::new(&url).await.unwrap();
        std::mem::forget(dir);
        db
    }

    async fn test_db_postgres() -> Database {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for postgres tests");
        let db_name = format!("test_{}", uuid::Uuid::new_v4().as_simple());
        // Connect to the base URL to create the test database
        install_default_drivers();
        let admin_pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query(&format!("CREATE DATABASE \"{}\"", db_name))
            .execute(&admin_pool)
            .await
            .unwrap();
        admin_pool.close().await;

        let test_url = if url.ends_with('/') {
            format!("{}{}", url, db_name)
        } else {
            format!("{}/{}", url.rsplit_once('/').map(|(base, _)| base).unwrap_or(&url), db_name)
        };
        let db = Database::new(&test_url).await.unwrap();

        // Store cleanup info — in a real setup we'd drop this DB on teardown.
        // For tests, leaked databases are acceptable (CI cleans up).
        db
    }

    macro_rules! db_test {
        ($name:ident, |$db:ident| $body:block) => {
            paste::paste! {
                #[tokio::test]
                async fn [<$name _sqlite>]() {
                    let $db = test_db_sqlite().await;
                    $body
                }

                #[tokio::test]
                #[cfg_attr(not(feature = "postgres-tests"), ignore)]
                async fn [<$name _postgres>]() {
                    let $db = test_db_postgres().await;
                    $body
                }
            }
        };
    }

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
