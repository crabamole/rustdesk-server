use hbb_common::{log, toml::de, ResultType};
use sqlx::{
    sqlite::SqliteConnectOptions, ConnectOptions, Connection, Error as SqlxError, SqliteConnection,
};
use std::{ops::DerefMut, str::FromStr};
//use sqlx::postgres::PgPoolOptions;
//use sqlx::mysql::MySqlPoolOptions;

type Pool = deadpool::managed::Pool<DbPool>;

pub struct DbPool {
    url: String,
}

impl deadpool::managed::Manager for DbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<SqliteConnection, SqlxError> {
        let opt = SqliteConnectOptions::from_str(&self.url).unwrap();
        let opt = opt.log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut SqliteConnection,
        _:&deadpool::managed::Metrics
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    pool: Pool,
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

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        if !std::path::Path::new(url).exists() {
            std::fs::File::create(url).ok();
        }
        let deadpool_default_size = num_cpus::get()*4; // cf: https://docs.rs/deadpool/0.12.1/deadpool/managed/struct.PoolConfig.html#structfield.max_size
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or_else(|_| deadpool_default_size.to_string().to_owned())
            .parse()
            .unwrap_or(1);
        log::info!("MAX_DATABASE_CONNECTIONS={}", n);

        let pool = Pool::builder(DbPool {
            url: url.to_owned(),
        }).max_size(n).build().unwrap();
        let _ = pool.get().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        sqlx::query!(
            "
            create table if not exists peer (
                guid blob primary key not null,
                id varchar(100) not null,
                uuid blob not null,
                pk blob not null,
                created_at datetime not null default(current_timestamp),
                user blob,
                status tinyint,
                note varchar(300),
                info text not null
            ) without rowid;
            create unique index if not exists index_peer_id on peer (id);
            create index if not exists index_peer_user on peer (user);
            create index if not exists index_peer_created_at on peer (created_at);
            create index if not exists index_peer_status on peer (status);
        "
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        Ok(sqlx::query_as!(
            Peer,
            r#"select guid, id, uuid, pk, user, status, info as "info!: String" from peer where id = ?"#,
            id
        )
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?)
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        sqlx::query!(
            "insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)",
            guid,
            id,
            uuid,
            pk,
            info
        )
        .execute(self.pool.get().await?.deref_mut())
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
        sqlx::query!(
            "update peer set id=?, pk=?, info=? where guid=?",
            id,
            pk,
            info,
            guid
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn temp_db() -> Database {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite3");
        let db = Database::new(path.to_str().unwrap()).await.unwrap();
        // Leak the tempdir so it lives until process exit
        std::mem::forget(dir);
        db
    }

    #[tokio::test]
    async fn test_insert_and_get_peer() {
        let db = temp_db().await;
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
    }

    #[tokio::test]
    async fn test_get_peer_not_found() {
        let db = temp_db().await;
        let result = db.get_peer("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_update_pk() {
        let db = temp_db().await;
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
    }

    #[tokio::test]
    async fn test_insert_duplicate_id_fails() {
        let db = temp_db().await;
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";

        db.insert_peer("dup_id", uuid, pk, "{}").await.unwrap();
        let result = db.insert_peer("dup_id", uuid, pk, "{}").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_pk_changes_id() {
        let db = temp_db().await;
        let uuid = b"test-uuid-bytes!";
        let pk = b"test-pk-bytes!!!";

        let guid = db.insert_peer("old_id", uuid, pk, "{}").await.unwrap();
        db.update_pk(&guid, "new_id", pk, "{}").await.unwrap();

        let old = db.get_peer("old_id").await.unwrap();
        assert!(old.is_none());

        let new = db.get_peer("new_id").await.unwrap();
        assert!(new.is_some());
    }

    #[tokio::test]
    async fn test_concurrent_insert_and_read() {
        let db = temp_db().await;
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
    }
}
