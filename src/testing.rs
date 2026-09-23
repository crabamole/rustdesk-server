//! Throwaway Postgres databases for tests. One container per test process.
use hbb_common::tokio::sync::OnceCell;
use sqlx::{Connection, Executor, PgConnection};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;

const PEER_FIXTURE: &str = include_str!("../tests/fixtures/peer.sql");

struct PgContainer {
    _container: ContainerAsync<Postgres>,
    base_url: String,
}

static PG: OnceCell<PgContainer> = OnceCell::const_new();

async fn container() -> &'static PgContainer {
    PG.get_or_init(|| async {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .expect("failed to start postgres test container (is Docker running?)");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        PgContainer {
            _container: container,
            base_url: format!("postgres://postgres:postgres@127.0.0.1:{port}"),
        }
    })
    .await
}

/// URL of a new, empty database on the shared test container.
pub async fn fresh_database_url() -> String {
    let pg = container().await;
    let name = format!("test_{}", uuid::Uuid::new_v4().as_simple());
    let mut admin = PgConnection::connect(&format!("{}/postgres", pg.base_url))
        .await
        .unwrap();
    admin
        .execute(format!("CREATE DATABASE \"{name}\"").as_str())
        .await
        .unwrap();
    admin.close().await.unwrap();
    format!("{}/{}", pg.base_url, name)
}

/// URL of a new database that already has the api-server's `peer` table.
pub async fn fresh_peer_database_url() -> String {
    let url = fresh_database_url().await;
    let mut conn = PgConnection::connect(&url).await.unwrap();
    conn.execute(PEER_FIXTURE).await.unwrap();
    conn.close().await.unwrap();
    url
}
