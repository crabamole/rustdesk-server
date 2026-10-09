//! Which hbbs pod holds which device, in the api-server's UNLOGGED tables. A row counts only
//! while its pod refreshes `alive_at` and its epoch is the pod's current one.
use hbb_common::{log, ResultType};
use sqlx::PgPool;
use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
};

pub(crate) const HEARTBEAT_SECS: u64 = 10;

const UPSERT_POD: &str = "INSERT INTO hbbs_pod (pod, addr, epoch, alive_at) \
    VALUES ($1, $2, $3::uuid, now()) \
    ON CONFLICT (pod) DO UPDATE SET addr = EXCLUDED.addr, epoch = EXCLUDED.epoch, alive_at = now()";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PodRow {
    pub(crate) pod: String,
    pub(crate) addr: String,
}

pub(crate) struct Presence {
    pool: PgPool,
    pod: String,
    addr: String,
    epoch: String,
    dirty: AtomicBool,
}

impl Presence {
    pub(crate) fn new(pool: PgPool, pod: String, addr: String) -> Self {
        Self {
            pool,
            pod,
            addr,
            epoch: uuid::Uuid::new_v4().to_string(),
            dirty: AtomicBool::new(false),
        }
    }

    pub(crate) fn pod(&self) -> &str {
        &self.pod
    }

    pub(crate) fn epoch(&self) -> &str {
        &self.epoch
    }

    /// Rows left by an earlier process of this pod name are gone with it.
    pub(crate) async fn start(&self) -> ResultType<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM peer_presence WHERE pod = $1")
            .bind(&self.pod)
            .execute(&mut *tx)
            .await?;
        sqlx::query(UPSERT_POD)
            .bind(&self.pod)
            .bind(&self.addr)
            .bind(&self.epoch)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// True when every held device must be written again: the row was gone (database emptied)
    /// or an earlier presence write failed.
    pub(crate) async fn heartbeat(&self) -> bool {
        let res =
            sqlx::query("UPDATE hbbs_pod SET alive_at = now() WHERE pod = $1 AND epoch = $2::uuid")
                .bind(&self.pod)
                .bind(&self.epoch)
                .execute(&self.pool)
                .await;
        match res {
            Ok(r) if r.rows_affected() > 0 => self.dirty.swap(false, Ordering::SeqCst),
            Ok(_) => {
                log::warn!(
                    "hbbs_pod row of {} is gone, writing presence again",
                    self.pod
                );
                // A newer process of this pod name owns the row; do not overwrite it.
                let res = sqlx::query(
                    "INSERT INTO hbbs_pod (pod, addr, epoch, alive_at) \
                     SELECT $1, $2, $3::uuid, now() \
                     WHERE NOT EXISTS (SELECT 1 FROM hbbs_pod WHERE pod = $1)",
                )
                .bind(&self.pod)
                .bind(&self.addr)
                .bind(&self.epoch)
                .execute(&self.pool)
                .await;
                match res {
                    Ok(r) if r.rows_affected() > 0 => {
                        self.dirty.store(false, Ordering::SeqCst);
                        true
                    }
                    Ok(_) => false,
                    Err(e) => {
                        log::warn!("presence heartbeat failed: {e}");
                        false
                    }
                }
            }
            Err(e) => {
                log::warn!("presence heartbeat failed: {e}");
                self.dirty.store(true, Ordering::SeqCst);
                false
            }
        }
    }

    /// The newest connection of a device is the one to reach, so the latest registration wins.
    pub(crate) async fn register(&self, id: &str, gen: u64) {
        let res = sqlx::query(
            "INSERT INTO peer_presence (id, pod, epoch, gen) VALUES ($1, $2, $3::uuid, $4) \
             ON CONFLICT (id) DO UPDATE SET pod = EXCLUDED.pod, epoch = EXCLUDED.epoch, \
             gen = EXCLUDED.gen, since = now()",
        )
        .bind(id)
        .bind(&self.pod)
        .bind(&self.epoch)
        .bind(gen_i64(gen))
        .execute(&self.pool)
        .await;
        if let Err(e) = res {
            log::warn!("presence register {id} failed: {e}");
            self.dirty.store(true, Ordering::SeqCst);
        }
    }

    /// Guarded by pod, epoch and generation so a late delete cannot undo a reconnect.
    pub(crate) async fn unregister(&self, id: &str, gen: u64) {
        let res = sqlx::query(
            "DELETE FROM peer_presence WHERE id = $1 AND pod = $2 AND epoch = $3::uuid AND gen = $4",
        )
        .bind(id)
        .bind(&self.pod)
        .bind(&self.epoch)
        .bind(gen_i64(gen))
        .execute(&self.pool)
        .await;
        if let Err(e) = res {
            log::warn!("presence unregister {id} failed: {e}");
            self.dirty.store(true, Ordering::SeqCst);
        }
    }

    /// Takes over only rows of dead pods and older own connections; others moved on.
    pub(crate) async fn reassert(&self, devices: &[(String, u64)]) {
        let ids: Vec<String> = devices.iter().map(|(id, _)| id.clone()).collect();
        let gens: Vec<i64> = devices.iter().map(|(_, gen)| gen_i64(*gen)).collect();
        let res = async {
        let mut tx = self.pool.begin().await?;
        let upserted = sqlx::query(
            "INSERT INTO peer_presence (id, pod, epoch, gen) \
             SELECT t.id, $2, $3::uuid, t.gen FROM UNNEST($1::text[], $4::bigint[]) AS t(id, gen) \
             ON CONFLICT (id) DO UPDATE SET pod = EXCLUDED.pod, epoch = EXCLUDED.epoch, \
             gen = EXCLUDED.gen, since = now() \
             WHERE (peer_presence.pod = EXCLUDED.pod AND peer_presence.epoch = EXCLUDED.epoch \
                    AND peer_presence.gen <= EXCLUDED.gen) \
                OR NOT EXISTS (SELECT 1 FROM hbbs_pod h WHERE h.pod = peer_presence.pod \
                    AND h.epoch = peer_presence.epoch AND h.alive_at > now() - interval '30 seconds')",
        )
        .bind(&ids)
        .bind(&self.pod)
        .bind(&self.epoch)
        .bind(gens)
        .execute(&mut *tx)
        .await?;
        // Rows of connections that ended while a delete failed.
        sqlx::query("DELETE FROM peer_presence WHERE pod = $1 AND epoch = $2::uuid AND id <> ALL($3)")
            .bind(&self.pod)
            .bind(&self.epoch)
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(upserted)
        }
        .await;
        match res {
            Ok(r) => log::info!(
                "presence re-asserted {} of {} devices",
                r.rows_affected(),
                devices.len()
            ),
            Err(e) => {
                log::warn!("presence re-assert failed: {e}");
                self.dirty.store(true, Ordering::SeqCst);
            }
        }
    }

    pub(crate) async fn holder(&self, id: &str) -> Option<PodRow> {
        let res = sqlx::query_as::<_, (String, String)>(
            "SELECT h.pod, h.addr FROM peer_presence p \
             JOIN hbbs_pod h ON h.pod = p.pod AND h.epoch = p.epoch \
             WHERE p.id = $1 AND h.alive_at > now() - interval '30 seconds' \
             AND NOT (h.pod = $2 AND h.epoch = $3::uuid)",
        )
        .bind(id)
        .bind(&self.pod)
        .bind(&self.epoch)
        .fetch_optional(&self.pool)
        .await;
        match res {
            Ok(row) => row.map(|(pod, addr)| PodRow { pod, addr }),
            Err(e) => {
                log::warn!("presence lookup {id} failed: {e}");
                None
            }
        }
    }

    pub(crate) async fn online(&self, ids: &[String]) -> HashSet<String> {
        let res = sqlx::query_scalar::<_, String>(
            "SELECT p.id FROM peer_presence p \
             JOIN hbbs_pod h ON h.pod = p.pod AND h.epoch = p.epoch \
             WHERE p.id = ANY($1) AND h.alive_at > now() - interval '30 seconds'",
        )
        .bind(ids.to_vec())
        .fetch_all(&self.pool)
        .await;
        match res {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                log::warn!("presence online lookup failed: {e}");
                HashSet::new()
            }
        }
    }

    pub(crate) async fn pod_by_ordinal(&self, ordinal: u16) -> Option<PodRow> {
        let res = sqlx::query_as::<_, (String, String)>(
            "SELECT pod, addr FROM hbbs_pod WHERE alive_at > now() - interval '30 seconds'",
        )
        .fetch_all(&self.pool)
        .await;
        match res {
            Ok(rows) => rows
                .into_iter()
                .find(|(pod, _)| ordinal_of_pod(pod) == Some(ordinal))
                .map(|(pod, addr)| PodRow { pod, addr }),
            Err(e) => {
                log::warn!("hbbs_pod lookup failed: {e}");
                None
            }
        }
    }

    pub(crate) async fn stop(&self) {
        for sql in [
            "DELETE FROM peer_presence WHERE pod = $1 AND epoch = $2::uuid",
            "DELETE FROM hbbs_pod WHERE pod = $1 AND epoch = $2::uuid",
        ] {
            let res = sqlx::query(sql)
                .bind(&self.pod)
                .bind(&self.epoch)
                .execute(&self.pool)
                .await;
            if let Err(e) = res {
                log::warn!("presence cleanup failed: {e}");
            }
        }
    }
}

/// StatefulSet pod names end in `-<ordinal>`.
pub(crate) fn ordinal_of_pod(pod: &str) -> Option<u16> {
    pod.rsplit_once('-')?.1.parse().ok()
}

fn gen_i64(gen: u64) -> i64 {
    i64::try_from(gen).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn pool() -> PgPool {
        PgPool::connect(&crate::testing::fresh_peer_database_url().await)
            .await
            .unwrap()
    }

    async fn pod(pool: &PgPool, name: &str) -> Presence {
        let n = ordinal_of_pod(name).unwrap_or(0);
        let p = Presence::new(pool.clone(), name.to_owned(), format!("10.0.0.{n}:21120"));
        p.start().await.unwrap();
        p
    }

    async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql).execute(pool).await.unwrap();
    }

    fn holder_pod(row: Option<PodRow>) -> Option<String> {
        row.map(|r| r.pod)
    }

    #[test]
    fn ordinal_of_pod_reads_the_last_number() {
        assert_eq!(ordinal_of_pod("rustdesk-hbbs-3"), Some(3));
        assert_eq!(ordinal_of_pod("hbbs-0"), Some(0));
        assert_eq!(ordinal_of_pod("hbbs"), None);
        assert_eq!(ordinal_of_pod("hbbs-"), None);
        assert_eq!(ordinal_of_pod("hbbs-x"), None);
    }

    #[tokio::test]
    async fn start_claims_the_pod_and_drops_earlier_rows() {
        let pool = pool().await;
        let old = pod(&pool, "hbbs-0").await;
        old.register("dev_old", 1).await;
        let new = pod(&pool, "hbbs-0").await;
        let other = pod(&pool, "hbbs-1").await;
        assert_eq!(other.holder("dev_old").await, None);
        let epoch: String =
            sqlx::query_scalar("SELECT epoch::text FROM hbbs_pod WHERE pod = 'hbbs-0'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(epoch, new.epoch());
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM peer_presence WHERE pod = 'hbbs-0'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn latest_registration_wins() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        p0.register("dev", 1).await;
        assert_eq!(
            holder_pod(p1.holder("dev").await).as_deref(),
            Some("hbbs-0")
        );
        p1.register("dev", 7).await;
        assert_eq!(
            p0.holder("dev").await,
            Some(PodRow {
                pod: "hbbs-1".to_owned(),
                addr: "10.0.0.1:21120".to_owned()
            })
        );
        assert_eq!(
            p1.holder("dev").await,
            None,
            "a pod is never its own holder"
        );
    }

    #[tokio::test]
    async fn unregister_removes_only_the_same_connection() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        p0.register("dev", 1).await;
        p0.register("dev", 2).await;
        p0.unregister("dev", 1).await;
        assert!(
            p1.holder("dev").await.is_some(),
            "an older connection must not remove a newer one"
        );
        p0.unregister("dev", 2).await;
        assert!(p1.holder("dev").await.is_none());
        p1.register("dev", 2).await;
        p0.unregister("dev", 2).await;
        assert!(
            p0.holder("dev").await.is_some(),
            "a late delete must not undo a reconnect elsewhere"
        );
    }

    #[tokio::test]
    async fn rows_of_stale_or_restarted_pods_do_not_count() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        p1.register("dev_a", 1).await;
        exec(
            &pool,
            "UPDATE hbbs_pod SET alive_at = now() - interval '31 seconds' WHERE pod = 'hbbs-1'",
        )
        .await;
        assert_eq!(p0.holder("dev_a").await, None);
        assert!(p0.online(&["dev_a".to_owned()]).await.is_empty());
        assert_eq!(p0.pod_by_ordinal(1).await, None);
        exec(
            &pool,
            "UPDATE hbbs_pod SET alive_at = now(), epoch = gen_random_uuid() WHERE pod = 'hbbs-1'",
        )
        .await;
        assert_eq!(
            p0.holder("dev_a").await,
            None,
            "rows of an earlier epoch do not count"
        );
    }

    #[tokio::test]
    async fn online_lists_devices_on_live_pods() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        p0.register("a", 1).await;
        p1.register("b", 1).await;
        let got = p0
            .online(&["a".to_owned(), "b".to_owned(), "c".to_owned()])
            .await;
        assert_eq!(got, HashSet::from(["a".to_owned(), "b".to_owned()]));
    }

    #[tokio::test]
    async fn pod_by_ordinal_finds_live_pods() {
        let pool = pool().await;
        let (p0, _p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        assert_eq!(
            p0.pod_by_ordinal(1).await,
            Some(PodRow {
                pod: "hbbs-1".to_owned(),
                addr: "10.0.0.1:21120".to_owned()
            })
        );
        assert_eq!(p0.pod_by_ordinal(5).await, None);
    }

    #[tokio::test]
    async fn heartbeat_rewrites_after_the_tables_are_emptied() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        assert!(
            !p0.heartbeat().await,
            "nothing to re-assert on a healthy row"
        );
        p0.register("dev", 4).await;
        exec(&pool, "TRUNCATE hbbs_pod, peer_presence").await;
        assert!(p0.heartbeat().await);
        p0.reassert(&[("dev".to_owned(), 4)]).await;
        assert_eq!(
            holder_pod(p1.holder("dev").await).as_deref(),
            Some("hbbs-0")
        );
    }

    #[tokio::test]
    async fn a_failed_write_asks_for_a_reassert() {
        let pool = pool().await;
        let p0 = pod(&pool, "hbbs-0").await;
        exec(
            &pool,
            "ALTER TABLE peer_presence RENAME TO peer_presence_off",
        )
        .await;
        p0.register("dev", 1).await;
        exec(
            &pool,
            "ALTER TABLE peer_presence_off RENAME TO peer_presence",
        )
        .await;
        assert!(p0.heartbeat().await);
        assert!(!p0.heartbeat().await);
    }

    #[tokio::test]
    async fn a_failed_unregister_asks_for_a_reassert() {
        let pool = pool().await;
        let p0 = pod(&pool, "hbbs-0").await;
        p0.register("dev", 1).await;
        exec(
            &pool,
            "ALTER TABLE peer_presence RENAME TO peer_presence_off",
        )
        .await;
        p0.unregister("dev", 1).await;
        exec(
            &pool,
            "ALTER TABLE peer_presence_off RENAME TO peer_presence",
        )
        .await;
        assert!(p0.heartbeat().await);
    }

    #[tokio::test]
    async fn reassert_removes_own_rows_missing_from_the_snapshot() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        p0.register("kept", 1).await;
        p0.register("gone", 1).await;
        p1.register("theirs", 1).await;
        p0.reassert(&[("kept".to_owned(), 1)]).await;
        assert_eq!(p1.holder("gone").await, None);
        assert!(p1.holder("kept").await.is_some());
        assert!(
            p0.holder("theirs").await.is_some(),
            "rows of other pods stay"
        );
    }

    #[tokio::test]
    async fn heartbeat_does_not_overwrite_a_newer_epoch_of_the_same_pod() {
        let pool = pool().await;
        let old = pod(&pool, "hbbs-0").await;
        let newer = Presence::new(
            pool.clone(),
            "hbbs-0".to_owned(),
            "10.9.9.9:21120".to_owned(),
        );
        newer.start().await.unwrap();
        assert!(!old.heartbeat().await);
        let epoch: String =
            sqlx::query_scalar("SELECT epoch::text FROM hbbs_pod WHERE pod = 'hbbs-0'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(epoch, newer.epoch());
    }

    #[tokio::test]
    async fn reassert_leaves_rows_of_other_live_pods_and_newer_connections() {
        let pool = pool().await;
        let (p0, p1, p2) = (
            pod(&pool, "hbbs-0").await,
            pod(&pool, "hbbs-1").await,
            pod(&pool, "hbbs-2").await,
        );
        p1.register("dev", 9).await;
        p0.reassert(&[("dev".to_owned(), 1)]).await;
        assert_eq!(
            holder_pod(p2.holder("dev").await).as_deref(),
            Some("hbbs-1")
        );
        exec(
            &pool,
            "UPDATE hbbs_pod SET alive_at = now() - interval '31 seconds' WHERE pod = 'hbbs-1'",
        )
        .await;
        p0.reassert(&[("dev".to_owned(), 1)]).await;
        assert_eq!(
            holder_pod(p2.holder("dev").await).as_deref(),
            Some("hbbs-0"),
            "rows of dead pods are taken over"
        );

        p0.register("dev2", 5).await;
        p0.reassert(&[("dev2".to_owned(), 3)]).await;
        p0.unregister("dev2", 5).await;
        assert_eq!(
            p2.holder("dev2").await,
            None,
            "an older snapshot must not overwrite a newer connection"
        );
    }

    #[tokio::test]
    async fn stop_removes_this_process_rows() {
        let pool = pool().await;
        let (p0, p1) = (pod(&pool, "hbbs-0").await, pod(&pool, "hbbs-1").await);
        p0.register("dev", 1).await;
        p0.stop().await;
        assert_eq!(p1.holder("dev").await, None);
        let pods: i64 = sqlx::query_scalar("SELECT count(*) FROM hbbs_pod WHERE pod = 'hbbs-0'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(pods, 0);
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM peer_presence WHERE pod = 'hbbs-0'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rows, 0);
    }
}
