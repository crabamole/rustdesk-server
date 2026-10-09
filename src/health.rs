//! `/livez` and `/readyz` for Kubernetes probes.
use axum::{http::StatusCode, routing::get, Extension, Router};
use hbb_common::ResultType;
use std::{
    net::{Ipv4Addr, Ipv6Addr},
    sync::Arc,
};

pub(crate) const PROBE_TIMEOUT_MS: u64 = 2_000;

#[async_trait::async_trait]
pub(crate) trait Probe: Send + Sync + 'static {
    /// The main event loop answered.
    async fn live(&self) -> bool;
    /// Dependencies answer and the process is not shutting down.
    async fn ready(&self) -> bool;
}

pub(crate) fn bind(port: u16) -> ResultType<std::net::TcpListener> {
    Ok(std::net::TcpListener::bind((Ipv6Addr::UNSPECIFIED, port))
        .or_else(|_| std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)))?)
}

pub(crate) async fn serve(listener: std::net::TcpListener, probe: Arc<dyn Probe>) -> ResultType<()> {
    let app = Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .layer(Extension(probe));
    axum::Server::from_tcp(listener)?.serve(app.into_make_service()).await?;
    Ok(())
}

async fn livez(Extension(probe): Extension<Arc<dyn Probe>>) -> StatusCode {
    status(bounded(probe.live()).await)
}

async fn readyz(Extension(probe): Extension<Arc<dyn Probe>>) -> StatusCode {
    status(bounded(probe.live()).await && bounded(probe.ready()).await)
}

/// A probe that does not answer in time counts as failed.
async fn bounded(probe: impl std::future::Future<Output = bool>) -> bool {
    hbb_common::timeout(PROBE_TIMEOUT_MS, probe).await.unwrap_or(false)
}

fn status(ok: bool) -> StatusCode {
    if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
struct Stub(bool, bool);

#[cfg(test)]
#[async_trait::async_trait]
impl Probe for Stub {
    async fn live(&self) -> bool {
        self.0
    }
    async fn ready(&self) -> bool {
        self.1
    }
}

#[cfg(test)]
pub(crate) async fn spawn_stub(live: bool, ready: bool) -> String {
    let listener = bind(0).unwrap();
    let port = listener.local_addr().unwrap().port();
    hbb_common::tokio::spawn(serve(listener, Arc::new(Stub(live, ready))));
    format!("http://127.0.0.1:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    async fn status(url: String) -> u16 {
        reqwest::get(url).await.unwrap().status().as_u16()
    }

    #[tokio::test]
    async fn probes_answer_by_state() {
        for (live, ready, livez, readyz) in [(true, true, 200, 200), (true, false, 200, 503), (false, true, 503, 503)] {
            let base = spawn_stub(live, ready).await;
            assert_eq!(status(format!("{base}/livez")).await, livez, "live={live} ready={ready}");
            assert_eq!(status(format!("{base}/readyz")).await, readyz, "live={live} ready={ready}");
        }
    }

    #[tokio::test]
    async fn other_paths_are_404() {
        let base = spawn_stub(true, true).await;
        assert_eq!(status(format!("{base}/")).await, 404);
    }

    struct Hang {
        hang_live: bool,
    }

    #[async_trait::async_trait]
    impl Probe for Hang {
        async fn live(&self) -> bool {
            if self.hang_live {
                std::future::pending::<()>().await;
            }
            true
        }
        async fn ready(&self) -> bool {
            std::future::pending::<()>().await;
            true
        }
    }

    #[tokio::test]
    async fn hung_probe_is_503_within_bound() {
        for (hang_live, path) in [(true, "livez"), (true, "readyz"), (false, "readyz")] {
            let listener = bind(0).unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(serve(listener, Arc::new(Hang { hang_live })));
            let start = std::time::Instant::now();
            let got = tokio::time::timeout(std::time::Duration::from_secs(4), status(format!("http://127.0.0.1:{port}/{path}"))).await;
            assert_eq!(got.ok(), Some(503), "{path} hang_live={hang_live}");
            assert!(start.elapsed() < std::time::Duration::from_millis(2_500), "{path} took {:?}", start.elapsed());
        }
    }
}
