// Runs the hbbr binary: SIGTERM drains, SIGINT exits at once.
use hbb_common::tokio::{self, time::sleep};
use std::{
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

mod common;
use common::pair;

struct Hbbr {
    child: Child,
    ws_port: u16,
    health_port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Hbbr {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A port P with P and P + 2 free: hbbr listens on both.
fn free_relay_port() -> u16 {
    for _ in 0..100 {
        let port = free_port();
        if port < 65000 && std::net::TcpListener::bind(("127.0.0.1", port + 2)).is_ok() {
            return port;
        }
    }
    panic!("no free port pair");
}

async fn start_hbbr() -> Hbbr {
    let dir = tempfile::tempdir().unwrap();
    let port = free_relay_port();
    let health_port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_hbbr"))
        .args(["-p", &port.to_string()])
        .env("HEALTH_PORT", health_port.to_string())
        .env_remove("PORT")
        .env_remove("KEY")
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let hbbr = Hbbr {
        child,
        ws_port: port + 2,
        health_port,
        _dir: dir,
    };
    assert!(
        wait_for_status(hbbr.health_port, "/readyz", 200, Duration::from_secs(10)).await,
        "hbbr did not become ready"
    );
    hbbr
}

async fn status(port: u16, path: &str) -> Option<u16> {
    let client = reqwest::Client::builder().no_proxy().build().ok()?;
    let resp = client
        .get(format!("http://127.0.0.1:{port}{path}"))
        .send()
        .await
        .ok()?;
    Some(resp.status().as_u16())
}

async fn wait_for_status(port: u16, path: &str, want: u16, within: Duration) -> bool {
    let end = Instant::now() + within;
    while Instant::now() < end {
        if status(port, path).await == Some(want) {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn wait_for_exit(child: &mut Child, within: Duration) -> Option<ExitStatus> {
    let end = Instant::now() + within;
    while Instant::now() < end {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        sleep(Duration::from_millis(100)).await;
    }
    None
}

fn signal(child: &Child, name: &str) {
    let ok = Command::new("kill")
        .args([&format!("-{name}"), &child.id().to_string()])
        .status()
        .unwrap()
        .success();
    assert!(ok, "kill -{name} failed");
}

#[tokio::test]
async fn test_sigterm_fails_readyz_and_waits_for_sessions() {
    let mut hbbr = start_hbbr().await;
    let before = pair(hbbr.ws_port, "drain-before-sigterm").await;

    signal(&hbbr.child, "TERM");
    assert!(
        wait_for_status(hbbr.health_port, "/readyz", 503, Duration::from_secs(2)).await,
        "/readyz must fail once SIGTERM arrives"
    );
    assert_eq!(status(hbbr.health_port, "/livez").await, Some(200));

    // hbbs may have handed out this pod's URL just before; it still pairs.
    let after = pair(hbbr.ws_port, "drain-after-sigterm").await;

    // Past the 10 s drain minimum, open sessions keep it running.
    sleep(Duration::from_secs(11)).await;
    assert!(
        hbbr.child.try_wait().unwrap().is_none(),
        "hbbr exited with sessions open"
    );

    drop(before);
    drop(after);
    let exit = wait_for_exit(&mut hbbr.child, Duration::from_secs(5))
        .await
        .expect("hbbr exits once its sessions end");
    assert!(exit.success(), "exit status {exit:?}");
}

#[tokio::test]
async fn test_sigint_exits_at_once() {
    let mut hbbr = start_hbbr().await;
    let _open = pair(hbbr.ws_port, "sigint-open-session").await;
    signal(&hbbr.child, "INT");
    let exit = wait_for_exit(&mut hbbr.child, Duration::from_secs(3))
        .await
        .expect("SIGINT does not drain");
    assert!(exit.success(), "exit status {exit:?}");
}
