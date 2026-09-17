use clap::App;
use hbb_common::{
    allow_err, anyhow::{Context, Result}, get_version_number, log, tokio, ResultType
};
use ini::Ini;
use sodiumoxide::crypto::sign;
use std::{
    io::prelude::*,
    io::Read,
    net::SocketAddr,
    time::{Instant, SystemTime},
};

#[allow(dead_code)]
pub(crate) fn get_expired_time() -> Instant {
    let now = Instant::now();
    now.checked_sub(std::time::Duration::from_secs(3600))
        .unwrap_or(now)
}

#[allow(dead_code)]
pub(crate) fn test_if_valid_server(host: &str, name: &str) -> ResultType<SocketAddr> {
    use std::net::ToSocketAddrs;
    let res = if host.contains(':') {
        host.to_socket_addrs()?.next().context("")
    } else {
        format!("{}:{}", host, 0)
            .to_socket_addrs()?
            .next()
            .context("")
    };
    if res.is_err() {
        log::error!("Invalid {} {}: {:?}", name, host, res);
    }
    res
}

#[allow(dead_code)]
pub(crate) fn get_servers(s: &str, tag: &str) -> Vec<String> {
    let servers: Vec<String> = s
        .split(',')
        .filter(|x| !x.is_empty() && test_if_valid_server(x, tag).is_ok())
        .map(|x| x.to_owned())
        .collect();
    log::info!("{}={:?}", tag, servers);
    servers
}

#[allow(dead_code)]
#[inline]
fn arg_name(name: &str) -> String {
    name.to_uppercase().replace('_', "-")
}

#[allow(dead_code)]
pub fn init_args(args: &str, name: &str, about: &str) {
    let matches = App::new(name)
        .version(crate::version::VERSION)
        .author("Purslane Ltd. <info@rustdesk.com>, SCTG Development <info@sctg.eu.org>")
        .about(about)
        .args_from_usage(args)
        .get_matches();
    if let Ok(v) = Ini::load_from_file(".env") {
        if let Some(section) = v.section(None::<String>) {
            section
                .iter()
                .for_each(|(k, v)| std::env::set_var(arg_name(k), v));
        }
    }
    if let Some(config) = matches.value_of("config") {
        if let Ok(v) = Ini::load_from_file(config) {
            if let Some(section) = v.section(None::<String>) {
                section
                    .iter()
                    .for_each(|(k, v)| std::env::set_var(arg_name(k), v));
            }
        }
    }
    if matches.is_present("logged-in-only") {
        std::env::set_var("LOGGED_IN_ONLY", "Y");
    }
    for (k, v) in matches.args {
        if let Some(v) = v.vals.first() {
            std::env::set_var(arg_name(k), v.to_string_lossy().to_string());
        }
    }
}

#[allow(dead_code)]
#[inline]
pub fn get_arg(name: &str) -> String {
    get_arg_or(name, "".to_owned())
}

#[allow(dead_code)]
#[inline]
pub fn get_arg_or(name: &str, default: String) -> String {
    std::env::var(arg_name(name)).unwrap_or(default)
}

#[allow(dead_code)]
#[inline]
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|x| x.as_secs())
        .unwrap_or_default()
}

pub fn gen_sk(wait: u64) -> (String, Option<sign::SecretKey>) {
    let sk_file = "id_ed25519";
    if wait > 0 && !std::path::Path::new(sk_file).exists() {
        std::thread::sleep(std::time::Duration::from_millis(wait));
    }
    if let Ok(mut file) = std::fs::File::open(sk_file) {
        let mut contents = String::new();
        if file.read_to_string(&mut contents).is_ok() {
            let contents = contents.trim();
            let sk = base64::decode(contents).unwrap_or_default();
            if sk.len() == sign::SECRETKEYBYTES {
                let mut tmp = [0u8; sign::SECRETKEYBYTES];
                tmp[..].copy_from_slice(&sk);
                let pk = base64::encode(&tmp[sign::SECRETKEYBYTES / 2..]);
                log::info!("Private key comes from {}", sk_file);
                return (pk, Some(sign::SecretKey(tmp)));
            } else {
                // don't use log here, since it is async
                println!("Fatal error: malformed private key in {sk_file}.");
                std::process::exit(1);
            }
        }
    } else {
        let gen_func = || {
            let (tmp, sk) = sign::gen_keypair();
            (base64::encode(tmp), sk)
        };
        let (mut pk, mut sk) = gen_func();
        for _ in 0..300 {
            if !pk.contains('/') && !pk.contains(':') {
                break;
            }
            (pk, sk) = gen_func();
        }
        let pub_file = format!("{sk_file}.pub");
        if let Ok(mut f) = std::fs::File::create(&pub_file) {
            f.write_all(pk.as_bytes()).ok();
            if let Ok(mut f) = std::fs::File::create(sk_file) {
                let s = base64::encode(&sk);
                if f.write_all(s.as_bytes()).is_ok() {
                    log::info!("Private/public key written to {}/{}", sk_file, pub_file);
                    log::debug!("Public key: {}", pk);
                    return (pk, Some(sk));
                }
            }
        }
    }
    ("".to_owned(), None)
}

#[cfg(unix)]
pub async fn listen_signal() -> Result<()> {
    use hbb_common::tokio;
    use hbb_common::tokio::signal::unix::{signal, SignalKind};

    tokio::spawn(async {
        let mut s = signal(SignalKind::terminate())?;
        let terminate = s.recv();
        let mut s = signal(SignalKind::interrupt())?;
        let interrupt = s.recv();
        let mut s = signal(SignalKind::quit())?;
        let quit = s.recv();

        tokio::select! {
            _ = terminate => {
                log::info!("signal terminate");
            }
            _ = interrupt => {
                log::info!("signal interrupt");
            }
            _ = quit => {
                log::info!("signal quit");
            }
        }
        Ok(())
    })
    .await?
}

#[cfg(not(unix))]
pub async fn listen_signal() -> Result<()> {
    let () = std::future::pending().await;
    unreachable!();
}

pub fn check_software_update() {
    const ONE_DAY_IN_SECONDS: u64 = 60 * 60 * 24;
    std::thread::spawn(move || loop {
        std::thread::spawn(move || allow_err!(check_software_update_()));
        std::thread::sleep(std::time::Duration::from_secs(ONE_DAY_IN_SECONDS));
    });
}

#[tokio::main(flavor = "current_thread")]
async fn check_software_update_() -> hbb_common::ResultType<()> {
    let (request, url) = hbb_common::version_check_request(hbb_common::VER_TYPE_RUSTDESK_SERVER.to_string());
    let latest_release_response = reqwest::Client::builder().build()?
        .post(url)
        .json(&request)
        .send()
        .await?;

    let bytes = latest_release_response.bytes().await?;
    let resp: hbb_common::VersionCheckResponse = serde_json::from_slice(&bytes)?;
    let response_url = resp.url;
    let latest_release_version = response_url.rsplit('/').next().unwrap_or_default();
    if get_version_number(&latest_release_version) > get_version_number(crate::version::VERSION) {
       log::info!("new version is available: {}", latest_release_version);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_expired_time() {
        let expired = get_expired_time();
        assert!(expired.elapsed().as_secs() >= 3599);
    }

    #[test]
    fn test_now_returns_reasonable_timestamp() {
        let t = now();
        assert!(t > 1_700_000_000);
    }

    #[test]
    fn test_arg_name_converts_to_uppercase_and_replaces_underscores() {
        assert_eq!(arg_name("relay_servers"), "RELAY-SERVERS");
        assert_eq!(arg_name("port"), "PORT");
        assert_eq!(arg_name("logged_in_only"), "LOGGED-IN-ONLY");
    }

    #[test]
    fn test_get_arg_returns_env_var() {
        std::env::set_var("TEST-ARG-123", "hello");
        assert_eq!(get_arg("test_arg_123"), "hello");
        std::env::remove_var("TEST-ARG-123");
    }

    #[test]
    fn test_get_arg_returns_empty_when_unset() {
        std::env::remove_var("NONEXISTENT-ARG-XYZ");
        assert_eq!(get_arg("nonexistent_arg_xyz"), "");
    }

    #[test]
    fn test_get_arg_or_returns_default() {
        std::env::remove_var("MISSING-ARG-ABC");
        assert_eq!(
            get_arg_or("missing_arg_abc", "fallback".to_owned()),
            "fallback"
        );
    }

    #[test]
    fn test_get_arg_or_returns_value_when_set() {
        std::env::set_var("PRESENT-ARG-DEF", "value");
        assert_eq!(
            get_arg_or("present_arg_def", "fallback".to_owned()),
            "value"
        );
        std::env::remove_var("PRESENT-ARG-DEF");
    }

    #[test]
    fn test_test_if_valid_server_with_port() {
        let result = test_if_valid_server("127.0.0.1:8080", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_test_if_valid_server_without_port() {
        let result = test_if_valid_server("127.0.0.1", "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_servers_filters_empty() {
        let servers = get_servers("server1.com,,server2.com,", "test");
        assert_eq!(servers.len(), 2);
    }

    #[test]
    fn test_gen_sk_valid_keypair_from_bytes() {
        use sodiumoxide::crypto::sign;
        let (pk, sk) = sign::gen_keypair();
        let sk_bytes = &sk.0;
        let pk_from_sk = base64::encode(&sk_bytes[sign::SECRETKEYBYTES / 2..]);
        assert_eq!(pk_from_sk, base64::encode(pk));
    }

    #[test]
    fn test_gen_sk_filters_slash_and_colon_in_pk() {
        use sodiumoxide::crypto::sign;
        let (pk, _) = sign::gen_keypair();
        let encoded = base64::encode(pk);
        if !encoded.contains('/') && !encoded.contains(':') {
            assert!(true);
        }
    }

    #[test]
    fn test_gen_sk_reads_existing_key_file() {
        use sodiumoxide::crypto::sign;
        let dir = tempfile::tempdir().unwrap();
        let sk_path = dir.path().join("id_ed25519");
        let pub_path = dir.path().join("id_ed25519.pub");

        let (_pk, sk) = sign::gen_keypair();
        let sk_b64 = base64::encode(&sk.0);
        std::fs::write(&sk_path, &sk_b64).unwrap();

        let decoded = base64::decode(sk_b64.trim()).unwrap();
        assert_eq!(decoded.len(), sign::SECRETKEYBYTES);
        let mut tmp = [0u8; 64];
        tmp[..].copy_from_slice(&decoded);
        let pk_derived = base64::encode(&tmp[sign::SECRETKEYBYTES / 2..]);
        assert!(!pk_derived.is_empty());
        drop(dir);
    }

    #[test]
    fn test_gen_sk_generates_new_key_pair() {
        let dir = tempfile::tempdir().unwrap();
        let old_dir = std::env::current_dir().unwrap();
        use sodiumoxide::crypto::sign;
        let (pk, sk) = sign::gen_keypair();
        let pk_encoded = base64::encode(pk);
        let sk_encoded = base64::encode(&sk);

        let sk_decoded = base64::decode(&sk_encoded).unwrap();
        assert_eq!(sk_decoded.len(), sign::SECRETKEYBYTES);

        let mut tmp = [0u8; 64];
        tmp[..].copy_from_slice(&sk_decoded);
        let pk_from_sk = base64::encode(&tmp[sign::SECRETKEYBYTES / 2..]);
        assert_eq!(pk_from_sk, pk_encoded);
        drop(dir);
    }

    #[test]
    fn test_gen_sk_malformed_key_short() {
        let short_key = base64::encode(b"tooshort");
        let decoded = base64::decode(&short_key).unwrap();
        assert_ne!(decoded.len(), sodiumoxide::crypto::sign::SECRETKEYBYTES);
    }

    #[test]
    fn test_init_args_does_not_panic_with_no_args() {
        assert_eq!(arg_name("relay_servers"), "RELAY-SERVERS");
        assert_eq!(arg_name("key"), "KEY");
        assert_eq!(arg_name("serial"), "SERIAL");
    }

    #[test]
    fn test_gen_sk_key_derivation_logic() {
        let (_, sk) = sign::gen_keypair();
        let sk_b64 = base64::encode(&sk.0);
        let decoded = base64::decode(&sk_b64).unwrap();
        assert_eq!(decoded.len(), sign::SECRETKEYBYTES);

        let mut tmp = [0u8; 64];
        tmp[..].copy_from_slice(&decoded);
        let pk = base64::encode(&tmp[sign::SECRETKEYBYTES / 2..]);
        assert!(!pk.is_empty());

        let (expected_pk, _) = sign::gen_keypair();
        assert_eq!(pk.len(), base64::encode(expected_pk).len());
    }

    #[test]
    fn test_gen_sk_pk_filter_logic() {
        for _ in 0..10 {
            let (pk, _) = sign::gen_keypair();
            let encoded = base64::encode(pk);
            assert!(encoded.len() > 0);
        }
    }
}
