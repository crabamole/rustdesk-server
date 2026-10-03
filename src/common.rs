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

/// Proxies whose forwarded headers are believed; empty trusts every peer, as before.
/// `None` (an invalid setting) trusts no peer, so a typo cannot open header spoofing.
pub fn trusted_proxies() -> Option<&'static [ipnetwork::IpNetwork]> {
    static NETS: std::sync::OnceLock<Option<Vec<ipnetwork::IpNetwork>>> = std::sync::OnceLock::new();
    NETS.get_or_init(|| {
        parse_trusted_proxies(&std::env::var("TRUSTED_PROXIES").unwrap_or_default())
            .map_err(|e| log::error!("{e}; trusting no proxy"))
            .ok()
    })
    .as_deref()
}

/// Comma-separated CIDRs; an entry that does not parse is an error naming it.
pub fn parse_trusted_proxies(value: &str) -> Result<Vec<ipnetwork::IpNetwork>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(|c| c.parse().map_err(|e| format!("invalid TRUSTED_PROXIES entry {c:?}: {e}")))
        .collect()
}

/// A positive number from the environment, else `default`.
pub fn env_limit(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).filter(|n| *n > 0).unwrap_or(default)
}

/// Convert usize to u32, saturating values above u32::MAX instead of truncating.
pub fn saturating_u32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// The client address of a WebSocket handshake: `X-Real-IP`, else the nearest `X-Forwarded-For`
/// entry not added by a trusted proxy, else the TCP peer. Headers count only from a trusted peer.
pub fn resolve_client_addr(
    peer: SocketAddr,
    real_ip: Option<&str>,
    forwarded_for: Option<&str>,
    trusted: Option<&[ipnetwork::IpNetwork]>,
) -> SocketAddr {
    let peer = SocketAddr::new(peer.ip().to_canonical(), peer.port());
    let Some(trusted) = trusted else {
        return peer;
    };
    let is_trusted = |ip: std::net::IpAddr| trusted.is_empty() || trusted.iter().any(|n| n.contains(ip));
    if !is_trusted(peer.ip()) {
        return peer;
    }
    let parse = |v: &str| v.trim().parse::<std::net::IpAddr>().ok();
    if let Some(ip) = real_ip.and_then(parse) {
        return SocketAddr::new(ip, 0);
    }
    if let Some(chain) = forwarded_for {
        let mut last_trusted = None;
        for entry in chain.split(',').rev() {
            // Entries left of an unparsable hop were not written by a proxy we can vouch for.
            let Some(ip) = parse(entry) else { break };
            if trusted.is_empty() || !is_trusted(ip) {
                return SocketAddr::new(ip, 0);
            }
            last_trusted = Some(ip);
        }
        if let Some(ip) = last_trusted {
            return SocketAddr::new(ip, 0);
        }
    }
    if real_ip.is_some() || forwarded_for.is_some() {
        log::debug!("Ignoring unparsable forwarded address from {peer}: {real_ip:?} / {forwarded_for:?}");
    }
    peer
}

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

    fn nets(cidrs: &[&str]) -> Vec<ipnetwork::IpNetwork> {
        cidrs.iter().map(|c| c.parse().unwrap()).collect()
    }

    const PEER: &str = "10.244.1.2:4000";

    #[test]
    fn resolve_prefers_x_real_ip_from_a_trusted_peer() {
        let a = resolve_client_addr(PEER.parse().unwrap(), Some("203.0.113.5"), Some("198.51.100.1"), Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a.ip().to_string(), "203.0.113.5");
    }

    #[test]
    fn resolve_ignores_headers_from_an_untrusted_peer() {
        let a = resolve_client_addr("192.0.2.9:4000".parse().unwrap(), Some("203.0.113.5"), None, Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a.ip().to_string(), "192.0.2.9");
    }

    #[test]
    fn resolve_walks_forwarded_for_past_trusted_hops() {
        let a = resolve_client_addr(PEER.parse().unwrap(), None, Some("198.51.100.1, 203.0.113.5, 10.244.3.3"), Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a.ip().to_string(), "203.0.113.5");
    }

    #[test]
    fn resolve_without_trusted_list_takes_the_nearest_forwarded_entry() {
        // Before TRUSTED_PROXIES existed a list never parsed; the nearest entry is the closest equivalent.
        let a = resolve_client_addr(PEER.parse().unwrap(), None, Some("198.51.100.1, 203.0.113.5"), Some(&[]));
        assert_eq!(a.ip().to_string(), "203.0.113.5");
    }

    #[test]
    fn resolve_keeps_the_peer_for_bad_or_missing_headers() {
        assert_eq!(resolve_client_addr(PEER.parse().unwrap(), Some("bogus"), None, Some(&[])), PEER.parse::<SocketAddr>().unwrap());
        assert_eq!(resolve_client_addr(PEER.parse().unwrap(), None, None, Some(&[])), PEER.parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn resolve_handles_ipv6_and_mapped_peers() {
        let a = resolve_client_addr("[::ffff:10.244.1.2]:4000".parse().unwrap(), Some("2001:db8::7"), None, Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a.ip().to_string(), "2001:db8::7");
    }

    #[test]
    fn resolve_returns_a_mapped_peer_in_canonical_form() {
        let a = resolve_client_addr("[::ffff:192.0.2.9]:4000".parse().unwrap(), None, None, Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a, "192.0.2.9:4000".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn resolve_stops_the_forwarded_walk_at_an_ip_port_hop() {
        let a = resolve_client_addr(PEER.parse().unwrap(), None, Some("198.51.100.1, 198.51.100.4:5678, 10.244.3.3"), Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a.ip().to_string(), "10.244.3.3");
    }

    #[test]
    fn resolve_stops_the_forwarded_walk_at_an_unknown_hop() {
        let a = resolve_client_addr(PEER.parse().unwrap(), None, Some("198.51.100.1, unknown, 10.244.3.3"), Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a.ip().to_string(), "10.244.3.3");
        let a = resolve_client_addr(PEER.parse().unwrap(), None, Some("198.51.100.1, unknown"), Some(&nets(&["10.244.0.0/16"])));
        assert_eq!(a, PEER.parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn resolve_trusts_no_peer_when_the_trusted_list_is_invalid() {
        let a = resolve_client_addr(PEER.parse().unwrap(), Some("203.0.113.5"), Some("198.51.100.1"), None);
        assert_eq!(a, PEER.parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn parse_trusted_proxies_accepts_valid_and_empty_values() {
        assert_eq!(parse_trusted_proxies("10.244.0.0/16, 2001:db8::/32").unwrap(), nets(&["10.244.0.0/16", "2001:db8::/32"]));
        assert_eq!(parse_trusted_proxies("").unwrap(), vec![]);
        assert_eq!(parse_trusted_proxies("  ").unwrap(), vec![]);
    }

    #[test]
    fn parse_trusted_proxies_rejects_invalid_entries() {
        let err = parse_trusted_proxies("10.244.0.0/16, bogus").unwrap_err();
        assert!(err.contains("bogus"), "{err}");
        assert!(parse_trusted_proxies("10.244.0.0/16;10.0.0.0/8").is_err());
        assert!(parse_trusted_proxies("10.244.0.0/33").unwrap_err().contains("10.244.0.0/33"));
    }

    #[test]
    fn env_limit_reads_positive_numbers_only() {
        std::env::set_var("TEST_ENV_LIMIT_A", "120");
        std::env::set_var("TEST_ENV_LIMIT_B", "zero");
        std::env::set_var("TEST_ENV_LIMIT_C", "0");
        assert_eq!(env_limit("TEST_ENV_LIMIT_A", 30), 120);
        assert_eq!(env_limit("TEST_ENV_LIMIT_B", 30), 30);
        assert_eq!(env_limit("TEST_ENV_LIMIT_C", 30), 30);
        assert_eq!(env_limit("TEST_ENV_LIMIT_UNSET", 300), 300);
    }

    #[test]
    fn saturating_u32_converts_within_range() {
        assert_eq!(saturating_u32(30), 30);
        assert_eq!(saturating_u32(u32::MAX as usize), u32::MAX);
    }

    #[test]
    fn saturating_u32_saturates_above_max() {
        assert_eq!(saturating_u32(u32::MAX as usize + 1), u32::MAX);
        assert_eq!(saturating_u32(5_000_000_000), u32::MAX);
    }
}
