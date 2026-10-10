//! Relay URLs handed to clients (`RELAY_URLS`) and the health URLs polled for them (`RELAY_CHECKS`).
use hbb_common::{bail, futures::future::join_all, rand::seq::SliceRandom, ResultType};
use once_cell::sync::Lazy;
use std::time::Duration;

const CHECK_TIMEOUT: Duration = Duration::from_secs(2);
// Checks target in-cluster services; an egress proxy from the environment cannot reach them.
static CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder().no_proxy().build().unwrap_or_else(|e| {
        hbb_common::log::warn!("relay check client without no_proxy: {e}");
        reqwest::Client::new()
    })
});

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct RelayList {
    pub(crate) urls: Vec<String>,
    pub(crate) checks: Vec<String>,
}

impl RelayList {
    pub(crate) fn parse(urls: &str, checks: &str) -> ResultType<Self> {
        let split = |s: &str| -> Vec<String> {
            s.split(',').map(str::trim).filter(|x| !x.is_empty()).map(str::to_owned).collect()
        };
        let (urls, checks) = (split(urls), split(checks));
        if !checks.is_empty() && checks.len() != urls.len() {
            bail!("RELAY_CHECKS has {} entries but RELAY_URLS has {}", checks.len(), urls.len());
        }
        Ok(Self { urls, checks })
    }

    /// hbbs is the only source of relays, so it refuses to start without one.
    pub(crate) fn from_env() -> ResultType<Self> {
        let var = |name: &str| std::env::var(name).unwrap_or_default();
        let list = Self::parse(&var("RELAY_URLS"), &var("RELAY_CHECKS"))?;
        if list.urls.is_empty() {
            bail!("RELAY_URLS is empty: set the relay URLs hbbs hands out, e.g. wss://<host>/ws/relay/0");
        }
        Ok(list)
    }

    /// When every check fails, handing out all relays beats handing out none.
    pub(crate) async fn healthy(&self) -> Vec<String> {
        let checks = self.checks.iter().map(|url| async move {
            matches!(
                CLIENT.get(url).timeout(CHECK_TIMEOUT).send().await,
                Ok(r) if r.status() == reqwest::StatusCode::OK
            )
        });
        let up: Vec<String> = self
            .urls
            .iter()
            .zip(join_all(checks).await)
            .filter(|(_, ok)| *ok)
            .map(|(url, _)| url.clone())
            .collect();
        if up.is_empty() {
            self.urls.clone()
        } else {
            up
        }
    }
}

pub(crate) fn pick(urls: &[String]) -> String {
    urls.choose(&mut hbb_common::rand::thread_rng()).cloned().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::tokio;

    #[test]
    fn parse_trims_and_requires_matching_checks() {
        let l = RelayList::parse(" wss://h/ws/relay/0 ,,wss://h/ws/relay/1", "http://a/readyz, http://b/readyz").unwrap();
        assert_eq!(l.urls, vec!["wss://h/ws/relay/0", "wss://h/ws/relay/1"]);
        assert_eq!(l.checks, vec!["http://a/readyz", "http://b/readyz"]);
        assert!(RelayList::parse("wss://a,wss://b", "http://a/readyz").is_err());
        assert_eq!(RelayList::parse("wss://a", "").unwrap().checks, Vec::<String>::new());
        assert_eq!(RelayList::parse("", "").unwrap(), RelayList::default());
    }

    #[test]
    fn from_env_reads_both_lists_and_requires_urls() {
        std::env::set_var("RELAY_URLS", "wss://h/ws/relay/0");
        std::env::set_var("RELAY_CHECKS", "http://hbbr-0:21122/readyz");
        let l = RelayList::from_env().unwrap();
        std::env::remove_var("RELAY_URLS");
        std::env::remove_var("RELAY_CHECKS");
        assert_eq!(l.urls, vec!["wss://h/ws/relay/0"]);
        assert_eq!(l.checks, vec!["http://hbbr-0:21122/readyz"]);
        // Same test: both mutate RELAY_URLS.
        let err = RelayList::from_env().unwrap_err().to_string();
        assert!(err.contains("RELAY_URLS is empty"), "{err}");
    }

    #[test]
    fn pick_returns_a_member_at_random() {
        assert_eq!(pick(&[]), "");
        let urls = vec!["wss://a".to_owned(), "wss://b".to_owned()];
        let seen: std::collections::HashSet<String> = (0..200).map(|_| pick(&urls)).collect();
        assert_eq!(seen.len(), 2);
    }

    #[tokio::test]
    async fn healthy_keeps_relays_whose_check_answers_200() {
        let up = crate::health::spawn_stub(true, true).await;
        let draining = crate::health::spawn_stub(true, false).await;
        let l = RelayList::parse(
            "wss://h/ws/relay/0,wss://h/ws/relay/1,wss://h/ws/relay/2",
            &format!("{up}/readyz,{draining}/readyz,http://127.0.0.1:1/readyz"),
        )
        .unwrap();
        assert_eq!(l.healthy().await, vec!["wss://h/ws/relay/0".to_owned()]);
    }

    #[tokio::test]
    async fn healthy_hands_out_all_when_none_answers() {
        let l = RelayList::parse("wss://a,wss://b", "http://127.0.0.1:1/readyz,http://127.0.0.1:1/readyz").unwrap();
        assert_eq!(l.healthy().await, l.urls);
    }
}
