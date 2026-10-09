//! The viewer's port carries the pod ordinal (5 bits) and a per-process counter (11 bits),
//! so whichever pod gets the device's answer knows which pod holds the waiting viewer.
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU16, Ordering},
};

pub(crate) const MAX_ORDINAL: u16 = 31;
const COUNTER_BITS: u16 = 11;
const MAX_COUNTER: u16 = (1 << COUNTER_BITS) - 1;

pub(crate) struct RoutingTokens {
    ordinal: u16,
    counter: AtomicU16,
}

impl RoutingTokens {
    /// `ClusterConfig` rejects ordinals above `MAX_ORDINAL`.
    pub(crate) fn new(ordinal: u16) -> Self {
        Self {
            ordinal: ordinal & MAX_ORDINAL,
            counter: AtomicU16::new(0),
        }
    }

    pub(crate) fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// `addr` with its port replaced by this pod's next token.
    pub(crate) fn tag(&self, addr: SocketAddr) -> SocketAddr {
        let next = |c: u16| if c >= MAX_COUNTER { 1 } else { c + 1 };
        let prev = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| Some(next(c)))
            .unwrap_or_else(|c| c);
        SocketAddr::new(addr.ip(), (self.ordinal << COUNTER_BITS) | next(prev))
    }
}

/// Port 0 is never a token: such an address was not tagged by any pod.
pub(crate) fn ordinal_of(addr: SocketAddr) -> Option<u16> {
    match addr.port() {
        0 => None,
        port => Some(port >> COUNTER_BITS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbb_common::AddrMangle;

    fn viewer() -> SocketAddr {
        "203.0.113.5:0".parse().unwrap()
    }

    #[test]
    fn tokens_start_at_one_and_carry_the_ordinal() {
        let t = RoutingTokens::new(1);
        assert_eq!(t.tag(viewer()).port(), (1 << 11) | 1);
        let second = t.tag(viewer());
        assert_eq!(second.port(), (1 << 11) | 2);
        assert_eq!(second.ip(), viewer().ip());
    }

    #[test]
    fn counter_wraps_to_one_and_never_yields_port_zero() {
        let t = RoutingTokens::new(0);
        for _ in 0..MAX_COUNTER {
            assert_ne!(t.tag(viewer()).port(), 0);
        }
        assert_eq!(t.tag(viewer()).port(), 1);
    }

    #[test]
    fn highest_ordinal_and_counter_fill_the_port() {
        let t = RoutingTokens::new(MAX_ORDINAL);
        let mut last = 0;
        for _ in 0..MAX_COUNTER {
            last = t.tag(viewer()).port();
        }
        assert_eq!(last, u16::MAX);
    }

    #[test]
    fn ordinal_of_reads_the_tag() {
        assert_eq!(ordinal_of(viewer()), None);
        for n in [0, 1, 7, MAX_ORDINAL] {
            assert_eq!(ordinal_of(RoutingTokens::new(n).tag(viewer())), Some(n));
        }
    }

    #[test]
    fn tagged_addresses_survive_addr_mangle() {
        let t = RoutingTokens::new(MAX_ORDINAL);
        for base in ["203.0.113.5:0", "[2001:db8::7]:0"] {
            let addr = t.tag(base.parse().unwrap());
            assert_eq!(AddrMangle::decode(&AddrMangle::encode(addr)), addr);
        }
    }
}
