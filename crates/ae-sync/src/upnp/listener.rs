use super::{Error, Mapping, Ports};
use std::{
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundListener {
    pub address: SocketAddrV4,
    pub external: u16,
    incarnation: u64,
}
impl BoundListener {
    pub fn ports(self) -> Ports {
        Ports {
            internal: self.address.port(),
            external: self.external,
        }
    }
}

// A stable gate shared by the connection owner and replacement mapping
// workers. Only the connection owner holds the publication lease. The gate
// starts closed; it never falls back to the standalone configured ports.
#[derive(Debug, Clone)]
pub struct ListenerSource {
    state: Arc<Mutex<State>>,
    value: watch::Sender<Option<BoundListener>>,
}
#[derive(Debug, Default)]
struct State {
    owner: bool,
    advertiser: bool,
    incarnation: u64,
    advertisement: Option<Advertisement>,
}
#[derive(Debug)]
struct Advertisement {
    port: u16,
    expires: Option<Instant>,
}
impl Default for ListenerSource {
    fn default() -> Self {
        let (value, _) = watch::channel(None);
        Self {
            state: Arc::new(Mutex::new(State::default())),
            value,
        }
    }
}
impl ListenerSource {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(crate) fn acquire(&self) -> Result<ListenerLease, Error> {
        let mut state = self.lock();
        if state.owner {
            return Err(Error::Busy);
        }
        state.owner = true;
        Ok(ListenerLease(self.clone()))
    }
    pub(super) fn advertise(&self) -> Result<AdvertisementLease, Error> {
        let mut state = self.lock();
        if state.advertiser {
            return Err(Error::Busy);
        }
        state.advertiser = true;
        state.advertisement = None;
        Ok(AdvertisementLease(self.clone()))
    }
    // Sampling never extends the router lease. Missing, withdrawn and expired
    // assignments all use the caller's configured port, including zero.
    pub fn advertised_port(&self, configured: u16) -> u16 {
        self.advertised_at(configured, Instant::now())
    }
    fn advertised_at(&self, configured: u16, now: Instant) -> u16 {
        let state = self.lock();
        state
            .advertisement
            .as_ref()
            .filter(|a| a.expires.is_none_or(|expires| now < expires))
            .map_or(configured, |a| a.port)
    }
    pub(crate) fn current(&self) -> Option<BoundListener> {
        *self.value.borrow()
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<BoundListener>> {
        self.value.subscribe()
    }
}
pub(crate) struct ListenerLease(ListenerSource);
impl ListenerLease {
    pub fn publish(&self, address: Option<SocketAddrV4>, external: u16) {
        let mut state = self.0.lock();
        state.incarnation = state
            .incarnation
            .checked_add(1)
            .expect("listener incarnation");
        state.advertisement = None;
        self.0.value.send_replace(address.map(|address| {
            assert_ne!(address.port(), 0, "bound listener port");
            BoundListener {
                address,
                external,
                incarnation: state.incarnation,
            }
        }));
    }
    pub fn clear(&self) {
        let mut state = self.0.lock();
        state.advertisement = None;
        self.0.value.send_replace(None);
    }
}
impl Drop for ListenerLease {
    fn drop(&mut self) {
        self.clear();
        self.0.lock().owner = false;
    }
}

// Kept by the mapping driver, not by its cloneable clients. Dropping or
// unwinding the driver withdraws even permanent assignments; the ownership
// ledger remains available to a replacement for reconciliation.
pub(super) struct AdvertisementLease(ListenerSource);
impl AdvertisementLease {
    pub fn clear(&self) {
        self.0.lock().advertisement = None;
    }
    pub fn confirm(&self, bound: BoundListener, mapping: &Mapping, began: Instant) -> bool {
        let expires = match mapping.lifetime {
            None => None,
            Some(seconds) => match began.checked_add(Duration::from_secs(u64::from(seconds))) {
                Some(deadline) => Some(deadline),
                None => return false,
            },
        };
        let mut state = self.0.lock();
        if self.0.current() != Some(bound)
            || mapping.internal != bound.address.port()
            || mapping.external == 0
            || expires.is_some_and(|deadline| deadline <= Instant::now())
        {
            return false;
        }
        state.advertisement = Some(Advertisement {
            port: mapping.external,
            expires,
        });
        true
    }
}
impl Drop for AdvertisementLease {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.advertisement = None;
        state.advertiser = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upnp::Protocol;
    use std::net::Ipv4Addr;

    fn bound(lease: &ListenerLease) -> BoundListener {
        lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6000)), 7000);
        lease.0.current().unwrap()
    }
    fn mapping(lifetime: Option<u32>) -> Mapping {
        Mapping {
            protocol: Protocol::Upnp2,
            internal: 6000,
            external: 8000,
            lifetime,
            gateway_epoch: None,
        }
    }

    #[test]
    fn finite_advertisement_expires_from_attempt_start_and_preserves_configured_fallback() {
        let source = ListenerSource::default();
        let lease = source.acquire().unwrap();
        let bound = bound(&lease);
        let advertisement = source.advertise().unwrap();
        let began = Instant::now();
        assert!(advertisement.confirm(bound, &mapping(Some(10)), began));
        assert_eq!(
            source.advertised_at(7000, began + Duration::from_secs(9)),
            8000
        );
        assert_eq!(
            source.advertised_at(7000, began + Duration::from_secs(10)),
            7000
        );
        assert_eq!(source.advertised_at(0, began + Duration::from_secs(11)), 0);
        advertisement.clear();
        assert!(!advertisement.confirm(bound, &mapping(Some(1)), began - Duration::from_secs(2)));
        assert_eq!(source.advertised_port(7000), 7000);
    }

    #[test]
    fn equal_socket_publications_and_owner_replacement_reject_stale_confirmations() {
        let source = ListenerSource::default();
        let lease = source.acquire().unwrap();
        let advertisement = source.advertise().unwrap();
        let first = bound(&lease);
        assert!(advertisement.confirm(first, &mapping(None), Instant::now()));
        let second = bound(&lease);
        assert_ne!(first, second);
        assert_eq!(first.ports(), second.ports());
        assert_eq!(source.advertised_port(7000), 7000);
        assert!(!advertisement.confirm(first, &mapping(None), Instant::now()));
        assert!(advertisement.confirm(second, &mapping(None), Instant::now()));
        drop(lease);
        assert_eq!(source.advertised_port(7000), 7000);
        let replacement = source.acquire().unwrap();
        let third = bound(&replacement);
        assert_ne!(second, third);
        assert!(!advertisement.confirm(second, &mapping(None), Instant::now()));
        assert!(advertisement.confirm(third, &mapping(None), Instant::now()));
        replacement.clear();
        assert!(!advertisement.confirm(third, &mapping(None), Instant::now()));
        assert_eq!(source.advertised_port(7000), 7000);
    }

    #[test]
    fn permanent_advertisement_is_exclusive_and_withdrawn_on_unwind() {
        let source = ListenerSource::default();
        let lease = source.acquire().unwrap();
        let bound = bound(&lease);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let advertisement = source.advertise().unwrap();
            assert!(matches!(source.advertise(), Err(Error::Busy)));
            let now = Instant::now();
            assert!(advertisement.confirm(bound, &mapping(None), now));
            assert_eq!(
                source.advertised_at(7000, now + Duration::from_secs(100_000)),
                8000
            );
            panic!("mapping driver failure");
        }));
        assert!(result.is_err());
        assert_eq!(source.advertised_port(7000), 7000);
        let replacement = source.advertise().unwrap();
        assert!(replacement.confirm(bound, &mapping(None), Instant::now()));
        drop(replacement);
        assert_eq!(source.advertised_port(7000), 7000);
    }

    #[test]
    fn invalid_or_expired_assignments_are_never_advertised() {
        let source = ListenerSource::default();
        let lease = source.acquire().unwrap();
        let bound = bound(&lease);
        let advertisement = source.advertise().unwrap();
        for invalid in [
            Mapping {
                internal: 6001,
                ..mapping(None)
            },
            Mapping {
                external: 0,
                ..mapping(None)
            },
            mapping(Some(0)),
        ] {
            assert!(!advertisement.confirm(bound, &invalid, Instant::now()));
            assert_eq!(source.advertised_port(7000), 7000);
        }
    }
}
