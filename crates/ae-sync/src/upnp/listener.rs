use super::{Error, Ports};
use std::{
    net::SocketAddrV4,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundListener {
    pub address: SocketAddrV4,
    pub external: u16,
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
    owner: Arc<Mutex<bool>>,
    value: watch::Sender<Option<BoundListener>>,
}
impl Default for ListenerSource {
    fn default() -> Self {
        let (value, _) = watch::channel(None);
        Self {
            owner: Arc::new(Mutex::new(false)),
            value,
        }
    }
}
impl ListenerSource {
    pub(crate) fn acquire(&self) -> Result<ListenerLease, Error> {
        let mut owner = self.owner.lock().unwrap_or_else(|e| e.into_inner());
        if *owner {
            return Err(Error::Busy);
        }
        *owner = true;
        Ok(ListenerLease(self.clone()))
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
        self.0.value.send_replace(address.map(|address| {
            assert_ne!(address.port(), 0, "bound listener port");
            BoundListener { address, external }
        }));
    }
    pub fn clear(&self) {
        self.0.value.send_replace(None);
    }
}
impl Drop for ListenerLease {
    fn drop(&mut self) {
        self.clear();
        *self.0.owner.lock().unwrap_or_else(|e| e.into_inner()) = false;
    }
}
