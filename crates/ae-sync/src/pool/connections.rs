use super::*;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};
use tokio::sync::OwnedSemaphorePermit;

pub struct Connection {
    pub(super) client: net::Client,
    events: net::Events,
    driver: Pin<Box<dyn Future<Output = net::Exit> + Send>>,
    exit: Option<net::Exit>,
}
impl Connection {
    pub fn new(
        client: net::Client,
        events: net::Events,
        driver: net::Driver,
    ) -> Result<Self, Error> {
        if client.generation() != driver.generation() {
            return Err(Error::InvalidConnection);
        }
        Ok(Self {
            client,
            events,
            driver: Box::pin(driver.run()),
            exit: None,
        })
    }
    pub fn peer(&self) -> PeerPublicKey {
        self.client.peer()
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.client.shutdown();
    }
}
pub(super) struct Entry {
    pub id: u64,
    pub connection: Connection,
    pub busy: bool,
    _permit: OwnedSemaphorePermit,
}
pub(super) enum Event {
    Message {
        id: u64,
        peer: PeerPublicKey,
        event: net::Event,
    },
    Down {
        id: u64,
        peer: PeerPublicKey,
    },
}
pub(super) struct Connections {
    entries: Vec<Entry>,
    cursor: usize,
}
impl Connections {
    pub fn new() -> Self {
        Self {
            entries: vec![],
            cursor: 0,
        }
    }
    pub fn clients(&self) -> Vec<net::Client> {
        self.entries
            .iter()
            .filter(|e| !e.connection.client.is_closed())
            .map(|e| e.connection.client.clone())
            .collect()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn add(&mut self, id: u64, connection: Connection, permit: OwnedSemaphorePermit) {
        for entry in &self.entries {
            if entry.connection.peer() == connection.peer() {
                entry.connection.client.shutdown();
            }
        }
        self.entries.push(Entry {
            id,
            connection,
            busy: false,
            _permit: permit,
        });
    }
    pub fn current(&self, peer: PeerPublicKey) -> Option<&Entry> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.connection.peer() == peer && !entry.connection.client.is_closed())
    }
    pub fn get(&self, id: u64) -> Option<&Entry> {
        self.entries.iter().find(|e| e.id == id)
    }
    pub fn busy(&mut self, id: u64, value: bool) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id == id) {
            entry.busy = value;
        }
    }
    pub fn close(&self, id: u64) {
        if let Some(entry) = self.get(id) {
            entry.connection.client.shutdown();
        }
    }
    pub fn close_all(&self) {
        for entry in &self.entries {
            entry.connection.client.shutdown();
        }
    }
    pub fn clear(&mut self) {
        self.entries.clear();
    }
    pub async fn next(&mut self) -> Event {
        poll_fn(|cx| {
            let length = self.entries.len();
            if length == 0 {
                return Poll::Pending;
            }
            for offset in 0..length {
                let index = (self.cursor + offset) % length;
                let entry = &mut self.entries[index];
                if entry.connection.exit.is_none()
                    && let Poll::Ready(exit) = entry.connection.driver.as_mut().poll(cx)
                {
                    entry.connection.exit = Some(exit);
                }
                if !entry.busy {
                    let event = {
                        let future = entry.connection.events.recv();
                        let mut future = std::pin::pin!(future);
                        future.as_mut().poll(cx)
                    };
                    if let Poll::Ready(Some(event)) = event {
                        self.cursor = (index + 1) % length;
                        return Poll::Ready(Event::Message {
                            id: entry.id,
                            peer: entry.connection.peer(),
                            event,
                        });
                    }
                }
                if entry.connection.exit.is_some() {
                    let entry = self.entries.swap_remove(index);
                    self.cursor = index;
                    return Poll::Ready(Event::Down {
                        id: entry.id,
                        peer: entry.connection.peer(),
                    });
                }
            }
            self.cursor = (self.cursor + 1) % length;
            Poll::Pending
        })
        .await
    }
}
