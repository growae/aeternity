use super::*;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::OwnedSemaphorePermit;

pub struct Connection {
    pub(super) client: net::Client,
    events: net::Events,
    driver: Pin<Box<dyn Future<Output = net::Exit> + Send>>,
    exit: Option<net::Exit>,
    buffered: VecDeque<net::Event>,
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
            buffered: VecDeque::new(),
        })
    }
    pub fn peer(&self) -> PeerPublicKey {
        self.client.peer()
    }
    pub fn client(&self) -> &net::Client {
        &self.client
    }
    fn poll_driver(&mut self, cx: &mut Context<'_>) {
        if self.exit.is_none()
            && let Poll::Ready(exit) = self.driver.as_mut().poll(cx)
        {
            self.exit = Some(exit);
        }
    }
    fn poll_wire_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<net::Event>> {
        let future = self.events.recv();
        let mut future = std::pin::pin!(future);
        future.as_mut().poll(cx)
    }
    // Admission keeps the same driver future and replays retained events after
    // handoff. Neither encrypted stream state nor incoming byte credits reset.
    pub(crate) async fn admission_event(&mut self) -> Result<net::Event, net::Error> {
        poll_fn(|cx| {
            self.poll_driver(cx);
            if let Some(exit) = &self.exit {
                return Poll::Ready(Err(exit.reason.clone()));
            }
            match self.poll_wire_event(cx) {
                Poll::Ready(Some(event)) => Poll::Ready(Ok(event)),
                Poll::Ready(None) => Poll::Ready(Err(net::Error::Closed)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
    pub(crate) fn retain_admission_event(&mut self, event: net::Event) {
        self.buffered.push_back(event);
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
    pub admitted: bool,
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
            .filter(|e| {
                e.admitted
                    && !e.connection.client.is_closed()
                    && !e.connection.client.is_disconnecting()
            })
            .map(|e| e.connection.client.clone())
            .collect()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn add(
        &mut self,
        id: u64,
        connection: Connection,
        permit: OwnedSemaphorePermit,
        admitted: bool,
    ) {
        for entry in &self.entries {
            if admitted && entry.admitted && entry.connection.peer() == connection.peer() {
                entry.connection.client.disconnect();
            }
        }
        self.entries.push(Entry {
            id,
            connection,
            busy: false,
            admitted,
            _permit: permit,
        });
    }
    pub fn promote(&mut self, client: &net::Client) -> Result<u64, Error> {
        let Some(index) = self.entries.iter().position(|e| {
            e.connection.client.generation() == client.generation()
                && e.connection.peer() == client.peer()
                && !e.connection.client.is_closed()
                && !e.connection.client.is_disconnecting()
        }) else {
            return Err(Error::NoPeer);
        };
        let id = self.entries[index].id;
        self.entries[index].admitted = true;
        for entry in &self.entries {
            if entry.id != id && entry.admitted && entry.connection.peer() == client.peer() {
                entry.connection.client.disconnect();
            }
        }
        Ok(id)
    }
    pub fn current(&self, peer: PeerPublicKey) -> Option<&Entry> {
        self.entries.iter().rev().find(|entry| {
            entry.admitted
                && entry.connection.peer() == peer
                && !entry.connection.client.is_closed()
                && !entry.connection.client.is_disconnecting()
        })
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
                entry.connection.poll_driver(cx);
                if !entry.busy {
                    let event = match entry.connection.buffered.pop_front() {
                        Some(event) => Poll::Ready(Some(event)),
                        None => entry.connection.poll_wire_event(cx),
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
