use super::*;
use std::sync::Mutex;

// Only the composing owner publishes generations. Every operation captures
// one fixed client before submitting; accepted calls never follow this route.
pub struct Route<A = MemoryStore, C = MemoryStore>(Arc<Mutex<Option<Client<A, C>>>>);
impl<A, C> Clone for Route<A, C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<A, C> Route<A, C> {
    pub fn new(client: Client<A, C>) -> Self {
        Self(Arc::new(Mutex::new(Some(client))))
    }
    pub fn current(&self) -> Option<Client<A, C>> {
        self.0.lock().expect("EOE route").clone()
    }
    pub fn replace(&self, client: Option<Client<A, C>>) {
        *self.0.lock().expect("EOE route") = client;
    }
}
