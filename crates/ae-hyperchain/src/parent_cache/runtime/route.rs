use super::*;

// Only the owner publishes generations. Callers capture a fixed client before
// starting an operation; changing this route cannot redirect accepted calls.
#[derive(Clone, Default)]
pub struct Route(Arc<Mutex<Option<Client>>>);
impl Route {
    pub fn new(client: Client) -> Self {
        Self(Arc::new(Mutex::new(Some(client))))
    }
    pub fn current(&self) -> Option<Client> {
        self.0.lock().expect("parent cache route").clone()
    }
    pub fn replace(&self, client: Option<Client>) {
        *self.0.lock().expect("parent cache route") = client;
    }
}
impl connector::Sink for Route {
    fn post(&mut self, block: &ParentBlock) -> Result<(), connector::SinkError> {
        // Native casts to an absent cache are discarded. An already accepted
        // fetch may post to the currently registered cache when it completes.
        match self.current() {
            Some(client) => PostSink(client).post(block),
            None => Ok(()),
        }
    }
}
