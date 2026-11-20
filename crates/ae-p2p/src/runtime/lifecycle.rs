use super::{Error, Exit, Generation};
use ae_types::PeerPublicKey;
use tokio::sync::watch;

// Observation alone does not keep a command sender or connection alive. One
// terminal value is retained for late observers, independently of event queues.
#[derive(Debug, Clone)]
pub struct Monitor {
    peer: PeerPublicKey,
    generation: Generation,
    receiver: watch::Receiver<Option<Exit>>,
}
impl Monitor {
    pub fn peer(&self) -> PeerPublicKey {
        self.peer
    }
    pub fn generation(&self) -> &Generation {
        &self.generation
    }
    pub fn exit(&self) -> Option<Exit> {
        self.receiver.borrow().clone()
    }
    // Cancellation of this wait has no effect on the driver. Completion means
    // driver-owned resources have dropped; externally retained messages and
    // higher-level routing jobs may still exist and need their own cleanup.
    pub async fn wait(&self) -> Exit {
        let mut receiver = self.receiver.clone();
        loop {
            let exit = receiver.borrow_and_update().clone();
            if let Some(exit) = exit {
                return exit;
            }
            receiver
                .changed()
                .await
                .expect("driver guard publishes exit before closing");
        }
    }
}
pub(super) struct Guard {
    sender: watch::Sender<Option<Exit>>,
    exit: Exit,
}
impl Guard {
    pub fn new(peer: PeerPublicKey, generation: Generation) -> (Self, Monitor) {
        let (sender, receiver) = watch::channel(None);
        (
            Self {
                sender,
                exit: Exit {
                    generation: generation.clone(),
                    reason: Error::Cancelled,
                },
            },
            Monitor {
                peer,
                generation,
                receiver,
            },
        )
    }
    pub fn reason(&mut self, reason: Error) {
        self.exit.reason = reason;
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.sender.send_replace(Some(self.exit.clone()));
    }
}
