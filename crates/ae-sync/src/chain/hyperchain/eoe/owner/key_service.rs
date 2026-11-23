use crate::{bindings, preset_keys};
use ae_hyperchain::signing::Keys;
use ae_storage::Database;
use std::sync::Arc;

pub(crate) struct KeyService {
    worker: Option<preset_keys::Worker>,
    publisher: bindings::Owner<preset_keys::Client>,
    binding: bindings::PresetKeys,
    configured: Keys,
    client: preset_keys::Client,
    capacity: preset_keys::Capacity,
    #[cfg(test)]
    pub(super) restart_fault: Option<bool>,
}
impl KeyService {
    pub(crate) fn start(db: &Arc<Database>, keys: Keys) -> Result<Self, preset_keys::Error> {
        let worker = preset_keys::Worker::spawn(keys.clone(), Default::default())?;
        let (publisher, binding) = bindings::Owner::new(db);
        publisher.install(worker.client(), 1);
        let client = worker.client();
        let capacity = client.capacity();
        Ok(Self {
            worker: Some(worker),
            publisher,
            binding,
            configured: keys,
            client,
            capacity,
            #[cfg(test)]
            restart_fault: None,
        })
    }
    pub(crate) fn template(&self) -> Keys {
        self.configured.fresh()
    }
    pub(crate) fn client(&self) -> preset_keys::Client {
        self.client.clone()
    }
    pub(crate) fn binding(&self) -> bindings::PresetKeys {
        self.binding.clone()
    }
    pub(crate) fn replacement_pending(&self) -> bool {
        self.worker.is_none() || !self.client.info().accepting || self.finished()
    }
    pub(crate) fn finished(&self) -> bool {
        self.worker
            .as_ref()
            .is_some_and(preset_keys::Worker::is_finished)
    }
    pub(crate) fn retired(&self) -> bool {
        self.worker.is_none()
    }
    pub(crate) fn request_restart(&self) -> Result<(), preset_keys::Error> {
        if self.replacement_pending() || self.binding.is_closed() {
            return Err(preset_keys::Error::Stopped);
        }
        self.worker.as_ref().expect("key worker").request_shutdown();
        Ok(())
    }
    pub(crate) fn retire(&mut self) -> Result<preset_keys::Info, preset_keys::Error> {
        if !self.finished() {
            return Err(preset_keys::Error::Busy);
        }
        self.publisher.withdraw();
        Ok(self.worker.take().expect("finished key worker").retire())
    }
    pub(crate) fn restart(&mut self, generation: u64) -> Result<(), preset_keys::Error> {
        if !self.retired() || self.binding.is_closed() {
            return Err(preset_keys::Error::Stopped);
        }
        #[cfg(test)]
        if let Some(panic) = self.restart_fault.take() {
            assert!(!panic, "controlled preset-key constructor panic");
            return Err(preset_keys::Error::Spawn(std::io::ErrorKind::Other));
        }
        let worker = preset_keys::Worker::spawn_with_capacity(
            self.configured.clone(),
            self.capacity.clone(),
        )?;
        self.client = worker.client();
        self.worker = Some(worker);
        self.publisher.install(self.client.clone(), generation);
        Ok(())
    }
    pub(crate) fn close(&self) {
        self.publisher.close();
        if let Some(worker) = &self.worker {
            worker.request_shutdown();
        }
    }
    pub(crate) fn shutdown(mut self) -> preset_keys::Info {
        self.publisher.close();
        self.worker
            .take()
            .map_or_else(|| self.client.info(), preset_keys::Worker::retire)
    }
}

pub(crate) fn departure(error: &super::Error) -> bool {
    matches!(
        error,
        super::Error::Preset(preset_keys::Error::Stopped | preset_keys::Error::Disconnected)
            | super::Error::Producer(super::super::super::delivery::Error::Preset(
                preset_keys::Error::Stopped | preset_keys::Error::Disconnected
            ))
    )
}
impl Drop for KeyService {
    fn drop(&mut self) {
        self.publisher.close();
        drop(self.worker.take());
    }
}
