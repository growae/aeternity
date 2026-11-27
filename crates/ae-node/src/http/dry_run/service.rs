use super::super::{Error, response::Response};
use crate::{
    dry_run_worker::{self as worker, pipeline},
    runtime,
};
use ae_chain::repository::{self as repo, execution};
use ae_execution::transactions::owned::Owned;
use ae_storage::{Database, snapshots};
use ae_sync::dry_run::{self as admission, Timeout};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct Config {
    pub pipeline: pipeline::Config,
    pub admission: admission::Config,
    pub maximum_history_headers: usize,
    pub maximum_signaling_headers: usize,
    pub existing_keys: BTreeSet<String>,
}
impl Config {
    pub fn new(executable: PathBuf, directory: PathBuf) -> Self {
        Self {
            pipeline: pipeline::Config {
                process: admission::process::Config::new(executable, directory),
                body_bytes: 5_000_000,
                query_bytes: 16 << 10,
                work_bytes: 64 << 20,
            },
            admission: admission::Config {
                bytes: 512 << 20,
                ..Default::default()
            },
            maximum_history_headers: 1024,
            maximum_signaling_headers: 100_000,
            existing_keys: [
                "txs",
                "top",
                "accounts",
                "tx_events",
                "tx",
                "tx_hash",
                "call_req",
                "calldata",
                "contract",
                "amount",
                "gas",
                "caller",
                "nonce",
                "abi_version",
                "context",
                "stateful",
                "pub_key",
                "auth_tx",
                "auth_tx_hash",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
    pub fn sibling(directory: PathBuf) -> std::io::Result<Self> {
        let file = if cfg!(windows) {
            "ae-dry-run-worker.exe"
        } else {
            "ae-dry-run-worker"
        };
        Ok(Self::new(
            std::env::current_exe()?.with_file_name(file),
            directory,
        ))
    }
    fn validate(&self) -> Result<(), Error> {
        let weight = self.pipeline.weight().map_err(|_| Error::Configuration)?;
        if weight > self.admission.bytes
            || self.maximum_history_headers == 0
            || self.maximum_history_headers > 1_000_000
            || self.maximum_signaling_headers == 0
            || self.maximum_signaling_headers > 1_000_000
            || self.existing_keys.len() > 65536
            || self.existing_keys.iter().any(|k| k.len() > 1024)
            || !self.pipeline.process.arguments.is_empty()
            || self.pipeline.process.heap.source != ae_worker_heap::Source::Service
        {
            return Err(Error::Configuration);
        }
        let metadata = std::fs::metadata(&self.pipeline.process.executable)
            .map_err(|e| Error::Io(e.to_string()))?;
        if !metadata.is_file() {
            return Err(Error::Configuration);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }
}

// Only node startup can construct this binding. State, historical execution,
// network identity and settings all originate from the same configured Open.
pub(crate) struct Bound {
    database: std::sync::Weak<Database>,
    identity: repo::Identity,
    execution: Owned,
    settings: Arc<pipeline::configuration::Settings>,
    gas_limit: ae_types::Unsigned,
}
impl Bound {
    pub(in crate::http) fn authorize_internal(&self) -> Result<(), Response> {
        authorize_internal(&self.settings)
    }
    pub fn capture(prepared: &runtime::Prepared) -> Self {
        let open = prepared.configured();
        Self {
            database: Arc::downgrade(open.chain().database()),
            identity: open.chain().identity().clone(),
            execution: open.execution().execution().clone(),
            settings: open.dry_run().clone(),
            gas_limit: open.execution().effective_gas_limit().clone(),
        }
    }
}
pub(in crate::http) fn authorize_internal(
    settings: &pipeline::configuration::Settings,
) -> Result<(), Response> {
    match settings.internal_debug_endpoints() {
        Ok(true) => Ok(()),
        Ok(false) => Err(Response::empty(403)),
        // Native forbidden callback errors become empty stream-level 500s.
        Err(_) => Err(Response::empty(500)),
    }
}
pub(crate) type Source = pipeline::repository::Repository<snapshots::Reader>;
pub(crate) type Receipt = admission::Retained<pipeline::Completed<Source>>;
#[derive(Clone)]
pub(in crate::http) struct Service {
    bound: Arc<Bound>,
    snapshots: snapshots::Owner,
    config: Arc<Config>,
    client: admission::Client,
}
pub(in crate::http) struct Owner {
    worker: admission::Worker,
    service: Service,
}
impl Owner {
    pub fn start(bound: Arc<Bound>, config: Config) -> Result<Self, Error> {
        config.validate()?;
        let database = bound.database.upgrade().ok_or(Error::Closed)?;
        let snapshots = snapshots::Owner::current(&database).map_err(|_| Error::Configuration)?;
        let (client, worker) =
            admission::create(config.admission).map_err(|_| Error::Configuration)?;
        Ok(Self {
            worker,
            service: Service {
                bound,
                snapshots,
                config: Arc::new(config),
                client,
            },
        })
    }
    pub fn service(&self) -> Service {
        self.service.clone()
    }
    pub fn into_worker(self) -> admission::Worker {
        self.worker
    }
}
impl Service {
    pub fn reserve(&self, timeout: Duration) -> Result<pipeline::Reservation, Response> {
        self.config
            .pipeline
            .reserve(&self.client, Timeout::After(timeout))
            .map_err(admission_error)
    }
    pub fn submit(
        &self,
        reservation: pipeline::Reservation,
        request: pipeline::Request,
    ) -> Result<admission::Ticket<pipeline::Completed<Source>>, Response> {
        let service = self.clone();
        let profile = request.profile;
        reservation
            .submit(request, move |_| {
                let bound = &service.bound;
                let captured = service.snapshots.view().map_err(|e| match e {
                    snapshots::Error::Repository(e) => e,
                    _ => repo::Error::ResourceLimit,
                })?;
                bound.execution.with_config(|base| {
                    let network = Arc::new(base.network.clone());
                    let snapshot = execution::snapshot(
                        captured.value,
                        &bound.identity,
                        &network,
                        execution::Config {
                            maximum_history_headers: service.config.maximum_history_headers,
                            configured_block_gas_limit: bound.gas_limit.clone(),
                            aevm: base.aevm.cloned(),
                            ..Default::default()
                        },
                    )?;
                    let source = pipeline::repository::Repository::new(
                        snapshot,
                        network,
                        service.config.maximum_signaling_headers,
                    )?;
                    let order = base.fate.as_ref().expect("configured FATE").order;
                    let state = ae_state::transport::Config {
                        existing_name_status_atoms: base.channel_state_atoms,
                        ..Default::default()
                    };
                    Ok(pipeline::Captured::new(
                        source,
                        worker::Parameters {
                            execution: base,
                            preparation: Default::default(),
                            state,
                            overlays: Default::default(),
                            node_decode: Default::default(),
                            rendering: Default::default(),
                            reporting_floor: None,
                        },
                        super::Config {
                            profile,
                            gas_limit: &0.into(),
                            existing_keys: &service.config.existing_keys,
                            order,
                            state,
                            limits: Default::default(),
                        },
                        Default::default(),
                    )
                    .with_configuration(bound.settings.clone()))
                })
            })
            .map_err(admission_error)
    }
}
pub(in crate::http) fn admission_error(error: admission::Error) -> Response {
    if let Some(reason) = super::failure::native_admission(error) {
        return match super::failure::body(reason, 1 << 20) {
            Ok((status, value)) => Response::json(status, value),
            Err(_) => Response::internal(),
        };
    }
    match error {
        admission::Error::Busy | admission::Error::Timeout => Response::unavailable(true),
        admission::Error::Stopped | admission::Error::Cancelled => Response::unavailable(false),
        _ => Response::internal(),
    }
}
pub(in crate::http) fn response(receipt: &mut Receipt) -> Response {
    match std::mem::replace(&mut receipt.value.response, Err(pipeline::Error::Capacity)) {
        Ok(response) => Response::raw_json(response.status, response.body),
        Err(pipeline::Error::Control(error)) => admission_error(error),
        Err(pipeline::Error::Capacity) => Response::unavailable(true),
        Err(_) => Response::internal(),
    }
}
