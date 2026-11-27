// One admitted job owns schema/helper preparation, captured selection, the
// disposable executable, and final response serialization. No nested admission.
use super::{Parameters, core, metadata, process};
use crate::http::dry_run::{self as http, failure, schema};
use ae_sync::dry_run::{self as admission, Control, Ticket, Timeout};
use ae_trie::{NodeReader, Overlay};
use ae_types::Hash256;
use ae_vm_fate::maps::order::Otp26;
use serde_json::{Value, json};
use std::{collections::BTreeSet, io::Write, sync::Arc};

pub mod repository;

pub type Selected<S> = Arc<
    ae_hyperchain::chain::View<Overlay<<S as Source>::Accounts>, Overlay<<S as Source>::Cache>>,
>;
pub trait Source: http::Source + Send + 'static {
    type Accounts: NodeReader + Clone + Send + Sync + 'static;
    type Cache: NodeReader + Clone + Send + Sync + 'static;
    type SelectionError: Send + 'static;
    fn select(
        &mut self,
        hash: Hash256,
    ) -> Result<Selected<Self>, ae_chain::dry_run::Error<Self::SelectionError>>;
}

pub struct Request {
    pub body: Vec<u8>,
    pub more: bool,
    pub query: String,
    pub v3: bool,
    pub profile: http::Profile,
}

#[derive(Clone)]
pub struct Config {
    pub process: process::Config,
    pub body_bytes: usize,
    pub query_bytes: usize,
    // Explicit parent-side preparation/metadata/rendering allowance. These
    // weights are service reservations, not total RSS or foreign-allocation caps.
    pub work_bytes: usize,
}
impl Config {
    pub fn weight(&self) -> Result<usize, admission::Error> {
        if self.body_bytes > 64 << 20
            || self.query_bytes > 1 << 20
            || self.work_bytes == 0
            || self.work_bytes > 1 << 30
        {
            return Err(admission::Error::Configuration);
        }
        let process = self.process.reservation(self.process.input_bytes)?;
        self.body_bytes
            .checked_mul(24)
            .and_then(|n| n.checked_add(self.query_bytes))
            .and_then(|n| n.checked_add(self.work_bytes))
            .and_then(|n| n.checked_add(process))
            .ok_or(admission::Error::Configuration)
    }
    // Reserve before collecting a socket body. The caller must keep the
    // resulting retained receipt through the actual socket write/disconnect.
    // This is the whole-job service deadline. It is not the native core timer,
    // whose profile/configuration and later start belong to HTTP composition.
    pub fn reserve(
        &self,
        client: &admission::Client,
        timeout: Timeout,
    ) -> Result<Reservation, admission::Error> {
        let reservation = client.reserve(self.weight()?, timeout)?;
        Ok(Reservation {
            reservation,
            config: self.clone(),
        })
    }
}
pub struct Reservation {
    reservation: admission::Reservation,
    config: Config,
}

struct Owned {
    execution: metadata::Execution,
    preparation: core::prepare::Limits,
    state: ae_state::transport::Limits,
    atoms: Option<BTreeSet<Vec<u8>>>,
    overlays: ae_trie::OverlayLimits,
    node_decode: ae_codec::rlp::DecodeLimits,
    rendering: http::response::Limits,
    reporting_floor: Option<ae_types::Unsigned>,
}
struct Helper {
    gas_limit: num_bigint::BigInt,
    existing_keys: BTreeSet<String>,
    order: Otp26,
    state: ae_state::transport::Limits,
    atoms: Option<BTreeSet<Vec<u8>>>,
    limits: http::Limits,
}
pub struct Captured<S> {
    pub source: S,
    parameters: Owned,
    helper: Helper,
    options: core::Options,
    native_timeout: Option<Timeout>,
    heap: Option<HeapPolicy>,
    reporting: Option<super::ReportingFloor>,
}
type HeapPolicy =
    Box<dyn FnOnce() -> Result<Option<usize>, crate::configuration::resolution::Error> + Send>;
impl<S> Captured<S> {
    // Call inside the admitted capture closure, from one owner/configuration
    // generation. Native helper preparation and selected execution share source.
    pub fn new(
        source: S,
        parameters: Parameters<'_>,
        helper: http::Config<'_, Otp26>,
        options: core::Options,
    ) -> Self {
        Self {
            source,
            parameters: Owned {
                execution: metadata::Execution::capture(parameters.execution),
                preparation: parameters.preparation,
                state: parameters.state.limits,
                atoms: parameters.state.existing_name_status_atoms.cloned(),
                overlays: parameters.overlays,
                node_decode: parameters.node_decode,
                rendering: parameters.rendering,
                reporting_floor: parameters.reporting_floor.cloned(),
            },
            helper: Helper {
                gas_limit: helper.gas_limit.clone(),
                existing_keys: helper.existing_keys.clone(),
                order: helper.order.clone(),
                state: helper.state.limits,
                atoms: helper.state.existing_name_status_atoms.cloned(),
                limits: helper.limits,
            },
            options,
            native_timeout: None,
            heap: None,
            reporting: None,
        }
    }
    // Supply the captured native profile/explicit timer policy. Its clock is
    // armed only after successful schema/helper preparation, before selection.
    pub fn with_native_timeout(mut self, timeout: Timeout) -> Self {
        self.native_timeout = Some(timeout);
        self
    }
    // Resolve after helper preparation and native timer creation, before
    // state selection. The closure belongs to the captured configuration.
    // None disables only the native core cap, never the service bootstrap cap.
    pub fn with_native_heap(
        mut self,
        read: impl FnOnce() -> Result<Option<usize>, crate::configuration::resolution::Error>
        + Send
        + 'static,
    ) -> Self {
        self.heap = Some(Box::new(read));
        self
    }
    // Resolve after successful public core execution and result/event
    // serialization. The closure must
    // remain bound to this captured owner/configuration, not a replacement.
    pub fn with_reporting_floor(
        mut self,
        read: impl FnOnce()
            -> Result<Option<ae_types::Unsigned>, crate::configuration::resolution::Error>
        + Send
        + 'static,
    ) -> Self {
        self.reporting = Some(Box::new(read));
        self
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}
#[derive(Debug)]
pub enum Error<E, S> {
    Capture(E),
    Preparation(http::Error<ReadError<E>>),
    Selection(ae_chain::dry_run::Error<S>),
    Control(admission::Error),
    Configuration(crate::configuration::resolution::Error),
    Encoding(super::Error),
    Reply(super::EncodingError),
    Process(process::Error<super::ReadError>),
    Worker(Box<super::Failure>),
    Capacity,
}
pub struct Completed<S: Source> {
    // Even helper/selection/worker errors retain the successfully captured
    // reader/configuration until the response owner releases its receipt.
    pub captured: Option<Captured<S>>,
    pub selected: Option<Selected<S>>,
    pub response: Result<Response, Error<S::Error, S::SelectionError>>,
}

impl Reservation {
    pub fn submit<S: Source>(
        self,
        request: Request,
        capture: impl FnOnce(&Control) -> Result<Captured<S>, S::Error> + Send + 'static,
    ) -> Result<Ticket<Completed<S>>, admission::Error>
    where
        S::Error: Send + 'static,
    {
        if request.body.len() > self.config.body_bytes
            || request.query.len() > self.config.query_bytes
        {
            return Err(admission::Error::Configuration);
        }
        self.reservation.submit_phased(move |control| {
            let mut captured = None;
            let mut selected = None;
            let response = run(
                &self.config,
                request,
                capture,
                &control,
                &mut captured,
                &mut selected,
            );
            Completed {
                captured,
                selected,
                response,
            }
        })
    }
}

#[derive(Debug)]
pub enum ReadError<E> {
    Control(admission::Error),
    Source(E),
}
struct Checked<'a, S> {
    source: &'a mut S,
    control: &'a Control,
}
impl<S: http::Source> Checked<'_, S> {
    fn read<T>(
        &mut self,
        f: impl FnOnce(&mut S) -> Result<T, S::Error>,
    ) -> Result<T, ReadError<S::Error>> {
        self.control.check().map_err(ReadError::Control)?;
        let result = f(self.source);
        self.control.check().map_err(ReadError::Control)?;
        result.map_err(ReadError::Source)
    }
}
impl<S: http::Source> http::Source for Checked<'_, S> {
    type Error = ReadError<S::Error>;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        self.read(S::top_height)
    }
    fn protocol(&mut self, height: u64) -> Result<ae_protocol::Protocol, Self::Error> {
        self.read(|s| s.protocol(height))
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        self.read(S::top_hash)
    }
    fn transaction(
        &mut self,
        hash: Hash256,
    ) -> Result<http::Location, http::LookupError<Self::Error>> {
        let check = || {
            self.control
                .check()
                .map_err(|e| http::LookupError::Operational(ReadError::Control(e)))
        };
        check()?;
        let result = self.source.transaction(hash);
        check()?;
        result.map_err(|e| match e {
            http::LookupError::Native => http::LookupError::Native,
            http::LookupError::Operational(e) => {
                http::LookupError::Operational(ReadError::Source(e))
            }
        })
    }
}

fn encoded<E, S>(status: u16, value: &Value, maximum: usize) -> Result<Response, Error<E, S>> {
    struct Output {
        bytes: Vec<u8>,
        maximum: usize,
    }
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
                return Err(std::io::ErrorKind::OutOfMemory.into());
            }
            self.bytes
                .try_reserve(bytes.len())
                .map_err(|_| std::io::ErrorKind::OutOfMemory)?;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Output {
        bytes: vec![],
        maximum,
    };
    serde_json::to_writer(&mut out, value).map_err(|_| Error::Capacity)?;
    Ok(Response {
        status,
        body: out.bytes,
    })
}
fn schema_error<E, S>(error: schema::Error, maximum: usize) -> Result<Response, Error<E, S>> {
    let (status, body) = error.response_parts();
    if body.len() > maximum {
        return Err(Error::Capacity);
    }
    Ok(Response { status, body })
}
fn finish<E, S>(
    status: u16,
    value: Value,
    request: &Request,
    strings: bool,
    maximum: usize,
) -> Result<Response, Error<E, S>> {
    match schema::render(status, value, request.v3, strings) {
        Ok(value) => encoded(status, &value, maximum),
        Err(e) => schema_error(e, maximum),
    }
}
fn run<S: Source>(
    config: &Config,
    request: Request,
    capture: impl FnOnce(&Control) -> Result<Captured<S>, S::Error>,
    control: &Control,
    retained: &mut Option<Captured<S>>,
    selected: &mut Option<Selected<S>>,
) -> Result<Response, Error<S::Error, S::SelectionError>> {
    let maximum = config.process.output_bytes;
    control.check().map_err(Error::Control)?;
    let body = if request.more {
        schema::Body::More(&request.body)
    } else {
        schema::Body::Complete(&request.body)
    };
    let parsed = match schema::request(body, request.v3, &request.query, config.body_bytes) {
        Ok(v) => v,
        Err(e) => return schema_error(e, maximum),
    };
    control.check().map_err(Error::Control)?;
    *retained = Some(capture(control).map_err(Error::Capture)?);
    control.check().map_err(Error::Control)?;
    let captured = retained.as_mut().expect("captured source");
    let helper = &captured.helper;
    if helper.limits.work_bytes > config.work_bytes
        || captured.parameters.preparation.work_bytes > config.work_bytes
    {
        return Err(Error::Capacity);
    }
    let prepared = match http::prepare(
        &parsed.input,
        &mut Checked {
            source: &mut captured.source,
            control,
        },
        &http::Config {
            profile: request.profile,
            gas_limit: &helper.gas_limit,
            existing_keys: &helper.existing_keys,
            order: &helper.order,
            state: ae_state::transport::Config {
                limits: helper.state,
                existing_name_status_atoms: helper.atoms.as_ref(),
            },
            limits: helper.limits,
        },
    ) {
        Ok(v) => v,
        Err(http::Error::Rejected { code, reason }) => {
            return finish(
                code,
                json!({"reason":reason}),
                &request,
                parsed.strings,
                maximum,
            );
        }
        Err(e) => return Err(Error::Preparation(e)),
    };
    control.check().map_err(Error::Control)?;
    if let Some(timeout) = captured.native_timeout {
        control.start_native(timeout).map_err(Error::Control)?;
    }
    control.check().map_err(Error::Control)?;
    let native_heap = if let Some(read) = captured.heap.take() {
        if config.process.heap.source != ae_worker_heap::Source::Service {
            return Err(Error::Control(admission::Error::Configuration));
        }
        let bytes = read().map_err(Error::Configuration)?;
        if matches!(bytes, Some(0 | usize::MAX)) {
            return Err(Error::Configuration(
                crate::configuration::resolution::Error::Field("max_heap_words"),
            ));
        }
        bytes
    } else {
        None
    };
    control.check().map_err(Error::Control)?;
    let view = match captured.source.select(prepared.top) {
        Ok(v) => v,
        Err(e) => {
            if captured.native_timeout.is_some() {
                control.finish_native().map_err(Error::Control)?;
            }
            if let Some(reason) = failure::selection(&e) {
                let (status, value) =
                    failure::body(reason, maximum).map_err(|_| Error::Capacity)?;
                return finish(status, value, &request, parsed.strings, maximum);
            }
            return Err(Error::Selection(e));
        }
    };
    control.check().map_err(Error::Control)?;
    *selected = Some(view.clone());
    let p = &captured.parameters;
    let input = super::http_request(prepared, captured.options.clone());
    let (bytes, broker) = p
        .execution
        .with_config(|execution| {
            super::prepare(
                &config.process,
                view,
                input,
                Parameters {
                    execution,
                    preparation: p.preparation,
                    state: ae_state::transport::Config {
                        limits: p.state,
                        existing_name_status_atoms: p.atoms.as_ref(),
                    },
                    overlays: p.overlays,
                    node_decode: p.node_decode,
                    rendering: p.rendering,
                    reporting_floor: p.reporting_floor.as_ref(),
                },
                captured.native_timeout.map(|_| control.clone()),
                captured.reporting.take(),
                native_heap,
            )
        })
        .map_err(Error::Encoding)?;
    control.check().map_err(Error::Control)?;
    let done = process::run(config.process.clone(), bytes, broker, control.clone());
    control.check().map_err(Error::Control)?;
    // A reaped child cannot acknowledge heap exhaustion or another early
    // exit. End its native timer before formatting the terminal response.
    if done.result.is_err()
        && captured.native_timeout.is_some()
        && done
            .broker
            .completion
            .as_ref()
            .is_some_and(|c| !c.completed)
    {
        control.finish_native().map_err(Error::Control)?;
    }
    if done.result.is_ok()
        && done
            .broker
            .completion
            .as_ref()
            .is_some_and(|c| !c.completed)
    {
        return Err(Error::Process(process::Error::Protocol));
    }
    let (status, value) = match done.result {
        Ok(bytes) => match super::decode_reply(&bytes).map_err(Error::Reply)? {
            super::Reply::Done(v) => {
                if done
                    .broker
                    .completion
                    .as_ref()
                    .is_some_and(|c| c.public && !c.reporting_read)
                {
                    return Err(Error::Process(process::Error::Protocol));
                }
                (200, v.body)
            }
            super::Reply::Failed(e) => match failure::worker(&e) {
                Some(reason) => failure::body(reason, maximum).map_err(|_| Error::Capacity)?,
                None => return Err(Error::Worker(Box::new(e))),
            },
        },
        Err(e) => match failure::process(&e) {
            Some(reason) => failure::body(reason, maximum).map_err(|_| Error::Capacity)?,
            None => return Err(Error::Process(e)),
        },
    };
    let response = finish(status, value, &request, parsed.strings, maximum)?;
    control.check().map_err(Error::Control)?;
    Ok(response)
}
