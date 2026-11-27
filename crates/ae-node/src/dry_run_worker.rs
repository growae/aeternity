// Local disposable-worker transport. These encodings never enter consensus,
// durable storage, or the public HTTP schema. The child has no database handle.
mod codec;
mod job;
mod metadata;
pub mod pipeline;
use ae_execution::{dry_run as core, transactions};
use ae_hyperchain::chain::View;
use ae_sync::dry_run::{self, Ticket, Timeout, process};
use ae_trie::{NodeReader, Overlay};
pub use codec::Error as EncodingError;
pub use job::{Failure, Kind, Reply, ResultBody, Stage};
use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
};

pub struct Request<'a> {
    pub inputs: Vec<core::prepare::Input<'a>>,
    pub funding: Vec<core::Funding>,
    pub options: core::Options,
}
pub struct Parameters<'a> {
    pub execution: &'a transactions::Config<'a>,
    pub preparation: core::prepare::Limits,
    pub state: ae_state::transport::Config<'a>,
    pub overlays: ae_trie::OverlayLimits,
    pub node_decode: ae_codec::rlp::DecodeLimits,
    pub rendering: crate::http::dry_run::response::Limits,
    pub reporting_floor: Option<&'a ae_types::Unsigned>,
}
#[derive(Debug)]
pub enum Error {
    Admission(dry_run::Error),
    Encoding(EncodingError),
    Capacity,
}
impl From<EncodingError> for Error {
    fn from(e: EncodingError) -> Self {
        Self::Encoding(e)
    }
}
// Retain this broker with process output and the Ticket::wait_retained credits
// through response rendering/sending. A captured view owns all eight stores.
pub struct Broker<A, C> {
    pub view: Arc<View<Overlay<A>, Overlay<C>>>,
    controls: Vec<ae_types::work::Control>,
    node_bytes: usize,
    completion: Option<Completion>,
}
pub type ReportingFloor = Box<
    dyn FnOnce() -> Result<Option<ae_types::Unsigned>, crate::configuration::resolution::Error>
        + Send,
>;
pub type Metering =
    Box<dyn FnOnce() -> Result<bool, crate::configuration::resolution::Error> + Send>;
#[derive(Default)]
struct Policies {
    native: Option<dry_run::Control>,
    reporting: Option<ReportingFloor>,
    metering: Option<Metering>,
    heap: Option<usize>,
}
struct Completion {
    native: Option<dry_run::Control>,
    reporting: Option<ReportingFloor>,
    public: bool,
    completed: bool,
    succeeded: bool,
    reporting_read: bool,
    metering: Option<Metering>,
    metering_expected: bool,
    metering_read: bool,
}
#[derive(Debug)]
pub enum ReadError {
    Query,
    Capacity,
    Node(ae_trie::Error),
    Phase(dry_run::Error),
    Reporting(crate::configuration::resolution::Error),
    Metering(crate::configuration::resolution::Error),
    Encoding(EncodingError),
}
impl<A: NodeReader, C: NodeReader> process::Broker for Broker<A, C> {
    type Error = ReadError;
    fn read(&mut self, q: &[u8]) -> Result<Vec<u8>, ReadError> {
        if q == [130] {
            let completion = self.completion.as_mut().ok_or(ReadError::Query)?;
            if !completion.completed
                || !completion.succeeded
                || !completion.public
                || completion.reporting_read
            {
                return Err(ReadError::Query);
            }
            completion.reporting_read = true;
            let floor = completion
                .reporting
                .take()
                .map(|read| read().map_err(ReadError::Reporting))
                .transpose()?;
            return codec::encode(&floor, self.node_bytes).map_err(ReadError::Encoding);
        }
        if self.completion.as_ref().is_some_and(|c| c.completed) {
            return Err(ReadError::Query);
        }
        if q == [131] {
            let completion = self.completion.as_mut().ok_or(ReadError::Query)?;
            if !completion.metering_expected || completion.metering_read {
                return Err(ReadError::Query);
            }
            completion.metering_read = true;
            let enabled = completion.metering.take().ok_or(ReadError::Query)?()
                .map_err(ReadError::Metering)?;
            return codec::encode(&enabled, self.node_bytes).map_err(ReadError::Encoding);
        }
        if let [129, success @ 0..=1] = q {
            let completion = self.completion.as_mut().ok_or(ReadError::Query)?;
            if *success == 1 && completion.metering_expected && !completion.metering_read {
                return Err(ReadError::Query);
            }
            if let Some(native) = &completion.native {
                native.finish_native().map_err(ReadError::Phase)?;
            }
            completion.completed = true;
            completion.succeeded = *success == 1;
            return Ok(vec![]);
        }
        if let [128, id] = q {
            let control = self.controls.get(*id as usize).ok_or(ReadError::Query)?;
            return Ok(vec![u8::from(control.check().is_err())]);
        }
        if q.len() != 33 {
            return Err(ReadError::Query);
        }
        let hash = ae_types::Hash256(q[1..].try_into().map_err(|_| ReadError::Query)?);
        let trees = &self.view.trees;
        let value = match q[0] {
            0 => trees.accounts.tree().store().node(&hash),
            1 => trees.calls.tree().store().node(&hash),
            2 => trees.channels.tree().store().node(&hash),
            3 => trees.contracts.tree().store().node(&hash),
            4 => trees.names.tree().store().node(&hash),
            5 => trees.oracles.tree().store().node(&hash),
            6 => trees.names.cache().store().node(&hash),
            7 => trees.oracles.cache().store().node(&hash),
            _ => return Err(ReadError::Query),
        }
        .map_err(ReadError::Node)?;
        let Some(mut value) = value else {
            return Ok(vec![0]);
        };
        if value.len() > self.node_bytes {
            return Err(ReadError::Capacity);
        }
        if ae_crypto::blake2b_256(&value) != hash {
            return Err(ReadError::Node(ae_trie::Error::HashMismatch(hash)));
        }
        value
            .try_reserve_exact(1)
            .map_err(|_| ReadError::Capacity)?;
        value.insert(0, 1);
        Ok(value)
    }
}
pub fn submit<
    A: NodeReader + Clone + Send + Sync + 'static,
    C: NodeReader + Clone + Send + Sync + 'static,
>(
    client: &dry_run::Client,
    process: process::Config,
    view: Arc<View<Overlay<A>, Overlay<C>>>,
    request: Request<'_>,
    parameters: Parameters<'_>,
    timeout: Timeout,
) -> Result<Ticket<process::Completed<Broker<A, C>>>, Error> {
    // Reserve the maximum input weight before encoding or capturing configuration.
    let weight = process
        .reservation(process.input_bytes)
        .map_err(Error::Admission)?;
    let reservation = client.reserve(weight, timeout).map_err(Error::Admission)?;
    let (bytes, broker) = prepare(&process, view, request, parameters, Policies::default())?;
    reservation
        .submit(move |control| process::run(process, bytes, broker, control))
        .map_err(Error::Admission)
}
fn prepare<A: NodeReader + Clone, C: NodeReader + Clone>(
    process: &process::Config,
    view: Arc<View<Overlay<A>, Overlay<C>>>,
    request: Request<'_>,
    parameters: Parameters<'_>,
    policies: Policies,
) -> Result<(Vec<u8>, Broker<A, C>), Error> {
    let Policies {
        native,
        reporting,
        metering,
        heap: native_heap,
    } = policies;
    let deferred_reporting = reporting.is_some();
    let metering_expected = metering.is_some()
        && matches!(
            request.options.profile,
            core::Profile::Public | core::Profile::Internal
        );
    let completion =
        (native.is_some() || deferred_reporting || metering_expected).then_some(Completion {
            native,
            reporting,
            public: request.options.profile == core::Profile::Public,
            completed: false,
            succeeded: false,
            reporting_read: false,
            metering,
            metering_expected,
            metering_read: false,
        });
    if request.inputs.len() > parameters.preparation.requests
        || request.inputs.len() > request.options.max_transactions
        || request.funding.len() > request.options.max_accounts
    {
        return Err(Error::Capacity);
    }
    let mut remaining = process.input_bytes;
    let mut work = parameters.preparation.work_bytes;
    let mut inputs = vec![];
    for input in request.inputs {
        let data = match input {
            core::prepare::Input::Transaction(tx) => {
                let tx = tx.view();
                if tx.layers().len() > parameters.preparation.transaction.max_layers
                    || tx.wire_size() > parameters.preparation.encoded_bytes
                    || tx.wire_size() > remaining
                {
                    return Err(Error::Capacity);
                }
                for n in std::iter::once(tx.leaf().wire_size())
                    .chain(tx.layers().iter().map(|l| l.wire_size()))
                {
                    work = work.checked_sub(n).ok_or(Error::Capacity)?;
                }
                job::Input::Transaction(tx.encode())
            }
            core::prepare::Input::CallRequest(v) => job::Input::Call(codec::encode(v, remaining)?),
        };
        let length = match &data {
            job::Input::Transaction(v) | job::Input::Call(v) => v.len(),
        };
        remaining = remaining.checked_sub(length).ok_or(Error::Capacity)?;
        inputs.push(data);
    }
    let mut execution = metadata::Execution::capture(parameters.execution);
    if let Some(fate) = &mut execution.fate {
        fate.control = ae_types::work::Control::combine(
            fate.control.as_ref(),
            request.options.control.as_ref(),
        );
        fate.environment = view.environment.clone();
        fate.history = view.history.clone();
        fate.auth_tx = None;
    }
    execution.aevm = view.aevm.clone();
    let job = job::Job {
        roots: ae_chain::repository::Roots::of(&view.trees),
        context: view.context.clone(),
        funding: request.funding,
        inputs,
        execution,
        options: request.options,
        preparation: parameters.preparation,
        state: parameters.state.limits,
        atoms: parameters.state.existing_name_status_atoms.cloned(),
        overlays: parameters.overlays,
        node_decode: parameters.node_decode,
        rendering: parameters.rendering,
        reporting_floor: if deferred_reporting {
            None
        } else {
            parameters.reporting_floor.cloned()
        },
        output_bytes: process.output_bytes,
        core_notification: completion.is_some(),
        native_heap,
        metering_notification: metering_expected,
    };
    let (bytes, controls) = codec::capture(&job, process.input_bytes)?;
    let broker = Broker {
        view,
        controls,
        node_bytes: process.frame_bytes.saturating_sub(1),
        completion,
    };
    Ok((bytes, broker))
}
pub fn submit_http<
    A: NodeReader + Clone + Send + Sync + 'static,
    C: NodeReader + Clone + Send + Sync + 'static,
>(
    client: &dry_run::Client,
    process: process::Config,
    view: Arc<View<Overlay<A>, Overlay<C>>>,
    request: crate::http::dry_run::Request<'_>,
    options: core::Options,
    parameters: Parameters<'_>,
    timeout: Timeout,
) -> Result<Ticket<process::Completed<Broker<A, C>>>, Error> {
    submit(
        client,
        process,
        view,
        http_request(request, options),
        parameters,
        timeout,
    )
}
fn http_request<'a>(
    request: crate::http::dry_run::Request<'a>,
    mut options: core::Options,
) -> Request<'a> {
    options.profile = match request.profile {
        crate::http::dry_run::Profile::Public => core::Profile::Public,
        crate::http::dry_run::Profile::Internal => core::Profile::Internal,
    };
    options.tx_events = request.tx_events;
    Request {
        inputs: request.inputs,
        funding: request
            .accounts
            .into_iter()
            .map(|a| core::Funding {
                account: a.public_key,
                amount: a.amount,
            })
            .collect(),
        options,
    }
}
pub fn decode_reply(bytes: &[u8]) -> Result<Reply, EncodingError> {
    codec::decode(bytes)
}
#[derive(Debug)]
pub enum ServeError {
    Transport(process::wire::Error),
    Encoding(EncodingError),
    Poisoned,
    Heap(ae_worker_heap::PhaseError),
}
// This is used by the trusted worker executable, with its installed Guard.
// Bootstrap arms the guard before parsing any job or allocating decoded values.
pub fn serve<R: Read + Send + 'static, W: Write + Send + 'static>(
    input: R,
    output: W,
    guard: &ae_worker_heap::Guard,
) -> Result<(), ServeError> {
    let mut session =
        process::wire::Session::start(input, output, guard).map_err(ServeError::Transport)?;
    let bytes = session
        .initial(process::wire::MAX_FRAME)
        .map_err(ServeError::Transport)?;
    let session = Arc::new(Mutex::new(session));
    let resolve = |id| {
        let s = session.clone();
        ae_types::work::Control::new(move || {
            let Ok(mut s) = s.lock() else { return true };
            !matches!(s.read(&[128, id]), Ok(v) if v == [0])
        })
    };
    let job: job::Job = codec::decode_with(&bytes, Some(&resolve)).map_err(ServeError::Encoding)?;
    let maximum = job.output_bytes;
    if maximum > process::wire::MAX_FRAME {
        return Err(ServeError::Encoding(EncodingError::Capacity));
    }
    drop(bytes);
    let reply = job::execute(job, session.clone(), guard)?;
    let bytes = codec::encode(&reply, maximum).map_err(ServeError::Encoding)?;
    session
        .lock()
        .map_err(|_| ServeError::Poisoned)?
        .finish(&bytes)
        .map_err(ServeError::Transport)
}
