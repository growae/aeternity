use super::{
    codec::{Decoder, Encoder, Error, Wire, structure},
    metadata::Execution,
};
use ae_execution::{Context, dry_run as core};
use std::{collections::BTreeSet, sync::Arc};

pub(super) enum Input {
    Transaction(Vec<u8>),
    Call(Vec<u8>),
}
impl Wire for Input {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Transaction(v) => {
                0_u8.put(e)?;
                e.blob(v)
            }
            Self::Call(v) => {
                1_u8.put(e)?;
                e.blob(v)
            }
        }
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(d)? {
            0 => Ok(Self::Transaction(d.blob()?.to_vec())),
            1 => Ok(Self::Call(d.blob()?.to_vec())),
            _ => Err(Error::Invalid),
        }
    }
}
pub(super) struct Job {
    pub roots: ae_chain::repository::Roots,
    pub context: Context,
    pub funding: Vec<core::Funding>,
    pub inputs: Vec<Input>,
    pub execution: Execution,
    pub options: core::Options,
    pub preparation: core::prepare::Limits,
    pub state: ae_state::transport::Limits,
    pub atoms: Option<BTreeSet<Vec<u8>>>,
    pub overlays: ae_trie::OverlayLimits,
    pub node_decode: ae_codec::rlp::DecodeLimits,
    pub rendering: crate::http::dry_run::response::Limits,
    pub reporting_floor: Option<ae_types::Unsigned>,
    pub output_bytes: usize,
    pub core_notification: bool,
    pub native_heap: Option<usize>,
}
impl Wire for Job {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.raw(b"AED4")?;
        self.roots.put(e)?;
        self.context.put(e)?;
        self.funding.put(e)?;
        self.inputs.put(e)?;
        self.execution.put(e)?;
        self.options.put(e)?;
        self.preparation.put(e)?;
        self.state.put(e)?;
        self.atoms.put(e)?;
        self.overlays.put(e)?;
        self.node_decode.put(e)?;
        self.rendering.put(e)?;
        self.reporting_floor.put(e)?;
        self.output_bytes.put(e)?;
        self.core_notification.put(e)?;
        self.native_heap.put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        if d.raw(4)? != b"AED4" {
            return Err(Error::Invalid);
        }
        Ok(Self {
            roots: Wire::get(d)?,
            context: Wire::get(d)?,
            funding: Wire::get(d)?,
            inputs: Wire::get(d)?,
            execution: Wire::get(d)?,
            options: Wire::get(d)?,
            preparation: Wire::get(d)?,
            state: Wire::get(d)?,
            atoms: Wire::get(d)?,
            overlays: Wire::get(d)?,
            node_decode: Wire::get(d)?,
            rendering: Wire::get(d)?,
            reporting_floor: Wire::get(d)?,
            output_bytes: Wire::get(d)?,
            core_notification: Wire::get(d)?,
            native_heap: Wire::get(d)?,
        })
    }
}
#[derive(Debug)]
pub struct ResultBody {
    pub body: serde_json::Value,
    pub roots: ae_chain::repository::Roots,
    pub context: Context,
}
structure!(ResultBody {
    body,
    roots,
    context
});
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    State,
    Execution,
    Rendering,
}
super::codec::enumeration!(Stage{State=0,Execution=1,Rendering=2});
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Interrupted,
    Capacity,
    NegativeBalance,
    Preparation,
    Operational,
    BadCallRequest,
}
super::codec::enumeration!(Kind{Interrupted=0,Capacity=1,NegativeBalance=2,Preparation=3,Operational=4,BadCallRequest=5});
// Diagnostic is bounded Rust diagnostic text for operators, not an HTTP reason
// or a replacement for a captured native exception rendering policy.
#[derive(Debug)]
pub struct Failure {
    pub stage: Stage,
    pub kind: Kind,
    pub index: Option<usize>,
    pub account: Option<ae_types::AccountPublicKey>,
    pub balance: Option<num_bigint::BigInt>,
    pub diagnostic: String,
    pub native_reason: Option<String>,
}
structure!(Failure {
    stage,
    kind,
    index,
    account,
    balance,
    diagnostic,
    native_reason
});
#[derive(Debug)]
pub enum Reply {
    Done(Box<ResultBody>),
    Failed(Failure),
}
impl Wire for Reply {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.raw(b"AER2")?;
        match self {
            Self::Done(v) => {
                0_u8.put(e)?;
                v.as_ref().put(e)
            }
            Self::Failed(v) => {
                1_u8.put(e)?;
                v.put(e)
            }
        }
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        if d.raw(4)? != b"AER2" {
            return Err(Error::Invalid);
        }
        match u8::get(d)? {
            0 => Ok(Self::Done(Box::new(Wire::get(d)?))),
            1 => Ok(Self::Failed(Wire::get(d)?)),
            _ => Err(Error::Invalid),
        }
    }
}
fn diagnostic(v: &impl std::fmt::Debug) -> String {
    struct Text(String);
    impl std::fmt::Write for Text {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            if self.0.len().saturating_add(s.len()) > 8192 {
                return Err(std::fmt::Error);
            }
            self.0.push_str(s);
            Ok(())
        }
    }
    let mut out = Text(String::new());
    let _ = std::fmt::write(&mut out, format_args!("{v:?}"));
    out.0
}
impl Failure {
    fn new(stage: Stage, kind: Kind, v: &impl std::fmt::Debug) -> Self {
        Self {
            stage,
            kind,
            index: None,
            account: None,
            balance: None,
            diagnostic: diagnostic(v),
            native_reason: None,
        }
    }
    fn execution<A: std::fmt::Debug, C: std::fmt::Debug>(e: core::Error<A, C>) -> Self {
        let kind = match &e {
            core::Error::Interrupted => Kind::Interrupted,
            core::Error::ResourceLimit => Kind::Capacity,
            core::Error::NegativeBalance { .. } => Kind::NegativeBalance,
            core::Error::Preparation(_) => Kind::Preparation,
            _ => Kind::Operational,
        };
        let mut f = Self::new(Stage::Execution, kind, &e);
        match e {
            core::Error::NegativeBalance {
                index,
                account,
                balance,
            } => {
                f.index = Some(index);
                f.account = Some(account);
                f.balance = Some(balance)
            }
            core::Error::Preparation(p) => f.index = p.index,
            core::Error::Operational { index, .. } => f.index = Some(index),
            _ => {}
        }
        f
    }
}
type CoreOutput<R, W> = core::Output<
    ae_sync::dry_run::process::trie::Reader<R, W>,
    ae_sync::dry_run::process::trie::Reader<R, W>,
>;
fn execute_core<R: std::io::Read, W: std::io::Write>(
    job: &Job,
    session: Arc<std::sync::Mutex<ae_sync::dry_run::process::wire::Session<R, W>>>,
) -> Result<CoreOutput<R, W>, Box<Failure>> {
    use ae_state::{
        Trees, accounts::Accounts, calls::Calls, channels::Channels, contracts::Contracts,
        names::Names, oracles::Oracles,
    };
    use ae_trie::Overlay;
    let source = |id| {
        Overlay::new(
            ae_sync::dry_run::process::trie::Reader::new(session.clone(), job.node_decode)
                .with_namespace(id),
            job.overlays,
        )
    };
    let roots = &job.roots;
    let trees = (|| -> Result<_, String> {
        Ok(Trees {
            accounts: Accounts::from_root(roots.state.accounts, source(0))
                .map_err(|e| diagnostic(&e))?,
            calls: Calls::from_root(roots.state.calls, source(1)).map_err(|e| diagnostic(&e))?,
            channels: Channels::from_root(roots.state.channels, source(2))
                .map_err(|e| diagnostic(&e))?,
            contracts: Contracts::from_root(roots.state.contracts, source(3))
                .map_err(|e| diagnostic(&e))?,
            names: Names::from_roots(roots.state.names, roots.names, source(4), source(6))
                .map_err(|e| diagnostic(&e))?,
            oracles: Oracles::from_roots(roots.state.oracles, roots.oracles, source(5), source(7))
                .map_err(|e| diagnostic(&e))?,
        })
    })();
    let trees = match trees {
        Ok(t) => t,
        Err(e) => return Err(Box::new(Failure::new(Stage::State, Kind::Operational, &e))),
    };
    // Funding precedes request preparation, preserving native failure order.
    let setup = match core::setup(trees, &job.context, &job.funding, &job.options) {
        Ok(v) => v,
        Err(e) => return Err(Box::new(Failure::execution(e))),
    };
    let state = ae_state::transport::Config {
        limits: job.state,
        existing_name_status_atoms: job.atoms.as_ref(),
    };
    let calls = job
        .inputs
        .iter()
        .map(|input| match input {
            Input::Call(bytes) => super::codec::decode::<serde_json::Value>(bytes).map(Some),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>, _>>();
    let calls = match calls {
        Ok(v) => v,
        Err(e) => {
            return Err(Box::new(Failure::new(
                Stage::Execution,
                Kind::Preparation,
                &e,
            )));
        }
    };
    let mut inputs = vec![];
    for (index, input) in job.inputs.iter().enumerate() {
        inputs.push(match input {
            Input::Call(_) => {
                core::prepare::Input::CallRequest(calls[index].as_ref().expect("decoded call"))
            }
            Input::Transaction(bytes) => match ae_tx::Transaction::decode_with_state_config(
                bytes,
                job.preparation.transaction,
                state,
            ) {
                Ok(tx) => core::prepare::Input::transaction(tx),
                Err(e) => {
                    let mut f = Failure::new(Stage::Execution, Kind::Preparation, &e);
                    f.index = Some(index);
                    return Err(Box::new(f));
                }
            },
        })
    }
    let output = job
        .execution
        .with_config(|c| setup.requests(c, inputs, job.preparation, state));
    let output = match output {
        Ok(v) => v,
        Err(e) => {
            let bad_call = crate::http::dry_run::failure::bad_call_index(&e)
                .and_then(|index| calls.get(index))
                .and_then(Option::as_ref);
            let mut failure = Failure::execution(e);
            if let Some(call) = bad_call {
                match crate::http::dry_run::failure::bad_call(call, job.rendering.diagnostic) {
                    Ok(reason) => {
                        failure.kind = Kind::BadCallRequest;
                        failure.native_reason = Some(reason)
                    }
                    Err(e) => {
                        return Err(Box::new(Failure::new(
                            Stage::Rendering,
                            if e.is_capacity() {
                                Kind::Capacity
                            } else {
                                Kind::Operational
                            },
                            &e,
                        )));
                    }
                }
            }
            return Err(Box::new(failure));
        }
    };
    Ok(output)
}
pub(super) fn execute<R: std::io::Read, W: std::io::Write>(
    job: Job,
    session: Arc<std::sync::Mutex<ae_sync::dry_run::process::wire::Session<R, W>>>,
    guard: &ae_worker_heap::Guard,
) -> Result<Reply, super::ServeError> {
    let heap = guard
        .begin_native(job.native_heap)
        .map_err(super::ServeError::Heap)?;
    let output = execute_core(&job, session.clone());
    drop(heap);
    if job.core_notification {
        let reply = session
            .lock()
            .map_err(|_| super::ServeError::Poisoned)?
            .read(&[129, u8::from(output.is_ok())])
            .map_err(super::ServeError::Transport)?;
        if !reply.is_empty() {
            return Err(super::ServeError::Encoding(Error::Invalid));
        }
    }
    let output = match output {
        Ok(v) => v,
        Err(e) => return Ok(Reply::Failed(*e)),
    };
    let profile = if job.options.profile == core::Profile::Public {
        crate::http::dry_run::Profile::Public
    } else {
        crate::http::dry_run::Profile::Internal
    };
    let render = |floor: Option<&ae_types::Unsigned>| {
        crate::http::dry_run::response::body(
            &output,
            profile,
            job.options.tx_events,
            floor,
            job.rendering,
        )
    };
    let body = render(job.reporting_floor.as_ref());
    // Native dry_run_results must succeed before the public floor getter runs.
    // An override reuses the bounded serializer; discard the first body before
    // reconstructing it so both response trees are never retained together.
    let body = match body {
        Ok(body) if job.core_notification && profile == crate::http::dry_run::Profile::Public => {
            let reply = session
                .lock()
                .map_err(|_| super::ServeError::Poisoned)?
                .read(&[130])
                .map_err(super::ServeError::Transport)?;
            let floor: Option<Option<ae_types::Unsigned>> =
                super::codec::decode(&reply).map_err(super::ServeError::Encoding)?;
            match floor {
                Some(Some(floor)) => {
                    drop(body);
                    render(Some(&floor))
                }
                _ => Ok(body),
            }
        }
        other => other,
    };
    Ok(match body {
        Ok(body) => Reply::Done(Box::new(ResultBody {
            body,
            roots: ae_chain::repository::Roots::of(&output.trees),
            context: output.context,
        })),
        Err(e) => Reply::Failed(Failure::new(
            Stage::Rendering,
            if matches!(e, crate::http::dry_run::response::Error::Capacity) {
                Kind::Capacity
            } else {
                Kind::Operational
            },
            &e,
        )),
    })
}
