use super::*;
enum OwnedInvocation {
    Contract(ContractTransaction),
    Attach(ae_tx::GaAttachTransaction),
}
impl OwnedInvocation {
    fn new(tx: Invocation<'_>) -> Self {
        match tx {
            Invocation::Contract(t) => Self::Contract(t.clone()),
            Invocation::Attach(t) => Self::Attach(t.clone()),
        }
    }
    fn view(&self) -> Invocation<'_> {
        match self {
            Self::Contract(t) => Invocation::Contract(t),
            Self::Attach(t) => Invocation::Attach(t),
        }
    }
}
struct Frame<A, C> {
    tx: OwnedInvocation,
    context: Context,
    rollback: Trees<A, C>,
    events: Vec<Event>,
    created: Option<Contract>,
}
struct Waiting<A, C> {
    frame: Frame<A, C>,
    call: Call,
    initial: aevm_chain::State<A, C>,
    suspension: Box<outer_call::Suspension<aevm_chain::State<A, C>>>,
}
type Begun<A, C> = Result<(Frame<A, C>, Step<A, C>), Error<A, C>>;
fn begin<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: Invocation<'_>,
    trees: &Trees<A, C>,
    context: &Context,
    config: &Config,
    b: &mut Budget,
) -> Begun<A, C> {
    b.work(1)?;
    b.bytes(tx.wire_size())?;
    let (p, created) = match tx {
        Invocation::Contract(t) if !tx.is_create() => (
            prepare_call(t, trees, context).map_err(transaction_fault)?,
            None,
        ),
        _ => {
            let (p, c) = create::prepare(tx, trees, context, b)?;
            (p, Some(c))
        }
    };
    let step = start(tx, &p.contract, p.call, p.trees, context, config, b)?;
    Ok((
        Frame {
            tx: OwnedInvocation::new(tx),
            context: context.clone(),
            rollback: p.rollback,
            events: p.events,
            created,
        },
        step,
    ))
}
// Each iteration enters a child or resumes its parent. No host callback on this
// path recurses into process; the Vec owns all suspended VM/transaction frames.
pub(super) fn process<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: Invocation<'_>,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config,
    b: &mut Budget,
) -> Result<Receipt, Error<A, C>> {
    b.check_call_depth(1)?;
    let (mut frame, mut step) = begin(tx, trees, context, config, b)?;
    let mut waiting: Vec<Waiting<A, C>> = Vec::new();
    let host = Host::new(config);
    loop {
        match step {
            Step::Complete(completed) => {
                let completed = match frame.created {
                    Some(contract) => create::finish(contract, completed, config, b)?,
                    None => completed,
                };
                let (receipt, committed) = settle_detached(
                    frame.tx.view(),
                    frame.rollback,
                    &frame.context,
                    completed,
                    frame.events,
                )
                .map_err(transaction_fault)?;
                let Some(parent) = waiting.pop() else {
                    *trees = committed;
                    return Ok(receipt);
                };
                let (response, next) =
                    host::finish_callback(parent.suspension.chain(), committed, receipt, b)?;
                frame = parent.frame;
                step = advance(
                    frame.tx.view(),
                    parent.call,
                    parent.initial,
                    outer_call::resume(
                        *parent.suspension,
                        response,
                        next,
                        &host,
                        config.store_order,
                        b,
                    ),
                )?;
            }
            Step::Suspended {
                call,
                initial,
                suspension,
            } => {
                let request = suspension.request();
                let chain = suspension.chain();
                let prepared = host::prepare_callback(request, chain, b)?;
                let immediate = match prepared {
                    Err(response) => Some((response, chain.clone())),
                    Ok(tx) => {
                        b.check_call_depth(
                            waiting.len().checked_add(2).ok_or(Stop::ResourceLimit)?,
                        )?;
                        let mut child_context = (*chain.context).clone();
                        child_context.origin = Origin::Contract;
                        match begin(
                            Invocation::Contract(&tx),
                            &chain.trees,
                            &child_context,
                            config,
                            b,
                        ) {
                            Ok((child, child_step)) => {
                                b.bytes(std::mem::size_of::<Waiting<A, C>>())?;
                                waiting.push(Waiting {
                                    frame,
                                    call,
                                    initial,
                                    suspension,
                                });
                                frame = child;
                                step = child_step;
                                continue;
                            }
                            Err(e) => Some(host::reject_callback(
                                e,
                                &BigUint::from_bytes_be(tx.gas().as_bytes()),
                                chain,
                            )?),
                        }
                    }
                };
                let (response, next) = immediate.expect("callback response");
                step = advance(
                    frame.tx.view(),
                    call,
                    initial,
                    outer_call::resume(*suspension, response, next, &host, config.store_order, b),
                )?;
            }
        }
    }
}
