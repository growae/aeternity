use super::*;
struct Frame<A, C> {
    tx: ContractTransaction,
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
    tx: &ContractTransaction,
    trees: &Trees<A, C>,
    context: &Context,
    config: &Config,
    b: &mut Budget,
) -> Begun<A, C> {
    b.work(1)?;
    b.bytes(tx.wire_size())?;
    let (p, created) = if matches!(tx.operation(), ContractOperation::Create { .. }) {
        let (p, c) = create::prepare(tx, trees, context, b)?;
        (p, Some(c))
    } else {
        (
            prepare_call(tx, trees, context).map_err(transaction_fault)?,
            None,
        )
    };
    let step = start(tx, &p.contract, p.call, p.trees, context, config, b)?;
    Ok((
        Frame {
            tx: tx.clone(),
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
    tx: &ContractTransaction,
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
                    &frame.tx,
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
                    &frame.tx,
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
                        match begin(&tx, &chain.trees, &child_context, config, b) {
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
                    &frame.tx,
                    call,
                    initial,
                    outer_call::resume(*suspension, response, next, &host, config.store_order, b),
                )?;
            }
        }
    }
}
