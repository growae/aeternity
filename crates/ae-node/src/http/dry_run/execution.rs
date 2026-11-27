use super::{Profile, Request};
use ae_execution::{dry_run as core, transactions};
use ae_hyperchain::chain::View;
use ae_trie::{NodeReader, Overlay};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::sync::Arc;

// Keep the selected immutable reader alive through execution and rendering.
// The service must additionally retain its admission/output credits until send.
pub struct Captured<A, C> {
    pub view: Arc<View<Overlay<A>, Overlay<C>>>,
    pub output: core::Output<A, C>,
}
#[derive(Debug)]
pub enum Error<E, A, C> {
    Selection(E),
    Execution(core::Error<A, C>),
}
pub fn execute<A: NodeReader + Clone, C: NodeReader + Clone, O: MapOrder, E>(
    request: Request<'_>,
    select: impl FnOnce(Hash256) -> Result<Arc<View<Overlay<A>, Overlay<C>>>, E>,
    base: &transactions::Config<'_, O>,
    options: &core::Options,
    limits: core::prepare::Limits,
    state: ae_state::transport::Config<'_>,
) -> Result<Captured<A, C>, Error<E, A, C>> {
    let view = select(request.top).map_err(Error::Selection)?;
    let mut options = options.clone();
    options.profile = match request.profile {
        Profile::Public => core::Profile::Public,
        Profile::Internal => core::Profile::Internal,
    };
    options.tx_events = request.tx_events;
    let funding: Vec<_> = request
        .accounts
        .into_iter()
        .map(|a| core::Funding {
            account: a.public_key,
            amount: a.amount,
        })
        .collect();
    let fate = base.fate.map(|f| {
        let mut f = f.with_control(options.control.as_ref());
        f.environment = &view.environment;
        f.history = view.history.clone();
        f.auth_tx = None;
        f
    });
    let config = transactions::Config {
        fate: fate.as_ref(),
        aevm: view.aevm.as_ref(),
        ..*base
    };
    let setup = core::setup((*view.trees).clone(), &view.context, &funding, &options)
        .map_err(Error::Execution)?;
    let output = setup
        .requests(&config, request.inputs, limits, state)
        .map_err(Error::Execution)?;
    Ok(Captured { view, output })
}

pub fn from_snapshot<S: ae_chain::repository::Read, O: MapOrder>(
    request: Request<'_>,
    snapshot: &ae_chain::repository::execution::Snapshot<S>,
    base: &transactions::Config<'_, O>,
    options: &core::Options,
    limits: core::prepare::Limits,
    state: ae_state::transport::Config<'_>,
) -> Result<Captured<S, S>, Error<ae_chain::dry_run::Error<ae_hyperchain::chain::LookupError>, S, S>>
{
    execute(
        request,
        |hash| snapshot.dry_run_view(&ae_chain::dry_run::Selector::Hash(hash)),
        base,
        options,
        limits,
        state,
    )
}
