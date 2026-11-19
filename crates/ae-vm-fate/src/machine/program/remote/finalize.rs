// aefa_engine_state:finalize and aefa_fate:loop, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::store::Backend;

// Store installation changes the invocation's chain cache, not published
// state roots. As for Host, a clone must isolate all logical mutations.
pub trait StoreHost<S: Backend>: Host<S::Error> {
    fn set_contract_store(
        &mut self,
        contract: [u8; 32],
        store: S,
        limits: Limits,
    ) -> Result<Limits, HostFailure<S::Error>>;
}

impl<S: CachedSubtreeBackend, O: MapOrder> Executor<'_, S, O> {
    // Complete a returned program once: serial traversal, store writes and
    // chain-cache installation, then final result traversal. The VM's dirty
    // store cache stays unchanged, including on successful completion.
    pub fn finalize_remote_program<H: StoreHost<S>>(
        &self,
        input: &State<S, H>,
        limits: Limits,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        if !input.program.cursor.returned || input.program.cursor.suspended {
            return Err(Error::FinalizationRequiresReturn);
        }
        let limits = input
            .session
            .host
            .environment()
            .budget(limits)
            .map_err(Error::Data)?;
        let limits = input
            .program
            .cursor
            .budget(input.session.budget(limits).map_err(Error::Data)?)
            .map_err(Error::Data)?;
        let context = Context {
            current_contract: input.session.current_contract,
            vm_version: input.session.vm_version,
            reserved_gas: input.program.cursor.reserved(),
            ..self.context.clone()
        };
        let executor = Executor {
            context: &context,
            order: self.order,
            overrides: self.overrides,
            contracts: self.contracts,
        };
        let mut session = input.session.clone();
        let (result, host) = executor.action_with_host(
            &input.program.machine,
            &[],
            limits,
            session.host.clone(),
            |step| {
                let charged = step
                    .state
                    .stores
                    .charge_finalization_checked(
                        step.state.meter.gas.clone(),
                        step.executor.order,
                        step.remaining,
                        Some(step.native_capacity()),
                    )
                    .map_err(|e| match e {
                        StoreError::Data(DataError::InvalidValue | DataError::InvalidType) => {
                            step.native("function_clause")
                        }
                        e => step.store_fault(e),
                    })?;
                step.state.meter.gas = charged.gas;
                step.remaining = charged.remaining_limits;
                let written = step
                    .state
                    .stores
                    .clone()
                    .finalize(
                        step.state.meter.gas.clone(),
                        step.executor.order,
                        step.remaining,
                    )
                    .map_err(|e| step.store_fault(e))?;
                step.remaining = written.remaining_limits;
                // Original finalize_stores installs the reverse-fold list only
                // after every contract's planning/write stage has succeeded.
                for (contract, store) in written.stores {
                    step.remaining = step
                        .host
                        .set_contract_store(contract, store, step.remaining)
                        .map_err(Step::<S, O, H>::host_fault)?;
                }
                step.state.meter.gas = written.gas;
                let result = step.state.accumulator.top.clone();
                // A failure here retains the newly installed chain cache. It
                // cannot roll back to the pre-finalization host checkpoint.
                step.traversal(&result, gas::CostModel::Final)
            },
        )?;
        session.host = host;
        Ok(outcome(
            super::super::outcome(result, input.program.cursor.clone()),
            session,
        ))
    }

    // Like original execute/loop: paused and failed runs bypass finalization.
    // max_steps is an operational bound, separate from consensus gas.
    pub fn execute_remote_program<H: StoreHost<S>>(
        &self,
        input: &State<S, H>,
        limits: Limits,
        max_steps: usize,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        match self.run_remote_program(input, limits, max_steps)? {
            Outcome::Returned {
                state,
                remaining_limits,
            } => self.finalize_remote_program(&state, remaining_limits),
            other => Ok(other),
        }
    }
}
