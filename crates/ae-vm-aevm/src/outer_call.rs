// aevm_eeevm recursive_call and aevm_eeevm_state call_contract, fa2a6d7.
// Copyright (c) 2017-2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    calls::{self, ContractVersion, FunctionTypes, InputHost},
    execution::{self, Budget, CallCreate, CallStep, Failure, Limits, State, Stop},
    gas,
    opcode::Version,
    primops::{self, ChainHost},
    runtime::{self, Native, Outcome},
    runtime_heap::TypeRef,
    store::OrderProfile,
    word::Word,
};
use num_bigint::{BigInt, BigUint};
use num_traits::Zero;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Complete { data: Vec<u8>, gas_spent: BigInt },
    Exception { reason: String, gas_spent: BigInt },
    Revert { data: Vec<u8>, gas_spent: BigInt },
}
pub struct Request<'a> {
    pub target: Word,
    pub gas: &'a BigUint,
    pub value: &'a BigInt,
    pub data: &'a [u8],
    pub call_stack: &'a [BigInt],
    pub origin: Word,
}
// Every host operation reads or returns a staged immutable chain snapshot.
// Neither a successful child call nor set_store may durably commit here.
pub trait Host: ChainHost {
    fn function_types(
        &self,
        target: Word,
        version: ContractVersion,
        function: Word,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> FunctionTypes<Self::Error>;
    fn balance(&self, address: Word, chain: &Self::Chain) -> Result<BigInt, Self::Error>;
    fn call(
        &self,
        request: Request<'_>,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> CallResult<Self::Error, Self::Chain>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault<E, C> {
    Runtime(runtime::Fault<E>),
    // Diagnostic snapshots of native case_clause terms, not committed results.
    ResponseCase {
        response: Response,
        state: Box<State>,
        chain: Box<C>,
    },
    VersionCase(Version),
}
pub type CallResult<E, C> = Result<(Response, C), Fault<E, C>>;
pub type Evaluation<E, C> = Result<Outcome<C>, Fault<E, C>>;
impl<E, C> From<runtime::Fault<E>> for Fault<E, C> {
    fn from(e: runtime::Fault<E>) -> Self {
        Self::Runtime(e)
    }
}
impl<E, C> From<Stop> for Fault<E, C> {
    fn from(e: Stop) -> Self {
        Self::Runtime(e.into())
    }
}
struct Bound<'a, H: Host> {
    host: &'a H,
    chain: &'a H::Chain,
}
impl<H: Host> InputHost for Bound<'_, H> {
    type Error = H::Error;
    fn oracle_format(
        &self,
        oracle: Word,
        response: bool,
        b: &mut Budget,
    ) -> crate::primops::HostResult<TypeRef, Self::Error> {
        crate::primops::oracle_format(self.host, self.chain, oracle, response, b)
    }
    fn function_types(
        &self,
        target: Word,
        version: ContractVersion,
        function: Word,
        budget: &mut Budget,
    ) -> FunctionTypes<Self::Error> {
        self.host
            .function_types(target, version, function, self.chain, budget)
    }
}
enum Problem<E, C> {
    Fault(Fault<E, C>),
    Vm(Failure, BigUint),
}
type StepResult<E, C> = Result<(CallStep, C), Problem<E, C>>;
type Preparation<E, C> = Result<Prepared<C>, Problem<E, C>>;
impl<E, C> From<Stop> for Problem<E, C> {
    fn from(e: Stop) -> Self {
        Self::Fault(e.into())
    }
}
impl<E, C> From<gas::Error> for Problem<E, C> {
    fn from(e: gas::Error) -> Self {
        Self::Fault(Fault::Runtime(e.into()))
    }
}
impl<E, C> From<runtime::Fault<E>> for Problem<E, C> {
    fn from(e: runtime::Fault<E>) -> Self {
        match e {
            runtime::Fault::Native(Native::OutOfGas) => Self::Vm(Failure::OutOfGas, 0u8.into()),
            other => Self::Fault(Fault::Runtime(other)),
        }
    }
}
impl<E, C> From<execution::Fault> for Problem<E, C> {
    fn from(e: execution::Fault) -> Self {
        match e {
            execution::Fault::Vm(reason) => Self::Vm(reason, 0u8.into()),
            execution::Fault::Stop(e) => e.into(),
            execution::Fault::Native(e) => e.into(),
        }
    }
}
fn spend<E, C>(s: &mut State, cost: BigUint) -> Result<(), Problem<E, C>> {
    if cost > s.gas {
        return Err(Problem::Vm(Failure::OutOfGas, 0u8.into()));
    }
    s.gas -= cost;
    Ok(())
}
fn peek<E, C>(s: &State, index: usize, b: &mut Budget) -> Result<Word, Problem<E, C>> {
    if let Some(value) = s.stack.get(index) {
        return Ok(*value);
    }
    b.bytes(s.stack.len().checked_mul(32).ok_or(Stop::ResourceLimit)?)?;
    Err(runtime::Fault::Native(Native::BadStackPeek {
        index,
        stack: s.stack.clone(),
    })
    .into())
}
fn pop<E, C>(s: &mut State) -> Result<Word, Problem<E, C>> {
    if s.stack.is_empty() {
        Err(Problem::Vm(Failure::PopEmpty, 0u8.into()))
    } else {
        Ok(s.stack.remove(0))
    }
}
fn failure(reason: String) -> Failure {
    if reason == "out_of_gas" {
        Failure::OutOfGas
    } else {
        Failure::Call(reason)
    }
}
fn refund(call_gas: &BigUint, spent: &BigInt, stipend: &BigUint) -> BigUint {
    (BigInt::from(call_gas.clone()) - spent - BigInt::from(stipend.clone()))
        .to_biguint()
        .unwrap_or_default()
}
struct Pending {
    state: Box<State>,
    destination: BigInt,
    input: Vec<u8>,
    output_type: Option<TypeRef>,
    out_offset: Word,
    out_size: Word,
    call_gas: BigUint,
    stipend: BigUint,
    after_spend: BigUint,
    limited: bool,
    value: BigInt,
    stack: Vec<BigInt>,
    origin: Word,
}
impl Pending {
    fn request(&self) -> Request<'_> {
        Request {
            target: Word::wrapping(&self.destination),
            gas: &self.call_gas,
            value: &self.value,
            data: &self.input,
            call_stack: &self.stack,
            origin: self.origin,
        }
    }
}
enum Prepared<C> {
    Step(CallStep, C),
    Call(Box<Pending>),
}
fn run<H: Host>(
    op: u8,
    state: &State,
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> StepResult<H::Error, H::Chain> {
    match prepare(op, state, chain, host, b)? {
        Prepared::Step(step, chain) => Ok((step, chain)),
        Prepared::Call(p) => {
            let (response, chain) = host.call(p.request(), chain, b).map_err(Problem::Fault)?;
            finish_response(*p, response, chain, b)
        }
    }
}
fn prepare<H: Host>(
    op: u8,
    state: &State,
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Preparation<H::Error, H::Chain> {
    let mut s = execution::snapshot(state, b, true)?;
    if s.call_stack.len() >= 1024 {
        return Ok(Prepared::Step(
            CallStep::Continue {
                value: Word::ZERO,
                state: Box::new(s),
            },
            chain.clone(),
        ));
    }
    let to = peek(&s, 1, b)?;
    let offset = peek(&s, if op == 0xf4 { 2 } else { 3 }, b)?;
    let size = peek(&s, if op == 0xf4 { 3 } else { 4 }, b)?;
    let gas_before = s.gas.clone();
    let input = calls::input_inner(
        &s,
        calls::Input {
            target: to,
            offset,
            size,
        },
        &Bound { host, chain },
        b,
    )?;
    s = *input.state;
    if s.version != Version::Solidity1 {
        let charge = gas::memory(
            &(input.data.len() / 32).into(),
            &s.gas_table,
            s.float_profile,
        )?;
        spend(&mut s, charge)?;
    }
    let (cap, cost) = execution::call_gas(&s)?;
    spend(&mut s, cost)?;
    let requested = pop(&mut s)?.unsigned();
    let limited = requested != gas_before;
    let target = pop(&mut s)?;
    let value = if op == 0xf4 {
        s.context.value.clone()
    } else {
        BigInt::from(pop(&mut s)?.unsigned())
    };
    pop(&mut s)?;
    pop(&mut s)?;
    let out_offset = pop(&mut s)?;
    let out_size = pop(&mut s)?;
    let balance = host
        .balance(Word::wrapping(&s.context.address), chain)
        .map_err(runtime::Fault::Host)?;
    b.bytes(usize::try_from(balance.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
    if value > balance {
        return Err(Problem::Vm(Failure::OutOfGas, 0u8.into()));
    }
    let destination = if op == 0xf1 {
        BigInt::from(target.unsigned())
    } else {
        s.context.address.clone()
    };
    let stipend = if s.version == Version::Solidity1 && !value.is_zero() {
        BigUint::from(s.gas_table.call_stipend)
    } else {
        BigUint::zero()
    };
    let call_gas = cap + &stipend;
    let caller = if op == 0xf4 {
        s.context.caller.clone()
    } else {
        s.context.address.clone()
    };
    let after_spend = s.gas.clone();
    if s.no_recursion {
        s.gas += &call_gas;
        b.bytes(input.data.len())?;
        b.bytes(128)?;
        s.call_creates.insert(
            0,
            CallCreate {
                data: input.data,
                destination,
                gas_limit: call_gas,
                value,
            },
        );
        return Ok(Prepared::Step(
            CallStep::Continue {
                value: Word::from(1),
                state: Box::new(s),
            },
            chain.clone(),
        ));
    }
    let (response, next_chain) = if s.version != Version::Solidity1 && destination.is_zero() {
        let (result, next_chain) =
            primops::call_inner(&s, &call_gas, &value, &input.data, chain, host, b)?;
        match result {
            primops::Outcome::Complete {
                data,
                gas_spent,
                state,
            } => {
                s = *state;
                (
                    Response::Complete {
                        data,
                        gas_spent: gas_spent.into(),
                    },
                    next_chain,
                )
            }
            primops::Outcome::Error(primops::Error::Chain(reason))
                if reason == "not_allowed_off_chain" || reason == "account_is_not_payable" =>
            {
                return Err(Problem::Vm(Failure::Call(reason), 0u8.into()));
            }
            primops::Outcome::Error(_) => return Err(Problem::Vm(Failure::OutOfGas, 0u8.into())),
        }
    } else {
        b.work(s.call_stack.len())?;
        b.bytes(
            (s.call_stack.len() + 1)
                .checked_mul(32)
                .ok_or(Stop::ResourceLimit)?,
        )?;
        for n in s.call_stack.iter().chain(std::iter::once(&caller)) {
            b.bytes(usize::try_from(n.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
        }
        let mut stack = Vec::with_capacity(s.call_stack.len() + 1);
        stack.push(caller);
        stack.extend_from_slice(&s.call_stack);
        b.bytes(std::mem::size_of::<Pending>())?;
        return Ok(Prepared::Call(Box::new(Pending {
            origin: Word::wrapping(&s.context.origin),
            state: Box::new(s),
            destination,
            input: input.data,
            output_type: input.output_type,
            out_offset,
            out_size,
            call_gas,
            stipend,
            after_spend,
            limited,
            value,
            stack,
        })));
    };
    finish_response(
        Pending {
            origin: Word::ZERO,
            state: Box::new(s),
            destination,
            input: input.data,
            output_type: input.output_type,
            out_offset,
            out_size,
            call_gas,
            stipend,
            after_spend,
            limited,
            value,
            stack: vec![],
        },
        response,
        next_chain,
        b,
    )
    .map(|(step, chain)| Prepared::Step(step, chain))
}
fn finish_response<E, C>(
    p: Pending,
    response: Response,
    next_chain: C,
    b: &mut Budget,
) -> StepResult<E, C> {
    let Pending {
        state,
        destination,
        input,
        output_type,
        out_offset,
        out_size,
        call_gas,
        stipend,
        after_spend,
        limited,
        ..
    } = p;
    let mut s = *state;
    let spent = match &response {
        Response::Complete { data, gas_spent } | Response::Revert { data, gas_spent } => {
            b.bytes(data.len())?;
            gas_spent
        }
        Response::Exception { reason, gas_spent } => {
            b.bytes(reason.len())?;
            gas_spent
        }
    };
    b.bytes(usize::try_from(spent.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
    match response {
        Response::Complete { ref gas_spent, .. }
            if gas_spent < &BigInt::zero() || gas_spent > &BigInt::from(call_gas.clone()) =>
        {
            Err(Problem::Fault(Fault::ResponseCase {
                response,
                state: Box::new(s),
                chain: Box::new(next_chain),
            }))
        }
        Response::Complete { data, gas_spent } => {
            s.gas = after_spend + refund(&call_gas, &gas_spent, &stipend);
            let (pointer, s) = calls::return_inner(
                &s,
                calls::Return {
                    target: Word::wrapping(&destination),
                    input: &input,
                    output_offset: out_offset,
                    output_size: out_size,
                    data: &data,
                    ty: output_type.as_ref(),
                },
                b,
            )
            .map_err(|e| e.widen())?;
            Ok((
                CallStep::Continue {
                    value: Word::from_unsigned(&pointer),
                    state: Box::new(s),
                },
                next_chain,
            ))
        }
        Response::Exception { reason, gas_spent } => {
            let gas = if s.version == Version::Solidity1
                || (!limited && gas_spent == BigInt::from(call_gas.clone()))
            {
                BigUint::zero()
            } else if limited {
                after_spend + refund(&call_gas, &gas_spent, &stipend)
            } else {
                return Err(Problem::Fault(Fault::VersionCase(s.version)));
            };
            Err(Problem::Vm(failure(reason), gas))
        }
        Response::Revert { data, gas_spent } => Ok((
            CallStep::Halt(execution::Outcome::Revert {
                output: data,
                gas: after_spend + refund(&call_gas, &gas_spent, &stipend),
            }),
            next_chain,
        )),
    }
}
pub(crate) fn evaluate_inner<H: Host>(
    state: &State,
    chain: &H::Chain,
    host: &H,
    b: &mut Budget,
) -> Evaluation<H::Error, H::Chain> {
    b.enter_call()?;
    let mut current = chain.clone();
    let result = execution::evaluate_calls(state, b, true, |op, s, b| {
        match run(op, s, &current, host, b) {
            Ok((step, next)) => {
                current = next;
                Ok(step)
            }
            Err(Problem::Vm(reason, gas)) => {
                Ok(CallStep::Halt(execution::Outcome::Error { reason, gas }))
            }
            Err(Problem::Fault(e)) => Err(e),
        }
    });
    b.leave_call();
    let result = result.map_err(|e| match e {
        execution::RunError::Stop(e) => Fault::from(e),
        execution::RunError::Call(e) => e,
    })?;
    match result {
        execution::Outcome::Complete(state) => Ok(Outcome::Complete {
            state,
            chain: current,
        }),
        execution::Outcome::Error { reason, gas } => Ok(Outcome::Error { reason, gas }),
        execution::Outcome::Revert { output, gas } => Ok(Outcome::Revert { output, gas }),
        execution::Outcome::Native(e) => Err(Fault::Runtime(e.into())),
    }
}
pub fn evaluate<H: Host>(
    state: &State,
    chain: &H::Chain,
    host: &H,
    limits: Limits,
) -> Evaluation<H::Error, H::Chain> {
    evaluate_inner(state, chain, host, &mut Budget::new(limits))
}
pub fn invoke<H: Host>(
    state: &State,
    call_data_type: Option<&TypeRef>,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    limits: Limits,
) -> Evaluation<H::Error, H::Chain> {
    invoke_budgeted(
        state,
        call_data_type,
        chain,
        host,
        profile,
        &mut Budget::new(limits),
    )
}
pub fn invoke_budgeted<H: Host>(
    state: &State,
    call_data_type: Option<&TypeRef>,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    b: &mut Budget,
) -> Evaluation<H::Error, H::Chain> {
    let state = runtime::initialize_inner(state, call_data_type, b)?;
    match evaluate_inner(&state, chain, host, b)? {
        Outcome::Complete { state, chain } => {
            runtime::finalize_inner(&state, &chain, host, profile, b).map_err(Fault::Runtime)
        }
        other => Ok(other),
    }
}

// A suspended invocation owns a prepared external call and its exact interpreter
// continuation. It does not own or mutate the caller's native stack or budget.
pub struct Suspension<C> {
    pending: Box<Pending>,
    continuation: execution::Continuation,
    chain: C,
}
impl<C> Suspension<C> {
    pub fn request(&self) -> Request<'_> {
        self.pending.request()
    }
    pub fn chain(&self) -> &C {
        &self.chain
    }
}
pub enum Progress<C> {
    Complete(Outcome<C>),
    Call(Box<Suspension<C>>),
}
pub type ProgressResult<E, C> = Result<Progress<C>, Fault<E, C>>;
enum Start<'a> {
    Fresh(&'a State),
    Resume(execution::Continuation, CallStep),
}
fn poll<H: Host>(
    start: Start<'_>,
    mut current: H::Chain,
    host: &H,
    profile: OrderProfile,
    b: &mut Budget,
) -> ProgressResult<H::Error, H::Chain> {
    let mut pending = None;
    let call = |op, s: &State, b: &mut Budget| match prepare(op, s, &current, host, b) {
        Ok(Prepared::Step(step, next)) => {
            current = next;
            Ok(step)
        }
        Ok(Prepared::Call(p)) => {
            pending = Some(p);
            Ok(CallStep::Suspend)
        }
        Err(Problem::Vm(reason, gas)) => {
            Ok(CallStep::Halt(execution::Outcome::Error { reason, gas }))
        }
        Err(Problem::Fault(e)) => Err(e),
    };
    let segment = match start {
        Start::Fresh(s) => execution::evaluate_segment(s, b, true, call),
        Start::Resume(c, step) => execution::resume_segment(c, step, b, call),
    }
    .map_err(|e| match e {
        execution::RunError::Stop(e) => Fault::from(e),
        execution::RunError::Call(e) => e,
    })?;
    match segment {
        execution::Segment::Suspended(continuation) => {
            b.bytes(std::mem::size_of::<Suspension<H::Chain>>())?;
            Ok(Progress::Call(Box::new(Suspension {
                pending: pending.expect("suspended external call"),
                continuation,
                chain: current,
            })))
        }
        execution::Segment::Done(outcome) => {
            let result = match outcome {
                execution::Outcome::Complete(state) => {
                    runtime::finalize_inner(&state, &current, host, profile, b)
                        .map_err(Fault::Runtime)?
                }
                execution::Outcome::Error { reason, gas } => Outcome::Error { reason, gas },
                execution::Outcome::Revert { output, gas } => Outcome::Revert { output, gas },
                execution::Outcome::Native(n) => return Err(Fault::Runtime(n.into())),
            };
            Ok(Progress::Complete(result))
        }
    }
}
// The enclosing driver must check the shared Budget's active invocation depth.
// Neither entry nor resumption calls Host::call; external calls are yielded.
pub fn start<H: Host>(
    state: &State,
    call_data_type: Option<&TypeRef>,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    b: &mut Budget,
) -> ProgressResult<H::Error, H::Chain> {
    let state = runtime::initialize_inner(state, call_data_type, b)?;
    poll(Start::Fresh(&state), chain.clone(), host, profile, b)
}
pub fn resume<H: Host>(
    suspension: Suspension<H::Chain>,
    response: Response,
    chain: H::Chain,
    host: &H,
    profile: OrderProfile,
    b: &mut Budget,
) -> ProgressResult<H::Error, H::Chain> {
    let Suspension {
        pending,
        continuation,
        ..
    } = suspension;
    match finish_response(*pending, response, chain, b) {
        Ok((step, chain)) => poll(Start::Resume(continuation, step), chain, host, profile, b),
        Err(Problem::Vm(reason, gas)) => Ok(Progress::Complete(Outcome::Error { reason, gas })),
        Err(Problem::Fault(e)) => Err(e),
    }
}
