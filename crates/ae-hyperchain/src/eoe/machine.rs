use super::*;
use ae_vm_fate::maps::MapOrder;
use payload::{ConversionError, Converted, EPOCH, LEADER};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    AwaitEpoch,
    Proposal,
    Vote,
    Finalize,
    Complete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timeout {
    NoProposal,
    NoQuorum,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionKind {
    KeepData,
    Keep,
    Next,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply<R> {
    NotReady,
    NoConsensus,
    Finalized(R),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition<R> {
    pub kind: TransitionKind,
    pub timeout: Option<(Unsigned, Timeout)>,
    pub replies: Vec<(u64, Reply<R>)>,
}
impl<R> Transition<R> {
    fn keep_data() -> Self {
        Self {
            kind: TransitionKind::KeepData,
            timeout: None,
            replies: vec![],
        }
    }
    fn keep() -> Self {
        Self {
            kind: TransitionKind::Keep,
            ..Self::keep_data()
        }
    }
    fn next() -> Self {
        Self {
            kind: TransitionKind::Next,
            ..Self::keep_data()
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiation {
    pub epoch: Unsigned,
    pub height: Unsigned,
    pub hash: Vec<u8>,
    pub leader: AccountPublicKey,
    // Preserve the list as well as the remaining-voter map. Duplicate keys
    // contribute repeatedly to total stake but last-wins to remaining stake.
    pub validators: Vec<(AccountPublicKey, Unsigned)>,
    pub seed: Option<Vec<u8>>,
    pub current_length: Unsigned,
}
#[derive(Debug, Clone)]
pub enum Event<V = ()> {
    Negotiate(Negotiation),
    Transaction {
        received: bool,
        signed: Box<SignedTransaction<Transaction>>,
    },
    Timeout(Timeout),
    ParentBlock {
        epoch: Unsigned,
        block: ParentBlock,
    },
    NewEpoch,
    GetFinalize {
        request: u64,
        view: V,
        nonce_offset: Unsigned,
    },
    Ignore,
}

// A host clone MUST isolate its publication outbox and selected-state effects.
// Signing can use shared immutable key handles. Publish must enqueue locally;
// resource/storage failures discard staged effects and can retry. Reference
// native exceptions retain effects emitted before the exception, while the
// failed callback does not commit its next FSM state.
pub trait Host {
    type Error;
    type View;
    type Finalized;
    // False identifies a modeled reference native exception, not a local fault.
    fn is_operational(error: &Self::Error) -> bool;
    fn commit(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn create_vote(
        &mut self,
        voter: AccountPublicKey,
        epoch: &Unsigned,
        kind: &Unsigned,
        fields: &Payload,
        limits: Limits,
    ) -> Result<SignedTransaction<Transaction>, Self::Error>;
    // false is an ordinary pool rejection. Errors are operational/native stops.
    fn publish(&mut self, vote: SignedTransaction<Transaction>) -> Result<bool, Self::Error>;
    // None selects the current top, matching completion of a pending call.
    // A supplied view is used only for an immediately answered GetFinalize.
    fn finalize(
        &mut self,
        epoch: &Unsigned,
        leader: AccountPublicKey,
        calldata: &[u8],
        nonce_offset: &Unsigned,
        view: Option<&Self::View>,
    ) -> Result<Self::Finalized, Self::Error>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepError<E> {
    Core(Failure),
    Host(E),
}
impl<E> From<Failure> for StepError<E> {
    fn from(e: Failure) -> Self {
        Self::Core(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultState {
    Calldata(Vec<u8>),
    NoConsensus,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Machine {
    pub(super) state: State,
    pub(super) client: ClientState,
    pub(super) epoch: Option<Unsigned>,
    pub(super) height: Option<Unsigned>,
    pub(super) seed: Option<Vec<u8>>,
    pub(super) leader: Option<AccountPublicKey>,
    pub(super) validators: Vec<(AccountPublicKey, Unsigned)>,
    pub(super) remaining: BTreeMap<AccountPublicKey, Unsigned>,
    pub(super) stakers: BTreeSet<AccountPublicKey>,
    pub(super) majority: BigInt,
    pub(super) proposal: Option<Payload>,
    pub(super) block_time: Unsigned,
    pub(super) result: Option<ResultState>,
    pub(super) pending_request: Option<u64>,
    pub(super) nonce_offset: Unsigned,
    pub(super) votes: BTreeMap<AccountPublicKey, Payload>,
    pub(super) parents: BTreeMap<Unsigned, ParentBlock>,
    pub(super) buffered: Vec<SignedTransaction<Transaction>>,
}
impl Machine {
    pub fn new(mode: Mode, stakers: BTreeSet<AccountPublicKey>, block_time: Unsigned) -> Self {
        Self {
            state: State::AwaitEpoch,
            client: ClientState::empty(mode),
            epoch: None,
            height: None,
            seed: None,
            leader: None,
            validators: vec![],
            remaining: BTreeMap::new(),
            stakers,
            majority: 0.into(),
            proposal: None,
            block_time,
            result: None,
            pending_request: None,
            nonce_offset: 1.into(),
            votes: BTreeMap::new(),
            parents: BTreeMap::new(),
            buffered: vec![],
        }
    }
    pub fn state(&self) -> State {
        self.state
    }
    pub fn client(&self) -> &ClientState {
        &self.client
    }
    pub fn epoch(&self) -> Option<&Unsigned> {
        self.epoch.as_ref()
    }
    pub fn height(&self) -> Option<&Unsigned> {
        self.height.as_ref()
    }
    pub fn seed(&self) -> Option<&[u8]> {
        self.seed.as_deref()
    }
    pub fn leader(&self) -> Option<AccountPublicKey> {
        self.leader
    }
    pub fn validators(&self) -> &[(AccountPublicKey, Unsigned)] {
        &self.validators
    }
    pub fn remaining(&self) -> &BTreeMap<AccountPublicKey, Unsigned> {
        &self.remaining
    }
    pub fn majority(&self) -> &BigInt {
        &self.majority
    }
    pub fn proposal(&self) -> Option<&Payload> {
        self.proposal.as_ref()
    }
    pub fn result(&self) -> Option<&ResultState> {
        self.result.as_ref()
    }
    pub fn pending_request(&self) -> Option<u64> {
        self.pending_request
    }
    pub fn nonce_offset(&self) -> &Unsigned {
        &self.nonce_offset
    }
    pub fn votes(&self) -> &BTreeMap<AccountPublicKey, Payload> {
        &self.votes
    }
    pub fn parents(&self) -> &BTreeMap<Unsigned, ParentBlock> {
        &self.parents
    }
    pub fn buffered(&self) -> &[SignedTransaction<Transaction>] {
        &self.buffered
    }
    pub fn step<H: Host + Clone>(
        &mut self,
        event: Event<H::View>,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        self.check_limits(limits)?;
        Self::check_event(&event, limits)?;
        let mut staged = host.clone();
        let mut machine = self.clone();
        let result = machine.apply_staged(event, &mut staged, order, limits);
        let retain = match &result {
            Ok(_) => true,
            Err(StepError::Core(Failure::Native(_))) => true,
            Err(StepError::Host(e)) => !H::is_operational(e),
            _ => false,
        };
        if retain {
            staged.commit().map_err(StepError::Host)?;
            *self = machine;
            *host = staged;
        }
        result
    }
    // The coordinator already owns an isolated host and commits/discards the
    // complete callback, including its downstream queue/publication effects.
    pub(super) fn step_staged<H: Host>(
        &mut self,
        event: Event<H::View>,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        self.check_limits(limits)?;
        Self::check_event(&event, limits)?;
        self.apply_staged(event, host, order, limits)
    }
    fn apply_staged<H: Host>(
        &mut self,
        event: Event<H::View>,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        let mut next = self.clone();
        let transition = next.handle(event, host, order, limits)?;
        next.check_limits(limits)?;
        *self = next;
        Ok(transition)
    }
    fn check_limits(&self, limits: Limits) -> Result<(), Failure> {
        if self.validators.len() > limits.maximum_validators
            || self.stakers.len() > limits.maximum_validators
            || self.parents.len() > limits.maximum_parent_blocks
            || self.buffered.len() > limits.maximum_buffered_votes
        {
            return Err(Failure::ResourceLimit);
        }
        let mut budget = Budget::new(limits);
        for n in self
            .epoch
            .iter()
            .chain(self.height.iter())
            .chain([&self.block_time, &self.nonce_offset])
        {
            budget.add(n.as_bytes().len())?;
        }
        budget.integer(&self.majority)?;
        for (_, n) in &self.validators {
            budget.add(32)?;
            budget.add(n.as_bytes().len())?;
        }
        for n in self.remaining.values() {
            budget.add(32)?;
            budget.add(n.as_bytes().len())?;
        }
        budget.add(self.stakers.len().saturating_mul(32))?;
        match &self.client {
            ClientState::Fork { hash } => {
                if let Some(hash) = hash {
                    budget.add(hash.len())?;
                }
            }
            ClientState::Length { length, delta } => {
                for n in length.iter().chain(delta.iter()) {
                    budget.integer(n)?;
                }
            }
        }
        for signed in &self.buffered {
            budget.signed(signed)?;
        }
        for fields in self.votes.values().chain(self.proposal.iter()) {
            budget.payload(fields)?;
        }
        budget.add(self.votes.len().saturating_mul(32))?;
        for (epoch, parent) in &self.parents {
            budget.add(epoch.as_bytes().len())?;
            budget.parent(parent)?;
        }
        if let Some(seed) = &self.seed {
            budget.add(seed.len())?;
        }
        if let Some(ResultState::Calldata(b)) = &self.result {
            budget.add(b.len())?;
        }
        Ok(())
    }
    fn check_event<V>(event: &Event<V>, limits: Limits) -> Result<(), Failure> {
        let mut budget = Budget::new(limits);
        match event {
            Event::Negotiate(n) => {
                if n.validators.len() > limits.maximum_validators {
                    return Err(Failure::ResourceLimit);
                }
                for number in [&n.epoch, &n.height, &n.current_length] {
                    budget.add(number.as_bytes().len())?;
                }
                budget.add(n.hash.len())?;
                budget.add(n.seed.as_ref().map_or(0, Vec::len))?;
                for (_, stake) in &n.validators {
                    budget.add(32)?;
                    budget.add(stake.as_bytes().len())?;
                }
            }
            Event::Transaction { signed, .. } => budget.signed(signed)?,
            Event::ParentBlock { epoch, block } => {
                budget.add(epoch.as_bytes().len())?;
                budget.parent(block)?;
            }
            Event::GetFinalize { nonce_offset, .. } => budget.add(nonce_offset.as_bytes().len())?,
            _ => {}
        }
        Ok(())
    }
    fn handle<H: Host>(
        &mut self,
        event: Event<H::View>,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        match event {
            Event::Negotiate(n) if self.state == State::AwaitEpoch => {
                self.negotiate(n, host, limits)
            }
            Event::Transaction { received, signed } => match self.state {
                State::AwaitEpoch if received => self.handle_proposal(&signed, host, order, limits),
                State::Proposal => self.handle_proposal(&signed, host, order, limits),
                State::Vote => self.handle_vote(&signed, VoteType::Vote, host, order, limits),
                State::Finalize => self.handle_vote(&signed, VoteType::Commit, host, order, limits),
                _ => Ok(Transition::keep_data()),
            },
            Event::Timeout(Timeout::NoProposal) if self.state == State::Proposal => {
                Ok(self.no_consensus())
            }
            Event::Timeout(Timeout::NoQuorum)
                if matches!(self.state, State::Vote | State::Finalize) =>
            {
                Ok(self.no_consensus())
            }
            Event::ParentBlock { epoch, block } => {
                self.parents.insert(epoch, block);
                Ok(Transition::keep())
            }
            Event::NewEpoch => {
                let mut parents = std::mem::take(&mut self.parents);
                if let Some(epoch) = &self.epoch {
                    let boundary = big(epoch) - 4;
                    parents.retain(|e, _| big(e) > boundary);
                }
                let mut reset = Self::new(
                    self.client.mode(),
                    self.stakers.clone(),
                    self.block_time.clone(),
                );
                reset.parents = parents;
                *self = reset;
                Ok(Transition::next())
            }
            Event::GetFinalize {
                request,
                view,
                nonce_offset,
            } => {
                if self.leader.is_none() {
                    let mut t = Transition::keep_data();
                    t.replies.push((request, Reply::NotReady));
                    return Ok(t);
                }
                if self.result.is_none() {
                    self.pending_request = Some(request);
                    self.nonce_offset = nonce_offset;
                    return Ok(Transition::keep());
                }
                let reply = self.finalize_reply(host, &nonce_offset, Some(&view))?;
                let mut t = Transition::keep();
                t.replies.push((request, reply));
                Ok(t)
            }
            _ => Ok(Transition::keep_data()),
        }
    }
    fn negotiate<H: Host>(
        &mut self,
        n: Negotiation,
        host: &mut H,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        if n.validators.len() > limits.maximum_validators
            || n.hash.len() > limits.maximum_bytes
            || n.seed
                .as_ref()
                .is_some_and(|b| b.len() > limits.maximum_bytes)
        {
            return Err(Failure::ResourceLimit.into());
        }
        self.client.initialize(
            &n.epoch,
            &n.hash,
            &self.parents,
            &n.current_length,
            &self.block_time,
        )?;
        self.epoch = Some(n.epoch);
        self.height = Some(n.height);
        self.seed = n.seed;
        self.leader = Some(n.leader);
        self.validators = n.validators;
        self.majority = majority(&self.validators);
        self.remaining = self.validators.iter().cloned().collect();
        if !self.stakers.contains(&n.leader) {
            let cached = self.proposal.as_ref().is_some_and(|p| {
                p.get(LEADER) == Some(&Field::Binary(n.leader.0.to_vec()))
                    && p.get(EPOCH) == Some(&Field::Integer(big(self.epoch.as_ref().unwrap())))
            });
            if cached {
                self.handle_voting(host, limits)?;
                self.state = State::Vote;
                return Ok(self.timer(Timeout::NoQuorum));
            }
            self.proposal = None;
            self.state = State::Proposal;
            return Ok(self.timer(Timeout::NoProposal));
        }
        let fields = self.client.proposal(self.height.as_ref().unwrap())?;
        self.proposal = Some(fields.clone());
        let tx = host
            .create_vote(
                n.leader,
                self.epoch.as_ref().unwrap(),
                &self.client.mode().type_number(VoteType::Proposal),
                &fields,
                limits,
            )
            .map_err(StepError::Host)?;
        if !host.publish(tx).map_err(StepError::Host)? {
            return Ok(self.no_consensus());
        }
        self.state = State::Proposal;
        Ok(self.timer(Timeout::NoProposal))
    }
    fn conversion(
        &self,
        signed: &SignedTransaction<Transaction>,
        limits: Limits,
    ) -> Result<Option<Converted>, Failure> {
        match payload::convert_transaction(signed, self.client.mode(), limits) {
            Ok(v) => Ok(Some(v)),
            Err(ConversionError::Failure(e)) => Err(e),
            Err(_) => Ok(None),
        }
    }
    fn handle_proposal<H: Host>(
        &mut self,
        signed: &SignedTransaction<Transaction>,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        let Some(mut vote) = self.conversion(signed, limits)? else {
            return Ok(Transition::keep_data());
        };
        if vote.kind != VoteType::Proposal {
            return Ok(Transition::keep_data());
        }
        if self.leader.is_none() && self.epoch.is_none() {
            vote.fields
                .insert(LEADER.to_vec(), Field::Binary(vote.voter.0.to_vec()));
            vote.fields
                .insert(EPOCH.to_vec(), Field::Integer(big(&vote.epoch)));
            self.proposal = Some(vote.fields);
            return Ok(Transition::keep());
        }
        if self.leader != Some(vote.voter) || self.epoch.as_ref() != Some(&vote.epoch) {
            return Ok(Transition::keep_data());
        }
        self.proposal = Some(vote.fields);
        self.handle_voting(host, limits)?;
        self.check_majority(host, order, limits)
    }
    fn handle_vote<H: Host>(
        &mut self,
        signed: &SignedTransaction<Transaction>,
        expected: VoteType,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        let Some(vote) = self.conversion(signed, limits)? else {
            return Ok(Transition::keep_data());
        };
        if vote.kind != expected {
            if expected == VoteType::Vote && vote.kind == VoteType::Commit {
                if self.buffered.len() >= limits.maximum_buffered_votes {
                    return Err(Failure::ResourceLimit.into());
                }
                self.buffered.push(signed.clone());
                return Ok(Transition::keep());
            }
            return Ok(Transition::keep_data());
        }
        // Original handle_vote deliberately ignores epoch and payload agreement.
        let Some(stake) = self.remaining.remove(&vote.voter) else {
            return Ok(Transition::keep_data());
        };
        self.majority -= big(&stake);
        if expected == VoteType::Vote {
            self.votes.insert(vote.voter, vote.fields);
            self.check_majority(host, order, limits)
        } else {
            self.commit(host)
        }
    }
    fn leader_stake(&self) -> BigInt {
        self.validators
            .iter()
            .find(|(k, _)| Some(*k) == self.leader)
            .map_or(0.into(), |(_, n)| big(n))
    }
    fn handle_voting<H: Host>(
        &mut self,
        host: &mut H,
        limits: Limits,
    ) -> Result<(), StepError<H::Error>> {
        self.send_votes(VoteType::Vote, host, limits)?;
        self.majority -= self.leader_stake();
        Ok(())
    }
    fn send_votes<H: Host>(
        &self,
        kind: VoteType,
        host: &mut H,
        limits: Limits,
    ) -> Result<(), StepError<H::Error>> {
        let Some(fields) = self.client.vote(
            self.proposal
                .as_ref()
                .ok_or(Failure::Native(Native::FunctionClause))?,
            self.height
                .as_ref()
                .ok_or(Failure::Native(Native::FunctionClause))?,
        )?
        else {
            return Ok(());
        };
        let epoch = self
            .epoch
            .as_ref()
            .ok_or(Failure::Native(Native::FunctionClause))?;
        let mut created = Vec::new();
        let mut budget = Budget::new(limits);
        for (voter, _) in &self.validators {
            if Some(*voter) != self.leader && self.stakers.contains(voter) {
                let tx = host
                    .create_vote(
                        *voter,
                        epoch,
                        &self.client.mode().type_number(kind),
                        &fields,
                        limits,
                    )
                    .map_err(StepError::Host)?;
                budget.signed(&tx)?;
                created.push(tx);
            }
        }
        for tx in created.into_iter().rev() {
            let _accepted = host.publish(tx).map_err(StepError::Host)?;
        }
        Ok(())
    }
    fn check_majority<H: Host>(
        &mut self,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        if self.majority > 0.into() {
            self.state = State::Vote;
            return Ok(self.timer(Timeout::NoQuorum));
        }
        let epoch = self
            .epoch
            .as_ref()
            .ok_or(Failure::Native(Native::FunctionClause))?;
        let seed = self
            .seed
            .as_deref()
            .or_else(|| self.parents.get(epoch).map(|b| b.hash.as_slice()))
            .unwrap_or(&[0]);
        let calldata = finalize_calldata(
            FinalizeInput {
                client: &self.client,
                epoch,
                leader: self.leader.ok_or(Failure::Native(Native::FunctionClause))?,
                seed,
                proposal: self
                    .proposal
                    .as_ref()
                    .ok_or(Failure::Native(Native::FunctionClause))?,
                votes: &self.votes,
            },
            order,
            limits,
        )?;
        self.result = Some(ResultState::Calldata(calldata));
        self.send_votes(VoteType::Commit, host, limits)?;
        self.majority = majority(&self.validators) - self.leader_stake();
        self.remaining = self.validators.iter().cloned().collect();
        self.state = State::Finalize;
        let next = self.timer(Timeout::NoQuorum);
        while let Some(tx) = self.buffered.pop() {
            // Buffered entries are commits, and processing cannot recurse into
            // check_majority. This retains LIFO order without stack growth.
            let Some(vote) = self.conversion(&tx, limits)? else {
                continue;
            };
            if vote.kind != VoteType::Commit {
                continue;
            }
            if let Some(stake) = self.remaining.remove(&vote.voter) {
                self.majority -= big(&stake);
                let t = self.commit(host)?;
                if t.kind == TransitionKind::Next {
                    return Ok(t);
                }
            }
        }
        Ok(next)
    }
    fn commit<H: Host>(
        &mut self,
        host: &mut H,
    ) -> Result<Transition<H::Finalized>, StepError<H::Error>> {
        if self.majority > 0.into() {
            let mut t = Transition::keep();
            t.timeout = Some((self.block_time.clone(), Timeout::NoQuorum));
            return Ok(t);
        }
        self.state = State::Complete;
        let mut t = Transition::next();
        if let Some(id) = self.pending_request {
            t.replies
                .push((id, self.finalize_reply(host, &self.nonce_offset, None)?));
        }
        Ok(t)
    }
    fn finalize_reply<H: Host>(
        &self,
        host: &mut H,
        nonce: &Unsigned,
        view: Option<&H::View>,
    ) -> Result<Reply<H::Finalized>, StepError<H::Error>> {
        match &self.result {
            Some(ResultState::NoConsensus) => Ok(Reply::NoConsensus),
            Some(ResultState::Calldata(data)) => Ok(Reply::Finalized(
                host.finalize(
                    self.epoch
                        .as_ref()
                        .ok_or(Failure::Native(Native::FunctionClause))?,
                    self.leader.ok_or(Failure::Native(Native::FunctionClause))?,
                    data,
                    nonce,
                    view,
                )
                .map_err(StepError::Host)?,
            )),
            None => Err(Failure::Native(Native::FunctionClause).into()),
        }
    }
    fn no_consensus<R>(&mut self) -> Transition<R> {
        self.state = State::Complete;
        self.result = Some(ResultState::NoConsensus);
        let mut t = Transition::next();
        if let Some(id) = self.pending_request {
            t.replies.push((id, Reply::NoConsensus));
        }
        t
    }
    fn timer<R>(&self, timeout: Timeout) -> Transition<R> {
        let mut t = Transition::next();
        t.timeout = Some((self.block_time.clone(), timeout));
        t
    }
}
pub fn majority(validators: &[(AccountPublicKey, Unsigned)]) -> BigInt {
    let total: BigInt = validators.iter().map(|(_, stake)| big(stake)).sum();
    (total * 2 + 2) / 3
}
