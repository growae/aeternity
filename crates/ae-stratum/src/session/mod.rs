use crate::{
    integer,
    job::{Job, Jobs, Limits, Timing, Work},
    nonce::{Kind, Part},
    proof::Proof,
    share::{self, Share, User},
    target::{self, Difference},
    wire::rpc,
};
use num_bigint::BigInt;
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Connected,
    Configured,
    Subscribed,
    Authorized,
    Disconnected,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Targets {
    pub initial: BigInt,
    pub maximum: BigInt,
    pub difference_threshold: f64,
    pub desired_solve_time: BigInt,
    pub maximum_solve_time: BigInt,
}
impl Targets {
    fn validate(&self) -> Result<(), Error> {
        for i in [
            &self.initial,
            &self.maximum,
            &self.desired_solve_time,
            &self.maximum_solve_time,
        ] {
            integer(i)?;
        }
        if !self.difference_threshold.is_finite() {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Settings {
    pub initial: Option<BigInt>,
    pub maximum: Option<BigInt>,
    pub difference_threshold: Option<f64>,
    pub desired_solve_time: Option<BigInt>,
    pub maximum_solve_time: Option<BigInt>,
}
impl From<Targets> for Settings {
    fn from(t: Targets) -> Self {
        Self {
            initial: Some(t.initial),
            maximum: Some(t.maximum),
            difference_threshold: Some(t.difference_threshold),
            desired_solve_time: Some(t.desired_solve_time),
            maximum_solve_time: Some(t.maximum_solve_time),
        }
    }
}
impl Settings {
    fn complete(&self) -> Result<Targets, Error> {
        Ok(Targets {
            initial: self.initial.clone().ok_or(Error::InvalidEvent)?,
            maximum: self.maximum.clone().ok_or(Error::InvalidEvent)?,
            difference_threshold: self.difference_threshold.ok_or(Error::InvalidEvent)?,
            desired_solve_time: self.desired_solve_time.clone().ok_or(Error::InvalidEvent)?,
            maximum_solve_time: self.maximum_solve_time.clone().ok_or(Error::InvalidEvent)?,
        })
    }
}
// Explicit configuration and time; this module reads neither environment nor clock.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub port: u16,
    pub extra_nonce_bytes: u8,
    pub message_timeout_ms: u64,
    pub skip_blocks: u64,
    pub edge_bits: u8,
    pub max_jobs: usize,
    pub targets: Targets,
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        Part::maximum(self.extra_nonce_bytes)?;
        if !(1..=63).contains(&self.edge_bits)
            || !(1..=crate::queue::MAX_CAPACITY).contains(&self.max_jobs)
        {
            return Err(Error::Configuration);
        }
        self.targets.validate()
    }
}
pub struct Context<'a> {
    pub config: &'a Config,
    pub now: &'a BigInt,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timer {
    pub id: u64,
    pub phase: Phase,
    pub active: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfo {
    pub id: String,
    pub work: Work,
    pub share_target: BigInt,
    pub desired: BigInt,
    pub maximum: BigInt,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chain {
    Block(Work),
    SetTarget,
    Notify(JobInfo),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    Both,
    AccountOnly,
    Neither,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    Added,
    AlreadyPresent,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRegistration {
    Added,
    AlreadyPresent,
    Exhausted,
    AccountNotFound,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    AllocateNonce { bytes: u8 },
    Membership { account: String },
    Register { user: User },
    AddWorker { user: User },
    ReleaseNonce { nonce: Part },
    DeleteConnection,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub id: u64,
    pub operation: Operation,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Nonce(Option<Part>),
    Membership(Membership),
    Registered(Registration),
    Worker(WorkerRegistration),
    Released,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Init,
    Data(Vec<u8>),
    Timeout,
    Close,
    Chain(Chain),
    Complete { id: u64, reply: Reply },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    Closed,
    Timeout,
    InvalidEvent,
    Work(crate::Error),
    Wire(rpc::Error),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    ScheduleTimer { timer: Timer, after_ms: u64 },
    CancelTimer { id: u64 },
    Enqueue(Chain),
    Pool(share::Effect),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    None,
    Send(Vec<u8>),
    Command(Command),
    Stop(StopReason),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub action: Action,
    pub effects: Vec<Effect>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Busy,
    UnexpectedCompletion,
    InvalidEvent,
    Configuration,
    SequenceExhausted,
    Work(crate::Error),
    Wire(rpc::Error),
}
impl From<crate::Error> for Error {
    fn from(e: crate::Error) -> Self {
        Self::Work(e)
    }
}
impl From<rpc::Error> for Error {
    fn from(e: rpc::Error) -> Self {
        Self::Wire(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pending {
    Subscribe {
        id: Value,
        bytes: u8,
    },
    Authorize {
        id: Value,
        user: User,
    },
    Register {
        id: Value,
        user: User,
    },
    Worker {
        id: Value,
    },
    Submit {
        id: BigInt,
        job_id: String,
        share: Share,
    },
    Release {
        reason: StopReason,
    },
    Delete {
        reason: StopReason,
    },
}
#[derive(Debug, Clone, PartialEq)]
pub struct Session {
    phase: Phase,
    timer: Option<Timer>,
    account: Option<String>,
    extra_nonce: Option<Part>,
    accept_blocks: bool,
    block_count: u64,
    targets: Settings,
    jobs: Option<Jobs>,
    pending: Option<(u64, Pending)>,
    sequence: u64,
}
impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
impl Session {
    pub fn new() -> Self {
        Self {
            phase: Phase::Connected,
            timer: None,
            account: None,
            extra_nonce: None,
            accept_blocks: false,
            block_count: 0,
            targets: Settings::default(),
            jobs: None,
            pending: None,
            sequence: 0,
        }
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }
    pub fn timer(&self) -> Option<Timer> {
        self.timer
    }
    pub fn account(&self) -> Option<&str> {
        self.account.as_deref()
    }
    pub fn extra_nonce(&self) -> Option<Part> {
        self.extra_nonce
    }
    pub fn accepts_blocks(&self) -> bool {
        self.accept_blocks
    }
    pub fn block_count(&self) -> u64 {
        self.block_count
    }
    pub fn targets(&self) -> &Settings {
        &self.targets
    }
    pub fn jobs(&self) -> Option<&Jobs> {
        self.jobs.as_ref()
    }
    pub fn pending_id(&self) -> Option<u64> {
        self.pending.as_ref().map(|(id, _)| *id)
    }
    // These correspond to the original handler's four administrative setters.
    // Updating initial target is intentionally not part of that interface.
    pub fn set(&mut self, update: Update) -> Result<(), Error> {
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let t = &mut self.targets;
        match update {
            Update::MaximumTarget(n) => {
                integer(&n)?;
                t.maximum = Some(n);
            }
            Update::DifferenceThreshold(n) => {
                if !n.is_finite() {
                    return Err(Error::Configuration);
                }
                t.difference_threshold = Some(n);
            }
            Update::DesiredSolveTime(n) => {
                integer(&n)?;
                t.desired_solve_time = Some(n);
            }
            Update::MaximumSolveTime(n) => {
                integer(&n)?;
                t.maximum_solve_time = Some(n);
            }
        }
        Ok(())
    }
    // A command suspends this connection just like the original synchronous
    // registry/nonce call. The owner must finish its exact command before
    // delivering another event, and consume a Send before queued self-events.
    pub fn handle(&mut self, event: Event, context: &Context<'_>) -> Result<Step, Error> {
        context.config.validate()?;
        integer(context.now)?;
        if self.pending.is_some() && !matches!(event, Event::Complete { .. }) {
            return Err(Error::Busy);
        }
        let mut next = self.clone();
        let mut effects = Vec::new();
        let action = match next.apply(event, context, &mut effects) {
            Ok(a) => a,
            Err(Error::Work(e)) => {
                // The native handler terminates using its last published
                // session, not a partially evaluated event-local record.
                next = self.clone();
                next.pending = None;
                next.close(StopReason::Work(e), &mut effects)?
            }
            Err(Error::Wire(e)) => {
                next = self.clone();
                next.pending = None;
                next.close(StopReason::Wire(e), &mut effects)?
            }
            Err(Error::InvalidEvent) => {
                next = self.clone();
                next.pending = None;
                next.close(StopReason::InvalidEvent, &mut effects)?
            }
            Err(e) => return Err(e),
        };
        *self = next;
        Ok(Step { action, effects })
    }
    fn token(&mut self) -> Result<u64, Error> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(Error::SequenceExhausted)?;
        Ok(self.sequence)
    }
    fn command(&mut self, operation: Operation, pending: Pending) -> Result<Action, Error> {
        let id = self.token()?;
        self.pending = Some((id, pending));
        Ok(Action::Command(Command { id, operation }))
    }
    fn cancel_timer(&mut self, effects: &mut Vec<Effect>) {
        if let Some(t) = self.timer.as_mut() {
            effects.push(Effect::CancelTimer { id: t.id });
            t.active = false;
        }
    }
    fn schedule_timer(
        &mut self,
        phase: Phase,
        ctx: &Context<'_>,
        effects: &mut Vec<Effect>,
    ) -> Result<(), Error> {
        let timer = Timer {
            id: self.token()?,
            phase,
            active: true,
        };
        self.timer = Some(timer);
        effects.push(Effect::ScheduleTimer {
            timer,
            after_ms: ctx.config.message_timeout_ms,
        });
        Ok(())
    }
    fn send(message: Value) -> Result<Action, Error> {
        Ok(Action::Send(rpc::encode(
            &message,
            &rpc::Options::default(),
        )?))
    }
    fn response(id: Value, method: &str, result: Value) -> Result<Action, Error> {
        Self::send(json!({"type":"rsp","method":method,"id":id,"result":result}))
    }
    fn reject(id: Value, reason: &str, data: Option<&str>) -> Result<Action, Error> {
        Self::send(json!({"type":"rsp","id":id,"reason":reason,"data":data}))
    }
    fn apply(
        &mut self,
        event: Event,
        ctx: &Context<'_>,
        effects: &mut Vec<Effect>,
    ) -> Result<Action, Error> {
        match event {
            Event::Init if self.phase == Phase::Connected => {
                self.schedule_timer(Phase::Connected, ctx, effects)?;
                Ok(Action::None)
            }
            Event::Init => Err(Error::InvalidEvent),
            Event::Data(bytes) => match rpc::decode(&bytes, &rpc::Options::default()) {
                Ok(m) => self.message(m, ctx, effects),
                Err(e) => Self::send(rpc::error_response(&e, &rpc::Options::default())?),
            },
            Event::Timeout => {
                if matches!(
                    self.phase,
                    Phase::Connected | Phase::Configured | Phase::Subscribed
                ) && self.timer.is_some_and(|t| t.phase == self.phase)
                {
                    self.close(StopReason::Timeout, effects)
                } else {
                    Ok(Action::None)
                }
            }
            Event::Close => self.close(StopReason::Closed, effects),
            Event::Chain(event) => self.chain(event, ctx, effects),
            Event::Complete { id, reply } => self.complete(id, reply, ctx, effects),
        }
    }
    fn message(
        &mut self,
        m: Value,
        ctx: &Context<'_>,
        effects: &mut Vec<Effect>,
    ) -> Result<Action, Error> {
        let id = m.get("id").cloned().unwrap_or(Value::Null);
        let method = m["method"].as_str().unwrap_or("");
        if m["type"] == "req" {
            match (method, self.phase) {
                ("configure", Phase::Connected) => {
                    self.cancel_timer(effects);
                    self.phase = Phase::Configured;
                    self.schedule_timer(Phase::Configured, ctx, effects)?;
                    return Self::response(id, "configure", json!([]));
                }
                ("subscribe", Phase::Connected | Phase::Configured) => {
                    if m["port"].as_u64() != Some(ctx.config.port.into()) {
                        return Self::reject(id, "unknown_error", Some("port_mismatch"));
                    }
                    self.cancel_timer(effects);
                    let bytes = ctx.config.extra_nonce_bytes;
                    return self.command(
                        Operation::AllocateNonce { bytes },
                        Pending::Subscribe { id, bytes },
                    );
                }
                ("authorize", Phase::Subscribed | Phase::Authorized) => {
                    let user = Self::user(&m)?;
                    return self.command(
                        Operation::Membership {
                            account: user.account.clone(),
                        },
                        Pending::Authorize { id, user },
                    );
                }
                ("submit", Phase::Authorized) => {
                    if self.targets.initial.is_none() {
                        return Self::reject(id, "unknown_error", Some("target_not_set"));
                    }
                    let text = m["miner_nonce"].as_str().ok_or(Error::InvalidEvent)?;
                    let bytes = u8::try_from(text.len() / 2).map_err(|_| Error::Configuration)?;
                    let share = Share {
                        user: Self::user(&m)?,
                        miner_nonce: Part::from_hex(Kind::Miner, text, bytes)?,
                        proof: Proof::from_json(&m["pow"])?,
                        validity: None,
                        created: ctx.now.clone(),
                    };
                    let job_id = m["job_id"].as_str().ok_or(Error::InvalidEvent)?.to_owned();
                    let id =
                        rpc::to_id(&id, &rpc::Options::default()).ok_or(Error::InvalidEvent)?;
                    if self.jobs.as_ref().and_then(|j| j.find(&job_id)).is_none() {
                        return self.submit(
                            id,
                            job_id,
                            share,
                            false,
                            ctx.config.edge_bits,
                            effects,
                        );
                    }
                    return self.command(
                        Operation::Membership {
                            account: share.user.account.clone(),
                        },
                        Pending::Submit { id, job_id, share },
                    );
                }
                ("submit", Phase::Subscribed) => {
                    return Self::reject(id, "unauthorized_worker", None);
                }
                ("authorize" | "submit", Phase::Connected | Phase::Configured) => {
                    return Self::reject(id, "not_subscribed", None);
                }
                _ => {}
            }
        }
        Self::reject(id, "unknown_error", Some("unexpected_msg"))
    }
    fn user(m: &Value) -> Result<User, Error> {
        Ok(User {
            account: m["user"][0].as_str().ok_or(Error::InvalidEvent)?.into(),
            worker: m["user"][1].as_str().ok_or(Error::InvalidEvent)?.into(),
        })
    }
    fn complete(
        &mut self,
        id: u64,
        reply: Reply,
        ctx: &Context<'_>,
        effects: &mut Vec<Effect>,
    ) -> Result<Action, Error> {
        let (expected, pending) = self.pending.take().ok_or(Error::UnexpectedCompletion)?;
        if id != expected {
            return Err(Error::UnexpectedCompletion);
        }
        match (pending, reply) {
            (Pending::Subscribe { id, bytes }, Reply::Nonce(nonce)) => {
                if let Some(n) = nonce {
                    if n.kind() != Kind::Extra || n.bytes() != bytes {
                        return Err(Error::UnexpectedCompletion);
                    }
                    self.extra_nonce = Some(n);
                    self.phase = Phase::Subscribed;
                    self.schedule_timer(Phase::Subscribed, ctx, effects)?;
                    Self::response(id, "subscribe", json!([null, n.to_hex()]))
                } else {
                    Self::reject(id, "unknown_error", Some("extra_nonce_not_found"))
                }
            }
            (Pending::Authorize { id, user }, Reply::Membership(m)) => match (self.phase, m) {
                (Phase::Subscribed, Membership::Neither) => {
                    self.cancel_timer(effects);
                    self.command(
                        Operation::Register { user: user.clone() },
                        Pending::Register { id, user },
                    )
                }
                (Phase::Authorized, Membership::Both) => {
                    self.command(Operation::AddWorker { user }, Pending::Worker { id })
                }
                _ => Self::response(id, "authorize", json!(false)),
            },
            // The original ignores the first registry add result, including
            // already_present from an interleaving registry mutation.
            (Pending::Register { id, user }, Reply::Registered(_)) => {
                self.phase = Phase::Authorized;
                self.timer = None;
                self.account = Some(user.account);
                self.jobs = Some(Jobs::new(ctx.config.max_jobs, 8192)?);
                effects.push(Effect::Enqueue(Chain::SetTarget));
                Self::response(id, "authorize", json!(true))
            }
            (Pending::Worker { id }, Reply::Worker(result)) => {
                Self::response(id, "authorize", json!(result == WorkerRegistration::Added))
            }
            (Pending::Submit { id, job_id, share }, Reply::Membership(m)) => self.submit(
                id,
                job_id,
                share,
                m == Membership::Both,
                ctx.config.edge_bits,
                effects,
            ),
            (Pending::Release { reason }, Reply::Released) => {
                self.close_after_release(reason, effects)
            }
            (Pending::Delete { reason }, Reply::Deleted) => Ok(self.closed(reason)),
            _ => Err(Error::UnexpectedCompletion),
        }
    }
    fn submit(
        &mut self,
        id: BigInt,
        job_id: String,
        share: Share,
        authorized: bool,
        edge_bits: u8,
        effects: &mut Vec<Effect>,
    ) -> Result<Action, Error> {
        let targets = self.targets.complete()?;
        let ctx = share::Context {
            extra_nonce: self.extra_nonce.ok_or(Error::InvalidEvent)?,
            authorized,
            maximum_solve_time: &targets.maximum_solve_time,
            edge_bits,
        };
        let receipt = self
            .jobs
            .as_mut()
            .ok_or(Error::InvalidEvent)?
            .submit(&job_id, share, &ctx)?;
        effects.extend(receipt.effects.into_iter().map(Effect::Pool));
        Self::send(rpc::share_response(&id, receipt.response)?)
    }
    fn close(&mut self, reason: StopReason, effects: &mut Vec<Effect>) -> Result<Action, Error> {
        if self.phase == Phase::Disconnected {
            return Ok(Action::Stop(reason));
        }
        if let Some(nonce) = self.extra_nonce {
            self.command(
                Operation::ReleaseNonce { nonce },
                Pending::Release { reason },
            )
        } else {
            self.close_after_release(reason, effects)
        }
    }
    fn close_after_release(
        &mut self,
        reason: StopReason,
        effects: &mut Vec<Effect>,
    ) -> Result<Action, Error> {
        self.cancel_timer(effects);
        if self.phase == Phase::Authorized {
            self.command(Operation::DeleteConnection, Pending::Delete { reason })
        } else {
            Ok(self.closed(reason))
        }
    }
    fn closed(&mut self, reason: StopReason) -> Action {
        self.phase = Phase::Disconnected;
        self.extra_nonce = None;
        self.timer = None;
        Action::Stop(reason)
    }
    fn advance_count(&mut self, skip: u64) -> Result<(), Error> {
        self.block_count = if self.block_count == skip {
            0
        } else {
            self.block_count
                .checked_add(1)
                .ok_or(Error::SequenceExhausted)?
        };
        Ok(())
    }
    fn chain(
        &mut self,
        event: Chain,
        ctx: &Context<'_>,
        effects: &mut Vec<Effect>,
    ) -> Result<Action, Error> {
        match event {
            Chain::SetTarget => {
                self.targets = ctx.config.targets.clone().into();
                self.accept_blocks = true;
                Self::send(
                    json!({"type":"ntf","method":"set_target","target":target::to_hex(&ctx.config.targets.initial)?}),
                )
            }
            Chain::Notify(info) => {
                if self.accept_blocks {
                    return Err(Error::InvalidEvent);
                }
                self.add_job(&info, ctx.now)?;
                self.accept_blocks = true;
                Self::notify(&info)
            }
            Chain::Block(work) => {
                if self.phase != Phase::Authorized {
                    return Ok(Action::None);
                }
                let accept = self.accept_blocks && self.block_count == 0;
                if !accept {
                    self.advance_count(ctx.config.skip_blocks)?;
                    return Ok(Action::None);
                }
                let id = work.id()?;
                self.advance_count(ctx.config.skip_blocks)?;
                let jobs = self.jobs.as_ref().ok_or(Error::InvalidEvent)?;
                if jobs.find(&id).is_some() {
                    return Ok(Action::None);
                }
                let t = self.targets.complete()?;
                let old = jobs
                    .rear()
                    .map_or_else(|| t.initial.clone(), Job::share_target);
                let new = match jobs.next_target(&t.desired_solve_time, &t.maximum) {
                    Ok(n) => n.max(work.block_target.clone()),
                    Err(crate::Error::NotEnoughJobs) => t.initial.clone(),
                    Err(e) => return Err(e.into()),
                };
                let changed = match target::difference(&new, &old)? {
                    Difference::Unchanged => false,
                    Difference::Increase(n) | Difference::Decrease(n) => n > t.difference_threshold,
                };
                let info = JobInfo {
                    id,
                    work,
                    share_target: if changed { new.clone() } else { old },
                    desired: t.desired_solve_time.clone(),
                    maximum: t.maximum_solve_time.clone(),
                };
                if changed {
                    self.accept_blocks = false;
                    effects.push(Effect::Enqueue(Chain::Notify(info)));
                    Self::send(
                        json!({"type":"ntf","method":"set_target","target":target::to_hex(&new)?}),
                    )
                } else {
                    self.add_job(&info, ctx.now)?;
                    Self::notify(&info)
                }
            }
        }
    }
    fn add_job(&mut self, info: &JobInfo, now: &BigInt) -> Result<(), Error> {
        let job = Job::new(
            info.id.clone(),
            info.work.clone(),
            info.share_target.clone(),
            Timing {
                desired: info.desired.clone(),
                maximum: info.maximum.clone(),
                created: now.clone(),
            },
            Limits::default(),
        )?;
        self.jobs.as_mut().ok_or(Error::InvalidEvent)?.add(job)?;
        Ok(())
    }
    fn notify(info: &JobInfo) -> Result<Action, Error> {
        Self::send(
            json!({"type":"ntf","method":"notify","job_id":info.id,"block_hash":info.work.hash_text(),"block_version":info.work.version,"empty_queue":true}),
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    MaximumTarget(BigInt),
    DifferenceThreshold(f64),
    DesiredSolveTime(BigInt),
    MaximumSolveTime(BigInt),
}

#[cfg(test)]
mod tests;
