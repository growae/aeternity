use super::{Config, Error, Module, number::Number};
use std::collections::VecDeque;

#[derive(Debug)]
struct Current {
    count: usize,
    total: Number,
    min: Number,
    max: Number,
    extra: Vec<Number>,
}
impl Current {
    fn first(v: Number) -> Self {
        Self {
            count: 1,
            total: v.clone(),
            min: v.clone(),
            max: v,
            extra: vec![],
        }
    }
    fn bytes(&self) -> usize {
        128 + self.total.bytes()
            + self.min.bytes()
            + self.max.bytes()
            + self.extra.iter().map(Number::bytes).sum::<usize>()
    }
    fn bucket(self, mean: Number) -> Bucket {
        Bucket {
            count: self.count,
            mean,
            min: self.min,
            max: self.max,
            extra: self.extra,
        }
    }
}
#[derive(Debug)]
struct Bucket {
    count: usize,
    mean: Number,
    min: Number,
    max: Number,
    extra: Vec<Number>,
}
impl Bucket {
    fn bytes(&self) -> usize {
        128 + self.mean.bytes()
            + self.min.bytes()
            + self.max.bytes()
            + self.extra.iter().map(Number::bytes).sum::<usize>()
    }
    fn view(&self) -> Sample<'_> {
        Sample {
            count: self.count,
            value: self.mean.clone(),
            min: &self.min,
            max: &self.max,
            extra: &self.extra,
        }
    }
}
#[derive(Debug, Default)]
struct Buffer<T> {
    values: VecDeque<(i128, T)>,
    bytes: usize,
}
impl<T> Buffer<T> {
    fn empty() -> Self {
        Self {
            values: VecDeque::new(),
            bytes: 0,
        }
    }
    fn push(&mut self, time: i128, value: T, bytes: usize) {
        self.values.push_front((time, value));
        self.bytes += bytes;
    }
    fn since(&self, oldest: i128, count: usize) -> impl DoubleEndedIterator<Item = &(i128, T)> {
        let n = self
            .values
            .iter()
            .take(count)
            .take_while(|(t, _)| *t >= oldest)
            .count();
        self.values.range(..n)
    }
}
#[derive(Debug)]
pub(super) struct Slide {
    config: Config,
    plain: Option<Plain>,
    slot: Option<Slot>,
}
#[derive(Debug)]
struct Plain {
    last: i128,
    first: Buffer<Number>,
    second: Buffer<Number>,
}
#[derive(Debug)]
struct Slot {
    current_slot: i128,
    first_slot: i128,
    current: Option<Current>,
    first: Buffer<Bucket>,
    second: Buffer<Bucket>,
}
pub(super) struct Sample<'a> {
    pub count: usize,
    pub value: Number,
    pub min: &'a Number,
    pub max: &'a Number,
    pub extra: &'a [Number],
}
pub(super) struct Plan {
    pub wrapped: bool,
    pub bytes: usize,
    action: Action,
}
enum Action {
    Ignore,
    Plain {
        time: i128,
        value: Number,
    },
    Slot {
        slot: i128,
        flush: Option<Number>,
        current: Next,
    },
}
enum Next {
    First(Current),
    More {
        count: usize,
        total: Number,
        min: Number,
        max: Number,
        extra: Option<Number>,
    },
}
impl Slide {
    pub fn new(config: Config, now: i128) -> Self {
        Self {
            config,
            plain: (config.module == Module::Plain).then(|| Plain {
                last: now,
                first: Buffer::empty(),
                second: Buffer::empty(),
            }),
            slot: (config.module == Module::Slots).then(|| Slot {
                current_slot: slot(now, config.slot_period_ms),
                first_slot: 0,
                current: None,
                first: Buffer::empty(),
                second: Buffer::empty(),
            }),
        }
    }
    pub fn bytes(&self) -> usize {
        if let Some(p) = &self.plain {
            p.first.bytes + p.second.bytes
        } else {
            let s = self.slot.as_ref().expect("slot slide");
            s.first.bytes + s.second.bytes + s.current.as_ref().map_or(0, Current::bytes)
        }
    }
    pub fn prepare(&self, now: i128, value: Number) -> Result<Plan, Error> {
        if let Some(p) = &self.plain {
            if self.config.time_span_ms == 0 {
                return Ok(Plan {
                    wrapped: false,
                    bytes: self.bytes(),
                    action: Action::Ignore,
                });
            }
            let wrapped = now.saturating_sub(p.last) > i128::from(self.config.time_span_ms)
                || self.config.max_n.is_some_and(|n| p.first.values.len() >= n);
            return Ok(Plan {
                wrapped,
                bytes: p.first.bytes
                    + if wrapped { 0 } else { p.second.bytes }
                    + 64
                    + value.bytes(),
                action: Action::Plain { time: now, value },
            });
        }
        let s = self.slot.as_ref().expect("slot slide");
        let next_slot = slot(now, self.config.slot_period_ms);
        if s.current_slot != next_slot || s.current.is_none() {
            let flush = if s.current_slot != next_slot {
                s.current
                    .as_ref()
                    .map(|c| c.total.divide(c.count))
                    .transpose()?
            } else {
                None
            };
            let wrapped =
                flush.is_some() && s.first_slot < next_slot.saturating_sub(self.span_slots());
            let bucket = flush.as_ref().map_or(0, |mean| {
                let c = s.current.as_ref().expect("flushed current");
                c.bytes() - c.total.bytes() + mean.bytes() + 32
            });
            let current = Current::first(value);
            return Ok(Plan {
                wrapped,
                bytes: s.first.bytes
                    + if wrapped { 0 } else { s.second.bytes }
                    + bucket
                    + current.bytes(),
                action: Action::Slot {
                    slot: next_slot,
                    flush,
                    current: Next::First(current),
                },
            });
        }
        let c = s.current.as_ref().expect("current slot");
        let count = c.count.checked_add(1).ok_or(Error::Limit)?;
        let total = c.total.add(&value)?;
        let min = c.min.min(&value);
        let max = c.max.max(&value);
        let extra = count.is_multiple_of(4).then_some(value);
        let bytes = self.bytes() - c.total.bytes() - c.min.bytes() - c.max.bytes()
            + total.bytes()
            + min.bytes()
            + max.bytes()
            + extra.as_ref().map_or(0, Number::bytes);
        Ok(Plan {
            wrapped: false,
            bytes,
            action: Action::Slot {
                slot: next_slot,
                flush: None,
                current: Next::More {
                    count,
                    total,
                    min,
                    max,
                    extra,
                },
            },
        })
    }
    pub fn apply(&mut self, plan: Plan) {
        match plan.action {
            Action::Ignore => (),
            Action::Plain { time, value } => {
                let p = self.plain.as_mut().expect("plain plan");
                if plan.wrapped {
                    p.second = std::mem::replace(&mut p.first, Buffer::empty());
                    p.last = time;
                }
                let bytes = 64 + value.bytes();
                p.first.push(time, value, bytes);
            }
            Action::Slot {
                slot,
                flush,
                current,
            } => {
                let s = self.slot.as_mut().expect("slot plan");
                if let Some(mean) = flush {
                    let bucket = s.current.take().expect("flushed current").bucket(mean);
                    if plan.wrapped {
                        s.second = std::mem::replace(&mut s.first, Buffer::empty());
                        s.first_slot = s.current_slot;
                    }
                    let bytes = 32 + bucket.bytes();
                    s.first.push(s.current_slot, bucket, bytes);
                }
                s.current_slot = slot;
                match current {
                    Next::First(c) => s.current = Some(c),
                    Next::More {
                        count,
                        total,
                        min,
                        max,
                        extra,
                    } => {
                        let c = s.current.as_mut().expect("updated current");
                        c.count = count;
                        c.total = total;
                        c.min = min;
                        c.max = max;
                        if let Some(v) = extra {
                            c.extra.push(v);
                        }
                    }
                }
            }
        }
        debug_assert_eq!(self.bytes(), plan.bytes);
    }
    fn span_slots(&self) -> i128 {
        slot(
            i128::from(self.config.time_span_ms),
            self.config.slot_period_ms,
        )
    }
    pub fn samples(&self, now: i128) -> Result<Vec<Sample<'_>>, Error> {
        if let Some(p) = &self.plain {
            if self.config.time_span_ms == 0 {
                return Err(Error::Arithmetic);
            }
            let oldest = now.saturating_sub(i128::from(self.config.time_span_ms));
            let count = self.config.max_n.map_or(p.first.values.len(), |n| {
                n.saturating_sub(p.first.values.len())
            });
            // Preserve the original fold: old buffer oldest first, then the
            // reversed fold of the primary buffer's already reversed prefix.
            return Ok(p
                .second
                .since(oldest, count)
                .rev()
                .chain(p.first.since(oldest, count))
                .map(|(_, n)| Sample {
                    count: 1,
                    value: n.clone(),
                    min: n,
                    max: n,
                    extra: &[],
                })
                .collect());
        }
        let s = self.slot.as_ref().expect("slot slide");
        let next_slot = slot(now, self.config.slot_period_ms);
        let oldest = next_slot.saturating_sub(self.span_slots());
        // take_since materializes the current sample in a temporary slide.
        // Reading must not rotate the retained buffers or advance their clock.
        // Materialization precedes expiry filtering: even an expired slot can
        // fail its integer-to-float conversion in the original callback.
        let current_value = s
            .current
            .as_ref()
            .map(|c| c.total.divide(c.count))
            .transpose()?;
        let rotate = s.current.is_some() && s.first_slot < oldest;
        let mut result = Vec::new();
        if !rotate {
            result.extend(
                s.second
                    .since(oldest, usize::MAX)
                    .rev()
                    .map(|(_, b)| b.view()),
            );
        }
        // A backwards clock can place an expired current slot in front of
        // otherwise eligible primary entries. take_since stops at that head.
        if rotate || s.current.is_none() || s.current_slot >= oldest {
            result.extend(
                s.first
                    .since(oldest, usize::MAX)
                    .rev()
                    .map(|(_, b)| b.view()),
            );
        }
        if let Some(c) = &s.current
            && s.current_slot >= oldest
        {
            result.push(Sample {
                count: c.count,
                value: current_value.expect("current slot materialized"),
                min: &c.min,
                max: &c.max,
                extra: &c.extra,
            });
        }
        Ok(result)
    }
    pub fn reset(&mut self) {
        if let Some(p) = &mut self.plain {
            p.last = 0;
            p.first = Buffer::empty();
            p.second = Buffer::empty();
        }
        if let Some(s) = &mut self.slot {
            s.current_slot = 0;
            s.first_slot = 0;
            s.current = None;
            s.first = Buffer::empty();
            s.second = Buffer::empty();
        }
    }
}
fn slot(time: i128, period: u64) -> i128 {
    // The original uses floating division followed by truncation, including for
    // negative clock values; integer Euclidean division would differ.
    (time as f64 / period as f64).trunc() as i128
}
