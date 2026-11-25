use super::{Error, Limits, Part, Points, Value};
mod heap;
mod number;
pub mod probe;
mod slide;
use heap::Heap;
use number::Number;
use slide::Slide;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Module {
    Slots,
    Plain,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    Truncate,
    Preserve,
    Round,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub module: Module,
    pub rounding: Rounding,
    pub time_span_ms: u64,
    pub slot_period_ms: u64,
    pub keep_high: usize,
    pub max_n: Option<usize>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            module: Module::Slots,
            rounding: Rounding::Truncate,
            time_span_ms: 60_000,
            slot_period_ms: 10,
            keep_high: 0,
            max_n: None,
        }
    }
}
impl Config {
    pub fn validate(self, limits: Limits) -> Result<(), Error> {
        limits.validate().map_err(|_| Error::Limit)?;
        if self.module == Module::Slots && self.slot_period_ms == 0 {
            return Err(Error::InvalidValue);
        }
        if self.keep_high > limits.retained_bytes / 96 {
            return Err(Error::Limit);
        }
        Ok(())
    }
}

pub fn datapoints() -> Vec<Part> {
    ["n", "mean", "min", "max", "median"]
        .into_iter()
        .map(Part::atom)
        .chain([50, 75, 90, 95, 99, 999].map(|n| Part::Integer(n.into())))
        .collect()
}

// Arithmetic is independent of worker scheduling. The caller supplies the
// original millisecond clock and can reserve a shared quota before each update.
pub struct Histogram {
    config: Config,
    limits: Limits,
    slide: Slide,
    new: Heap,
    old: Heap,
}
impl Histogram {
    pub fn new(config: Config, limits: Limits, now_ms: i128) -> Result<Self, Error> {
        config.validate(limits)?;
        Ok(Self {
            config,
            limits,
            slide: Slide::new(config, now_ms),
            new: Heap::default(),
            old: Heap::default(),
        })
    }
    pub fn retained_bytes(&self) -> usize {
        self.slide.bytes() + self.new.bytes + self.old.bytes
    }
    pub fn update(&mut self, now_ms: i128, value: &Value) -> Result<(), Error> {
        self.update_with(now_ms, value, |_, _| Ok(()))
    }
    pub(super) fn update_with(
        &mut self,
        now_ms: i128,
        value: &Value,
        reserve: impl FnOnce(usize, usize) -> Result<(), Error>,
    ) -> Result<(), Error> {
        value.bytes(self.limits.report)?;
        let Some(value) = Number::from_value(value) else {
            return Ok(());
        };
        let plan = self.slide.prepare(now_ms, value.clone())?;
        let heap = self.config.module == Module::Slots && self.config.keep_high > 0;
        let next = plan.bytes.saturating_add(if heap {
            if plan.wrapped {
                self.new.bytes.saturating_add(64 + value.bytes())
            } else {
                self.old
                    .bytes
                    .saturating_add(self.new.projected(&value, self.config.keep_high))
            }
        } else {
            0
        });
        if next > self.limits.retained_bytes {
            return Err(Error::Limit);
        }
        reserve(self.retained_bytes(), next)?;
        if heap {
            if plan.wrapped {
                self.old = std::mem::take(&mut self.new);
            }
            self.new.insert(value, now_ms, self.config.keep_high);
        }
        self.slide.apply(plan);
        debug_assert_eq!(self.retained_bytes(), next);
        Ok(())
    }
    pub fn reset(&mut self) {
        // The reference resets the slide alone. Top-value heaps survive reset.
        self.slide.reset();
    }
    pub fn values(&self, now_ms: i128, points: &Points) -> Result<Vec<(Part, Value)>, Error> {
        self.limits.points(points).map_err(|_| Error::Limit)?;
        let defaults;
        let requested = if *points == Points::default_points() {
            defaults = datapoints();
            &defaults[..]
        } else {
            match points {
                Points::One(p) => std::slice::from_ref(p),
                Points::Many(v) => v,
            }
        };
        if requested.is_empty() {
            return Ok(vec![]);
        }
        let samples = self.slide.samples(now_ms)?;
        let length = samples.len();
        let mut full = 0usize;
        let mut total = if self.config.rounding == Rounding::Preserve {
            Number::Float(0.0)
        } else {
            Number::zero()
        };
        let mut min: Option<Number> = None;
        let mut max = Number::zero();
        let mut values = Vec::with_capacity(length + 2);
        for s in &samples {
            full = full.checked_add(s.count).ok_or(Error::Limit)?;
            total = total.add(&s.value)?;
            if self.config.module == Module::Slots {
                min = Some(min.as_ref().map_or_else(|| s.min.clone(), |n| n.min(s.min)));
                max = max.max(s.max);
            } else {
                min = Some(min.as_ref().map_or_else(|| s.min.clone(), |n| s.min.min(n)));
                max = s.max.max(&max);
            }
            values.push(s.value.clone());
        }
        // Fold accumulation prepends slot values and extra lists. Each stored
        // extra vector uses append order, so reverse it when consuming the
        // original list's newest-first prefix.
        values.reverse();
        let mean = if length == 0 {
            Number::Float(0.0)
        } else {
            total.divide(length)?
        };
        if self.config.module == Module::Slots && length > 0 {
            let pick = if length < 300 {
                2.max((600 - length) / length + 1)
            } else {
                1
            };
            let extra: Vec<Number> = samples
                .iter()
                .rev()
                .flat_map(|s| s.extra.iter().rev().take(pick))
                .cloned()
                .collect();
            // pick_extra prepends each selected value, then lists:sort is stable
            // for numeric ties. Preserve that order for represented int/float.
            let mut combined: Vec<Number> = extra.into_iter().rev().collect();
            combined.extend(values);
            values = combined;
        }
        values.sort_by(Number::cmp);
        if self.config.module == Module::Slots {
            values.insert(0, min.unwrap_or_else(Number::zero));
            values.push(max);
        }
        let mut results = self.top(now_ms, full, requested)?;
        // Erlang's [0,0] pattern uses exact integer matching, not numeric ==.
        let empty = values.len() == 2
            && values
                .iter()
                .all(|n| matches!(n, Number::Integer(i) if *i == 0.into()));
        if !empty && !values.is_empty() {
            let n = values.len();
            results.extend([
                (Part::atom("n"), Number::Integer(n.into())),
                (Part::atom("mean"), mean),
                (Part::atom("total"), total),
            ]);
            let p50 = percentile(50, n)?;
            let mut items = vec![
                (Part::atom("min"), 1),
                (Part::Integer(50.into()), p50),
                (Part::atom("median"), p50),
            ];
            for p in [75, 90, 95, 99, 999] {
                items.push((Part::Integer(p.into()), percentile(p, n)?));
            }
            items.push((Part::atom("max"), n));
            results.extend(pick_items(&values, items));
        }
        let mut bytes = 32usize;
        requested
            .iter()
            .map(|point| {
                let found = results.iter().find(|(p, _)| p == point);
                let value = found.map(|(_, v)| v.clone()).unwrap_or_else(|| {
                    if self.config.rounding == Rounding::Preserve {
                        Number::Float(0.0)
                    } else {
                        Number::zero()
                    }
                });
                let value = if (found.is_some() && *point == Part::atom("median"))
                    || self.config.rounding == Rounding::Truncate
                {
                    value.integer(false)
                } else if self.config.rounding == Rounding::Round {
                    value.integer(true)
                } else {
                    value
                };
                let value = value.value();
                bytes = bytes
                    .saturating_add(self.limits.report.part(point).map_err(|_| Error::Limit)?)
                    .saturating_add(value.bytes(self.limits.report)?);
                if bytes > self.limits.retained_bytes {
                    return Err(Error::Limit);
                }
                Ok((point.clone(), value))
            })
            .collect()
    }
    fn top(
        &self,
        now_ms: i128,
        full: usize,
        requested: &[Part],
    ) -> Result<Vec<(Part, Number)>, Error> {
        let size = self.new.len + self.old.len;
        if full == 0 || size == 0 {
            return Ok(vec![]);
        }
        let min = 100i128 - (size as i128 * 100 / full as i128);
        let mut items = Vec::new();
        for point in requested.iter().rev() {
            if let Part::Integer(p) = point
                && ((*p < 100.into() && *p >= min.into())
                    || (*p > 100.into() && *p >= (min * 10).into()))
            {
                let p = p.to_string().parse().map_err(|_| Error::Arithmetic)?;
                items.push((point.clone(), full - percentile(p, full)? + 1));
            }
        }
        if items.is_empty() {
            return Ok(vec![]);
        }
        let mut values = Vec::new();
        self.old.values(
            Some(now_ms.saturating_sub(i128::from(self.config.time_span_ms))),
            &mut values,
        );
        self.new.values(None, &mut values);
        // lists:sort(>/2) reverses the represented order of equal numbers.
        values.reverse();
        values.sort_by(|a, b| b.cmp(a));
        Ok(pick_items(&values, items))
    }
}
fn percentile(point: i64, n: usize) -> Result<usize, Error> {
    let p = match point {
        50 => 0.5,
        75 => 0.75,
        90 => 0.9,
        95 => 0.95,
        99 => 0.99,
        999 => 0.999,
        _ => return Err(Error::Arithmetic),
    };
    Ok((p * n as f64).round() as usize)
}
fn pick_items(values: &[Number], items: Vec<(Part, usize)>) -> Vec<(Part, Number)> {
    let mut position = 1;
    items
        .into_iter()
        .map(|(point, target)| {
            // The native traversal never rewinds. A target behind the cursor
            // exhausts the input, and all remaining items receive floating zero.
            if target >= position && target <= values.len() {
                position = target;
                (point, values[position - 1].clone())
            } else {
                position = values.len() + 1;
                (point, Number::Float(0.0))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;
