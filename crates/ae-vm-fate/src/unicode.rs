// Explicit historical Unicode behavior; never use the Rust host's tables.
use crate::{Budget, Error, Limits};
mod tables;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Otp26Unicode15,
}
pub struct Mapped {
    pub points: Vec<u32>,
    pub remaining_limits: Limits,
}
struct Work {
    budget: Budget,
    bytes: usize,
}
impl Work {
    fn push(&mut self, output: &mut Vec<u32>, cp: u32) -> Result<(), Error> {
        self.budget.visit(0)?;
        if char::from_u32(cp).is_none() {
            return Err(Error::InvalidValue);
        }
        self.bytes = self.bytes.checked_add(4).ok_or(Error::ResourceLimit)?;
        if self.bytes > self.budget.limits.max_bytes {
            return Err(Error::ResourceLimit);
        }
        output.push(cp);
        Ok(())
    }
    fn finish(self, points: Vec<u32>) -> Mapped {
        Mapped {
            points,
            remaining_limits: Limits {
                max_nodes: self.budget.limits.max_nodes - self.budget.nodes,
                max_bytes: self.budget.limits.max_bytes - self.bytes,
                ..self.budget.limits
            },
        }
    }
    fn sort(&mut self, points: &mut [u32]) -> Result<(), Error> {
        let cost = points
            .len()
            .checked_mul((points.len().max(1).ilog2() + 2) as usize)
            .ok_or(Error::ResourceLimit)?;
        if cost > self.budget.limits.max_nodes - self.budget.nodes {
            return Err(Error::ResourceLimit);
        }
        self.budget.nodes += cost;
        points.sort_by_key(|cp| ccc(*cp));
        Ok(())
    }
}
impl Profile {
    pub fn nfc(self, input: &[u32], limits: Limits) -> Result<Mapped, Error> {
        let mut work = Work {
            budget: Budget::new(limits),
            bytes: 0,
        };
        let mut decomposed = Vec::new();
        for &cp in input {
            work.budget.visit(0)?;
            if (0xac00..=0xd7a3).contains(&cp) {
                let index = cp - 0xac00;
                work.push(&mut decomposed, 0x1100 + index / 588)?;
                work.push(&mut decomposed, 0x1161 + (index % 588) / 28)?;
                if index % 28 != 0 {
                    work.push(&mut decomposed, 0x11a7 + index % 28)?;
                }
            } else if let Some(values) = lookup(tables::DECOMPOSITION, cp) {
                for &c in values {
                    work.push(&mut decomposed, c)?;
                }
            } else {
                work.push(&mut decomposed, cp)?;
            }
        }
        let mut start = 0;
        for i in 0..decomposed.len() {
            work.budget.visit(0)?;
            if ccc(decomposed[i]) == 0 {
                work.sort(&mut decomposed[start..i])?;
                start = i + 1;
            }
        }
        work.sort(&mut decomposed[start..])?;
        let mut points = Vec::new();
        let mut starter = None;
        let mut last_class = 0;
        for cp in decomposed {
            work.budget.visit(0)?;
            let class = ccc(cp);
            if let Some(i) = starter
                && (last_class < class || last_class == 0)
                && let Some(composed) = compose(points[i], cp)
            {
                points[i] = composed;
                continue;
            }
            if class == 0 {
                starter = Some(points.len());
            }
            work.push(&mut points, cp)?;
            last_class = class;
        }
        Ok(work.finish(points))
    }
    pub fn case(self, input: &[u32], upper: bool, limits: Limits) -> Result<Mapped, Error> {
        let table = if upper { tables::UPPER } else { tables::LOWER };
        let mut work = Work {
            budget: Budget::new(limits),
            bytes: 0,
        };
        let mut points = Vec::new();
        for &cp in input {
            work.budget.visit(0)?;
            if let Some(values) = lookup(table, cp) {
                for &c in values {
                    work.push(&mut points, c)?;
                }
            } else {
                work.push(&mut points, cp)?;
            }
        }
        Ok(work.finish(points))
    }
}
fn lookup(table: &[(u32, &'static [u32])], cp: u32) -> Option<&'static [u32]> {
    table
        .binary_search_by_key(&cp, |r| r.0)
        .ok()
        .map(|i| table[i].1)
}
fn ccc(cp: u32) -> u8 {
    tables::CCC
        .binary_search_by_key(&cp, |r| r.0)
        .ok()
        .map_or(0, |i| tables::CCC[i].1)
}
fn compose(a: u32, b: u32) -> Option<u32> {
    if (0x1100..0x1113).contains(&a) && (0x1161..0x1176).contains(&b) {
        return Some(0xac00 + (a - 0x1100) * 588 + (b - 0x1161) * 28);
    }
    if (0xac00..=0xd7a3).contains(&a)
        && (a - 0xac00).is_multiple_of(28)
        && (0x11a8..0x11c3).contains(&b)
    {
        return Some(a + b - 0x11a7);
    }
    tables::COMPOSITION
        .binary_search_by_key(&(a, b), |r| (r.0, r.1))
        .ok()
        .map(|i| tables::COMPOSITION[i].2)
}
