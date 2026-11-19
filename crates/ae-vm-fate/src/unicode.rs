// SPDX-License-Identifier: Apache-2.0
// Native normalization adapted from OTP-26.2.5.21 unicode_util.erl.
// Copyright Ericsson AB 2015-2023. All Rights Reserved.
// Apache-2.0 text: repository licenses/Apache-2.0.txt.
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
        // Charge and validate the input before the shared native cluster scan.
        for &cp in input {
            work.budget.visit(0)?;
            if char::from_u32(cp).is_none() {
                return Err(Error::InvalidValue);
            }
        }
        let mut points = Vec::new();
        let mut rest = input;
        while !rest.is_empty() {
            let len = ae_codec::unicode::Profile::Otp26Unicode15.first_grapheme_len(rest);
            normalize_cluster(&rest[..len], &mut points, &mut work)?;
            rest = &rest[len..];
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
fn normalize_cluster(input: &[u32], points: &mut Vec<u32>, work: &mut Work) -> Result<(), Error> {
    let mut decomposed = Vec::new();
    for &cp in input {
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
    // Preserve OTP's cluster-local base, including composition across
    // zero-class joiners. Its recursive Hangul prefix path is iterative.
    let mut tail = decomposed.as_slice();
    while tail.len() >= 2 && (0x1100..=0x1112).contains(&tail[0]) {
        work.budget.visit(0)?;
        if (0x1161..=0x1175).contains(&tail[1]) {
            let mut cp = 0xac00 + (tail[0] - 0x1100) * 588 + (tail[1] - 0x1161) * 28;
            tail = &tail[2..];
            if let Some(&t) = tail.first()
                && (0x11a7..=0x11c2).contains(&t)
            {
                cp += t - 0x11a7;
                tail = &tail[1..];
            }
            work.push(points, cp)?;
            for &cp in tail {
                work.push(points, cp)?;
            }
            return Ok(());
        }
        work.push(points, tail[0])?;
        tail = &tail[1..];
    }
    let Some((&first, rest)) = tail.split_first() else {
        return Ok(());
    };
    if tail.len() == 2
        && let Some(cp) = compose(first, rest[0])
    {
        work.push(points, cp)?;
        return Ok(());
    }
    let base = points.len();
    work.push(points, first)?;
    let mut previous = ccc(first);
    for &cp in rest {
        work.budget.visit(0)?;
        let class = ccc(cp);
        if (previous == 0 || previous < class)
            && let Some(composed) = compose(points[base], cp)
        {
            points[base] = composed;
        } else {
            work.push(points, cp)?;
            previous = class;
        }
    }
    Ok(())
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
    tables::COMPOSITION
        .binary_search_by_key(&(a, b), |r| (r.0, r.1))
        .ok()
        .map(|i| tables::COMPOSITION[i].2)
}
