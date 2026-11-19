// SPDX-License-Identifier: Apache-2.0
// Rust adaptation of OTP-26.2.5.21 io_lib/io_lib_pretty/io_lib_format.
// Copyright Ericsson AB 1996-2023. All Rights Reserved.
// Modified for the native terms reachable from FATE, with operational budgets.
// License text: licenses/Apache-2.0.txt at the repository root.
use crate::{Budget, Error, Limits};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::borrow::Cow;
mod abort;
mod idna;
mod pretty;
mod reason;
pub use abort::native_abort;
pub use idna::idna_error;
pub use reason::reason;

// Map entries are already in the selected runtime's maps:iterator order.
// They must not be sorted by Rust's ordering or by FATE value comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term<'a> {
    Integer(Cow<'a, BigInt>),
    Atom(Cow<'a, str>),
    Binary(Cow<'a, [u8]>),
    Tuple(Vec<Term<'a>>),
    List(Vec<Term<'a>>),
    Map(Vec<(Term<'a>, Term<'a>)>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Pretty,
    Write,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Release,
    Test,
}
impl Profile {
    pub fn depth(self) -> i32 {
        match self {
            Self::Release => 6,
            Self::Test => 20,
        }
    }
}

pub fn format(
    term: &Term<'_>,
    style: Style,
    depth: i32,
    column: usize,
    limits: Limits,
) -> Result<(Vec<u8>, Limits), Error> {
    let mut work = Work::new(limits);
    work.input(term, 0)?;
    let mut out = Vec::new();
    render(term, style, depth, column, &mut work, &mut out)?;
    Ok((out, work.remaining()))
}

struct Work {
    budget: Budget,
    bytes: usize,
}
impl Work {
    fn new(limits: Limits) -> Self {
        Self {
            budget: Budget::new(limits),
            bytes: 0,
        }
    }
    fn visit(&mut self, depth: usize) -> Result<(), Error> {
        self.budget.visit(depth)
    }
    fn bytes(&mut self, n: usize) -> Result<(), Error> {
        self.bytes = self.bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
        if self.bytes > self.budget.limits.max_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    fn remaining(&self) -> Limits {
        Limits {
            max_bytes: self.budget.limits.max_bytes - self.bytes,
            max_nodes: self.budget.limits.max_nodes - self.budget.nodes,
            ..self.budget.limits
        }
    }
    fn push(&mut self, out: &mut Vec<u8>, s: &[u8]) -> Result<(), Error> {
        self.bytes(s.len())?;
        out.extend_from_slice(s);
        Ok(())
    }
    fn input(&mut self, t: &Term<'_>, d: usize) -> Result<(), Error> {
        self.visit(d)?;
        match t {
            Term::Integer(n) => self
                .bytes(usize::try_from(n.bits().div_ceil(8)).map_err(|_| Error::ResourceLimit)?)?,
            Term::Atom(a) => self.bytes(a.len())?,
            Term::Binary(b) => self.bytes(b.len())?,
            Term::Tuple(v) | Term::List(v) => {
                for t in v {
                    self.input(t, d + 1)?;
                }
            }
            Term::Map(v) => {
                for (k, v) in v {
                    self.input(k, d + 1)?;
                    self.input(v, d + 1)?;
                }
            }
        }
        Ok(())
    }
    fn integer(&mut self, n: &BigInt) -> Result<Vec<u8>, Error> {
        // Reserve a decimal upper bound before allocating the conversion.
        self.bytes(
            usize::try_from(n.bits())
                .map_err(|_| Error::ResourceLimit)?
                .checked_add(2)
                .ok_or(Error::ResourceLimit)?,
        )?;
        Ok(n.to_string().into_bytes())
    }
}

fn render(
    t: &Term<'_>,
    style: Style,
    d: i32,
    column: usize,
    w: &mut Work,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    match style {
        Style::Write => write(t, d, w, out, 0),
        Style::Pretty => {
            let node = node(t, d, w, 0)?;
            pretty::render(&node, column.max(1), w, out)
        }
    }
}
fn dec(d: i32) -> i32 {
    if d < 0 { -1 } else { d - 1 }
}
fn printable(c: u32) -> bool {
    matches!(c, 32..=126 | 160..=255 | 10 | 13 | 9 | 11 | 8 | 12 | 27)
}
fn quoted(chars: impl Iterator<Item = u32>, quote: u8, w: &mut Work) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    w.push(&mut out, &[quote])?;
    for c in chars {
        w.visit(0)?;
        match c {
            c if c == u32::from(quote) || c == 92 => w.push(&mut out, &[b'\\', c as u8])?,
            32..=126 | 160..=255 => w.push(&mut out, &[c as u8])?,
            10 => w.push(&mut out, b"\\n")?,
            13 => w.push(&mut out, b"\\r")?,
            9 => w.push(&mut out, b"\\t")?,
            11 => w.push(&mut out, b"\\v")?,
            8 => w.push(&mut out, b"\\b")?,
            12 => w.push(&mut out, b"\\f")?,
            27 => w.push(&mut out, b"\\e")?,
            127 => w.push(&mut out, b"\\d")?,
            0..=159 => w.push(&mut out, format!("\\{c:03o}").as_bytes())?,
            _ => w.push(&mut out, format!("\\x{{{c:X}}}").as_bytes())?,
        }
    }
    w.push(&mut out, &[quote])?;
    Ok(out)
}
fn atom(a: &str, w: &mut Work) -> Result<Vec<u8>, Error> {
    fn lower(c: char) -> bool {
        c.is_ascii_lowercase() || matches!(c, '\u{df}'..='\u{ff}') && c != '\u{f7}'
    }
    fn rest(c: char) -> bool {
        lower(c)
            || c.is_ascii_alphanumeric()
            || matches!(c, '_' | '@' | '\u{c0}'..='\u{de}') && c != '\u{d7}'
    }
    let reserved = matches!(
        a,
        "after"
            | "begin"
            | "case"
            | "try"
            | "cond"
            | "catch"
            | "andalso"
            | "orelse"
            | "end"
            | "fun"
            | "if"
            | "let"
            | "of"
            | "receive"
            | "when"
            | "bnot"
            | "not"
            | "div"
            | "rem"
            | "band"
            | "and"
            | "bor"
            | "bxor"
            | "bsl"
            | "bsr"
            | "or"
            | "xor"
    );
    let mut chars = a.chars();
    if !reserved && chars.next().is_some_and(lower) && chars.all(rest) {
        let mut out = Vec::new();
        for c in a.chars() {
            w.push(&mut out, &[c as u8])?;
        }
        Ok(out)
    } else {
        quoted(a.chars().map(u32::from), b'\'', w)
    }
}
fn write(t: &Term<'_>, d: i32, w: &mut Work, out: &mut Vec<u8>, level: usize) -> Result<(), Error> {
    w.visit(level)?;
    if d == 0 {
        return w.push(out, b"...");
    }
    match t {
        Term::Integer(n) => {
            let s = w.integer(n)?;
            w.push(out, &s)
        }
        Term::Atom(a) => {
            let s = atom(a, w)?;
            w.push(out, &s)
        }
        Term::Binary(b) => {
            let chunks = binary_chunks(b, d, w)?;
            w.push(out, b"<<")?;
            for (i, c) in chunks.iter().enumerate() {
                if i > 0 {
                    w.push(out, b",")?;
                }
                w.push(out, c)?;
            }
            w.push(out, b">>")
        }
        Term::Tuple(v) | Term::List(v) => {
            let list = matches!(t, Term::List(_));
            w.push(out, if list { b"[" } else { b"{" })?;
            let mut depth = d;
            for (i, t) in v.iter().enumerate() {
                if depth == 1 {
                    if i > 0 {
                        w.push(out, if list { b"|" } else { b"," })?;
                    }
                    w.push(out, b"...")?;
                    break;
                }
                if i > 0 {
                    w.push(out, b",")?;
                }
                depth = dec(depth);
                write(t, depth, w, out, level + 1)?;
            }
            w.push(out, if list { b"]" } else { b"}" })
        }
        Term::Map(v) => {
            w.push(out, b"#{")?;
            if d != 1 && !v.is_empty() {
                let d0 = dec(d);
                let mut depth = d0;
                let mut printed = 0;
                for (i, (k, v)) in v.iter().enumerate() {
                    if i > 0 {
                        w.push(out, b",")?;
                        if depth == 1 {
                            w.push(out, b"...")?;
                            break;
                        }
                        depth = dec(depth);
                    }
                    write(k, d0, w, out, level + 1)?;
                    w.push(out, b" => ")?;
                    write(v, d0, w, out, level + 1)?;
                    printed += 1;
                }
                // Unlike pretty printing, write_map_body tests depth before
                // checking whether its iterator is exhausted.
                if depth == 1 && printed == v.len() {
                    w.push(out, b",...")?;
                }
            }
            w.push(out, b"}")
        }
    }
}
fn binary_chunks(b: &[u8], mut d: i32, w: &mut Work) -> Result<Vec<Vec<u8>>, Error> {
    let mut chunks = Vec::new();
    for x in b {
        w.visit(0)?;
        if d == 1 {
            w.bytes(3)?;
            chunks.push(b"...".to_vec());
            break;
        }
        let s = x.to_string().into_bytes();
        w.bytes(s.len())?;
        chunks.push(s);
        d = dec(d);
    }
    Ok(chunks)
}

#[derive(Debug)]
struct Node {
    kind: Kind,
    len: usize,
}
#[derive(Debug)]
enum Kind {
    Atom(Vec<u8>),
    Seq {
        list: bool,
        tagged: bool,
        children: Vec<Node>,
        dots: bool,
    },
    Map {
        pairs: Vec<Node>,
        dots: bool,
    },
    Pair(Box<Node>, Box<Node>),
    Binary(Vec<Vec<u8>>),
}
impl Node {
    fn atom(s: &[u8], w: &mut Work) -> Result<Self, Error> {
        w.bytes(s.len())?;
        Ok(Self::owned(s.to_vec()))
    }
    fn owned(s: Vec<u8>) -> Self {
        Self {
            len: s.len(),
            kind: Kind::Atom(s),
        }
    }
    fn atomic(&self) -> bool {
        matches!(self.kind, Kind::Atom(_))
    }
    fn atomic_pair(&self) -> bool {
        matches!(&self.kind,Kind::Pair(k,v) if k.atomic() && v.atomic())
    }
}
fn node(t: &Term<'_>, d: i32, w: &mut Work, level: usize) -> Result<Node, Error> {
    w.visit(level)?;
    if d == 0 {
        return Node::atom(b"...", w);
    }
    Ok(match t {
        Term::Integer(n) => Node::owned(w.integer(n)?),
        Term::Atom(a) => Node::owned(atom(a, w)?),
        Term::Binary(b) if b.is_empty() => Node::atom(b"<<>>", w)?,
        Term::Binary(_) if d == 1 => Node::atom(b"<<...>>", w)?,
        Term::Binary(b) => {
            let cap = if d < 0 {
                b.len()
            } else {
                b.len().min((d as usize - 1).saturating_mul(4))
            };
            let mut prefix = 0;
            for &c in &b[..cap] {
                w.visit(level + 1)?;
                if !printable(c.into()) {
                    break;
                }
                prefix += 1;
            }
            if prefix == b.len() || prefix == cap || (d > 0 && prefix >= d as usize - 1) {
                let mut s = b"<<".to_vec();
                w.bytes(2)?;
                let q = quoted(b[..prefix].iter().copied().map(u32::from), b'"', w)?;
                w.push(&mut s, &q)?;
                if prefix < b.len() {
                    w.push(&mut s, b"...")?;
                }
                w.push(&mut s, b">>")?;
                Node::owned(s)
            } else {
                let chunks = binary_chunks(b, d, w)?;
                let len =
                    4 + chunks.iter().map(Vec::len).sum::<usize>() + chunks.len().saturating_sub(1);
                Node {
                    len,
                    kind: Kind::Binary(chunks),
                }
            }
        }
        Term::List(v) | Term::Tuple(v) if v.is_empty() => Node::atom(
            if matches!(t, Term::List(_)) {
                b"[]"
            } else {
                b"{}"
            },
            w,
        )?,
        Term::List(v) | Term::Tuple(v) => {
            let list = matches!(t, Term::List(_));
            let mut chars = Vec::new();
            if list && d != 1 {
                for t in v {
                    w.visit(level + 1)?;
                    match t {
                        Term::Integer(n) if n.to_u32().is_some_and(printable) => {
                            w.bytes(4)?;
                            chars.push(n.to_u32().unwrap());
                        }
                        _ => break,
                    }
                }
            }
            if list && chars.len() == v.len() {
                Node::owned(quoted(chars.into_iter(), b'"', w)?)
            } else if d == 1 {
                if list {
                    Node {
                        len: 5,
                        kind: Kind::Seq {
                            list: true,
                            tagged: false,
                            children: vec![],
                            dots: true,
                        },
                    }
                } else {
                    Node::atom(b"{...}", w)?
                }
            } else {
                let mut depth = d;
                let mut children = Vec::new();
                let mut dots = false;
                for t in v {
                    if depth == 1 {
                        dots = true;
                        break;
                    }
                    depth = dec(depth);
                    children.push(node(t, depth, w, level + 1)?);
                }
                let len = 2 + sequence_len(&children, dots);
                Node {
                    len,
                    kind: Kind::Seq {
                        list,
                        tagged: !list && v.len() > 1 && matches!(v[0], Term::Atom(_)),
                        children,
                        dots,
                    },
                }
            }
        }
        Term::Map(v) if v.is_empty() => Node::atom(b"#{}", w)?,
        Term::Map(_) if d == 1 => Node::atom(b"#{...}", w)?,
        Term::Map(v) => {
            let mut depth = d;
            let mut pairs = Vec::new();
            let mut dots = false;
            for (k, v) in v {
                if depth == 1 {
                    dots = true;
                    break;
                }
                depth = dec(depth);
                let k = node(k, dec(d), w, level + 1)?;
                let v = node(v, dec(d), w, level + 1)?;
                pairs.push(Node {
                    len: k.len + 4 + v.len,
                    kind: Kind::Pair(Box::new(k), Box::new(v)),
                });
            }
            Node {
                len: 3 + sequence_len(&pairs, dots),
                kind: Kind::Map { pairs, dots },
            }
        }
    })
}
fn sequence_len(v: &[Node], dots: bool) -> usize {
    v.iter().map(|n| n.len).sum::<usize>()
        + v.len().saturating_sub(1)
        + if dots {
            3 + usize::from(!v.is_empty())
        } else {
            0
        }
}
