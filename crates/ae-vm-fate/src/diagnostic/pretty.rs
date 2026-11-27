// SPDX-License-Identifier: Apache-2.0
// Adapted from OTP-26.2.5.21 io_lib_pretty pp/cind, for FATE native terms.
// Copyright Ericsson AB 1996-2023. All Rights Reserved.
// License text: licenses/Apache-2.0.txt at the repository root.
use super::{Error, Kind, Node, Work};

#[derive(Clone, Copy)]
struct Pos {
    col: i128,
    ld: i128,
    used: i128,
}
enum Failure {
    Indent,
    Work(Error),
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Work(e)
    }
}
type Result<T> = std::result::Result<T, Failure>;

pub(super) fn render(
    node: &Node,
    column: usize,
    line_length: usize,
    work: &mut Work,
    out: &mut Vec<u8>,
) -> std::result::Result<(), Error> {
    let col = i128::try_from(column).map_err(|_| Error::ResourceLimit)?;
    let max = i128::try_from(node.len).map_err(|_| Error::ResourceLimit)?;
    let mut layout = Layout {
        line: i128::try_from(line_length).map_err(|_| Error::ResourceLimit)?,
        work,
        out,
        max,
        tag_indent: -1,
        check: true,
    };
    // OTP uses zero width to disable line wrapping, after constructing the
    // same depth-limited printable representation as other pretty formats.
    if line_length == 0 {
        layout.check = false;
        return layout.flat(node, 0).map_err(|e| match e {
            Failure::Work(e) => e,
            Failure::Indent => Error::InvalidValue,
        });
    }
    let pos = Pos {
        col,
        ld: 0,
        used: 0,
    };
    let mut chosen = 1;
    for indent in [-1, 4] {
        layout.tag_indent = indent;
        match layout.pp(node, pos, 0) {
            Ok(()) => {
                chosen = indent;
                break;
            }
            Err(Failure::Indent) => {}
            Err(Failure::Work(e)) => return Err(e),
        }
    }
    layout.tag_indent = chosen;
    layout.check = false;
    layout.pp(node, pos, 0).map_err(|e| match e {
        Failure::Work(e) => e,
        Failure::Indent => Error::InvalidValue,
    })
}
struct Layout<'a> {
    line: i128,
    work: &'a mut Work,
    out: &'a mut Vec<u8>,
    max: i128,
    tag_indent: i128,
    check: bool,
}
impl Layout<'_> {
    fn emit(&mut self, s: &[u8]) -> Result<()> {
        if !self.check {
            self.work.push(self.out, s)?;
        }
        Ok(())
    }
    fn newline(&mut self, col: i128) -> Result<()> {
        if !self.check {
            self.work.push(self.out, b"\n")?;
            let n = usize::try_from(col.saturating_sub(1)).map_err(|_| Error::ResourceLimit)?;
            self.work.bytes(n)?;
            self.out.resize(
                self.out.len().checked_add(n).ok_or(Error::ResourceLimit)?,
                b' ',
            );
        }
        Ok(())
    }
    fn fits(&self, len: usize, p: Pos) -> bool {
        let n = len as i128;
        n < self.line - p.col - p.ld && n + p.used + p.ld <= self.max
    }
    fn flat(&mut self, n: &Node, depth: usize) -> Result<()> {
        self.work.visit(depth)?;
        match &n.kind {
            Kind::Atom(s) => self.emit(s),
            Kind::Binary(v) => {
                self.emit(b"<<")?;
                for (i, s) in v.iter().enumerate() {
                    if i > 0 {
                        self.emit(b",")?;
                    }
                    self.emit(s)?;
                }
                self.emit(b">>")
            }
            Kind::Pair(k, v) => {
                self.flat(k, depth + 1)?;
                self.emit(b" => ")?;
                self.flat(v, depth + 1)
            }
            Kind::Seq {
                list,
                children,
                dots,
                ..
            } => {
                self.emit(if *list { b"[" } else { b"{" })?;
                self.flat_sequence(children, *dots, if *list { b"|" } else { b"," }, depth + 1)?;
                self.emit(if *list { b"]" } else { b"}" })
            }
            Kind::Map { pairs, dots } => {
                self.emit(b"#{")?;
                self.flat_sequence(pairs, *dots, b",", depth + 1)?;
                self.emit(b"}")
            }
        }
    }
    fn flat_sequence(
        &mut self,
        v: &[Node],
        dots: bool,
        separator: &[u8],
        depth: usize,
    ) -> Result<()> {
        for (i, n) in v.iter().enumerate() {
            if i > 0 {
                self.emit(b",")?;
            }
            self.flat(n, depth)?;
        }
        if dots {
            if !v.is_empty() {
                self.emit(separator)?;
            }
            self.emit(b"...")?;
        }
        Ok(())
    }
    fn pp(&mut self, n: &Node, p: Pos, depth: usize) -> Result<()> {
        self.work.visit(depth)?;
        if self.fits(n.len, p) {
            return if self.check {
                Ok(())
            } else {
                self.flat(n, depth)
            };
        }
        match &n.kind {
            Kind::Atom(s) => self.emit(s),
            Kind::Pair(..) => {
                self.pair(n, p, depth)?;
                Ok(())
            }
            Kind::Binary(chunks) => {
                if self.check {
                    return Ok(());
                }
                let capacity = 8.max((self.line - p.col - 2).min(self.max - 4 - p.used) - p.ld);
                let mut remaining = capacity;
                self.emit(b"<<")?;
                for (i, s) in chunks.iter().enumerate() {
                    self.work.visit(depth + 1)?;
                    let comma = i + 1 < chunks.len();
                    let len = s.len() as i128 + i128::from(comma);
                    if remaining < len {
                        self.newline(p.col + 2)?;
                        remaining = capacity;
                    }
                    self.emit(s)?;
                    if comma {
                        self.emit(b",")?;
                    }
                    remaining -= len;
                }
                self.emit(b">>")
            }
            Kind::Seq {
                list,
                tagged,
                children,
                dots,
            } => {
                self.emit(if *list { b"[" } else { b"{" })?;
                let sep = if *list { b"|" } else { b"," };
                if *tagged && !children.is_empty() {
                    let tag = &children[0];
                    let n = tag.len as i128;
                    let tag_ind = n + 2;
                    let tcol = p.col + tag_ind;
                    self.flat(tag, depth + 1)?;
                    if self.tag_indent > 0 && tag_ind > self.tag_indent {
                        let col = p.col + self.tag_indent;
                        if self.check && !(self.max + col <= self.line || col <= self.line / 2) {
                            return Err(Failure::Indent);
                        }
                        self.tail(
                            &children[1..],
                            *dots,
                            col,
                            tcol,
                            Pos {
                                used: p.used + 1 + n,
                                ..p
                            },
                            sep,
                            depth + 1,
                            false,
                        )?;
                    } else {
                        if self.check && !(self.max + tcol < self.line || tcol < self.line / 2) {
                            return Err(Failure::Indent);
                        }
                        self.emit(b",")?;
                        self.sequence(
                            &children[1..],
                            *dots,
                            Pos {
                                col: tcol,
                                used: p.used + n + 2,
                                ..p
                            },
                            sep,
                            depth + 1,
                            false,
                        )?;
                    }
                } else {
                    self.sequence(
                        children,
                        *dots,
                        Pos {
                            col: p.col + 1,
                            used: p.used + 1,
                            ..p
                        },
                        sep,
                        depth + 1,
                        false,
                    )?;
                }
                self.emit(if *list { b"]" } else { b"}" })
            }
            Kind::Map { pairs, dots } => {
                self.emit(b"#{")?;
                self.sequence(
                    pairs,
                    *dots,
                    Pos {
                        col: p.col + 2,
                        used: p.used + if self.check { 2 } else { 1 },
                        ..p
                    },
                    b",",
                    depth + 1,
                    true,
                )?;
                self.emit(b"}")
            }
        }
    }
    fn element(&mut self, n: &Node, p: Pos, depth: usize) -> Result<i128> {
        if n.atomic() && self.fits(n.len, p) {
            self.flat(n, depth)?;
            Ok(n.len as i128)
        } else {
            self.pp(n, p, depth)?;
            Ok(self.line)
        }
    }
    fn pair(&mut self, n: &Node, p: Pos, depth: usize) -> Result<i128> {
        if self.fits(n.len, p) {
            self.flat(n, depth)?;
            return Ok(if n.atomic_pair() {
                n.len as i128
            } else {
                self.line
            });
        }
        let Kind::Pair(k, v) = &n.kind else {
            return Err(Error::InvalidValue.into());
        };
        self.pp(k, p, depth + 1)?;
        self.emit(b" =>")?;
        let indent = if self.tag_indent > 0 {
            self.tag_indent
        } else {
            4
        };
        self.newline(p.col + indent)?;
        self.pp(
            v,
            Pos {
                col: p.col + indent,
                used: 0,
                ..p
            },
            depth + 1,
        )?;
        Ok(self.line)
    }
    fn sequence(
        &mut self,
        v: &[Node],
        dots: bool,
        p: Pos,
        sep: &[u8],
        depth: usize,
        map: bool,
    ) -> Result<()> {
        let Some((first, rest)) = v.split_first() else {
            if dots {
                self.emit(b"...")?;
            }
            return Ok(());
        };
        let ld = if rest.is_empty() { p.ld + 1 } else { 0 };
        let width = if map {
            self.pair(first, Pos { ld, ..p }, depth)?
        } else {
            self.element(first, Pos { ld, ..p }, depth)?
        };
        let used = if map && !self.check {
            width
        } else {
            p.used + width
        };
        self.tail(
            rest,
            dots,
            p.col,
            p.col + width,
            Pos { used, ..p },
            sep,
            depth,
            map,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn tail(
        &mut self,
        v: &[Node],
        dots: bool,
        col0: i128,
        mut col: i128,
        p: Pos,
        sep: &[u8],
        depth: usize,
        map: bool,
    ) -> Result<()> {
        let mut used = p.used;
        for (i, n) in v.iter().enumerate() {
            self.work.visit(depth)?;
            let ld = if i + 1 == v.len() { p.ld + 1 } else { 0 };
            let len = 1 + n.len as i128;
            let fits = if ld == 0 {
                len + 1 < self.line - col && used + len < self.max
            } else {
                len < self.line - col - ld && used + len + ld <= self.max
            };
            let atomic = if map { n.atomic_pair() } else { n.atomic() };
            self.emit(b",")?;
            if fits && atomic {
                self.flat(n, depth)?;
                col += len;
                used += len;
            } else {
                self.newline(col0)?;
                let pos = Pos {
                    col: col0,
                    ld,
                    used: 0,
                };
                let width = if map {
                    self.pair(n, pos, depth)?
                } else {
                    self.element(n, pos, depth)?
                };
                col = col0 + width;
                used = width;
            }
        }
        if dots {
            self.emit(sep)?;
            self.emit(b"...")?;
        }
        Ok(())
    }
}
