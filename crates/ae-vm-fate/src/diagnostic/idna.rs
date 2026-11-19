// Native messages from erlang-idna 6cff72747821110169ecfac871b0c69e5064afff.
// Copyright the authors and contributors. MIT: crates/ae-codec/LICENSE-IDNA.
use super::{
    abort::{Part, render_parts},
    *,
};
use ae_codec::idna::{BadLabel, BidiError, DetailedError, Error as NameError, LabelError};

pub fn idna_error(
    error: &DetailedError,
    limits: Limits,
) -> Result<Option<(Term<'static>, Limits)>, Error> {
    let mut work = Work::new(limits);
    Ok(convert(error, &mut work, 0)?.map(|t| (t, work.remaining())))
}
fn atom(s: &'static str) -> Term<'static> {
    Term::Atom(Cow::Borrowed(s))
}
fn number(n: impl Into<BigInt>) -> Term<'static> {
    Term::Integer(Cow::Owned(n.into()))
}
fn chars(
    chars: impl ExactSizeIterator<Item = u32>,
    work: &mut Work,
    depth: usize,
) -> Result<Term<'static>, Error> {
    work.visit(depth)?;
    work.bytes(
        chars
            .len()
            .checked_mul(std::mem::size_of::<Term<'_>>())
            .ok_or(Error::ResourceLimit)?,
    )?;
    let mut out = Vec::new();
    for cp in chars {
        work.visit(depth + 1)?;
        out.push(number(cp));
    }
    Ok(Term::List(out))
}
fn category(kind: BadLabel) -> &'static str {
    match kind {
        BadLabel::Nfc => "nfc",
        BadLabel::Hyphen => "hyphen",
        BadLabel::InitialCombiner => "initial_combiner",
        BadLabel::Context => "context",
        BadLabel::ContextJ => "contextj",
        BadLabel::ContextO => "contexto",
        BadLabel::Bidi => "bidi",
        BadLabel::Alabel => "alabel",
        BadLabel::TooLong => "too_long",
    }
}
fn convert(
    error: &DetailedError,
    work: &mut Work,
    depth: usize,
) -> Result<Option<Term<'static>>, Error> {
    work.visit(depth)?;
    let label_error = match error {
        DetailedError::Label(e) => e,
        DetailedError::Other(e) => {
            return Ok(Some(match e {
                NameError::NoRegistrar => atom("no_registrar"),
                NameError::RegistrarUnknown => atom("registrar_unknown"),
                NameError::MultipleNamespaces => atom("multiple_namespaces"),
                NameError::NoLabelInRegistrar => atom("no_label_in_registrar"),
                NameError::InvalidCodepoint(cp) => {
                    Term::Tuple(vec![atom("invalid_codepoint"), number(*cp)])
                }
                NameError::ReferenceException {
                    reason: reason @ ("function_clause" | "badarg" | "overflow"),
                    ..
                } => atom(reason),
                _ => return Ok(None),
            }));
        }
    };
    use Part::{Term as T, Text as S};
    let p = |t| T(t, Style::Pretty, -1);
    let label;
    let cp;
    let pos;
    let status;
    let inner;
    let parts = match label_error {
        LabelError::Nfc => vec![S(b"Label must be in Normalization Form C")],
        LabelError::InitialCombiner => vec![S(b"Label begins with an illegal combining character")],
        LabelError::Hyphen {
            label: l,
            third_fourth,
        } => {
            label = chars(l.iter().copied(), work, depth + 1)?;
            vec![
                S(b"Label "),
                p(&label),
                S(if *third_fourth {
                    b" has disallowed hyphens in 3rd and 4th position"
                } else {
                    b" must not start or end with a hyphen"
                }),
            ]
        }
        LabelError::Context {
            label: l,
            codepoint,
            position,
            unassigned,
        } => {
            label = chars(l.iter().copied(), work, depth + 1)?;
            cp = number(*codepoint);
            pos = number(*position);
            status = atom(if *unassigned {
                "UNASSIGNED"
            } else {
                "DISALLOWED"
            });
            vec![
                S(b"Codepoint "),
                p(&cp),
                S(b" not allowed ("),
                p(&status),
                S(b") at posion "),
                p(&pos),
                S(b" in "),
                p(&label),
            ]
        }
        LabelError::Joiner {
            label: l,
            codepoint,
            position,
            ..
        } => {
            label = chars(l.iter().copied(), work, depth + 1)?;
            cp = number(*codepoint);
            pos = number(*position);
            vec![
                S(b"Joiner "),
                p(&cp),
                S(b" not allowed at posion "),
                p(&pos),
                S(b" in "),
                p(&label),
            ]
        }
        LabelError::TooLong(l) => {
            label = chars(l.iter().copied(), work, depth + 1)?;
            vec![S(b"The label "), p(&label), S(b"  is too long")]
        }
        LabelError::Alabel { label: l, error } => {
            label = chars(l.iter().copied(), work, depth + 1)?;
            let Some(t) = convert(error, work, depth + 1)? else {
                return Ok(None);
            };
            inner = t;
            vec![
                S(b"The label "),
                p(&label),
                S(b"  is not a valid A-label: ulabel error="),
                p(&inner),
            ]
        }
        LabelError::Bidi(e) => match e {
            BidiError::UnknownDirection {
                label: l,
                codepoint,
            } => {
                label = chars(l.iter().copied(), work, depth + 1)?;
                cp = number(*codepoint);
                vec![
                    S(b"unknown directionality in label="),
                    p(&label),
                    S(b" c="),
                    T(&cp, Style::Write, -1),
                    S(b"\n"),
                ]
            }
            BidiError::FirstDirection(l) => {
                label = chars(l.iter().copied(), work, depth + 1)?;
                vec![
                    S(b"first codepoint in label "),
                    p(&label),
                    S(b" must be directionality L, R or AL "),
                ]
            }
            BidiError::InvalidRight => vec![S(
                b"Invalid direction for codepoint  in a right-to-left label",
            )],
            BidiError::InvalidLeft => vec![S(
                b"Invalid direction for codepoint in a left-to-right label",
            )],
            BidiError::MixedNumerals => {
                vec![S(b"Can not mix numeral types in a right-to-left label")]
            }
            BidiError::IllegalEnding => {
                vec![S(b"Label ends with illegal codepoint directionality")]
            }
        },
    };
    let text = render_parts(parts, work)?;
    let text = chars(text.iter().copied().map(u32::from), work, depth + 1)?;
    Ok(Some(Term::Tuple(vec![
        atom("bad_label"),
        Term::Tuple(vec![atom(category(label_error.category())), text]),
    ])))
}
