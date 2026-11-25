use super::*;
use num_bigint::BigInt;
use std::{borrow::Cow, cmp::Ordering};

// Only the represented, validated key terms enter this comparison. Formatting
// sentinels (Other/Invalid) intentionally do not pretend to identify an arbitrary
// Erlang term: callers must supply a concrete represented subscription key.
enum Term<'a> {
    Integer(Cow<'a, BigInt>),
    Float(f64),
    Atom(&'a str),
    Tuple(Vec<Term<'a>>),
    List(Vec<Term<'a>>),
    Binary(&'a [u8]),
}
impl Term<'_> {
    fn rank(&self) -> u8 {
        match self {
            Self::Integer(_) | Self::Float(_) => 0,
            Self::Atom(_) => 1,
            Self::Tuple(_) => 2,
            Self::List(v) if v.is_empty() => 3,
            Self::List(_) => 4,
            Self::Binary(_) => 5,
        }
    }
    fn cmp(&self, b: &Self) -> Ordering {
        let rank = self.rank().cmp(&b.rank());
        if rank != Ordering::Equal {
            return rank;
        }
        match (self, b) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b).expect("finite key"),
            (Self::Integer(a), Self::Float(b)) => integer_float(a, *b),
            (Self::Float(a), Self::Integer(b)) => integer_float(b, *a).reverse(),
            (Self::Atom(a), Self::Atom(b)) => a.cmp(b),
            (Self::Binary(a), Self::Binary(b)) => a.cmp(b),
            (Self::Tuple(a), Self::Tuple(b)) => a.len().cmp(&b.len()).then_with(|| sequence(a, b)),
            (Self::List(a), Self::List(b)) => sequence(a, b),
            _ => unreachable!("distinct term ranks"),
        }
    }
}
fn sequence(a: &[Term<'_>], b: &[Term<'_>]) -> Ordering {
    for (a, b) in a.iter().zip(b) {
        let c = a.cmp(b);
        if c != Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}
fn integer_float(integer: &BigInt, float: f64) -> Ordering {
    let bits = float.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    let (mantissa, shift) = if exponent == 0 {
        (fraction, -1074)
    } else {
        ((1u64 << 52) | fraction, exponent - 1075)
    };
    let mut mantissa = BigInt::from(mantissa);
    if bits >> 63 != 0 {
        mantissa = -mantissa;
    }
    if shift >= 0 {
        integer.cmp(&(mantissa << shift as usize))
    } else {
        (integer << (-shift) as usize).cmp(&mantissa)
    }
}
fn part(p: &Part) -> Term<'_> {
    match p {
        Part::Integer(n) => Term::Integer(Cow::Borrowed(n)),
        Part::Atom(s) => Term::Atom(s),
        Part::Binary(b) => Term::Binary(b),
        Part::List(v) => Term::List(
            v.iter()
                .map(|n| Term::Integer(Cow::Owned((*n).into())))
                .collect(),
        ),
    }
}
fn number(n: &report::Value) -> Result<Term<'_>, Error> {
    Ok(match n {
        report::Value::Integer(n) => Term::Integer(Cow::Borrowed(n)),
        report::Value::Float(n) => Term::Float(*n),
        report::Value::Other => return Err(Error::UnsupportedKey),
    })
}
fn extra(e: &report::Extra) -> Result<Term<'_>, Error> {
    Ok(match e {
        report::Extra::Undefined => Term::Atom("undefined"),
        report::Extra::Other => return Err(Error::UnsupportedKey),
        report::Extra::List(v) => Term::List(
            v.iter()
                .map(|o| {
                    Ok(match o {
                        report::OptionValue::ReportType(t) => {
                            Term::Tuple(vec![Term::Atom("report_type"), Term::Atom(t)])
                        }
                        report::OptionValue::Tags(tags) => Term::Tuple(vec![
                            Term::Atom("tags"),
                            Term::List(
                                tags.iter()
                                    .map(|t| {
                                        Ok(match t {
                                            report::Tag::Text(p) => part(p),
                                            report::Tag::Pair(a, b) => {
                                                Term::Tuple(vec![number(a)?, number(b)?])
                                            }
                                            report::Tag::Invalid => {
                                                return Err(Error::UnsupportedKey);
                                            }
                                        })
                                    })
                                    .collect::<Result<_, _>>()?,
                            ),
                        ]),
                        report::OptionValue::NonListTags | report::OptionValue::Other => {
                            return Err(Error::UnsupportedKey);
                        }
                    })
                })
                .collect::<Result<_, _>>()?,
        ),
    })
}
fn key(s: &Subscription) -> Result<Term<'_>, Error> {
    fn name(n: &Name) -> Term<'_> {
        Term::List(n.iter().map(part).collect())
    }
    let metric = match &s.metric {
        Selector::Metric(n) => name(n),
        Selector::Find(n) => Term::Tuple(vec![Term::Atom("find"), name(n)]),
    };
    let points = match &s.points {
        Points::One(p) => part(p),
        Points::Many(v) => Term::List(v.iter().map(part).collect()),
    };
    Ok(Term::Tuple(vec![
        Term::Atom(&s.reporter),
        metric,
        points,
        Term::Atom(if s.retry { "true" } else { "false" }),
        extra(&s.extra)?,
    ]))
}
pub(super) fn validate(s: &Subscription) -> Result<(), Error> {
    key(s).map(|_| ())
}
pub(super) fn compare(a: &Subscription, b: &Subscription) -> Ordering {
    key(a)
        .expect("validated subscription")
        .cmp(&key(b).expect("validated subscription"))
}
