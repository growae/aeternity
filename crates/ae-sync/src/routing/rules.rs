use super::*;

fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|b| char::from(*b)).collect()
}
fn integer(bytes: &[u8]) -> Result<BigInt, Error> {
    let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidRule)?;
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::InvalidRule);
    }
    s.parse().map_err(|_| Error::InvalidRule)
}
fn point(bytes: &[u8], atoms: &BTreeSet<String>) -> Result<Part, Error> {
    if let Ok(s) = std::str::from_utf8(bytes)
        && atoms.contains(s)
    {
        return Ok(Part::atom(s));
    }
    integer(bytes).map(Part::Integer)
}
fn quoted(bytes: &[u8]) -> Result<String, Error> {
    let mut chars = latin1(bytes)
        .chars()
        .collect::<Vec<_>>()
        .into_iter()
        .peekable();
    if chars.next() != Some('\'') {
        return Err(Error::InvalidRule);
    }
    let mut out = String::new();
    while let Some(c) = chars.next() {
        if c == '\'' {
            let mut comment = false;
            for c in chars {
                if comment {
                    comment = c != '\n';
                } else if c == '%' {
                    comment = true;
                } else if !matches!(c, '\0'..=' ' | '\u{80}'..='\u{a0}') {
                    return Err(Error::InvalidRule);
                }
            }
            return Ok(out);
        }
        if c != '\\' {
            out.push(c);
            continue;
        }
        let c = chars.next().ok_or(Error::InvalidRule)?;
        let value = match c {
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => '\u{b}',
            'b' => '\u{8}',
            'f' => '\u{c}',
            'e' => '\u{1b}',
            's' => ' ',
            'd' => '\u{7f}',
            '^' => match chars.next().ok_or(Error::InvalidRule)? {
                '?' => '\u{7f}',
                c @ ('@'..='_' | 'a'..='z') => char::from(c as u8 & 31),
                _ => return Err(Error::InvalidRule),
            },
            'x' => {
                let mut digits = String::new();
                if chars.peek() == Some(&'{') {
                    chars.next();
                    loop {
                        match chars.next() {
                            Some('}') => break,
                            Some(c) if c.is_ascii_hexdigit() => digits.push(c),
                            _ => return Err(Error::InvalidRule),
                        }
                    }
                } else {
                    for _ in 0..2 {
                        match chars.next() {
                            Some(c) if c.is_ascii_hexdigit() => digits.push(c),
                            _ => return Err(Error::InvalidRule),
                        }
                    }
                }
                let value = u32::from_str_radix(&digits, 16).map_err(|_| Error::InvalidRule)?;
                if matches!(value, 0xfffe | 0xffff) {
                    return Err(Error::InvalidRule);
                }
                char::from_u32(value).ok_or(Error::InvalidRule)?
            }
            '0'..='7' => {
                let mut n = c as u32 - '0' as u32;
                for _ in 0..2 {
                    if let Some(c @ '0'..='7') = chars.peek().copied() {
                        chars.next();
                        n = n * 8 + c as u32 - '0' as u32;
                    } else {
                        break;
                    }
                }
                char::from_u32(n).ok_or(Error::InvalidRule)?
            }
            other => other,
        };
        out.push(value);
    }
    Err(Error::InvalidRule)
}
fn atom(s: String, atoms: &mut BTreeSet<String>, limits: Limits) -> Result<Part, Error> {
    if s.chars().count() > 255 {
        return Err(Error::InvalidRule);
    }
    atoms.insert(s.clone());
    if atoms.len() > limits.atoms {
        return Err(Error::Limit);
    }
    Ok(Part::Atom(s))
}
fn component(bytes: &[u8], atoms: &mut BTreeSet<String>, limits: Limits) -> Result<Part, Error> {
    if bytes == b"*" {
        return Ok(Part::atom("_"));
    }
    if bytes.starts_with(b"'") {
        return atom(quoted(bytes)?, atoms, limits);
    }
    if bytes.first().is_some_and(u8::is_ascii_digit)
        && let Ok(n) = integer(bytes)
    {
        return Ok(Part::Integer(n));
    }
    atom(latin1(bytes), atoms, limits)
}
#[derive(Clone)]
struct Pattern {
    head: Vec<Part>,
    tail: bool,
}
fn parts(parts: &[&[u8]], atoms: &mut BTreeSet<String>, limits: Limits) -> Result<Pattern, Error> {
    let mut head = Vec::new();
    let mut index = 0;
    while index < parts.len() {
        head.push(component(parts[index], atoms, limits)?);
        if parts.len() - index == 2 && parts[index + 1] == b"**" {
            return Ok(Pattern { head, tail: true });
        }
        index += 1;
    }
    Ok(Pattern { head, tail: false })
}
fn patterns(
    name: &[u8],
    atoms: &mut BTreeSet<String>,
    limits: Limits,
) -> Result<Vec<Pattern>, Error> {
    if matches!(name, b"" | b"*" | b"**") {
        return Ok(vec![Pattern {
            head: vec![Part::atom("ae"), Part::atom("epoch")],
            tail: true,
        }]);
    }
    let split = name.split(|b| *b == b'.').collect::<Vec<_>>();
    if split.len() > limits.parts {
        return Err(Error::Limit);
    }
    if let Some(at) = split.iter().position(|s| *s == b"**")
        && at + 1 < split.len()
    {
        let before = parts(&split[..at], atoms, limits)?;
        let after = parts(&split[at + 1..], atoms, limits)?;
        return Ok((1..=16)
            .map(|n| {
                let mut head = before.head.clone();
                head.extend(vec![Part::atom("_"); n]);
                head.extend(after.head.clone());
                Pattern {
                    head,
                    tail: after.tail,
                }
            })
            .collect());
    }
    Ok(vec![parts(&split, atoms, limits)?])
}
fn matches_part(expected: &Part, actual: &Part, variables: &mut BTreeMap<i32, Part>) -> bool {
    if let Part::Atom(a) = expected {
        if a == "_" {
            return true;
        }
        if let Some(n) = a.strip_prefix('$')
            && !n.is_empty()
            && (n == "0" || !n.starts_with('0'))
            && n.bytes().all(|c| c.is_ascii_digit())
        {
            // The pinned OTP 26 binary uses a signed 32-bit accumulator here.
            // Keep its observed wrap behavior explicit; do not inherit C overflow.
            let index = n.bytes().fold(0i32, |v, digit| {
                v.wrapping_mul(10).wrapping_add(i32::from(digit - b'0'))
            });
            if index < 0 {
                return expected == actual;
            }
            return match variables.get(&index) {
                Some(previous) => previous == actual,
                None => {
                    variables.insert(index, actual.clone());
                    true
                }
            };
        }
    }
    expected == actual
}
pub(super) fn select(
    command: &[u8],
    entries: &[Entry],
    atoms: &mut BTreeSet<String>,
    limits: Limits,
) -> Result<Selection, Error> {
    if command.len() > limits.text.saturating_mul(4) + 32 {
        return Err(Error::Limit);
    }
    let mut name: &[u8] = b"";
    let mut kind = Some(Part::atom("_"));
    let mut enabled = Some(true);
    let mut datapoints: Option<Vec<Part>> = None;
    for part in command.split(|b| *b == b'/') {
        if let Some(t) = part.strip_prefix(b"type=") {
            let value = latin1(t);
            kind = if t == b"*" {
                Some(Part::atom("_"))
            } else if atoms.contains(&value) {
                Some(Part::Atom(value))
            } else {
                None
            };
        } else if let Some(status) = part.strip_prefix(b"status=") {
            enabled = match status {
                b"enabled" => Some(true),
                b"disabled" => Some(false),
                b"*" => None,
                _ => return Err(Error::InvalidRule),
            };
        } else if part.starts_with(b"[") || part.contains(&b'.') || part.contains(&b'*') {
            name = part;
        } else {
            let next = part
                .split(|b| *b == b',')
                .map(|s| point(s, atoms))
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(points) = &mut datapoints {
                for p in next {
                    if !points.contains(&p) {
                        points.push(p)
                    }
                }
            } else {
                datapoints = Some(next)
            }
        }
    }
    let patterns = patterns(name, atoms, limits)?;
    let mut selected = Vec::new();
    for entry in entries {
        if enabled.is_some_and(|s| s != entry.enabled) {
            continue;
        }
        let Some(kind) = &kind else { continue };
        if patterns.iter().any(|p| {
            if entry.name.len() < p.head.len() || (!p.tail && entry.name.len() != p.head.len()) {
                return false;
            }
            let mut vars = BTreeMap::new();
            p.head
                .iter()
                .zip(&entry.name)
                .all(|(a, b)| matches_part(a, b, &mut vars))
                && matches_part(kind, &Part::Atom(entry.kind.clone()), &mut vars)
        }) {
            selected.push(entry.clone())
        }
    }
    Ok(Selection {
        entries: selected,
        datapoints,
    })
}
pub(super) fn apply(
    rule: &Rule,
    entries: &[Entry],
    atoms: &mut BTreeSet<String>,
    limits: Limits,
) -> Result<(Selection, Vec<String>), Error> {
    limits.rule(rule)?;
    let mut command = b"type=".to_vec();
    command.extend(&rule.kind);
    command.push(b'/');
    command.extend(&rule.name);
    command.push(b'/');
    command.extend(&rule.datapoints);
    let selection = select(&command, entries, atoms, limits)?;
    let mut destinations = BTreeSet::new();
    for value in rule.actions.split(|b| *b == b',') {
        let atom = latin1(value);
        if !atoms.contains(&atom) {
            return Err(Error::InvalidRule);
        }
        destinations.insert(atom);
    }
    let destinations = if destinations.len() == 1 && destinations.contains("none") {
        vec![]
    } else {
        destinations.into_iter().collect()
    };
    Ok((selection, destinations))
}
