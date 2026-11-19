// AENS normalization: aeternity fa2a6d7 aens_utils and pinned erlang-idna
// 6cff72747821110169ecfac871b0c69e5064afff. Unicode 11 mapping/validity.
// IDNA portions: Copyright the authors and contributors. MIT: LICENSE-IDNA.
// AENS wrapper: Copyright (c) 2018 Aeternity Anstalt. ISC: repository LICENSE.
mod error;
mod normalize;
mod punycode;
mod tables;
pub use error::{BidiError, DetailedError, JoinerKind, LabelError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadLabel {
    Nfc,
    Hyphen,
    InitialCombiner,
    Context,
    ContextJ,
    ContextO,
    Bidi,
    Alabel,
    TooLong,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NoRegistrar,
    RegistrarUnknown,
    MultipleNamespaces,
    NoLabelInRegistrar,
    InvalidCodepoint(u32),
    BadLabel(BadLabel),
    ReferenceException {
        class: &'static str,
        reason: &'static str,
    },
}
fn exception(class: &'static str, reason: &'static str) -> DetailedError {
    Error::ReferenceException { class, reason }.into()
}
pub fn to_ascii(name: &[u8]) -> Result<Vec<u8>, Error> {
    to_ascii_detailed(name).map_err(|e| e.category())
}
pub fn to_ascii_detailed(name: &[u8]) -> Result<Vec<u8>, DetailedError> {
    let parts = name.split(|c| *c == b'.').collect::<Vec<_>>();
    let count = parts
        .iter()
        .rposition(|p| !p.is_empty())
        .map_or(0, |i| i + 1);
    match &parts[..count] {
        [] => return Err(exception("error", "case_clause")),
        [_] => return Err(Error::NoRegistrar.into()),
        [_, registrar] if *registrar == b"chain" || *registrar == b"test" => {}
        [_, _] => return Err(Error::RegistrarUnknown.into()),
        _ => return Err(Error::MultipleNamespaces.into()),
    }
    let utf8 = std::str::from_utf8(name).map_err(|_| exception("error", "function_clause"))?;
    let mut mapped = Vec::new();
    for c in utf8.chars() {
        let cp = c as u32;
        let row = &tables::MAPPING[tables::MAPPING.partition_point(|r| r.0 < cp)];
        match row.1 {
            0 => mapped.push(cp),
            1 => mapped.extend_from_slice(row.2),
            2 => {}
            _ => return Err(Error::InvalidCodepoint(cp).into()),
        }
    }
    let normalized = normalize::nfc(&mapped);
    let mut output = Vec::new();
    for label in normalized.split(|cp| matches!(*cp, 0x2e | 0x3002 | 0xff0e | 0xff61)) {
        let label = alabel(label)?;
        // encode_1 treats an empty accumulated label as no previous label.
        if !output.is_empty() {
            output.push(b'.');
        }
        output.extend(label);
    }
    if !output.contains(&b'.') {
        return Err(Error::NoLabelInRegistrar.into());
    }
    Ok(output)
}
fn alabel(label: &[u32]) -> Result<Vec<u8>, DetailedError> {
    let result = if label.iter().all(|cp| *cp < 128) {
        if !label.is_empty() {
            let ascii = label
                .iter()
                .map(|cp| (*cp as u8).to_ascii_lowercase())
                .collect::<Vec<_>>();
            let validate = || {
                let decoded = if let Some(tail) = ascii.strip_prefix(b"xn--") {
                    punycode::decode(tail).map_err(|e| exception(e.0, e.1))?
                } else {
                    ascii.iter().map(|b| u32::from(*b)).collect()
                };
                check_label(&decoded)
            };
            validate().map_err(|error| LabelError::Alabel {
                label: label.to_vec(),
                error: Box::new(error),
            })?;
        }
        label.iter().map(|cp| *cp as u8).collect()
    } else {
        check_label(label)?;
        let mut result = b"xn--".to_vec();
        result.extend(punycode::encode(label).map_err(|_| exception("exit", "oveflow"))?);
        result
    };
    if result.len() > 63 {
        return Err(LabelError::TooLong(result.iter().map(|b| u32::from(*b)).collect()).into());
    }
    Ok(result)
}
fn properties(cp: u32) -> u32 {
    tables::PROPERTIES
        .get(tables::PROPERTIES.partition_point(|r| r.0 < cp))
        .map_or(4, |r| r.1)
}
fn check_label(label: &[u32]) -> Result<(), DetailedError> {
    if label.iter().any(|cp| *cp > 0x10ffff) {
        return Err(exception("error", "function_clause"));
    }
    if normalize::nfc(label) != label {
        return Err(LabelError::Nfc.into());
    }
    if label.is_empty() {
        return Err(exception("error", "function_clause"));
    }
    if label.first() == Some(&45) || label.last() == Some(&45) || label.get(2..4) == Some(&[45, 45])
    {
        return Err(LabelError::Hyphen {
            label: label.to_vec(),
            third_fourth: label.get(2..4) == Some(&[45, 45]),
        }
        .into());
    }
    if properties(label[0]) & 8 != 0 {
        return Err(LabelError::InitialCombiner.into());
    }
    for (pos, &cp) in label.iter().enumerate() {
        match properties(cp) & 7 {
            1 => {}
            2 if context_j(cp, label, pos) => {}
            3 if context_o(cp, label, pos) => {}
            status @ (2 | 3) => {
                return Err(LabelError::Joiner {
                    label: label.to_vec(),
                    codepoint: cp,
                    position: pos,
                    kind: if status == 2 {
                        JoinerKind::J
                    } else {
                        JoinerKind::O
                    },
                }
                .into());
            }
            status => {
                return Err(LabelError::Context {
                    label: label.to_vec(),
                    codepoint: cp,
                    position: pos,
                    unassigned: status == 4,
                }
                .into());
            }
        }
    }
    bidi(label)
}
fn context_j(cp: u32, label: &[u32], pos: usize) -> bool {
    if pos > 0 && normalize::ccc(label[pos - 1]) == 9 {
        return true;
    }
    if cp != 0x200c {
        return false;
    }
    // Preserve the pinned implementation's suffix scans (not RFC intent).
    let ld = |c: &u32| properties(*c) & 256 != 0;
    label[pos..].iter().rev().any(ld)
        && label.get(pos + 2..).is_some_and(|tail| tail.iter().any(ld))
}
fn context_o(cp: u32, label: &[u32], pos: usize) -> bool {
    let script = |c: u32| (properties(c) >> 9) & 7;
    match cp {
        0xb7 if pos > 0 && pos < label.len() - 1 => {
            // lists:sublist(Label, Pos, Pos+2) uses length, not end position.
            let end = (pos - 1 + pos + 2).min(label.len());
            label[pos - 1..end] == [0x6c, 0xb7, 0x6c]
        }
        0x375 => label.get(pos + 1).is_some_and(|c| script(*c) == 1),
        0x5f3 | 0x5f4 => pos > 0 && script(label[pos - 1]) == 2,
        0x30fb => label
            .iter()
            .any(|c| *c != 0x30fb && matches!(script(*c), 3..=5)),
        0x660..=0x669 => !label.iter().any(|c| (0x6f0..=0x6f9).contains(c)),
        0x6f0..=0x6f9 => !label.iter().any(|c| (0x660..=0x669).contains(c)),
        _ => false,
    }
}
fn bidi(label: &[u32]) -> Result<(), DetailedError> {
    let dir = |c: u32| (properties(c) >> 4) & 15;
    let mut has_rtl = false;
    for &cp in label {
        let d = dir(cp);
        if d == 11 {
            return Err(LabelError::Bidi(BidiError::UnknownDirection {
                label: label.to_vec(),
                codepoint: cp,
            })
            .into());
        }
        if matches!(d, 1..=3) {
            has_rtl = true;
            break;
        }
    }
    if !has_rtl {
        return Ok(());
    }
    let rtl = match dir(label[0]) {
        1 | 2 => true,
        0 => false,
        _ => return Err(LabelError::Bidi(BidiError::FirstDirection(label.to_vec())).into()),
    };
    let mut ending = false;
    let mut number = None;
    for &cp in label {
        let d = dir(cp);
        if if rtl {
            !(1..=10).contains(&d)
        } else {
            !matches!(d, 0 | 4..=10)
        } {
            return Err(LabelError::Bidi(if rtl {
                BidiError::InvalidRight
            } else {
                BidiError::InvalidLeft
            })
            .into());
        }
        if if rtl {
            (1..=4).contains(&d)
        } else {
            matches!(d, 0 | 4)
        } {
            ending = true;
        } else if d != 10 {
            ending = false;
        }
        if rtl && matches!(d, 3 | 4) {
            if number.is_some_and(|n| n != d) {
                return Err(LabelError::Bidi(BidiError::MixedNumerals).into());
            }
            number = Some(d);
        }
    }
    if ending {
        Ok(())
    } else {
        Err(LabelError::Bidi(BidiError::IllegalEnding).into())
    }
}
