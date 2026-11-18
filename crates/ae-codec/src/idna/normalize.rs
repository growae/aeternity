use super::{properties, tables};

pub(super) fn ccc(cp: u32) -> u32 {
    properties(cp) >> 12
}
pub(super) fn nfc(input: &[u32]) -> Vec<u32> {
    let mut decomposed = Vec::new();
    for &cp in input {
        if (0xac00..=0xd7a3).contains(&cp) {
            let index = cp - 0xac00;
            decomposed.extend([0x1100 + index / 588, 0x1161 + (index % 588) / 28]);
            if index % 28 != 0 {
                decomposed.push(0x11a7 + index % 28);
            }
        } else if let Ok(i) = tables::DECOMPOSITION.binary_search_by_key(&cp, |r| r.0) {
            decomposed.extend_from_slice(tables::DECOMPOSITION[i].1);
        } else {
            decomposed.push(cp);
        }
    }
    let mut start = 0;
    for i in 0..decomposed.len() {
        if ccc(decomposed[i]) == 0 {
            decomposed[start..i].sort_by_key(|cp| ccc(*cp));
            start = i + 1;
        }
    }
    decomposed[start..].sort_by_key(|cp| ccc(*cp));
    let mut output = Vec::new();
    let mut starter = None;
    let mut last_class = 0;
    for cp in decomposed {
        let class = ccc(cp);
        if let Some(i) = starter
            && (last_class < class || last_class == 0)
            && let Some(composed) = compose(output[i], cp)
        {
            output[i] = composed;
            continue;
        }
        if class == 0 {
            starter = Some(output.len());
        }
        output.push(cp);
        last_class = class;
    }
    output
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
