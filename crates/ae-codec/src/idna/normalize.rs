// SPDX-License-Identifier: Apache-2.0
// OTP-26.2.5.21 unicode_util:nfc decomposition and cluster composition.
// Copyright Ericsson AB 2015-2023. All Rights Reserved.
// Apache-2.0 text: crate LICENSE-APACHE.
use super::{grapheme, properties, tables};

pub(super) fn ccc(cp: u32) -> u32 {
    properties(cp) >> 12
}
pub(super) fn nfc(input: &[u32]) -> Vec<u32> {
    let mut output = Vec::new();
    let mut rest = input;
    while !rest.is_empty() {
        let len = grapheme::len(rest);
        output.extend(cluster(&rest[..len]));
        rest = &rest[len..];
    }
    output
}
fn cluster(input: &[u32]) -> Vec<u32> {
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
    compose_cluster(&decomposed)
}
fn compose_cluster(input: &[u32]) -> Vec<u32> {
    // The native tail recursion can skip arbitrarily many Hangul leads.
    // Keep that path iterative so a long caller-supplied label cannot consume
    // the Rust call stack.
    let mut prefix = Vec::new();
    let mut tail = input;
    while tail.len() >= 2
        && (0x1100..=0x1112).contains(&tail[0])
        && !(0x1161..=0x1175).contains(&tail[1])
    {
        prefix.push(tail[0]);
        tail = &tail[1..];
    }
    prefix.extend(compose_tail(tail));
    prefix
}
fn compose_tail(input: &[u32]) -> Vec<u32> {
    let Some((&first, rest)) = input.split_first() else {
        return vec![];
    };
    if (0x1100..=0x1112).contains(&first) && !rest.is_empty() {
        if (0x1161..=0x1175).contains(&rest[0]) {
            let mut cp = 0xac00 + (first - 0x1100) * 588 + (rest[0] - 0x1161) * 28;
            let mut tail = &rest[1..];
            if let Some(&t) = tail.first()
                && (0x11a7..=0x11c2).contains(&t)
            {
                cp += t - 0x11a7;
                tail = &tail[1..];
            }
            let mut out = vec![cp];
            out.extend_from_slice(tail);
            return out;
        }
        unreachable!("leading Hangul run was already consumed");
    }
    if input.len() == 2
        && let Some(cp) = compose(first, rest[0])
    {
        return vec![cp];
    }
    let mut out = vec![first];
    let mut previous = ccc(first);
    for &cp in rest {
        let class = ccc(cp);
        if (previous == 0 || previous < class)
            && let Some(combined) = compose(out[0], cp)
        {
            out[0] = combined;
        } else {
            out.push(cp);
            previous = class;
        }
    }
    out
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

#[cfg(test)]
mod reference_tests {
    use super::*;
    use serde_json::Value as J;
    fn expand(v: &J, pool: &[J], ceiling: usize) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                let i = m["$ref"].as_u64().unwrap() as usize;
                assert!(i < ceiling);
                expand(&pool[i], pool, i)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), expand(v, pool, ceiling)))
                    .collect(),
            ),
            J::Array(v) => J::Array(v.iter().map(|v| expand(v, pool, ceiling)).collect()),
            _ => v.clone(),
        }
    }
    fn points(v: &J) -> Vec<u32> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap().try_into().unwrap())
            .collect()
    }
    #[test]
    fn native_grapheme_clusters_and_normalization_match_original_otp26() {
        let data: J = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_native_normalization.json"
        ))
        .unwrap();
        let pool = data["values"].as_array().unwrap();
        let data = expand(&data["root"], pool, pool.len());
        assert_eq!(data["cases"].as_array().unwrap().len(), 56285);
        for row in data["cases"].as_array().unwrap() {
            let input = points(&row["input"]);
            let mut rest = input.as_slice();
            let mut clusters = Vec::new();
            while !rest.is_empty() {
                let n = grapheme::len(rest);
                clusters.push(rest[..n].to_vec());
                rest = &rest[n..];
            }
            let expected = row["clusters"]
                .as_array()
                .unwrap()
                .iter()
                .map(points)
                .collect::<Vec<_>>();
            assert_eq!(clusters, expected, "clusters {input:?}");
            assert_eq!(nfc(&input), points(&row["nfc"]), "nfc {input:?}");
        }
    }
}
