// SPDX-License-Identifier: Apache-2.0
// OTP-26.2.5.21 unicode_util:gc, restricted to scalar lists.
// Copyright Ericsson AB 2015-2023. All Rights Reserved.
// Apache-2.0 text: crate LICENSE-APACHE.
use super::grapheme_tables::*;
fn member(cp: u32, table: &[(u32, u32)]) -> bool {
    table
        .get(table.partition_point(|r| r.1 < cp))
        .is_some_and(|r| r.0 <= cp)
}
fn hangul(cp: u32) -> u8 {
    match cp {
        4352..=4447 | 43360..=43388 => 1,
        4448..=4519 | 55216..=55238 => 2,
        4520..=4607 | 55243..=55291 => 3,
        44032..=55203 if (cp - 44032).is_multiple_of(28) => 4,
        44032..=55203 => 5,
        _ => 0,
    }
}
pub(super) fn len(input: &[u32]) -> usize {
    if input.is_empty() {
        return 0;
    }
    // Prepend attaches the following cluster unless it starts with a control.
    let mut start = 0;
    while member(input[start], PREPEND)
        && input
            .get(start + 1)
            .is_some_and(|c| !member(*c, IS_CONTROL))
    {
        start += 1;
    }
    let first = input[start];
    let mut end = start + 1;
    if member(first, IS_CONTROL) {
        if first == 13 && input.get(end) == Some(&10) {
            end += 1;
        }
        return end;
    }
    if member(first, IS_EXT_PICT) {
        while let Some(&cp) = input.get(end) {
            if !member(cp, IS_EXTEND) {
                break;
            }
            end += 1;
            if cp == 8205 {
                if input.get(end).is_some_and(|c| member(*c, IS_EXT_PICT)) {
                    end += 1;
                } else {
                    break;
                }
            }
        }
        return end;
    }
    if (127462..=127487).contains(&first) {
        if input
            .get(end)
            .is_some_and(|c| (127462..=127487).contains(c))
        {
            end += 1;
        }
    } else {
        let mut h = hangul(first);
        while let Some(&cp) = input.get(end) {
            let next = hangul(cp);
            if !matches!((h, next), (1, 1 | 2 | 4 | 5) | (2 | 4, 2 | 3) | (3 | 5, 3)) {
                break;
            }
            h = next;
            end += 1;
        }
    }
    while input.get(end).is_some_and(|c| member(*c, IS_EXTEND)) {
        end += 1;
    }
    end
}
