// Adapted from OTP 26 stdlib lists:umerge/3 (ufmerge2_1/ufmerge2_2).
// Copyright Ericsson AB 1996-2025. All Rights Reserved.
// Licensed under the Apache License, Version 2.0; see crate LICENSE-APACHE.
// Rust adaptation: keep native left precedence and duplicate-run behavior.
use super::Point;

pub(super) fn hints(first: &[Point], second: &[Point]) -> Vec<Point> {
    let mut result = Vec::new();
    let (mut left, mut right) = (0, 0);
    while left < first.len() && right < second.len() {
        let b = second[right];
        if first[left].height >= b.height {
            while left < first.len() && first[left].height >= b.height {
                result.push(first[left]);
                left += 1;
            }
            // The native merge removes this one right-hand head, not every
            // equal-height item. Its callers normally provide unique lists.
            if b.height < first[left - 1].height {
                result.push(b);
            }
        } else {
            result.push(b);
        }
        right += 1;
    }
    result.extend_from_slice(&first[left..]);
    result.extend_from_slice(&second[right..]);
    result
}
