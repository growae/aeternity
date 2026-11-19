// Port of erlang-idna 6cff727, punycode.erl. MIT: LICENSE-IDNA.
// Copyright the authors and contributors. All rights reserved.
const MAX: u64 = 0xffff_ffff;
fn threshold(k: u64, bias: u64) -> u64 {
    if k <= bias {
        1
    } else if k >= bias + 26 {
        26
    } else {
        k - bias
    }
}
fn adapt(delta: u64, points: u64, first: bool) -> u64 {
    let d = if first { delta / 700 } else { delta / 2 };
    let mut d = d + d / points;
    let mut k = 0;
    while d > 455 {
        d /= 35;
        k += 36;
    }
    k + 36 * d / (d + 38)
}
fn to_digit(n: u64) -> Result<u8, ()> {
    match n {
        0..=25 => Ok(b'a' + n as u8),
        26..=35 => Ok(b'0' + n as u8 - 26),
        _ => Err(()),
    }
}
fn digit(c: u8) -> Result<u64, ()> {
    match c {
        b'a'..=b'z' => Ok(u64::from(c - b'a')),
        b'A'..=b'Z' => Ok(u64::from(c - b'A')),
        b'0'..=b'9' => Ok(u64::from(c - b'0' + 26)),
        _ => Err(()),
    }
}
pub(super) fn encode(input: &[u32]) -> Result<Vec<u8>, ()> {
    let mut output = input
        .iter()
        .filter(|c| **c < 128)
        .map(|c| *c as u8)
        .collect::<Vec<_>>();
    let basic = output.len();
    if basic > 0 {
        output.push(b'-');
    }
    let (mut h, mut n, mut delta, mut bias) = (basic, 128u64, 0u64, 72u64);
    while h < input.len() {
        let m = input
            .iter()
            .map(|c| u64::from(*c))
            .filter(|c| *c >= n)
            .min()
            .ok_or(())?;
        let points = h as u64 + 1;
        if delta > MAX || m - n > (MAX - delta) / points {
            return Err(());
        }
        delta += (m - n) * points;
        n = m;
        for &cp in input {
            if u64::from(cp) < n {
                delta += 1;
            }
            if u64::from(cp) == n {
                let (mut q, mut k) = (delta, 36);
                loop {
                    let t = threshold(k, bias);
                    if q < t {
                        break;
                    }
                    output.push(to_digit(t + (q - t) % (36 - t))?);
                    q = (q - t) / (36 - t);
                    k += 36;
                }
                output.push(to_digit(q)?);
                bias = adapt(delta, h as u64 + 1, h == basic);
                delta = 0;
                h += 1;
            }
        }
        delta += 1;
        n += 1;
    }
    Ok(output)
}
pub(super) fn decode(input: &[u8]) -> Result<Vec<u32>, (&'static str, &'static str)> {
    let (mut output, mut rest) = match input.iter().rposition(|b| *b == b'-') {
        None => (vec![], input),
        Some(i) => (
            input[..i].iter().map(|c| u32::from(*c)).collect(),
            &input[i + 1..],
        ),
    };
    let (mut n, mut bias, mut i) = (128u64, 72u64, 0u64);
    while !rest.is_empty() {
        let (old_i, mut weight, mut k) = (i, 1u64, 36u64);
        loop {
            let (&c, tail) = rest.split_first().ok_or(("error", "function_clause"))?;
            rest = tail;
            let d = digit(c).map_err(|_| ("exit", "badarg"))?;
            if d > (MAX - i) / weight {
                return Err(("exit", "overflow"));
            }
            i += d * weight;
            let t = threshold(k, bias);
            if d < t {
                break;
            }
            if weight > MAX / (36 - t) {
                return Err(("exit", "overflow"));
            }
            weight *= 36 - t;
            k += 36;
        }
        let len = output.len() as u64 + 1;
        bias = adapt(i - old_i, len, old_i == 0);
        if i / len > MAX - n {
            return Err(("exit", "overflow"));
        }
        n += i / len;
        i %= len;
        output.insert(i as usize, n as u32);
        i += 1;
    }
    Ok(output)
}
