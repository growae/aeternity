// inet:parse_address accepts legacy IPv4 forms and OTP scoped IPv6 tuples.
// This is a numeric parser only; configuration must never trigger DNS lookup.
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub(super) fn parse(text: &str) -> Option<IpAddr> {
    ipv4(text)
        .map(IpAddr::V4)
        .or_else(|| ipv6(text).map(IpAddr::V6))
}
fn ipv4(text: &str) -> Option<Ipv4Addr> {
    let mut fields = Vec::with_capacity(4);
    for field in text.split('.') {
        if fields.len() == 4 {
            return None;
        }
        let (digits, base, max, octal) = if let Some(rest) = field
            .strip_prefix("0x")
            .or_else(|| field.strip_prefix("0X"))
        {
            (rest.trim_start_matches('0'), 16, 8, false)
        } else if let Some(rest) = field.strip_prefix('0') {
            (rest.trim_start_matches('0'), 8, 11, true)
        } else {
            (field, 10, 10, false)
        };
        if digits.len() > max || (octal && digits.starts_with(['+', '-'])) {
            return None;
        }
        let value = if octal && digits.is_empty() {
            0
        } else {
            i64::from_str_radix(digits, base).ok()?
        };
        fields.push(u32::try_from(value).ok()?);
    }
    let (last, prefix) = fields.split_last()?;
    let bits = 8 * (4 - prefix.len());
    if u64::from(*last) >= 1u64 << bits || prefix.iter().any(|v| *v > 255) {
        return None;
    }
    let prefix = prefix.iter().fold(0u32, |a, b| (a << 8) | b);
    Some(Ipv4Addr::from(if bits == 32 {
        *last
    } else {
        (prefix << bits) | last
    }))
}
fn ipv6(text: &str) -> Option<Ipv6Addr> {
    let (address, scope) = text
        .split_once('%')
        .map_or((text, None), |(a, s)| (a, Some(s)));
    // OTP applies its strict decimal IPv4 parser to embedded dotted tails.
    let normalized;
    let address = if address.contains('.') {
        if scope.is_some() {
            return None;
        }
        let (head, tail) = address.rsplit_once(':')?;
        if !tail.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            return None;
        }
        let mut bytes = Vec::with_capacity(4);
        for part in tail.split('.') {
            if bytes.len() == 4 || part.len() > 3 || (part.len() > 1 && part.starts_with('0')) {
                return None;
            }
            let n: i16 = part.parse().ok()?;
            bytes.push(u8::try_from(n).ok()?);
        }
        let [a, b, c, d] = bytes.as_slice() else {
            return None;
        };
        normalized = format!(
            "{head}:{:x}:{:x}",
            u16::from_be_bytes([*a, *b]),
            u16::from_be_bytes([*c, *d])
        );
        normalized.as_str()
    } else {
        address
    };
    let ip: Ipv6Addr = address.parse().ok()?;
    let Some(scope) = scope else { return Some(ip) };
    let mut words = ip.segments();
    if !matches!(words[0], 0xfe80 | 0xff02) || words[1] != 0 {
        return None;
    }
    // OTP encodes a leading-zero decimal scope in the second 16-bit word;
    // other interface names resolve to zero, not an OS interface index.
    words[1] = if let Some(digits) = scope.strip_prefix('0') {
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        if digits.is_empty() {
            0
        } else {
            digits.parse().ok()?
        }
    } else {
        0
    };
    Some(Ipv6Addr::from(words))
}
