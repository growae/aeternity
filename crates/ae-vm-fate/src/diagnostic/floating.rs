// OTP 26 ryu/d2s.c:to_chars chooses fixed/scientific spelling from the
// decimal exponent and significant digit count, with a special 2^53 bound.
pub(super) fn finite(value: f64) -> String {
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let raw = format!("{:?}", value.abs());
    let (mantissa, exponent) = raw.split_once('e').map_or((raw.as_str(), 0), |(m, e)| {
        (m, e.parse::<i32>().expect("float exponent"))
    });
    let decimal = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let mut point = decimal + exponent;
    while digits.starts_with('0') && digits.len() > 1 {
        digits.remove(0);
        point -= 1;
    }
    while digits.ends_with('0') && digits.len() > 1 {
        digits.pop();
    }
    if digits == "0" {
        return format!("{sign}0.0");
    }
    let fixed = if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if point as usize >= digits.len() {
        format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
    } else {
        let (a, b) = digits.split_at(point as usize);
        format!("{a}.{b}")
    };
    let tail = if digits.len() == 1 { "0" } else { &digits[1..] };
    let scientific = format!("{}.{tail}e{}", &digits[..1], point - 1);
    let exponent = point - digits.len() as i32;
    let lower = if digits.len() == 1 {
        -4
    } else {
        -(digits.len() as i32 + 2)
    };
    let upper = if digits.len() == 1 || point > 10 {
        2
    } else {
        1
    };
    let mantissa: u64 = digits.parse().expect("at most seventeen decimal digits");
    let large_integer = (exponent == 0 && mantissa >= 1 << 53)
        || (exponent == 1 && mantissa > (1 << 52) / 5)
        || (exponent == 2 && mantissa > (1 << 51) / 25);
    format!(
        "{sign}{}",
        if (lower..=upper).contains(&exponent) && !large_integer {
            fixed
        } else {
            scientific
        }
    )
}
