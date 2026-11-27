use jsonschema::{Keyword, ValidationError};
use serde_json::Value;

pub(super) enum Pattern {
    Hex,
    Bytes(String),
    Word(String),
}
impl Pattern {
    pub(super) fn new(pattern: &str) -> Self {
        if pattern == "^(0x|0X)?[a-fA-F0-9]+$" {
            return Self::Hex;
        }
        let pattern = pattern.strip_prefix('^').expect("anchored HTTP pattern");
        if let Some(prefix) = pattern.strip_suffix("[0-9A-Za-z+/=]+$") {
            Self::Bytes(prefix.into())
        } else {
            Self::Word(
                pattern
                    .strip_suffix(r"\w+$")
                    .expect("supported native HTTP pattern")
                    .into(),
            )
        }
    }
    pub(super) fn matches(&self, value: &str) -> bool {
        // PCRE's $ permits a single final LF; \w uses the original UCP table,
        // which differs from both Rust's current Unicode tables and regex \w.
        let value = value.strip_suffix('\n').unwrap_or(value);
        match self {
            Self::Hex => {
                let value = value
                    .strip_prefix("0x")
                    .or_else(|| value.strip_prefix("0X"))
                    .unwrap_or(value);
                !value.is_empty() && value.bytes().all(|b| b.is_ascii_hexdigit())
            }
            Self::Bytes(prefix) => value.strip_prefix(prefix).is_some_and(|value| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'='))
            }),
            Self::Word(prefix) => value.strip_prefix(prefix).is_some_and(|value| {
                !value.is_empty()
                    && value.chars().all(|c| {
                        let c = c as u32;
                        let ranges = super::word_ranges::RANGES;
                        let i = ranges.partition_point(|(_, last)| *last < c);
                        ranges.get(i).is_some_and(|(first, _)| *first <= c)
                    })
            }),
        }
    }
}
impl Keyword for Pattern {
    fn is_valid(&self, value: &Value) -> bool {
        value.as_str().is_none_or(|s| self.matches(s))
    }
    fn validate<'i>(&self, value: &'i Value) -> Result<(), ValidationError<'i>> {
        if self.is_valid(value) {
            Ok(())
        } else {
            Err(ValidationError::custom("native HTTP pattern"))
        }
    }
}
