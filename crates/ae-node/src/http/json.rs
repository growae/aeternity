// Original aehttp_api_validate body decoding, fa2a6d7.
use super::response::Response;
use ae_codec::jsx;
use serde_json::{Value, json};

pub enum Body<'a> {
    Complete(&'a [u8]),
    More(&'a [u8]),
}
#[derive(Debug)]
pub enum Error {
    Validation {
        parameter: &'static str,
        info: Value,
    },
    Stream(u16),
    Capacity,
}
impl Error {
    pub(super) fn response(self) -> Response {
        match self {
            Self::Validation { parameter, info } => Response::json(
                400,
                json!({"reason":"validation_error","parameter":parameter,"info":info}),
            ),
            Self::Stream(code) => Response::stream_error(code),
            Self::Capacity => Response::unavailable(true),
        }
    }
}
pub(super) fn parse(body: Body<'_>, maximum: usize) -> Result<Value, Error> {
    let raw = match body {
        Body::Complete(raw) => raw,
        Body::More(raw) => {
            if raw.len() > maximum {
                return Err(Error::Capacity);
            }
            return Err(Error::Validation {
                parameter: "",
                info: json!({"error":"body_too_big","data":super::text::binary(raw)}),
            });
        }
    };
    if raw.is_empty() {
        return Ok(json!(""));
    }
    jsx::decode(
        raw,
        jsx::Limits {
            bytes: maximum,
            string_bytes: maximum.saturating_mul(3).min(64 << 20),
            values: 65536,
            ..Default::default()
        },
    )
    .map_err(|error| match error {
        jsx::Error::Parse => Error::Validation {
            parameter: "",
            info: json!({"error":"invalid_body","data":super::text::binary(raw)}),
        },
        jsx::Error::Limit(_) | jsx::Error::Limits => Error::Capacity,
    })
}
