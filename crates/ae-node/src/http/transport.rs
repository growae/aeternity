use super::{
    Config,
    response::{self, Headers, Operation, Response},
};
use crate::runtime;
use ae_sync::chain::{self, query::Current};
use serde_json::json;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
mod body;

struct Request {
    method: String,
    path: String,
    query: String,
    headers: Headers,
}
struct Read {
    request: Request,
    prefetched: Vec<u8>,
    http11: bool,
}
async fn read(stream: &mut TcpStream, config: &Config) -> Result<Read, u16> {
    let mut bytes = Vec::new();
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut request = httparse::Request::new(&mut headers);
        match request.parse(&bytes) {
            Ok(httparse::Status::Complete(end)) => {
                let method = request.method.ok_or(400u16)?.to_owned();
                let uri = request.path.ok_or(400u16)?;
                if !uri.starts_with('/') || uri.contains('#') {
                    return Err(400);
                }
                let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
                let path = path.to_owned();
                let mut fields = Headers::new();
                for header in request.headers {
                    let key = header.name.to_ascii_lowercase();
                    let value = std::str::from_utf8(header.value)
                        .map_err(|_| 400u16)?
                        .to_owned();
                    if let Some(previous) = fields.get_mut(&key) {
                        if matches!(
                            key.as_str(),
                            "host" | "content-length" | "transfer-encoding"
                        ) {
                            return Err(400);
                        }
                        previous.push_str(", ");
                        previous.push_str(&value);
                    } else {
                        fields.insert(key, value);
                    }
                }
                if request.version == Some(1) && fields.get("host").is_none_or(String::is_empty) {
                    return Err(400);
                }
                if fields.contains_key("transfer-encoding") {
                    if fields.contains_key("content-length") {
                        return Err(400);
                    }
                    if !fields["transfer-encoding"].eq_ignore_ascii_case("chunked") {
                        return Err(400);
                    }
                }
                if let Some(length) = fields.get("content-length") {
                    if length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(400);
                    }
                    let length = length.parse::<u64>().map_err(|_| 413u16)?;
                    if length > config.body_bytes as u64 {
                        return Err(413);
                    }
                }
                return Ok(Read {
                    request: Request {
                        method,
                        path,
                        query: query.into(),
                        headers: fields,
                    },
                    prefetched: bytes[end..].to_vec(),
                    http11: request.version == Some(1),
                });
            }
            Ok(httparse::Status::Partial) => (),
            Err(httparse::Error::TooManyHeaders) => return Err(431),
            Err(_) => return Err(400),
        }
        if bytes.len() == config.header_bytes {
            return Err(431);
        }
        let mut chunk = [0; 2048];
        let count = (config.header_bytes - bytes.len()).min(chunk.len());
        let n = stream.read(&mut chunk[..count]).await.map_err(|_| 400u16)?;
        if n == 0 {
            return Err(400);
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
}
pub(super) fn path_segments(path: &str) -> Option<Vec<Vec<u8>>> {
    // Cowboy splits before percent-decoding: an encoded slash remains inside
    // one binding. A trailing separator is ignored, interior empty segments
    // remain, and decoded dot segments are removed before route matching.
    let mut raw: Vec<_> = path.strip_prefix('/')?.split('/').collect();
    if raw.last() == Some(&"") {
        raw.pop();
    }
    let mut parts = Vec::new();
    for segment in raw {
        let bytes = decode_bytes(segment, false)?;
        match bytes.as_slice() {
            b"." => (),
            b".." => {
                parts.pop();
            }
            // Preserve bindings exactly; JSON cleaning belongs at emission.
            _ => parts.push(bytes),
        }
    }
    Some(parts)
}
fn decode_bytes(value: &str, plus: bool) -> Option<Vec<u8>> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'%' => {
                let high = char::from(bytes.next()?).to_digit(16)?;
                let low = char::from(bytes.next()?).to_digit(16)?;
                decoded.push((high * 16 + low) as u8);
            }
            b'+' if plus => decoded.push(b' '),
            b => decoded.push(b),
        }
    }
    Some(decoded)
}
pub(super) fn strings(query: &str) -> Result<bool, Response> {
    match query_value(query, b"int-as-string")? {
        None => Ok(false),
        Some(None) => Ok(true),
        Some(Some(value)) if value.eq_ignore_ascii_case(b"true") => Ok(true),
        Some(Some(value)) if value.eq_ignore_ascii_case(b"false") => Ok(false),
        Some(Some(_)) => Err(Response::stream_error(500)),
    }
}
pub(super) fn query_value(query: &str, name: &[u8]) -> Result<Option<Option<Vec<u8>>>, Response> {
    let mut selected = None;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair
            .split_once('=')
            .map_or((pair, None), |(k, v)| (k, Some(v)));
        if key.is_empty() {
            return Err(Response::stream_error(400));
        }
        let key = decode_bytes(key, true).ok_or_else(|| Response::stream_error(400))?;
        let value = match value {
            Some(v) => Some(decode_bytes(v, true).ok_or_else(|| Response::stream_error(400))?),
            None => None,
        };
        if key == name && selected.is_none() {
            selected = Some(value);
        }
    }
    Ok(selected)
}
fn accepts_json(headers: &Headers) -> bool {
    let Some(value) = headers.get("accept") else {
        return true;
    };
    let mut selected: Option<(u8, u16)> = None;
    for part in value.split(',') {
        let mut fields = part.trim().split(';');
        let media = fields.next().unwrap_or_default().trim();
        let specificity = if media.eq_ignore_ascii_case("application/json") {
            2
        } else if media.eq_ignore_ascii_case("application/*") {
            1
        } else if media == "*/*" {
            0
        } else {
            continue;
        };
        let mut weight = 1000;
        let mut matched = true;
        let mut weighted = false;
        for field in fields {
            let Some((key, value)) = field.trim().split_once('=') else {
                matched = false;
                break;
            };
            if key.eq_ignore_ascii_case("q") {
                weighted = true;
                weight = quality(value).unwrap_or(0);
            } else if !weighted {
                matched = false;
            }
        }
        if matched
            && selected.is_none_or(|(s, w)| specificity > s || (specificity == s && weight > w))
        {
            selected = Some((specificity, weight));
        }
    }
    selected.is_some_and(|(_, weight)| weight > 0)
}
fn quality(value: &str) -> Option<u16> {
    let (integer, decimal) = value.trim().split_once('.').unwrap_or((value.trim(), ""));
    if decimal.len() > 3 || !decimal.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match integer {
        "1" if decimal.bytes().all(|b| b == b'0') => Some(1000),
        "0" => {
            let mut weight = 0;
            let mut place = 100;
            for b in decimal.bytes() {
                weight += u16::from(b - b'0') * place;
                place /= 10;
            }
            Some(weight)
        }
        _ => None,
    }
}
async fn selected(
    parent: &runtime::Handle,
    config: &Config,
    captured: Option<&Arc<chain::Importer>>,
) -> Result<Current, Response> {
    if !parent.status().ready {
        return Err(Response::unavailable(false));
    }
    let importer = captured.ok_or_else(|| Response::unavailable(false))?;
    let read = async {
        let value = importer.current()?.wait().await?;
        if !parent.status().ready
            || parent
                .importer()
                .is_none_or(|live| !Arc::ptr_eq(&live, importer))
        {
            return Err(chain::Error::Stale);
        }
        Ok(value)
    };
    match tokio::time::timeout(config.timeout, read).await {
        Ok(Ok(current)) => Ok(current),
        Ok(Err(error)) => Err(failure(error)),
        Err(_) => Err(failure(chain::Error::Timeout)),
    }
}
pub(super) fn failure(error: chain::Error) -> Response {
    match error {
        chain::Error::Busy => Response::unavailable(true),
        chain::Error::Storage(_) => Response::json(500, json!({"reason":"Internal server error"})),
        _ => Response::unavailable(false),
    }
}
fn plan(request: &Request, config: &Config) -> Result<(Operation, bool), Response> {
    if request.method == "OPTIONS" {
        return Err(Response::empty(200));
    }
    if path_segments(&request.path).is_none() {
        return Err(Response::empty(400));
    }
    let Some((v3, mut operation)) = response::route(&request.path, config) else {
        return Err(Response::empty(404));
    };
    let method = if matches!(operation, Operation::PostTransaction { .. }) {
        "POST"
    } else {
        "GET"
    };
    if !matches!(
        request.method.as_str(),
        "GET" | "HEAD" | "POST" | "PUT" | "DELETE" | "CONNECT" | "OPTIONS" | "TRACE" | "PATCH"
    ) {
        return Err(Response::empty(501));
    }
    if request.method != method {
        let mut response = Response::empty(405);
        response.headers.insert("allow".into(), method.into());
        return Err(response);
    }
    if !accepts_json(&request.headers) {
        return Err(Response::empty(406));
    }
    if matches!(operation, Operation::PostTransaction { .. }) {
        body::validate_headers(&request.headers)?;
    }
    let strings = if v3
        && !matches!(
            operation,
            Operation::SyncStatus { .. } | Operation::PostTransaction { .. }
        ) {
        strings(&request.query)?
    } else {
        false
    };
    if let Operation::Blocks { operation, v3 } = &operation {
        operation.validate_path(*v3)?;
    }
    if let Operation::Transactions { operation, .. } = &operation {
        operation.validate_path()?;
    }
    if let Operation::Accounts { operation, v3 } = &mut operation {
        operation.validate(*v3, &request.query)?;
    }
    Ok((operation, strings))
}
pub(super) fn dispatch(
    plan: Result<(Operation, bool), Response>,
    result: Result<serde_json::Value, Response>,
) -> Response {
    match (plan, result) {
        (Ok((Operation::Contracts { operation, v3 }, strings)), Ok(body)) => {
            super::contracts::finish(&operation, body, v3, strings)
        }
        (Ok((Operation::Accounts { operation, v3 }, strings)), Ok(body)) => {
            super::accounts::finish(&operation, body, v3, strings)
        }
        (Err(response), _) | (_, Err(response)) => response,
        (Ok((Operation::Transactions { operation, v3 }, strings)), Ok(body)) => {
            super::transactions::finish(&operation, body, v3, strings)
        }
        (Ok((Operation::SyncStatus { v3 }, _)), Ok(body)) => {
            if super::sync::valid_response(&body, v3) {
                Response::json(200, body)
            } else {
                Response::invalid_response()
            }
        }
        (Ok((Operation::Status { v3 }, strings)), Ok(mut body)) => {
            if strings {
                response::integers_as_strings(&mut body);
            }
            if super::status::valid_response(&body, v3) {
                Response::json(200, body)
            } else {
                Response::invalid_response()
            }
        }
        (
            Ok((
                Operation::Current { .. }
                | Operation::PeerPublicKey
                | Operation::Blocks { .. }
                | Operation::PendingKey
                | Operation::PostTransaction { .. },
                strings,
            )),
            Ok(mut body),
        ) => {
            if strings {
                response::integers_as_strings(&mut body);
            }
            Response::json(200, body)
        }
    }
}
pub(super) async fn exchange(
    mut stream: TcpStream,
    parent: runtime::Handle,
    config: Arc<Config>,
    posts: super::post::Service,
) {
    let request = match tokio::time::timeout(config.timeout, read(&mut stream, &config)).await {
        Ok(result) => result,
        Err(_) => Err(408),
    };
    // Capture one owner before the header marker. Cached height does not need
    // a read-work slot, so preflight and overload responses keep their marker.
    let captured = parent.importer().filter(|_| parent.status().ready);
    let mut height = captured.as_ref().and_then(|i| i.cached_height());
    let (plan, headers, prefetched, query, http11) = match request {
        Ok(Read {
            request,
            prefetched,
            http11,
        }) => (
            plan(&request, &config),
            request.headers,
            prefetched,
            request.query,
            http11,
        ),
        Err(status) => (
            Err(Response::empty(status)),
            Headers::new(),
            vec![],
            String::new(),
            false,
        ),
    };
    let selected = if matches!(plan, Ok((Operation::Current { .. }, _))) || height.is_none() {
        selected(&parent, &config, captured.as_ref()).await
    } else {
        Err(Response::unavailable(false))
    };
    if height.is_none() {
        height = selected.as_ref().ok().map(|s| s.height);
    }
    let mut retained = None;
    let mut rendered = None;
    let mut post_credits = None;
    let mut pending_candidate = None;
    let mut account_pool_credits = None;
    let result = match &plan {
        Ok((Operation::Contracts { operation, v3 }, strings)) => {
            match super::contracts::query(
                &parent,
                &config,
                captured.as_ref(),
                operation,
                *v3,
                *strings,
            )
            .await
            {
                Ok(reply) => {
                    retained = Some(reply.credits);
                    rendered = Some(reply.value);
                    Err(Response::unavailable(false))
                }
                Err(response) => Err(response),
            }
        }
        Ok((Operation::Accounts { operation, v3 }, strings)) => {
            match super::accounts::query(
                &parent,
                &config,
                captured.as_ref(),
                operation,
                *v3,
                *strings,
            )
            .await
            {
                Ok(reply) => {
                    retained = Some(reply.credits);
                    account_pool_credits = reply.value.pool_credits;
                    rendered = Some(reply.value.value);
                    Err(Response::unavailable(false))
                }
                Err(response) => Err(response),
            }
        }
        Ok((Operation::PendingKey, _)) => {
            match super::pending::query(&parent, &config, captured.as_ref()).await {
                Ok(reply) => {
                    retained = Some(reply.credits);
                    pending_candidate = Some(reply.candidate);
                    Ok(reply.value)
                }
                Err(response) => Err(response),
            }
        }
        Ok((Operation::PostTransaction { v3 }, _)) => {
            match post(
                &mut stream,
                &headers,
                prefetched,
                &posts,
                parent.clone(),
                &config,
                (*v3, query, http11),
            )
            .await
            {
                Ok(reply) => {
                    post_credits = Some(reply.credits);
                    rendered = Some(reply.value);
                    Err(Response::unavailable(false))
                }
                Err(response) => Err(response),
            }
        }
        Ok((Operation::Transactions { operation, v3 }, strings)) => {
            match super::transactions::query(
                &parent,
                &config,
                captured.as_ref(),
                operation,
                *v3,
                *strings,
            )
            .await
            {
                Ok(reply) => {
                    retained = Some(reply.credits);
                    rendered = Some(reply.value);
                    Err(Response::unavailable(false))
                }
                Err(response) => Err(response),
            }
        }
        Ok((Operation::Current { height }, _)) => selected.map(|s| response::current(s, *height)),
        Ok((Operation::SyncStatus { .. }, _)) => super::sync::query(&parent, &config).await,
        Ok((Operation::PeerPublicKey, _)) => super::key::query(&parent, &config).await,
        Ok((Operation::Status { .. }, _)) => super::status::query(&parent, &config).await,
        Ok((Operation::Blocks { operation, v3 }, _)) => {
            super::blocks::query(&parent, &config, captured.as_ref(), operation, *v3).await
        }
        Err(_) => Err(Response::unavailable(false)),
    };
    let mut response = rendered.unwrap_or_else(|| dispatch(plan, result));
    response.stamp(&config, &headers, height);
    let written = matches!(
        tokio::time::timeout(config.timeout, write(&mut stream, response)).await,
        Ok(Ok(()))
    );
    drop(retained);
    drop(post_credits);
    drop(pending_candidate);
    drop(account_pool_credits);
    if written {
        // Half-close the response first. Bounded draining gives the client a
        // chance to receive the error before unread request bytes cause a TCP
        // reset (notably a header rejected at the configured byte limit).
        let _ = tokio::time::timeout(
            config.timeout.min(std::time::Duration::from_millis(100)),
            async {
                let mut remaining = config.header_bytes + config.body_bytes;
                let mut buffer = [0; 2048];
                while remaining > 0 {
                    let size = remaining.min(buffer.len());
                    match stream.read(&mut buffer[..size]).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => remaining -= n,
                    }
                }
            },
        )
        .await;
    }
}
async fn post(
    stream: &mut TcpStream,
    headers: &Headers,
    prefetched: Vec<u8>,
    posts: &super::post::Service,
    parent: runtime::Handle,
    config: &Config,
    options: (bool, String, bool),
) -> Result<ae_sync::blocking::Retained<Response>, Response> {
    let (v3, query, http11) = options;
    let reservation = posts.reserve(config)?;
    if http11
        && headers
            .get("expect")
            .is_some_and(|e| e.eq_ignore_ascii_case("100-continue"))
    {
        tokio::time::timeout(
            config.timeout,
            stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n"),
        )
        .await
        .map_err(|_| Response::empty(408))?
        .map_err(|_| Response::empty(400))?;
    }
    let body = body::read(stream, headers, prefetched, config).await?;
    super::post::submit(posts, reservation, parent, config, body, (v3, query)).await
}
async fn write(stream: &mut TcpStream, mut response: Response) -> std::io::Result<()> {
    let phrase = match response.status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        417 => "Expectation Failed",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Error",
    };
    response.headers.insert("connection".into(), "close".into());
    response
        .headers
        .insert("content-length".into(), response.body.len().to_string());
    let mut header = format!("HTTP/1.1 {} {phrase}\r\n", response.status);
    for (name, value) in response.headers {
        header.push_str(&format!("{name}: {value}\r\n"));
    }
    header.push_str("\r\n");
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests;
