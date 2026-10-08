// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Boundary 5 of the resident Workerd host: the request/response read/write
//! primitives shared by `examples/workerd-demo.rs` and `workerd-host`
//! (`src/main.rs`'s `cmd_workerd_host`).
//!
//! This module is an extraction, not a rewrite: every function here is
//! byte-for-byte the same parsing/writing logic that both call sites used to
//! duplicate. `workerd-demo` always calls these with
//! [`ConnectionMode::Close`], so its wire behavior is unchanged; only
//! `workerd-host`'s connection loop (boundary 4/5) uses
//! [`ConnectionMode::KeepAlive`] to add the new keep-alive behavior that is
//! out of scope for the unchanged demo.

use super::{Header, MAX_BODY_BYTES, PROTOCOL_VERSION, RequestEnvelope};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Whether a response declares the connection reusable for another request
/// (`keep-alive`) or to be closed after this response (`close`). Threads the
/// decided mode into the `Connection` response header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionMode {
    Close,
    KeepAlive,
}

impl ConnectionMode {
    fn header_value(self) -> &'static str {
        match self {
            ConnectionMode::Close => "close",
            ConnectionMode::KeepAlive => "keep-alive",
        }
    }
}

/// A request parsed off the wire, plus its already-extracted path (so
/// callers doing path-based routing don't re-parse the URL).
pub struct ParsedHttpRequest {
    pub envelope: RequestEnvelope,
    pub path: String,
}

/// Extracts the path component (no scheme/authority/query) from a request
/// URL already normalized to an absolute `http://`/`https://` form by
/// [`read_http_request`].
pub fn request_path(url: &str) -> &str {
    let authority_and_path = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = authority_and_path
        .find('/')
        .map_or("/", |index| &authority_and_path[index..]);
    path.split_once('?').map_or(path, |(path, _)| path)
}

/// Returns `true` if the parsed request's headers mean the connection can
/// stay open for another request, per HTTP/1.1 semantics: keep-alive unless
/// an explicit `Connection: close` header says otherwise.
pub fn wants_keep_alive(envelope: &RequestEnvelope) -> bool {
    !envelope.headers.iter().any(|header| {
        header.name.eq_ignore_ascii_case("connection") && header.value.eq_ignore_ascii_case("close")
    })
}

/// Reads one HTTP/1.1 request off `stream`: a request line, headers, and
/// (if `Content-Length` is present) exactly that many body bytes. Rejects
/// `Transfer-Encoding`, oversized heads/bodies, and anything but HTTP/1.1.
/// Does not consume bytes belonging to a next pipelined request.
pub fn read_http_request(
    stream: &mut TcpStream,
    request_id: String,
    max_head_bytes: usize,
) -> Result<ParsedHttpRequest, String> {
    read_http_request_with_control(stream, request_id, max_head_bytes, false)
}

/// Operator control JSON may be one bounded envelope, while actual buffered
/// guest requests remain subject to the original 32 KiB body limit.
pub fn read_http_request_with_control(
    stream: &mut TcpStream,
    request_id: String,
    max_head_bytes: usize,
    operator_control: bool,
) -> Result<ParsedHttpRequest, String> {
    let original_timeout = stream.read_timeout().map_err(|error| error.to_string())?;
    let budget = original_timeout.unwrap_or(std::time::Duration::from_secs(5));
    let deadline = std::time::Instant::now()
        .checked_add(budget)
        .ok_or("HTTP read timeout too large")?;
    let result = read_http_request_until(
        stream,
        request_id,
        max_head_bytes,
        operator_control,
        deadline,
    );
    stream
        .set_read_timeout(original_timeout)
        .map_err(|error| error.to_string())?;
    result
}

fn read_http_request_until(
    stream: &mut TcpStream,
    request_id: String,
    max_head_bytes: usize,
    operator_control: bool,
    deadline: std::time::Instant,
) -> Result<ParsedHttpRequest, String> {
    fn remaining(stream: &TcpStream, deadline: std::time::Instant) -> Result<(), String> {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err("HTTP request read deadline exceeded".into());
        }
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|error| error.to_string())
    }
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if bytes.len() >= max_head_bytes {
            return Err("request headers exceed limit".into());
        }
        remaining(stream, deadline)?;
        let count = stream.peek(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request headers".into());
        }
        let prefix = bytes.len().saturating_sub(3);
        let mut candidate = bytes[prefix..].to_vec();
        candidate.extend_from_slice(&chunk[..count]);
        let end = candidate.windows(4).position(|part| part == b"\r\n\r\n");
        let consume = end
            .map_or(count, |end| prefix + end + 4 - bytes.len())
            .min(max_head_bytes - bytes.len());
        stream
            .read_exact(&mut chunk[..consume])
            .map_err(|error| error.to_string())?;
        bytes.extend_from_slice(&chunk[..consume]);
        if let Some(end) = end {
            let end = prefix + end + 4;
            if end > max_head_bytes {
                return Err("request headers exceed limit".into());
            }
            break end;
        }
    };
    let head = std::str::from_utf8(&bytes[..head_end])
        .map_err(|_| "headers are not UTF-8")?
        .to_owned();
    let mut lines = head[..head.len() - 4].split("\r\n");
    let mut request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?
        .split_ascii_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| "missing method".to_string())?;
    let target = request_line
        .next()
        .ok_or_else(|| "missing request target".to_string())?;
    if request_line.next() != Some("HTTP/1.1") || request_line.next().is_some() {
        return Err("only HTTP/1.1 is supported".into());
    }
    let mut headers = Vec::new();
    let mut host = None;
    let mut content_length = 0usize;
    let mut seen_content_length = false;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "malformed header".to_string())?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("host") {
            if host.is_some() {
                return Err("duplicate Host header".into());
            }
            host = Some(value);
        } else if name.eq_ignore_ascii_case("content-length") {
            if seen_content_length {
                return Err("duplicate Content-Length header".into());
            }
            seen_content_length = true;
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("invalid Content-Length".into());
            }
            content_length = value
                .parse()
                .map_err(|_| "invalid Content-Length".to_string())?;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("Transfer-Encoding is not supported".into());
        }
        headers.push(Header {
            name: name.into(),
            value: value.into(),
        });
    }
    let url = if target.starts_with("http://") || target.starts_with("https://") {
        target.into()
    } else {
        let host = host.ok_or_else(|| "missing Host header".to_string())?;
        format!("http://{host}{target}")
    };
    let path = request_path(&url).to_owned();
    let control =
        operator_control && (path.starts_with("/v1/apps/") || path.starts_with("/v1/instances/"));
    let body_limit = if control {
        super::MAX_ENVELOPE_BYTES
    } else {
        MAX_BODY_BYTES
    };
    if content_length > body_limit {
        return Err("request body exceeds limit".into());
    }
    while bytes.len() - head_end < content_length {
        remaining(stream, deadline)?;
        let wanted = (content_length - (bytes.len() - head_end)).min(chunk.len());
        let count = stream
            .read(&mut chunk[..wanted])
            .map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request body".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() - head_end > content_length {
            return Err("bytes after request body".into());
        }
    }
    if bytes.len() - head_end != content_length {
        return Err("bytes after request body".into());
    }
    let mut envelope = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id,
        method: method.into(),
        url,
        headers,
        body_base64: String::new(),
    };
    envelope.validate().map_err(|error| error.to_string())?;
    envelope.body_base64 = STANDARD.encode(&bytes[head_end..]);
    if !control {
        envelope.validate().map_err(|error| error.to_string())?;
    }
    Ok(ParsedHttpRequest { envelope, path })
}

/// Maps an HTTP status code to its reason phrase. A superset of every
/// status either call site has ever written, so adding a new response
/// elsewhere never needs a matching addition here.
pub fn http_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Worker Response",
    }
}

/// Writes a complete response with a known, already-materialized body
/// (health/ready/status/pool-status endpoints, not a worker's own
/// response passthrough).
pub fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    connection: ConnectionMode,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: {}\r\n\r\n",
        http_reason(status),
        body.len(),
        connection.header_value(),
    )?;
    stream.write_all(body)
}

/// Writes a plain-text error response.
pub fn write_http_error(
    stream: &mut TcpStream,
    status: u16,
    message: &str,
    connection: ConnectionMode,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain\r\n\
         Content-Length: {}\r\nConnection: {}\r\n\r\n{message}",
        http_reason(status),
        message.len(),
        connection.header_value(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_path_strips_scheme_authority_and_query() {
        assert_eq!(request_path("http://host/a/b?q=1"), "/a/b");
        assert_eq!(request_path("http://host"), "/");
        assert_eq!(request_path("/already-a-path?x"), "/already-a-path");
    }

    #[test]
    fn keep_alive_is_the_http11_default() {
        let mut envelope = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: "t".into(),
            method: "GET".into(),
            url: "http://host/".into(),
            headers: Vec::new(),
            body_base64: String::new(),
        };
        assert!(wants_keep_alive(&envelope));
        envelope.headers.push(Header {
            name: "Connection".into(),
            value: "close".into(),
        });
        assert!(!wants_keep_alive(&envelope));
    }

    #[test]
    fn fragmented_headers_obey_one_absolute_read_deadline() {
        use std::net::TcpListener;
        use std::time::{Duration, Instant};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let sender = std::thread::spawn(move || {
            for byte in b"GET / HTTP/1.1\r\nHost: slow.test\r\n\r\n" {
                if client.write_all(&[*byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        let started = Instant::now();
        assert!(read_http_request(&mut server, "slow".into(), 16384).is_err());
        assert!(started.elapsed() < Duration::from_millis(300));
        drop(server);
        sender.join().unwrap();
    }
}
