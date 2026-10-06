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
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if bytes.len() > max_head_bytes {
            return Err("request headers exceed limit".into());
        }
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request headers".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
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
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "malformed header".to_string())?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("host") {
            host = Some(value);
        } else if name.eq_ignore_ascii_case("content-length") {
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
    if content_length > MAX_BODY_BYTES {
        return Err("request body exceeds limit".into());
    }
    while bytes.len() - head_end < content_length {
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
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
    let url = if target.starts_with("http://") || target.starts_with("https://") {
        target.into()
    } else {
        let host = host.ok_or_else(|| "missing Host header".to_string())?;
        format!("http://{host}{target}")
    };
    let path = request_path(&url).to_owned();
    let envelope = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id,
        method: method.into(),
        url,
        headers,
        body_base64: STANDARD.encode(&bytes[head_end..]),
    };
    envelope.validate().map_err(|error| error.to_string())?;
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
}
