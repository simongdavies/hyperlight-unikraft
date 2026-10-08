// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Bounded HTTP/SSE and WebSocket edge for the negotiated guest stream.
use super::{
    Error, FrameKind, HostIngress, InvocationExecution, InvocationResponse, RequestEnvelope,
    Result, StreamFrame,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::Duration;
use tungstenite::{Message, WebSocket, protocol::Role};

enum Socket {
    Http(TcpStream),
    Websocket(Box<WebSocket<TcpStream>>),
}

/// Holds both client transport and guest completion until CallDone; a final
/// response frame never releases the invocation's VM admission by itself.
pub fn pump_http_stream(
    stream: TcpStream,
    request: &RequestEnvelope,
    body: &[u8],
    mut ingress: HostIngress,
    completion: mpsc::Receiver<InvocationExecution>,
) -> Result<()> {
    let mut error_stream = stream.try_clone()?;
    let mut headers_started = false;
    let mut result = pump_http_stream_inner(
        stream,
        request,
        body,
        &mut ingress,
        &completion,
        &mut headers_started,
    );
    if result.is_err() {
        ingress.cancel();
        match completion.recv_timeout(Duration::from_secs(2)) {
            Ok(execution) => {
                if let Err(error) = execution.result {
                    tracing::debug!(%error, "stream guest termination outcome");
                    if matches!(result, Err(Error::Cancelled)) {
                        result = Err(error);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(Error::State(
                    "stream transport failed and cancelled guest termination was not reconciled"
                        .into(),
                ));
            }
        }
        if !headers_started {
            let (status, message) = match &result {
                Err(Error::Fence(_)) => (409, "stale instance generation"),
                Err(Error::NotReady(_)) => (503, "instance is not ready"),
                Err(Error::Timeout) => (504, "streaming invocation timed out"),
                _ => (502, "streaming invocation failed"),
            };
            super::write_http_error(
                &mut error_stream,
                status,
                message,
                super::ConnectionMode::Close,
            )?;
        }
    }
    result
}

fn pump_http_stream_inner(
    stream: TcpStream,
    request: &RequestEnvelope,
    body: &[u8],
    ingress: &mut HostIngress,
    completion: &mpsc::Receiver<InvocationExecution>,
    headers_started: &mut bool,
) -> Result<()> {
    let mut socket = Socket::Http(stream);
    let mut body_offset = 0;
    let mut body_ended = websocket_requested(request);
    let mut response_ended = false;
    let mut response_has_body = true;
    let mut pending_ws: Option<(Vec<u8>, u8)> = None;
    let mut websocket_closed = false;
    let mut guest_close_sent = false;
    let mut completed: Option<Result<InvocationResponse>> = None;
    loop {
        let remaining = ingress.remaining();
        if remaining.is_zero() {
            ingress.cancel();
            return Err(Error::Timeout);
        }
        let socket_ref = match &socket {
            Socket::Http(stream) => stream,
            Socket::Websocket(websocket) => websocket.get_ref(),
        };
        socket_ref.set_write_timeout(Some(remaining.min(Duration::from_secs(1))))?;
        if !response_ended
            && (matches!(&socket, Socket::Http(_)) || (websocket_closed && !guest_close_sent))
        {
            socket_ref.set_read_timeout(Some(Duration::from_millis(1)))?;
            let mut byte = [0];
            match socket_ref.peek(&mut byte) {
                Ok(0) => {
                    ingress.cancel();
                    return Err(Error::Cancelled);
                }
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                    ) => {}
                Err(error) => {
                    ingress.cancel();
                    return Err(error.into());
                }
            }
        }
        if !body_ended && !ingress.is_completed() {
            if body_offset < body.len() {
                let end = (body_offset + super::MAX_FRAME_BYTES).min(body.len());
                if ingress.try_send(FrameKind::Data, &body[body_offset..end], None)? {
                    body_offset = end;
                }
            } else if ingress.try_send(FrameKind::End, &[], None)? {
                body_ended = true;
            }
        }
        if !ingress.is_completed()
            && let Some((bytes, opcode)) = &pending_ws
        {
            if ingress.try_send(FrameKind::Websocket, bytes, Some(*opcode))? {
                pending_ws = None;
            }
        } else if body_ended
            && !response_ended
            && !websocket_closed
            && !ingress.is_completed()
            && let Socket::Websocket(websocket) = &mut socket
        {
            match websocket.read() {
                Ok(Message::Text(text)) => pending_ws = Some((text.as_bytes().to_vec(), 1)),
                Ok(Message::Binary(bytes)) => pending_ws = Some((bytes.to_vec(), 2)),
                Ok(Message::Close(close)) => {
                    websocket_closed = true;
                    let mut bytes = Vec::new();
                    if let Some(close) = close {
                        bytes.extend_from_slice(&u16::from(close.code).to_be_bytes());
                        bytes.extend_from_slice(close.reason.as_bytes());
                    } else {
                        bytes.extend_from_slice(&1000u16.to_be_bytes());
                    }
                    pending_ws = Some((bytes, 8));
                    // The guest, not an automatic host acknowledgement, closes its half.
                }
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Err(tungstenite::Error::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => {
                    ingress.cancel();
                    return Err(Error::State(format!(
                        "WebSocket client disconnected:{error}"
                    )));
                }
            }
        }
        match ingress.receive(Duration::from_millis(1)) {
            Ok(Some(frame)) => match frame.kind {
                FrameKind::Headers => {
                    *headers_started = true;
                    response_has_body = http_response_has_body(
                        request,
                        frame
                            .status
                            .ok_or_else(|| Error::Protocol("stream status missing".into()))?,
                    );
                    socket = write_headers(socket, request, &frame)?;
                }
                FrameKind::Data => {
                    let Socket::Http(stream) = &mut socket else {
                        return Err(Error::Protocol("HTTP data on upgraded socket".into()));
                    };
                    let bytes = frame.decoded_body()?;
                    if response_has_body && !bytes.is_empty() {
                        write!(stream, "{:x}\r\n", bytes.len())?;
                        stream.write_all(&bytes)?;
                        stream.write_all(b"\r\n")?;
                        stream.flush()?;
                    }
                }
                FrameKind::Websocket => {
                    let Socket::Websocket(websocket) = &mut socket else {
                        return Err(Error::Protocol("WebSocket data before upgrade".into()));
                    };
                    let bytes = frame.decoded_body()?;
                    let message = match frame.opcode {
                        Some(1) => Message::Text(
                            String::from_utf8(bytes)
                                .map_err(|error| Error::Protocol(error.to_string()))?
                                .into(),
                        ),
                        Some(2) => Message::Binary(bytes.into()),
                        Some(8) => {
                            let code = u16::from_be_bytes([bytes[0], bytes[1]]);
                            let reason = std::str::from_utf8(&bytes[2..])
                                .map_err(|error| Error::Protocol(error.to_string()))?
                                .to_string();
                            Message::Close(Some(tungstenite::protocol::CloseFrame {
                                code: code.into(),
                                reason: reason.into(),
                            }))
                        }
                        _ => return Err(Error::Protocol("unsupported outbound opcode".into())),
                    };
                    let closing = frame.opcode == Some(8);
                    let sent = if websocket_closed && let Message::Close(close) = message {
                        tungstenite::protocol::frame::Frame::close(close)
                            .format(websocket.get_mut())
                    } else {
                        websocket.send(message)
                    };
                    match sent {
                        Ok(()) => {}
                        Err(
                            tungstenite::Error::ConnectionClosed
                            | tungstenite::Error::AlreadyClosed,
                        ) if closing && websocket_closed => {}
                        Err(error) => {
                            return Err(Error::State(format!("WebSocket write failed:{error}")));
                        }
                    }
                    websocket.get_mut().flush()?;
                    guest_close_sent |= closing;
                }
                FrameKind::End => {
                    if response_has_body && let Socket::Http(stream) = &mut socket {
                        stream.write_all(b"0\r\n\r\n")?;
                        stream.flush()?;
                    }
                    response_ended = true;
                }
                FrameKind::Error => {
                    ingress.cancel();
                    return Err(Error::State(format!(
                        "guest stream failed:{}",
                        frame.error.as_deref().unwrap_or("missing error")
                    )));
                }
                FrameKind::Cancel | FrameKind::Pending => {
                    return Err(Error::Protocol(
                        "unexpected guest transport control frame".into(),
                    ));
                }
            },
            Ok(None) => {}
            Err(error) => {
                ingress.cancel();
                return Err(error);
            }
        }
        if completed.is_none() {
            match completion.try_recv() {
                Ok(execution) => completed = Some(execution.result),
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    ingress.cancel();
                    return Err(Error::State("stream owner lost before completion".into()));
                }
            }
        }
        if let Some(Err(_)) = &completed {
            return completed.take().expect("completion checked").map(|_| ());
        }
        if response_ended && let Some(result) = completed.take() {
            return match result? {
                InvocationResponse::StreamComplete => Ok(()),
                _ => Err(Error::Protocol("stream returned buffered response".into())),
            };
        }
    }
}

fn http_response_has_body(request: &RequestEnvelope, status: u16) -> bool {
    !request.method.eq_ignore_ascii_case("HEAD") && !matches!(status, 100..=199 | 204 | 304)
}

fn write_headers(socket: Socket, request: &RequestEnvelope, frame: &StreamFrame) -> Result<Socket> {
    let Socket::Http(mut stream) = socket else {
        return Err(Error::Protocol(
            "duplicate response headers after upgrade".into(),
        ));
    };
    let status = frame
        .status
        .ok_or_else(|| Error::Protocol("stream status missing".into()))?;
    let headers = frame
        .headers
        .as_deref()
        .ok_or_else(|| Error::Protocol("stream headers missing".into()))?;
    if status == 101 {
        let mut builder = tungstenite::handshake::server::Request::builder()
            .method(request.method.as_str())
            .uri(request.url.as_str());
        for header in &request.headers {
            builder = builder.header(&header.name, &header.value);
        }
        let handshake = builder
            .body(())
            .map_err(|error| Error::Protocol(error.to_string()))?;
        let response = tungstenite::handshake::server::create_response(&handshake)
            .map_err(|error| Error::Protocol(format!("invalid WebSocket handshake:{error}")))?;
        stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\n")?;
        for (name, value) in response.headers() {
            write!(
                stream,
                "{}: {}\r\n",
                name.as_str(),
                value
                    .to_str()
                    .map_err(|error| Error::Protocol(error.to_string()))?
            )?;
        }
        for header in headers {
            let name = header.name.to_ascii_lowercase();
            if matches!(
                name.as_str(),
                "connection"
                    | "upgrade"
                    | "sec-websocket-accept"
                    | "content-length"
                    | "transfer-encoding"
            ) {
                continue;
            }
            if name == "sec-websocket-protocol" {
                let offered = request
                    .headers
                    .iter()
                    .filter(|header| header.name.eq_ignore_ascii_case("sec-websocket-protocol"))
                    .flat_map(|header| header.value.split(',').map(str::trim))
                    .any(|protocol| protocol == header.value);
                if !offered {
                    return Err(Error::Protocol(
                        "guest selected an unoffered WebSocket subprotocol".into(),
                    ));
                }
            }
            write!(stream, "{}: {}\r\n", header.name, header.value)?;
        }
        stream.write_all(b"\r\n")?;
        stream.flush()?;
        stream.set_read_timeout(Some(Duration::from_millis(1)))?;
        let mut config = tungstenite::protocol::WebSocketConfig::default();
        config.write_buffer_size = 0;
        config.max_write_buffer_size = 2 * super::MAX_FRAME_BYTES + 1024;
        config.max_message_size = Some(super::MAX_FRAME_BYTES);
        config.max_frame_size = Some(super::MAX_FRAME_BYTES);
        config.accept_unmasked_frames = false;
        Ok(Socket::Websocket(Box::new(WebSocket::from_raw_socket(
            stream,
            Role::Server,
            Some(config),
        ))))
    } else {
        let has_body = http_response_has_body(request, status);
        write!(
            stream,
            "HTTP/1.1 {status} {}\r\n",
            super::http_reason(status)
        )?;
        for header in headers {
            if header.name.eq_ignore_ascii_case("content-length")
                && !has_body
                && (request.method.eq_ignore_ascii_case("HEAD") || status == 304)
            {
                header.value.parse::<u64>().map_err(|_| {
                    Error::Protocol("invalid bodyless response content-length".into())
                })?;
                write!(stream, "{}: {}\r\n", header.name, header.value)?;
                continue;
            }
            if !matches!(
                header.name.to_ascii_lowercase().as_str(),
                "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "keep-alive"
                    | "upgrade"
                    | "trailer"
                    | "proxy-authenticate"
                    | "proxy-authorization"
            ) {
                write!(stream, "{}: {}\r\n", header.name, header.value)?;
            }
        }
        if has_body {
            stream.write_all(b"Transfer-Encoding: chunked\r\n")?;
        }
        stream.write_all(b"Connection: close\r\n\r\n")?;
        stream.flush()?;
        Ok(Socket::Http(stream))
    }
}

pub fn websocket_requested(request: &RequestEnvelope) -> bool {
    request.headers.iter().any(|header| {
        header.name.eq_ignore_ascii_case("upgrade")
            && header.value.eq_ignore_ascii_case("websocket")
    })
}

pub fn decode_buffered_input(request: &RequestEnvelope) -> Result<Vec<u8>> {
    STANDARD
        .decode(&request.body_base64)
        .map_err(|error| Error::Protocol(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::InvocationCancellation;
    use super::*;
    use std::io::{BufRead, BufReader, Read};
    use std::net::TcpListener;
    use std::thread;

    fn send(
        guest: &super::super::GuestIngress,
        sequence: u64,
        kind: &str,
        extra: serde_json::Value,
    ) {
        let mut frame = serde_json::json!({"protocol_version":1,"request_id":"transport-1","sequence":sequence,"kind":kind});
        for (key, value) in extra.as_object().unwrap() {
            frame[key] = value.clone();
        }
        let ack: serde_json::Value =
            serde_json::from_str(&guest.send(&frame.to_string()).unwrap()).unwrap();
        assert_eq!(ack["accepted"], true, "{ack}");
    }

    #[test]
    fn completed_guest_input_does_not_discard_responses_or_frame_bodyless_http() {
        for (method, status, body) in [
            ("GET", 200, b"hello".as_slice()),
            ("PATCH", 204, b"".as_slice()),
            ("HEAD", 200, b"hello".as_slice()),
            ("GET", 304, b"".as_slice()),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let (server, _) = listener.accept().unwrap();
            let request = RequestEnvelope {
                protocol_version: 1,
                request_id: "transport-1".into(),
                method: method.into(),
                url: "https://example.test/".into(),
                headers: vec![],
                body_base64: String::new(),
            };
            let (host, guest) = HostIngress::pair(
                "transport-1",
                false,
                Duration::from_secs(5),
                InvocationCancellation::default(),
            )
            .unwrap();
            send(
                &guest,
                0,
                "headers",
                serde_json::json!({"status":status,"headers":[{"name":"content-length","value":body.len().to_string()}]}),
            );
            let mut sequence = 1;
            if !body.is_empty() {
                send(
                    &guest,
                    sequence,
                    "data",
                    serde_json::json!({"body_base64":STANDARD.encode(body)}),
                );
                sequence += 1;
            }
            send(&guest, sequence, "end", serde_json::json!({}));
            guest.finish(true).unwrap();
            let (done, completion) = mpsc::channel();
            done.send(InvocationExecution {
                request_id: "transport-1".into(),
                result: Ok(InvocationResponse::StreamComplete),
                profile: Default::default(),
                submit_error: None,
            })
            .unwrap();
            pump_http_stream(server, &request, b"unconsumed input", host, completion).unwrap();
            let mut response = String::new();
            client.read_to_string(&mut response).unwrap();
            let (head, actual_body) = response.split_once("\r\n\r\n").unwrap();
            assert!(head.starts_with(&format!("HTTP/1.1 {status} ")));
            if http_response_has_body(&request, status) {
                assert!(head.contains("Transfer-Encoding: chunked"));
                assert_eq!(actual_body, "5\r\nhello\r\n0\r\n\r\n");
            } else {
                assert!(!head.contains("Transfer-Encoding"));
                assert!(actual_body.is_empty());
                if status == 204 {
                    assert!(!head.to_ascii_lowercase().contains("content-length"));
                } else {
                    assert!(head.contains(&format!("content-length: {}", body.len())));
                }
            }
        }
    }

    #[test]
    fn sse_delivers_progressively_and_retains_transport_until_drained_completion() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client =
            BufReader::new(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
        client
            .get_mut()
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();
        let request = RequestEnvelope {
            protocol_version: 1,
            request_id: "transport-1".into(),
            method: "GET".into(),
            url: "https://example.test/".into(),
            headers: vec![],
            body_base64: String::new(),
        };
        let (host, guest) = HostIngress::pair(
            "transport-1",
            false,
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .unwrap();
        let (completion_tx, completion_rx) = mpsc::channel();
        let (drain_tx, drain_rx) = mpsc::channel();
        let pump =
            thread::spawn(move || pump_http_stream(server, &request, &[], host, completion_rx));
        send(
            &guest,
            0,
            "headers",
            serde_json::json!({"status":200,"headers":[{"name":"content-type","value":"text/event-stream"}]}),
        );
        send(
            &guest,
            1,
            "data",
            serde_json::json!({"body_base64":STANDARD.encode(b"data: first\n\n")}),
        );
        let mut head = String::new();
        loop {
            let mut line = String::new();
            client.read_line(&mut line).unwrap();
            head.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        assert!(head.contains("text/event-stream"));
        let mut line = String::new();
        client.read_line(&mut line).unwrap();
        let length = usize::from_str_radix(line.trim(), 16).unwrap();
        let mut bytes = vec![0; length + 2];
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes[..length], b"data: first\n\n");
        send(&guest, 2, "end", serde_json::json!({}));
        line.clear();
        client.read_line(&mut line).unwrap();
        assert_eq!(line, "0\r\n");
        line.clear();
        client.read_line(&mut line).unwrap();
        assert_eq!(line, "\r\n");
        drop(client);
        let owner = thread::spawn(move || {
            drain_rx.recv().unwrap();
            guest.finish(true).unwrap();
            completion_tx
                .send(InvocationExecution {
                    request_id: "transport-1".into(),
                    result: Ok(InvocationResponse::StreamComplete),
                    profile: Default::default(),
                    submit_error: None,
                })
                .unwrap();
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            !pump.is_finished(),
            "endframe must not release transport/VM beforetracked drain"
        );
        drain_tx.send(()).unwrap();
        assert!(pump.join().unwrap().is_ok());
        owner.join().unwrap();
    }

    #[test]
    fn early_stream_fence_readiness_and_deadline_preserve_http_status() {
        for (error, status) in [
            (Error::Fence("stale generation".into()), 409),
            (Error::NotReady("draining".into()), 503),
            (Error::Timeout, 504),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let (server, _) = listener.accept().unwrap();
            let request = RequestEnvelope {
                protocol_version: 1,
                request_id: "transport-1".into(),
                method: "GET".into(),
                url: "https://example.test/".into(),
                headers: vec![],
                body_base64: String::new(),
            };
            let (host, guest) = HostIngress::pair(
                "transport-1",
                false,
                Duration::from_secs(5),
                InvocationCancellation::default(),
            )
            .unwrap();
            guest.abort();
            let (done, completion) = mpsc::channel();
            done.send(InvocationExecution {
                request_id: "transport-1".into(),
                result: Err(error),
                profile: Default::default(),
                submit_error: None,
            })
            .unwrap();
            assert!(pump_http_stream(server, &request, &[], host, completion).is_err());
            let mut response = String::new();
            client.read_to_string(&mut response).unwrap();
            assert!(
                response.starts_with(&format!("HTTP/1.1 {status} ")),
                "{response}"
            );
        }
    }

    #[test]
    fn guest_failure_is_not_masked_by_its_transport_cancellation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let request = RequestEnvelope {
            protocol_version: 1,
            request_id: "transport-1".into(),
            method: "GET".into(),
            url: "https://example.test/".into(),
            headers: vec![],
            body_base64: String::new(),
        };
        let (host, guest) = HostIngress::pair(
            "transport-1",
            false,
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .unwrap();
        guest.abort();
        let (done, completion) = mpsc::channel();
        done.send(InvocationExecution {
            request_id: "transport-1".into(),
            result: Err(Error::State("guest completion failed".into())),
            profile: Default::default(),
            submit_error: None,
        })
        .unwrap();
        assert!(matches!(
            pump_http_stream(server, &request, &[], host, completion),
            Err(Error::State(message)) if message == "guest completion failed"
        ));
    }

    #[test]
    fn websocket_close_waits_for_guest_reply_and_cancels_half_open_eof() {
        for graceful in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let (waiting, no_ack) = mpsc::channel();
            let client = thread::spawn(move || {
                let stream = TcpStream::connect(address).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let (mut socket, _) =
                    tungstenite::client(format!("ws://{address}/"), stream).unwrap();
                socket
                    .send(Message::Close(Some(tungstenite::protocol::CloseFrame {
                        code: 1000.into(),
                        reason: "peer".into(),
                    })))
                    .unwrap();
                socket
                    .get_ref()
                    .set_read_timeout(Some(Duration::from_millis(30)))
                    .unwrap();
                assert!(
                    matches!(
                        socket.read(),
                        Err(tungstenite::Error::Io(error))
                            if matches!(error.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock)
                    ),
                    "host must not acknowledge before the guest closes"
                );
                waiting.send(()).unwrap();
                if graceful {
                    socket
                        .get_ref()
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let Message::Close(Some(close)) = socket.read().unwrap() else {
                        panic!("expected the guest close reply");
                    };
                    assert_eq!(u16::from(close.code), 1000);
                    assert_eq!(close.reason, "guest");
                }
                socket.get_mut().shutdown(std::net::Shutdown::Both).unwrap();
            });
            let (mut stream, _) = listener.accept().unwrap();
            let request =
                super::super::read_http_request(&mut stream, "transport-1".into(), 16_384)
                    .unwrap()
                    .envelope;
            let (host, guest) = HostIngress::pair(
                "transport-1",
                true,
                Duration::from_secs(5),
                InvocationCancellation::default(),
            )
            .unwrap();
            let (done, completion) = mpsc::channel();
            let pump =
                thread::spawn(move || pump_http_stream(stream, &request, &[], host, completion));
            send(
                &guest,
                0,
                "headers",
                serde_json::json!({"status":101,"headers":[]}),
            );
            no_ack.recv_timeout(Duration::from_secs(2)).unwrap();
            if graceful {
                loop {
                    let frame: StreamFrame = serde_json::from_str(
                        &guest
                            .receive(r#"{"protocol_version":1,"request_id":"transport-1"}"#)
                            .unwrap(),
                    )
                    .unwrap();
                    if frame.kind == FrameKind::Websocket {
                        assert_eq!(frame.opcode, Some(8));
                        break;
                    }
                    assert_eq!(frame.kind, FrameKind::Pending);
                    thread::sleep(Duration::from_millis(1));
                }
                send(
                    &guest,
                    1,
                    "websocket",
                    serde_json::json!({
                        "opcode":8,"body_base64":STANDARD.encode(b"\x03\xe8guest")
                    }),
                );
                client.join().unwrap();
                thread::sleep(Duration::from_millis(30));
                assert!(!guest.cancellation().is_cancelled());
                assert!(
                    !pump.is_finished(),
                    "tracked work must still retain admission"
                );
                send(&guest, 2, "end", serde_json::json!({}));
                guest.finish(true).unwrap();
                done.send(InvocationExecution {
                    request_id: "transport-1".into(),
                    result: Ok(InvocationResponse::StreamComplete),
                    profile: Default::default(),
                    submit_error: None,
                })
                .unwrap();
                assert!(pump.join().unwrap().is_ok());
            } else {
                client.join().unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(1);
                while !guest.cancellation().is_cancelled() && std::time::Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(1));
                }
                assert!(
                    guest.cancellation().is_cancelled(),
                    "half-open EOF must cancel"
                );
                guest.abort();
                done.send(InvocationExecution {
                    request_id: "transport-1".into(),
                    result: Err(Error::Cancelled),
                    profile: Default::default(),
                    submit_error: None,
                })
                .unwrap();
                assert!(matches!(pump.join().unwrap(), Err(Error::Cancelled)));
            }
        }
    }

    #[test]
    fn websocket_handshake_is_synthesized_and_server_can_greet_before_client_sends() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let stream = TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let (mut socket, response) =
                tungstenite::client(format!("ws://{address}/"), stream).unwrap();
            assert_eq!(response.status(), 101);
            assert_eq!(socket.read().unwrap(), Message::Text("hello".into()));
            socket.send(Message::Text("client".into())).unwrap();
            assert_eq!(socket.read().unwrap(), Message::Text("echo".into()));
        });
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let parsed =
            super::super::read_http_request(&mut stream, "transport-1".into(), 16384).unwrap();
        let request = parsed.envelope;
        assert!(
            request
                .headers
                .iter()
                .any(|header| header.name.eq_ignore_ascii_case("sec-websocket-key"))
        );
        let (host, guest) = HostIngress::pair(
            "transport-1",
            true,
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .unwrap();
        let (completion_tx, completion_rx) = mpsc::channel();
        let pump =
            thread::spawn(move || pump_http_stream(stream, &request, &[], host, completion_rx));
        send(
            &guest,
            0,
            "headers",
            serde_json::json!({"status":101,"headers":[]}),
        );
        send(
            &guest,
            1,
            "websocket",
            serde_json::json!({"opcode":1,"body_base64":STANDARD.encode(b"hello")}),
        );
        loop {
            let frame: StreamFrame = serde_json::from_str(
                &guest
                    .receive(r#"{"protocol_version":1,"request_id":"transport-1"}"#)
                    .unwrap(),
            )
            .unwrap();
            if frame.kind == FrameKind::Websocket {
                assert_eq!(frame.decoded_body().unwrap(), b"client");
                break;
            }
            assert_eq!(frame.kind, FrameKind::Pending);
            thread::sleep(Duration::from_millis(1));
        }
        send(
            &guest,
            2,
            "websocket",
            serde_json::json!({"opcode":1,"body_base64":STANDARD.encode(b"echo")}),
        );
        client.join().unwrap();
        guest.abort();
        drop(completion_tx);
        assert!(
            pump.join().unwrap().is_err(),
            "disconnect must fail/cancel instead of releasing a reusable VM"
        );
    }
}
