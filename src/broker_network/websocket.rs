// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{DeadlineTcpStream, NetworkExecutor, NetworkResource};
use crate::broker::{BrokerEndpoint, EndpointHost, WebSocketProfile};
use crate::broker_adapter::{BrokerExecution, BrokerHostError};
use rustls::pki_types::ServerName;
use std::io::{Read, Write};
use std::sync::Arc;
use tungstenite::Message;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;

pub(crate) enum WebSocketTransport {
    Tcp(DeadlineTcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, DeadlineTcpStream>>),
}

impl WebSocketTransport {
    pub(crate) fn set_deadline(&mut self, deadline: std::time::Instant) -> std::io::Result<()> {
        let state = match self {
            Self::Tcp(stream) => &stream.deadline,
            Self::Tls(stream) => &stream.sock.deadline,
        };
        *state
            .lock()
            .map_err(|_| std::io::Error::other("network deadline state poisoned"))? =
            Some(deadline);
        Ok(())
    }

    pub(crate) fn tcp(&self) -> &std::net::TcpStream {
        match self {
            Self::Tcp(stream) => &stream.inner,
            Self::Tls(stream) => &stream.sock.inner,
        }
    }

    pub(crate) fn polling(&mut self, timeout: std::time::Duration) {
        match self {
            Self::Tcp(stream) => stream.io_timeout = timeout,
            Self::Tls(stream) => stream.sock.io_timeout = timeout,
        }
    }
}

impl Read for WebSocketTransport {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for WebSocketTransport {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

pub(super) fn open(
    executor: &mut NetworkExecutor,
    endpoint: &BrokerEndpoint,
    secure: bool,
    profile: &WebSocketProfile,
) -> Result<BrokerExecution, BrokerHostError> {
    let stream = super::tcp_tls::connect(executor, endpoint)?;
    let transport = if secure {
        WebSocketTransport::Tls(Box::new(tls(executor, endpoint, stream)?))
    } else {
        WebSocketTransport::Tcp(stream)
    };
    let scheme = if secure { "wss" } else { "ws" };
    let host = match endpoint.host() {
        EndpointHost::Dns(name) => name.as_str().to_string(),
        EndpointHost::Ip(ip) if ip.is_ipv6() => format!("[{ip}]"),
        EndpointHost::Ip(ip) => ip.to_string(),
    };
    let mut request = format!("{scheme}://{host}:{}/", endpoint.port())
        .into_client_request()
        .map_err(|_| BrokerHostError::new("websocket_request"))?;
    if !profile.subprotocols().is_empty() {
        let protocols = profile.subprotocols().join(", ");
        let value = HeaderValue::from_str(&protocols)
            .map_err(|_| BrokerHostError::new("websocket_request"))?;
        request.headers_mut().insert(SEC_WEBSOCKET_PROTOCOL, value);
    }
    let mut config = tungstenite::protocol::WebSocketConfig::default();
    config.max_message_size = Some(executor.max_message_bytes);
    config.max_frame_size = Some(executor.max_message_bytes);
    let (socket, response) =
        tungstenite::client::client_with_config(request, transport, Some(config))
            .map_err(|_| BrokerHostError::new("websocket_handshake"))?;
    if let Some(selected) = response.headers().get(SEC_WEBSOCKET_PROTOCOL) {
        let selected = selected
            .to_str()
            .map_err(|_| BrokerHostError::new("websocket_subprotocol"))?;
        if !profile
            .subprotocols()
            .iter()
            .any(|protocol| protocol == selected)
        {
            return Err(BrokerHostError::new("websocket_subprotocol"));
        }
    }
    let handle_id = executor.insert(NetworkResource::WebSocket(Box::new(socket)))?;
    Ok(BrokerExecution::Opened { handle_id })
}

pub(super) fn send(
    executor: &mut NetworkExecutor,
    socket_id: u64,
    payload: &[u8],
    binary: bool,
) -> Result<BrokerExecution, BrokerHostError> {
    let Some(NetworkResource::WebSocket(socket)) = executor.resources.get_mut(&socket_id) else {
        return Err(BrokerHostError::new("invalid_websocket_handle"));
    };
    let message = if binary {
        Message::Binary(payload.to_vec().into())
    } else {
        let text = String::from_utf8(payload.to_vec())
            .map_err(|_| BrokerHostError::new("invalid_websocket_text"))?;
        Message::Text(text.into())
    };
    socket
        .send(message)
        .map_err(|_| BrokerHostError::new("websocket_send"))?;
    Ok(BrokerExecution::Transferred {
        bytes: payload.len() as u64,
    })
}

pub(super) fn receive(
    executor: &mut NetworkExecutor,
    socket_id: u64,
    max_bytes: u32,
) -> Result<BrokerExecution, BrokerHostError> {
    let Some(NetworkResource::WebSocket(socket)) = executor.resources.get_mut(&socket_id) else {
        return Err(BrokerHostError::new("invalid_websocket_handle"));
    };
    for _ in 0..=16 {
        match socket
            .read()
            .map_err(|_| BrokerHostError::new("websocket_receive"))?
        {
            Message::Binary(payload) => {
                if payload.len() > max_bytes as usize {
                    return Err(BrokerHostError::new("websocket_message_too_large"));
                }
                return Ok(BrokerExecution::Received {
                    payload: payload.to_vec(),
                    binary: true,
                });
            }
            Message::Text(payload) => {
                if payload.len() > max_bytes as usize {
                    return Err(BrokerHostError::new("websocket_message_too_large"));
                }
                return Ok(BrokerExecution::Received {
                    payload: payload.as_bytes().to_vec(),
                    binary: false,
                });
            }
            Message::Close(_) => return Err(BrokerHostError::new("websocket_closed")),
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    Err(BrokerHostError::new("websocket_control_limit"))
}

pub(super) fn tls(
    executor: &NetworkExecutor,
    endpoint: &BrokerEndpoint,
    stream: DeadlineTcpStream,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, DeadlineTcpStream>, BrokerHostError> {
    let config = rustls::ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates((*executor.tls_roots).clone())
    .with_no_client_auth();
    let server_name = match endpoint.host() {
        EndpointHost::Dns(name) => ServerName::try_from(name.as_str().to_string()),
        EndpointHost::Ip(ip) => ServerName::try_from(ip.to_string()),
    }
    .map_err(|_| BrokerHostError::new("invalid_server_name"))?;
    let connection = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|_| BrokerHostError::new("tls_configuration"))?;
    let mut stream = rustls::StreamOwned::new(connection, stream);
    stream
        .conn
        .complete_io(&mut stream.sock)
        .map_err(|_| BrokerHostError::new("tls_handshake"))?;
    Ok(stream)
}
