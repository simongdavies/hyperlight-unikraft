// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{NetworkExecutor, NetworkResource, resolve};
use crate::broker::{BrokerEndpoint, TlsProfile, TlsVersion};
use crate::broker_adapter::{BrokerExecution, BrokerHostError};
use rustls::pki_types::ServerName;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub(super) fn connect_tcp(
    executor: &mut NetworkExecutor,
    endpoint: &BrokerEndpoint,
) -> Result<BrokerExecution, BrokerHostError> {
    let stream = connect(executor, endpoint)?;
    let handle_id = executor.insert(NetworkResource::Tcp(stream))?;
    Ok(BrokerExecution::Opened { handle_id })
}

pub(super) fn connect_tls(
    executor: &mut NetworkExecutor,
    endpoint: &BrokerEndpoint,
    profile: &TlsProfile,
) -> Result<BrokerExecution, BrokerHostError> {
    let stream = connect(executor, endpoint)?;
    let versions: &[&'static rustls::SupportedProtocolVersion] = match profile.minimum_version() {
        TlsVersion::Tls12 => &[&rustls::version::TLS13, &rustls::version::TLS12],
        TlsVersion::Tls13 => &[&rustls::version::TLS13],
    };
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(versions)
        .with_root_certificates((*executor.tls_roots).clone())
        .with_no_client_auth();
    config.alpn_protocols = profile
        .alpn()
        .iter()
        .map(|protocol| protocol.as_bytes().to_vec())
        .collect();
    let server_name = ServerName::try_from(profile.server_name().as_str().to_string())
        .map_err(|_| BrokerHostError::new("invalid_server_name"))?;
    let connection = rustls::ClientConnection::new(Arc::new(config), server_name)
        .map_err(|_| BrokerHostError::new("tls_configuration"))?;
    let mut stream = rustls::StreamOwned::new(connection, stream);
    stream
        .conn
        .complete_io(&mut stream.sock)
        .map_err(|_| BrokerHostError::new("tls_handshake"))?;
    let handle_id = executor.insert(NetworkResource::Tls(Box::new(stream)))?;
    Ok(BrokerExecution::Opened { handle_id })
}

pub(super) fn send(
    executor: &mut NetworkExecutor,
    stream_id: u64,
    payload: &[u8],
) -> Result<BrokerExecution, BrokerHostError> {
    match executor.resources.get_mut(&stream_id) {
        Some(NetworkResource::Tcp(stream)) => stream.write_all(payload),
        Some(NetworkResource::Tls(stream)) => stream.write_all(payload),
        Some(NetworkResource::Udp(_) | NetworkResource::WebSocket(_)) => {
            return Err(BrokerHostError::new("invalid_stream_handle"));
        }
        None => return Err(BrokerHostError::new("invalid_stream_handle")),
    }
    .map_err(|_| BrokerHostError::new("stream_write"))?;
    Ok(BrokerExecution::Transferred {
        bytes: payload.len() as u64,
    })
}

pub(super) fn receive(
    executor: &mut NetworkExecutor,
    stream_id: u64,
    max_bytes: u32,
) -> Result<BrokerExecution, BrokerHostError> {
    let mut payload = vec![0; max_bytes as usize];
    let read = match executor.resources.get_mut(&stream_id) {
        Some(NetworkResource::Tcp(stream)) => stream.read(&mut payload),
        Some(NetworkResource::Tls(stream)) => stream.read(&mut payload),
        Some(NetworkResource::Udp(_) | NetworkResource::WebSocket(_)) => {
            return Err(BrokerHostError::new("invalid_stream_handle"));
        }
        None => return Err(BrokerHostError::new("invalid_stream_handle")),
    }
    .map_err(|_| BrokerHostError::new("stream_read"))?;
    payload.truncate(read);
    Ok(BrokerExecution::Received {
        payload,
        binary: true,
    })
}

pub(super) fn connect(
    executor: &NetworkExecutor,
    endpoint: &BrokerEndpoint,
) -> Result<TcpStream, BrokerHostError> {
    let addresses = resolve(executor, endpoint)?;
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, executor.connect_timeout) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(executor.io_timeout))
                    .map_err(|_| BrokerHostError::new("stream_configuration"))?;
                stream
                    .set_write_timeout(Some(executor.io_timeout))
                    .map_err(|_| BrokerHostError::new("stream_configuration"))?;
                return Ok(stream);
            }
            Err(error) => last_error = Some(error),
        }
    }
    let _ = last_error;
    Err(BrokerHostError::new("connect_failed"))
}
