// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{NetworkExecutor, NetworkResource, resolve};
use crate::broker::BrokerEndpoint;
use crate::broker_adapter::{BrokerExecution, BrokerHostError};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

pub(super) fn open(
    executor: &mut NetworkExecutor,
    endpoint: &BrokerEndpoint,
) -> Result<BrokerExecution, BrokerHostError> {
    let socket = connect(executor, endpoint)?;
    let handle_id = executor.insert(NetworkResource::Udp(socket))?;
    Ok(BrokerExecution::Opened { handle_id })
}

pub(super) fn send_once(
    executor: &NetworkExecutor,
    endpoint: &BrokerEndpoint,
    payload: &[u8],
) -> Result<BrokerExecution, BrokerHostError> {
    let socket = connect(executor, endpoint)?;
    let bytes = socket
        .send(payload)
        .map_err(|_| BrokerHostError::new("udp_send"))?;
    Ok(BrokerExecution::Transferred {
        bytes: bytes as u64,
    })
}

pub(super) fn send(
    executor: &mut NetworkExecutor,
    socket_id: u64,
    payload: &[u8],
) -> Result<BrokerExecution, BrokerHostError> {
    let Some(NetworkResource::Udp(socket)) = executor.resources.get_mut(&socket_id) else {
        return Err(BrokerHostError::new("invalid_udp_handle"));
    };
    let bytes = socket
        .send(payload)
        .map_err(|_| BrokerHostError::new("udp_send"))?;
    Ok(BrokerExecution::Transferred {
        bytes: bytes as u64,
    })
}

pub(super) fn receive(
    executor: &mut NetworkExecutor,
    socket_id: u64,
    max_bytes: u32,
) -> Result<BrokerExecution, BrokerHostError> {
    let Some(NetworkResource::Udp(socket)) = executor.resources.get_mut(&socket_id) else {
        return Err(BrokerHostError::new("invalid_udp_handle"));
    };
    let mut payload = vec![0; max_bytes as usize];
    let bytes = socket
        .recv(&mut payload)
        .map_err(|_| BrokerHostError::new("udp_receive"))?;
    payload.truncate(bytes);
    Ok(BrokerExecution::Received {
        payload,
        binary: true,
    })
}

fn connect(
    executor: &NetworkExecutor,
    endpoint: &BrokerEndpoint,
) -> Result<UdpSocket, BrokerHostError> {
    for address in resolve(executor, endpoint)? {
        let bind = match address.ip() {
            IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };
        let Ok(socket) = UdpSocket::bind(bind) else {
            continue;
        };
        if socket.connect(address).is_err() {
            continue;
        }
        socket
            .set_read_timeout(Some(executor.io_timeout))
            .map_err(|_| BrokerHostError::new("udp_configuration"))?;
        socket
            .set_write_timeout(Some(executor.io_timeout))
            .map_err(|_| BrokerHostError::new("udp_configuration"))?;
        return Ok(socket);
    }
    Err(BrokerHostError::new("udp_connect"))
}
