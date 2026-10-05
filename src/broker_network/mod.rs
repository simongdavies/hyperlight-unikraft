// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Concrete host networking beneath the bounded broker adapter.

mod tcp_tls;
mod udp;
mod websocket;

use crate::broker::{
    BrokerContractError, BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy, DnsPolicy,
    EndpointHost, RequestIdentity, is_host_local_ip,
};
use crate::broker_adapter::{BrokerAdapter, BrokerExecution, BrokerExecutor, BrokerHostError};
use crate::broker_runtime::BrokerRuntime;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

/// Host-owned settings for the concrete outbound network broker.
#[derive(Clone, Debug)]
pub struct NetworkBrokerConfig {
    /// Deny-by-default destination and DNS policy.
    pub policy: BrokerPolicy,
    /// Finite request-scoped quotas.
    pub limits: BrokerLimits,
    /// Resolver identity checked against [`DnsPolicy`].
    pub resolver: IpAddr,
    /// Maximum time spent establishing a transport.
    pub connect_timeout: Duration,
    /// Maximum time spent in one blocking send or receive operation.
    pub io_timeout: Duration,
    /// Host-owned TLS trust roots. They are never serialized to the guest.
    pub tls_roots: Arc<rustls::RootCertStore>,
}

impl NetworkBrokerConfig {
    /// Validate all finite settings before a runtime can be constructed.
    pub fn validate(self) -> Result<Self, BrokerContractError> {
        self.limits.validate()?;
        if self.connect_timeout.is_zero() || self.io_timeout.is_zero() {
            return Err(BrokerContractError::UnboundedOrZeroLimit);
        }
        Ok(self)
    }
}

/// Cloneable factory for a fresh request-scoped network runtime.
#[derive(Clone, Debug)]
pub struct NetworkBroker {
    config: NetworkBrokerConfig,
}

impl Default for NetworkBroker {
    fn default() -> Self {
        Self {
            config: NetworkBrokerConfig {
                policy: BrokerPolicy::deny_all(),
                limits: BrokerLimits {
                    max_connections: 8,
                    max_sockets: 8,
                    max_datagrams: 64,
                    max_datagram_bytes: 64 * 1024,
                    max_stream_bytes: 64 * 1024,
                    max_messages: 64,
                    max_message_bytes: 1024 * 1024,
                    max_bytes: 8 * 1024 * 1024,
                    max_elapsed: Duration::from_secs(30),
                    max_operations: 256,
                    max_operations_per_window: 64,
                    rate_window: Duration::from_secs(1),
                    max_concurrency: 1,
                },
                resolver: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                connect_timeout: Duration::from_secs(5),
                io_timeout: Duration::from_secs(5),
                tls_roots: Arc::new(default_tls_roots()),
            },
        }
    }
}

impl NetworkBroker {
    /// Create a broker from explicit policy, quotas, resolver identity, and timeouts.
    pub fn new(config: NetworkBrokerConfig) -> Result<Self, BrokerContractError> {
        Ok(Self {
            config: config.validate()?,
        })
    }

    /// Deny all networking while retaining finite conservative limits.
    pub fn denied(limits: BrokerLimits, resolver: IpAddr) -> Result<Self, BrokerContractError> {
        Self::new(NetworkBrokerConfig {
            policy: BrokerPolicy::deny_all(),
            limits,
            resolver,
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(5),
            tls_roots: Arc::new(default_tls_roots()),
        })
    }

    /// Build isolated executor, handle table, budget, and audit state for one VM assignment.
    pub fn runtime(&self, identity: RequestIdentity) -> Result<BrokerRuntime, BrokerContractError> {
        let executor = NetworkExecutor::new(&self.config);
        let adapter = BrokerAdapter::new(self.config.policy.clone(), self.config.limits, executor)?;
        Ok(BrokerRuntime::deny_all(identity).with_network(adapter))
    }

    /// Validated immutable configuration.
    pub fn config(&self) -> &NetworkBrokerConfig {
        &self.config
    }
}

enum NetworkResource {
    Tcp(std::net::TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>>),
    Udp(std::net::UdpSocket),
    WebSocket(Box<tungstenite::WebSocket<websocket::WebSocketTransport>>),
}

struct NetworkExecutor {
    policy: BrokerPolicy,
    resolver: IpAddr,
    connect_timeout: Duration,
    io_timeout: Duration,
    tls_roots: Arc<rustls::RootCertStore>,
    max_message_bytes: usize,
    next_handle: u64,
    resources: HashMap<u64, NetworkResource>,
}

impl NetworkExecutor {
    fn new(config: &NetworkBrokerConfig) -> Self {
        Self {
            policy: config.policy.clone(),
            resolver: config.resolver,
            connect_timeout: config.connect_timeout,
            io_timeout: config.io_timeout,
            tls_roots: Arc::clone(&config.tls_roots),
            max_message_bytes: usize::try_from(config.limits.max_message_bytes)
                .unwrap_or(usize::MAX),
            next_handle: 1,
            resources: HashMap::new(),
        }
    }

    fn insert(&mut self, resource: NetworkResource) -> Result<u64, BrokerHostError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| BrokerHostError::new("handle_exhausted"))?;
        self.resources.insert(handle, resource);
        Ok(handle)
    }

    fn dns_policy(&self) -> &DnsPolicy {
        self.policy.dns()
    }
}

/// Mozilla WebPKI roots used by the default host TLS policy.
pub fn default_tls_roots() -> rustls::RootCertStore {
    rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
}

impl BrokerExecutor for NetworkExecutor {
    fn execute(&mut self, operation: &BrokerOperation) -> Result<BrokerExecution, BrokerHostError> {
        match operation {
            BrokerOperation::TcpConnect { endpoint } => tcp_tls::connect_tcp(self, endpoint),
            BrokerOperation::TlsConnect { endpoint, profile } => {
                tcp_tls::connect_tls(self, endpoint, profile)
            }
            BrokerOperation::StreamSend { stream_id, payload } => {
                tcp_tls::send(self, *stream_id, payload)
            }
            BrokerOperation::StreamReceive {
                stream_id,
                max_bytes,
            } => tcp_tls::receive(self, *stream_id, *max_bytes),
            BrokerOperation::UdpOpen { endpoint } => udp::open(self, endpoint),
            BrokerOperation::UdpSend { endpoint, payload } => {
                udp::send_once(self, endpoint, payload)
            }
            BrokerOperation::UdpSocketSend { socket_id, payload } => {
                udp::send(self, *socket_id, payload)
            }
            BrokerOperation::UdpReceive {
                socket_id,
                max_bytes,
            } => udp::receive(self, *socket_id, *max_bytes),
            BrokerOperation::WebSocketOpen {
                endpoint,
                secure,
                profile,
            } => websocket::open(self, endpoint, *secure, profile),
            BrokerOperation::WebSocketSend {
                socket_id,
                payload,
                binary,
            } => websocket::send(self, *socket_id, payload, *binary),
            BrokerOperation::WebSocketReceive {
                socket_id,
                max_bytes,
            } => websocket::receive(self, *socket_id, *max_bytes),
            BrokerOperation::Close { handle_id } => {
                if let Some(NetworkResource::WebSocket(socket)) = self.resources.get_mut(handle_id)
                {
                    socket
                        .close(None)
                        .map_err(|_| BrokerHostError::new("websocket_close"))?;
                }
                if self.resources.remove(handle_id).is_some() {
                    Ok(BrokerExecution::Closed)
                } else {
                    Err(BrokerHostError::new("invalid_handle"))
                }
            }
        }
    }

    fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
        self.resources.clear();
        self.next_handle = 1;
        Ok(())
    }
}

fn resolve(
    executor: &NetworkExecutor,
    endpoint: &BrokerEndpoint,
) -> Result<Vec<SocketAddr>, BrokerHostError> {
    let addresses: Vec<_> = match endpoint.host() {
        EndpointHost::Ip(ip) => vec![SocketAddr::new(*ip, endpoint.port())],
        EndpointHost::Dns(name) => {
            if !matches!(executor.dns_policy(), DnsPolicy::Allow { .. })
                || !executor.dns_policy().allows(name, executor.resolver)
            {
                return Err(BrokerHostError::new("dns_denied"));
            }
            (name.as_str(), endpoint.port())
                .to_socket_addrs()
                .map_err(|_| BrokerHostError::new("dns_failed"))?
                .collect()
        }
    };
    let mut allowed: Vec<_> = addresses
        .into_iter()
        .filter(|address| !is_host_local_ip(address.ip()))
        .collect();
    if allowed.is_empty() {
        return Err(BrokerHostError::new("resolved_address_denied"));
    }
    allowed.sort_by_key(|address| match address.ip() {
        IpAddr::V4(_) => 0,
        IpAddr::V6(_) => 1,
    });
    Ok(allowed)
}
