// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Concrete host networking beneath the bounded broker adapter.

mod tcp_tls;
mod udp;
mod websocket;
pub(crate) use websocket::WebSocketTransport;

use crate::broker::{
    BrokerContractError, BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy, DnsPolicy,
    EndpointHost, RequestIdentity,
};
use crate::broker_adapter::{BrokerAdapter, BrokerExecution, BrokerExecutor, BrokerHostError};
use crate::broker_runtime::BrokerRuntime;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

static DNS_LOOKUPS: AtomicUsize = AtomicUsize::new(0);
const MAX_DNS_LOOKUPS: usize = 32;

struct DnsAdmission;
impl Drop for DnsAdmission {
    fn drop(&mut self) {
        DNS_LOOKUPS.fetch_sub(1, Ordering::AcqRel);
    }
}

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
    Tcp(DeadlineTcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, DeadlineTcpStream>>),
    Udp(std::net::UdpSocket),
    WebSocket(Box<tungstenite::WebSocket<websocket::WebSocketTransport>>),
}

pub(crate) struct DeadlineTcpStream {
    inner: std::net::TcpStream,
    deadline: Arc<Mutex<Option<Instant>>>,
    io_timeout: Duration,
}
impl DeadlineTcpStream {
    fn configure(&self) -> std::io::Result<()> {
        let deadline = *self
            .deadline
            .lock()
            .map_err(|_| std::io::Error::other("network deadline state poisoned"))?;
        let timeout = deadline.map_or(self.io_timeout, |deadline| {
            self.io_timeout
                .min(deadline.saturating_duration_since(Instant::now()))
        });
        if timeout.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "admitted network deadline exceeded",
            ));
        }
        self.inner.set_read_timeout(Some(timeout))?;
        self.inner.set_write_timeout(Some(timeout))
    }
}
impl Read for DeadlineTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.configure()?;
        self.inner.read(buffer)
    }
}
impl Write for DeadlineTcpStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.configure()?;
        self.inner.write(buffer)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.configure()?;
        self.inner.flush()
    }
}

pub(crate) fn provider_tls_transport(
    config: &NetworkBrokerConfig,
    endpoint: &BrokerEndpoint,
    deadline: Instant,
) -> Result<WebSocketTransport, BrokerHostError> {
    let executor = NetworkExecutor::new(config);
    *executor
        .deadline
        .lock()
        .map_err(|_| BrokerHostError::new("deadline_poisoned"))? = Some(deadline);
    let tcp = tcp_tls::connect(&executor, endpoint)?;
    Ok(WebSocketTransport::Tls(Box::new(websocket::tls(
        &executor, endpoint, tcp,
    )?)))
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
    deadline: Arc<Mutex<Option<Instant>>>,
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
            deadline: Arc::new(Mutex::new(None)),
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

    fn remaining_timeout(&self, configured: Duration) -> Result<Duration, BrokerHostError> {
        match *self
            .deadline
            .lock()
            .map_err(|_| BrokerHostError::new("deadline_poisoned"))?
        {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(BrokerHostError::new("execution_deadline"));
                }
                Ok(configured.min(remaining))
            }
            None => Ok(configured),
        }
    }

    fn retime_resources(&self) -> Result<(), BrokerHostError> {
        let timeout = self.remaining_timeout(self.io_timeout)?;
        let tcp = |stream: &DeadlineTcpStream| -> Result<(), BrokerHostError> {
            stream
                .inner
                .set_read_timeout(Some(timeout))
                .and_then(|()| stream.inner.set_write_timeout(Some(timeout)))
                .map_err(|_| BrokerHostError::new("stream_configuration"))
        };
        for resource in self.resources.values() {
            match resource {
                NetworkResource::Tcp(stream) => tcp(stream)?,
                NetworkResource::Tls(stream) => tcp(&stream.sock)?,
                NetworkResource::Udp(socket) => {
                    socket
                        .set_read_timeout(Some(timeout))
                        .and_then(|()| socket.set_write_timeout(Some(timeout)))
                        .map_err(|_| BrokerHostError::new("udp_configuration"))?;
                }
                NetworkResource::WebSocket(socket) => match socket.get_ref() {
                    websocket::WebSocketTransport::Tcp(stream) => tcp(stream)?,
                    websocket::WebSocketTransport::Tls(stream) => tcp(&stream.sock)?,
                },
            }
        }
        Ok(())
    }
}

/// Mozilla WebPKI roots used by the default host TLS policy.
pub fn default_tls_roots() -> rustls::RootCertStore {
    rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
}

impl BrokerExecutor for NetworkExecutor {
    fn checkpoint_next_handle(&self) -> Option<u64> {
        Some(self.next_handle)
    }
    fn restore_next_handle(&mut self, next: u64) -> Result<(), BrokerHostError> {
        if next == 0 || !self.resources.is_empty() {
            return Err(BrokerHostError::new("invalid_handle_watermark"));
        }
        self.next_handle = next;
        Ok(())
    }
    fn set_deadline(&mut self, deadline: Instant) {
        *self
            .deadline
            .lock()
            .expect("network deadline state poisoned") = Some(deadline);
    }
    fn execute(&mut self, operation: &BrokerOperation) -> Result<BrokerExecution, BrokerHostError> {
        self.retime_resources()?;
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
            if DNS_LOOKUPS
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                    (active < MAX_DNS_LOOKUPS).then_some(active + 1)
                })
                .is_err()
            {
                return Err(BrokerHostError::new("dns_overloaded"));
            }
            let admission = DnsAdmission;
            let name = name.as_str().to_string();
            let port = endpoint.port();
            let (sender, receiver) = std::sync::mpsc::channel();
            std::thread::Builder::new()
                .name("broker-dns".into())
                .spawn(move || {
                    let _admission = admission;
                    let result = (name.as_str(), port)
                        .to_socket_addrs()
                        .map(|addresses| addresses.take(65).collect::<Vec<_>>());
                    if sender.send(result).is_err() {
                        tracing::debug!("broker DNS deadline elapsed");
                    }
                })
                .map_err(|_| BrokerHostError::new("dns_unavailable"))?;
            let addresses = receiver
                .recv_timeout(executor.remaining_timeout(executor.connect_timeout)?)
                .map_err(|_| BrokerHostError::new("dns_deadline"))?
                .map_err(|_| BrokerHostError::new("dns_failed"))?;
            if addresses.len() > 64 {
                return Err(BrokerHostError::new("dns_answer_limit"));
            }
            addresses
        }
    };
    if addresses
        .iter()
        .any(|address| !executor.policy.address_allowed(address.ip()))
    {
        return Err(BrokerHostError::new("resolved_address_denied"));
    }
    let mut allowed = addresses;
    if allowed.is_empty() {
        return Err(BrokerHostError::new("resolved_address_denied"));
    }
    allowed.sort_by_key(|address| match address.ip() {
        IpAddr::V4(_) => 0,
        IpAddr::V6(_) => 1,
    });
    Ok(allowed)
}
