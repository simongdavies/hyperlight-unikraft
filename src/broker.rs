//! Shared host-brokered protocol contracts for constrained guest capabilities.
//!
//! These types are transport-independent. They deliberately contain no socket,
//! TLS credential, or runtime handles so policy enforcement and quota accounting
//! can be shared by TCP/TLS, UDP, WebSocket, scheduled, and queue implementations.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

/// Maximum identity component length accepted from the trusted host boundary.
const MAX_IDENTITY_COMPONENT_LEN: usize = 128;

/// Network protocol exposed through the host broker.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BrokerProtocol {
    /// Host-brokered TCP stream.
    Tcp,
    /// Host-terminated TLS stream. The guest never receives TLS credentials.
    Tls,
    /// Host-brokered UDP datagrams.
    Udp,
    /// Bounded WebSocket over cleartext TCP.
    WebSocket,
    /// Bounded WebSocket over host-terminated TLS.
    WebSocketSecure,
}

/// A validated DNS name used by policy and protocol requests.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DnsName(String);

impl DnsName {
    /// Return the normalized, lower-case DNS name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DnsName {
    type Err = BrokerContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim_end_matches('.').to_ascii_lowercase();
        if normalized.is_empty() || normalized.len() > 253 {
            return Err(BrokerContractError::InvalidDnsName(value.to_string()));
        }
        if normalized.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        }) {
            return Err(BrokerContractError::InvalidDnsName(value.to_string()));
        }
        Ok(Self(normalized))
    }
}

impl fmt::Display for DnsName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A destination supplied to the host broker.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum EndpointHost {
    /// Literal destination IP.
    Ip(IpAddr),
    /// Validated destination DNS name.
    Dns(DnsName),
}

/// A typed broker destination.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BrokerEndpoint {
    host: EndpointHost,
    port: u16,
}

impl BrokerEndpoint {
    /// Create a destination. Port zero is rejected.
    pub fn new(host: EndpointHost, port: u16) -> Result<Self, BrokerContractError> {
        if port == 0 {
            return Err(BrokerContractError::InvalidPort(port));
        }
        Ok(Self { host, port })
    }

    /// Destination host.
    pub fn host(&self) -> &EndpointHost {
        &self.host
    }

    /// Destination port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Inclusive destination port range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortRange {
    start: u16,
    end: u16,
}

impl PortRange {
    /// Create an inclusive, non-zero port range.
    pub fn new(start: u16, end: u16) -> Result<Self, BrokerContractError> {
        if start == 0 || start > end {
            return Err(BrokerContractError::InvalidPortRange { start, end });
        }
        Ok(Self { start, end })
    }

    fn contains(self, port: u16) -> bool {
        (self.start..=self.end).contains(&port)
    }
}

/// Host matching rule. Wildcards only match subdomains, never the apex.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostRule {
    /// Match one literal IP.
    Ip(IpAddr),
    /// Match one exact DNS name.
    ExactDns(DnsName),
    /// Match subdomains such as `api.example.com`, but not `example.com`.
    DnsSubdomains(DnsName),
}

impl HostRule {
    fn matches(&self, host: &EndpointHost) -> bool {
        match (self, host) {
            (Self::Ip(allowed), EndpointHost::Ip(actual)) => allowed == actual,
            (Self::ExactDns(allowed), EndpointHost::Dns(actual)) => allowed == actual,
            (Self::DnsSubdomains(suffix), EndpointHost::Dns(actual)) => actual
                .as_str()
                .strip_suffix(suffix.as_str())
                .is_some_and(|prefix| prefix.ends_with('.') && prefix.len() > 1),
            _ => false,
        }
    }
}

/// One explicit egress allow rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EgressRule {
    host: HostRule,
    ports: Vec<PortRange>,
    protocols: Vec<BrokerProtocol>,
}

impl EgressRule {
    /// Create an allow rule. Empty protocol or port sets are rejected.
    pub fn new(
        host: HostRule,
        ports: Vec<PortRange>,
        protocols: Vec<BrokerProtocol>,
    ) -> Result<Self, BrokerContractError> {
        if ports.is_empty() {
            return Err(BrokerContractError::EmptyRulePorts);
        }
        if protocols.is_empty() {
            return Err(BrokerContractError::EmptyRuleProtocols);
        }
        Ok(Self {
            host,
            ports,
            protocols,
        })
    }

    fn allows(&self, protocol: BrokerProtocol, endpoint: &BrokerEndpoint) -> bool {
        self.host.matches(endpoint.host())
            && self
                .ports
                .iter()
                .any(|ports| ports.contains(endpoint.port()))
            && self.protocols.contains(&protocol)
    }
}

/// DNS access policy. DNS is denied unless both name and resolver are listed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DnsPolicy {
    /// Deny all guest-requested DNS resolution.
    Deny,
    /// Allow only listed names through listed resolver IPs.
    Allow {
        /// Names the host may resolve on behalf of the guest.
        names: Vec<DnsName>,
        /// Resolver IPs the host broker may contact.
        resolvers: Vec<IpAddr>,
    },
}

impl DnsPolicy {
    /// Check whether a name may be resolved through a resolver.
    pub fn allows(&self, name: &DnsName, resolver: IpAddr) -> bool {
        match self {
            Self::Deny => false,
            Self::Allow { names, resolvers } => {
                names.contains(name) && resolvers.contains(&resolver)
            }
        }
    }
}

/// Deny-by-default host broker policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerPolicy {
    egress: Vec<EgressRule>,
    dns: DnsPolicy,
}

impl BrokerPolicy {
    /// Create a policy that denies every protocol and DNS request.
    pub fn deny_all() -> Self {
        Self {
            egress: Vec::new(),
            dns: DnsPolicy::Deny,
        }
    }

    /// Create a policy from explicit allow rules and a separate DNS policy.
    pub fn new(egress: Vec<EgressRule>, dns: DnsPolicy) -> Self {
        Self { egress, dns }
    }

    /// Authorize one endpoint request.
    pub fn authorize(
        &self,
        protocol: BrokerProtocol,
        endpoint: &BrokerEndpoint,
    ) -> Result<(), BrokerDenied> {
        if is_host_local(endpoint.host()) {
            return Err(BrokerDenied::HostLocalAddress);
        }
        if self
            .egress
            .iter()
            .any(|rule| rule.allows(protocol, endpoint))
        {
            Ok(())
        } else {
            Err(BrokerDenied::NoMatchingRule)
        }
    }

    /// DNS policy associated with this broker.
    pub fn dns(&self) -> &DnsPolicy {
        &self.dns
    }
}

fn is_host_local(host: &EndpointHost) -> bool {
    match host {
        EndpointHost::Ip(IpAddr::V4(ip)) => {
            ip.is_loopback() || ip.is_link_local() || ip.is_unspecified()
        }
        EndpointHost::Ip(IpAddr::V6(ip)) => {
            let first = ip.segments()[0];
            ip.is_loopback() || ip.is_unspecified() || (first & 0xffc0) == 0xfe80
        }
        EndpointHost::Dns(name) => name.as_str() == "localhost",
    }
}

/// Stable execution identity assigned by the trusted host boundary.
///
/// This identity is never decoded from guest-controlled wire data.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RequestIdentity {
    workload_id: String,
    snapshot_id: String,
    attempt: u32,
}

impl RequestIdentity {
    /// Construct an identity from bounded printable ASCII components.
    pub fn new(
        workload_id: impl Into<String>,
        snapshot_id: impl Into<String>,
        attempt: u32,
    ) -> Result<Self, BrokerContractError> {
        let workload_id = workload_id.into();
        let snapshot_id = snapshot_id.into();
        validate_identity_component("workload_id", &workload_id)?;
        validate_identity_component("snapshot_id", &snapshot_id)?;
        Ok(Self {
            workload_id,
            snapshot_id,
            attempt,
        })
    }

    /// Host-selected workload or deployment identifier.
    pub fn workload_id(&self) -> &str {
        &self.workload_id
    }

    /// Snapshot or deployment image identity exact-matched by the host.
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Zero-based delivery attempt.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// Guest-supplied correlation identifier carried by the versioned wire envelope.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BrokerRequestId(String);

impl BrokerRequestId {
    /// Construct a bounded printable ASCII request identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, BrokerContractError> {
        let value = value.into();
        validate_identity_component("request_id", &value)?;
        Ok(Self(value))
    }

    /// Correlation value echoed in responses and audit events.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_identity_component(
    field: &'static str,
    value: &str,
) -> Result<(), BrokerContractError> {
    if value.is_empty()
        || value.len() > MAX_IDENTITY_COMPONENT_LEN
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(BrokerContractError::InvalidIdentity { field });
    }
    Ok(())
}

/// TLS versions available to host-terminated TLS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsVersion {
    /// TLS 1.2.
    Tls12,
    /// TLS 1.3.
    Tls13,
}

/// Non-secret TLS settings accepted from a guest request.
///
/// Credentials and trust roots are intentionally absent. They remain host-owned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlsProfile {
    server_name: DnsName,
    alpn: Vec<String>,
    minimum_version: TlsVersion,
}

impl TlsProfile {
    /// Construct host-terminated TLS settings.
    pub fn new(
        server_name: DnsName,
        alpn: Vec<String>,
        minimum_version: TlsVersion,
    ) -> Result<Self, BrokerContractError> {
        if alpn.len() > 8
            || alpn
                .iter()
                .any(|value| value.is_empty() || value.len() > 255 || !value.is_ascii())
        {
            return Err(BrokerContractError::InvalidAlpn);
        }
        Ok(Self {
            server_name,
            alpn,
            minimum_version,
        })
    }

    /// Server name validated by the host TLS implementation.
    pub fn server_name(&self) -> &DnsName {
        &self.server_name
    }

    /// Application protocols offered by the host TLS implementation.
    pub fn alpn(&self) -> &[String] {
        &self.alpn
    }

    /// Minimum accepted TLS version.
    pub fn minimum_version(&self) -> TlsVersion {
        self.minimum_version
    }
}

/// Bounded WebSocket upgrade settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebSocketProfile {
    subprotocols: Vec<String>,
}

impl WebSocketProfile {
    /// Construct a profile with at most eight bounded ASCII subprotocols.
    pub fn new(subprotocols: Vec<String>) -> Result<Self, BrokerContractError> {
        if subprotocols.len() > 8
            || subprotocols
                .iter()
                .any(|value| value.is_empty() || value.len() > 128 || !value.is_ascii())
        {
            return Err(BrokerContractError::InvalidWebSocketSubprotocol);
        }
        Ok(Self { subprotocols })
    }

    /// Ordered subprotocols offered during the host-owned upgrade.
    pub fn subprotocols(&self) -> &[String] {
        &self.subprotocols
    }
}

/// A bounded guest-to-host protocol request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerRequest {
    request_id: BrokerRequestId,
    operation: BrokerOperation,
}

impl BrokerRequest {
    /// Create a request from a guest correlation ID and typed operation.
    pub fn new(request_id: BrokerRequestId, operation: BrokerOperation) -> Self {
        Self {
            request_id,
            operation,
        }
    }

    /// Guest correlation ID echoed without granting authority.
    pub fn request_id(&self) -> &BrokerRequestId {
        &self.request_id
    }

    /// Requested broker operation.
    pub fn operation(&self) -> &BrokerOperation {
        &self.operation
    }
}

/// Protocol operation requested by the guest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerOperation {
    /// Open a host-brokered TCP stream.
    TcpConnect { endpoint: BrokerEndpoint },
    /// Open a host-terminated TLS stream.
    TlsConnect {
        endpoint: BrokerEndpoint,
        profile: TlsProfile,
    },
    /// Send one bounded UDP datagram.
    UdpSend {
        endpoint: BrokerEndpoint,
        payload: Vec<u8>,
    },
    /// Open a bounded WebSocket.
    WebSocketOpen {
        endpoint: BrokerEndpoint,
        secure: bool,
        profile: WebSocketProfile,
    },
    /// Send one bounded WebSocket message.
    WebSocketSend {
        socket_id: u64,
        payload: Vec<u8>,
        binary: bool,
    },
    /// Close a broker-owned handle.
    Close { handle_id: u64 },
}

/// Trusted host ingress kind. Guests cannot construct an ingress envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrustedIngressKind {
    /// Scheduled event selected by a trusted scheduler.
    Scheduled {
        schedule_id: String,
        scheduled_at_unix_ms: u64,
    },
    /// Queue delivery selected by a trusted queue consumer.
    Queue { queue: String, message_id: String },
}

/// Host-created scheduled or queue ingress delivered to a fresh request VM.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedIngress {
    identity: RequestIdentity,
    kind: TrustedIngressKind,
    payload: Vec<u8>,
}

impl TrustedIngress {
    /// Create scheduled ingress at the trusted host boundary.
    pub fn scheduled(
        identity: RequestIdentity,
        schedule_id: impl Into<String>,
        scheduled_at_unix_ms: u64,
        payload: Vec<u8>,
        max_payload_bytes: u64,
    ) -> Result<Self, BrokerContractError> {
        let schedule_id = schedule_id.into();
        validate_identity_component("schedule_id", &schedule_id)?;
        validate_payload(&payload, max_payload_bytes)?;
        Ok(Self {
            identity,
            kind: TrustedIngressKind::Scheduled {
                schedule_id,
                scheduled_at_unix_ms,
            },
            payload,
        })
    }

    /// Create queue ingress at the trusted host boundary.
    pub fn queue(
        identity: RequestIdentity,
        queue: impl Into<String>,
        message_id: impl Into<String>,
        payload: Vec<u8>,
        max_payload_bytes: u64,
    ) -> Result<Self, BrokerContractError> {
        let queue = queue.into();
        let message_id = message_id.into();
        validate_identity_component("queue", &queue)?;
        validate_identity_component("message_id", &message_id)?;
        validate_payload(&payload, max_payload_bytes)?;
        Ok(Self {
            identity,
            kind: TrustedIngressKind::Queue { queue, message_id },
            payload,
        })
    }

    /// Identity shared with policy, quota, and audit records.
    pub fn identity(&self) -> &RequestIdentity {
        &self.identity
    }

    /// Trusted ingress metadata.
    pub fn kind(&self) -> &TrustedIngressKind {
        &self.kind
    }

    /// Bounded, credential-free event payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

fn validate_payload(payload: &[u8], max_payload_bytes: u64) -> Result<(), BrokerContractError> {
    if payload.len() as u64 > max_payload_bytes {
        return Err(BrokerContractError::PayloadTooLarge {
            actual: payload.len() as u64,
            limit: max_payload_bytes,
        });
    }
    Ok(())
}

/// Complete per-request budget set. Every field is host-selected and finite.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerLimits {
    /// Simultaneously open connected transports.
    pub max_connections: u32,
    /// Simultaneously allocated broker socket handles.
    pub max_sockets: u32,
    /// Datagram count over the request lifetime.
    pub max_datagrams: u64,
    /// Decoded bytes in one UDP datagram.
    pub max_datagram_bytes: u64,
    /// WebSocket message count over the request lifetime.
    pub max_messages: u64,
    /// Decoded bytes in one complete WebSocket message.
    pub max_message_bytes: u64,
    /// Aggregate application payload bytes over the request lifetime.
    pub max_bytes: u64,
    /// Maximum monotonic time since request assignment.
    pub max_elapsed: Duration,
    /// Broker operations over the request lifetime.
    pub max_operations: u64,
    /// Broker operations allowed in one rate window.
    pub max_operations_per_window: u32,
    /// Deterministic rate-accounting window.
    pub rate_window: Duration,
    /// Simultaneously executing broker operations.
    pub max_concurrency: u32,
}

impl BrokerLimits {
    /// Validate that every budget is finite and non-zero.
    pub fn validate(self) -> Result<Self, BrokerContractError> {
        if self.max_connections == 0
            || self.max_sockets == 0
            || self.max_datagrams == 0
            || self.max_datagram_bytes == 0
            || self.max_messages == 0
            || self.max_message_bytes == 0
            || self.max_bytes == 0
            || self.max_elapsed.is_zero()
            || self.max_operations == 0
            || self.max_operations_per_window == 0
            || self.rate_window.is_zero()
            || self.max_concurrency == 0
        {
            return Err(BrokerContractError::UnboundedOrZeroLimit);
        }
        Ok(self)
    }
}

/// Resource mutation charged to one request budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetEvent {
    OpenConnection,
    CloseConnection,
    OpenSocket,
    CloseSocket,
    Datagram { bytes: u64 },
    Message { bytes: u64 },
    BeginOperation,
    EndOperation,
}

/// Deterministic per-request budget accountant.
///
/// Callers supply elapsed monotonic time, making policy tests independent of
/// wall-clock scheduling. `reset_for_fresh_vm` clears every host-side counter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerBudget {
    limits: BrokerLimits,
    generation: u64,
    connections: u32,
    sockets: u32,
    datagrams: u64,
    messages: u64,
    bytes: u64,
    operations: u64,
    concurrency: u32,
    window_started: Duration,
    window_operations: u32,
}

/// Read-only budget usage included in host audit events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetSnapshot {
    /// Fresh-VM generation.
    pub generation: u64,
    /// Currently open connected transports.
    pub connections: u32,
    /// Currently allocated broker handles.
    pub sockets: u32,
    /// Charged UDP datagrams.
    pub datagrams: u64,
    /// Charged WebSocket messages.
    pub messages: u64,
    /// Aggregate charged application payload bytes.
    pub bytes: u64,
    /// Charged broker operations.
    pub operations: u64,
    /// Currently executing broker operations.
    pub concurrency: u32,
}

impl BrokerBudget {
    /// Create a budget accountant from validated limits.
    pub fn new(limits: BrokerLimits) -> Result<Self, BrokerContractError> {
        Ok(Self {
            limits: limits.validate()?,
            generation: 0,
            connections: 0,
            sockets: 0,
            datagrams: 0,
            messages: 0,
            bytes: 0,
            operations: 0,
            concurrency: 0,
            window_started: Duration::ZERO,
            window_operations: 0,
        })
    }

    /// Charge one event at a caller-provided elapsed monotonic time.
    pub fn charge(&mut self, event: BudgetEvent, elapsed: Duration) -> Result<(), BudgetExceeded> {
        if elapsed > self.limits.max_elapsed {
            return Err(BudgetExceeded::Time);
        }
        if elapsed.saturating_sub(self.window_started) >= self.limits.rate_window {
            self.window_started = elapsed;
            self.window_operations = 0;
        }

        match event {
            BudgetEvent::OpenConnection => {
                check_increment_u32(
                    self.connections,
                    1,
                    self.limits.max_connections,
                    BudgetExceeded::Connections,
                )?;
                self.connections += 1;
            }
            BudgetEvent::CloseConnection => {
                self.connections = self.connections.saturating_sub(1);
            }
            BudgetEvent::OpenSocket => {
                check_increment_u32(
                    self.sockets,
                    1,
                    self.limits.max_sockets,
                    BudgetExceeded::Sockets,
                )?;
                self.sockets += 1;
            }
            BudgetEvent::CloseSocket => {
                self.sockets = self.sockets.saturating_sub(1);
            }
            BudgetEvent::Datagram { bytes } => {
                if bytes > self.limits.max_datagram_bytes {
                    return Err(BudgetExceeded::DatagramBytes);
                }
                check_increment_u64(
                    self.datagrams,
                    1,
                    self.limits.max_datagrams,
                    BudgetExceeded::Datagrams,
                )?;
                self.charge_bytes(bytes)?;
                self.datagrams += 1;
            }
            BudgetEvent::Message { bytes } => {
                if bytes > self.limits.max_message_bytes {
                    return Err(BudgetExceeded::MessageBytes);
                }
                check_increment_u64(
                    self.messages,
                    1,
                    self.limits.max_messages,
                    BudgetExceeded::Messages,
                )?;
                self.charge_bytes(bytes)?;
                self.messages += 1;
            }
            BudgetEvent::BeginOperation => {
                check_increment_u64(
                    self.operations,
                    1,
                    self.limits.max_operations,
                    BudgetExceeded::Operations,
                )?;
                check_increment_u32(
                    self.window_operations,
                    1,
                    self.limits.max_operations_per_window,
                    BudgetExceeded::Rate,
                )?;
                check_increment_u32(
                    self.concurrency,
                    1,
                    self.limits.max_concurrency,
                    BudgetExceeded::Concurrency,
                )?;
                self.operations += 1;
                self.window_operations += 1;
                self.concurrency += 1;
            }
            BudgetEvent::EndOperation => {
                self.concurrency = self.concurrency.saturating_sub(1);
            }
        }
        Ok(())
    }

    fn charge_bytes(&mut self, bytes: u64) -> Result<(), BudgetExceeded> {
        check_increment_u64(
            self.bytes,
            bytes,
            self.limits.max_bytes,
            BudgetExceeded::Bytes,
        )?;
        self.bytes += bytes;
        Ok(())
    }

    /// Clear all request-scoped state before assigning a fresh VM.
    pub fn reset_for_fresh_vm(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.connections = 0;
        self.sockets = 0;
        self.datagrams = 0;
        self.messages = 0;
        self.bytes = 0;
        self.operations = 0;
        self.concurrency = 0;
        self.window_started = Duration::ZERO;
        self.window_operations = 0;
    }

    /// Number of fresh-VM resets observed by this accountant.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Capture current usage without exposing mutable accounting state.
    pub fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot {
            generation: self.generation,
            connections: self.connections,
            sockets: self.sockets,
            datagrams: self.datagrams,
            messages: self.messages,
            bytes: self.bytes,
            operations: self.operations,
            concurrency: self.concurrency,
        }
    }

    pub(crate) fn finish_operation(&mut self) {
        self.concurrency = self.concurrency.saturating_sub(1);
    }

    pub(crate) fn release_connection(&mut self) {
        self.connections = self.connections.saturating_sub(1);
    }

    pub(crate) fn release_socket(&mut self) {
        self.sockets = self.sockets.saturating_sub(1);
    }
}

fn check_increment_u32(
    current: u32,
    increment: u32,
    limit: u32,
    error: BudgetExceeded,
) -> Result<(), BudgetExceeded> {
    if increment > limit.saturating_sub(current) {
        Err(error)
    } else {
        Ok(())
    }
}

fn check_increment_u64(
    current: u64,
    increment: u64,
    limit: u64,
    error: BudgetExceeded,
) -> Result<(), BudgetExceeded> {
    if increment > limit.saturating_sub(current) {
        Err(error)
    } else {
        Ok(())
    }
}

/// Policy denial reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerDenied {
    HostLocalAddress,
    NoMatchingRule,
}

impl fmt::Display for BrokerDenied {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostLocalAddress => formatter.write_str("host-local address denied"),
            Self::NoMatchingRule => formatter.write_str("no matching broker allow rule"),
        }
    }
}

impl std::error::Error for BrokerDenied {}

/// Resource whose finite budget was exhausted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetExceeded {
    Connections,
    Sockets,
    Datagrams,
    DatagramBytes,
    Messages,
    MessageBytes,
    Bytes,
    Time,
    Operations,
    Rate,
    Concurrency,
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker {:?} budget exceeded", self)
    }
}

impl std::error::Error for BudgetExceeded {}

/// Invalid broker contract supplied before execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerContractError {
    InvalidDnsName(String),
    InvalidPort(u16),
    InvalidPortRange { start: u16, end: u16 },
    EmptyRulePorts,
    EmptyRuleProtocols,
    InvalidIdentity { field: &'static str },
    InvalidAlpn,
    InvalidWebSocketSubprotocol,
    PayloadTooLarge { actual: u64, limit: u64 },
    UnboundedOrZeroLimit,
}

impl fmt::Display for BrokerContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid broker contract: {:?}", self)
    }
}

impl std::error::Error for BrokerContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    const POLICY_CASES: &str = include_str!("../tests/fixtures/broker_policy_cases.tsv");

    fn endpoint(name: &str, port: u16) -> BrokerEndpoint {
        BrokerEndpoint::new(EndpointHost::Dns(name.parse().unwrap()), port).unwrap()
    }

    fn limits() -> BrokerLimits {
        BrokerLimits {
            max_connections: 1,
            max_sockets: 1,
            max_datagrams: 1,
            max_datagram_bytes: 8,
            max_messages: 1,
            max_message_bytes: 8,
            max_bytes: 8,
            max_elapsed: Duration::from_secs(5),
            max_operations: 2,
            max_operations_per_window: 1,
            rate_window: Duration::from_secs(1),
            max_concurrency: 1,
        }
    }

    #[test]
    fn deny_all_policy_rejects_egress_and_dns() {
        let policy = BrokerPolicy::deny_all();
        let name: DnsName = "api.example.com".parse().unwrap();

        assert_eq!(
            (
                policy.authorize(BrokerProtocol::Tls, &endpoint(name.as_str(), 443)),
                policy
                    .dns()
                    .allows(&name, IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))),
            ),
            (Err(BrokerDenied::NoMatchingRule), false)
        );
    }

    #[test]
    fn allow_rule_is_host_port_and_protocol_specific() {
        let rule = EgressRule::new(
            HostRule::DnsSubdomains("example.com".parse().unwrap()),
            vec![PortRange::new(443, 443).unwrap()],
            vec![BrokerProtocol::Tls, BrokerProtocol::WebSocketSecure],
        )
        .unwrap();
        let policy = BrokerPolicy::new(vec![rule], DnsPolicy::Deny);

        assert_eq!(
            (
                policy.authorize(
                    BrokerProtocol::WebSocketSecure,
                    &endpoint("events.example.com", 443),
                ),
                policy.authorize(BrokerProtocol::Tcp, &endpoint("events.example.com", 443)),
                policy.authorize(BrokerProtocol::Tls, &endpoint("example.com", 443)),
            ),
            (
                Ok(()),
                Err(BrokerDenied::NoMatchingRule),
                Err(BrokerDenied::NoMatchingRule),
            )
        );
    }

    #[test]
    fn policy_always_rejects_host_local_addresses() {
        let rule = EgressRule::new(
            HostRule::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            vec![PortRange::new(1, u16::MAX).unwrap()],
            vec![BrokerProtocol::Tcp],
        )
        .unwrap();
        let policy = BrokerPolicy::new(vec![rule], DnsPolicy::Deny);
        let endpoint =
            BrokerEndpoint::new(EndpointHost::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)), 8080).unwrap();

        assert_eq!(
            policy.authorize(BrokerProtocol::Tcp, &endpoint),
            Err(BrokerDenied::HostLocalAddress)
        );
    }

    #[test]
    fn budget_enforces_rate_and_bytes_without_partial_charge() {
        let mut budget = BrokerBudget::new(limits()).unwrap();
        budget
            .charge(BudgetEvent::BeginOperation, Duration::ZERO)
            .unwrap();
        budget
            .charge(BudgetEvent::EndOperation, Duration::ZERO)
            .unwrap();
        let rate = budget.charge(BudgetEvent::BeginOperation, Duration::ZERO);
        budget
            .charge(BudgetEvent::Message { bytes: 8 }, Duration::from_secs(1))
            .unwrap();
        let bytes = budget.charge(BudgetEvent::Datagram { bytes: 1 }, Duration::from_secs(1));

        assert_eq!(
            (rate, bytes),
            (Err(BudgetExceeded::Rate), Err(BudgetExceeded::Bytes))
        );
    }

    #[test]
    fn budget_enforces_per_datagram_and_message_sizes() {
        let mut budget = BrokerBudget::new(limits()).unwrap();

        assert_eq!(
            (
                budget.charge(BudgetEvent::Datagram { bytes: 9 }, Duration::ZERO),
                budget.charge(BudgetEvent::Message { bytes: 9 }, Duration::ZERO),
            ),
            (
                Err(BudgetExceeded::DatagramBytes),
                Err(BudgetExceeded::MessageBytes),
            )
        );
    }

    #[test]
    fn budget_enforces_concurrency_independently_of_rate() {
        let mut limits = limits();
        limits.max_operations_per_window = 2;
        let mut budget = BrokerBudget::new(limits).unwrap();
        budget
            .charge(BudgetEvent::BeginOperation, Duration::ZERO)
            .unwrap();

        assert_eq!(
            budget.charge(BudgetEvent::BeginOperation, Duration::ZERO),
            Err(BudgetExceeded::Concurrency)
        );
    }

    #[test]
    fn fresh_vm_reset_clears_every_budget_dimension() {
        let mut budget = BrokerBudget::new(limits()).unwrap();
        budget
            .charge(BudgetEvent::OpenConnection, Duration::ZERO)
            .unwrap();
        budget
            .charge(BudgetEvent::OpenSocket, Duration::ZERO)
            .unwrap();
        budget
            .charge(BudgetEvent::BeginOperation, Duration::ZERO)
            .unwrap();
        budget
            .charge(BudgetEvent::Message { bytes: 8 }, Duration::ZERO)
            .unwrap();
        budget.reset_for_fresh_vm();

        assert_eq!(
            (
                budget.generation(),
                budget.charge(BudgetEvent::OpenConnection, Duration::ZERO),
                budget.charge(BudgetEvent::OpenSocket, Duration::ZERO),
                budget.charge(BudgetEvent::BeginOperation, Duration::ZERO),
                budget.charge(BudgetEvent::Message { bytes: 8 }, Duration::ZERO),
            ),
            (1, Ok(()), Ok(()), Ok(()), Ok(()))
        );
    }

    #[test]
    fn trusted_ingress_rejects_oversized_payload() {
        let identity = RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap();

        assert_eq!(
            TrustedIngress::queue(identity, "jobs", "msg-1", vec![0; 9], 8),
            Err(BrokerContractError::PayloadTooLarge {
                actual: 9,
                limit: 8,
            })
        );
    }

    #[test]
    fn tls_profile_contains_no_guest_credentials() {
        let profile = TlsProfile::new(
            "api.example.com".parse().unwrap(),
            vec!["h2".to_string()],
            TlsVersion::Tls13,
        )
        .unwrap();

        assert_eq!(profile.server_name().as_str(), "api.example.com");
    }

    #[test]
    fn policy_fixture_is_deterministic() {
        let rule = EgressRule::new(
            HostRule::DnsSubdomains("example.com".parse().unwrap()),
            vec![PortRange::new(443, 443).unwrap()],
            vec![BrokerProtocol::Tls, BrokerProtocol::WebSocketSecure],
        )
        .unwrap();
        let policy = BrokerPolicy::new(vec![rule], DnsPolicy::Deny);

        let actual: Vec<_> = POLICY_CASES
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| {
                let fields: Vec<_> = line.split('\t').collect();
                assert_eq!(fields.len(), 4, "invalid fixture row: {line}");
                let protocol = match fields[0] {
                    "tcp" => BrokerProtocol::Tcp,
                    "tls" => BrokerProtocol::Tls,
                    "wss" => BrokerProtocol::WebSocketSecure,
                    value => panic!("unknown fixture protocol: {value}"),
                };
                let endpoint = BrokerEndpoint::new(
                    EndpointHost::Dns(fields[1].parse().unwrap()),
                    fields[2].parse().unwrap(),
                )
                .unwrap();
                let allowed = policy.authorize(protocol, &endpoint).is_ok();
                format!("{}\t{}", fields[3], allowed)
            })
            .collect();

        assert_eq!(
            actual,
            vec![
                "true\ttrue",
                "true\ttrue",
                "false\tfalse",
                "false\tfalse",
                "false\tfalse",
            ]
        );
    }
}
