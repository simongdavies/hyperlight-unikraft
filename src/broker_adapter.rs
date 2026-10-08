//! Minimal host adapter from versioned guest bytes to policy-checked execution.

use crate::broker::{
    BrokerBudget, BrokerDenied, BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy,
    BrokerProtocol, BrokerRequest, BrokerRequestId, BudgetEvent, BudgetExceeded, BudgetSnapshot,
    RequestIdentity,
};
use crate::broker_wire::{
    BrokerWireError, BrokerWireResponse, BrokerWireResult, BrokerWireStatus, decode_request,
    encode_response,
};
use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

/// Audit schema version shared with logical-service event semantics.
pub const BROKER_AUDIT_VERSION: u16 = 1;

/// Stable audit action. Payload contents are never included.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerAuditAction {
    Decode,
    TcpConnect,
    TlsConnect,
    StreamSend,
    StreamReceive,
    UdpOpen,
    UdpSend,
    UdpReceive,
    WebSocketOpen,
    WebSocketSend,
    WebSocketReceive,
    Close,
}

/// Stable audit outcome aligned with guest-visible response classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerAuditOutcome {
    Ok,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

/// Host decision phase, separate from the guest-visible status class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerAuditDecision {
    DecodeDenied,
    PolicyDenied,
    QuotaDenied,
    Dispatched,
    InvalidRequest,
    AdapterError,
}

/// Credential-free host audit event emitted for every adapter invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerAuditEvent {
    version: u16,
    identity: RequestIdentity,
    request_id: Option<BrokerRequestId>,
    action: BrokerAuditAction,
    decision: BrokerAuditDecision,
    outcome: BrokerAuditOutcome,
    code: String,
    target: Option<BrokerEndpoint>,
    request_bytes: u64,
    response_bytes: u64,
    usage: BudgetSnapshot,
}

impl BrokerAuditEvent {
    /// Audit schema version.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Host-owned identity supplied out-of-band from guest bytes.
    pub fn identity(&self) -> &RequestIdentity {
        &self.identity
    }

    /// Guest correlation ID, when decoding reached that field.
    pub fn request_id(&self) -> Option<&BrokerRequestId> {
        self.request_id.as_ref()
    }

    /// Attempted broker action.
    pub fn action(&self) -> BrokerAuditAction {
        self.action
    }

    /// Host decision phase.
    pub fn decision(&self) -> BrokerAuditDecision {
        self.decision
    }

    /// Stable result class.
    pub fn outcome(&self) -> BrokerAuditOutcome {
        self.outcome
    }

    /// Stable non-secret category code.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Requested destination, absent for malformed, send, or close operations.
    pub fn target(&self) -> Option<&BrokerEndpoint> {
        self.target.as_ref()
    }

    /// Guest request envelope bytes.
    pub fn request_bytes(&self) -> u64 {
        self.request_bytes
    }

    /// Guest response envelope bytes.
    pub fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Usage after the operation completed or was rejected.
    pub fn usage(&self) -> BudgetSnapshot {
        self.usage
    }

    /// Canonical tab-separated audit record.
    ///
    /// Field order is fixed as: version, workload, snapshot, attempt,
    /// request ID, action, decision, outcome, code, target host, target port,
    /// request bytes, response bytes, generation, connections, sockets,
    /// datagrams, messages, bytes, operations, concurrency. Payloads and host
    /// error text are never included.
    pub fn encode_line(&self) -> String {
        let (target_host, target_port) = self
            .target
            .as_ref()
            .map(|endpoint| {
                let host = match endpoint.host() {
                    crate::broker::EndpointHost::Ip(IpAddr::V4(ip)) => ip.to_string(),
                    crate::broker::EndpointHost::Ip(IpAddr::V6(ip)) => ip.to_string(),
                    crate::broker::EndpointHost::Dns(name) => name.as_str().to_string(),
                };
                (host, endpoint.port())
            })
            .unwrap_or_else(|| (String::new(), 0));
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.version,
            self.identity.workload_id(),
            self.identity.snapshot_id(),
            self.identity.attempt(),
            self.request_id
                .as_ref()
                .map(BrokerRequestId::as_str)
                .unwrap_or(""),
            audit_action_token(self.action),
            audit_decision_token(self.decision),
            audit_outcome_token(self.outcome),
            self.code,
            target_host,
            target_port,
            self.request_bytes,
            self.response_bytes,
            self.usage.generation,
            self.usage.connections,
            self.usage.sockets,
            self.usage.datagrams,
            self.usage.messages,
            self.usage.bytes,
            self.usage.operations,
            self.usage.concurrency,
        )
    }
}

/// Adapter output returned to the dispatch bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerAdapterOutput {
    response: Vec<u8>,
    audit: BrokerAuditEvent,
}

impl BrokerAdapterOutput {
    /// Versioned response bytes returned to the guest.
    pub fn response(&self) -> &[u8] {
        &self.response
    }

    /// Host-only audit event.
    pub fn audit(&self) -> &BrokerAuditEvent {
        &self.audit
    }
}

/// Successful executor result. It contains no host socket or credential object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerExecution {
    Opened { handle_id: u64 },
    Transferred { bytes: u64 },
    Received { payload: Vec<u8>, binary: bool },
    Closed,
}

/// Stable host execution error category. Raw host error text stays in host logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerHostError {
    code: &'static str,
}

impl BrokerHostError {
    /// Construct a stable ASCII category code.
    pub fn new(code: &'static str) -> Self {
        let code = if code.is_empty()
            || code.len() > 128
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            "host_error"
        } else {
            code
        };
        Self { code }
    }

    /// Category exposed to metrics, audit, and the guest.
    pub fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Display for BrokerHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for BrokerHostError {}

/// Host protocol implementation injected beneath policy and quota enforcement.
pub trait BrokerExecutor {
    fn checkpoint_next_handle(&self) -> Option<u64> {
        None
    }
    fn restore_next_handle(&mut self, _next: u64) -> Result<(), BrokerHostError> {
        Err(BrokerHostError::new("handle_reconstruction_unsupported"))
    }
    /// Concrete transports clamp blocking operations to the admitted deadline.
    fn set_deadline(&mut self, _deadline: std::time::Instant) {}
    /// Execute one already-authorized and already-charged operation.
    fn execute(&mut self, operation: &BrokerOperation) -> Result<BrokerExecution, BrokerHostError>;

    /// Drop every host resource before assigning a fresh request VM.
    fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleKind {
    Tcp,
    Tls,
    Udp,
    WebSocket,
}

/// Stateful request-scoped adapter.
pub struct BrokerAdapter<E> {
    policy: BrokerPolicy,
    budget: BrokerBudget,
    executor: E,
    handles: HashMap<u64, HandleKind>,
    poisoned: bool,
}

impl<E: BrokerExecutor> BrokerAdapter<E> {
    pub(crate) fn checkpoint_next_handle(&self) -> Option<u64> {
        self.executor.checkpoint_next_handle()
    }
    pub(crate) fn restore_next_handle(&mut self, next: u64) -> Result<(), BrokerHostError> {
        self.executor.restore_next_handle(next)
    }
    pub(crate) fn set_deadline(&mut self, deadline: std::time::Instant) {
        self.executor.set_deadline(deadline);
    }
    pub(crate) fn checkpoint_quiescent(&self) -> bool {
        self.handles.is_empty() && !self.poisoned
    }

    /// Construct an adapter with explicit policy and finite limits.
    pub fn new(
        policy: BrokerPolicy,
        limits: BrokerLimits,
        executor: E,
    ) -> Result<Self, crate::broker::BrokerContractError> {
        Ok(Self {
            policy,
            budget: BrokerBudget::new(limits)?,
            executor,
            handles: HashMap::new(),
            poisoned: false,
        })
    }

    /// Decode, authorize, charge, execute, and audit one guest request.
    ///
    /// `identity` is supplied by the trusted host dispatcher and is never read
    /// from `input`.
    pub fn handle_wire(
        &mut self,
        input: &[u8],
        identity: &RequestIdentity,
        elapsed: Duration,
    ) -> Result<BrokerAdapterOutput, BrokerWireError> {
        if self.poisoned {
            return self.output(
                identity,
                None,
                BrokerAuditAction::Decode,
                BrokerAuditDecision::AdapterError,
                BrokerAuditOutcome::HostError,
                "adapter_poisoned",
                None,
                input.len() as u64,
                BrokerWireStatus::HostError,
                BrokerWireResult::None,
            );
        }
        let request = match decode_request(input) {
            Ok(request) => request,
            Err(_) => {
                return self.output(
                    identity,
                    None,
                    BrokerAuditAction::Decode,
                    BrokerAuditDecision::DecodeDenied,
                    BrokerAuditOutcome::InvalidRequest,
                    "invalid_request",
                    None,
                    input.len() as u64,
                    BrokerWireStatus::InvalidRequest,
                    BrokerWireResult::None,
                );
            }
        };
        self.handle_request(request, identity, elapsed, input.len() as u64)
    }

    /// Reset both executor resources and accounting for a fresh request VM.
    pub fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
        if let Err(error) = self.executor.reset_for_fresh_vm() {
            self.poisoned = true;
            return Err(error);
        }
        self.handles.clear();
        self.budget.reset_for_fresh_vm();
        self.poisoned = false;
        Ok(())
    }

    /// Current request-scoped usage.
    pub fn usage(&self) -> BudgetSnapshot {
        self.budget.snapshot()
    }

    /// Borrow the executor for host-owned inspection or integration.
    pub fn executor(&self) -> &E {
        &self.executor
    }

    fn handle_request(
        &mut self,
        request: BrokerRequest,
        identity: &RequestIdentity,
        elapsed: Duration,
        request_bytes: u64,
    ) -> Result<BrokerAdapterOutput, BrokerWireError> {
        let request_id = request.request_id().clone();
        let (action, target) = audit_context(request.operation());

        if let Err(denied) = self.authorize(request.operation()) {
            return self.output(
                identity,
                Some(request_id),
                action,
                BrokerAuditDecision::PolicyDenied,
                BrokerAuditOutcome::Denied,
                denied_code(denied),
                target,
                request_bytes,
                BrokerWireStatus::Denied,
                BrokerWireResult::None,
            );
        }

        let reservation = match self.reserve(elapsed) {
            Ok(reservation) => reservation,
            Err(exceeded) => {
                return self.output(
                    identity,
                    Some(request_id),
                    action,
                    BrokerAuditDecision::QuotaDenied,
                    BrokerAuditOutcome::QuotaExceeded,
                    quota_code(exceeded),
                    target,
                    request_bytes,
                    BrokerWireStatus::QuotaExceeded,
                    BrokerWireResult::None,
                );
            }
        };

        let decision = self.execute_charged(request.operation(), elapsed);
        self.settle(reservation);

        match decision {
            Ok(result) => self.output(
                identity,
                Some(request_id),
                action,
                BrokerAuditDecision::Dispatched,
                BrokerAuditOutcome::Ok,
                "ok",
                target,
                request_bytes,
                BrokerWireStatus::Ok,
                execution_result(result),
            ),
            Err(AdapterRejection::Quota(exceeded)) => self.output(
                identity,
                Some(request_id),
                action,
                BrokerAuditDecision::QuotaDenied,
                BrokerAuditOutcome::QuotaExceeded,
                quota_code(exceeded),
                target,
                request_bytes,
                BrokerWireStatus::QuotaExceeded,
                BrokerWireResult::None,
            ),
            Err(AdapterRejection::Invalid(code)) => self.output(
                identity,
                Some(request_id),
                action,
                BrokerAuditDecision::InvalidRequest,
                BrokerAuditOutcome::InvalidRequest,
                code,
                target,
                request_bytes,
                BrokerWireStatus::InvalidRequest,
                BrokerWireResult::None,
            ),
            Err(AdapterRejection::Host(error)) => self.output(
                identity,
                Some(request_id),
                action,
                BrokerAuditDecision::AdapterError,
                BrokerAuditOutcome::HostError,
                error.code(),
                target,
                request_bytes,
                BrokerWireStatus::HostError,
                BrokerWireResult::None,
            ),
        }
    }

    fn reserve(&mut self, elapsed: Duration) -> Result<OperationReservation, BudgetExceeded> {
        self.budget.charge(BudgetEvent::BeginOperation, elapsed)?;
        Ok(OperationReservation)
    }

    fn settle(&mut self, _reservation: OperationReservation) {
        self.budget.finish_operation();
    }

    fn authorize(&self, operation: &BrokerOperation) -> Result<(), BrokerDenied> {
        match operation {
            BrokerOperation::TcpConnect { endpoint } => {
                self.policy.authorize(BrokerProtocol::Tcp, endpoint)
            }
            BrokerOperation::TlsConnect { endpoint, .. } => {
                self.policy.authorize(BrokerProtocol::Tls, endpoint)
            }
            BrokerOperation::StreamSend { .. }
            | BrokerOperation::StreamReceive { .. }
            | BrokerOperation::UdpSocketSend { .. }
            | BrokerOperation::UdpReceive { .. }
            | BrokerOperation::WebSocketSend { .. }
            | BrokerOperation::WebSocketReceive { .. }
            | BrokerOperation::Close { .. } => Ok(()),
            BrokerOperation::UdpOpen { endpoint } | BrokerOperation::UdpSend { endpoint, .. } => {
                self.policy.authorize(BrokerProtocol::Udp, endpoint)
            }
            BrokerOperation::WebSocketOpen {
                endpoint, secure, ..
            } => self.policy.authorize(
                if *secure {
                    BrokerProtocol::WebSocketSecure
                } else {
                    BrokerProtocol::WebSocket
                },
                endpoint,
            ),
        }
    }

    fn execute_charged(
        &mut self,
        operation: &BrokerOperation,
        elapsed: Duration,
    ) -> Result<BrokerExecution, AdapterRejection> {
        match operation {
            BrokerOperation::TcpConnect { .. } => self.open(operation, HandleKind::Tcp, elapsed),
            BrokerOperation::TlsConnect { .. } => self.open(operation, HandleKind::Tls, elapsed),
            BrokerOperation::WebSocketOpen { .. } => {
                self.open(operation, HandleKind::WebSocket, elapsed)
            }
            BrokerOperation::StreamSend { stream_id, payload } => {
                if !matches!(
                    self.handles.get(stream_id),
                    Some(HandleKind::Tcp | HandleKind::Tls)
                ) {
                    return Err(AdapterRejection::Invalid("invalid_stream_handle"));
                }
                self.budget
                    .charge(
                        BudgetEvent::Stream {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                self.transfer(operation, payload.len())
            }
            BrokerOperation::StreamReceive {
                stream_id,
                max_bytes,
            } => {
                if !matches!(
                    self.handles.get(stream_id),
                    Some(HandleKind::Tcp | HandleKind::Tls)
                ) {
                    return Err(AdapterRejection::Invalid("invalid_stream_handle"));
                }
                if *max_bytes == 0 || u64::from(*max_bytes) > self.budget.max_stream_bytes() {
                    return Err(AdapterRejection::Invalid("invalid_receive_limit"));
                }
                let result = self
                    .executor
                    .execute(operation)
                    .map_err(AdapterRejection::Host)?;
                let BrokerExecution::Received { payload, binary } = result else {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                };
                if payload.len() > *max_bytes as usize {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                }
                self.budget
                    .charge(
                        BudgetEvent::Stream {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                Ok(BrokerExecution::Received { payload, binary })
            }
            BrokerOperation::UdpOpen { .. } => {
                self.open_socket(operation, HandleKind::Udp, elapsed)
            }
            BrokerOperation::UdpSend { payload, .. } => {
                self.budget
                    .charge(
                        BudgetEvent::Datagram {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                self.transfer(operation, payload.len())
            }
            BrokerOperation::UdpSocketSend { socket_id, payload } => {
                if self.handles.get(socket_id) != Some(&HandleKind::Udp) {
                    return Err(AdapterRejection::Invalid("invalid_udp_handle"));
                }
                self.budget
                    .charge(
                        BudgetEvent::Datagram {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                self.transfer(operation, payload.len())
            }
            BrokerOperation::UdpReceive {
                socket_id,
                max_bytes,
            } => {
                if self.handles.get(socket_id) != Some(&HandleKind::Udp) {
                    return Err(AdapterRejection::Invalid("invalid_udp_handle"));
                }
                if *max_bytes == 0 || u64::from(*max_bytes) > self.budget.max_datagram_bytes() {
                    return Err(AdapterRejection::Invalid("invalid_receive_limit"));
                }
                let result = self
                    .executor
                    .execute(operation)
                    .map_err(AdapterRejection::Host)?;
                let BrokerExecution::Received { payload, binary } = result else {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                };
                if payload.len() > *max_bytes as usize {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                }
                self.budget
                    .charge(
                        BudgetEvent::Datagram {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                Ok(BrokerExecution::Received { payload, binary })
            }
            BrokerOperation::WebSocketSend {
                socket_id, payload, ..
            } => {
                if self.handles.get(socket_id) != Some(&HandleKind::WebSocket) {
                    return Err(AdapterRejection::Invalid("invalid_websocket_handle"));
                }
                self.budget
                    .charge(
                        BudgetEvent::Message {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                self.transfer(operation, payload.len())
            }
            BrokerOperation::WebSocketReceive {
                socket_id,
                max_bytes,
            } => {
                if self.handles.get(socket_id) != Some(&HandleKind::WebSocket) {
                    return Err(AdapterRejection::Invalid("invalid_websocket_handle"));
                }
                if *max_bytes == 0 || u64::from(*max_bytes) > self.budget.max_message_bytes() {
                    return Err(AdapterRejection::Invalid("invalid_receive_limit"));
                }
                let result = self
                    .executor
                    .execute(operation)
                    .map_err(AdapterRejection::Host)?;
                let BrokerExecution::Received { payload, binary } = result else {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                };
                if payload.len() > *max_bytes as usize {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                }
                self.budget
                    .charge(
                        BudgetEvent::Message {
                            bytes: payload.len() as u64,
                        },
                        elapsed,
                    )
                    .map_err(AdapterRejection::Quota)?;
                Ok(BrokerExecution::Received { payload, binary })
            }
            BrokerOperation::Close { handle_id } => {
                if !self.handles.contains_key(handle_id) {
                    return Err(AdapterRejection::Invalid("invalid_handle"));
                }
                let result = self
                    .executor
                    .execute(operation)
                    .map_err(AdapterRejection::Host)?;
                if result != BrokerExecution::Closed {
                    return Err(AdapterRejection::Host(BrokerHostError::new(
                        "invalid_executor_result",
                    )));
                }
                let Some(kind) = self.handles.remove(handle_id) else {
                    return Err(AdapterRejection::Invalid("invalid_handle"));
                };
                if kind != HandleKind::Udp {
                    self.budget.release_connection();
                }
                self.budget.release_socket();
                Ok(result)
            }
        }
    }

    fn open(
        &mut self,
        operation: &BrokerOperation,
        kind: HandleKind,
        elapsed: Duration,
    ) -> Result<BrokerExecution, AdapterRejection> {
        self.budget
            .charge(BudgetEvent::OpenSocket, elapsed)
            .map_err(AdapterRejection::Quota)?;
        if let Err(error) = self.budget.charge(BudgetEvent::OpenConnection, elapsed) {
            self.budget.release_socket();
            return Err(AdapterRejection::Quota(error));
        }

        let result = match self.executor.execute(operation) {
            Ok(result) => result,
            Err(error) => {
                self.rollback_open(elapsed);
                return Err(AdapterRejection::Host(error));
            }
        };
        let BrokerExecution::Opened { handle_id } = result else {
            self.rollback_open(elapsed);
            return Err(AdapterRejection::Host(BrokerHostError::new(
                "invalid_executor_result",
            )));
        };
        if self.handles.contains_key(&handle_id) {
            // The executor returned an identifier that already names a live
            // resource. Its new resource cannot be addressed safely, so keep
            // the conservative quota charge and reject every later operation
            // until a fresh-VM reset drops all executor resources.
            self.poisoned = true;
            return Err(AdapterRejection::Host(BrokerHostError::new(
                "duplicate_handle",
            )));
        }
        self.handles.insert(handle_id, kind);
        Ok(result)
    }

    fn transfer(
        &mut self,
        operation: &BrokerOperation,
        payload_bytes: usize,
    ) -> Result<BrokerExecution, AdapterRejection> {
        let result = self
            .executor
            .execute(operation)
            .map_err(AdapterRejection::Host)?;
        match result {
            BrokerExecution::Transferred { bytes } if bytes <= payload_bytes as u64 => Ok(result),
            _ => Err(AdapterRejection::Host(BrokerHostError::new(
                "invalid_executor_result",
            ))),
        }
    }

    fn open_socket(
        &mut self,
        operation: &BrokerOperation,
        kind: HandleKind,
        elapsed: Duration,
    ) -> Result<BrokerExecution, AdapterRejection> {
        self.budget
            .charge(BudgetEvent::OpenSocket, elapsed)
            .map_err(AdapterRejection::Quota)?;
        let result = match self.executor.execute(operation) {
            Ok(result) => result,
            Err(error) => {
                self.budget.release_socket();
                return Err(AdapterRejection::Host(error));
            }
        };
        let BrokerExecution::Opened { handle_id } = result else {
            self.budget.release_socket();
            return Err(AdapterRejection::Host(BrokerHostError::new(
                "invalid_executor_result",
            )));
        };
        if self.handles.contains_key(&handle_id) {
            self.poisoned = true;
            return Err(AdapterRejection::Host(BrokerHostError::new(
                "duplicate_handle",
            )));
        }
        self.handles.insert(handle_id, kind);
        Ok(result)
    }

    fn rollback_open(&mut self, _elapsed: Duration) {
        self.budget.release_connection();
        self.budget.release_socket();
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "canonical audit and response construction boundary"
    )]
    fn output(
        &self,
        identity: &RequestIdentity,
        request_id: Option<BrokerRequestId>,
        action: BrokerAuditAction,
        decision: BrokerAuditDecision,
        outcome: BrokerAuditOutcome,
        code: &str,
        target: Option<BrokerEndpoint>,
        request_bytes: u64,
        status: BrokerWireStatus,
        result: BrokerWireResult,
    ) -> Result<BrokerAdapterOutput, BrokerWireError> {
        let response =
            BrokerWireResponse::new(request_id.clone(), status, result, Some(code.to_string()))?;
        let response = encode_response(&response)?;
        Ok(BrokerAdapterOutput {
            audit: BrokerAuditEvent {
                version: BROKER_AUDIT_VERSION,
                identity: identity.clone(),
                request_id,
                action,
                decision,
                outcome,
                code: code.to_string(),
                target,
                request_bytes,
                response_bytes: response.len() as u64,
                usage: self.budget.snapshot(),
            },
            response,
        })
    }
}

fn audit_context(operation: &BrokerOperation) -> (BrokerAuditAction, Option<BrokerEndpoint>) {
    match operation {
        BrokerOperation::TcpConnect { endpoint } => {
            (BrokerAuditAction::TcpConnect, Some(endpoint.clone()))
        }
        BrokerOperation::TlsConnect { endpoint, .. } => {
            (BrokerAuditAction::TlsConnect, Some(endpoint.clone()))
        }
        BrokerOperation::StreamSend { .. } => (BrokerAuditAction::StreamSend, None),
        BrokerOperation::StreamReceive { .. } => (BrokerAuditAction::StreamReceive, None),
        BrokerOperation::UdpOpen { endpoint } => {
            (BrokerAuditAction::UdpOpen, Some(endpoint.clone()))
        }
        BrokerOperation::UdpSend { endpoint, .. } => {
            (BrokerAuditAction::UdpSend, Some(endpoint.clone()))
        }
        BrokerOperation::UdpSocketSend { .. } => (BrokerAuditAction::UdpSend, None),
        BrokerOperation::UdpReceive { .. } => (BrokerAuditAction::UdpReceive, None),
        BrokerOperation::WebSocketOpen { endpoint, .. } => {
            (BrokerAuditAction::WebSocketOpen, Some(endpoint.clone()))
        }
        BrokerOperation::WebSocketSend { .. } => (BrokerAuditAction::WebSocketSend, None),
        BrokerOperation::WebSocketReceive { .. } => (BrokerAuditAction::WebSocketReceive, None),
        BrokerOperation::Close { .. } => (BrokerAuditAction::Close, None),
    }
}

fn denied_code(denied: BrokerDenied) -> &'static str {
    match denied {
        BrokerDenied::HostLocalAddress => "host_local_denied",
        BrokerDenied::NoMatchingRule => "policy_denied",
    }
}

fn quota_code(exceeded: BudgetExceeded) -> &'static str {
    match exceeded {
        BudgetExceeded::Connections => "connection_quota",
        BudgetExceeded::Sockets => "socket_quota",
        BudgetExceeded::Datagrams => "datagram_quota",
        BudgetExceeded::DatagramBytes => "datagram_size_quota",
        BudgetExceeded::StreamBytes => "stream_size_quota",
        BudgetExceeded::Messages => "message_quota",
        BudgetExceeded::MessageBytes => "message_size_quota",
        BudgetExceeded::Bytes => "byte_quota",
        BudgetExceeded::Time => "time_quota",
        BudgetExceeded::Operations => "operation_quota",
        BudgetExceeded::Rate => "rate_quota",
        BudgetExceeded::Concurrency => "concurrency_quota",
    }
}

fn execution_result(result: BrokerExecution) -> BrokerWireResult {
    match result {
        BrokerExecution::Opened { handle_id } => BrokerWireResult::Opened { handle_id },
        BrokerExecution::Transferred { bytes } => BrokerWireResult::Transferred { bytes },
        BrokerExecution::Received { payload, binary } => {
            BrokerWireResult::Received { payload, binary }
        }
        BrokerExecution::Closed => BrokerWireResult::Closed,
    }
}

fn audit_action_token(action: BrokerAuditAction) -> &'static str {
    match action {
        BrokerAuditAction::Decode => "decode",
        BrokerAuditAction::TcpConnect => "tcp.connect",
        BrokerAuditAction::TlsConnect => "tls.connect",
        BrokerAuditAction::StreamSend => "stream.send",
        BrokerAuditAction::StreamReceive => "stream.receive",
        BrokerAuditAction::UdpOpen => "udp.open",
        BrokerAuditAction::UdpSend => "udp.send",
        BrokerAuditAction::UdpReceive => "udp.receive",
        BrokerAuditAction::WebSocketOpen => "websocket.open",
        BrokerAuditAction::WebSocketSend => "websocket.send",
        BrokerAuditAction::WebSocketReceive => "websocket.receive",
        BrokerAuditAction::Close => "handle.close",
    }
}

fn audit_outcome_token(outcome: BrokerAuditOutcome) -> &'static str {
    match outcome {
        BrokerAuditOutcome::Ok => "ok",
        BrokerAuditOutcome::Denied => "denied",
        BrokerAuditOutcome::QuotaExceeded => "quota_exceeded",
        BrokerAuditOutcome::InvalidRequest => "invalid_request",
        BrokerAuditOutcome::HostError => "host_error",
    }
}

fn audit_decision_token(decision: BrokerAuditDecision) -> &'static str {
    match decision {
        BrokerAuditDecision::DecodeDenied => "decode_denied",
        BrokerAuditDecision::PolicyDenied => "policy_denied",
        BrokerAuditDecision::QuotaDenied => "quota_denied",
        BrokerAuditDecision::Dispatched => "dispatched",
        BrokerAuditDecision::InvalidRequest => "invalid_request",
        BrokerAuditDecision::AdapterError => "adapter_error",
    }
}

struct OperationReservation;

enum AdapterRejection {
    Quota(BudgetExceeded),
    Invalid(&'static str),
    Host(BrokerHostError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{
        BrokerEndpoint, BrokerOperation, BrokerRequest, BrokerRequestId, DnsPolicy, EgressRule,
        EndpointHost, HostRule, PortRange, WebSocketProfile,
    };
    use crate::broker_wire::{decode_response, encode_request};

    #[derive(Default)]
    struct FakeExecutor {
        next_handle: u64,
        reset_count: u32,
    }

    impl BrokerExecutor for FakeExecutor {
        fn execute(
            &mut self,
            operation: &BrokerOperation,
        ) -> Result<BrokerExecution, BrokerHostError> {
            match operation {
                BrokerOperation::TcpConnect { .. }
                | BrokerOperation::TlsConnect { .. }
                | BrokerOperation::UdpOpen { .. }
                | BrokerOperation::WebSocketOpen { .. } => {
                    self.next_handle += 1;
                    Ok(BrokerExecution::Opened {
                        handle_id: self.next_handle,
                    })
                }
                BrokerOperation::UdpSend { payload, .. }
                | BrokerOperation::UdpSocketSend { payload, .. }
                | BrokerOperation::WebSocketSend { payload, .. } => {
                    Ok(BrokerExecution::Transferred {
                        bytes: payload.len() as u64,
                    })
                }
                BrokerOperation::StreamSend { payload, .. } => Ok(BrokerExecution::Transferred {
                    bytes: payload.len() as u64,
                }),
                BrokerOperation::StreamReceive { max_bytes, .. } => Ok(BrokerExecution::Received {
                    payload: vec![b'x'; *max_bytes as usize],
                    binary: true,
                }),
                BrokerOperation::UdpReceive { max_bytes, .. } => Ok(BrokerExecution::Received {
                    payload: vec![b'u'; *max_bytes as usize],
                    binary: true,
                }),
                BrokerOperation::WebSocketReceive { max_bytes, .. } => {
                    Ok(BrokerExecution::Received {
                        payload: vec![b'w'; *max_bytes as usize],
                        binary: false,
                    })
                }
                BrokerOperation::Close { .. } => Ok(BrokerExecution::Closed),
            }
        }

        fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
            self.reset_count += 1;
            self.next_handle = 0;
            Ok(())
        }
    }

    fn limits() -> BrokerLimits {
        BrokerLimits {
            max_connections: 2,
            max_sockets: 2,
            max_datagrams: 1,
            max_datagram_bytes: 4,
            max_stream_bytes: 4,
            max_messages: 1,
            max_message_bytes: 4,
            max_bytes: 8,
            max_elapsed: Duration::from_secs(10),
            max_operations: 8,
            max_operations_per_window: 8,
            rate_window: Duration::from_secs(1),
            max_concurrency: 1,
        }
    }

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn endpoint() -> BrokerEndpoint {
        BrokerEndpoint::new(EndpointHost::Dns("api.example.com".parse().unwrap()), 443).unwrap()
    }

    fn policy() -> BrokerPolicy {
        BrokerPolicy::new(
            vec![
                EgressRule::new(
                    HostRule::ExactDns("api.example.com".parse().unwrap()),
                    vec![PortRange::new(443, 443).unwrap()],
                    vec![
                        BrokerProtocol::Tcp,
                        BrokerProtocol::Udp,
                        BrokerProtocol::WebSocketSecure,
                    ],
                )
                .unwrap(),
            ],
            DnsPolicy::Deny,
        )
    }

    fn request(id: &str, operation: BrokerOperation) -> Vec<u8> {
        encode_request(&BrokerRequest::new(
            BrokerRequestId::new(id).unwrap(),
            operation,
        ))
        .unwrap()
    }

    #[test]
    fn adapter_applies_policy_and_emits_host_identity_audit() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        let output = adapter
            .handle_wire(
                &request(
                    "req-1",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();
        let response = decode_response(output.response()).unwrap();

        assert_eq!(
            (
                response.status(),
                response.result(),
                output.audit().identity().snapshot_id(),
                output.audit().outcome(),
                output.audit().usage().connections,
            ),
            (
                BrokerWireStatus::Ok,
                &BrokerWireResult::Opened { handle_id: 1 },
                "snapshot-1",
                BrokerAuditOutcome::Ok,
                1,
            )
        );
    }

    #[test]
    fn denied_target_never_reaches_executor_or_consumes_quota() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        let denied = BrokerEndpoint::new(
            EndpointHost::Dns("denied.example.com".parse().unwrap()),
            443,
        )
        .unwrap();
        let output = adapter
            .handle_wire(
                &request("req-2", BrokerOperation::TcpConnect { endpoint: denied }),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();

        assert_eq!(
            (
                decode_response(output.response()).unwrap().status(),
                output.audit().code(),
                adapter.usage(),
                adapter.executor().next_handle,
            ),
            (
                BrokerWireStatus::Denied,
                "policy_denied",
                BrokerBudget::new(limits()).unwrap().snapshot(),
                0,
            )
        );
    }

    #[test]
    fn websocket_messages_require_a_tracked_websocket_handle_and_quota() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        let open = adapter
            .handle_wire(
                &request(
                    "open",
                    BrokerOperation::WebSocketOpen {
                        endpoint: endpoint(),
                        secure: true,
                        profile: WebSocketProfile::new(vec!["chat".to_string()]).unwrap(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(
            decode_response(open.response()).unwrap().result(),
            &BrokerWireResult::Opened { handle_id: 1 }
        );

        let invalid = adapter
            .handle_wire(
                &request(
                    "send-invalid",
                    BrokerOperation::WebSocketSend {
                        socket_id: 99,
                        payload: vec![1],
                        binary: false,
                    },
                ),
                &identity(),
                Duration::from_secs(1),
            )
            .unwrap();
        let valid = adapter
            .handle_wire(
                &request(
                    "send",
                    BrokerOperation::WebSocketSend {
                        socket_id: 1,
                        payload: vec![1, 2, 3, 4],
                        binary: false,
                    },
                ),
                &identity(),
                Duration::from_secs(2),
            )
            .unwrap();

        assert_eq!(
            (
                decode_response(invalid.response()).unwrap().status(),
                decode_response(valid.response()).unwrap().status(),
                valid.audit().usage().messages,
            ),
            (BrokerWireStatus::InvalidRequest, BrokerWireStatus::Ok, 1)
        );
    }

    #[test]
    fn fresh_vm_reset_clears_handles_and_budget_and_resets_executor() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        adapter
            .handle_wire(
                &request(
                    "open",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();
        adapter.reset_for_fresh_vm().unwrap();

        assert_eq!(
            (
                adapter.usage().generation,
                adapter.usage().connections,
                adapter.executor().reset_count,
            ),
            (1, 0, 1)
        );
    }

    #[test]
    fn duplicate_handle_poisons_adapter_until_executor_reset() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        adapter
            .handle_wire(
                &request(
                    "open-1",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();
        adapter.executor.next_handle = 0;

        let duplicate = adapter
            .handle_wire(
                &request(
                    "open-2",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();
        let rejected = adapter
            .handle_wire(
                &request(
                    "open-3",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();

        assert_eq!(
            (
                decode_response(duplicate.response()).unwrap().status(),
                duplicate.audit().code(),
                duplicate.audit().usage().connections,
                decode_response(rejected.response()).unwrap().status(),
                rejected.audit().code(),
            ),
            (
                BrokerWireStatus::HostError,
                "duplicate_handle",
                2,
                BrokerWireStatus::HostError,
                "adapter_poisoned",
            )
        );

        adapter.reset_for_fresh_vm().unwrap();
        let recovered = adapter
            .handle_wire(
                &request(
                    "open-4",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(
            decode_response(recovered.response()).unwrap().status(),
            BrokerWireStatus::Ok
        );
    }

    #[test]
    fn datagram_size_quota_rejects_before_executor() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        let output = adapter
            .handle_wire(
                &request(
                    "udp-large",
                    BrokerOperation::UdpSend {
                        endpoint: endpoint(),
                        payload: vec![0; 5],
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();

        assert_eq!(
            (
                decode_response(output.response()).unwrap().status(),
                output.audit().code(),
                output.audit().usage().datagrams,
            ),
            (BrokerWireStatus::QuotaExceeded, "datagram_size_quota", 0,)
        );
    }

    #[test]
    fn audit_line_has_fixed_credential_free_field_order() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        let output = adapter
            .handle_wire(
                &request(
                    "req-1",
                    BrokerOperation::TcpConnect {
                        endpoint: endpoint(),
                    },
                ),
                &identity(),
                Duration::ZERO,
            )
            .unwrap();

        assert_eq!(
            output.audit().encode_line(),
            "1\tworker-a\tsnapshot-1\t0\treq-1\ttcp.connect\tdispatched\tok\tok\tapi.example.com\t443\t35\t28\t0\t1\t1\t0\t0\t0\t1\t0"
        );
    }

    #[test]
    fn malformed_wire_has_no_guest_identity_or_raw_error_in_audit() {
        let mut adapter = BrokerAdapter::new(policy(), limits(), FakeExecutor::default()).unwrap();
        let output = adapter
            .handle_wire(b"not-a-request", &identity(), Duration::ZERO)
            .unwrap();

        assert_eq!(
            (
                decode_response(output.response()).unwrap().status(),
                output.audit().request_id(),
                output.audit().identity().workload_id(),
                output.audit().code(),
            ),
            (
                BrokerWireStatus::InvalidRequest,
                None,
                "worker-a",
                "invalid_request",
            )
        );
    }
}
