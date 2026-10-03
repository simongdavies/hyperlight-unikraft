//! Runtime registration surface for raw Hyperlight broker host calls.

use crate::broker::{BrokerRequestId, RequestIdentity};
use crate::broker_adapter::{BrokerAdapter, BrokerExecutor, BrokerHostError};
use crate::broker_wire::{BrokerWireResponse, BrokerWireResult, BrokerWireStatus, encode_response};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Raw host function for bounded TCP/TLS/UDP/WebSocket requests.
pub const NETWORK_BROKER_HOST_FUNCTION: &str = "__hl_broker_v1";

/// Raw host function used by Workerd's explicit logical-service transport.
pub const LOGICAL_BROKER_HOST_FUNCTION: &str = "WorkerdLogicalServiceV1Invoke";

trait NetworkWireService: Send {
    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8>;
    fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError>;
}

struct NetworkAdapterService<E> {
    adapter: BrokerAdapter<E>,
    started: Instant,
}

impl<E: BrokerExecutor + Send> NetworkWireService for NetworkAdapterService<E> {
    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        let elapsed = self.started.elapsed();
        self.adapter
            .handle_wire(payload, identity, elapsed)
            .map(|output| output.response().to_vec())
            .unwrap_or_else(|_| network_rejection(None, BrokerWireStatus::HostError, "host_error"))
    }

    fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
        self.adapter.reset_for_fresh_vm()?;
        self.started = Instant::now();
        Ok(())
    }
}

/// Minimal information extracted from a validated logical-service request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalInvocation {
    binding: String,
}

impl LogicalInvocation {
    /// Construct a bounded binding route.
    pub fn new(binding: impl Into<String>) -> Result<Self, LogicalWireError> {
        let binding = binding.into();
        if binding.is_empty()
            || binding.len() > 128
            || !binding.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(LogicalWireError::InvalidBinding);
        }
        Ok(Self { binding })
    }

    /// Binding selected by the canonical logical request.
    pub fn binding(&self) -> &str {
        &self.binding
    }
}

/// Logical wire inspection failure before typed adapter execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalWireError {
    Malformed,
    UnsupportedVersion,
    InvalidBinding,
}

/// Guest-safe logical rejection class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalRuntimeStatus {
    Denied,
    InvalidRequest,
    HostError,
}

/// Protocol-specific logical transport registered beneath the runtime surface.
///
/// The implementation owns canonical JSON parsing and response serialization.
pub trait LogicalWireService: Send {
    /// Inspect version and binding without invoking the service adapter.
    fn inspect(&self, payload: &[u8]) -> Result<LogicalInvocation, LogicalWireError>;

    /// Dispatch after identity and binding registration have been checked.
    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8>;

    /// Encode a non-leaking protocol-native rejection.
    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8>;

    /// Reset request-scoped transport state for a fresh VM.
    fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError>;
}

/// Cloneable runtime handle stored by a sandbox and captured by host functions.
#[derive(Clone)]
pub struct BrokerRuntime {
    identity: RequestIdentity,
    network: Option<Arc<Mutex<Box<dyn NetworkWireService>>>>,
    logical: Option<Arc<Mutex<Box<dyn LogicalWireService>>>>,
    logical_bindings: Arc<HashSet<String>>,
    poisoned: Arc<AtomicBool>,
}

impl BrokerRuntime {
    /// Create a deny-by-default runtime with no registered capability.
    pub fn deny_all(identity: RequestIdentity) -> Self {
        Self {
            identity,
            network: None,
            logical: None,
            logical_bindings: Arc::new(HashSet::new()),
            poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register the bounded network adapter.
    pub fn with_network<E>(mut self, adapter: BrokerAdapter<E>) -> Self
    where
        E: BrokerExecutor + Send + 'static,
    {
        self.network = Some(Arc::new(Mutex::new(Box::new(NetworkAdapterService {
            adapter,
            started: Instant::now(),
        }))));
        self
    }

    /// Register a logical transport and its explicit binding allowlist.
    pub fn with_logical<S>(
        mut self,
        bindings: impl IntoIterator<Item = String>,
        service: S,
    ) -> Result<Self, LogicalWireError>
    where
        S: LogicalWireService + 'static,
    {
        let bindings: HashSet<String> = bindings
            .into_iter()
            .map(LogicalInvocation::new)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|invocation| invocation.binding)
            .collect();
        self.logical = Some(Arc::new(Mutex::new(Box::new(service))));
        self.logical_bindings = Arc::new(bindings);
        Ok(self)
    }

    /// Host identity captured by both registered functions.
    pub fn identity(&self) -> &RequestIdentity {
        &self.identity
    }

    /// Whether the raw network host function should be registered.
    pub fn has_network(&self) -> bool {
        self.network.is_some()
    }

    /// Whether the logical-service host function should be registered.
    pub fn has_logical(&self) -> bool {
        self.logical.is_some()
    }

    /// Dispatch using the runtime's captured trusted identity.
    pub fn dispatch_network(&self, payload: &[u8]) -> Vec<u8> {
        self.dispatch_network_as(&self.identity, payload)
    }

    /// Dispatch with an explicitly supplied trusted identity.
    ///
    /// This is primarily for host routing and focused identity-mismatch tests;
    /// the Hyperlight closure always calls [`dispatch_network`](Self::dispatch_network).
    pub fn dispatch_network_as(&self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        if self.poisoned.load(Ordering::Acquire) {
            return network_rejection(None, BrokerWireStatus::HostError, "runtime_poisoned");
        }
        if identity != &self.identity {
            return network_rejection(None, BrokerWireStatus::InvalidRequest, "identity_mismatch");
        }
        let Some(service) = &self.network else {
            return network_rejection(
                None,
                BrokerWireStatus::InvalidRequest,
                "unregistered_capability",
            );
        };
        let Ok(mut service) = service.lock() else {
            self.poisoned.store(true, Ordering::Release);
            return network_rejection(None, BrokerWireStatus::HostError, "runtime_poisoned");
        };
        service.dispatch(identity, payload)
    }

    /// Dispatch a logical request using the captured trusted identity.
    pub fn dispatch_logical(&self, payload: &[u8]) -> Vec<u8> {
        self.dispatch_logical_as(&self.identity, payload)
    }

    /// Inspect and dispatch a logical request after identity and binding checks.
    pub fn dispatch_logical_as(&self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        if self.poisoned.load(Ordering::Acquire) {
            return Vec::new();
        }
        let Some(service) = &self.logical else {
            return Vec::new();
        };
        let Ok(mut service) = service.lock() else {
            self.poisoned.store(true, Ordering::Release);
            return Vec::new();
        };
        if identity != &self.identity {
            return service.reject(LogicalRuntimeStatus::InvalidRequest, "identity_mismatch");
        }
        let invocation = match service.inspect(payload) {
            Ok(invocation) => invocation,
            Err(LogicalWireError::UnsupportedVersion) => {
                return service.reject(LogicalRuntimeStatus::InvalidRequest, "unsupported_version");
            }
            Err(LogicalWireError::Malformed | LogicalWireError::InvalidBinding) => {
                return service.reject(LogicalRuntimeStatus::InvalidRequest, "invalid_request");
            }
        };
        if !self.logical_bindings.contains(invocation.binding()) {
            return service.reject(LogicalRuntimeStatus::Denied, "binding_denied");
        }
        service.dispatch(identity, payload)
    }

    /// Reset all registered request-scoped state before a fresh VM assignment.
    pub fn reset_for_fresh_vm(&self) -> Result<(), BrokerHostError> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(BrokerHostError::new("runtime_poisoned"));
        }
        if let Some(network) = &self.network {
            let Ok(mut network) = network.lock() else {
                self.poisoned.store(true, Ordering::Release);
                return Err(BrokerHostError::new("runtime_poisoned"));
            };
            network.reset_for_fresh_vm()?;
        }
        if let Some(logical) = &self.logical {
            let Ok(mut logical) = logical.lock() else {
                self.poisoned.store(true, Ordering::Release);
                return Err(BrokerHostError::new("runtime_poisoned"));
            };
            logical.reset_for_fresh_vm()?;
        }
        Ok(())
    }
}

fn network_rejection(
    request_id: Option<BrokerRequestId>,
    status: BrokerWireStatus,
    code: &str,
) -> Vec<u8> {
    match BrokerWireResponse::new(
        request_id,
        status,
        BrokerWireResult::None,
        Some(code.to_string()),
    )
    .and_then(|response| encode_response(&response))
    {
        Ok(response) => response,
        Err(_) => {
            let mut fallback = b"HLBR\x00\x01\x02\x00\x00\x04\x00\x00\x0a".to_vec();
            fallback.extend_from_slice(b"host_error");
            fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{
        BrokerEndpoint, BrokerLimits, BrokerOperation, BrokerPolicy, BrokerRequest, DnsPolicy,
        EgressRule, EndpointHost, HostRule, PortRange,
    };
    use crate::broker_adapter::{BrokerExecution, BrokerExecutor};
    use crate::broker_wire::{decode_response, encode_request};
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Clone)]
    struct CountingExecutor {
        calls: Arc<AtomicU32>,
    }

    impl BrokerExecutor for CountingExecutor {
        fn execute(
            &mut self,
            _operation: &BrokerOperation,
        ) -> Result<BrokerExecution, BrokerHostError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(BrokerExecution::Opened { handle_id: 1 })
        }

        fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
            Ok(())
        }
    }

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn runtime(calls: Arc<AtomicU32>) -> BrokerRuntime {
        let rule = EgressRule::new(
            HostRule::ExactDns("api.example.com".parse().unwrap()),
            vec![PortRange::new(443, 443).unwrap()],
            vec![crate::broker::BrokerProtocol::Tcp],
        )
        .unwrap();
        let limits = BrokerLimits {
            max_connections: 1,
            max_sockets: 1,
            max_datagrams: 1,
            max_datagram_bytes: 8,
            max_messages: 1,
            max_message_bytes: 8,
            max_bytes: 8,
            max_elapsed: std::time::Duration::from_secs(10),
            max_operations: 4,
            max_operations_per_window: 4,
            rate_window: std::time::Duration::from_secs(1),
            max_concurrency: 1,
        };
        let adapter = BrokerAdapter::new(
            BrokerPolicy::new(vec![rule], DnsPolicy::Deny),
            limits,
            CountingExecutor { calls },
        )
        .unwrap();
        BrokerRuntime::deny_all(identity()).with_network(adapter)
    }

    fn request() -> Vec<u8> {
        encode_request(&BrokerRequest::new(
            BrokerRequestId::new("req-1").unwrap(),
            BrokerOperation::TcpConnect {
                endpoint: BrokerEndpoint::new(
                    EndpointHost::Dns("api.example.com".parse().unwrap()),
                    443,
                )
                .unwrap(),
            },
        ))
        .unwrap()
    }

    #[test]
    fn unknown_version_opcode_and_malformed_length_do_not_execute() {
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = runtime(calls.clone());
        let mut version = request();
        version[5] = 2;
        let mut opcode = request();
        opcode[14] = 99;
        let mut length = request();
        length[7] = 0xff;
        length[8] = 0xff;

        let statuses: Vec<_> = [version, opcode, length]
            .iter()
            .map(|payload| {
                decode_response(&runtime.dispatch_network(payload))
                    .unwrap()
                    .status()
            })
            .collect();

        assert_eq!(
            (statuses, calls.load(Ordering::Relaxed)),
            (
                vec![
                    BrokerWireStatus::InvalidRequest,
                    BrokerWireStatus::InvalidRequest,
                    BrokerWireStatus::InvalidRequest,
                ],
                0,
            )
        );
    }

    #[test]
    fn identity_mismatch_is_rejected_before_network_execution() {
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = runtime(calls.clone());
        let other = RequestIdentity::new("worker-b", "snapshot-1", 0).unwrap();
        let response = decode_response(&runtime.dispatch_network_as(&other, &request())).unwrap();

        assert_eq!(
            (response.code(), calls.load(Ordering::Relaxed)),
            (Some("identity_mismatch"), 0)
        );
    }

    struct LogicalService {
        dispatches: Arc<AtomicU32>,
    }

    impl LogicalWireService for LogicalService {
        fn inspect(&self, payload: &[u8]) -> Result<LogicalInvocation, LogicalWireError> {
            let binding = std::str::from_utf8(payload).map_err(|_| LogicalWireError::Malformed)?;
            LogicalInvocation::new(binding)
        }

        fn dispatch(&mut self, _identity: &RequestIdentity, _payload: &[u8]) -> Vec<u8> {
            self.dispatches.fetch_add(1, Ordering::Relaxed);
            b"ok".to_vec()
        }

        fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
            format!("{status:?}:{code}").into_bytes()
        }

        fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
            Ok(())
        }
    }

    #[test]
    fn unregistered_logical_binding_is_denied_before_adapter_execution() {
        let dispatches = Arc::new(AtomicU32::new(0));
        let runtime = BrokerRuntime::deny_all(identity())
            .with_logical(
                ["allowed".to_string()],
                LogicalService {
                    dispatches: dispatches.clone(),
                },
            )
            .unwrap();

        assert_eq!(
            (
                String::from_utf8(runtime.dispatch_logical(b"denied")).unwrap(),
                dispatches.load(Ordering::Relaxed),
            ),
            ("Denied:binding_denied".to_string(), 0)
        );
    }

    #[test]
    fn registration_surface_is_deny_by_default_and_uses_fixed_names() {
        let calls = Arc::new(AtomicU32::new(0));
        let denied = BrokerRuntime::deny_all(identity());
        let network = runtime(calls);

        assert_eq!(
            (
                denied.has_network(),
                denied.has_logical(),
                network.has_network(),
                NETWORK_BROKER_HOST_FUNCTION,
                LOGICAL_BROKER_HOST_FUNCTION,
            ),
            (
                false,
                false,
                true,
                "__hl_broker_v1",
                "WorkerdLogicalServiceV1Invoke",
            )
        );
    }

    #[test]
    fn poisoned_service_mutex_permanently_fails_closed() {
        let calls = Arc::new(AtomicU32::new(0));
        let runtime = runtime(calls.clone());
        let network = runtime.network.as_ref().unwrap().clone();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _service = network.lock().unwrap();
            panic!("poison service mutex");
        }));
        assert!(panic.is_err());

        let response = decode_response(&runtime.dispatch_network(&request())).unwrap();
        assert_eq!(
            (
                response.status(),
                response.code(),
                calls.load(Ordering::Relaxed),
                runtime.reset_for_fresh_vm().unwrap_err().to_string(),
            ),
            (
                BrokerWireStatus::HostError,
                Some("runtime_poisoned"),
                0,
                "runtime_poisoned".to_string(),
            )
        );
    }
}
