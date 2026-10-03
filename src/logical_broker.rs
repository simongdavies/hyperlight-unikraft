//! Generic trusted dispatcher seam for versioned logical-service protocols.
//!
//! This module does not define or parse Workerd's JSON ABI. It provides the
//! reusable host sequencing contract beneath any validated request type:
//! policy, reserve, typed adapter dispatch, response validation, exactly-once
//! settlement, and payload-free audit.

use crate::broker::RequestIdentity;

/// Guest-safe status classes shared by logical-service protocols.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalStatus {
    Ok,
    NotFound,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

/// Host authorization denial category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalPolicyDenial {
    Identity,
    Binding,
    Operation,
    Size,
}

/// Budget reservation failure before dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalReserveError {
    Quota(&'static str),
    Budget(&'static str),
}

/// Stable adapter failure category. Raw backing errors remain host-local.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalAdapterError {
    code: &'static str,
}

impl LogicalAdapterError {
    /// Construct a sanitized stable category.
    pub fn new(code: &'static str) -> Self {
        Self {
            code: safe_code(code),
        }
    }

    /// Safe category for audit.
    pub fn code(self) -> &'static str {
        self.code
    }
}

/// Stable response-validation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalResponseError {
    code: &'static str,
}

impl LogicalResponseError {
    /// Construct a sanitized stable category.
    pub fn new(code: &'static str) -> Self {
        Self {
            code: safe_code(code),
        }
    }

    /// Safe category for audit.
    pub fn code(self) -> &'static str {
        self.code
    }
}

/// Validated guest request metadata. Payload values are intentionally absent.
pub trait LogicalRequest {
    /// Guest correlation ID.
    fn request_id(&self) -> &str;
    /// Logical binding selected by the validated request.
    fn binding(&self) -> &str;
    /// Canonical operation name.
    fn operation(&self) -> &str;
    /// Canonical encoded request size.
    fn encoded_bytes(&self) -> u64;
}

/// Canonical adapter response metadata.
pub trait LogicalResponse {
    /// Correlation ID that must match the request.
    fn request_id(&self) -> &str;
    /// Guest-safe status.
    fn status(&self) -> LogicalStatus;
    /// Canonical encoded response size.
    fn encoded_bytes(&self) -> u64;
}

/// Deny-by-default logical policy.
pub trait LogicalPolicy<R: LogicalRequest> {
    /// Authorize identity, binding, operation, and request size.
    fn authorize(&self, identity: &RequestIdentity, request: &R)
    -> Result<(), LogicalPolicyDenial>;
}

/// Request-scoped budget with opaque reservations and exactly-once settlement.
pub trait LogicalBudget<R: LogicalRequest> {
    /// Opaque token consumed by exactly one settlement.
    type Reservation;

    /// Reserve request-scoped quota before adapter dispatch.
    fn try_reserve(
        &mut self,
        identity: &RequestIdentity,
        request: &R,
        request_bytes: u64,
    ) -> Result<Self::Reservation, LogicalReserveError>;

    /// Settle one issued reservation exactly once.
    fn settle(
        &mut self,
        reservation: Self::Reservation,
        status: LogicalStatus,
        response_bytes: u64,
    ) -> Result<(), &'static str>;
}

/// Typed service adapter. Credentials and backing details are not parameters.
pub trait LogicalServiceAdapter<R: LogicalRequest, S: LogicalResponse> {
    /// Dispatch a typed request without credentials or backing details.
    fn dispatch(
        &mut self,
        identity: &RequestIdentity,
        request: &R,
    ) -> Result<S, LogicalAdapterError>;
}

/// Canonical response and request-ID correlation validator.
pub trait LogicalResponseValidator<R: LogicalRequest, S: LogicalResponse> {
    /// Validate canonical shape and request-ID correlation.
    fn validate(&self, request: &R, response: &S) -> Result<(), LogicalResponseError>;
}

/// Host dispatcher decision, separate from safe status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalDecision {
    PolicyDenied,
    QuotaDenied,
    BudgetError,
    Dispatched,
    InvalidResponse,
    AdapterError,
}

/// Payload-free logical service audit v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalAuditEvent {
    identity: RequestIdentity,
    request_id: String,
    binding: String,
    operation: String,
    decision: LogicalDecision,
    status: LogicalStatus,
    code: String,
    request_bytes: u64,
    response_bytes: u64,
}

impl LogicalAuditEvent {
    /// Host-owned identity.
    pub fn identity(&self) -> &RequestIdentity {
        &self.identity
    }

    /// Guest correlation ID.
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Authorized logical binding.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Canonical operation name.
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Host decision phase.
    pub fn decision(&self) -> LogicalDecision {
        self.decision
    }

    /// Guest-safe status class.
    pub fn status(&self) -> LogicalStatus {
        self.status
    }

    /// Stable non-secret category.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Canonical request bytes.
    pub fn request_bytes(&self) -> u64 {
        self.request_bytes
    }

    /// Canonical response bytes.
    pub fn response_bytes(&self) -> u64 {
        self.response_bytes
    }
}

/// Dispatcher result. Rejected or invalid adapter responses are suppressed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalDispatchOutput<S> {
    response: Option<S>,
    audit: LogicalAuditEvent,
}

impl<S> LogicalDispatchOutput<S> {
    /// Validated response, absent for every rejection or invalid response.
    pub fn response(&self) -> Option<&S> {
        self.response.as_ref()
    }

    /// Payload-free audit event.
    pub fn audit(&self) -> &LogicalAuditEvent {
        &self.audit
    }
}

/// Generic logical-service host dispatcher.
pub struct LogicalDispatcher<P, B, A, V> {
    policy: P,
    budget: B,
    adapter: A,
    validator: V,
}

impl<P, B, A, V> LogicalDispatcher<P, B, A, V> {
    /// Construct a dispatcher from host-owned components.
    pub fn new(policy: P, budget: B, adapter: A, validator: V) -> Self {
        Self {
            policy,
            budget,
            adapter,
            validator,
        }
    }

    /// Authorize, reserve, dispatch, validate, settle, and audit one request.
    pub fn dispatch<R, S>(
        &mut self,
        identity: &RequestIdentity,
        request: &R,
    ) -> LogicalDispatchOutput<S>
    where
        R: LogicalRequest,
        S: LogicalResponse,
        P: LogicalPolicy<R>,
        B: LogicalBudget<R>,
        A: LogicalServiceAdapter<R, S>,
        V: LogicalResponseValidator<R, S>,
    {
        let request_bytes = request.encoded_bytes();
        if let Err(denial) = self.policy.authorize(identity, request) {
            return output(
                identity,
                request,
                None,
                LogicalDecision::PolicyDenied,
                LogicalStatus::Denied,
                policy_code(denial),
                0,
            );
        }

        let reservation = match self.budget.try_reserve(identity, request, request_bytes) {
            Ok(reservation) => reservation,
            Err(LogicalReserveError::Quota(code)) => {
                return output(
                    identity,
                    request,
                    None,
                    LogicalDecision::QuotaDenied,
                    LogicalStatus::QuotaExceeded,
                    safe_code(code),
                    0,
                );
            }
            Err(LogicalReserveError::Budget(code)) => {
                return output(
                    identity,
                    request,
                    None,
                    LogicalDecision::BudgetError,
                    LogicalStatus::HostError,
                    safe_code(code),
                    0,
                );
            }
        };

        match self.adapter.dispatch(identity, request) {
            Ok(response) => {
                let response_bytes = response.encoded_bytes();
                let validation = self.validator.validate(request, &response);
                let status = if validation.is_ok() {
                    response.status()
                } else {
                    LogicalStatus::HostError
                };
                if let Err(code) = self.budget.settle(reservation, status, response_bytes) {
                    return output(
                        identity,
                        request,
                        None,
                        LogicalDecision::BudgetError,
                        LogicalStatus::HostError,
                        safe_code(code),
                        response_bytes,
                    );
                }
                match validation {
                    Ok(()) => output(
                        identity,
                        request,
                        Some(response),
                        LogicalDecision::Dispatched,
                        status,
                        "ok",
                        response_bytes,
                    ),
                    Err(error) => output(
                        identity,
                        request,
                        None,
                        LogicalDecision::InvalidResponse,
                        LogicalStatus::HostError,
                        error.code(),
                        response_bytes,
                    ),
                }
            }
            Err(error) => {
                if let Err(code) = self.budget.settle(reservation, LogicalStatus::HostError, 0) {
                    return output(
                        identity,
                        request,
                        None,
                        LogicalDecision::BudgetError,
                        LogicalStatus::HostError,
                        safe_code(code),
                        0,
                    );
                }
                output(
                    identity,
                    request,
                    None,
                    LogicalDecision::AdapterError,
                    LogicalStatus::HostError,
                    error.code(),
                    0,
                )
            }
        }
    }
}

fn output<R: LogicalRequest, S>(
    identity: &RequestIdentity,
    request: &R,
    response: Option<S>,
    decision: LogicalDecision,
    status: LogicalStatus,
    code: &str,
    response_bytes: u64,
) -> LogicalDispatchOutput<S> {
    LogicalDispatchOutput {
        response,
        audit: LogicalAuditEvent {
            identity: identity.clone(),
            request_id: request.request_id().to_string(),
            binding: request.binding().to_string(),
            operation: request.operation().to_string(),
            decision,
            status,
            code: safe_code_owned(code),
            request_bytes: request.encoded_bytes(),
            response_bytes,
        },
    }
}

fn policy_code(denial: LogicalPolicyDenial) -> &'static str {
    match denial {
        LogicalPolicyDenial::Identity => "identity_denied",
        LogicalPolicyDenial::Binding => "binding_denied",
        LogicalPolicyDenial::Operation => "operation_denied",
        LogicalPolicyDenial::Size => "size_denied",
    }
}

fn safe_code(value: &'static str) -> &'static str {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        "host_error"
    } else {
        value
    }
}

fn safe_code_owned(value: &str) -> String {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        "host_error".to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Request {
        request_id: &'static str,
        binding: &'static str,
        operation: &'static str,
        bytes: u64,
    }

    impl LogicalRequest for Request {
        fn request_id(&self) -> &str {
            self.request_id
        }

        fn binding(&self) -> &str {
            self.binding
        }

        fn operation(&self) -> &str {
            self.operation
        }

        fn encoded_bytes(&self) -> u64 {
            self.bytes
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Response {
        request_id: &'static str,
        status: LogicalStatus,
        bytes: u64,
    }

    impl LogicalResponse for Response {
        fn request_id(&self) -> &str {
            self.request_id
        }

        fn status(&self) -> LogicalStatus {
            self.status
        }

        fn encoded_bytes(&self) -> u64 {
            self.bytes
        }
    }

    struct Policy;

    impl LogicalPolicy<Request> for Policy {
        fn authorize(
            &self,
            _identity: &RequestIdentity,
            request: &Request,
        ) -> Result<(), LogicalPolicyDenial> {
            if request.binding == "allowed" {
                Ok(())
            } else {
                Err(LogicalPolicyDenial::Binding)
            }
        }
    }

    #[derive(Default)]
    struct Budget {
        settlements: u32,
        last_status: Option<LogicalStatus>,
        last_response_bytes: u64,
    }

    impl LogicalBudget<Request> for Budget {
        type Reservation = u64;

        fn try_reserve(
            &mut self,
            _identity: &RequestIdentity,
            request: &Request,
            request_bytes: u64,
        ) -> Result<Self::Reservation, LogicalReserveError> {
            if request.operation == "quota" {
                Err(LogicalReserveError::Quota("request_quota"))
            } else {
                Ok(request_bytes)
            }
        }

        fn settle(
            &mut self,
            _reservation: Self::Reservation,
            status: LogicalStatus,
            response_bytes: u64,
        ) -> Result<(), &'static str> {
            self.settlements += 1;
            self.last_status = Some(status);
            self.last_response_bytes = response_bytes;
            Ok(())
        }
    }

    struct Adapter {
        response_id: &'static str,
        fail: bool,
    }

    impl LogicalServiceAdapter<Request, Response> for Adapter {
        fn dispatch(
            &mut self,
            _identity: &RequestIdentity,
            _request: &Request,
        ) -> Result<Response, LogicalAdapterError> {
            if self.fail {
                Err(LogicalAdapterError::new("adapter_error"))
            } else {
                Ok(Response {
                    request_id: self.response_id,
                    status: LogicalStatus::Ok,
                    bytes: 146,
                })
            }
        }
    }

    struct Validator;

    impl LogicalResponseValidator<Request, Response> for Validator {
        fn validate(
            &self,
            request: &Request,
            response: &Response,
        ) -> Result<(), LogicalResponseError> {
            if request.request_id() == response.request_id() {
                Ok(())
            } else {
                Err(LogicalResponseError::new("request_id_mismatch"))
            }
        }
    }

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn request() -> Request {
        Request {
            request_id: "req-1",
            binding: "allowed",
            operation: "kv.get",
            bytes: 308,
        }
    }

    #[test]
    fn valid_response_settles_once_with_fixture_byte_counts() {
        let mut dispatcher = LogicalDispatcher::new(
            Policy,
            Budget::default(),
            Adapter {
                response_id: "req-1",
                fail: false,
            },
            Validator,
        );
        let result = dispatcher.dispatch(&identity(), &request());

        assert_eq!(
            (
                result.response().is_some(),
                result.audit().decision(),
                result.audit().request_bytes(),
                result.audit().response_bytes(),
                dispatcher.budget.settlements,
                dispatcher.budget.last_response_bytes,
            ),
            (true, LogicalDecision::Dispatched, 308, 146, 1, 146)
        );
    }

    #[test]
    fn invalid_response_is_suppressed_and_settled_once() {
        let mut dispatcher = LogicalDispatcher::new(
            Policy,
            Budget::default(),
            Adapter {
                response_id: "wrong",
                fail: false,
            },
            Validator,
        );
        let result = dispatcher.dispatch(&identity(), &request());

        assert_eq!(
            (
                result.response().is_none(),
                result.audit().decision(),
                result.audit().code(),
                dispatcher.budget.settlements,
                dispatcher.budget.last_status,
            ),
            (
                true,
                LogicalDecision::InvalidResponse,
                "request_id_mismatch",
                1,
                Some(LogicalStatus::HostError),
            )
        );
    }

    #[test]
    fn adapter_error_is_settled_once_without_response_bytes() {
        let mut dispatcher = LogicalDispatcher::new(
            Policy,
            Budget::default(),
            Adapter {
                response_id: "req-1",
                fail: true,
            },
            Validator,
        );
        let result: LogicalDispatchOutput<Response> = dispatcher.dispatch(&identity(), &request());

        assert_eq!(
            (
                result.audit().decision(),
                result.audit().response_bytes(),
                dispatcher.budget.settlements,
            ),
            (LogicalDecision::AdapterError, 0, 1)
        );
    }

    #[test]
    fn policy_and_quota_denials_do_not_settle_unissued_reservations() {
        let mut denied_request = request();
        denied_request.binding = "denied";
        let mut quota_request = request();
        quota_request.operation = "quota";
        let mut dispatcher = LogicalDispatcher::new(
            Policy,
            Budget::default(),
            Adapter {
                response_id: "req-1",
                fail: false,
            },
            Validator,
        );
        let denied: LogicalDispatchOutput<Response> =
            dispatcher.dispatch(&identity(), &denied_request);
        let quota: LogicalDispatchOutput<Response> =
            dispatcher.dispatch(&identity(), &quota_request);

        assert_eq!(
            (
                denied.audit().decision(),
                quota.audit().decision(),
                dispatcher.budget.settlements,
            ),
            (
                LogicalDecision::PolicyDenied,
                LogicalDecision::QuotaDenied,
                0,
            )
        );
    }
}
