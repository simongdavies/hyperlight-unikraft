use super::{
    DataError, Result, canonical_backing_path, safe_code, validate_binding, validate_key,
    validate_request_id,
};
use crate::broker::RequestIdentity;
use crate::broker_adapter::BrokerHostError;
use crate::broker_runtime::{
    LogicalInvocation, LogicalRuntimeStatus, LogicalWireError, LogicalWireService,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const KV_PROTOCOL_VERSION: u16 = 1;
const MAX_WIRE_BYTES: usize = 64 * 1024;
const MAX_LIST_LIMIT: u32 = 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvLimits {
    pub max_operations: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_value_bytes: u64,
    pub max_entries: u64,
    pub max_total_bytes: u64,
}

impl KvLimits {
    pub fn validate(self) -> Result<Self> {
        if self.max_operations == 0
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_value_bytes == 0
            || self.max_entries == 0
            || self.max_total_bytes == 0
        {
            return Err(DataError::InvalidLimit);
        }
        Ok(self)
    }
}

impl Default for KvLimits {
    fn default() -> Self {
        Self {
            max_operations: 128,
            max_request_bytes: 64 * 1024,
            max_response_bytes: 64 * 1024,
            max_value_bytes: 32 * 1024,
            max_entries: 1024,
            max_total_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvBinding {
    name: String,
    backing: Option<PathBuf>,
    read_only: bool,
    limits: KvLimits,
}

impl KvBinding {
    pub fn in_memory(name: impl Into<String>, read_only: bool, limits: KvLimits) -> Result<Self> {
        Self::new(name.into(), None, read_only, limits)
    }

    pub fn persistent(
        name: impl Into<String>,
        path: impl AsRef<Path>,
        read_only: bool,
        limits: KvLimits,
    ) -> Result<Self> {
        Self::new(
            name.into(),
            Some(canonical_backing_path(path.as_ref())?),
            read_only,
            limits,
        )
    }

    fn new(
        name: String,
        backing: Option<PathBuf>,
        read_only: bool,
        limits: KvLimits,
    ) -> Result<Self> {
        validate_binding(&name)?;
        Ok(Self {
            name,
            backing,
            read_only,
            limits: limits.validate()?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn backing(&self) -> Option<&Path> {
        self.backing.as_deref()
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub fn limits(&self) -> KvLimits {
        self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum KvOperation {
    #[serde(rename = "kv_get")]
    Get { key: String },
    #[serde(rename = "kv_put")]
    Put { key: String, value_base64: String },
    #[serde(rename = "kv_delete")]
    Delete { key: String },
    #[serde(rename = "kv_list")]
    List { prefix: String, limit: u32 },
}

impl KvOperation {
    fn name(&self) -> &'static str {
        match self {
            Self::Get { .. } => "kv_get",
            Self::Put { .. } => "kv_put",
            Self::Delete { .. } => "kv_delete",
            Self::List { .. } => "kv_list",
        }
    }

    fn validate(&self, limits: KvLimits) -> std::result::Result<(), &'static str> {
        match self {
            Self::Get { key } | Self::Delete { key } => {
                validate_key(key).map_err(|_| "invalid_key")
            }
            Self::Put { key, value_base64 } => {
                validate_key(key).map_err(|_| "invalid_key")?;
                let value = STANDARD
                    .decode(value_base64)
                    .map_err(|_| "invalid_base64")?;
                if value.len() as u64 > limits.max_value_bytes {
                    return Err("value_too_large");
                }
                Ok(())
            }
            Self::List { prefix, limit } => {
                if !prefix.is_empty() {
                    validate_key(prefix).map_err(|_| "invalid_prefix")?;
                }
                if *limit == 0 || *limit > MAX_LIST_LIMIT {
                    return Err("invalid_limit");
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvRequest {
    pub version: u16,
    pub request_id: String,
    pub binding: String,
    pub operation: KvOperation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KvStatus {
    Ok,
    NotFound,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvResponse {
    pub version: u16,
    pub request_id: String,
    pub status: KvStatus,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvDecision {
    Dispatched,
    PolicyDenied,
    QuotaDenied,
    InvalidRequest,
    HostError,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvAuditEvent {
    identity: RequestIdentity,
    request_id: String,
    binding: String,
    operation: String,
    decision: KvDecision,
    code: String,
    request_bytes: u64,
    response_bytes: u64,
}

impl KvAuditEvent {
    pub fn identity(&self) -> &RequestIdentity {
        &self.identity
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn binding(&self) -> &str {
        &self.binding
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn decision(&self) -> KvDecision {
        self.decision
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn request_bytes(&self) -> u64 {
        self.request_bytes
    }

    pub fn response_bytes(&self) -> u64 {
        self.response_bytes
    }
}

struct KvStore {
    binding: KvBinding,
    connection: Connection,
    operations: u64,
    request_bytes: u64,
    response_bytes: u64,
}

impl KvStore {
    fn open(binding: KvBinding) -> Result<Self> {
        let connection = match &binding.backing {
            Some(path) => Connection::open(path).map_err(|source| DataError::OpenBacking {
                path: path.clone(),
                source,
            })?,
            None => Connection::open_in_memory().map_err(DataError::InitializeBacking)?,
        };
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS _hluk_kv (
                    key TEXT PRIMARY KEY NOT NULL,
                    value BLOB NOT NULL
                ) WITHOUT ROWID;",
            )
            .map_err(DataError::InitializeBacking)?;
        Ok(Self {
            binding,
            connection,
            operations: 0,
            request_bytes: 0,
            response_bytes: 0,
        })
    }

    fn reset_request_budget(&mut self) {
        self.operations = 0;
        self.request_bytes = 0;
        self.response_bytes = 0;
    }

    fn reserve(&mut self, request_bytes: u64) -> std::result::Result<(), &'static str> {
        let limits = self.binding.limits;
        if self.operations >= limits.max_operations {
            return Err("operation_quota");
        }
        if self.request_bytes.saturating_add(request_bytes) > limits.max_request_bytes {
            return Err("request_bytes_quota");
        }
        self.operations += 1;
        self.request_bytes += request_bytes;
        Ok(())
    }

    fn settle(&mut self, response_bytes: u64) -> std::result::Result<(), &'static str> {
        if self.response_bytes.saturating_add(response_bytes)
            > self.binding.limits.max_response_bytes
        {
            return Err("response_bytes_quota");
        }
        self.response_bytes += response_bytes;
        Ok(())
    }

    fn execute(&mut self, operation: &KvOperation) -> KvOperationResult {
        operation.validate(self.binding.limits)?;
        match operation {
            KvOperation::Get { key } => {
                let value = self
                    .connection
                    .query_row("SELECT value FROM _hluk_kv WHERE key = ?1", [key], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .optional()
                    .map_err(|_| "sqlite_read")?;
                Ok(match value {
                    Some(value) => (KvStatus::Ok, "ok", Some(value), Vec::new()),
                    None => (KvStatus::NotFound, "not_found", None, Vec::new()),
                })
            }
            KvOperation::Put { key, value_base64 } => {
                if self.binding.read_only {
                    return Ok((KvStatus::Denied, "read_only", None, Vec::new()));
                }
                let value = STANDARD
                    .decode(value_base64)
                    .map_err(|_| "invalid_base64")?;
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|_| "sqlite_transaction")?;
                transaction
                    .execute(
                        "INSERT INTO _hluk_kv(key, value) VALUES (?1, ?2)
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        params![key, value],
                    )
                    .map_err(|_| "sqlite_write")?;
                let (entries, bytes): (u64, u64) = transaction
                    .query_row(
                        "SELECT COUNT(*),
                                COALESCE(SUM(length(CAST(key AS BLOB)) + length(value)), 0)
                         FROM _hluk_kv",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|_| "sqlite_quota_read")?;
                if entries > self.binding.limits.max_entries
                    || bytes > self.binding.limits.max_total_bytes
                {
                    return Err("storage_quota");
                }
                transaction.commit().map_err(|_| "sqlite_commit")?;
                Ok((KvStatus::Ok, "ok", None, Vec::new()))
            }
            KvOperation::Delete { key } => {
                if self.binding.read_only {
                    return Ok((KvStatus::Denied, "read_only", None, Vec::new()));
                }
                let deleted = self
                    .connection
                    .execute("DELETE FROM _hluk_kv WHERE key = ?1", [key])
                    .map_err(|_| "sqlite_write")?;
                Ok(if deleted == 0 {
                    (KvStatus::NotFound, "not_found", None, Vec::new())
                } else {
                    (KvStatus::Ok, "ok", None, Vec::new())
                })
            }
            KvOperation::List { prefix, limit } => {
                let mut statement = self
                    .connection
                    .prepare(
                        "SELECT key FROM _hluk_kv
                         WHERE substr(key, 1, length(?1)) = ?1
                         ORDER BY key
                         LIMIT ?2",
                    )
                    .map_err(|_| "sqlite_read")?;
                let keys = statement
                    .query_map(params![prefix, limit], |row| row.get(0))
                    .map_err(|_| "sqlite_read")?
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .map_err(|_| "sqlite_read")?;
                Ok((KvStatus::Ok, "ok", None, keys))
            }
        }
    }
}

pub struct KvService {
    stores: Vec<KvStore>,
    audits: Vec<KvAuditEvent>,
}

type KvOperationResult =
    std::result::Result<(KvStatus, &'static str, Option<Vec<u8>>, Vec<String>), &'static str>;

impl KvService {
    pub fn new(bindings: impl IntoIterator<Item = KvBinding>) -> Result<Self> {
        let mut stores = bindings
            .into_iter()
            .map(KvStore::open)
            .collect::<Result<Vec<_>>>()?;
        stores.sort_by(|left, right| left.binding.name.cmp(&right.binding.name));
        if stores
            .windows(2)
            .any(|pair| pair[0].binding.name == pair[1].binding.name)
        {
            return Err(DataError::InvalidBinding);
        }
        Ok(Self {
            stores,
            audits: Vec::new(),
        })
    }

    pub fn bindings(&self) -> impl Iterator<Item = &KvBinding> {
        self.stores.iter().map(|store| &store.binding)
    }

    pub fn audits(&self) -> &[KvAuditEvent] {
        &self.audits
    }

    pub fn take_audits(&mut self) -> Vec<KvAuditEvent> {
        std::mem::take(&mut self.audits)
    }

    fn parse(payload: &[u8]) -> std::result::Result<KvRequest, &'static str> {
        if payload.len() > MAX_WIRE_BYTES {
            return Err("request_too_large");
        }
        let request: KvRequest = serde_json::from_slice(payload).map_err(|_| "invalid_request")?;
        if request.version != KV_PROTOCOL_VERSION {
            return Err("unsupported_version");
        }
        validate_request_id(&request.request_id).map_err(|_| "invalid_request_id")?;
        validate_binding(&request.binding).map_err(|_| "invalid_binding")?;
        Ok(request)
    }

    fn response(
        request_id: String,
        status: KvStatus,
        code: &str,
        value: Option<Vec<u8>>,
        keys: Vec<String>,
    ) -> KvResponse {
        KvResponse {
            version: KV_PROTOCOL_VERSION,
            request_id,
            status,
            code: safe_code(code).to_string(),
            value_base64: value.map(|value| STANDARD.encode(value)),
            keys,
        }
    }

    fn encode(response: &KvResponse) -> Vec<u8> {
        serde_json::to_vec(response).unwrap_or_else(|_| {
            br#"{"version":1,"request_id":"","status":"host_error","code":"host_error"}"#.to_vec()
        })
    }

    fn rejection(status: KvStatus, code: &str) -> Vec<u8> {
        Self::encode(&Self::response(
            String::new(),
            status,
            code,
            None,
            Vec::new(),
        ))
    }

    fn audit(
        &mut self,
        identity: &RequestIdentity,
        request: &KvRequest,
        decision: KvDecision,
        code: &str,
        request_bytes: u64,
        response_bytes: u64,
    ) {
        self.audits.push(KvAuditEvent {
            identity: identity.clone(),
            request_id: request.request_id.clone(),
            binding: request.binding.clone(),
            operation: request.operation.name().to_string(),
            decision,
            code: safe_code(code).to_string(),
            request_bytes,
            response_bytes,
        });
    }
}

impl LogicalWireService for KvService {
    fn inspect(&self, payload: &[u8]) -> std::result::Result<LogicalInvocation, LogicalWireError> {
        let request = Self::parse(payload).map_err(|code| match code {
            "unsupported_version" => LogicalWireError::UnsupportedVersion,
            _ => LogicalWireError::Malformed,
        })?;
        LogicalInvocation::new(request.binding)
    }

    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        let request_bytes = payload.len() as u64;
        let request = match Self::parse(payload) {
            Ok(request) => request,
            Err(code) => {
                return Self::rejection(KvStatus::InvalidRequest, code);
            }
        };
        let Some(index) = self
            .stores
            .iter()
            .position(|store| store.binding.name == request.binding)
        else {
            let response = Self::response(
                request.request_id.clone(),
                KvStatus::Denied,
                "binding_denied",
                None,
                Vec::new(),
            );
            let encoded = Self::encode(&response);
            self.audit(
                identity,
                &request,
                KvDecision::PolicyDenied,
                "binding_denied",
                request_bytes,
                encoded.len() as u64,
            );
            return encoded;
        };

        let result: std::result::Result<(Vec<u8>, KvDecision, &'static str), _> = {
            let store = &mut self.stores[index];
            if let Err(code) = store.reserve(request_bytes) {
                Err((KvStatus::QuotaExceeded, KvDecision::QuotaDenied, code))
            } else {
                match store.execute(&request.operation) {
                    Ok((status, code, value, keys)) => {
                        let response =
                            Self::response(request.request_id.clone(), status, code, value, keys);
                        let encoded = Self::encode(&response);
                        match store.settle(encoded.len() as u64) {
                            Ok(()) => {
                                let decision = if status == KvStatus::Denied {
                                    KvDecision::PolicyDenied
                                } else {
                                    KvDecision::Dispatched
                                };
                                Ok((encoded, decision, code))
                            }
                            Err(code) => {
                                Err((KvStatus::QuotaExceeded, KvDecision::QuotaDenied, code))
                            }
                        }
                    }
                    Err(
                        code @ ("operation_quota"
                        | "request_bytes_quota"
                        | "response_bytes_quota"
                        | "value_too_large"
                        | "storage_quota"),
                    ) => Err((KvStatus::QuotaExceeded, KvDecision::QuotaDenied, code)),
                    Err(
                        code @ ("invalid_key" | "invalid_prefix" | "invalid_limit"
                        | "invalid_base64"),
                    ) => Err((KvStatus::InvalidRequest, KvDecision::InvalidRequest, code)),
                    Err(code) => Err((KvStatus::HostError, KvDecision::HostError, code)),
                }
            }
        };

        match result {
            Ok((encoded, decision, code)) => {
                self.audit(
                    identity,
                    &request,
                    decision,
                    code,
                    request_bytes,
                    encoded.len() as u64,
                );
                encoded
            }
            Err((status, decision, code)) => {
                let response =
                    Self::response(request.request_id.clone(), status, code, None, Vec::new());
                let encoded = Self::encode(&response);
                self.audit(
                    identity,
                    &request,
                    decision,
                    code,
                    request_bytes,
                    encoded.len() as u64,
                );
                encoded
            }
        }
    }

    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
        let status = match status {
            LogicalRuntimeStatus::Denied => KvStatus::Denied,
            LogicalRuntimeStatus::InvalidRequest => KvStatus::InvalidRequest,
            LogicalRuntimeStatus::HostError => KvStatus::HostError,
        };
        Self::rejection(status, code)
    }

    fn reset_for_fresh_vm(&mut self) -> std::result::Result<(), BrokerHostError> {
        self.stores
            .iter_mut()
            .for_each(KvStore::reset_request_budget);
        self.audits.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    const PUT_FIXTURE: &str = include_str!("../../tests/fixtures/data_kv_put_v1.json");

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn runtime(binding: KvBinding) -> BrokerRuntime {
        let name = binding.name().to_string();
        BrokerRuntime::deny_all(identity())
            .with_logical([name], KvService::new([binding]).unwrap())
            .unwrap()
    }

    fn request(request_id: &str, binding: &str, operation: KvOperation) -> Vec<u8> {
        serde_json::to_vec(&KvRequest {
            version: KV_PROTOCOL_VERSION,
            request_id: request_id.to_string(),
            binding: binding.to_string(),
            operation,
        })
        .unwrap()
    }

    fn response(runtime: &BrokerRuntime, request: &[u8]) -> KvResponse {
        serde_json::from_slice(&runtime.dispatch_logical(request)).unwrap()
    }

    #[test]
    fn deterministic_fixture_put_round_trips_and_persists_across_vm_reset() {
        let runtime =
            runtime(KvBinding::in_memory("settings", false, KvLimits::default()).unwrap());
        let put = response(&runtime, PUT_FIXTURE.as_bytes());
        runtime.reset_for_fresh_vm().unwrap();
        let get = response(
            &runtime,
            &request(
                "req-get",
                "settings",
                KvOperation::Get {
                    key: "theme".to_string(),
                },
            ),
        );

        assert_eq!(
            (put.status, get.status, get.value_base64.as_deref()),
            (KvStatus::Ok, KvStatus::Ok, Some("ZGFyaw=="))
        );
    }

    #[test]
    fn read_only_binding_denies_mutation_but_allows_reads() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("readonly.db");
        let writable =
            runtime(KvBinding::persistent("config", &path, false, KvLimits::default()).unwrap());
        assert_eq!(
            response(
                &writable,
                &request(
                    "seed",
                    "config",
                    KvOperation::Put {
                        key: "mode".to_string(),
                        value_base64: "c2FmZQ==".to_string(),
                    },
                ),
            )
            .status,
            KvStatus::Ok
        );
        drop(writable);
        let readonly =
            runtime(KvBinding::persistent("config", &path, true, KvLimits::default()).unwrap());
        let get = response(
            &readonly,
            &request(
                "read",
                "config",
                KvOperation::Get {
                    key: "mode".to_string(),
                },
            ),
        );
        let put = response(
            &readonly,
            &request(
                "write",
                "config",
                KvOperation::Put {
                    key: "mode".to_string(),
                    value_base64: "ZmFzdA==".to_string(),
                },
            ),
        );

        assert_eq!(
            (
                get.status,
                get.value_base64.as_deref(),
                put.status,
                put.code
            ),
            (
                KvStatus::Ok,
                Some("c2FmZQ=="),
                KvStatus::Denied,
                "read_only".to_string(),
            )
        );
    }

    #[test]
    fn operation_and_storage_quotas_fail_closed() {
        let limits = KvLimits {
            max_operations: 2,
            max_entries: 1,
            max_total_bytes: 32,
            ..KvLimits::default()
        };
        let runtime = runtime(KvBinding::in_memory("quota", false, limits).unwrap());
        let first = response(
            &runtime,
            &request(
                "first",
                "quota",
                KvOperation::Put {
                    key: "a".to_string(),
                    value_base64: "MQ==".to_string(),
                },
            ),
        );
        let storage = response(
            &runtime,
            &request(
                "second",
                "quota",
                KvOperation::Put {
                    key: "b".to_string(),
                    value_base64: "Mg==".to_string(),
                },
            ),
        );
        let operations = response(
            &runtime,
            &request(
                "third",
                "quota",
                KvOperation::Get {
                    key: "a".to_string(),
                },
            ),
        );

        assert_eq!(
            (
                first.status,
                storage.status,
                storage.code,
                operations.status,
                operations.code,
            ),
            (
                KvStatus::Ok,
                KvStatus::QuotaExceeded,
                "storage_quota".to_string(),
                KvStatus::QuotaExceeded,
                "operation_quota".to_string(),
            )
        );
    }

    #[test]
    fn storage_quota_counts_utf8_bytes() {
        let limits = KvLimits {
            max_total_bytes: 2,
            ..KvLimits::default()
        };
        let runtime = runtime(KvBinding::in_memory("quota", false, limits).unwrap());
        let result = response(
            &runtime,
            &request(
                "utf8",
                "quota",
                KvOperation::Put {
                    key: "é".to_string(),
                    value_base64: "eA==".to_string(),
                },
            ),
        );

        assert_eq!(
            (result.status, result.code),
            (KvStatus::QuotaExceeded, "storage_quota".to_string())
        );
    }

    #[test]
    fn runtime_identity_and_binding_checks_precede_dispatch() {
        let binding = KvBinding::in_memory("allowed", false, KvLimits::default()).unwrap();
        let service = KvService::new([binding]).unwrap();
        let runtime = BrokerRuntime::deny_all(identity())
            .with_logical(["allowed".to_string()], service)
            .unwrap();
        let denied_binding = response(
            &runtime,
            &request(
                "denied",
                "other",
                KvOperation::Get {
                    key: "a".to_string(),
                },
            ),
        );
        let other = RequestIdentity::new("worker-b", "snapshot-1", 0).unwrap();
        let denied_identity: KvResponse = serde_json::from_slice(&runtime.dispatch_logical_as(
            &other,
            &request(
                "identity",
                "allowed",
                KvOperation::Get {
                    key: "a".to_string(),
                },
            ),
        ))
        .unwrap();

        assert_eq!(
            (
                denied_binding.status,
                denied_binding.code,
                denied_identity.status,
                denied_identity.code,
            ),
            (
                KvStatus::Denied,
                "binding_denied".to_string(),
                KvStatus::InvalidRequest,
                "identity_mismatch".to_string(),
            )
        );
    }

    #[test]
    fn fresh_vm_reset_clears_request_budget_and_audit_buffer_only() {
        let binding = KvBinding::in_memory("state", false, KvLimits::default()).unwrap();
        let mut service = KvService::new([binding]).unwrap();
        let put = request(
            "put",
            "state",
            KvOperation::Put {
                key: "key".to_string(),
                value_base64: "dmFsdWU=".to_string(),
            },
        );
        let put_response: KvResponse =
            serde_json::from_slice(&service.dispatch(&identity(), &put)).unwrap();
        assert_eq!(
            (put_response.status, service.audits().len()),
            (KvStatus::Ok, 1)
        );

        service.reset_for_fresh_vm().unwrap();
        let get = request(
            "get",
            "state",
            KvOperation::Get {
                key: "key".to_string(),
            },
        );
        let get_response: KvResponse =
            serde_json::from_slice(&service.dispatch(&identity(), &get)).unwrap();

        assert_eq!(
            (
                service.audits().len(),
                get_response.status,
                get_response.value_base64.as_deref(),
            ),
            (1, KvStatus::Ok, Some("dmFsdWU="))
        );
    }

    #[test]
    fn list_is_canonical_and_prefix_bounded() {
        let runtime = runtime(KvBinding::in_memory("items", false, KvLimits::default()).unwrap());
        for key in ["prefix-b", "prefix_a", "prefix-a", "prefixXa", "other"] {
            let result = response(
                &runtime,
                &request(
                    key,
                    "items",
                    KvOperation::Put {
                        key: key.to_string(),
                        value_base64: "eA==".to_string(),
                    },
                ),
            );
            assert_eq!(result.status, KvStatus::Ok);
        }
        let listed = response(
            &runtime,
            &request(
                "list",
                "items",
                KvOperation::List {
                    prefix: "prefix-".to_string(),
                    limit: 10,
                },
            ),
        );

        assert_eq!(
            listed.keys,
            vec!["prefix-a".to_string(), "prefix-b".to_string()]
        );
        let literal_underscore = response(
            &runtime,
            &request(
                "literal-underscore",
                "items",
                KvOperation::List {
                    prefix: "prefix_".to_string(),
                    limit: 10,
                },
            ),
        );
        assert_eq!(literal_underscore.keys, vec!["prefix_a".to_string()]);
    }
}
