use super::{
    DataError, Result, canonical_backing_path, safe_code, validate_binding, validate_cache_key,
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
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const CACHE_PROTOCOL_VERSION: u16 = 1;
const MAX_WIRE_BYTES: usize = 128 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_BYTES: usize = 16 * 1024;

pub trait CacheClock: Send + Sync {
    fn now_unix_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemCacheClock;

impl CacheClock for SystemCacheClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheLimits {
    pub max_operations: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_body_bytes: u64,
    pub max_entries: u64,
    pub max_total_bytes: u64,
    pub max_ttl_ms: u64,
}

impl CacheLimits {
    pub fn validate(self) -> Result<Self> {
        if self.max_operations == 0
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_body_bytes == 0
            || self.max_entries == 0
            || self.max_total_bytes == 0
            || self.max_ttl_ms == 0
        {
            return Err(DataError::InvalidLimit);
        }
        Ok(self)
    }
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            max_operations: 128,
            max_request_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_body_bytes: 64 * 1024,
            max_entries: 512,
            max_total_bytes: 8 * 1024 * 1024,
            max_ttl_ms: 24 * 60 * 60 * 1000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheBinding {
    name: String,
    backing: Option<PathBuf>,
    read_only: bool,
    limits: CacheLimits,
}

impl CacheBinding {
    pub fn in_memory(
        name: impl Into<String>,
        read_only: bool,
        limits: CacheLimits,
    ) -> Result<Self> {
        Self::new(name.into(), None, read_only, limits)
    }

    pub fn persistent(
        name: impl Into<String>,
        path: impl AsRef<Path>,
        read_only: bool,
        limits: CacheLimits,
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
        limits: CacheLimits,
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

    pub fn limits(&self) -> CacheLimits {
        self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum CacheOperation {
    #[serde(rename = "cache_match")]
    Match { key: String },
    #[serde(rename = "cache_put")]
    Put {
        key: String,
        status: u16,
        headers: BTreeMap<String, String>,
        body_base64: String,
        ttl_ms: u64,
    },
    #[serde(rename = "cache_delete")]
    Delete { key: String },
}

impl CacheOperation {
    fn validate(&self, limits: CacheLimits) -> std::result::Result<(), &'static str> {
        match self {
            Self::Match { key } | Self::Delete { key } => {
                validate_cache_key(key).map_err(|_| "invalid_key")
            }
            Self::Put {
                key,
                status,
                headers,
                body_base64,
                ttl_ms,
            } => {
                validate_cache_key(key).map_err(|_| "invalid_key")?;
                if !(200..=599).contains(status) {
                    return Err("invalid_status");
                }
                validate_headers(headers)?;
                let body = STANDARD.decode(body_base64).map_err(|_| "invalid_base64")?;
                if body.len() as u64 > limits.max_body_bytes {
                    return Err("body_too_large");
                }
                if *ttl_ms == 0 || *ttl_ms > limits.max_ttl_ms {
                    return Err("invalid_ttl");
                }
                Ok(())
            }
        }
    }
}

fn validate_headers(headers: &BTreeMap<String, String>) -> std::result::Result<(), &'static str> {
    if headers.len() > MAX_HEADERS {
        return Err("too_many_headers");
    }
    let mut bytes = 0usize;
    for (name, value) in headers {
        if name.is_empty()
            || name.len() > 256
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
            || value
                .bytes()
                .any(|byte| byte.is_ascii_control() && byte != b'\t')
        {
            return Err("invalid_header");
        }
        bytes += name.len() + value.len();
        if bytes > MAX_HEADER_BYTES {
            return Err("headers_too_large");
        }
    }
    Ok(())
}

fn canonical_headers(
    headers: &BTreeMap<String, String>,
) -> std::result::Result<BTreeMap<String, String>, &'static str> {
    validate_headers(headers)?;
    headers
        .iter()
        .try_fold(BTreeMap::new(), |mut canonical, (name, value)| {
            if canonical
                .insert(name.to_ascii_lowercase(), value.clone())
                .is_some()
            {
                return Err("duplicate_header");
            }
            Ok(canonical)
        })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheRequest {
    pub version: u16,
    pub request_id: String,
    pub binding: String,
    pub operation: CacheOperation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheStatus {
    Ok,
    NotFound,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheResponse {
    pub version: u16,
    pub request_id: String,
    pub status: CacheStatus,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_status: Option<u16>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_base64: Option<String>,
}

struct CacheStore {
    binding: CacheBinding,
    connection: Connection,
    operations: u64,
    request_bytes: u64,
    response_bytes: u64,
}

impl CacheStore {
    fn open(binding: CacheBinding) -> Result<Self> {
        let connection = match &binding.backing {
            Some(path) => Connection::open(path).map_err(|source| DataError::OpenBacking {
                path: path.clone(),
                source,
            })?,
            None => Connection::open_in_memory().map_err(DataError::InitializeBacking)?,
        };
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS _hluk_cache (
                    key TEXT PRIMARY KEY NOT NULL,
                    status INTEGER NOT NULL,
                    headers_json TEXT NOT NULL,
                    body BLOB NOT NULL,
                    expires_at_ms INTEGER NOT NULL
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

    fn reserve(&mut self, bytes: u64) -> std::result::Result<(), &'static str> {
        if self.operations >= self.binding.limits.max_operations {
            return Err("operation_quota");
        }
        if self.request_bytes.saturating_add(bytes) > self.binding.limits.max_request_bytes {
            return Err("request_bytes_quota");
        }
        self.operations += 1;
        self.request_bytes += bytes;
        Ok(())
    }

    fn settle(&mut self, bytes: u64) -> std::result::Result<(), &'static str> {
        if self.response_bytes.saturating_add(bytes) > self.binding.limits.max_response_bytes {
            return Err("response_bytes_quota");
        }
        self.response_bytes += bytes;
        Ok(())
    }

    fn execute(
        &mut self,
        operation: &CacheOperation,
        now_ms: u64,
    ) -> std::result::Result<CacheResponseData, &'static str> {
        operation.validate(self.binding.limits)?;
        match operation {
            CacheOperation::Match { key } => {
                let entry = self
                    .connection
                    .query_row(
                        "SELECT status, headers_json, body, expires_at_ms
                         FROM _hluk_cache WHERE key = ?1",
                        [key],
                        |row| {
                            Ok((
                                row.get::<_, u16>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                                row.get::<_, u64>(3)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|_| "sqlite_read")?;
                let Some((status, headers, body, expires_at_ms)) = entry else {
                    return Ok(CacheResponseData::not_found());
                };
                if expires_at_ms <= now_ms {
                    return Ok(CacheResponseData::not_found());
                }
                let headers =
                    serde_json::from_str(&headers).map_err(|_| "invalid_stored_headers")?;
                Ok(CacheResponseData {
                    status: CacheStatus::Ok,
                    code: "ok",
                    cached_status: Some(status),
                    headers,
                    body: Some(body),
                })
            }
            CacheOperation::Put {
                key,
                status,
                headers,
                body_base64,
                ttl_ms,
            } => {
                if self.binding.read_only {
                    return Ok(CacheResponseData::denied());
                }
                let body = STANDARD.decode(body_base64).map_err(|_| "invalid_base64")?;
                let headers = canonical_headers(headers)?;
                let headers_json = serde_json::to_string(&headers).map_err(|_| "invalid_header")?;
                let expires_at_ms = now_ms.checked_add(*ttl_ms).ok_or("invalid_ttl")?;
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|_| "sqlite_transaction")?;
                transaction
                    .execute(
                        "DELETE FROM _hluk_cache WHERE expires_at_ms <= ?1",
                        [now_ms],
                    )
                    .map_err(|_| "sqlite_write")?;
                transaction
                    .execute(
                        "INSERT INTO _hluk_cache(key, status, headers_json, body, expires_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(key) DO UPDATE SET
                           status = excluded.status,
                           headers_json = excluded.headers_json,
                           body = excluded.body,
                           expires_at_ms = excluded.expires_at_ms",
                        params![key, status, headers_json, body, expires_at_ms],
                    )
                    .map_err(|_| "sqlite_write")?;
                let (entries, bytes): (u64, u64) = transaction
                    .query_row(
                        "SELECT COUNT(*),
                                COALESCE(SUM(
                                  length(CAST(key AS BLOB))
                                  + length(CAST(headers_json AS BLOB))
                                  + length(body)
                                ), 0)
                         FROM _hluk_cache",
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
                Ok(CacheResponseData::ok())
            }
            CacheOperation::Delete { key } => {
                if self.binding.read_only {
                    return Ok(CacheResponseData::denied());
                }
                let deleted = self
                    .connection
                    .execute("DELETE FROM _hluk_cache WHERE key = ?1", [key])
                    .map_err(|_| "sqlite_write")?;
                Ok(if deleted == 0 {
                    CacheResponseData::not_found()
                } else {
                    CacheResponseData::ok()
                })
            }
        }
    }
}

struct CacheResponseData {
    status: CacheStatus,
    code: &'static str,
    cached_status: Option<u16>,
    headers: BTreeMap<String, String>,
    body: Option<Vec<u8>>,
}

impl CacheResponseData {
    fn ok() -> Self {
        Self {
            status: CacheStatus::Ok,
            code: "ok",
            cached_status: None,
            headers: BTreeMap::new(),
            body: None,
        }
    }

    fn not_found() -> Self {
        Self {
            status: CacheStatus::NotFound,
            code: "not_found",
            cached_status: None,
            headers: BTreeMap::new(),
            body: None,
        }
    }

    fn denied() -> Self {
        Self {
            status: CacheStatus::Denied,
            code: "read_only",
            cached_status: None,
            headers: BTreeMap::new(),
            body: None,
        }
    }
}

pub struct CacheService {
    stores: Vec<CacheStore>,
    clock: Arc<dyn CacheClock>,
}

impl CacheService {
    pub fn new(bindings: impl IntoIterator<Item = CacheBinding>) -> Result<Self> {
        Self::with_clock(bindings, Arc::new(SystemCacheClock))
    }

    pub fn with_clock(
        bindings: impl IntoIterator<Item = CacheBinding>,
        clock: Arc<dyn CacheClock>,
    ) -> Result<Self> {
        let mut stores = bindings
            .into_iter()
            .map(CacheStore::open)
            .collect::<Result<Vec<_>>>()?;
        stores.sort_by(|left, right| left.binding.name.cmp(&right.binding.name));
        if stores
            .windows(2)
            .any(|pair| pair[0].binding.name == pair[1].binding.name)
        {
            return Err(DataError::InvalidBinding);
        }
        Ok(Self { stores, clock })
    }

    pub fn bindings(&self) -> impl Iterator<Item = &CacheBinding> {
        self.stores.iter().map(|store| &store.binding)
    }

    fn parse(payload: &[u8]) -> std::result::Result<CacheRequest, &'static str> {
        if payload.len() > MAX_WIRE_BYTES {
            return Err("request_too_large");
        }
        let request: CacheRequest =
            serde_json::from_slice(payload).map_err(|_| "invalid_request")?;
        if request.version != CACHE_PROTOCOL_VERSION {
            return Err("unsupported_version");
        }
        validate_request_id(&request.request_id).map_err(|_| "invalid_request_id")?;
        validate_binding(&request.binding).map_err(|_| "invalid_binding")?;
        Ok(request)
    }

    fn response(request_id: String, data: CacheResponseData) -> CacheResponse {
        CacheResponse {
            version: CACHE_PROTOCOL_VERSION,
            request_id,
            status: data.status,
            code: safe_code(data.code).to_string(),
            cached_status: data.cached_status,
            headers: data.headers,
            body_base64: data.body.map(|body| STANDARD.encode(body)),
        }
    }

    fn rejection(request_id: impl Into<String>, status: CacheStatus, code: &str) -> Vec<u8> {
        serde_json::to_vec(&CacheResponse {
            version: CACHE_PROTOCOL_VERSION,
            request_id: request_id.into(),
            status,
            code: safe_code(code).to_string(),
            cached_status: None,
            headers: BTreeMap::new(),
            body_base64: None,
        })
        .unwrap_or_default()
    }
}

impl LogicalWireService for CacheService {
    fn inspect(&self, payload: &[u8]) -> std::result::Result<LogicalInvocation, LogicalWireError> {
        let request = Self::parse(payload).map_err(|code| match code {
            "unsupported_version" => LogicalWireError::UnsupportedVersion,
            _ => LogicalWireError::Malformed,
        })?;
        LogicalInvocation::new(request.binding)
    }

    fn dispatch(&mut self, _identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        let request = match Self::parse(payload) {
            Ok(request) => request,
            Err(code) => return Self::rejection("", CacheStatus::InvalidRequest, code),
        };
        let Some(store) = self
            .stores
            .iter_mut()
            .find(|store| store.binding.name == request.binding)
        else {
            return Self::rejection(request.request_id, CacheStatus::Denied, "binding_denied");
        };
        if let Err(code) = store.reserve(payload.len() as u64) {
            return Self::rejection(request.request_id, CacheStatus::QuotaExceeded, code);
        }
        let data = match store.execute(&request.operation, self.clock.now_unix_ms()) {
            Ok(data) => data,
            Err(
                code @ ("operation_quota"
                | "request_bytes_quota"
                | "response_bytes_quota"
                | "body_too_large"
                | "storage_quota"),
            ) => {
                return Self::rejection(request.request_id, CacheStatus::QuotaExceeded, code);
            }
            Err(
                code @ ("invalid_key" | "invalid_status" | "invalid_header" | "duplicate_header"
                | "too_many_headers" | "headers_too_large" | "invalid_base64"
                | "invalid_ttl"),
            ) => {
                return Self::rejection(request.request_id, CacheStatus::InvalidRequest, code);
            }
            Err(code) => {
                return Self::rejection(request.request_id, CacheStatus::HostError, code);
            }
        };
        let response = Self::response(request.request_id.clone(), data);
        let encoded = serde_json::to_vec(&response).unwrap_or_else(|_| {
            Self::rejection(
                request.request_id.clone(),
                CacheStatus::HostError,
                "host_error",
            )
        });
        if let Err(code) = store.settle(encoded.len() as u64) {
            return Self::rejection(request.request_id, CacheStatus::QuotaExceeded, code);
        }
        encoded
    }

    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
        let status = match status {
            LogicalRuntimeStatus::Denied => CacheStatus::Denied,
            LogicalRuntimeStatus::InvalidRequest => CacheStatus::InvalidRequest,
            LogicalRuntimeStatus::HostError => CacheStatus::HostError,
        };
        Self::rejection("", status, code)
    }

    fn reset_for_fresh_vm(&mut self) -> std::result::Result<(), BrokerHostError> {
        self.stores
            .iter_mut()
            .for_each(CacheStore::reset_request_budget);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker_runtime::BrokerRuntime;
    use std::sync::atomic::{AtomicU64, Ordering};

    const PUT_FIXTURE: &str = include_str!("../../tests/fixtures/data_cache_put_v1.json");

    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn new(now_ms: u64) -> Self {
            Self(AtomicU64::new(now_ms))
        }

        fn set(&self, now_ms: u64) {
            self.0.store(now_ms, Ordering::Release);
        }
    }

    impl CacheClock for ManualClock {
        fn now_unix_ms(&self) -> u64 {
            self.0.load(Ordering::Acquire)
        }
    }

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn runtime(binding: CacheBinding, clock: Arc<dyn CacheClock>) -> BrokerRuntime {
        let name = binding.name().to_string();
        BrokerRuntime::deny_all(identity())
            .with_logical([name], CacheService::with_clock([binding], clock).unwrap())
            .unwrap()
    }

    fn request(request_id: &str, binding: &str, operation: CacheOperation) -> Vec<u8> {
        serde_json::to_vec(&CacheRequest {
            version: CACHE_PROTOCOL_VERSION,
            request_id: request_id.to_string(),
            binding: binding.to_string(),
            operation,
        })
        .unwrap()
    }

    fn response(runtime: &BrokerRuntime, request: &[u8]) -> CacheResponse {
        serde_json::from_slice(&runtime.dispatch_logical(request)).unwrap()
    }

    #[test]
    fn deterministic_fixture_survives_fresh_vm_reset() {
        let clock = Arc::new(ManualClock::new(1000));
        let runtime = runtime(
            CacheBinding::in_memory("assets", false, CacheLimits::default()).unwrap(),
            clock,
        );
        let put = response(&runtime, PUT_FIXTURE.as_bytes());
        runtime.reset_for_fresh_vm().unwrap();
        let matched = response(
            &runtime,
            &request(
                "match",
                "assets",
                CacheOperation::Match {
                    key: "https://example.test/app.js".to_string(),
                },
            ),
        );

        assert_eq!(
            (
                put.status,
                matched.status,
                matched.cached_status,
                matched.body_base64.as_deref(),
            ),
            (
                CacheStatus::Ok,
                CacheStatus::Ok,
                Some(200),
                Some("Y29uc29sZS5sb2coMSk="),
            )
        );
    }

    #[test]
    fn expired_entry_is_not_observable() {
        let clock = Arc::new(ManualClock::new(1000));
        let runtime = runtime(
            CacheBinding::in_memory("ttl", false, CacheLimits::default()).unwrap(),
            clock.clone(),
        );
        let put = request(
            "put",
            "ttl",
            CacheOperation::Put {
                key: "https://example.test/ttl".to_string(),
                status: 200,
                headers: BTreeMap::new(),
                body_base64: "eA==".to_string(),
                ttl_ms: 10,
            },
        );
        assert_eq!(response(&runtime, &put).status, CacheStatus::Ok);
        clock.set(1010);
        let matched = response(
            &runtime,
            &request(
                "match",
                "ttl",
                CacheOperation::Match {
                    key: "https://example.test/ttl".to_string(),
                },
            ),
        );

        assert_eq!(matched.status, CacheStatus::NotFound);
    }

    #[test]
    fn read_only_and_storage_quotas_fail_closed() {
        let clock = Arc::new(ManualClock::new(1000));
        let readonly = runtime(
            CacheBinding::in_memory("readonly", true, CacheLimits::default()).unwrap(),
            clock.clone(),
        );
        let denied = response(
            &readonly,
            &request(
                "put",
                "readonly",
                CacheOperation::Put {
                    key: "https://example.test/a".to_string(),
                    status: 200,
                    headers: BTreeMap::new(),
                    body_base64: "eA==".to_string(),
                    ttl_ms: 10,
                },
            ),
        );
        let limits = CacheLimits {
            max_entries: 1,
            ..CacheLimits::default()
        };
        let quota = runtime(
            CacheBinding::in_memory("quota", false, limits).unwrap(),
            clock,
        );
        for key in ["a", "b"] {
            let result = response(
                &quota,
                &request(
                    key,
                    "quota",
                    CacheOperation::Put {
                        key: format!("https://example.test/{key}"),
                        status: 200,
                        headers: BTreeMap::new(),
                        body_base64: "eA==".to_string(),
                        ttl_ms: 10,
                    },
                ),
            );
            if key == "b" {
                assert_eq!(
                    (result.status, result.code),
                    (CacheStatus::QuotaExceeded, "storage_quota".to_string())
                );
            }
        }

        assert_eq!(
            (denied.status, denied.code),
            (CacheStatus::Denied, "read_only".to_string())
        );
    }

    #[test]
    fn invalid_headers_and_ttl_are_rejected() {
        let runtime = runtime(
            CacheBinding::in_memory("cache", false, CacheLimits::default()).unwrap(),
            Arc::new(ManualClock::new(1000)),
        );
        let mut headers = BTreeMap::new();
        headers.insert("bad header".to_string(), "value".to_string());
        let bad_header = response(
            &runtime,
            &request(
                "header",
                "cache",
                CacheOperation::Put {
                    key: "https://example.test/a".to_string(),
                    status: 200,
                    headers,
                    body_base64: "eA==".to_string(),
                    ttl_ms: 10,
                },
            ),
        );
        let bad_ttl = response(
            &runtime,
            &request(
                "ttl",
                "cache",
                CacheOperation::Put {
                    key: "https://example.test/a".to_string(),
                    status: 200,
                    headers: BTreeMap::new(),
                    body_base64: "eA==".to_string(),
                    ttl_ms: 0,
                },
            ),
        );

        assert_eq!(
            (
                bad_header.status,
                bad_header.code,
                bad_ttl.status,
                bad_ttl.code,
            ),
            (
                CacheStatus::InvalidRequest,
                "invalid_header".to_string(),
                CacheStatus::InvalidRequest,
                "invalid_ttl".to_string(),
            )
        );
    }

    #[test]
    fn canonical_errors_keep_request_id_and_unknown_operations_fail_closed() {
        let runtime = runtime(
            CacheBinding::in_memory("cache", false, CacheLimits::default()).unwrap(),
            Arc::new(ManualClock::new(1000)),
        );
        let invalid_ttl = response(
            &runtime,
            &request(
                "ttl-correlation",
                "cache",
                CacheOperation::Put {
                    key: "https://example.test/a".to_string(),
                    status: 200,
                    headers: BTreeMap::new(),
                    body_base64: "eA==".to_string(),
                    ttl_ms: 0,
                },
            ),
        );
        let unknown: CacheResponse = serde_json::from_slice(&runtime.dispatch_logical(
            br#"{"version":1,"request_id":"unknown","binding":"cache","operation":{"kind":"cache_private_variant"}}"#,
        ))
        .unwrap();

        assert_eq!(
            (
                invalid_ttl.request_id,
                invalid_ttl.status,
                unknown.request_id,
                unknown.status,
                unknown.code,
            ),
            (
                "ttl-correlation".to_string(),
                CacheStatus::InvalidRequest,
                String::new(),
                CacheStatus::InvalidRequest,
                "invalid_request".to_string(),
            )
        );
    }

    #[test]
    fn headers_are_canonical_and_expired_entries_release_storage_quota() {
        let clock = Arc::new(ManualClock::new(1000));
        let limits = CacheLimits {
            max_entries: 1,
            ..CacheLimits::default()
        };
        let runtime = runtime(
            CacheBinding::in_memory("cache", false, limits).unwrap(),
            clock.clone(),
        );
        let mut headers = BTreeMap::new();
        headers.insert("Content-Type".to_string(), "text/plain".to_string());
        let first = response(
            &runtime,
            &request(
                "first",
                "cache",
                CacheOperation::Put {
                    key: "https://example.test/first".to_string(),
                    status: 200,
                    headers,
                    body_base64: "MQ==".to_string(),
                    ttl_ms: 10,
                },
            ),
        );
        let canonical = response(
            &runtime,
            &request(
                "canonical",
                "cache",
                CacheOperation::Match {
                    key: "https://example.test/first".to_string(),
                },
            ),
        );
        clock.set(1010);
        let second = response(
            &runtime,
            &request(
                "second",
                "cache",
                CacheOperation::Put {
                    key: "https://example.test/second".to_string(),
                    status: 200,
                    headers: BTreeMap::new(),
                    body_base64: "Mg==".to_string(),
                    ttl_ms: 10,
                },
            ),
        );
        let matched = response(
            &runtime,
            &request(
                "match",
                "cache",
                CacheOperation::Match {
                    key: "https://example.test/first".to_string(),
                },
            ),
        );

        assert_eq!(
            (
                first.status,
                canonical.headers,
                second.status,
                matched.status,
            ),
            (
                CacheStatus::Ok,
                BTreeMap::from([("content-type".to_string(), "text/plain".to_string())]),
                CacheStatus::Ok,
                CacheStatus::NotFound,
            )
        );
    }
}
