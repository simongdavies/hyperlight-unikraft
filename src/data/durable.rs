use super::{
    DataError, Result, canonical_backing_path, safe_code, validate_binding, validate_key,
    validate_request_id,
};
use crate::actor::{ActorId, ActorLifecycle, ActorNamespace, ActorRoute};
use crate::broker::RequestIdentity;
use crate::broker_adapter::BrokerHostError;
use crate::broker_runtime::{
    LogicalInvocation, LogicalRuntimeStatus, LogicalWireError, LogicalWireService,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const DURABLE_PROTOCOL_VERSION: u16 = 1;
const MAX_WIRE_BYTES: usize = 128 * 1024;
const MAX_LIST_LIMIT: u32 = 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableObjectLimits {
    pub max_operations: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_delivery_bytes: u64,
    pub max_value_bytes: u64,
    pub max_entries: u64,
    pub max_total_bytes: u64,
}

impl DurableObjectLimits {
    pub fn validate(self) -> Result<Self> {
        if self.max_operations == 0
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_delivery_bytes == 0
            || self.max_value_bytes == 0
            || self.max_entries == 0
            || self.max_total_bytes == 0
        {
            return Err(DataError::InvalidLimit);
        }
        Ok(self)
    }
}

impl Default for DurableObjectLimits {
    fn default() -> Self {
        Self {
            max_operations: 128,
            max_request_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_delivery_bytes: 64 * 1024,
            max_value_bytes: 32 * 1024,
            max_entries: 1024,
            max_total_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableObjectBinding {
    name: String,
    namespace: ActorNamespace,
    backing: Option<PathBuf>,
    limits: DurableObjectLimits,
}

impl DurableObjectBinding {
    pub fn in_memory(
        name: impl Into<String>,
        namespace: ActorNamespace,
        limits: DurableObjectLimits,
    ) -> Result<Self> {
        Self::new(name.into(), namespace, None, limits)
    }

    pub fn persistent(
        name: impl Into<String>,
        namespace: ActorNamespace,
        path: impl AsRef<Path>,
        limits: DurableObjectLimits,
    ) -> Result<Self> {
        Self::new(
            name.into(),
            namespace,
            Some(canonical_backing_path(path.as_ref())?),
            limits,
        )
    }

    fn new(
        name: String,
        namespace: ActorNamespace,
        backing: Option<PathBuf>,
        limits: DurableObjectLimits,
    ) -> Result<Self> {
        validate_binding(&name)?;
        Ok(Self {
            name,
            namespace,
            backing,
            limits: limits.validate()?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn namespace(&self) -> &ActorNamespace {
        &self.namespace
    }

    pub fn backing(&self) -> Option<&Path> {
        self.backing.as_deref()
    }

    pub fn limits(&self) -> DurableObjectLimits {
        self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableDeliveryContext {
    route: ActorRoute,
    request: RequestIdentity,
    lifecycle: ActorLifecycle,
}

impl DurableDeliveryContext {
    pub fn new(
        route: ActorRoute,
        request: RequestIdentity,
        lifecycle: ActorLifecycle,
    ) -> std::result::Result<Self, DurableObjectError> {
        if !matches!(lifecycle, ActorLifecycle::Deliver | ActorLifecycle::Alarm) {
            return Err(DurableObjectError::InvalidLifecycle);
        }
        Ok(Self {
            route,
            request,
            lifecycle,
        })
    }

    pub fn route(&self) -> &ActorRoute {
        &self.route
    }

    pub fn request(&self) -> &RequestIdentity {
        &self.request
    }

    pub fn lifecycle(&self) -> ActorLifecycle {
        self.lifecycle
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DurableObjectError {
    #[error("durable object delivery context is already active")]
    DeliveryActive,
    #[error("durable object lifecycle must be deliver or alarm")]
    InvalidLifecycle,
    #[error("durable object namespace does not match the binding")]
    NamespaceMismatch,
    #[error("durable object route generation is stale: current {current}, supplied {supplied}")]
    StaleGeneration { current: u64, supplied: u64 },
    #[error("durable object route generation exceeds SQLite integer range")]
    GenerationOutOfRange,
    #[error("durable object storage failed")]
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum DurableObjectOperation {
    #[serde(rename = "do_deliver")]
    Deliver { payload_base64: String },
    #[serde(rename = "do_alarm")]
    Alarm,
    #[serde(rename = "do_storage_get")]
    StorageGet { key: String },
    #[serde(rename = "do_storage_put")]
    StoragePut { key: String, value_base64: String },
    #[serde(rename = "do_storage_delete")]
    StorageDelete { key: String },
    #[serde(rename = "do_storage_list")]
    StorageList { prefix: String, limit: u32 },
    #[serde(rename = "do_set_alarm")]
    SetAlarm { scheduled_at_unix_ms: u64 },
    #[serde(rename = "do_delete_alarm")]
    DeleteAlarm,
    #[serde(rename = "do_passivate")]
    Passivate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableObjectRequest {
    pub version: u16,
    pub request_id: String,
    pub binding: String,
    pub operation: DurableObjectOperation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableObjectStatus {
    Ok,
    NotFound,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableObjectResponse {
    pub version: u16,
    pub request_id: String,
    pub status: DurableObjectStatus,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_at_unix_ms: Option<u64>,
}

pub struct DurableObjectService {
    binding: DurableObjectBinding,
    connection: Connection,
    active: Option<DurableDeliveryContext>,
    operations: u64,
    request_bytes: u64,
    response_bytes: u64,
}

impl DurableObjectService {
    pub fn new(binding: DurableObjectBinding) -> Result<Self> {
        let connection = match &binding.backing {
            Some(path) => Connection::open(path).map_err(|source| DataError::OpenBacking {
                path: path.clone(),
                source,
            })?,
            None => Connection::open_in_memory().map_err(DataError::InitializeBacking)?,
        };
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS _hluk_do_actor (
                    namespace TEXT NOT NULL,
                    actor_key TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    alarm_ms INTEGER,
                    PRIMARY KEY(namespace, actor_key)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS _hluk_do_state (
                    namespace TEXT NOT NULL,
                    actor_key TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value BLOB NOT NULL,
                    PRIMARY KEY(namespace, actor_key, key)
                ) WITHOUT ROWID;",
            )
            .map_err(DataError::InitializeBacking)?;
        Ok(Self {
            binding,
            connection,
            active: None,
            operations: 0,
            request_bytes: 0,
            response_bytes: 0,
        })
    }

    pub fn binding(&self) -> &DurableObjectBinding {
        &self.binding
    }

    pub fn begin_delivery(
        &mut self,
        context: DurableDeliveryContext,
    ) -> std::result::Result<(), DurableObjectError> {
        if self.active.is_some() {
            return Err(DurableObjectError::DeliveryActive);
        }
        if context.route.actor().namespace() != &self.binding.namespace {
            return Err(DurableObjectError::NamespaceMismatch);
        }
        if context.route.generation() > i64::MAX as u64 {
            return Err(DurableObjectError::GenerationOutOfRange);
        }
        let actor_key = context.route.actor().key().as_str();
        let current = self
            .connection
            .query_row(
                "SELECT generation FROM _hluk_do_actor
                 WHERE namespace = ?1 AND actor_key = ?2",
                params![self.binding.namespace.as_str(), actor_key],
                |row| row.get::<_, u64>(0),
            )
            .optional()
            .map_err(|_| DurableObjectError::Storage)?;
        if current.is_some_and(|generation| generation > context.route.generation()) {
            return Err(DurableObjectError::StaleGeneration {
                current: current.unwrap_or_default(),
                supplied: context.route.generation(),
            });
        }
        self.connection
            .execute(
                "INSERT INTO _hluk_do_actor(namespace, actor_key, generation, alarm_ms)
                 VALUES (?1, ?2, ?3, NULL)
                 ON CONFLICT(namespace, actor_key) DO UPDATE SET
                   generation = excluded.generation",
                params![
                    self.binding.namespace.as_str(),
                    actor_key,
                    context.route.generation()
                ],
            )
            .map_err(|_| DurableObjectError::Storage)?;
        self.active = Some(context);
        Ok(())
    }

    pub fn active_actor(&self) -> Option<&ActorId> {
        self.active.as_ref().map(|context| context.route.actor())
    }

    fn parse(payload: &[u8]) -> std::result::Result<DurableObjectRequest, &'static str> {
        if payload.len() > MAX_WIRE_BYTES {
            return Err("request_too_large");
        }
        let request: DurableObjectRequest =
            serde_json::from_slice(payload).map_err(|_| "invalid_request")?;
        if request.version != DURABLE_PROTOCOL_VERSION {
            return Err("unsupported_version");
        }
        validate_request_id(&request.request_id).map_err(|_| "invalid_request_id")?;
        validate_binding(&request.binding).map_err(|_| "invalid_binding")?;
        Ok(request)
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

    fn actor_parts(&self) -> std::result::Result<(String, String), &'static str> {
        let Some(context) = &self.active else {
            return Err("delivery_context_required");
        };
        Ok((
            context.route.actor().namespace().as_str().to_string(),
            context.route.actor().key().as_str().to_string(),
        ))
    }

    fn execute(
        &mut self,
        identity: &RequestIdentity,
        operation: &DurableObjectOperation,
    ) -> std::result::Result<DurableResponseData, &'static str> {
        let Some(context) = &self.active else {
            return Err("delivery_context_required");
        };
        if context.request() != identity {
            return Err("identity_mismatch");
        }
        match operation {
            DurableObjectOperation::Deliver { payload_base64 } => {
                if context.lifecycle() != ActorLifecycle::Deliver {
                    return Err("lifecycle_mismatch");
                }
                let payload = STANDARD
                    .decode(payload_base64)
                    .map_err(|_| "invalid_base64")?;
                if payload.len() as u64 > self.binding.limits.max_delivery_bytes {
                    return Err("delivery_bytes_quota");
                }
                Ok(DurableResponseData::ok())
            }
            DurableObjectOperation::Alarm => {
                if context.lifecycle() != ActorLifecycle::Alarm {
                    return Err("lifecycle_mismatch");
                }
                let (namespace, actor_key) = self.actor_parts()?;
                let alarm = self
                    .connection
                    .query_row(
                        "SELECT alarm_ms FROM _hluk_do_actor
                         WHERE namespace = ?1 AND actor_key = ?2",
                        params![namespace, actor_key],
                        |row| row.get::<_, Option<u64>>(0),
                    )
                    .optional()
                    .map_err(|_| "sqlite_read")?
                    .flatten();
                let Some(alarm) = alarm else {
                    return Ok(DurableResponseData::not_found());
                };
                self.connection
                    .execute(
                        "UPDATE _hluk_do_actor SET alarm_ms = NULL
                         WHERE namespace = ?1 AND actor_key = ?2",
                        params![namespace, actor_key],
                    )
                    .map_err(|_| "sqlite_write")?;
                Ok(DurableResponseData::alarm(Some(alarm)))
            }
            DurableObjectOperation::StorageGet { key } => {
                validate_key(key).map_err(|_| "invalid_key")?;
                let (namespace, actor_key) = self.actor_parts()?;
                let value = self
                    .connection
                    .query_row(
                        "SELECT value FROM _hluk_do_state
                         WHERE namespace = ?1 AND actor_key = ?2 AND key = ?3",
                        params![namespace, actor_key, key],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    .map_err(|_| "sqlite_read")?;
                Ok(match value {
                    Some(value) => DurableResponseData::value(value),
                    None => DurableResponseData::not_found(),
                })
            }
            DurableObjectOperation::StoragePut { key, value_base64 } => {
                validate_key(key).map_err(|_| "invalid_key")?;
                let value = STANDARD
                    .decode(value_base64)
                    .map_err(|_| "invalid_base64")?;
                if value.len() as u64 > self.binding.limits.max_value_bytes {
                    return Err("value_bytes_quota");
                }
                let (namespace, actor_key) = self.actor_parts()?;
                let transaction = self
                    .connection
                    .transaction()
                    .map_err(|_| "sqlite_transaction")?;
                transaction
                    .execute(
                        "INSERT INTO _hluk_do_state(namespace, actor_key, key, value)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(namespace, actor_key, key) DO UPDATE SET
                           value = excluded.value",
                        params![namespace, actor_key, key, value],
                    )
                    .map_err(|_| "sqlite_write")?;
                let (entries, bytes): (u64, u64) = transaction
                    .query_row(
                        "SELECT COUNT(*),
                                COALESCE(SUM(length(CAST(key AS BLOB)) + length(value)), 0)
                         FROM _hluk_do_state
                         WHERE namespace = ?1 AND actor_key = ?2",
                        params![namespace, actor_key],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|_| "sqlite_quota_read")?;
                if entries > self.binding.limits.max_entries
                    || bytes > self.binding.limits.max_total_bytes
                {
                    return Err("storage_quota");
                }
                transaction.commit().map_err(|_| "sqlite_commit")?;
                Ok(DurableResponseData::ok())
            }
            DurableObjectOperation::StorageDelete { key } => {
                validate_key(key).map_err(|_| "invalid_key")?;
                let (namespace, actor_key) = self.actor_parts()?;
                let deleted = self
                    .connection
                    .execute(
                        "DELETE FROM _hluk_do_state
                         WHERE namespace = ?1 AND actor_key = ?2 AND key = ?3",
                        params![namespace, actor_key, key],
                    )
                    .map_err(|_| "sqlite_write")?;
                Ok(if deleted == 0 {
                    DurableResponseData::not_found()
                } else {
                    DurableResponseData::ok()
                })
            }
            DurableObjectOperation::StorageList { prefix, limit } => {
                if !prefix.is_empty() {
                    validate_key(prefix).map_err(|_| "invalid_prefix")?;
                }
                if *limit == 0 || *limit > MAX_LIST_LIMIT {
                    return Err("invalid_limit");
                }
                let (namespace, actor_key) = self.actor_parts()?;
                let mut statement = self
                    .connection
                    .prepare(
                        "SELECT key FROM _hluk_do_state
                         WHERE namespace = ?1
                           AND actor_key = ?2
                           AND substr(key, 1, length(?3)) = ?3
                         ORDER BY key
                         LIMIT ?4",
                    )
                    .map_err(|_| "sqlite_read")?;
                let keys = statement
                    .query_map(params![namespace, actor_key, prefix, limit], |row| {
                        row.get(0)
                    })
                    .map_err(|_| "sqlite_read")?
                    .collect::<std::result::Result<Vec<String>, _>>()
                    .map_err(|_| "sqlite_read")?;
                Ok(DurableResponseData::keys(keys))
            }
            DurableObjectOperation::SetAlarm {
                scheduled_at_unix_ms,
            } => {
                if *scheduled_at_unix_ms > i64::MAX as u64 {
                    return Err("invalid_alarm");
                }
                let (namespace, actor_key) = self.actor_parts()?;
                self.connection
                    .execute(
                        "UPDATE _hluk_do_actor SET alarm_ms = ?3
                         WHERE namespace = ?1 AND actor_key = ?2",
                        params![namespace, actor_key, scheduled_at_unix_ms],
                    )
                    .map_err(|_| "sqlite_write")?;
                Ok(DurableResponseData::alarm(Some(*scheduled_at_unix_ms)))
            }
            DurableObjectOperation::DeleteAlarm => {
                let (namespace, actor_key) = self.actor_parts()?;
                self.connection
                    .execute(
                        "UPDATE _hluk_do_actor SET alarm_ms = NULL
                         WHERE namespace = ?1 AND actor_key = ?2",
                        params![namespace, actor_key],
                    )
                    .map_err(|_| "sqlite_write")?;
                Ok(DurableResponseData::alarm(None))
            }
            DurableObjectOperation::Passivate => Ok(DurableResponseData::ok()),
        }
    }

    fn rejection(
        request_id: impl Into<String>,
        status: DurableObjectStatus,
        code: &str,
    ) -> Vec<u8> {
        serde_json::to_vec(&DurableObjectResponse {
            version: DURABLE_PROTOCOL_VERSION,
            request_id: request_id.into(),
            status,
            code: safe_code(code).to_string(),
            value_base64: None,
            keys: Vec::new(),
            scheduled_at_unix_ms: None,
        })
        .unwrap_or_default()
    }
}

struct DurableResponseData {
    status: DurableObjectStatus,
    code: &'static str,
    value: Option<Vec<u8>>,
    keys: Vec<String>,
    alarm: Option<u64>,
}

impl DurableResponseData {
    fn ok() -> Self {
        Self {
            status: DurableObjectStatus::Ok,
            code: "ok",
            value: None,
            keys: Vec::new(),
            alarm: None,
        }
    }

    fn not_found() -> Self {
        Self {
            status: DurableObjectStatus::NotFound,
            code: "not_found",
            ..Self::ok()
        }
    }

    fn value(value: Vec<u8>) -> Self {
        Self {
            value: Some(value),
            ..Self::ok()
        }
    }

    fn keys(keys: Vec<String>) -> Self {
        Self { keys, ..Self::ok() }
    }

    fn alarm(alarm: Option<u64>) -> Self {
        Self {
            alarm,
            ..Self::ok()
        }
    }
}

impl LogicalWireService for DurableObjectService {
    fn inspect(&self, payload: &[u8]) -> std::result::Result<LogicalInvocation, LogicalWireError> {
        let request = Self::parse(payload).map_err(|code| match code {
            "unsupported_version" => LogicalWireError::UnsupportedVersion,
            _ => LogicalWireError::Malformed,
        })?;
        LogicalInvocation::new(request.binding)
    }

    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        let request = match Self::parse(payload) {
            Ok(request) => request,
            Err(code) => return Self::rejection("", DurableObjectStatus::InvalidRequest, code),
        };
        if request.binding != self.binding.name {
            return Self::rejection(
                request.request_id,
                DurableObjectStatus::Denied,
                "binding_denied",
            );
        }
        if let Err(code) = self.reserve(payload.len() as u64) {
            return Self::rejection(request.request_id, DurableObjectStatus::QuotaExceeded, code);
        }
        let passivate = matches!(request.operation, DurableObjectOperation::Passivate);
        let data = match self.execute(identity, &request.operation) {
            Ok(data) => data,
            Err(
                code @ ("delivery_context_required" | "identity_mismatch" | "lifecycle_mismatch"),
            ) => {
                return Self::rejection(request.request_id, DurableObjectStatus::Denied, code);
            }
            Err(
                code @ ("operation_quota"
                | "request_bytes_quota"
                | "response_bytes_quota"
                | "delivery_bytes_quota"
                | "value_bytes_quota"
                | "storage_quota"),
            ) => {
                return Self::rejection(
                    request.request_id,
                    DurableObjectStatus::QuotaExceeded,
                    code,
                );
            }
            Err(
                code @ ("invalid_key" | "invalid_prefix" | "invalid_limit" | "invalid_base64"
                | "invalid_alarm"),
            ) => {
                return Self::rejection(
                    request.request_id,
                    DurableObjectStatus::InvalidRequest,
                    code,
                );
            }
            Err(code) => {
                return Self::rejection(request.request_id, DurableObjectStatus::HostError, code);
            }
        };
        let response = DurableObjectResponse {
            version: DURABLE_PROTOCOL_VERSION,
            request_id: request.request_id.clone(),
            status: data.status,
            code: data.code.to_string(),
            value_base64: data.value.map(|value| STANDARD.encode(value)),
            keys: data.keys,
            scheduled_at_unix_ms: data.alarm,
        };
        let encoded = serde_json::to_vec(&response).unwrap_or_else(|_| {
            Self::rejection(
                request.request_id.clone(),
                DurableObjectStatus::HostError,
                "host_error",
            )
        });
        if let Err(code) = self.settle(encoded.len() as u64) {
            return Self::rejection(request.request_id, DurableObjectStatus::QuotaExceeded, code);
        }
        if passivate {
            self.active = None;
        }
        encoded
    }

    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
        let status = match status {
            LogicalRuntimeStatus::Denied => DurableObjectStatus::Denied,
            LogicalRuntimeStatus::InvalidRequest => DurableObjectStatus::InvalidRequest,
            LogicalRuntimeStatus::HostError => DurableObjectStatus::HostError,
        };
        Self::rejection("", status, code)
    }

    fn reset_for_fresh_vm(&mut self) -> std::result::Result<(), BrokerHostError> {
        self.active = None;
        self.operations = 0;
        self.request_bytes = 0;
        self.response_bytes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorKey;

    const PUT_FIXTURE: &str = include_str!("../../tests/fixtures/data_do_put_v1.json");

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn route(key: &str, generation: u64) -> ActorRoute {
        ActorRoute::new(
            ActorId::new(
                ActorNamespace::new("rooms").unwrap(),
                ActorKey::new(key).unwrap(),
            ),
            "cell-a",
            generation,
        )
        .unwrap()
    }

    fn service() -> DurableObjectService {
        DurableObjectService::new(
            DurableObjectBinding::in_memory(
                "rooms",
                ActorNamespace::new("rooms").unwrap(),
                DurableObjectLimits::default(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn context(key: &str, generation: u64, lifecycle: ActorLifecycle) -> DurableDeliveryContext {
        DurableDeliveryContext::new(route(key, generation), identity(), lifecycle).unwrap()
    }

    fn request(id: &str, operation: DurableObjectOperation) -> Vec<u8> {
        serde_json::to_vec(&DurableObjectRequest {
            version: DURABLE_PROTOCOL_VERSION,
            request_id: id.to_string(),
            binding: "rooms".to_string(),
            operation,
        })
        .unwrap()
    }

    fn response(service: &mut DurableObjectService, payload: &[u8]) -> DurableObjectResponse {
        serde_json::from_slice(&service.dispatch(&identity(), payload)).unwrap()
    }

    #[test]
    fn trusted_context_partitions_persistent_state_across_fresh_vm_reset() {
        let mut service = service();
        service
            .begin_delivery(context("room-7", 1, ActorLifecycle::Alarm))
            .unwrap();
        assert_eq!(
            response(&mut service, PUT_FIXTURE.as_bytes()).status,
            DurableObjectStatus::Ok
        );
        service.reset_for_fresh_vm().unwrap();
        service
            .begin_delivery(context("room-8", 1, ActorLifecycle::Deliver))
            .unwrap();
        assert_eq!(
            response(
                &mut service,
                &request(
                    "other",
                    DurableObjectOperation::StorageGet {
                        key: "topic".to_string(),
                    },
                ),
            )
            .status,
            DurableObjectStatus::NotFound
        );
        service.reset_for_fresh_vm().unwrap();
        service
            .begin_delivery(context("room-7", 1, ActorLifecycle::Deliver))
            .unwrap();
        let restored = response(
            &mut service,
            &request(
                "restored",
                DurableObjectOperation::StorageGet {
                    key: "topic".to_string(),
                },
            ),
        );

        assert_eq!(
            (restored.status, restored.value_base64.as_deref()),
            (DurableObjectStatus::Ok, Some("aHlwZXJsaWdodA=="))
        );
    }

    #[test]
    fn stale_generation_and_untrusted_delivery_fail_closed() {
        let mut service = service();
        service
            .begin_delivery(context("room-7", 2, ActorLifecycle::Deliver))
            .unwrap();
        service.reset_for_fresh_vm().unwrap();
        let stale = service
            .begin_delivery(context("room-7", 1, ActorLifecycle::Deliver))
            .unwrap_err();
        let denied = response(
            &mut service,
            &request(
                "untrusted",
                DurableObjectOperation::StorageGet {
                    key: "topic".to_string(),
                },
            ),
        );

        assert_eq!(
            (stale, denied.status, denied.code),
            (
                DurableObjectError::StaleGeneration {
                    current: 2,
                    supplied: 1,
                },
                DurableObjectStatus::Denied,
                "delivery_context_required".to_string(),
            )
        );
    }

    #[test]
    fn lifecycle_operations_require_matching_host_context() {
        let mut service = service();
        service
            .begin_delivery(context("room-7", 1, ActorLifecycle::Alarm))
            .unwrap();
        let deliver = response(
            &mut service,
            &request(
                "deliver",
                DurableObjectOperation::Deliver {
                    payload_base64: "eA==".to_string(),
                },
            ),
        );
        let set_alarm = response(
            &mut service,
            &request(
                "set-alarm",
                DurableObjectOperation::SetAlarm {
                    scheduled_at_unix_ms: 42,
                },
            ),
        );
        service.reset_for_fresh_vm().unwrap();
        service
            .begin_delivery(context("room-7", 1, ActorLifecycle::Alarm))
            .unwrap();
        let alarm = response(
            &mut service,
            &request("alarm", DurableObjectOperation::Alarm),
        );
        let passivated = response(
            &mut service,
            &request("passivate", DurableObjectOperation::Passivate),
        );

        assert_eq!(
            (
                deliver.status,
                deliver.code,
                set_alarm.status,
                alarm.status,
                alarm.scheduled_at_unix_ms,
                passivated.status,
                service.active_actor(),
            ),
            (
                DurableObjectStatus::Denied,
                "lifecycle_mismatch".to_string(),
                DurableObjectStatus::Ok,
                DurableObjectStatus::Ok,
                Some(42),
                DurableObjectStatus::Ok,
                None,
            )
        );
    }

    #[test]
    fn alarms_and_storage_quotas_are_durable_and_bounded() {
        let limits = DurableObjectLimits {
            max_entries: 1,
            ..DurableObjectLimits::default()
        };
        let mut service = DurableObjectService::new(
            DurableObjectBinding::in_memory("rooms", ActorNamespace::new("rooms").unwrap(), limits)
                .unwrap(),
        )
        .unwrap();
        service
            .begin_delivery(context("room-7", 1, ActorLifecycle::Deliver))
            .unwrap();
        let alarm = response(
            &mut service,
            &request(
                "set-alarm",
                DurableObjectOperation::SetAlarm {
                    scheduled_at_unix_ms: 42,
                },
            ),
        );
        let first = response(
            &mut service,
            &request(
                "first",
                DurableObjectOperation::StoragePut {
                    key: "a".to_string(),
                    value_base64: "MQ==".to_string(),
                },
            ),
        );
        let second = response(
            &mut service,
            &request(
                "second",
                DurableObjectOperation::StoragePut {
                    key: "b".to_string(),
                    value_base64: "Mg==".to_string(),
                },
            ),
        );

        assert_eq!(
            (
                alarm.scheduled_at_unix_ms,
                first.status,
                second.status,
                second.code,
            ),
            (
                Some(42),
                DurableObjectStatus::Ok,
                DurableObjectStatus::QuotaExceeded,
                "storage_quota".to_string(),
            )
        );
    }
}
