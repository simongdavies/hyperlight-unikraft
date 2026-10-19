use super::{
    DataError, Result, canonical_backing_path, safe_code, validate_binding, validate_request_id,
};
use crate::broker::RequestIdentity;
use crate::broker_adapter::BrokerHostError;
use crate::broker_runtime::{
    LogicalInvocation, LogicalRuntimeStatus, LogicalWireError, LogicalWireService,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, params_from_iter};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

const D1_PROTOCOL_VERSION: u16 = 1;
const MAX_WIRE_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct D1Limits {
    pub max_operations: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_statements: usize,
    pub max_statement_bytes: usize,
    pub max_parameters: usize,
    pub max_rows: usize,
    pub max_result_bytes: u64,
    pub max_execution_ms: u64,
}

impl D1Limits {
    pub fn validate(self) -> Result<Self> {
        if self.max_operations == 0
            || self.max_request_bytes == 0
            || self.max_response_bytes == 0
            || self.max_statements == 0
            || self.max_statement_bytes == 0
            || self.max_parameters == 0
            || self.max_rows == 0
            || self.max_result_bytes == 0
            || self.max_execution_ms == 0
            || self.max_execution_ms > 30_000
        {
            return Err(DataError::InvalidLimit);
        }
        Ok(self)
    }
}

impl Default for D1Limits {
    fn default() -> Self {
        Self {
            max_operations: 32,
            max_request_bytes: 256 * 1024,
            max_response_bytes: 256 * 1024,
            max_statements: 16,
            max_statement_bytes: 32 * 1024,
            max_parameters: 128,
            max_rows: 1000,
            max_result_bytes: 256 * 1024,
            max_execution_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct D1Binding {
    name: String,
    backing: Option<PathBuf>,
    read_only: bool,
    limits: D1Limits,
    api_metadata: bool,
}

impl D1Binding {
    pub fn in_memory(name: impl Into<String>, read_only: bool, limits: D1Limits) -> Result<Self> {
        Self::new(name.into(), None, read_only, limits)
    }

    pub fn persistent(
        name: impl Into<String>,
        path: impl AsRef<Path>,
        read_only: bool,
        limits: D1Limits,
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
        limits: D1Limits,
    ) -> Result<Self> {
        validate_binding(&name)?;
        Ok(Self {
            name,
            backing,
            read_only,
            limits: limits.validate()?,
            api_metadata: false,
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

    pub fn limits(&self) -> D1Limits {
        self.limits
    }

    /// Include measured statement metadata for an explicitly selected D1 API.
    pub fn with_d1_api_metadata(mut self) -> Self {
        self.api_metadata = true;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum D1Parameter {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(String),
}

impl D1Parameter {
    fn into_sql(self) -> std::result::Result<Value, &'static str> {
        Ok(match self {
            Self::Null => Value::Null,
            Self::Integer(value) => Value::Integer(value),
            Self::Real(value) if value.is_finite() => Value::Real(value),
            Self::Real(_) => return Err("invalid_real"),
            Self::Text(value) => Value::Text(value),
            Self::Blob(value) => Value::Blob(STANDARD.decode(value).map_err(|_| "invalid_base64")?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1Statement {
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<D1Parameter>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1Request {
    pub version: u16,
    pub request_id: String,
    pub binding: String,
    pub operation: D1Operation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum D1Operation {
    #[serde(rename = "d1_batch", alias = "sql_batch")]
    Batch { statements: Vec<D1Statement> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum D1Status {
    Ok,
    Denied,
    QuotaExceeded,
    InvalidRequest,
    HostError,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<D1Value>>,
    pub rows_affected: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<D1ResultMetadata>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1ResultMetadata {
    /// Actual elapsed statement execution time, in milliseconds.
    pub duration: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_row_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_written: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_db: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_after: Option<u64>,
}

fn result_metadata(
    enabled: bool,
    started: Instant,
    connection: &Connection,
    changes: u64,
    previous_row_id: i64,
) -> Option<D1ResultMetadata> {
    enabled.then(|| D1ResultMetadata {
        duration: started.elapsed().as_secs_f64() * 1000.0,
        changes: Some(changes),
        last_row_id: (changes > 0 && connection.last_insert_rowid() != previous_row_id)
            .then(|| connection.last_insert_rowid()),
        rows_read: None,
        rows_written: None,
        changed_db: None,
        size_after: None,
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum D1Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(String),
    /// An opt-in SQL API JavaScript number, not a SQLite storage-class assertion.
    Number(
        #[serde(
            serialize_with = "serialize_api_number",
            deserialize_with = "deserialize_api_number"
        )]
        f64,
    ),
}

fn checked_api_number(value: f64) -> Result<f64> {
    if !value.is_finite() || (value.fract() == 0.0 && value.abs() > 9_007_199_254_740_991.0) {
        return Err(DataError::InvalidApiNumber);
    }
    Ok(value)
}

fn serialize_api_number<S>(value: &f64, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    checked_api_number(*value)
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

fn deserialize_api_number<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    checked_api_number(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

impl D1Value {
    /// Preserve a standard SQL API number without claiming its original SQLite type.
    pub fn api_number(value: f64) -> Result<Self> {
        checked_api_number(value).map(Self::Number)
    }
}

fn json_size_bound(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(value) => {
            if *value {
                4
            } else {
                5
            }
        }
        serde_json::Value::Number(_) => 64,
        serde_json::Value::String(value) => string_size_bound(value),
        serde_json::Value::Array(values) => values.iter().fold(
            2usize.saturating_add(values.len().saturating_sub(1)),
            |size, value| size.saturating_add(json_size_bound(value)),
        ),
        serde_json::Value::Object(values) => values.iter().fold(
            2usize.saturating_add(values.len().saturating_sub(1)),
            |size, (name, value)| {
                size.saturating_add(string_size_bound(name))
                    .saturating_add(1)
                    .saturating_add(json_size_bound(value))
            },
        ),
    }
}

fn string_size_bound(value: &str) -> usize {
    2usize.saturating_add(value.len().saturating_mul(6))
}

fn api_cell_bound(value: &D1Value) -> Result<usize> {
    match value {
        D1Value::Null => Ok(4),
        D1Value::Integer(value)
            if (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(value) =>
        {
            Ok(64)
        }
        D1Value::Real(value) if value.is_finite() => Ok(64),
        D1Value::Number(value) => checked_api_number(*value).map(|_| 64),
        D1Value::Text(value) => Ok(string_size_bound(value)),
        D1Value::Blob(value) => {
            let bytes = STANDARD
                .decode(value)
                .map_err(|_| DataError::InvalidApiBlob)?;
            Ok(if bytes.is_empty() {
                2
            } else {
                1usize.saturating_add(bytes.len().saturating_mul(4))
            })
        }
        _ => Err(DataError::InvalidApiNumber),
    }
}

/// Conservative compact native SQL JSON size, including blob byte-array expansion.
/// The array form also bounds a single result. Strings allow six bytes per UTF-8
/// byte and numbers 64 bytes, covering the guest JSON codec before commit.
pub fn sql_api_response_bound(results: &[D1ResultSet]) -> Result<usize> {
    let mut size = 2usize.saturating_add(results.len().saturating_sub(1));
    for result in results {
        let meta = result.meta.as_ref().ok_or(DataError::InvalidApiMetadata)?;
        if !meta.duration.is_finite()
            || meta.duration < 0.0
            || meta.last_row_id.is_some_and(|value| {
                !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&value)
            })
            || [
                meta.changes,
                meta.rows_read,
                meta.rows_written,
                meta.size_after,
            ]
            .into_iter()
            .flatten()
            .any(|value| value > 9_007_199_254_740_991)
        {
            return Err(DataError::InvalidApiMetadata);
        }
        let prefix = br#"{"success":true,"meta":"#
            .len()
            .saturating_add(json_size_bound(
                &serde_json::to_value(meta).map_err(|_| DataError::InvalidApiMetadata)?,
            ));
        let mut row_columns = prefix
            .saturating_add(br#","results":{"columns":["#.len())
            .saturating_add(result.columns.len().saturating_sub(1));
        for name in &result.columns {
            row_columns = row_columns.saturating_add(string_size_bound(name));
        }
        row_columns = row_columns
            .saturating_add(br#"],"rows":["#.len())
            .saturating_add(result.rows.len().saturating_sub(1));
        let mut row_objects = prefix
            .saturating_add(br#","results":["#.len())
            .saturating_add(result.rows.len().saturating_sub(1));
        for row in &result.rows {
            if row.len() != result.columns.len() {
                return Err(DataError::InvalidApiMetadata);
            }
            row_columns = row_columns
                .saturating_add(2)
                .saturating_add(row.len().saturating_sub(1));
            row_objects = row_objects
                .saturating_add(2)
                .saturating_add(row.len().saturating_sub(1));
            for (name, value) in result.columns.iter().zip(row) {
                let value = api_cell_bound(value)?;
                row_columns = row_columns.saturating_add(value);
                row_objects = row_objects
                    .saturating_add(string_size_bound(name))
                    .saturating_add(1)
                    .saturating_add(value);
            }
        }
        row_columns = row_columns.saturating_add(b"]}}".len());
        row_objects = row_objects.saturating_add(b"]}".len());
        size = size.saturating_add(row_columns.max(row_objects));
    }
    Ok(size)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1Response {
    pub version: u16,
    pub request_id: String,
    pub status: D1Status,
    pub code: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<D1ResultSet>,
}

struct D1Store {
    binding: D1Binding,
    connection: Connection,
    operations: u64,
    request_bytes: u64,
    response_bytes: u64,
    authorizer_denied: Arc<AtomicBool>,
    invocation_deadline: Option<Instant>,
}

impl D1Store {
    fn open(binding: D1Binding) -> Result<Self> {
        let connection = match &binding.backing {
            Some(path) => Connection::open(path).map_err(|source| DataError::OpenBacking {
                path: path.clone(),
                source,
            })?,
            None => Connection::open_in_memory().map_err(DataError::InitializeBacking)?,
        };
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(DataError::InitializeBacking)?;
        for (limit, value) in [
            (
                Limit::SQLITE_LIMIT_LENGTH,
                binding
                    .limits
                    .max_result_bytes
                    .max(4096)
                    .min(i32::MAX as u64) as i32,
            ),
            (
                Limit::SQLITE_LIMIT_SQL_LENGTH,
                binding.limits.max_statement_bytes.min(i32::MAX as usize) as i32,
            ),
            (
                Limit::SQLITE_LIMIT_VARIABLE_NUMBER,
                binding.limits.max_parameters.min(i32::MAX as usize) as i32,
            ),
            (Limit::SQLITE_LIMIT_COLUMN, 256),
            (Limit::SQLITE_LIMIT_EXPR_DEPTH, 100),
            (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 32),
            (Limit::SQLITE_LIMIT_VDBE_OP, 100_000),
        ] {
            connection
                .set_limit(limit, value)
                .map_err(DataError::InitializeBacking)?;
        }
        let authorizer_denied = Arc::new(AtomicBool::new(false));
        let denied = authorizer_denied.clone();
        connection.authorizer(Some(move |context: AuthContext<'_>| {
            let allowed = context
                .database_name
                .is_none_or(|database| database == "main")
                && match context.action {
                    AuthAction::CreateIndex { .. }
                    | AuthAction::CreateTable { .. }
                    | AuthAction::CreateTrigger { .. }
                    | AuthAction::CreateView { .. }
                    | AuthAction::Delete { .. }
                    | AuthAction::DropIndex { .. }
                    | AuthAction::DropTable { .. }
                    | AuthAction::DropTrigger { .. }
                    | AuthAction::DropView { .. }
                    | AuthAction::Insert { .. }
                    | AuthAction::Read { .. }
                    | AuthAction::Select
                    | AuthAction::Transaction { .. }
                    | AuthAction::Update { .. }
                    | AuthAction::AlterTable { .. }
                    | AuthAction::Reindex { .. }
                    | AuthAction::Savepoint { .. }
                    | AuthAction::Recursive => true,
                    AuthAction::Function { function_name } => safe_sql_function(function_name),
                    _ => false,
                };
            if allowed {
                Authorization::Allow
            } else {
                denied.store(true, Ordering::Release);
                Authorization::Deny
            }
        }));
        Ok(Self {
            binding,
            connection,
            operations: 0,
            request_bytes: 0,
            response_bytes: 0,
            authorizer_denied,
            invocation_deadline: None,
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
        statements: Vec<D1Statement>,
        request_id: &str,
    ) -> std::result::Result<Vec<D1ResultSet>, &'static str> {
        validate_statements(&statements, self.binding.limits)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(self.binding.limits.max_execution_ms))
            .ok_or("execution_deadline")?;
        let deadline = self
            .invocation_deadline
            .map_or(deadline, |admitted| admitted.min(deadline));
        if Instant::now() >= deadline {
            return Err("execution_deadline");
        }
        self.connection
            .busy_timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50)),
            )
            .map_err(|_| "sqlite_deadline")?;
        self.connection
            .progress_handler(1000, Some(move || Instant::now() >= deadline));
        let transaction = self
            .connection
            .transaction()
            .map_err(|_| "sqlite_transaction")?;
        let mut total_rows = 0usize;
        let mut total_result_bytes = 0u64;
        let mut results = Vec::with_capacity(statements.len());
        for statement in statements {
            let started = Instant::now();
            let changes_before = transaction.total_changes();
            let previous_row_id = transaction.last_insert_rowid();
            let parameters = statement
                .parameters
                .into_iter()
                .map(D1Parameter::into_sql)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            self.authorizer_denied.store(false, Ordering::Release);
            let mut prepared = transaction.prepare(&statement.sql).map_err(|_| {
                if self.authorizer_denied.load(Ordering::Acquire) {
                    "forbidden_sql"
                } else {
                    "sqlite_prepare"
                }
            })?;
            if self.binding.read_only && !prepared.readonly() {
                return Err("read_only");
            }
            let columns = prepared
                .column_names()
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>();
            if columns.is_empty() {
                prepared
                    .execute(params_from_iter(parameters))
                    .map_err(|_| "sqlite_execute")?;
                let rows_affected =
                    if prepared.readonly() || transaction.total_changes() == changes_before {
                        0
                    } else {
                        transaction.changes()
                    };
                let result = D1ResultSet {
                    columns,
                    rows: Vec::new(),
                    rows_affected,
                    meta: result_metadata(
                        self.binding.api_metadata,
                        started,
                        &transaction,
                        rows_affected,
                        previous_row_id,
                    ),
                };
                charge_result_bytes(
                    &mut total_result_bytes,
                    &result,
                    self.binding.limits.max_result_bytes,
                )?;
                results.push(result);
                continue;
            }
            let mut rows = prepared
                .query(params_from_iter(parameters))
                .map_err(|_| "sqlite_query")?;
            let mut values = Vec::new();
            let empty_result = D1ResultSet {
                columns: columns.clone(),
                rows: Vec::new(),
                rows_affected: 0,
                meta: None,
            };
            let mut encoded_bytes = serde_json::to_vec(&empty_result)
                .map_err(|_| "sqlite_value")?
                .len() as u64;
            while let Some(row) = rows.next().map_err(|_| "sqlite_query")? {
                total_rows += 1;
                if total_rows > self.binding.limits.max_rows {
                    return Err("row_quota");
                }
                let row_values = (0..columns.len())
                    .map(|index| {
                        row.get_ref(index)
                            .map_err(|_| "sqlite_value")
                            .and_then(sql_value)
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let row_bytes = serde_json::to_vec(&row_values)
                    .map_err(|_| "sqlite_value")?
                    .len() as u64;
                encoded_bytes =
                    encoded_bytes.saturating_add(row_bytes + u64::from(!values.is_empty()));
                if total_result_bytes.saturating_add(encoded_bytes)
                    > self.binding.limits.max_result_bytes
                {
                    return Err("result_bytes_quota");
                }
                values.push(row_values);
            }
            drop(rows);
            let rows_affected =
                if prepared.readonly() || transaction.total_changes() == changes_before {
                    0
                } else {
                    transaction.changes()
                };
            let result = D1ResultSet {
                columns,
                rows: values,
                rows_affected,
                meta: result_metadata(
                    self.binding.api_metadata,
                    started,
                    &transaction,
                    rows_affected,
                    previous_row_id,
                ),
            };
            charge_result_bytes(
                &mut total_result_bytes,
                &result,
                self.binding.limits.max_result_bytes,
            )?;
            results.push(result);
        }
        if self.binding.api_metadata {
            let size = sql_api_response_bound(&results).map_err(|error| match error {
                DataError::InvalidApiNumber => "unsafe_integer",
                _ => "api_response_invalid",
            })?;
            if size > 60 * 1024 {
                return Err("api_response_bytes_quota");
            }
        }
        #[derive(Serialize)]
        struct BorrowedResponse<'a> {
            version: u16,
            request_id: &'a str,
            status: D1Status,
            code: &'a str,
            results: &'a [D1ResultSet],
        }
        let encoded = serde_json::to_vec(&BorrowedResponse {
            version: 1,
            request_id,
            status: D1Status::Ok,
            code: "ok",
            results: &results,
        })
        .map_err(|_| "sqlite_value")?;
        if encoded.len() > 60 * 1024 {
            return Err("response_bytes_quota");
        }
        if self.response_bytes.saturating_add(encoded.len() as u64)
            > self.binding.limits.max_response_bytes
        {
            return Err("response_bytes_quota");
        }
        transaction.commit().map_err(|_| "sqlite_commit")?;
        Ok(results)
    }
}

fn validate_statements(
    statements: &[D1Statement],
    limits: D1Limits,
) -> std::result::Result<(), &'static str> {
    if statements.is_empty() || statements.len() > limits.max_statements {
        return Err("statement_quota");
    }
    let mut parameters = 0usize;
    for statement in statements {
        if statement.sql.is_empty() || statement.sql.len() > limits.max_statement_bytes {
            return Err("statement_size");
        }
        parameters = parameters.saturating_add(statement.parameters.len());
        if parameters > limits.max_parameters {
            return Err("parameter_quota");
        }
    }
    Ok(())
}

fn safe_sql_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "abs"
            | "char"
            | "coalesce"
            | "concat"
            | "concat_ws"
            | "format"
            | "glob"
            | "hex"
            | "ifnull"
            | "iif"
            | "instr"
            | "length"
            | "like"
            | "likelihood"
            | "likely"
            | "lower"
            | "ltrim"
            | "max"
            | "min"
            | "nullif"
            | "octet_length"
            | "printf"
            | "quote"
            | "random"
            | "randomblob"
            | "replace"
            | "round"
            | "rtrim"
            | "sign"
            | "soundex"
            | "substr"
            | "substring"
            | "total_changes"
            | "changes"
            | "trim"
            | "typeof"
            | "unhex"
            | "unicode"
            | "unlikely"
            | "upper"
            | "zeroblob"
            | "last_insert_rowid"
            | "avg"
            | "count"
            | "sum"
            | "total"
            | "group_concat"
            | "string_agg"
            | "date"
            | "time"
            | "datetime"
            | "julianday"
            | "unixepoch"
            | "strftime"
            | "timediff"
            | "json"
            | "json_array"
            | "json_array_length"
            | "json_error_position"
            | "json_extract"
            | "json_insert"
            | "json_object"
            | "json_patch"
            | "json_quote"
            | "json_remove"
            | "json_replace"
            | "json_set"
            | "json_type"
            | "json_valid"
            | "json_group_array"
            | "json_group_object"
            | "jsonb"
            | "jsonb_array"
            | "jsonb_extract"
            | "jsonb_insert"
            | "jsonb_object"
            | "jsonb_patch"
            | "jsonb_remove"
            | "jsonb_replace"
            | "jsonb_set"
            | "->"
            | "->>"
            | "row_number"
            | "rank"
            | "dense_rank"
            | "percent_rank"
            | "cume_dist"
            | "ntile"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
            | "nth_value"
            | "sqlite_version"
            | "sqlite_source_id"
    )
}

fn charge_result_bytes(
    total: &mut u64,
    result: &D1ResultSet,
    limit: u64,
) -> std::result::Result<(), &'static str> {
    let bytes = serde_json::to_vec(result)
        .map_err(|_| "result_encoding")?
        .len() as u64;
    *total = total.checked_add(bytes).ok_or("result_bytes_quota")?;
    if *total > limit {
        return Err("result_bytes_quota");
    }
    Ok(())
}

fn sql_value(value: ValueRef<'_>) -> std::result::Result<D1Value, &'static str> {
    Ok(match value {
        ValueRef::Null => D1Value::Null,
        ValueRef::Integer(value) => D1Value::Integer(value),
        ValueRef::Real(value) if value.is_finite() => D1Value::Real(value),
        ValueRef::Real(_) => return Err("invalid_real"),
        ValueRef::Text(value) => D1Value::Text(
            std::str::from_utf8(value)
                .map_err(|_| "invalid_text")?
                .to_string(),
        ),
        ValueRef::Blob(value) => D1Value::Blob(STANDARD.encode(value)),
    })
}

pub struct D1Service {
    stores: Vec<D1Store>,
}

impl D1Service {
    pub fn new(bindings: impl IntoIterator<Item = D1Binding>) -> Result<Self> {
        let mut stores = bindings
            .into_iter()
            .map(D1Store::open)
            .collect::<Result<Vec<_>>>()?;
        stores.sort_by(|left, right| left.binding.name.cmp(&right.binding.name));
        if stores
            .windows(2)
            .any(|pair| pair[0].binding.name == pair[1].binding.name)
        {
            return Err(DataError::InvalidBinding);
        }
        Ok(Self { stores })
    }

    fn parse(payload: &[u8]) -> std::result::Result<D1Request, &'static str> {
        if payload.len() > MAX_WIRE_BYTES {
            return Err("request_too_large");
        }
        let request: D1Request = serde_json::from_slice(payload).map_err(|_| "invalid_request")?;
        if request.version != D1_PROTOCOL_VERSION {
            return Err("unsupported_version");
        }
        validate_request_id(&request.request_id).map_err(|_| "invalid_request_id")?;
        validate_binding(&request.binding).map_err(|_| "invalid_binding")?;
        Ok(request)
    }

    fn rejection(request_id: impl Into<String>, status: D1Status, code: &str) -> Vec<u8> {
        serde_json::to_vec(&D1Response {
            version: D1_PROTOCOL_VERSION,
            request_id: request_id.into(),
            status,
            code: safe_code(code).to_string(),
            results: Vec::new(),
        })
        .unwrap_or_default()
    }
}

impl LogicalWireService for D1Service {
    fn set_deadline(&mut self, deadline: Instant) {
        for store in &mut self.stores {
            store.invocation_deadline = Some(deadline);
        }
    }

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
            Err(code) => return Self::rejection("", D1Status::InvalidRequest, code),
        };
        let Some(store) = self
            .stores
            .iter_mut()
            .find(|store| store.binding.name == request.binding)
        else {
            return Self::rejection(request.request_id, D1Status::Denied, "binding_denied");
        };
        if let Err(code) = store.reserve(payload.len() as u64) {
            return Self::rejection(request.request_id, D1Status::QuotaExceeded, code);
        }
        let D1Operation::Batch { statements } = request.operation;
        let results = match store.execute(statements, &request.request_id) {
            Ok(results) => results,
            Err(code @ ("read_only" | "forbidden_sql")) => {
                return Self::rejection(request.request_id, D1Status::Denied, code);
            }
            Err(
                code @ ("operation_quota"
                | "request_bytes_quota"
                | "response_bytes_quota"
                | "api_response_bytes_quota"
                | "statement_quota"
                | "statement_size"
                | "parameter_quota"
                | "row_quota"
                | "result_bytes_quota"),
            ) => {
                return Self::rejection(request.request_id, D1Status::QuotaExceeded, code);
            }
            Err(code @ ("invalid_real" | "invalid_base64" | "unsafe_integer")) => {
                return Self::rejection(request.request_id, D1Status::InvalidRequest, code);
            }
            Err(code) => return Self::rejection(request.request_id, D1Status::HostError, code),
        };
        let response = D1Response {
            version: D1_PROTOCOL_VERSION,
            request_id: request.request_id.clone(),
            status: D1Status::Ok,
            code: "ok".to_string(),
            results,
        };
        let encoded = serde_json::to_vec(&response).unwrap_or_else(|_| {
            Self::rejection(&request.request_id, D1Status::HostError, "host_error")
        });
        if let Err(code) = store.settle(encoded.len() as u64) {
            return Self::rejection(request.request_id, D1Status::QuotaExceeded, code);
        }
        encoded
    }

    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
        let status = match status {
            LogicalRuntimeStatus::Denied => D1Status::Denied,
            LogicalRuntimeStatus::InvalidRequest => D1Status::InvalidRequest,
            LogicalRuntimeStatus::HostError => D1Status::HostError,
        };
        Self::rejection("", status, code)
    }

    fn reset_for_fresh_vm(&mut self) -> std::result::Result<(), BrokerHostError> {
        self.stores
            .iter_mut()
            .for_each(D1Store::reset_request_budget);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    const BATCH_FIXTURE: &str = include_str!("../../tests/fixtures/data_d1_batch_v1.json");

    fn identity() -> RequestIdentity {
        RequestIdentity::new("worker-a", "snapshot-1", 0).unwrap()
    }

    fn runtime(binding: D1Binding) -> BrokerRuntime {
        let name = binding.name().to_string();
        BrokerRuntime::deny_all(identity())
            .with_logical([name], D1Service::new([binding]).unwrap())
            .unwrap()
    }

    fn response(runtime: &BrokerRuntime, request: &[u8]) -> D1Response {
        serde_json::from_slice(&runtime.dispatch_logical(request)).unwrap()
    }

    fn api_query(runtime: &BrokerRuntime, statements: &[&str]) -> D1Response {
        response(
            runtime,
            &serde_json::to_vec(&D1Request {
                version: 1,
                request_id: "api-query".into(),
                binding: "DB".into(),
                operation: D1Operation::Batch {
                    statements: statements
                        .iter()
                        .map(|sql| D1Statement {
                            sql: (*sql).into(),
                            parameters: vec![],
                        })
                        .collect(),
                },
            })
            .unwrap(),
        )
    }

    #[test]
    fn api_metadata_measures_returning_and_never_reuses_prior_statement_changes() {
        let runtime = runtime(
            D1Binding::in_memory("DB", false, Default::default())
                .unwrap()
                .with_d1_api_metadata(),
        );
        let result = api_query(
            &runtime,
            &[
                "CREATE TABLE items(value TEXT)",
                "INSERT INTO items(value) VALUES('first') RETURNING rowid",
                "SELECT value FROM items",
                "CREATE TABLE other(value TEXT)",
                "UPDATE items SET value='second' RETURNING value",
                "SELECT value FROM items",
            ],
        );
        assert_eq!(result.status, D1Status::Ok, "{result:?}");
        assert_eq!(
            result
                .results
                .iter()
                .map(|result| result.rows_affected)
                .collect::<Vec<_>>(),
            [0, 1, 0, 0, 1, 0]
        );
        for result in &result.results {
            let meta = result.meta.as_ref().unwrap();
            assert!(meta.duration.is_finite() && meta.duration >= 0.0);
            assert_eq!(meta.changes, Some(result.rows_affected));
            assert_eq!(
                (
                    meta.rows_read,
                    meta.rows_written,
                    meta.changed_db,
                    meta.size_after
                ),
                (None, None, None, None)
            );
        }
        assert_eq!(
            result.results[1].meta.as_ref().unwrap().last_row_id,
            Some(1)
        );
        assert_eq!(result.results[2].meta.as_ref().unwrap().last_row_id, None);
        assert_eq!(result.results[3].meta.as_ref().unwrap().last_row_id, None);
        assert_eq!(result.results[4].meta.as_ref().unwrap().last_row_id, None);
    }

    #[test]
    fn api_result_failures_roll_back_mutation_before_commit() {
        let runtime = runtime(
            D1Binding::in_memory("DB", false, Default::default())
                .unwrap()
                .with_d1_api_metadata(),
        );
        assert_eq!(
            api_query(&runtime, &["CREATE TABLE items(value INTEGER)"]).status,
            D1Status::Ok
        );
        let repeated_columns = format!(
            "INSERT INTO items(value) VALUES {} RETURNING value AS \"{}\"",
            (0..70).map(|_| "(1)").collect::<Vec<_>>().join(","),
            "column".repeat(171),
        );
        for (sql, expected) in [
            (
                "INSERT INTO items(value) VALUES(9007199254740992) RETURNING value",
                "unsafe_integer",
            ),
            (
                "INSERT INTO items(value) VALUES(1) RETURNING randomblob(20000)",
                "api_response_bytes_quota",
            ),
            (repeated_columns.as_str(), "api_response_bytes_quota"),
        ] {
            let result = api_query(&runtime, &[sql]);
            assert_eq!(result.code, expected, "{result:?}");
            assert!(result.results.is_empty());
            let check = api_query(&runtime, &["SELECT COUNT(*) FROM items"]);
            assert_eq!(check.results[0].rows[0][0], D1Value::Integer(0));
        }
        let failed = api_query(
            &runtime,
            &["INSERT INTO items VALUES(1)", "SELECT missing FROM items"],
        );
        assert_eq!(failed.status, D1Status::HostError);
        assert_eq!(
            api_query(&runtime, &["SELECT COUNT(*) FROM items"]).results[0].rows[0][0],
            D1Value::Integer(0)
        );
    }

    #[test]
    fn api_numbers_are_explicit_result_values_and_reject_unsafe_or_nonfinite_values() {
        for number in [0.0, 1.0, -1.25, 9_007_199_254_740_991.0] {
            let value = D1Value::api_number(number).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(encoded["type"], "number");
            assert_eq!(encoded["value"].as_f64(), Some(number));
            assert_eq!(serde_json::from_value::<D1Value>(encoded).unwrap(), value);
        }
        for number in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            9_007_199_254_740_992.0,
            -9_007_199_254_740_992.0,
        ] {
            assert!(D1Value::api_number(number).is_err());
            assert!(serde_json::to_vec(&D1Value::Number(number)).is_err());
        }
        assert!(
            serde_json::from_value::<D1Parameter>(serde_json::json!({"type":"number","value":1}))
                .is_err()
        );
        assert_eq!(
            serde_json::to_string(&D1Value::Integer(1)).unwrap(),
            r#"{"type":"integer","value":1}"#
        );
        assert_eq!(
            serde_json::to_string(&D1Value::Blob("AAE=".into())).unwrap(),
            r#"{"type":"blob","value":"AAE="}"#
        );
    }

    #[test]
    fn legacy_sql_results_do_not_gain_api_metadata() {
        let runtime = runtime(D1Binding::in_memory("DB", false, Default::default()).unwrap());
        let response = api_query(&runtime, &["SELECT 1"]);
        assert_eq!(response.status, D1Status::Ok);
        assert!(response.results[0].meta.is_none());
        assert!(
            !serde_json::to_string(&response)
                .unwrap()
                .contains("\"meta\"")
        );
    }

    #[test]
    fn api_response_bound_covers_native_json_and_rejects_invalid_metadata() {
        let mut result = D1ResultSet {
            columns: ["n", "i", "r", "j", "t", "b"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            rows: vec![vec![
                D1Value::Null,
                D1Value::Integer(1),
                D1Value::Real(1.25),
                D1Value::api_number(1.0).unwrap(),
                D1Value::Text("é\n\"\\\0".into()),
                D1Value::Blob("AP8=".into()),
            ]],
            rows_affected: 0,
            meta: Some(D1ResultMetadata {
                duration: 0.25,
                changes: Some(0),
                last_row_id: None,
                rows_read: None,
                rows_written: None,
                changed_db: None,
                size_after: None,
            }),
        };
        let native = serde_json::json!([{"success":true,"meta":{"duration":0.25,"changes":0},
            "results":{"columns":["n","i","r","j","t","b"],"rows":[[null,1,1.25,1.0,"é\n\"\\\0",[0,255]]]}}]);
        assert!(
            sql_api_response_bound(&[result.clone()]).unwrap()
                >= serde_json::to_vec(&native).unwrap().len()
        );
        let objects = serde_json::json!([{"success":true,"meta":{"duration":0.25,"changes":0},
            "results":[{"n":null,"i":1,"r":1.25,"j":1.0,"t":"é\n\"\\\0","b":[0,255]}]}]);
        assert!(
            sql_api_response_bound(&[result.clone()]).unwrap()
                >= serde_json::to_vec(&objects).unwrap().len()
        );
        result.meta.as_mut().unwrap().duration = -1.0;
        assert!(sql_api_response_bound(&[result.clone()]).is_err());
        result.meta = None;
        assert!(sql_api_response_bound(&[result]).is_err());
    }

    #[test]
    fn sql_batch_alias_decodes_to_batch_operation() {
        let request: D1Request = serde_json::from_value(serde_json::json!({
            "version": 1,
            "request_id": "sql-query",
            "binding": "database",
            "operation": {
                "kind": "sql_batch",
                "statements": [{
                    "sql": "SELECT 1",
                    "parameters": []
                }]
            }
        }))
        .unwrap();

        assert_eq!(
            request.operation,
            D1Operation::Batch {
                statements: vec![D1Statement {
                    sql: "SELECT 1".to_string(),
                    parameters: Vec::new(),
                }],
            }
        );
    }

    #[test]
    fn sql_operation_serializes_to_workerd_compatibility_token() {
        let operation = D1Operation::Batch {
            statements: vec![D1Statement {
                sql: "SELECT 1".to_string(),
                parameters: Vec::new(),
            }],
        };

        assert_eq!(
            serde_json::to_value(operation).unwrap(),
            serde_json::json!({
                "kind": "d1_batch",
                "statements": [{
                    "sql": "SELECT 1",
                    "parameters": []
                }]
            })
        );
    }

    #[test]
    fn deterministic_batch_is_transactional_and_persists_across_reset() {
        let runtime =
            runtime(D1Binding::in_memory("database", false, D1Limits::default()).unwrap());
        let batch = response(&runtime, BATCH_FIXTURE.as_bytes());
        runtime.reset_for_fresh_vm().unwrap();
        let query = serde_json::to_vec(&D1Request {
            version: 1,
            request_id: "query".to_string(),
            binding: "database".to_string(),
            operation: D1Operation::Batch {
                statements: vec![D1Statement {
                    sql: "SELECT id, name FROM users ORDER BY id".to_string(),
                    parameters: Vec::new(),
                }],
            },
        })
        .unwrap();
        let queried = response(&runtime, &query);

        assert_eq!(
            (
                batch.status,
                queried.status,
                &queried.results[0].columns,
                &queried.results[0].rows,
            ),
            (
                D1Status::Ok,
                D1Status::Ok,
                &vec!["id".to_string(), "name".to_string()],
                &vec![vec![D1Value::Integer(1), D1Value::Text("Ada".to_string())]],
            )
        );
    }

    #[test]
    fn forbidden_sql_and_read_only_mutation_are_denied() {
        let runtime = runtime(D1Binding::in_memory("database", true, D1Limits::default()).unwrap());
        let request = |id: &str, sql: &str| {
            serde_json::to_vec(&D1Request {
                version: 1,
                request_id: id.to_string(),
                binding: "database".to_string(),
                operation: D1Operation::Batch {
                    statements: vec![D1Statement {
                        sql: sql.to_string(),
                        parameters: Vec::new(),
                    }],
                },
            })
            .unwrap()
        };
        let mutation = response(&runtime, &request("mutation", "CREATE TABLE x(id INTEGER)"));
        let attach = response(
            &runtime,
            &request("attach", "ATTACH DATABASE ':memory:' AS other"),
        );

        assert_eq!(
            (mutation.status, mutation.code, attach.status, attach.code,),
            (
                D1Status::Denied,
                "read_only".to_string(),
                D1Status::Denied,
                "forbidden_sql".to_string(),
            )
        );
    }

    #[test]
    fn sqlite_authorizer_denies_host_paths_extensions_pragmas_and_virtual_tables_without_token_filter()
     {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("private.sqlite");
        let exported = tmp.path().join("export.sqlite");
        let store =
            D1Store::open(D1Binding::in_memory("database", false, D1Limits::default()).unwrap())
                .unwrap();
        store
            .connection
            .execute_batch("CREATE TABLE allowed(id INTEGER);INSERT INTO allowed VALUES(1);")
            .unwrap();
        for sql in [
            format!(
                "-- comment\nATTACH DATABASE '{}' AS stolen",
                outside.display()
            ),
            "DETACH DATABASE main".into(),
            format!("VACUUM INTO '{}'", exported.display()),
            "PRAGMA writable_schema=ON".into(),
            "SELECT load_extension('/run/host-secrets/extension')".into(),
            "SELECT readfile('/run/host-secrets/checkpoint-key')".into(),
            "SELECT writefile('/state/checkpoints.db','overwrite')".into(),
            "CREATE VIRTUAL TABLE stolen USING csv(filename='/run/host-secrets/token')".into(),
            "SELECT * FROM pragma_database_list".into(),
        ] {
            assert!(store.connection.execute_batch(&sql).is_err(), "{sql}");
        }
        assert!(!outside.exists());
        assert!(!exported.exists());
        assert_eq!(
            store
                .connection
                .query_row("SELECT 'attach' || 'pragma' FROM allowed", [], |row| row
                    .get::<_, String>(
                    0
                ))
                .unwrap(),
            "attachpragma"
        );
    }

    #[test]
    fn bounded_sqlite_work_cannot_allocate_huge_values_or_spin_past_admitted_limit() {
        let mut store = D1Store::open(
            D1Binding::in_memory(
                "database",
                false,
                D1Limits {
                    max_execution_ms: 5,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
        let statement = |sql: &str| {
            vec![D1Statement {
                sql: sql.into(),
                parameters: vec![],
            }]
        };
        assert!(
            store
                .execute(statement("SELECT randomblob(1000000000)"), "too-large")
                .is_err()
        );
        let started = Instant::now();
        assert!(store.execute(statement("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n"),"deadline").is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn response_quota_failure_rolls_back_mutation_before_acknowledgement() {
        let mut store = D1Store::open(
            D1Binding::in_memory(
                "database",
                false,
                D1Limits {
                    max_response_bytes: 1,
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
        store
            .connection
            .execute_batch("CREATE TABLE records(value TEXT);INSERT INTO records VALUES('before')")
            .unwrap();
        let result = store.execute(
            vec![D1Statement {
                sql: "UPDATE records SET value='after' RETURNING value".into(),
                parameters: vec![],
            }],
            "quota",
        );
        assert_eq!(result.unwrap_err(), "response_bytes_quota");
        assert_eq!(
            store
                .connection
                .query_row("SELECT value FROM records", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "before"
        );
    }

    #[test]
    fn row_and_parameter_quotas_fail_closed() {
        let limits = D1Limits {
            max_parameters: 1,
            max_rows: 1,
            ..D1Limits::default()
        };
        let runtime = runtime(D1Binding::in_memory("database", false, limits).unwrap());
        let parameters = serde_json::to_vec(&D1Request {
            version: 1,
            request_id: "parameters".to_string(),
            binding: "database".to_string(),
            operation: D1Operation::Batch {
                statements: vec![D1Statement {
                    sql: "SELECT ?1, ?2".to_string(),
                    parameters: vec![D1Parameter::Integer(1), D1Parameter::Integer(2)],
                }],
            },
        })
        .unwrap();
        let rows = serde_json::to_vec(&D1Request {
            version: 1,
            request_id: "rows".to_string(),
            binding: "database".to_string(),
            operation: D1Operation::Batch {
                statements: vec![D1Statement {
                    sql: "SELECT 1 UNION ALL SELECT 2".to_string(),
                    parameters: Vec::new(),
                }],
            },
        })
        .unwrap();

        assert_eq!(
            (
                response(&runtime, &parameters).code,
                response(&runtime, &rows).code,
            ),
            ("parameter_quota".to_string(), "row_quota".to_string())
        );
    }

    #[test]
    fn later_statement_failure_rolls_back_the_whole_batch() {
        let runtime =
            runtime(D1Binding::in_memory("database", false, D1Limits::default()).unwrap());
        let setup = serde_json::to_vec(&D1Request {
            version: 1,
            request_id: "setup".to_string(),
            binding: "database".to_string(),
            operation: D1Operation::Batch {
                statements: vec![D1Statement {
                    sql: "CREATE TABLE items(id INTEGER PRIMARY KEY)".to_string(),
                    parameters: Vec::new(),
                }],
            },
        })
        .unwrap();
        assert_eq!(response(&runtime, &setup).status, D1Status::Ok);
        let failing = serde_json::to_vec(&D1Request {
            version: 1,
            request_id: "failing".to_string(),
            binding: "database".to_string(),
            operation: D1Operation::Batch {
                statements: vec![
                    D1Statement {
                        sql: "INSERT INTO items(id) VALUES (1)".to_string(),
                        parameters: Vec::new(),
                    },
                    D1Statement {
                        sql: "INSERT INTO items(id) VALUES (1)".to_string(),
                        parameters: Vec::new(),
                    },
                ],
            },
        })
        .unwrap();
        assert_eq!(response(&runtime, &failing).status, D1Status::HostError);
        let count = serde_json::to_vec(&D1Request {
            version: 1,
            request_id: "count".to_string(),
            binding: "database".to_string(),
            operation: D1Operation::Batch {
                statements: vec![D1Statement {
                    sql: "SELECT COUNT(*) AS count FROM items".to_string(),
                    parameters: Vec::new(),
                }],
            },
        })
        .unwrap();
        let counted = response(&runtime, &count);

        assert_eq!(counted.results[0].rows, vec![vec![D1Value::Integer(0)]]);
    }

    #[test]
    fn unknown_executor_local_operation_is_rejected_centrally() {
        let runtime =
            runtime(D1Binding::in_memory("database", false, D1Limits::default()).unwrap());
        let rejected: D1Response = serde_json::from_slice(&runtime.dispatch_logical(
            br#"{"version":1,"request_id":"private","binding":"database","operation":{"kind":"d1_private_query","sql":"SELECT 1"}}"#,
        ))
        .unwrap();

        assert_eq!(
            (rejected.status, rejected.code, rejected.request_id),
            (
                D1Status::InvalidRequest,
                "invalid_request".to_string(),
                String::new(),
            )
        );
    }
}
