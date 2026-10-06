// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_BODY_BYTES: usize = 32 * 1024;
pub const MAX_ENVELOPE_BYTES: usize = 60 * 1024;
pub const MAX_BUNDLE_SOURCE_BYTES: usize = 48 * 1024;
pub const MAX_MODULE_SOURCE_BYTES: usize = 48 * 1024;
pub const MAX_MODULES: usize = 32;
pub const MAX_COMPATIBILITY_FLAGS: usize = 32;
pub const MAX_HEADERS: usize = 64;
pub const MAX_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_REQUEST_ID_BYTES: usize = 64;
const EXECUTOR_INIT_PROTOCOL_VERSION: u16 = 2;
const EXECUTOR_BINDING_PROTOCOL_VERSION: u16 = 3;
const MAX_EXECUTOR_STORAGE_BINDINGS: usize = 8;
const MAX_EXECUTOR_BINDINGS: usize = 32;
const MAX_QUEUE_MESSAGES: usize = 100;
const MAX_QUEUE_NAME_BYTES: usize = 64;
const MAX_CRON_BYTES: usize = 256;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WorkerVersionId(String);

impl WorkerVersionId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        identifier(&value, 256)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for WorkerVersionId {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<WorkerVersionId> for String {
    fn from(value: WorkerVersionId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEnvelope {
    pub protocol_version: u16,
    pub request_id: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<Header>,
    pub body_base64: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseEnvelope {
    pub protocol_version: u16,
    pub request_id: String,
    pub status: u16,
    pub headers: Vec<Header>,
    pub body_base64: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ModuleType {
    #[serde(rename = "esModule")]
    EsModule,
    #[serde(rename = "commonJsModule")]
    CommonJsModule,
    #[serde(rename = "wasm")]
    Wasm,
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "json")]
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerModule {
    pub name: String,
    #[serde(rename = "type")]
    pub module_type: ModuleType,
    pub source: String,
}

impl WorkerModule {
    pub fn wasm(name: impl Into<String>, bytes: &[u8]) -> Self {
        Self {
            name: name.into(),
            module_type: ModuleType::Wasm,
            source: STANDARD.encode(bytes),
        }
    }

    fn decoded_source_len(&self) -> Result<usize> {
        if self.module_type != ModuleType::Wasm {
            return Ok(self.source.len());
        }
        STANDARD
            .decode(&self.source)
            .map(|bytes| bytes.len())
            .map_err(|_| invalid("invalid Wasm module base64"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBundle {
    pub protocol_version: u16,
    pub worker_version: WorkerVersionId,
    pub compatibility_date: String,
    pub compatibility_flags: Vec<String>,
    pub main_module: String,
    pub modules: Vec<WorkerModule>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerBindingKind {
    Kv,
    Cache,
    D1,
    DurableObject,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkerBinding {
    pub name: String,
    pub kind: WorkerBindingKind,
}

#[derive(Serialize)]
struct ExecutorStorageBinding<'a> {
    name: &'a str,
    mode: &'static str,
}

#[derive(Serialize)]
struct ExecutorBinding<'a> {
    name: &'a str,
    kind: WorkerBindingKind,
}

#[derive(Serialize)]
struct ExecutorInit<'a> {
    protocol_version: u16,
    worker_version: &'a WorkerVersionId,
    compatibility_date: &'a str,
    compatibility_flags: &'a [String],
    main_module: &'a str,
    modules: &'a [WorkerModule],
    storage: &'a [ExecutorStorageBinding<'a>],
    #[serde(skip_serializing_if = "Option::is_none")]
    bindings: Option<&'a [ExecutorBinding<'a>]>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledRequest {
    pub protocol_version: u16,
    pub request_id: String,
    pub scheduled_time_unix_ms: u64,
    pub cron: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduledResponse {
    pub protocol_version: u16,
    pub request_id: String,
    pub outcome: String,
    pub retry: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMessage {
    pub id: String,
    pub timestamp_unix_ms: u64,
    pub body_base64: String,
    pub content_type: Option<String>,
    pub attempts: u16,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMetadata {
    pub backlog_count: f64,
    pub backlog_bytes: f64,
    pub oldest_message_timestamp_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRequest {
    pub protocol_version: u16,
    pub request_id: String,
    pub queue: String,
    pub messages: Vec<QueueMessage>,
    pub metadata: QueueMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueBatchRetry {
    pub retry: bool,
    pub delay_seconds: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueMessageRetry {
    pub id: String,
    pub delay_seconds: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueResponse {
    pub protocol_version: u16,
    pub request_id: String,
    pub outcome: String,
    pub ack_all: bool,
    pub retry_batch: QueueBatchRetry,
    pub explicit_acks: Vec<String>,
    pub retry_messages: Vec<QueueMessageRetry>,
}

fn invalid(message: &str) -> Error {
    Error::Protocol(message.into())
}

fn identifier(value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
    {
        return Err(invalid("invalid identifier"));
    }
    Ok(())
}

fn common(version: u16, id: &str, headers: &[Header], body: &str) -> Result<()> {
    if version != PROTOCOL_VERSION {
        return Err(invalid("unsupported protocol version"));
    }
    identifier(id, MAX_REQUEST_ID_BYTES)?;
    if headers.len() > MAX_HEADERS {
        return Err(invalid("too many headers"));
    }
    let mut bytes = 0;
    for h in headers {
        if h.name.is_empty()
            || h.name.len() > 256
            || !h
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            || h.value.bytes().any(|b| b.is_ascii_control() && b != b'\t')
        {
            return Err(invalid("invalid header"));
        }
        bytes += h.name.len() + h.value.len();
        if bytes > MAX_HEADER_BYTES {
            return Err(invalid("headers exceed size limit"));
        }
    }
    if body.len() > MAX_BODY_BYTES.div_ceil(3) * 4 {
        return Err(invalid("body exceeds size limit"));
    }
    let decoded = STANDARD
        .decode(body)
        .map_err(|_| invalid("invalid base64"))?;
    if decoded.len() > MAX_BODY_BYTES {
        return Err(invalid("body exceeds size limit"));
    }
    Ok(())
}

fn bounded(input: &[u8]) -> Result<()> {
    if input.len() > MAX_ENVELOPE_BYTES {
        return Err(invalid("envelope exceeds size limit"));
    }
    Ok(())
}

impl WorkerBundle {
    pub fn from_json(input: &[u8]) -> Result<Self> {
        bounded(input)?;
        let mut bundle: Self = serde_json::from_slice(input)?;
        bundle.canonicalize()?;
        bundle.validate()?;
        Ok(bundle)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let mut bytes = Vec::new();
        File::open(path)?
            .take((MAX_ENVELOPE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        Self::from_json(&bytes)
    }

    pub fn single_script(
        worker_version: WorkerVersionId,
        compatibility_date: impl Into<String>,
        module_name: impl Into<String>,
        source: impl Into<String>,
    ) -> Result<Self> {
        let main_module = module_name.into();
        let mut bundle = Self {
            protocol_version: PROTOCOL_VERSION,
            worker_version,
            compatibility_date: compatibility_date.into(),
            compatibility_flags: Vec::new(),
            main_module: main_module.clone(),
            modules: vec![WorkerModule {
                name: main_module,
                module_type: ModuleType::EsModule,
                source: source.into(),
            }],
        };
        bundle.canonicalize()?;
        bundle.validate()?;
        Ok(bundle)
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(invalid("unsupported protocol version"));
        }
        validate_date(&self.compatibility_date)?;
        if self.compatibility_flags.len() > MAX_COMPATIBILITY_FLAGS {
            return Err(invalid("too many compatibility flags"));
        }
        let mut flag_bytes = 0;
        let mut previous = None;
        for flag in &self.compatibility_flags {
            if flag.is_empty()
                || flag.len() > 64
                || !flag
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(invalid("invalid compatibility flag"));
            }
            if previous.is_some_and(|value: &String| value >= flag) {
                return Err(invalid("compatibility flags are not unique and sorted"));
            }
            previous = Some(flag);
            flag_bytes += flag.len();
        }
        if flag_bytes > 2048 {
            return Err(invalid("compatibility flags exceed size limit"));
        }
        if self.modules.is_empty() || self.modules.len() > MAX_MODULES {
            return Err(invalid("invalid module count"));
        }
        if self.modules[0].name != self.main_module
            || self.modules[0].module_type != ModuleType::EsModule
        {
            return Err(invalid("main module must be the first ES module"));
        }
        let mut source_bytes = 0;
        for (index, module) in self.modules.iter().enumerate() {
            validate_module_name(&module.name)?;
            if self.modules[..index]
                .iter()
                .any(|previous| previous.name == module.name)
            {
                return Err(invalid("duplicate module name"));
            }
            if index > 1 && self.modules[index - 1].name >= module.name {
                return Err(invalid("modules are not unique and canonically ordered"));
            }
            let module_source_bytes = module.decoded_source_len()?;
            if module_source_bytes > MAX_MODULE_SOURCE_BYTES {
                return Err(invalid("module source exceeds size limit"));
            }
            source_bytes += module_source_bytes;
        }
        if source_bytes > MAX_BUNDLE_SOURCE_BYTES {
            return Err(invalid("aggregate module source exceeds size limit"));
        }
        let json = serde_json::to_vec(self)?;
        bounded(&json)?;
        Ok(())
    }

    pub fn to_canonical_json(&self) -> Result<String> {
        self.validate()?;
        let json = serde_json::to_string(self)?;
        bounded(json.as_bytes())?;
        Ok(json)
    }

    pub(super) fn to_executor_init_json_with_bindings<'a>(
        &self,
        storage: impl IntoIterator<Item = (&'a str, bool)>,
        bindings: impl IntoIterator<Item = &'a WorkerBinding>,
    ) -> Result<String> {
        self.validate()?;
        let mut storage: Vec<_> = storage
            .into_iter()
            .map(|(name, readonly)| ExecutorStorageBinding {
                name,
                mode: if readonly { "ro" } else { "rw" },
            })
            .collect();
        storage.sort_by(|left, right| left.name.cmp(right.name));
        if storage.len() > MAX_EXECUTOR_STORAGE_BINDINGS {
            return Err(invalid("too many executor storage bindings"));
        }
        let mut previous = None;
        for binding in &storage {
            if binding.name.is_empty()
                || binding.name.len() > 64
                || !binding
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || previous.is_some_and(|name| name == binding.name)
            {
                return Err(invalid("invalid executor storage binding"));
            }
            previous = Some(binding.name);
        }
        let mut bindings: Vec<_> = bindings
            .into_iter()
            .map(|binding| ExecutorBinding {
                name: &binding.name,
                kind: binding.kind,
            })
            .collect();
        bindings.sort_by(|left, right| left.name.cmp(right.name));
        if bindings.len() > MAX_EXECUTOR_BINDINGS {
            return Err(invalid("too many executor bindings"));
        }
        let mut previous = None;
        for binding in &bindings {
            identifier(binding.name, 64)?;
            if previous.is_some_and(|name| name == binding.name) {
                return Err(invalid("duplicate executor binding"));
            }
            previous = Some(binding.name);
        }
        let protocol_version = if bindings.is_empty() {
            if storage.is_empty() {
                return Err(invalid("executor capabilities are empty"));
            }
            EXECUTOR_INIT_PROTOCOL_VERSION
        } else {
            EXECUTOR_BINDING_PROTOCOL_VERSION
        };
        let init = ExecutorInit {
            protocol_version,
            worker_version: &self.worker_version,
            compatibility_date: &self.compatibility_date,
            compatibility_flags: &self.compatibility_flags,
            main_module: &self.main_module,
            modules: &self.modules,
            storage: &storage,
            bindings: (!bindings.is_empty()).then_some(&bindings),
        };
        let json = serde_json::to_string(&init)?;
        bounded(json.as_bytes())?;
        Ok(json)
    }

    #[cfg(test)]
    pub(super) fn to_executor_init_json<'a>(
        &self,
        storage: impl IntoIterator<Item = (&'a str, bool)>,
    ) -> Result<String> {
        self.to_executor_init_json_with_bindings(storage, std::iter::empty())
    }

    pub fn sha256(&self) -> Result<String> {
        let digest = Sha256::digest(self.to_canonical_json()?.as_bytes());
        Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn canonicalize(&mut self) -> Result<()> {
        self.compatibility_flags.sort();
        if self
            .compatibility_flags
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(invalid("duplicate compatibility flag"));
        }
        let Some(main_index) = self
            .modules
            .iter()
            .position(|module| module.name == self.main_module)
        else {
            return Err(invalid("main module is missing"));
        };
        let main = self.modules.remove(main_index);
        self.modules
            .sort_by(|left, right| left.name.cmp(&right.name));
        if self
            .modules
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(invalid("duplicate module name"));
        }
        self.modules.insert(0, main);
        Ok(())
    }
}

impl ScheduledRequest {
    pub fn to_json(&self) -> Result<String> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(invalid("unsupported scheduled protocol version"));
        }
        identifier(&self.request_id, MAX_REQUEST_ID_BYTES)?;
        if self.cron.is_empty()
            || self.cron.len() > MAX_CRON_BYTES
            || self.cron.bytes().any(|byte| byte < b' ' || byte == 0x7f)
        {
            return Err(invalid("invalid scheduled cron"));
        }
        let json = serde_json::to_string(self)?;
        bounded(json.as_bytes())?;
        Ok(json)
    }
}

impl ScheduledResponse {
    pub fn from_json(input: &[u8]) -> Result<Self> {
        bounded(input)?;
        let response: Self = serde_json::from_slice(input)?;
        if response.protocol_version != PROTOCOL_VERSION {
            return Err(invalid("unsupported scheduled protocol version"));
        }
        identifier(&response.request_id, MAX_REQUEST_ID_BYTES)?;
        identifier(&response.outcome, 64)?;
        Ok(response)
    }
}

impl QueueRequest {
    pub fn to_json(&self) -> Result<String> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(invalid("unsupported queue protocol version"));
        }
        identifier(&self.request_id, MAX_REQUEST_ID_BYTES)?;
        identifier(&self.queue, MAX_QUEUE_NAME_BYTES)?;
        if self.messages.is_empty() || self.messages.len() > MAX_QUEUE_MESSAGES {
            return Err(invalid("invalid queue batch"));
        }
        let mut body_bytes = 0usize;
        let mut previous = None;
        for message in &self.messages {
            identifier(&message.id, MAX_REQUEST_ID_BYTES)?;
            if previous.is_some_and(|id: &String| id >= &message.id) {
                return Err(invalid("queue message IDs are not unique and sorted"));
            }
            previous = Some(&message.id);
            body_bytes = body_bytes
                .checked_add(
                    STANDARD
                        .decode(&message.body_base64)
                        .map_err(|_| invalid("invalid queue message body"))?
                        .len(),
                )
                .ok_or_else(|| invalid("queue batch body exceeds limit"))?;
            if body_bytes > MAX_BODY_BYTES {
                return Err(invalid("queue batch body exceeds limit"));
            }
            if message
                .content_type
                .as_deref()
                .is_some_and(|value| !matches!(value, "text" | "bytes" | "json" | "v8"))
            {
                return Err(invalid("invalid queue message content type"));
            }
        }
        if !self.metadata.backlog_count.is_finite()
            || self.metadata.backlog_count < 0.0
            || !self.metadata.backlog_bytes.is_finite()
            || self.metadata.backlog_bytes < 0.0
        {
            return Err(invalid("invalid queue metadata"));
        }
        let json = serde_json::to_string(self)?;
        bounded(json.as_bytes())?;
        Ok(json)
    }
}

impl QueueResponse {
    pub fn from_json(input: &[u8]) -> Result<Self> {
        bounded(input)?;
        let response: Self = serde_json::from_slice(input)?;
        if response.protocol_version != PROTOCOL_VERSION {
            return Err(invalid("unsupported queue protocol version"));
        }
        identifier(&response.request_id, MAX_REQUEST_ID_BYTES)?;
        identifier(&response.outcome, 64)?;
        Ok(response)
    }
}

fn validate_module_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 256
        || name.starts_with('/')
        || name.ends_with('/')
        || name.contains('\\')
        || name.contains("//")
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._/-".contains(&b))
        || name
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid("invalid module name"));
    }
    Ok(())
}

fn validate_date(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(invalid("invalid compatibility date"));
    }
    let parse = |range: std::ops::Range<usize>| -> Option<u32> {
        bytes[range].iter().try_fold(0, |value, byte| {
            byte.is_ascii_digit()
                .then_some(value * 10 + u32::from(byte - b'0'))
        })
    };
    let Some(year) = parse(0..4) else {
        return Err(invalid("invalid compatibility date"));
    };
    let Some(month) = parse(5..7) else {
        return Err(invalid("invalid compatibility date"));
    };
    let Some(day) = parse(8..10) else {
        return Err(invalid("invalid compatibility date"));
    };
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0 || day == 0 || day > days {
        return Err(invalid("invalid compatibility date"));
    }
    Ok(())
}

impl RequestEnvelope {
    pub fn validate(&self) -> Result<()> {
        common(
            self.protocol_version,
            &self.request_id,
            &self.headers,
            &self.body_base64,
        )?;
        if self.method.is_empty()
            || self.method.len() > 32
            || !self
                .method
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b == b'-')
        {
            return Err(invalid("invalid method"));
        }
        let authority = self
            .url
            .strip_prefix("https://")
            .or_else(|| self.url.strip_prefix("http://"));
        if self.url.len() > 8 * 1024
            || self.url.bytes().any(|b| b.is_ascii_control() || b == b' ')
            || authority.is_none_or(|s| s.split('/').next().unwrap_or("").is_empty())
        {
            return Err(invalid("invalid HTTP(S) URL"));
        }
        Ok(())
    }

    pub fn from_json(input: &[u8]) -> Result<Self> {
        bounded(input)?;
        let value: Self = serde_json::from_slice(input)?;
        value.validate()?;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        let json = serde_json::to_string(self)?;
        bounded(json.as_bytes())?;
        Ok(json)
    }
}

impl ResponseEnvelope {
    pub fn validate(&self) -> Result<()> {
        common(
            self.protocol_version,
            &self.request_id,
            &self.headers,
            &self.body_base64,
        )?;
        if !(100..=599).contains(&self.status) {
            return Err(invalid("invalid HTTP status"));
        }
        Ok(())
    }

    pub fn from_json(input: &[u8]) -> Result<Self> {
        bounded(input)?;
        let value: Self = serde_json::from_slice(input)?;
        value.validate()?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperlight_common::flatbuffer_wrappers::{
        function_call::{FunctionCall, FunctionCallType},
        function_types::{ParameterValue, ReturnType},
    };

    fn request() -> RequestEnvelope {
        RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: "r-1".into(),
            method: "POST".into(),
            url: "https://example.test/".into(),
            headers: vec![],
            body_base64: String::new(),
        }
    }

    fn bundle() -> WorkerBundle {
        WorkerBundle {
            protocol_version: PROTOCOL_VERSION,
            worker_version: WorkerVersionId::new("v1").unwrap(),
            compatibility_date: "2025-01-01".into(),
            compatibility_flags: vec!["nodejs_compat".into()],
            main_module: "worker.js".into(),
            modules: vec![WorkerModule {
                name: "worker.js".into(),
                module_type: ModuleType::EsModule,
                source: "export default { fetch() { return new Response('ok') } }".into(),
            }],
        }
    }

    #[test]
    fn scheduled_and_queue_envelopes_are_canonical_and_bounded() {
        let scheduled = ScheduledRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "scheduled-1".into(),
            scheduled_time_unix_ms: 1_767_225_600_000,
            cron: "0 0 * * *".into(),
        };
        assert_eq!(
            scheduled.to_json().unwrap(),
            r#"{"protocol_version":1,"request_id":"scheduled-1","scheduled_time_unix_ms":1767225600000,"cron":"0 0 * * *"}"#
        );
        assert_eq!(
                ScheduledResponse::from_json(
                    br#"{"protocol_version":1,"request_id":"scheduled-1","outcome":"ok","retry":false}"#
                )
                .unwrap(),
                ScheduledResponse {
                    protocol_version: PROTOCOL_VERSION,
                    request_id: "scheduled-1".into(),
                    outcome: "ok".into(),
                    retry: false,
                }
            );

        let queue = QueueRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "queue-1".into(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "message-1".into(),
                timestamp_unix_ms: 1_767_225_600_000,
                body_base64: "aGVsbG8=".into(),
                content_type: Some("text".into()),
                attempts: 2,
            }],
            metadata: QueueMetadata {
                backlog_count: 3.0,
                backlog_bytes: 5.0,
                oldest_message_timestamp_unix_ms: Some(1_767_225_500_000),
            },
        };
        assert_eq!(
            queue.to_json().unwrap(),
            r#"{"protocol_version":1,"request_id":"queue-1","queue":"jobs","messages":[{"id":"message-1","timestamp_unix_ms":1767225600000,"body_base64":"aGVsbG8=","content_type":"text","attempts":2}],"metadata":{"backlog_count":3.0,"backlog_bytes":5.0,"oldest_message_timestamp_unix_ms":1767225500000}}"#
        );
    }

    #[test]
    fn executor_binding_manifest_uses_protocol_three() {
        let binding = WorkerBinding {
            name: "settings".into(),
            kind: WorkerBindingKind::Kv,
        };
        let json = bundle()
            .to_executor_init_json_with_bindings(std::iter::empty(), [&binding])
            .unwrap();

        assert_eq!(
            json,
            r#"{"protocol_version":3,"worker_version":"v1","compatibility_date":"2025-01-01","compatibility_flags":["nodejs_compat"],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default { fetch() { return new Response('ok') } }"}],"storage":[],"bindings":[{"name":"settings","kind":"kv"}]}"#
        );
    }

    #[test]
    fn malformed_unknown_duplicate_and_oversized_json() {
        for json in [
            b"{".as_slice(),
            br#"{"protocol_version":1,"protocol_version":1}"#,
        ] {
            assert!(RequestEnvelope::from_json(json).is_err());
        }
        let mut v = serde_json::to_value(request()).unwrap();
        v["unknown"] = true.into();
        assert!(RequestEnvelope::from_json(&serde_json::to_vec(&v).unwrap()).is_err());
        assert!(ResponseEnvelope::from_json(&vec![b' '; MAX_ENVELOPE_BYTES + 1]).is_err());
        let mut r = request();
        r.protocol_version += 1;
        assert!(r.to_json().is_err());
        r = request();
        r.body_base64 = "!!".into();
        assert!(r.to_json().is_err());
        assert!(serde_json::from_str::<WorkerVersionId>("\"bad version\"").is_err());
    }

    #[test]
    fn exact_body_limit_and_transport_framing() {
        let mut r = request();
        r.body_base64 = STANDARD.encode(vec![0; MAX_BODY_BYTES]);
        let json = r.to_json().unwrap();
        RequestEnvelope::from_json(json.as_bytes()).unwrap();
        r.body_base64 = STANDARD.encode(vec![0; MAX_BODY_BYTES + 1]);
        assert!(r.to_json().is_err());

        // Measure the actual Hyperlight framing, not just nominal JSON/body sizes.
        for (name, kind, ret) in [
            ("fetch", FunctionCallType::Guest, ReturnType::Void),
            ("HostPrint", FunctionCallType::Host, ReturnType::Int),
        ] {
            let call = FunctionCall::new(
                name.into(),
                Some(vec![ParameterValue::String(" ".repeat(MAX_ENVELOPE_BYTES))]),
                kind,
                ret,
            );
            let mut builder = flatbuffers::FlatBufferBuilder::new();
            let bytes = call.encode(&mut builder);
            assert!(bytes.len() <= crate::HOST_CALL_MAX, "{}", bytes.len());
        }
    }

    #[test]
    fn escaped_json_is_bounded_after_serialization() {
        let mut r = request();
        r.body_base64 = STANDARD.encode(vec![0; MAX_BODY_BYTES]);
        r.headers = vec![Header {
            name: "x".into(),
            value: "\\".repeat(MAX_HEADER_BYTES - 1),
        }];
        r.url = format!("https://example.test/{}", "\\".repeat(8000));
        assert!(r.validate().is_ok());
        assert!(r.to_json().is_err());
        r = request();
        r.headers = vec![Header {
            name: "x".into(),
            value: "a\r\nb".into(),
        }];
        assert!(r.validate().is_err());
        r.headers = vec![
            Header {
                name: "x".into(),
                value: "".into()
            };
            MAX_HEADERS + 1
        ];
        assert!(r.validate().is_err());
    }

    #[test]
    fn bundle_is_canonical_bounded_and_content_addressed() {
        let mut input = bundle();
        input.compatibility_flags = vec!["z".into(), "a".into()];
        input.modules.push(WorkerModule {
            name: "dependency.cjs".into(),
            module_type: ModuleType::CommonJsModule,
            source: "module.exports = { value: 1 };".into(),
        });
        input.modules.push(WorkerModule {
            name: "data.txt".into(),
            module_type: ModuleType::Text,
            source: "hello".into(),
        });
        let pretty = serde_json::to_vec_pretty(&input).unwrap();
        let canonical = WorkerBundle::from_json(&pretty).unwrap();
        assert_eq!(canonical.compatibility_flags, ["a", "z"]);
        assert_eq!(canonical.modules[0].name, "worker.js");
        assert_eq!(canonical.modules[1].name, "data.txt");
        assert_eq!(canonical.modules[2].name, "dependency.cjs");
        assert_eq!(canonical.modules[2].module_type, ModuleType::CommonJsModule);
        assert_eq!(
            WorkerBundle::from_json(canonical.to_canonical_json().unwrap().as_bytes()).unwrap(),
            canonical
        );
        assert_eq!(canonical.sha256().unwrap().len(), 64);
        let mut changed = canonical.clone();
        changed.modules[0].source.push('!');
        assert_ne!(canonical.sha256().unwrap(), changed.sha256().unwrap());

        let mut invalid = bundle();
        invalid.compatibility_date = "2025-02-29".into();
        assert!(invalid.validate().is_err());
        invalid = bundle();
        invalid.modules[0].name = "../worker.js".into();
        assert!(invalid.validate().is_err());
        invalid = bundle();
        invalid.modules[0].source = "x".repeat(MAX_MODULE_SOURCE_BYTES + 1);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn executor_storage_init_is_v2_sorted_and_bounded() {
        let init = bundle()
            .to_executor_init_json([("rw", false), ("ro", true)])
            .unwrap();
        assert_eq!(
            init,
            concat!(
                "{\"protocol_version\":2,\"worker_version\":\"v1\",",
                "\"compatibility_date\":\"2025-01-01\",",
                "\"compatibility_flags\":[\"nodejs_compat\"],",
                "\"main_module\":\"worker.js\",\"modules\":[",
                "{\"name\":\"worker.js\",\"type\":\"esModule\",",
                "\"source\":\"export default { fetch() { return new Response('ok') } }\"}],",
                "\"storage\":[{\"name\":\"ro\",\"mode\":\"ro\"},",
                "{\"name\":\"rw\",\"mode\":\"rw\"}]}"
            )
        );
        assert!(
            bundle()
                .to_executor_init_json(std::iter::empty::<(&str, bool)>())
                .is_err()
        );
        assert!(
            bundle()
                .to_executor_init_json([("bad_name", true)])
                .is_err()
        );
        assert!(
            bundle()
                .to_executor_init_json([
                    ("m0", true),
                    ("m1", true),
                    ("m2", true),
                    ("m3", true),
                    ("m4", true),
                    ("m5", true),
                    ("m6", true),
                    ("m7", true),
                    ("m8", true),
                ])
                .is_err()
        );
    }

    #[test]
    fn bundle_fits_hyperlight_transport() {
        let mut maximum = bundle();
        maximum.modules[0].source = "x".repeat(MAX_MODULE_SOURCE_BYTES);
        maximum.modules.push(WorkerModule {
            name: "data.txt".into(),
            module_type: ModuleType::Text,
            source: "x".repeat(MAX_BUNDLE_SOURCE_BYTES - MAX_MODULE_SOURCE_BYTES),
        });
        maximum.canonicalize().unwrap();
        let bundle = maximum.to_canonical_json().unwrap();
        let call = FunctionCall::new(
            "init".into(),
            Some(vec![ParameterValue::String(bundle)]),
            FunctionCallType::Guest,
            ReturnType::Void,
        );
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let encoded = call.encode(&mut builder);
        assert!(encoded.len() <= crate::HOST_CALL_MAX, "{}", encoded.len());

        maximum.modules[0].source = "\\".repeat(MAX_MODULE_SOURCE_BYTES);
        maximum.modules[1].source = "\\".repeat(MAX_BUNDLE_SOURCE_BYTES - MAX_MODULE_SOURCE_BYTES);
        assert!(maximum.to_canonical_json().is_err());
    }

    #[test]
    fn checked_in_bundles_are_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/workerd-bundles");
        for name in [
            "acceptance.json",
            "api-smoke.json",
            "helloworld_esm.json",
            "web-streams.json",
            "workerd-vfs-evidence.json",
        ] {
            let bundle = WorkerBundle::from_path(root.join(name)).unwrap();
            bundle.to_canonical_json().unwrap();
        }
    }
}
