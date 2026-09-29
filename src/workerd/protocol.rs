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
pub const MAX_MODULE_SOURCE_BYTES: usize = 32 * 1024;
pub const MAX_MODULES: usize = 32;
pub const MAX_COMPATIBILITY_FLAGS: usize = 32;
pub const MAX_HEADERS: usize = 64;
pub const MAX_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_REQUEST_ID_BYTES: usize = 64;

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
            if module.source.len() > MAX_MODULE_SOURCE_BYTES {
                return Err(invalid("module source exceeds size limit"));
            }
            source_bytes += module.source.len();
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
            name: "data.txt".into(),
            module_type: ModuleType::Text,
            source: "hello".into(),
        });
        let pretty = serde_json::to_vec_pretty(&input).unwrap();
        let canonical = WorkerBundle::from_json(&pretty).unwrap();
        assert_eq!(canonical.compatibility_flags, ["a", "z"]);
        assert_eq!(canonical.modules[0].name, "worker.js");
        assert_eq!(canonical.modules[1].name, "data.txt");
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
        ] {
            let bundle = WorkerBundle::from_path(root.join(name)).unwrap();
            bundle.to_canonical_json().unwrap();
        }
    }
}
