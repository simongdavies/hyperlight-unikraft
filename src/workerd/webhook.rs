// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, MAX_BODY_BYTES, Result};
use crate::broker::RequestIdentity;
use crate::broker_adapter::BrokerHostError;
use crate::broker_runtime::{
    LogicalInvocation, LogicalRuntimeStatus, LogicalWireError, LogicalWireService,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookPolicy {
    pub name: String,
    pub secret_reference: String,
    pub value_file: PathBuf,
    pub tolerance_secs: u64,
}

impl WebhookPolicy {
    pub(super) fn validate(&self) -> Result<()> {
        LogicalInvocation::new(&self.name).map_err(|error| Error::State(error.to_string()))?;
        if self.name.len() > 64
            || !self.name.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || (index > 0 && (byte.is_ascii_digit() || b"_-".contains(&byte)))
            })
            || self.secret_reference.is_empty()
            || self.secret_reference.len() > 128
            || !self
                .secret_reference
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            || !self.value_file.is_absolute()
            || self.tolerance_secs == 0
            || self.tolerance_secs > 300
        {
            return Err(Error::State(
                "invalid bounded webhook secret reference or replay tolerance".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u16,
    request_id: String,
    binding: String,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    WebhookVerify {
        body_base64: String,
        signature: String,
    },
}

#[derive(Serialize)]
struct Response<'a> {
    version: u16,
    request_id: &'a str,
    status: &'a str,
    code: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<serde_json::Value>,
}

pub(super) struct WebhookService {
    policy: WebhookPolicy,
    identity: RequestIdentity,
}

impl WebhookService {
    pub(super) fn new(policy: WebhookPolicy, identity: RequestIdentity) -> Result<Self> {
        policy.validate()?;
        Ok(Self { policy, identity })
    }

    fn parse(bytes: &[u8]) -> std::result::Result<Request, LogicalWireError> {
        if bytes.len() > super::MAX_ENVELOPE_BYTES {
            return Err(LogicalWireError::Malformed);
        }
        let request: Request =
            serde_json::from_slice(bytes).map_err(|_| LogicalWireError::Malformed)?;
        if request.version != 1 {
            return Err(LogicalWireError::UnsupportedVersion);
        }
        super::ControlRequest::new(&request.request_id).map_err(|_| LogicalWireError::Malformed)?;
        Ok(request)
    }

    fn verify(&self, raw: &[u8], signature: &str) -> Result<serde_json::Value> {
        let mut key_bytes = super::secret::read_private_secret(&self.policy.value_file)?;
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &key_bytes);
        key_bytes.fill(0);
        let mut timestamp = None;
        let mut digests = Vec::new();
        for field in signature.split(',') {
            let Some((name, value)) = field.trim().split_once('=') else {
                return Ok(serde_json::json!({"valid":false}));
            };
            match name {
                "t" => {
                    if timestamp.is_some()
                        || value.is_empty()
                        || !value.bytes().all(|byte| byte.is_ascii_digit())
                    {
                        return Ok(serde_json::json!({"valid":false}));
                    }
                    timestamp = value.parse::<u64>().ok();
                    if timestamp.is_none() {
                        return Ok(serde_json::json!({"valid":false}));
                    }
                }
                "v1" => {
                    if digests.len() >= 16 || value.len() != 64 {
                        return Ok(serde_json::json!({"valid":false}));
                    }
                    let mut digest = [0; 32];
                    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
                        let digit = |byte: u8| match byte {
                            b'0'..=b'9' => Some(byte - b'0'),
                            b'a'..=b'f' => Some(byte - b'a' + 10),
                            b'A'..=b'F' => Some(byte - b'A' + 10),
                            _ => None,
                        };
                        let (Some(high), Some(low)) = (digit(pair[0]), digit(pair[1])) else {
                            return Ok(serde_json::json!({"valid":false}));
                        };
                        digest[index] = high * 16 + low;
                    }
                    digests.push(digest);
                }
                _ => {}
            }
        }
        let Some(timestamp) = timestamp else {
            return Ok(serde_json::json!({"valid":false}));
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::State("trusted webhook clock unavailable".into()))?
            .as_secs();
        if now.abs_diff(timestamp) > self.policy.tolerance_secs {
            return Ok(serde_json::json!({"valid":false}));
        }
        let mut signed = format!("{timestamp}.").into_bytes();
        signed.extend_from_slice(raw);
        if !digests
            .iter()
            .any(|digest| ring::hmac::verify(&key, &signed, digest).is_ok())
        {
            return Ok(serde_json::json!({"valid":false}));
        }
        let event: serde_json::Value = serde_json::from_slice(raw)
            .map_err(|_| Error::Protocol("authenticated webhook body is not valid JSON".into()))?;
        Ok(serde_json::json!({"valid":true,"event":event}))
    }

    fn response(id: &str, status: &str, code: &str, value: Option<serde_json::Value>) -> Vec<u8> {
        serde_json::to_vec(&Response {
            version: 1,
            request_id: id,
            status,
            code,
            value,
        })
        .expect("typed webhook response serializes")
    }
}

impl LogicalWireService for WebhookService {
    fn inspect(&self, payload: &[u8]) -> std::result::Result<LogicalInvocation, LogicalWireError> {
        let request = Self::parse(payload)?;
        LogicalInvocation::new(request.binding)
    }
    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        let request = match Self::parse(payload) {
            Ok(request) => request,
            Err(_) => return Self::response("", "invalid_request", "invalid_envelope", None),
        };
        if identity != &self.identity || request.binding != self.policy.name {
            return Self::response(
                &request.request_id,
                "denied",
                "binding_authority_mismatch",
                None,
            );
        }
        let Operation::WebhookVerify {
            body_base64,
            signature,
        } = request.operation;
        if body_base64.len() > MAX_BODY_BYTES.div_ceil(3) * 4
            || signature.is_empty()
            || signature.len() > 1024
        {
            return Self::response(
                &request.request_id,
                "invalid_request",
                "webhook_limits",
                None,
            );
        }
        let raw = match STANDARD.decode(&body_base64) {
            Ok(raw) if raw.len() <= MAX_BODY_BYTES => raw,
            _ => {
                return Self::response(
                    &request.request_id,
                    "invalid_request",
                    "webhook_body",
                    None,
                );
            }
        };
        match self.verify(&raw, &signature) {
            Ok(value) => Self::response(&request.request_id, "ok", "verified", Some(value)),
            Err(Error::Protocol(_)) => Self::response(
                &request.request_id,
                "invalid_request",
                "invalid_event",
                None,
            ),
            Err(_) => Self::response(
                &request.request_id,
                "host_error",
                "secret_reference_unavailable",
                None,
            ),
        }
    }
    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
        let status = match status {
            LogicalRuntimeStatus::Denied => "denied",
            LogicalRuntimeStatus::InvalidRequest => "invalid_request",
            LogicalRuntimeStatus::HostError => "host_error",
        };
        Self::response("", status, code, None)
    }
    fn reset_for_fresh_vm(&mut self) -> std::result::Result<(), BrokerHostError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn host_only_verification_preserves_raw_body_rejects_replay_spoofing_and_missing_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("secret");
        std::fs::write(&file, b"synthetic-webhook-secret").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let identity = RequestIdentity::new("tenant-app", "revision", 0).unwrap();
        let mut service = WebhookService::new(
            WebhookPolicy {
                name: "webhook".into(),
                secret_reference: "stripe-hook".into(),
                value_file: file.clone(),
                tolerance_secs: 300,
            },
            identity.clone(),
        )
        .unwrap();
        let raw = br#"{"id":"evt-1","data":{}}"#;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut signed = format!("{now}.").into_bytes();
        signed.extend_from_slice(raw);
        let digest = ring::hmac::sign(
            &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"synthetic-webhook-secret"),
            &signed,
        );
        let signature = format!(
            "t={now},v1={}",
            digest
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let request = serde_json::json!({"version":1,"request_id":"verify-1","binding":"webhook",
            "operation":{"kind":"webhook_verify","body_base64":STANDARD.encode(raw),"signature":signature}});
        let response: serde_json::Value =
            serde_json::from_slice(&service.dispatch(&identity, request.to_string().as_bytes()))
                .unwrap();
        assert_eq!(response["value"]["valid"], true);
        assert_eq!(response["value"]["event"]["id"], "evt-1");
        assert!(!response.to_string().contains("synthetic-webhook-secret"));
        let wrong = RequestIdentity::new("other-app", "revision", 0).unwrap();
        let response: serde_json::Value =
            serde_json::from_slice(&service.dispatch(&wrong, request.to_string().as_bytes()))
                .unwrap();
        assert_eq!(response["status"], "denied");
        assert_eq!(service.verify(b"{ }", &signature).unwrap()["valid"], false);
        assert_eq!(
            service
                .verify(
                    raw,
                    "t=1,v1=0000000000000000000000000000000000000000000000000000000000000000"
                )
                .unwrap()["valid"],
            false
        );
        std::fs::remove_file(file).unwrap();
        let response: serde_json::Value =
            serde_json::from_slice(&service.dispatch(&identity, request.to_string().as_bytes()))
                .unwrap();
        assert_eq!(response["status"], "host_error");
        assert!(!response.to_string().contains("synthetic-webhook-secret"));
    }
}
