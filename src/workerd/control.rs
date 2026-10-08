// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{
    Error, Header, InvocationCancellation, MAX_ENVELOPE_BYTES, MAX_HEADER_BYTES, MAX_HEADERS,
    RequestEnvelope, Result,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

pub const MAX_FRAME_BYTES: usize = 16 * 1024;

fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    {
        return Err(Error::Protocol("invalid control request ID".into()));
    }
    Ok(())
}

pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(Error::Protocol(
            "control envelope exceeds size limit".into(),
        ));
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub protocol_version: u16,
    pub request_id: String,
}

impl ControlRequest {
    pub fn new(request_id: impl Into<String>) -> Result<Self> {
        let request_id = request_id.into();
        validate_id(&request_id)?;
        Ok(Self {
            protocol_version: 1,
            request_id,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleRequest {
    pub protocol_version: u16,
    pub request_id: String,
    pub app_id: String,
    pub revision: super::WorkerVersionId,
    pub expected_generation: u64,
    pub checkpoint_policy: super::CheckpointPolicy,
}

impl LifecycleRequest {
    pub fn validate(&self, app: &super::AppHandle) -> Result<()> {
        validate_id(&self.request_id)?;
        if self.protocol_version != 1
            || self.app_id != app.app_id()
            || self.revision != app.identity().worker_version
        {
            return Err(Error::Protocol(
                "lifecycle app or immutable revision authority mismatch".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceInvocation {
    pub protocol_version: u16,
    pub request_id: String,
    pub app_id: String,
    pub revision: super::WorkerVersionId,
    pub instance_id: String,
    pub expected_generation: u64,
    pub lifetime_budget_ms: u32,
    pub invocation: super::InvocationRequest,
}

impl InstanceInvocation {
    pub fn validate(&self, app: &super::AppHandle, route_instance: &str) -> Result<()> {
        validate_id(&self.request_id)?;
        validate_id(&self.instance_id)?;
        if self.protocol_version != 1
            || self.app_id != app.app_id()
            || self.revision != app.identity().worker_version
            || self.instance_id != route_instance
            || self.request_id != self.invocation.request_id()
            || self.expected_generation == 0
        {
            return Err(Error::Protocol(
                "instance invocation authority, route, correlation or generation mismatch".into(),
            ));
        }
        if self.lifetime_budget_ms == 0 {
            return Err(Error::Timeout);
        }
        self.invocation.encode()?;
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestCapabilities {
    pub protocol_version: u16,
    pub request_id: String,
    pub extensions: Vec<String>,
    pub max_frame_bytes: usize,
}

impl GuestCapabilities {
    pub fn validate(&self, expected_id: &str, required: &str) -> Result<()> {
        validate_id(&self.request_id)?;
        let unique: std::collections::HashSet<_> = self.extensions.iter().collect();
        if self.protocol_version != 1
            || self.request_id != expected_id
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > MAX_FRAME_BYTES
            || self.extensions.len() > 32
            || unique.len() != self.extensions.len()
            || self.extensions.iter().any(|extension| extension.len() > 64)
            || !self
                .extensions
                .iter()
                .any(|extension| extension == required)
        {
            return Err(Error::Protocol(
                "guest capability negotiation failed".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafePoint {
    pub protocol_version: u16,
    pub request_id: String,
    pub quiescent: bool,
    pub active_invocations: u64,
    pub active_streams: u64,
    pub active_websockets: u64,
    pub active_tasks: u64,
}

impl SafePoint {
    pub fn validate(&self, expected_id: &str) -> Result<()> {
        validate_id(&self.request_id)?;
        if self.protocol_version != 1
            || self.request_id != expected_id
            || !self.quiescent
            || [
                self.active_invocations,
                self.active_streams,
                self.active_websockets,
                self.active_tasks,
            ]
            .iter()
            .any(|count| *count != 0)
        {
            return Err(Error::State(
                "guest has not reached a checkpoint safe point".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    Headers,
    Data,
    Websocket,
    End,
    Error,
    Cancel,
    Pending,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamFrame {
    pub protocol_version: u16,
    pub request_id: String,
    pub sequence: u64,
    pub kind: FrameKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Vec<Header>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opcode: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl StreamFrame {
    pub fn decoded_body(&self) -> Result<Vec<u8>> {
        let Some(body) = &self.body_base64 else {
            return Ok(Vec::new());
        };
        if body.len() > MAX_FRAME_BYTES.div_ceil(3) * 4 {
            return Err(Error::Protocol(
                "stream frame exceeds decoded size limit".into(),
            ));
        }
        let bytes = STANDARD
            .decode(body)
            .map_err(|error| Error::Protocol(error.to_string()))?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(Error::Protocol(
                "stream frame exceeds decoded size limit".into(),
            ));
        }
        Ok(bytes)
    }

    pub(super) fn validate(&self, expected_id: &str) -> Result<()> {
        validate_id(&self.request_id)?;
        if self.protocol_version != 1 || self.request_id != expected_id {
            return Err(Error::Protocol(
                "stream protocol or request ID mismatch".into(),
            ));
        }
        let bytes = self.decoded_body()?;
        match self.kind {
            FrameKind::Headers => {
                let status = self
                    .status
                    .ok_or_else(|| Error::Protocol("headers frame has no status".into()))?;
                let headers = self
                    .headers
                    .as_deref()
                    .ok_or_else(|| Error::Protocol("headers frame has no headers".into()))?;
                if !(100..=599).contains(&status)
                    || self.body_base64.is_some()
                    || self.opcode.is_some()
                    || self.error.is_some()
                    || headers.len() > MAX_HEADERS
                    || headers
                        .iter()
                        .map(|header| header.name.len() + header.value.len())
                        .sum::<usize>()
                        > MAX_HEADER_BYTES
                {
                    return Err(Error::Protocol("invalid stream headers frame".into()));
                }
                let request = RequestEnvelope {
                    protocol_version: 1,
                    request_id: expected_id.into(),
                    method: "GET".into(),
                    url: "https://example.test/".into(),
                    headers: headers.to_vec(),
                    body_base64: String::new(),
                };
                request.validate()?;
            }
            FrameKind::Data | FrameKind::Websocket => {
                if self.body_base64.is_none()
                    || self.status.is_some()
                    || self.headers.is_some()
                    || self.error.is_some()
                {
                    return Err(Error::Protocol("invalid stream payload frame".into()));
                }
                if self.kind == FrameKind::Data && self.opcode.is_some() {
                    return Err(Error::Protocol(
                        "HTTP data frame has WebSocket opcode".into(),
                    ));
                }
                if self.kind == FrameKind::Websocket {
                    match self.opcode {
                        Some(1) => {
                            std::str::from_utf8(&bytes)
                                .map_err(|error| Error::Protocol(error.to_string()))?;
                        }
                        Some(2) => {}
                        Some(8) => {
                            if bytes.len() < 2 || bytes.len() > 125 {
                                return Err(Error::Protocol(
                                    "invalid WebSocket close payload".into(),
                                ));
                            }
                            let code = u16::from_be_bytes([bytes[0], bytes[1]]);
                            if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) {
                                return Err(Error::Protocol("invalid WebSocket close code".into()));
                            }
                            std::str::from_utf8(&bytes[2..])
                                .map_err(|error| Error::Protocol(error.to_string()))?;
                        }
                        _ => return Err(Error::Protocol("unsupported WebSocket opcode".into())),
                    }
                }
            }
            FrameKind::Error => {
                if self
                    .error
                    .as_ref()
                    .is_none_or(|error| error.is_empty() || error.len() > 1024)
                    || self.status.is_some()
                    || self.headers.is_some()
                    || self.body_base64.is_some()
                    || self.opcode.is_some()
                {
                    return Err(Error::Protocol("invalid stream error frame".into()));
                }
            }
            FrameKind::End | FrameKind::Cancel | FrameKind::Pending => {
                if self.status.is_some()
                    || self.headers.is_some()
                    || self.body_base64.is_some()
                    || self.opcode.is_some()
                    || self.error.is_some()
                {
                    return Err(Error::Protocol("invalid stream terminal frame".into()));
                }
            }
        }
        Ok(())
    }
}

/// State validation is separate from body decoding so malformed frames never
/// consume a sequence number or change the transport state.
#[derive(Clone, Debug)]
pub struct StreamState {
    request_id: String,
    next_sequence: u64,
    headers: bool,
    websocket: bool,
    websocket_allowed: bool,
    ended: bool,
    failure: Option<String>,
    cancelled: InvocationCancellation,
}

impl StreamState {
    pub fn new(
        request_id: impl Into<String>,
        websocket_allowed: bool,
        cancelled: InvocationCancellation,
    ) -> Result<Self> {
        let request_id = request_id.into();
        validate_id(&request_id)?;
        Ok(Self {
            request_id,
            next_sequence: 0,
            headers: false,
            websocket: false,
            websocket_allowed,
            ended: false,
            failure: None,
            cancelled,
        })
    }

    pub fn accept(&mut self, frame: &StreamFrame) -> Result<()> {
        if self.cancelled.is_cancelled() {
            return Err(Error::Cancelled);
        }
        frame.validate(&self.request_id)?;
        if self.ended || frame.sequence != self.next_sequence {
            return Err(Error::Protocol(
                "stream frame out of order or after completion".into(),
            ));
        }
        match frame.kind {
            FrameKind::Headers if !self.headers => {
                let websocket = frame.status == Some(101);
                if websocket && !self.websocket_allowed {
                    return Err(Error::Protocol("unsolicited WebSocket upgrade".into()));
                }
                self.headers = true;
                self.websocket = websocket;
            }
            FrameKind::Data if self.headers && !self.websocket => {}
            FrameKind::Websocket if self.headers && self.websocket => {}
            FrameKind::End if self.headers => self.ended = true,
            FrameKind::Error => {
                self.ended = true;
                self.failure = frame.error.clone();
            }
            FrameKind::Cancel | FrameKind::Pending => {
                return Err(Error::Protocol(
                    "guest cannot send cancellation/pending frame".into(),
                ));
            }
            _ => return Err(Error::Protocol("invalid stream frame transition".into())),
        }
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("stream sequence exhausted".into()))?;
        Ok(())
    }

    pub fn finish(&self, tracked_work_drained: bool) -> Result<()> {
        if self.cancelled.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if let Some(failure) = &self.failure {
            return Err(Error::State(format!(
                "guest streaming invocation failed: {failure}"
            )));
        }
        if !self.ended || !tracked_work_drained {
            return Err(Error::State(
                "stream ended without drained invocation completion".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_point_rejects_nonzero_work_wrong_id_and_false_quiescence() {
        let json = br#"{"protocol_version":1,"request_id":"park-1","quiescent":true,"active_invocations":0,"active_streams":0,"active_websockets":0,"active_tasks":0}"#;
        let safe: SafePoint = decode(json).unwrap();
        safe.validate("park-1").unwrap();
        safe.validate("park-2").unwrap_err();
        let mut unsafe_point = safe.clone();
        unsafe_point.active_tasks = 1;
        unsafe_point.validate("park-1").unwrap_err();
        unsafe_point.active_tasks = 0;
        unsafe_point.quiescent = false;
        unsafe_point.validate("park-1").unwrap_err();
    }

    #[test]
    fn frames_are_bounded_ordered_and_end_is_not_drained_completion() {
        let mut stream =
            StreamState::new("stream-1", false, InvocationCancellation::default()).unwrap();
        let mut frame: StreamFrame = decode(br#"{"protocol_version":1,"request_id":"stream-1","sequence":0,"kind":"headers","status":200,"headers":[]}"#).unwrap();
        stream.accept(&frame).unwrap();
        stream.accept(&frame).unwrap_err();
        frame = decode(br#"{"protocol_version":1,"request_id":"stream-1","sequence":1,"kind":"data","body_base64":"aGk="}"#).unwrap();
        let mut wrong_id = frame.clone();
        wrong_id.request_id = "stream-2".into();
        stream.accept(&wrong_id).unwrap_err();
        let mut oversized = frame.clone();
        oversized.body_base64 = Some(STANDARD.encode(vec![0; MAX_FRAME_BYTES + 1]));
        stream.accept(&oversized).unwrap_err();
        stream.accept(&frame).unwrap();
        frame =
            decode(br#"{"protocol_version":1,"request_id":"stream-1","sequence":2,"kind":"end"}"#)
                .unwrap();
        stream.accept(&frame).unwrap();
        stream.finish(false).unwrap_err();
        stream.finish(true).unwrap();
        stream.accept(&frame).unwrap_err();
    }

    #[test]
    fn error_frame_never_becomes_successful_completion() {
        let mut stream =
            StreamState::new("stream-1", false, InvocationCancellation::default()).unwrap();
        let error: StreamFrame = decode(br#"{"protocol_version":1,"request_id":"stream-1","sequence":0,"kind":"error","error":"handler failed"}"#).unwrap();
        stream.accept(&error).unwrap();
        assert!(stream.finish(true).is_err());
    }
}
