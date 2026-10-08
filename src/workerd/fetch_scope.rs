// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureChatPolicy {
    pub max_messages: usize,
    pub max_prompt_bytes: usize,
    pub max_tokens: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FetchBodyPolicy {
    AzureChat(AzureChatPolicy),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatRequest {
    messages: Vec<ChatMessage>,
    max_tokens: u32,
    stream: bool,
    temperature: Option<f64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatMessage {
    role: String,
    content: String,
}

impl FetchBodyPolicy {
    pub(super) fn validate(&self) -> Result<()> {
        let Self::AzureChat(policy) = self;
        if policy.max_messages == 0
            || policy.max_messages > 128
            || policy.max_prompt_bytes == 0
            || policy.max_prompt_bytes > 1024 * 1024
            || policy.max_tokens == 0
            || policy.max_tokens > 1_000_000
        {
            return Err(Error::State(
                "provider operation limits must be positive and bounded".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_request(&self, bytes: &[u8]) -> Result<()> {
        let Self::AzureChat(policy) = self;
        let request: ChatRequest = serde_json::from_slice(bytes)?;
        if request.messages.is_empty()
            || request.messages.len() > policy.max_messages
            || request.max_tokens == 0
            || request.max_tokens > policy.max_tokens
            || !request.stream
            || request
                .temperature
                .is_some_and(|temperature| !(0.0..=2.0).contains(&temperature))
            || request
                .messages
                .iter()
                .any(|message| !matches!(message.role.as_str(), "system" | "user" | "assistant"))
            || request
                .messages
                .iter()
                .map(|message| message.content.len())
                .sum::<usize>()
                > policy.max_prompt_bytes
        {
            return Err(Error::Protocol(
                "provider request exceeds admitted operation semantics".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_operation_token_prompt_and_field_limits_are_host_enforced() {
        let policy = FetchBodyPolicy::AzureChat(AzureChatPolicy {
            max_messages: 4,
            max_prompt_bytes: 128,
            max_tokens: 512,
        });
        let valid =
            br#"{"messages":[{"role":"user","content":"hello"}],"max_tokens":128,"stream":true}"#;
        policy.validate_request(valid).unwrap();
        for invalid in [
            br#"{"messages":[{"role":"user","content":"hello"}],"max_tokens":513,"stream":true}"#.as_slice(),
            br#"{"messages":[{"role":"user","content":"hello"}],"stream":true}"#,
            br#"{"messages":[{"role":"user","content":"hello"}],"max_tokens":128,"stream":true,"model":"another-resource"}"#,
            br#"{"messages":[{"role":"user","content":"hello"}],"max_tokens":128,"stream":true,"n":1000}"#,
        ]{assert!(policy.validate_request(invalid).is_err());}
    }
}
