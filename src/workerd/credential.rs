// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const EXPIRING_CREDENTIAL_CAPABILITY: &str = "credential-expiry-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum CredentialSource {
    #[serde(rename = "expiring_header_v1")]
    ExpiringHeaderV1 {
        #[serde(default = "default_margin")]
        safety_margin_ms: u64,
    },
}

fn default_margin() -> u64 {
    30_000
}

impl CredentialSource {
    pub(super) fn validate(&self) -> Result<()> {
        match self {
            Self::ExpiringHeaderV1 { safety_margin_ms }
                if cfg!(unix) && *safety_margin_ms <= 300_000 => Ok(()),
            _ => Err(Error::State("expiring header source requires Unix ownership checks and a safety margin of at most 300000 ms".into())),
        }
    }

    pub(super) fn read(
        &self,
        path: &Path,
        reference: &str,
        deadline: Instant,
    ) -> Result<HeaderValue> {
        let mut bytes = super::secret::read_expiring_header_file(path)?;
        // Sample monotonic time first so wall-time sampling cannot shorten
        // the admitted remaining attempt budget.
        let monotonic_now = Instant::now();
        let now = SystemTime::now();
        let result = deadline
            .checked_duration_since(monotonic_now)
            .ok_or_else(unavailable)
            .and_then(|remaining| self.decode(&bytes, reference, now, remaining));
        bytes.fill(0);
        result
    }

    fn decode(
        &self,
        bytes: &[u8],
        reference: &str,
        now: SystemTime,
        remaining: Duration,
    ) -> Result<HeaderValue> {
        self.validate()?;
        let envelope: ExpiringHeader = serde_json::from_slice(bytes).map_err(|_| unavailable())?;
        let Self::ExpiringHeaderV1 { safety_margin_ms } = self;
        let wall = now.duration_since(UNIX_EPOCH).map_err(|_| unavailable())?;
        if wall.is_zero() {
            return Err(unavailable());
        }
        let milliseconds = |duration: Duration| -> Result<u64> {
            u64::try_from(duration.as_nanos().div_ceil(1_000_000)).map_err(|_| unavailable())
        };
        let required_until = milliseconds(wall)?
            .checked_add(milliseconds(remaining)?)
            .and_then(|value| value.checked_add(*safety_margin_ms))
            .ok_or_else(unavailable)?;
        if envelope.version != 1
            || remaining.is_zero()
            || envelope.reference != reference
            || envelope.generation.is_empty()
            || envelope.generation.len() > 128
            || !envelope
                .generation
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            || envelope.expires_at_unix_ms <= required_until
            || envelope.header_value.0.is_empty()
            || envelope.header_value.0.len() > 4096
            || !envelope
                .header_value
                .0
                .bytes()
                .all(|byte| (0x20..=0x7e).contains(&byte))
        {
            return Err(unavailable());
        }
        let mut value = HeaderValue::from_bytes(envelope.header_value.0.as_bytes())
            .map_err(|_| unavailable())?;
        value.set_sensitive(true);
        Ok(value)
    }
}

pub(super) fn deserialize_source<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<CredentialSource>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    CredentialSource::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(transparent)]
struct HeaderSecret(String);

impl Drop for HeaderSecret {
    fn drop(&mut self) {
        let mut bytes = std::mem::take(&mut self.0).into_bytes();
        bytes.fill(0);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpiringHeader {
    version: u16,
    reference: String,
    generation: String,
    expires_at_unix_ms: u64,
    header_value: HeaderSecret,
}

fn unavailable() -> Error {
    Error::State("host expiring credential unavailable or invalid".into())
}

#[cfg(all(test, unix))]
mod tests {
    use super::super::{FetchBroker, FetchCredential};
    use super::*;

    fn envelope(expires: u64) -> serde_json::Value {
        serde_json::json!({"version":1,"reference":"test-ref","generation":"generation-1",
            "expires_at_unix_ms":expires,"header_value":"Bearer synthetic"})
    }

    fn source() -> CredentialSource {
        CredentialSource::ExpiringHeaderV1 {
            safety_margin_ms: 30_000,
        }
    }

    fn decode(value: &serde_json::Value) -> Result<HeaderValue> {
        source().decode(
            &serde_json::to_vec(value).unwrap(),
            "test-ref",
            UNIX_EPOCH + Duration::from_secs(1000),
            Duration::from_secs(5),
        )
    }

    #[test]
    fn expiring_header_covers_the_whole_attempt_budget_and_margin() {
        assert!(decode(&envelope(1_035_000)).is_err());
        let header = decode(&envelope(1_035_001)).unwrap();
        assert_eq!(header.as_bytes(), b"Bearer synthetic");
        assert!(header.is_sensitive());
    }

    #[test]
    fn malformed_or_mismatched_envelopes_fail_without_secret_diagnostics() {
        for (field, field_value) in [
            ("version", serde_json::json!(2)),
            ("reference", serde_json::json!("other")),
            ("generation", serde_json::json!("")),
            ("generation", serde_json::json!("bad/name")),
            ("expires_at_unix_ms", serde_json::json!(1036)),
            ("expires_at_unix_ms", serde_json::Value::Null),
            ("header_value", serde_json::json!("Bearer synthetic\r\nbad")),
            ("header_value", serde_json::json!("x".repeat(4097))),
        ] {
            let mut value = envelope(1_036_000).as_object().unwrap().clone();
            value.insert(field.into(), field_value);
            assert!(decode(&serde_json::Value::Object(value)).is_err());
        }
        let mut extra = envelope(1_036_000);
        extra["unknown"] = serde_json::json!(true);
        assert!(decode(&extra).is_err());
        assert!(
            !decode(&envelope(1))
                .unwrap_err()
                .to_string()
                .contains("synthetic")
        );
    }

    #[test]
    fn invalid_clocks_overflow_and_submillisecond_cutoffs_fail_closed() {
        let bytes = serde_json::to_vec(&envelope(u64::MAX)).unwrap();
        assert!(
            source()
                .decode(&bytes, "test-ref", UNIX_EPOCH, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            source()
                .decode(
                    &bytes,
                    "test-ref",
                    UNIX_EPOCH - Duration::from_secs(1),
                    Duration::from_secs(1)
                )
                .is_err()
        );
        assert!(
            source()
                .decode(
                    &bytes,
                    "test-ref",
                    UNIX_EPOCH + Duration::from_secs(1),
                    Duration::MAX
                )
                .is_err()
        );
        assert!(
            source()
                .decode(
                    &bytes,
                    "test-ref",
                    UNIX_EPOCH + Duration::from_secs(1),
                    Duration::ZERO
                )
                .is_err()
        );
        let exact = CredentialSource::ExpiringHeaderV1 {
            safety_margin_ms: 0,
        };
        let now = UNIX_EPOCH + Duration::from_nanos(1_000_000_001);
        assert!(
            exact
                .decode(
                    &serde_json::to_vec(&envelope(1002)).unwrap(),
                    "test-ref",
                    now,
                    Duration::from_nanos(1)
                )
                .is_err()
        );
        assert!(
            exact
                .decode(
                    &serde_json::to_vec(&envelope(1003)).unwrap(),
                    "test-ref",
                    now,
                    Duration::from_nanos(1)
                )
                .is_ok()
        );
    }

    #[test]
    fn raw_serialization_and_identity_are_unchanged_unless_expiry_is_selected() {
        let raw = FetchCredential {
            reference: "test-ref".into(),
            value_file: "/run/keys/provider".into(),
            header: "authorization".into(),
            scheme: "https".into(),
            host: "api.example.test".into(),
            port: 443,
            source: None,
        };
        let json = serde_json::to_string(&raw).unwrap();
        assert_eq!(
            json,
            r#"{"reference":"test-ref","value_file":"/run/keys/provider","header":"authorization","scheme":"https","host":"api.example.test","port":443}"#
        );
        let identity = |credential| {
            FetchBroker::denied()
                .with_credential(credential)
                .unwrap()
                .policy_identity()
        };
        let raw_identity = identity(raw.clone());
        let mut expiring = raw.clone();
        expiring.source = Some(source());
        assert_ne!(raw_identity, identity(expiring.clone()));
        let configured_identity = identity(expiring.clone());
        for field in [
            "reference",
            "value_file",
            "header",
            "host",
            "port",
            "source",
        ] {
            let mut changed = expiring.clone();
            match field {
                "reference" => changed.reference = "another-ref".into(),
                "value_file" => changed.value_file = "/run/keys/another".into(),
                "header" => changed.header = "api-key".into(),
                "host" => changed.host = "other.example.test".into(),
                "port" => changed.port = 8443,
                "source" => {
                    changed.source = Some(CredentialSource::ExpiringHeaderV1 {
                        safety_margin_ms: 1,
                    })
                }
                _ => unreachable!(),
            }
            assert_ne!(configured_identity, identity(changed));
        }
        let mut value = serde_json::to_value(&raw).unwrap();
        value["source"] = serde_json::json!({"kind":"expiring_header_v1"});
        let implicit: FetchCredential = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(identity(implicit), identity(expiring));
        value["source"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<FetchCredential>(value.clone()).is_err());
        for descriptor in [
            serde_json::json!({"kind":"typo"}),
            serde_json::json!({"kind":"expiring_header_v1","unknown":true}),
            serde_json::json!({"kind":"expiring_header_v1","safety_margin_ms":null}),
        ] {
            value["source"] = descriptor;
            assert!(serde_json::from_value::<FetchCredential>(value.clone()).is_err());
        }
        assert!(
            CredentialSource::ExpiringHeaderV1 {
                safety_margin_ms: 300_001
            }
            .validate()
            .is_err()
        );
    }
}
