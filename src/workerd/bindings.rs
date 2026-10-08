// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Reconstructable host-owned logical services. Only external persistent
//! backing paths and finite policies are captured, never live handles or
//! provider credentials. Each VM obtains a distinct runtime/session.
use super::{Error, Result, WorkerBinding, WorkerBindingKind};
use crate::broker::RequestIdentity;
use crate::broker_runtime::{BrokerRuntime, LogicalServiceRouter};
use crate::data::{D1Binding, D1Limits, D1Service, KvBinding, KvLimits, KvService};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalBindingConfig {
    Kv {
        name: String,
        backing_path: PathBuf,
        read_only: bool,
        #[serde(default)]
        limits: KvLimits,
    },
    #[serde(rename(serialize = "d1", deserialize = "sql"), alias = "d1")]
    D1 {
        name: String,
        backing_path: PathBuf,
        read_only: bool,
        #[serde(default)]
        limits: D1Limits,
    },
    Webhook(super::WebhookPolicy),
    ProviderWebsocket(Box<super::ProviderWebSocketConfig>),
}

impl LogicalBindingConfig {
    fn name(&self) -> &str {
        match self {
            Self::Kv { name, .. } | Self::D1 { name, .. } => name,
            Self::Webhook(policy) => &policy.name,
            Self::ProviderWebsocket(policy) => &policy.name,
        }
    }
    fn canonicalize(&mut self) -> Result<()> {
        let path = match self {
            Self::Kv { backing_path, .. } | Self::D1 { backing_path, .. } => backing_path,
            Self::Webhook(policy) => &mut policy.value_file,
            Self::ProviderWebsocket(policy) => &mut policy.credential.value_file,
        };
        let absolute = if path.is_absolute() {
            path.clone()
        } else {
            std::env::current_dir()?.join(&*path)
        };
        let parent = absolute
            .parent()
            .ok_or_else(|| Error::State("logical backing file has no parent".into()))?;
        let parent = std::fs::canonicalize(parent)?;
        let name = absolute
            .file_name()
            .ok_or_else(|| Error::State("logical backing file has no name".into()))?;
        *path = parent.join(name);
        match std::fs::symlink_metadata(&*path) {
            Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
                return Err(Error::State(
                    "logical backing must be a regular persistent file".into(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LogicalBindingsPolicy {
    workload_id: String,
    revision: String,
    bindings: Vec<LogicalBindingConfig>,
    network: Option<super::RawNetworkPolicyConfig>,
    budget_scope: LogicalBudgetScope,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBudgetScope {
    #[default]
    Invocation,
    Incarnation,
}

impl LogicalBindingsPolicy {
    pub fn new(
        workload_id: impl Into<String>,
        revision: impl Into<String>,
        mut bindings: Vec<LogicalBindingConfig>,
    ) -> Result<Self> {
        Self::with_network(workload_id, revision, std::mem::take(&mut bindings), None)
    }

    pub fn with_network(
        workload_id: impl Into<String>,
        revision: impl Into<String>,
        mut bindings: Vec<LogicalBindingConfig>,
        network: Option<super::RawNetworkPolicyConfig>,
    ) -> Result<Self> {
        let workload_id = workload_id.into();
        let revision = revision.into();
        RequestIdentity::new(&workload_id, &revision, 0)
            .map_err(|error| Error::State(error.to_string()))?;
        bindings.sort_by(|left, right| left.name().cmp(right.name()));
        if (bindings.is_empty() && network.is_none())
            || bindings.len() > 32
            || bindings
                .windows(2)
                .any(|pair| pair[0].name() == pair[1].name())
        {
            return Err(Error::State(
                "logical bindings must be nonempty, unique and at most32".into(),
            ));
        }
        for binding in &mut bindings {
            binding.canonicalize()?;
            match binding {
                LogicalBindingConfig::Kv {
                    name,
                    backing_path,
                    read_only,
                    limits,
                } => {
                    KvBinding::persistent(&*name, &*backing_path, *read_only, *limits)
                        .map_err(|error| Error::State(error.to_string()))?;
                }
                LogicalBindingConfig::D1 {
                    name,
                    backing_path,
                    read_only,
                    limits,
                } => {
                    D1Binding::persistent(&*name, &*backing_path, *read_only, *limits)
                        .map_err(|error| Error::State(error.to_string()))?;
                }
                LogicalBindingConfig::Webhook(policy) => policy.validate()?,
                LogicalBindingConfig::ProviderWebsocket(policy) => policy.validate()?,
            }
        }
        if let Some(network) = &network {
            network.broker()?;
        }
        let policy = Self {
            workload_id,
            revision,
            bindings,
            network,
            budget_scope: LogicalBudgetScope::Invocation,
        };
        Ok(policy)
    }

    pub(super) fn worker_bindings(&self) -> Vec<WorkerBinding> {
        self.bindings
            .iter()
            .map(|binding| WorkerBinding {
                name: binding.name().into(),
                kind: match binding {
                    LogicalBindingConfig::Kv { .. } => WorkerBindingKind::Kv,
                    LogicalBindingConfig::D1 { .. } => WorkerBindingKind::D1,
                    LogicalBindingConfig::Webhook(_) => WorkerBindingKind::Webhook,
                    LogicalBindingConfig::ProviderWebsocket(_) => {
                        WorkerBindingKind::ProviderWebsocket
                    }
                },
            })
            .collect()
    }

    pub fn with_budget_scope(mut self, scope: LogicalBudgetScope) -> Self {
        self.budget_scope = scope;
        self
    }
    pub(super) fn budget_scope(&self) -> LogicalBudgetScope {
        self.budget_scope
    }

    pub(super) fn authority(&self) -> String {
        serde_json::to_string(self).expect("typed logical authority is serializable")
    }

    pub(super) fn runtime(&self) -> Result<BrokerRuntime> {
        let identity = RequestIdentity::new(&self.workload_id, &self.revision, 0)
            .map_err(|error| Error::State(error.to_string()))?;
        let mut router = LogicalServiceRouter::new();
        for binding in &self.bindings {
            router = match binding {
                LogicalBindingConfig::Kv {
                    name,
                    backing_path,
                    read_only,
                    limits,
                } => {
                    let binding = KvBinding::persistent(name, backing_path, *read_only, *limits)
                        .map_err(|error| Error::State(error.to_string()))?;
                    let service = KvService::new([binding])
                        .map_err(|error| Error::State(error.to_string()))?;
                    router.with_service(name, service)
                }
                LogicalBindingConfig::D1 {
                    name,
                    backing_path,
                    read_only,
                    limits,
                } => {
                    let binding = D1Binding::persistent(name, backing_path, *read_only, *limits)
                        .map_err(|error| Error::State(error.to_string()))?;
                    let service = D1Service::new([binding])
                        .map_err(|error| Error::State(error.to_string()))?;
                    router.with_service(name, service)
                }
                LogicalBindingConfig::Webhook(policy) => {
                    let service =
                        super::webhook::WebhookService::new(policy.clone(), identity.clone())?;
                    router.with_service(&policy.name, service)
                }
                LogicalBindingConfig::ProviderWebsocket(_) => continue,
            }
            .map_err(|error| Error::State(error.to_string()))?;
        }
        let runtime = if let Some(network) = &self.network {
            network
                .broker()?
                .runtime(identity)
                .map_err(|error| Error::State(error.to_string()))?
        } else {
            BrokerRuntime::deny_all(identity)
        };
        let providers = self
            .bindings
            .iter()
            .filter_map(|binding| match binding {
                LogicalBindingConfig::ProviderWebsocket(policy) => Some(policy.as_ref().clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let runtime = runtime.with_provider_websockets(&providers)?;
        let logical_names = self
            .bindings
            .iter()
            .filter_map(|binding| match binding {
                LogicalBindingConfig::ProviderWebsocket(_) => None,
                binding => Some(binding.name().to_string()),
            })
            .collect::<Vec<_>>();
        if logical_names.is_empty() {
            Ok(runtime)
        } else {
            runtime
                .with_logical(logical_names, router)
                .map_err(|error| Error::State(error.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_sql_and_legacy_alias_keep_authority_and_guest_wire_identical() {
        let temporary = tempfile::tempdir().unwrap();
        let policy = |kind| {
            let config: LogicalBindingConfig = serde_json::from_value(serde_json::json!({
                "kind": kind,
                "name": "store",
                "backing_path": temporary.path().join("store.sqlite"),
                "read_only": false
            }))
            .unwrap();
            LogicalBindingsPolicy::new("tenant-app", "revision", vec![config]).unwrap()
        };
        let public = policy("sql");
        let legacy = policy("d1");
        assert_eq!(public.authority(), legacy.authority());
        assert!(public.authority().contains(r#""kind":"d1""#));
        assert_eq!(
            serde_json::to_string(&public.worker_bindings()[0]).unwrap(),
            r#"{"name":"store","kind":"d1"}"#
        );
    }

    #[test]
    fn reconstruction_reopens_external_data_without_sharing_session_quota_or_authority() {
        let tmp = tempfile::tempdir().unwrap();
        let policy = LogicalBindingsPolicy::new(
            "tenant-app",
            "revision-v1",
            vec![LogicalBindingConfig::Kv {
                name: "kv".into(),
                backing_path: tmp.path().join("kv.db"),
                read_only: false,
                limits: KvLimits {
                    max_operations: 1,
                    ..Default::default()
                },
            }],
        )
        .unwrap();
        let first = policy.runtime().unwrap();
        let put = br#"{"version":2,"request_id":"put-1","binding":"kv","operation":{"kind":"kv_put","key":"changed","value_base64":"cGVyc2lzdGVk"}}"#;
        let response: serde_json::Value =
            serde_json::from_slice(&first.dispatch_logical(put)).unwrap();
        assert_eq!(response["status"], "ok", "{response}");
        let get = br#"{"version":2,"request_id":"get-1","binding":"kv","operation":{"kind":"kv_get","key":"changed"}}"#;
        let exhausted: serde_json::Value =
            serde_json::from_slice(&first.dispatch_logical(get)).unwrap();
        assert_eq!(exhausted["status"], "quota_exceeded");
        let restored = policy.runtime().unwrap();
        let response: serde_json::Value =
            serde_json::from_slice(&restored.dispatch_logical(get)).unwrap();
        assert_eq!(response["status"], "ok", "{response}");
        let wrong = RequestIdentity::new("other-tenant", "revision-v1", 0).unwrap();
        let rejection: serde_json::Value =
            serde_json::from_slice(&restored.dispatch_logical_as(&wrong, get)).unwrap();
        assert_eq!(rejection["status"], "invalid_request");
    }

    #[test]
    fn same_binding_name_in_different_apps_cannot_share_authority_data_or_quota() {
        let tmp = tempfile::tempdir().unwrap();
        let policy = |app: &str| {
            LogicalBindingsPolicy::new(
                app,
                "revision",
                vec![LogicalBindingConfig::Kv {
                    name: "kv".into(),
                    backing_path: tmp.path().join(format!("{app}.db")),
                    read_only: false,
                    limits: KvLimits {
                        max_operations: 1,
                        ..Default::default()
                    },
                }],
            )
            .unwrap()
        };
        let a = policy("tenant-a");
        let b = policy("tenant-b");
        assert_ne!(a.authority(), b.authority());
        let runtime_a = a.runtime().unwrap();
        let runtime_b = b.runtime().unwrap();
        let put=br#"{"version":2,"request_id":"put-1","binding":"kv","operation":{"kind":"kv_put","key":"private","value_base64":"dGVuYW50LWE="}}"#;
        let get=br#"{"version":2,"request_id":"get-1","binding":"kv","operation":{"kind":"kv_get","key":"private"}}"#;
        let wrote: serde_json::Value =
            serde_json::from_slice(&runtime_a.dispatch_logical(put)).unwrap();
        assert_eq!(wrote["status"], "ok");
        let wrong = RequestIdentity::new("tenant-a", "revision", 0).unwrap();
        let denied: serde_json::Value =
            serde_json::from_slice(&runtime_b.dispatch_logical_as(&wrong, get)).unwrap();
        assert_eq!(denied["status"], "invalid_request");
        let own: serde_json::Value =
            serde_json::from_slice(&runtime_b.dispatch_logical(get)).unwrap();
        assert_ne!(
            own["status"], "quota_exceeded",
            "tenant-a operations must not consume tenant-b quota"
        );
        assert!(
            !own.to_string().contains("dGVuYW50LWE="),
            "tenant-b must not receive tenant-a private value"
        );
    }
}
