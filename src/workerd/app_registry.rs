// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Boundary 3 of the resident Workerd host: multi-app routing.
//!
//! A single host process can serve more than one Worker bundle at once, each
//! bound to its own [`AppRoute`] and its own pool (disposable/prewarmed via
//! the existing [`super::WorkerRequestPool`], or resident via the new
//! [`super::ResidentWorkerPool`]). Neither of those pool types, nor
//! [`super::sandbox`]'s existing disposable restore/execute paths, are
//! changed here: an [`AppRegistry`] only composes them per app.
//!
//! Capability policy configuration is intentionally minimal in this
//! boundary: [`WorkerCapabilityPolicyConfig`] only supports a deny-all fetch
//! broker and deny-all storage policy (matching every bundle this host is
//! currently exercised with). Richer per-app fetch allow-lists / storage
//! bindings through JSON are deferred to a follow-up change; nothing here
//! prevents adding fields to that struct later without breaking existing
//! configs (`#[serde(deny_unknown_fields)]` is intentionally *not* used on
//! it for that reason, while every other config struct in this module keeps
//! the stricter `deny_unknown_fields` convention from `protocol.rs`).

use super::{
    Error, FetchBroker, PoolSubmitError, RequestEnvelope, RequestExecution, ResidentHandle,
    ResidentPolicy, ResidentPoolConfig, ResidentWorkerPool, StoragePolicy, TimerLimits,
    WorkerBundle, WorkerCapabilityPolicy, WorkerPoolRestoreMode, WorkerRequestPool,
    WorkerVersionSandbox,
};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Generous upper bound on a `HostConfig` JSON file's size: this is operator
/// configuration, not untrusted guest/network input, so it is far larger
/// than the protocol envelope limits in `protocol.rs`.
const MAX_HOST_CONFIG_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum AppRegistryError {
    #[error("app config has no apps")]
    NoApps,
    #[error("duplicate app id: {0}")]
    DuplicateAppId(String),
    #[error("app '{0}' has no hostnames")]
    NoHostnames(String),
    #[error("routes for app '{first}' and app '{second}' are ambiguous")]
    AmbiguousRoute { first: String, second: String },
    #[error("failed to read host config at {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse host config at {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to load bundle for app '{app_id}': {source}")]
    BundleLoad {
        app_id: String,
        #[source]
        source: Error,
    },
    #[error("failed to initialize worker for app '{app_id}': {source}")]
    WorkerInit {
        app_id: String,
        #[source]
        source: Error,
    },
    #[error("failed to construct pool for app '{app_id}': {source}")]
    PoolInit {
        app_id: String,
        #[source]
        source: Error,
    },
}

/// Matches an inbound request to an app by exact `Host` header and/or a path
/// prefix. The first full match wins; [`AppRegistry::from_host_config`]
/// rejects any configuration where two routes could both match the same
/// `(hostname, path)` pair, so there is no runtime tie to break.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppRoute {
    pub app_id: String,
    pub hostnames: Vec<String>,
    pub path_prefix: Option<String>,
}

/// Capability policy for an app's worker. See the module docs: only
/// deny-all fetch/storage is supported today.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct WorkerCapabilityPolicyConfig {}

impl WorkerCapabilityPolicyConfig {
    fn build(&self) -> WorkerCapabilityPolicy {
        WorkerCapabilityPolicy::new(
            FetchBroker::denied(),
            TimerLimits::default(),
            StoragePolicy::denied(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisposablePoolConfig {
    pub max_concurrent_sandboxes: usize,
    pub queue_capacity: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResidentPoolConfigJson {
    pub capacity: usize,
    pub queue_capacity: usize,
    pub max_requests_per_vm: Option<u64>,
    pub max_lifetime_secs: Option<u64>,
}

impl From<ResidentPoolConfigJson> for ResidentPoolConfig {
    fn from(value: ResidentPoolConfigJson) -> Self {
        ResidentPoolConfig {
            capacity: value.capacity,
            queue_capacity: value.queue_capacity,
            policy: ResidentPolicy {
                max_requests_per_vm: value.max_requests_per_vm,
                max_lifetime: value.max_lifetime_secs.map(Duration::from_secs),
            },
        }
    }
}

/// Per-app choice between the existing disposable pool (unchanged) and the
/// new bounded resident pool, so one host process can mix both.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum AppPoolConfig {
    Disposable(DisposablePoolConfig),
    Resident(ResidentPoolConfigJson),
}

/// Boundary 5: whether requests on the same inbound keep-alive connection
/// should be pinned to the same resident VM for the connection's whole
/// lifetime. Only meaningful for [`AppPoolConfig::Resident`] apps —
/// [`AppHandle::reserve`] always returns `None` for a `Disposable` app
/// regardless of this setting, since disposable VMs are never reused across
/// requests by design (unchanged boundary-1/2 behavior).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionAffinity {
    /// No affinity: every request on the connection is submitted to the
    /// shared pool independently, exactly like today.
    #[default]
    None,
    /// Reserve one resident VM for the connection's lifetime so every
    /// request it sends observes the same VM.
    Sticky,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub route: AppRoute,
    pub bundle_path: PathBuf,
    pub scratch_memory_mb: usize,
    pub execute_timeout_secs: u64,
    #[serde(default)]
    pub capability_policy: WorkerCapabilityPolicyConfig,
    pub pool: AppPoolConfig,
    #[serde(default)]
    pub connection_affinity: ConnectionAffinity,
}

/// Top-level multi-app host configuration. `rootfs_path`/`executor_path`
/// name the shared guest kernel/executor image every app's
/// `WorkerVersionSandbox` is restored against; only the Worker bundle
/// (script) differs per app.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    pub rootfs_path: PathBuf,
    pub executor_path: PathBuf,
    pub apps: Vec<AppConfig>,
}

impl HostConfig {
    pub fn from_path(path: impl AsRef<Path>) -> std::result::Result<Self, AppRegistryError> {
        let path = path.as_ref();
        let mut bytes = Vec::new();
        File::open(path)
            .and_then(|file| file.take(MAX_HOST_CONFIG_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|source| AppRegistryError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        serde_json::from_slice(&bytes).map_err(|source| AppRegistryError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// One app's live pool, already bound to its initialized worker.
pub enum AppHandle {
    Disposable {
        app_id: String,
        pool: WorkerRequestPool,
    },
    Resident {
        app_id: String,
        pool: ResidentWorkerPool,
        affinity: ConnectionAffinity,
    },
}

impl AppHandle {
    pub fn app_id(&self) -> &str {
        match self {
            Self::Disposable { app_id, .. } | Self::Resident { app_id, .. } => app_id,
        }
    }

    /// This app's configured [`ConnectionAffinity`]. Always `None` for a
    /// `Disposable` app: the config field only applies to resident pools
    /// (see [`ConnectionAffinity`]'s docs), so a disposable app's
    /// `connection_affinity` setting, if any, is intentionally not stored
    /// or surfaced here.
    pub fn affinity(&self) -> ConnectionAffinity {
        match self {
            Self::Disposable { .. } => ConnectionAffinity::None,
            Self::Resident { affinity, .. } => *affinity,
        }
    }

    /// Reserve one resident VM for exclusive use by the caller (boundary 5
    /// connection affinity). Always `None` for a `Disposable` app. For a
    /// `Resident` app this delegates to [`ResidentWorkerPool::reserve`]
    /// regardless of the configured [`ConnectionAffinity`] — callers decide
    /// whether to reserve based on `affinity()` first, so an app's policy
    /// is enforced once, at the call site, not duplicated here.
    pub fn reserve(&self) -> Option<ResidentHandle> {
        match self {
            Self::Disposable { .. } => None,
            Self::Resident { pool, .. } => pool.reserve(),
        }
    }

    pub fn try_submit(
        &self,
        request: RequestEnvelope,
        timeout: Duration,
        completion: impl FnOnce(RequestExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        match self {
            Self::Disposable { pool, .. } => pool.try_submit(request, timeout, completion),
            Self::Resident { pool, .. } => pool.try_submit(request, timeout, completion),
        }
    }

    /// A per-app status summary for the `workerd-host` `/__hyperlight/status`
    /// contract endpoint (Boundary 4): a small, stable subset of each pool's
    /// full [`super::WorkerPoolStatus`]/[`super::ResidentPoolStatus`] fields,
    /// tagged by `kind`. Deliberately not the full field set the single-app
    /// `workerd-demo` example reports: richer per-app diagnostics are
    /// deferred to a follow-up change, same scope-limiting convention as
    /// [`WorkerCapabilityPolicyConfig`].
    pub fn status_json(&self) -> serde_json::Value {
        match self {
            Self::Disposable { app_id, pool } => {
                let status = pool.status();
                serde_json::json!({
                    "app_id": app_id,
                    "kind": "disposable",
                    "admitted": status.admitted,
                    "active": status.active,
                    "queued": status.queued,
                    "queue_capacity": status.queue_capacity,
                    "prewarmed_inventory": status.prewarmed_inventory,
                })
            }
            Self::Resident {
                app_id,
                pool,
                affinity,
            } => {
                let status = pool.status();
                serde_json::json!({
                    "app_id": app_id,
                    "kind": "resident",
                    "capacity": status.capacity,
                    "active": status.active,
                    "queued": status.queued,
                    "queue_capacity": status.queue_capacity,
                    "retirements": status.retirements,
                    "resident_requests_served": status.resident_requests_served,
                    "connection_affinity": matches!(affinity, ConnectionAffinity::Sticky)
                        .then_some("sticky")
                        .unwrap_or("none"),
                })
            }
        }
    }
}

struct RouteEntry {
    route: AppRoute,
    handle: AppHandle,
}

/// Owns every app's initialized worker and pool, and routes inbound
/// requests to the right one by `Host` header and path prefix.
pub struct AppRegistry {
    entries: Vec<RouteEntry>,
}

/// `true` if two optional path prefixes could both match the same path:
/// `None` matches every path, and two `Some` prefixes overlap exactly when
/// one is a prefix of the other (including being equal).
fn path_prefixes_overlap(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (None, _) | (_, None) => true,
        (Some(a), Some(b)) => a.starts_with(b) || b.starts_with(a),
    }
}

fn validate_routes(routes: &[&AppRoute]) -> std::result::Result<(), AppRegistryError> {
    let mut seen_ids = std::collections::HashSet::new();
    for route in routes {
        if !seen_ids.insert(route.app_id.as_str()) {
            return Err(AppRegistryError::DuplicateAppId(route.app_id.clone()));
        }
        if route.hostnames.is_empty() {
            return Err(AppRegistryError::NoHostnames(route.app_id.clone()));
        }
    }
    for i in 0..routes.len() {
        for j in (i + 1)..routes.len() {
            let (a, b) = (routes[i], routes[j]);
            let shares_hostname = a.hostnames.iter().any(|hostname| {
                b.hostnames
                    .iter()
                    .any(|other| other.eq_ignore_ascii_case(hostname))
            });
            if shares_hostname
                && path_prefixes_overlap(a.path_prefix.as_deref(), b.path_prefix.as_deref())
            {
                return Err(AppRegistryError::AmbiguousRoute {
                    first: a.app_id.clone(),
                    second: b.app_id.clone(),
                });
            }
        }
    }
    Ok(())
}

impl AppRegistry {
    /// Validate a [`HostConfig`]'s shape (at least one app, no duplicate app
    /// ids, no app with zero hostnames, no ambiguous routes) without
    /// touching the filesystem or starting any VM. Used by `workerd-host`
    /// (Boundary 4) to fail fast with a config error *before* binding a
    /// listener: every error this returns is a config-shape problem, never
    /// a bundle-load/worker-init/pool-init failure (those only surface from
    /// [`Self::from_host_config`], which calls this first).
    pub fn validate(config: &HostConfig) -> std::result::Result<(), AppRegistryError> {
        if config.apps.is_empty() {
            return Err(AppRegistryError::NoApps);
        }
        let routes: Vec<&AppRoute> = config.apps.iter().map(|app| &app.route).collect();
        validate_routes(&routes)
    }

    /// Validate every route up front (duplicate app ids, missing hostnames,
    /// ambiguous overlaps), then load and initialize every app's bundle and
    /// pool. No process/pool is started for *any* app if validation or any
    /// single app's load/init fails: a bad config fails closed as a whole.
    pub fn from_host_config(config: HostConfig) -> std::result::Result<Self, AppRegistryError> {
        Self::validate(&config)?;

        let mut entries = Vec::with_capacity(config.apps.len());
        for app in config.apps {
            let app_id = app.route.app_id.clone();
            let bundle = WorkerBundle::from_path(&app.bundle_path).map_err(|source| {
                AppRegistryError::BundleLoad {
                    app_id: app_id.clone(),
                    source,
                }
            })?;
            let policy = app.capability_policy.build();
            let worker = WorkerVersionSandbox::initialize_with_policy(
                bundle,
                &config.rootfs_path,
                &config.executor_path,
                app.scratch_memory_mb,
                Duration::from_secs(app.execute_timeout_secs),
                policy,
            )
            .map_err(|source| AppRegistryError::WorkerInit {
                app_id: app_id.clone(),
                source,
            })?;
            let handle = match app.pool {
                AppPoolConfig::Disposable(pool_config) => {
                    let pool = WorkerRequestPool::with_restore_mode(
                        worker,
                        pool_config.max_concurrent_sandboxes,
                        pool_config.queue_capacity,
                        WorkerPoolRestoreMode::OnDemand,
                    )
                    .map_err(|source| AppRegistryError::PoolInit {
                        app_id: app_id.clone(),
                        source,
                    })?;
                    AppHandle::Disposable { app_id, pool }
                }
                AppPoolConfig::Resident(pool_config) => {
                    let pool =
                        ResidentWorkerPool::new(worker, pool_config.into()).map_err(|source| {
                            AppRegistryError::PoolInit {
                                app_id: app_id.clone(),
                                source,
                            }
                        })?;
                    AppHandle::Resident {
                        app_id,
                        pool,
                        affinity: app.connection_affinity,
                    }
                }
            };
            entries.push(RouteEntry {
                route: app.route,
                handle,
            });
        }
        Ok(Self { entries })
    }

    /// Strips an optional `:port` suffix from `host_header` (as browsers and
    /// `reqwest` both send it) before matching against configured
    /// hostnames, case-insensitively.
    pub fn route(&self, host_header: Option<&str>, path: &str) -> Option<&AppHandle> {
        let host = host_header.map(|header| {
            header
                .rsplit_once(':')
                .map(|(host, _port)| host)
                .unwrap_or(header)
        });
        self.entries
            .iter()
            .find(|entry| {
                let hostname_matches = host
                    .map(|host| {
                        entry
                            .route
                            .hostnames
                            .iter()
                            .any(|candidate| candidate.eq_ignore_ascii_case(host))
                    })
                    .unwrap_or(false);
                hostname_matches
                    && entry
                        .route
                        .path_prefix
                        .as_deref()
                        .map(|prefix| path.starts_with(prefix))
                        .unwrap_or(true)
            })
            .map(|entry| &entry.handle)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every configured app's id, in configuration order. Used by
    /// `workerd-host`'s structured `"ready"` log event (Boundary 4).
    pub fn app_ids(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|entry| entry.handle.app_id())
    }

    /// Per-app status summaries for the `/__hyperlight/status` contract
    /// endpoint (Boundary 4), in configuration order.
    pub fn status_json(&self) -> Vec<serde_json::Value> {
        self.entries
            .iter()
            .map(|entry| entry.handle.status_json())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(app_id: &str, hostnames: &[&str], path_prefix: Option<&str>) -> AppRoute {
        AppRoute {
            app_id: app_id.into(),
            hostnames: hostnames.iter().map(|h| h.to_string()).collect(),
            path_prefix: path_prefix.map(str::to_string),
        }
    }

    #[test]
    fn disjoint_hostnames_are_never_ambiguous() {
        let a = route("a", &["a.test"], None);
        let b = route("b", &["b.test"], None);
        assert!(validate_routes(&[&a, &b]).is_ok());
    }

    #[test]
    fn same_hostname_disjoint_prefixes_are_allowed() {
        let a = route("a", &["shared.test"], Some("/a"));
        let b = route("b", &["shared.test"], Some("/b"));
        assert!(validate_routes(&[&a, &b]).is_ok());
    }

    #[test]
    fn same_hostname_overlapping_prefixes_are_rejected() {
        let a = route("a", &["shared.test"], Some("/api"));
        let b = route("b", &["shared.test"], Some("/api/v2"));
        let error = validate_routes(&[&a, &b]).unwrap_err();
        assert!(matches!(error, AppRegistryError::AmbiguousRoute { .. }));
    }

    #[test]
    fn same_hostname_with_a_catch_all_is_always_ambiguous() {
        let a = route("a", &["shared.test"], None);
        let b = route("b", &["shared.test"], Some("/only-b"));
        let error = validate_routes(&[&a, &b]).unwrap_err();
        assert!(matches!(error, AppRegistryError::AmbiguousRoute { .. }));
    }

    #[test]
    fn duplicate_app_ids_are_rejected() {
        let a = route("same", &["a.test"], None);
        let b = route("same", &["b.test"], None);
        let error = validate_routes(&[&a, &b]).unwrap_err();
        assert!(matches!(error, AppRegistryError::DuplicateAppId(id) if id == "same"));
    }

    #[test]
    fn empty_hostnames_are_rejected() {
        let a = route("a", &[], None);
        let error = validate_routes(&[&a]).unwrap_err();
        assert!(matches!(error, AppRegistryError::NoHostnames(id) if id == "a"));
    }
}
