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
//! Per-app capability configuration is deny-by-default and strictly typed.
//! Unsupported logical services and misspelled fields are rejected rather
//! than silently granting, dropping or substituting authority.

use super::{
    Error, FetchBroker, FetchBrokerConfig, FetchLimits, FetchPolicy, PoolSubmitError,
    RequestEnvelope, RequestExecution, ResidentHandle, ResidentPolicy, ResidentPoolConfig,
    ResidentWorkerPool, StorageBinding, StoragePolicy, TimerLimits, WorkerBundle,
    WorkerCapabilityPolicy, WorkerPoolRestoreMode, WorkerRequestPool, WorkerVersionSandbox,
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
    #[error("invalid capability policy for app '{app_id}': {source}")]
    CapabilityPolicy {
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FetchPolicyConfig {
    pub hosts: Vec<String>,
    pub schemes: Vec<String>,
    pub ports: Vec<u16>,
    pub methods: Vec<String>,
    pub ip_ranges: Vec<ipnet::IpNet>,
    pub credential: Option<super::FetchCredential>,
    pub paths: Vec<String>,
    pub query_parameters: std::collections::BTreeMap<String, Vec<String>>,
    pub body_policy: Option<super::FetchBodyPolicy>,
    pub allow_loopback: bool,
    pub allow_private: bool,
    pub allow_metadata: bool,
    pub limits: FetchLimits,
}

impl FetchPolicyConfig {
    fn build(&self) -> super::Result<FetchBroker> {
        if self
            .schemes
            .iter()
            .any(|scheme| scheme != "http" && scheme != "https")
            || self.ports.contains(&0)
            || self
                .hosts
                .iter()
                .any(|host| host.is_empty() || host.contains(char::is_whitespace))
        {
            return Err(Error::State(
                "fetch requires valid hosts, http/https schemes and nonzero ports".into(),
            ));
        }
        let hosts = crate::AllowList::from_hosts(&self.hosts)
            .map_err(|error| Error::State(format!("invalid fetch allowlist: {error}")))?;
        let policy = FetchPolicy::new(
            crate::NetworkPolicy::AllowList(hosts),
            self.schemes.clone(),
            self.ports.clone(),
        )
        .allow_loopback(self.allow_loopback)
        .allow_private(self.allow_private)
        .allow_metadata(self.allow_metadata)
        .with_methods(self.methods.clone())?
        .with_ip_ranges(self.ip_ranges.clone())
        .with_resource_scope(
            self.paths.clone(),
            self.query_parameters.clone(),
            self.body_policy.clone(),
        )?;
        let broker = FetchBroker::new(FetchBrokerConfig {
            policy,
            limits: self.limits.clone(),
        })?;
        if let Some(credential) = &self.credential {
            broker.with_credential(credential.clone())
        } else {
            Ok(broker)
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageBindingConfig {
    pub name: String,
    pub host_path: PathBuf,
    pub mode: StorageMode,
    pub max_operations: u64,
    pub max_read_bytes: u64,
    pub max_write_bytes: u64,
}

impl StorageBindingConfig {
    fn build(&self) -> super::Result<StorageBinding> {
        let limits = crate::MountLimits {
            max_operations: Some(self.max_operations),
            max_read_bytes: Some(self.max_read_bytes),
            max_write_bytes: Some(self.max_write_bytes),
        };
        match self.mode {
            StorageMode::ReadOnly => StorageBinding::read_only(&self.name, &self.host_path, limits),
            StorageMode::ReadWrite => {
                StorageBinding::read_write(&self.name, &self.host_path, limits)
            }
        }
    }
}

/// Empty configuration denies fetch/storage; timers remain bounded.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerCapabilityPolicyConfig {
    pub fetch: FetchPolicyConfig,
    pub timers: TimerLimits,
    pub storage: Vec<StorageBindingConfig>,
    pub bindings: Vec<super::LogicalBindingConfig>,
    pub network: Option<super::RawNetworkPolicyConfig>,
    pub binding_budget_scope: super::LogicalBudgetScope,
}

impl WorkerCapabilityPolicyConfig {
    pub fn sha256(&self) -> super::Result<String> {
        Ok(self.build()?.sha256())
    }

    fn build(&self) -> super::Result<WorkerCapabilityPolicy> {
        self.build_for("standalone", "unbound")
    }

    pub fn sha256_for(&self, app_id: &str, revision: &str) -> super::Result<String> {
        Ok(self.build_for(app_id, revision)?.sha256())
    }

    fn build_for(&self, app_id: &str, revision: &str) -> super::Result<WorkerCapabilityPolicy> {
        super::timer::TimerBroker::new(self.timers.clone())?;
        let bindings = self
            .storage
            .iter()
            .map(StorageBindingConfig::build)
            .collect::<super::Result<Vec<_>>>()?;
        let webhook_secrets = self.bindings.iter().filter_map(|binding| match binding {
            super::LogicalBindingConfig::Webhook(policy) => Some(&policy.value_file),
            super::LogicalBindingConfig::ProviderWebsocket(policy) => {
                Some(&policy.credential.value_file)
            }
            _ => None,
        });
        let fetch_secrets = self
            .fetch
            .credential
            .iter()
            .map(|credential| &credential.value_file);
        for secret in webhook_secrets.chain(fetch_secrets) {
            let parent = secret
                .parent()
                .ok_or_else(|| Error::State("secret reference has no secret directory".into()))?;
            let parent = std::fs::canonicalize(parent)?;
            if bindings
                .iter()
                .any(|binding| parent.starts_with(binding.host_path()))
            {
                return Err(Error::State(
                    "credential reference overlaps a guest-mounted host directory".into(),
                ));
            }
        }
        let policy = WorkerCapabilityPolicy::new(
            self.fetch.build()?,
            self.timers.clone(),
            StoragePolicy::new(bindings)?,
        );
        if self.bindings.is_empty() && self.network.is_none() {
            Ok(policy)
        } else {
            policy.with_logical_bindings(
                super::LogicalBindingsPolicy::with_network(
                    app_id,
                    revision,
                    self.bindings.clone(),
                    self.network.clone(),
                )?
                .with_budget_scope(self.binding_budget_scope),
            )
        }
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
    /// Optional prebuilt, on-disk [`super::VerifiedSnapshot`] directory
    /// (as written by a prior `.snapshot().save(dir)`, or the `hluk
    /// workerd-host --prewarm-snapshot` one-shot command). When set, this
    /// app's worker is restored directly from it
    /// ([`super::WorkerVersionSandbox::initialize_from_snapshot_dir`]),
    /// skipping the boot+init+snapshot sequence entirely — the dominant
    /// per-app startup cost at large `--benchmark-apps` counts. Only safe
    /// when `bundle_path`/the shared `rootfs_path`/`executor_path`/
    /// `capability_policy` are *exactly* what the snapshot was built from:
    /// a mismatch is rejected (fails closed), never silently restores the
    /// wrong code — see [`super::VerifiedSnapshot::open`]. Omitted/`None`
    /// (the default) keeps today's fresh-boot-every-app behavior
    /// unchanged.
    #[serde(default)]
    pub snapshot_dir: Option<PathBuf>,
    /// Opt-in fenced instance home replaces (never duplicates) the resident
    /// pool. Public requests must pass through the authenticated platform.
    #[serde(default)]
    pub instance_home: Option<super::InstanceHomeConfig>,
    #[serde(default)]
    pub streaming: bool,
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
    Instances {
        app_id: String,
        home: Box<super::InstanceHome>,
        identity: AppIdentity,
    },
    Disposable {
        app_id: String,
        pool: WorkerRequestPool,
        identity: AppIdentity,
    },
    Resident {
        app_id: String,
        pool: ResidentWorkerPool,
        affinity: ConnectionAffinity,
        identity: AppIdentity,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct AppIdentity {
    pub worker_version: super::WorkerVersionId,
    pub bundle_sha256: String,
    pub capability_policy_sha256: String,
    pub execute_timeout_secs: u64,
    pub streaming: bool,
    pub capabilities: Vec<String>,
}

impl AppHandle {
    pub fn identity(&self) -> &AppIdentity {
        match self {
            Self::Disposable { identity, .. }
            | Self::Resident { identity, .. }
            | Self::Instances { identity, .. } => identity,
        }
    }

    pub fn execute_timeout(&self) -> Duration {
        Duration::from_secs(self.identity().execute_timeout_secs)
    }
    pub fn app_id(&self) -> &str {
        match self {
            Self::Disposable { app_id, .. }
            | Self::Resident { app_id, .. }
            | Self::Instances { app_id, .. } => app_id,
        }
    }

    /// This app's configured [`ConnectionAffinity`]. Always `None` for a
    /// `Disposable` app: the config field only applies to resident pools
    /// (see [`ConnectionAffinity`]'s docs), so a disposable app's
    /// `connection_affinity` setting, if any, is intentionally not stored
    /// or surfaced here.
    pub fn affinity(&self) -> ConnectionAffinity {
        match self {
            Self::Disposable { .. } | Self::Instances { .. } => ConnectionAffinity::None,
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
            Self::Disposable { .. } | Self::Instances { .. } => None,
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
            Self::Instances { .. } => {
                completion(RequestExecution {
                    request_id: request.request_id,
                    result: Err(Error::State(
                        "instance home requires a fenced instance invocation".into(),
                    )),
                    profile: Default::default(),
                    submit_error: Some(PoolSubmitError::Unavailable),
                });
                Err(PoolSubmitError::Unavailable)
            }
        }
    }

    pub fn try_submit_invocation(
        &self,
        request: super::InvocationRequest,
        timeout: Duration,
        completion: impl FnOnce(super::InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        match self {
            Self::Disposable { pool, .. } => {
                pool.try_submit_invocation(request, timeout, completion)
            }
            Self::Resident { pool, .. } => pool.try_submit_invocation(request, timeout, completion),
            Self::Instances { .. } => {
                completion(super::InvocationExecution {
                    request_id: request.request_id().into(),
                    result: Err(Error::State(
                        "instance home requires a fenced instance invocation".into(),
                    )),
                    profile: Default::default(),
                    submit_error: Some(PoolSubmitError::Unavailable),
                });
                Err(PoolSubmitError::Unavailable)
            }
        }
    }

    pub fn try_submit_cancellable(
        &self,
        request: super::InvocationRequest,
        timeout: Duration,
        cancellation: super::InvocationCancellation,
        completion: impl FnOnce(super::InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        match self {
            Self::Disposable { pool, .. } => {
                pool.try_submit_cancellable(request, timeout, cancellation, completion)
            }
            Self::Resident { pool, .. } => {
                pool.try_submit_cancellable(request, timeout, cancellation, completion)
            }
            Self::Instances { .. } => self.try_submit_invocation(request, timeout, completion),
        }
    }

    pub fn try_submit_stream(
        &self,
        request: RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
        completion: impl FnOnce(super::InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        match self {
            Self::Disposable { pool, .. } => {
                pool.try_submit_stream(request, websocket, ingress, timeout, completion)
            }
            Self::Resident { pool, .. } => {
                pool.try_submit_stream(request, websocket, ingress, timeout, completion)
            }
            Self::Instances { .. } => {
                completion(super::InvocationExecution {
                    request_id: request.request_id,
                    result: Err(Error::State(
                        "stream requires fenced instance transport".into(),
                    )),
                    profile: Default::default(),
                    submit_error: Some(PoolSubmitError::Unavailable),
                });
                Err(PoolSubmitError::Unavailable)
            }
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
            Self::Instances {
                app_id,
                home,
                identity,
            } => match home.status() {
                Ok(instances) => serde_json::json!({
                    "app_id":app_id,"kind":"resident","identity":identity,
                    "instance_home":true,"checkpoint_policy":home.policy(),"instances":instances,
                }),
                Err(error) => serde_json::json!({
                    "app_id":app_id,"kind":"resident","identity":identity,
                    "instance_home":true,"status":"unavailable","error":error.to_string(),
                }),
            },
            Self::Disposable {
                app_id,
                pool,
                identity,
            } => {
                let status = pool.status();
                serde_json::json!({
                    "app_id": app_id,
                    "identity": identity,
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
                identity,
            } => {
                let status = pool.status();
                serde_json::json!({
                    "app_id": app_id,
                    "identity": identity,
                    "kind": "resident",
                    "capacity": status.capacity,
                    "live_vms": status.live_vms,
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
    ///
    /// Every app's bundle load + worker init + pool init is independent (no
    /// shared mutable state between apps), and Hyperlight sandbox creation
    /// can take low-single-digit seconds per app under nested
    /// virtualization. Initializing apps one at a time made startup time
    /// scale linearly with app count -- confirmed in practice: 32+ apps
    /// routinely exceeded `workerd-host`'s external 60s readiness wait and
    /// got killed before ever becoming ready. Apps are therefore
    /// initialized concurrently, one OS thread per app via
    /// `std::thread::scope` (no extra thread-pool dependency), so total
    /// init time tracks the slowest single app rather than the sum of all
    /// of them. Host configs are operator-authored, not attacker/request
    /// controlled, so no additional concurrency cap is applied here beyond
    /// `config.apps.len()`.
    pub fn from_host_config(config: HostConfig) -> std::result::Result<Self, AppRegistryError> {
        Self::validate(&config)?;

        let rootfs_path = &config.rootfs_path;
        let executor_path = &config.executor_path;

        let results: Vec<std::result::Result<RouteEntry, AppRegistryError>> =
            std::thread::scope(|scope| {
                let handles: Vec<_> = config
                    .apps
                    .into_iter()
                    .map(|app| {
                        scope.spawn(move || {
                            let app_id = app.route.app_id.clone();
                            let bundle =
                                WorkerBundle::from_path(&app.bundle_path).map_err(|source| {
                                    AppRegistryError::BundleLoad {
                                        app_id: app_id.clone(),
                                        source,
                                    }
                                })?;
                            let policy = app
                                .capability_policy
                                .build_for(&app_id, bundle.worker_version.as_str())
                                .map_err(|source| AppRegistryError::CapabilityPolicy {
                                    app_id: app_id.clone(),
                                    source,
                                })?;
                            let worker = if let Some(snapshot_dir) = &app.snapshot_dir {
                                WorkerVersionSandbox::initialize_from_snapshot_dir(
                                    snapshot_dir,
                                    &bundle,
                                    rootfs_path,
                                    executor_path,
                                    policy,
                                )
                                .map_err(|source| {
                                    AppRegistryError::WorkerInit {
                                        app_id: app_id.clone(),
                                        source,
                                    }
                                })?
                            } else {
                                WorkerVersionSandbox::initialize_with_policy(
                                    bundle,
                                    rootfs_path,
                                    executor_path,
                                    app.scratch_memory_mb,
                                    Duration::from_secs(app.execute_timeout_secs),
                                    policy,
                                )
                                .map_err(|source| {
                                    AppRegistryError::WorkerInit {
                                        app_id: app_id.clone(),
                                        source,
                                    }
                                })?
                            };
                            let identity = AppIdentity {
                                worker_version: worker.worker_version().clone(),
                                bundle_sha256: worker.snapshot().binding().bundle_sha256().into(),
                                capability_policy_sha256: worker
                                    .snapshot()
                                    .binding()
                                    .capability_policy_sha256()
                                    .into(),
                                execute_timeout_secs: app.execute_timeout_secs,
                                streaming: app.streaming,
                                capabilities: Vec::new(),
                            };
                            let mut identity = identity;
                            identity.capabilities = [
                                "fetch",
                                "scheduled",
                                "queue",
                                "bounded-capacity",
                                "cancel",
                                "outbound-policy-v1",
                            ]
                            .into_iter()
                            .map(str::to_string)
                            .collect();
                            let mut required = vec!["tracked-work-drain-v1", "safe-point-v1"];
                            if app.streaming {
                                required.push("ingress-stream-v1");
                            }
                            if app.capability_policy.bindings.iter().any(|binding| {
                                matches!(binding, super::LogicalBindingConfig::ProviderWebsocket(_))
                            }) {
                                required.push("authenticated-egress-websocket-v1");
                            }
                            let negotiated = worker
                                .negotiated_extensions(
                                    &required,
                                    Duration::from_secs(app.execute_timeout_secs),
                                )
                                .map_err(|source| AppRegistryError::WorkerInit {
                                    app_id: app_id.clone(),
                                    source,
                                })?;
                            identity.capabilities.push("tracked-work-drain".into());
                            if negotiated
                                .extensions
                                .iter()
                                .any(|extension| extension == "secure-entropy-v1")
                            {
                                identity.capabilities.push("secure-entropy-v1".into());
                            }
                            if negotiated
                                .extensions
                                .iter()
                                .any(|extension| extension == "authenticated-egress-websocket-v1")
                            {
                                identity
                                    .capabilities
                                    .push("authenticated-egress-websocket-v1".into());
                            }
                            if app.streaming {
                                identity.capabilities.extend(
                                    ["ingress-stream", "sse", "websocket", "fetch-stream"]
                                        .into_iter()
                                        .map(str::to_string),
                                );
                            }
                            if let Some(home) = &app.instance_home {
                                identity.capabilities.extend(
                                    [
                                        "current-state-checkpoint",
                                        "safe-point-drain",
                                        "fenced-ownership",
                                    ]
                                    .into_iter()
                                    .map(str::to_string),
                                );
                                if home.checkpoint_policy == super::CheckpointPolicy::Durable {
                                    identity.capabilities.push("durable-checkpoint".into());
                                }
                            }
                            for binding in &app.capability_policy.bindings {
                                match binding {
                                    super::LogicalBindingConfig::D1 { .. } => {
                                        identity.capabilities.extend(
                                            ["binding-sql", "binding-d1"]
                                                .into_iter()
                                                .map(str::to_string),
                                        );
                                    }
                                    super::LogicalBindingConfig::Kv { .. } => {
                                        identity.capabilities.push("binding-kv".into())
                                    }
                                    super::LogicalBindingConfig::Webhook(_) => {
                                        identity.capabilities.push("binding-webhook-secret".into())
                                    }
                                    super::LogicalBindingConfig::ProviderWebsocket(_) => {
                                        identity.capabilities.push("provider-websocket-v1".into())
                                    }
                                }
                            }
                            if app.capability_policy.fetch.body_policy.is_some()
                                && app.capability_policy.fetch.credential.is_some()
                                && !app.capability_policy.fetch.paths.is_empty()
                            {
                                identity.capabilities.push("binding-ai-azure".into());
                            }
                            let handle = if let Some(home_config) = app.instance_home {
                                let AppPoolConfig::Resident(pool_config) = app.pool else {
                                    return Err(AppRegistryError::PoolInit {
                                        app_id,
                                        source: Error::State(
                                            "instance home requires resident execution mode".into(),
                                        ),
                                    });
                                };
                                if app.connection_affinity != ConnectionAffinity::None {
                                    return Err(AppRegistryError::PoolInit {
                                        app_id,
                                        source: Error::State(
                                            "fenced instance home cannot use connection affinity"
                                                .into(),
                                        ),
                                    });
                                }
                                let home = super::InstanceHome::new(
                                    worker,
                                    home_config,
                                    pool_config.capacity,
                                    pool_config.queue_capacity,
                                )
                                .map_err(|source| {
                                    AppRegistryError::PoolInit {
                                        app_id: app_id.clone(),
                                        source,
                                    }
                                })?;
                                AppHandle::Instances {
                                    app_id,
                                    home: Box::new(home),
                                    identity,
                                }
                            } else {
                                match app.pool {
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
                                        AppHandle::Disposable {
                                            app_id,
                                            pool,
                                            identity,
                                        }
                                    }
                                    AppPoolConfig::Resident(pool_config) => {
                                        let pool =
                                            ResidentWorkerPool::new(worker, pool_config.into())
                                                .map_err(|source| AppRegistryError::PoolInit {
                                                    app_id: app_id.clone(),
                                                    source,
                                                })?;
                                        AppHandle::Resident {
                                            app_id,
                                            pool,
                                            affinity: app.connection_affinity,
                                            identity,
                                        }
                                    }
                                }
                            };
                            Ok(RouteEntry {
                                route: app.route,
                                handle,
                            })
                        })
                    })
                    .collect();
                // Join in the same order apps were declared so `entries`
                // (and therefore route-matching precedence) stays
                // deterministic regardless of which thread finished first.
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                    })
                    .collect()
            });

        let mut entries = Vec::with_capacity(results.len());
        for result in results {
            entries.push(result?);
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

    pub fn app(&self, app_id: &str) -> Option<&AppHandle> {
        self.entries
            .iter()
            .find(|entry| entry.handle.app_id() == app_id)
            .map(|entry| &entry.handle)
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

    pub fn capabilities_json(&self) -> serde_json::Value {
        let apps=self.entries.iter().map(|entry|serde_json::json!({
            "app_id":entry.handle.app_id(),"revision":entry.handle.identity().worker_version,
            "capabilities":entry.handle.identity().capabilities,
        })).collect::<Vec<_>>();
        let mut capabilities = self
            .entries
            .iter()
            .flat_map(|entry| entry.handle.identity().capabilities.iter().cloned())
            .collect::<Vec<_>>();
        capabilities.sort();
        capabilities.dedup();
        serde_json::json!({"protocol_version":1,"capabilities":capabilities,"apps":apps,
            "limits":{"bundle_source_bytes":super::MAX_PACKAGE_SOURCE_BYTES,"module_source_bytes":super::MAX_PACKAGE_MODULE_BYTES,
                "legacy_bundle_source_bytes":super::MAX_BUNDLE_SOURCE_BYTES,"body_bytes":super::MAX_BODY_BYTES,
                "envelope_bytes":super::MAX_ENVELOPE_BYTES,"frame_bytes":super::MAX_FRAME_BYTES}})
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

    #[test]
    fn capability_config_rejects_unknown_nested_and_unsupported_settings() {
        for json in [
            r#"{"fetxh":{}}"#,
            r#"{"fetch":{"allow_prviate":true}}"#,
            r#"{"fetch":{"limits":{"max_reponse_bytes":1}}}"#,
            r#"{"timers":{"unbounded":true}}"#,
            r#"{"bindings":[{"name":"KV","kind":"kv"}]}"#,
            r#"{"storage":[{"name":"data","host_path":"/tmp","mode":"read_write","max_operations":1,"max_read_bytes":1,"max_write_bytes":1,"unbounded":true}]}"#,
        ] {
            assert!(
                serde_json::from_str::<WorkerCapabilityPolicyConfig>(json).is_err(),
                "{json}"
            );
        }
    }

    #[test]
    fn configured_fetch_and_timer_authority_changes_fingerprint() {
        let baseline = WorkerCapabilityPolicyConfig::default()
            .build()
            .unwrap()
            .sha256();
        for json in [
            r#"{"fetch":{"hosts":["127.0.0.1"],"schemes":["http"],"ports":[80],"allow_loopback":true}}"#,
            r#"{"fetch":{"limits":{"max_response_bytes":1}}}"#,
            r#"{"timers":{"max_active_timers":1}}"#,
        ] {
            let config: WorkerCapabilityPolicyConfig = serde_json::from_str(json).unwrap();
            assert_ne!(config.build().unwrap().sha256(), baseline, "{json}");
        }
    }

    #[test]
    fn invalid_capability_limits_and_schemes_fail_closed() {
        for json in [
            r#"{"fetch":{"schemes":["file"]}}"#,
            r#"{"fetch":{"ports":[0]}}"#,
            r#"{"fetch":{"limits":{"max_concurrent_requests":0}}}"#,
            r#"{"timers":{"max_active_timers":0}}"#,
        ] {
            let config: WorkerCapabilityPolicyConfig = serde_json::from_str(json).unwrap();
            assert!(config.build().is_err(), "{json}");
        }
    }
}
