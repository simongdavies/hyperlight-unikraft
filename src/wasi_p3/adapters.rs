// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Fail-closed WASI Preview 3 adapter registration.
//!
//! The registry accepts only locked `@0.3.1` imports. Each accepted import
//! must have an explicit typed adapter; default preview-shim behavior is never
//! used.

use super::async_core::FutureReader;
use crate::broker::{BrokerLimits, BrokerPolicy};
use crate::{Mount, MountLimits};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

pub const WASI_P3_VERSION: &str = "0.3.1";
pub const WASI_P3_RELEASE_COMMIT: &str = "59e48bfe3fae9bf2480eb15abd8f55999eb3b395";

pub type AdapterFuture<T> = FutureReader<Result<T, P3AdapterError>>;

/// Stable guest-visible error categories. Host error text is never carried.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum P3ErrorCategory {
    UnsupportedImport,
    CapabilityDenied,
    InvalidArgument,
    QuotaExceeded,
    NotSupported,
    HostFailure,
    InvalidHandle,
    SizeLimit,
    Timeout,
    Canceled,
}

impl P3ErrorCategory {
    pub fn code(self) -> &'static str {
        match self {
            Self::UnsupportedImport => "unsupported-import",
            Self::CapabilityDenied => "capability-denied",
            Self::InvalidArgument => "invalid-argument",
            Self::QuotaExceeded => "quota-exceeded",
            Self::NotSupported => "not-supported",
            Self::HostFailure => "host-failure",
            Self::InvalidHandle => "invalid-handle",
            Self::SizeLimit => "size-limit",
            Self::Timeout => "timeout",
            Self::Canceled => "canceled",
        }
    }
}

/// Interface-qualified adapter failure with no host diagnostic or secret.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct P3AdapterError {
    interface: String,
    operation: String,
    category: P3ErrorCategory,
}

impl P3AdapterError {
    pub fn new(
        interface: impl Into<String>,
        operation: impl Into<String>,
        category: P3ErrorCategory,
    ) -> Self {
        Self {
            interface: interface.into(),
            operation: operation.into(),
            category,
        }
    }

    pub fn interface(&self) -> &str {
        &self.interface
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn category(&self) -> P3ErrorCategory {
        self.category
    }
}

impl fmt::Display for P3AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}",
            self.interface,
            self.operation,
            self.category.code()
        )
    }
}

impl std::error::Error for P3AdapterError {}

/// Initialization disposition for one WIT import.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportDisposition {
    Allowed,
    Denied,
    Deferred,
}

/// Fixed timezone configuration. Host-local timezone inheritance is absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixedTimezone {
    pub name: String,
    pub utc_offset_seconds: i32,
}

/// Bounded CLI configuration selected by the host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliGrant {
    pub environment: BTreeMap<String, String>,
    pub max_environment_entries: u32,
    pub max_environment_key_bytes: u32,
    pub max_environment_value_bytes: u32,
    pub max_environment_bytes: u64,
    pub max_stdin_bytes: u64,
    pub max_stdout_bytes: u64,
    pub max_stderr_bytes: u64,
    pub allow_exit: bool,
}

impl CliGrant {
    pub fn validate(self) -> Result<Self, P3AdapterError> {
        let environment_bytes = self
            .environment
            .iter()
            .try_fold(0u64, |total, (key, value)| {
                total.checked_add(key.len() as u64 + value.len() as u64 + 2)
            });
        if self.max_stdin_bytes == 0
            || self.max_stdout_bytes == 0
            || self.max_stderr_bytes == 0
            || self.max_environment_entries == 0
            || self.max_environment_key_bytes == 0
            || self.max_environment_value_bytes == 0
            || self.max_environment_bytes == 0
            || self.environment.len() > self.max_environment_entries as usize
            || environment_bytes.is_none_or(|bytes| bytes > self.max_environment_bytes)
            || self.environment.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > self.max_environment_key_bytes as usize
                    || value.len() > self.max_environment_value_bytes as usize
                    || !key
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            })
        {
            return Err(P3AdapterError::new(
                "wasi:cli/imports@0.3.1",
                "configure",
                P3ErrorCategory::InvalidArgument,
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
struct FilesystemGrant {
    preopens: Vec<String>,
}

impl FilesystemGrant {
    fn from_mounts(mounts: &[Mount]) -> Result<Self, P3AdapterError> {
        let mut preopens = Vec::with_capacity(mounts.len());
        for mount in mounts {
            if !mount.guest_path.starts_with('/')
                || mount.guest_path.split('/').any(|segment| segment == "..")
                || !finite_mount_limits(mount.limits)
            {
                return Err(P3AdapterError::new(
                    "wasi:filesystem/preopens@0.3.1",
                    "configure",
                    P3ErrorCategory::InvalidArgument,
                ));
            }
            preopens.push(mount.guest_path.clone());
        }
        Ok(Self { preopens })
    }
}

fn finite_mount_limits(limits: MountLimits) -> bool {
    limits.max_operations.is_some()
        && limits.max_read_bytes.is_some()
        && limits.max_write_bytes.is_some()
}

#[derive(Clone, Debug)]
struct SocketGrant {
    policy: BrokerPolicy,
    limits: BrokerLimits,
}

/// Explicit capability policy for the locked P3 adapters.
#[derive(Clone, Debug)]
pub struct P3Policy {
    system_clock: bool,
    monotonic_clock: bool,
    timezone: Option<FixedTimezone>,
    secure_random: bool,
    deterministic_random_seed: Option<u128>,
    filesystem: Option<FilesystemGrant>,
    sockets: Option<SocketGrant>,
    http_client: bool,
    cli: Option<CliGrant>,
}

impl Default for P3Policy {
    fn default() -> Self {
        Self::deny_all()
    }
}

impl P3Policy {
    /// Production-safe baseline: every capability is absent.
    pub fn deny_all() -> Self {
        Self {
            system_clock: false,
            monotonic_clock: false,
            timezone: None,
            secure_random: false,
            deterministic_random_seed: None,
            filesystem: None,
            sockets: None,
            http_client: false,
            cli: None,
        }
    }

    pub fn with_clocks(
        mut self,
        system: bool,
        monotonic: bool,
        timezone: Option<FixedTimezone>,
    ) -> Result<Self, P3AdapterError> {
        if let Some(timezone) = &timezone
            && (timezone.name.is_empty()
                || timezone.name.len() > 128
                || !timezone
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"/_+-".contains(&byte))
                || !(-86_400..=86_400).contains(&timezone.utc_offset_seconds))
        {
            return Err(P3AdapterError::new(
                "wasi:clocks/timezone@0.3.1",
                "configure",
                P3ErrorCategory::InvalidArgument,
            ));
        }
        self.system_clock = system;
        self.monotonic_clock = monotonic;
        self.timezone = timezone;
        Ok(self)
    }

    pub fn with_secure_random(mut self) -> Self {
        self.secure_random = true;
        self
    }

    /// Enable deterministic insecure random only for a test adapter.
    pub fn with_deterministic_random_for_tests(mut self, seed: u128) -> Self {
        self.deterministic_random_seed = Some(seed);
        self
    }

    /// Grant only host-selected cap-std mounts with finite budgets.
    pub fn with_filesystem(mut self, mounts: &[Mount]) -> Result<Self, P3AdapterError> {
        self.filesystem = Some(FilesystemGrant::from_mounts(mounts)?);
        Ok(self)
    }

    /// Grant client sockets under the shared broker policy and finite quotas.
    pub fn with_sockets(
        mut self,
        policy: BrokerPolicy,
        limits: BrokerLimits,
    ) -> Result<Self, P3AdapterError> {
        limits.validate().map_err(|_| {
            P3AdapterError::new(
                "wasi:sockets/types@0.3.1",
                "configure",
                P3ErrorCategory::InvalidArgument,
            )
        })?;
        self.sockets = Some(SocketGrant { policy, limits });
        Ok(self)
    }

    /// Grant the existing bounded outbound fetch seam.
    pub fn with_http_client(mut self) -> Self {
        self.http_client = true;
        self
    }

    /// Grant bounded CLI streams and an explicit environment map.
    pub fn with_cli(mut self, grant: CliGrant) -> Result<Self, P3AdapterError> {
        self.cli = Some(grant.validate()?);
        Ok(self)
    }

    /// Guest-visible preopen names. Host paths are intentionally absent.
    pub fn preopens(&self) -> &[String] {
        self.filesystem
            .as_ref()
            .map_or(&[], |grant| grant.preopens.as_slice())
    }

    /// Shared socket contract retained by the adapter implementation.
    pub fn socket_contract(&self) -> Option<(&BrokerPolicy, BrokerLimits)> {
        self.sockets
            .as_ref()
            .map(|grant| (&grant.policy, grant.limits))
    }

    pub fn cli_grant(&self) -> Option<&CliGrant> {
        self.cli.as_ref()
    }
}

/// P3 clock implementation. Waits return a real canonical future.
pub trait ClocksAdapter: Send + Sync {
    fn system_now(&self) -> Result<Duration, P3AdapterError>;
    fn monotonic_now(&self) -> Result<Duration, P3AdapterError>;
    fn wait_until(&self, deadline: Duration) -> AdapterFuture<()>;
}

/// P3 random implementation.
pub trait RandomAdapter: Send + Sync {
    fn secure_bytes(&self, len: u64) -> AdapterFuture<Vec<u8>>;
    fn deterministic_bytes(&self, len: u64, seed: u128) -> AdapterFuture<Vec<u8>>;
}

/// P3 filesystem implementation over host-selected preopens.
pub trait FilesystemAdapter: Send + Sync {
    fn read(&self, preopen: &str, path: &str, offset: u64, len: u64) -> AdapterFuture<Vec<u8>>;
    fn write(&self, preopen: &str, path: &str, offset: u64, bytes: Vec<u8>) -> AdapterFuture<()>;
}

/// P3 client socket implementation over the shared broker contract.
pub trait SocketsAdapter: Send + Sync {
    fn resolve(&self, name: &str) -> AdapterFuture<Vec<String>>;
    fn connect_tcp(&self, host: &str, port: u16) -> AdapterFuture<u64>;
    fn connect_tls(&self, host: &str, port: u16) -> AdapterFuture<u64>;
    fn send_tcp(&self, handle: u64, bytes: Vec<u8>) -> AdapterFuture<u64>;
    fn receive_tcp(&self, handle: u64, max_bytes: u64) -> AdapterFuture<Vec<u8>>;
    fn close(&self, handle: u64) -> AdapterFuture<()>;
}

/// P3 outbound HTTP implementation over the existing bounded fetch seam.
pub trait HttpAdapter: Send + Sync {
    fn request(&self, request: Vec<u8>) -> AdapterFuture<Vec<u8>>;
}

/// P3 CLI stream implementation. Environment values come from [`CliGrant`].
pub trait CliAdapter: Send + Sync {
    fn read_stdin(&self, max_bytes: u64) -> AdapterFuture<Vec<u8>>;
    fn write_stdout(&self, bytes: Vec<u8>) -> AdapterFuture<()>;
    fn write_stderr(&self, bytes: Vec<u8>) -> AdapterFuture<()>;
    fn run(&self) -> AdapterFuture<u8>;
    fn exit(&self, status: u8) -> AdapterFuture<()>;
}

/// Explicit typed adapter set used to validate every generated WIT import.
pub struct P3Adapters {
    policy: P3Policy,
    clocks: Option<Arc<dyn ClocksAdapter>>,
    random: Option<Arc<dyn RandomAdapter>>,
    filesystem: Option<Arc<dyn FilesystemAdapter>>,
    sockets: Option<Arc<dyn SocketsAdapter>>,
    http: Option<Arc<dyn HttpAdapter>>,
    cli: Option<Arc<dyn CliAdapter>>,
}

impl P3Adapters {
    pub fn deny_all() -> Self {
        Self {
            policy: P3Policy::deny_all(),
            clocks: None,
            random: None,
            filesystem: None,
            sockets: None,
            http: None,
            cli: None,
        }
    }

    pub fn new(policy: P3Policy) -> Self {
        Self {
            policy,
            clocks: None,
            random: None,
            filesystem: None,
            sockets: None,
            http: None,
            cli: None,
        }
    }

    pub fn with_clocks(mut self, adapter: impl ClocksAdapter + 'static) -> Self {
        self.clocks = Some(Arc::new(adapter));
        self
    }

    pub fn with_random(mut self, adapter: impl RandomAdapter + 'static) -> Self {
        self.random = Some(Arc::new(adapter));
        self
    }

    pub fn with_filesystem(mut self, adapter: impl FilesystemAdapter + 'static) -> Self {
        self.filesystem = Some(Arc::new(adapter));
        self
    }

    pub fn with_sockets(mut self, adapter: impl SocketsAdapter + 'static) -> Self {
        self.sockets = Some(Arc::new(adapter));
        self
    }

    pub fn with_http(mut self, adapter: impl HttpAdapter + 'static) -> Self {
        self.http = Some(Arc::new(adapter));
        self
    }

    pub fn with_cli(mut self, adapter: impl CliAdapter + 'static) -> Self {
        self.cli = Some(Arc::new(adapter));
        self
    }

    pub fn policy(&self) -> &P3Policy {
        &self.policy
    }

    /// Validate the complete generated import list before instantiation.
    ///
    /// Unknown, unversioned, or P3.2 imports are hard initialization errors.
    pub fn validate_imports<'a>(
        &self,
        imports: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), P3AdapterError> {
        for import in imports {
            match self.import_disposition(import)? {
                ImportDisposition::Allowed => {}
                ImportDisposition::Denied => {
                    return Err(P3AdapterError::new(
                        import,
                        "initialize",
                        P3ErrorCategory::CapabilityDenied,
                    ));
                }
                ImportDisposition::Deferred => {
                    return Err(P3AdapterError::new(
                        import,
                        "initialize",
                        P3ErrorCategory::NotSupported,
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn import_disposition(&self, import: &str) -> Result<ImportDisposition, P3AdapterError> {
        if !import.ends_with("@0.3.1") {
            return Err(P3AdapterError::new(
                import,
                "initialize",
                P3ErrorCategory::UnsupportedImport,
            ));
        }
        let disposition = match import {
            "wasi:clocks/system-clock@0.3.1" => allowed(self.policy.system_clock, &self.clocks),
            "wasi:clocks/monotonic-clock@0.3.1" => {
                allowed(self.policy.monotonic_clock, &self.clocks)
            }
            "wasi:clocks/timezone@0.3.1" => allowed(self.policy.timezone.is_some(), &self.clocks),
            "wasi:clocks/types@0.3.1" => allowed(
                self.policy.system_clock
                    || self.policy.monotonic_clock
                    || self.policy.timezone.is_some(),
                &self.clocks,
            ),
            "wasi:random/random@0.3.1" => allowed(self.policy.secure_random, &self.random),
            "wasi:random/insecure@0.3.1" | "wasi:random/insecure-seed@0.3.1" => allowed(
                self.policy.deterministic_random_seed.is_some(),
                &self.random,
            ),
            "wasi:filesystem/preopens@0.3.1" | "wasi:filesystem/types@0.3.1" => {
                allowed(self.policy.filesystem.is_some(), &self.filesystem)
            }
            "wasi:sockets/ip-name-lookup@0.3.1" | "wasi:sockets/types@0.3.1" => {
                allowed(self.policy.sockets.is_some(), &self.sockets)
            }
            "wasi:http/types@0.3.1" | "wasi:http/client@0.3.1" => {
                allowed(self.policy.http_client, &self.http)
            }
            "wasi:http/handler@0.3.1" => ImportDisposition::Deferred,
            "wasi:cli/types@0.3.1"
            | "wasi:cli/environment@0.3.1"
            | "wasi:cli/stdin@0.3.1"
            | "wasi:cli/stdout@0.3.1"
            | "wasi:cli/stderr@0.3.1"
            | "wasi:cli/run@0.3.1" => allowed(self.policy.cli.is_some(), &self.cli),
            "wasi:cli/exit@0.3.1" => allowed(
                self.policy
                    .cli
                    .as_ref()
                    .is_some_and(|grant| grant.allow_exit),
                &self.cli,
            ),
            "wasi:cli/terminal-input@0.3.1"
            | "wasi:cli/terminal-output@0.3.1"
            | "wasi:cli/terminal-stdin@0.3.1"
            | "wasi:cli/terminal-stdout@0.3.1"
            | "wasi:cli/terminal-stderr@0.3.1" => ImportDisposition::Denied,
            _ if is_forbidden_extension(import) => ImportDisposition::Denied,
            _ => {
                return Err(P3AdapterError::new(
                    import,
                    "initialize",
                    P3ErrorCategory::UnsupportedImport,
                ));
            }
        };
        Ok(disposition)
    }

    /// Inbound listen/accept is intentionally unavailable in this lane.
    pub fn deny_listener(&self, operation: &str) -> P3AdapterError {
        P3AdapterError::new(
            "wasi:sockets/types@0.3.1",
            operation,
            P3ErrorCategory::CapabilityDenied,
        )
    }

    /// Raw devices and terminals are intentionally unavailable.
    pub fn deny_raw_device(&self, operation: &str) -> P3AdapterError {
        P3AdapterError::new(
            "wasi:cli/terminal@0.3.1",
            operation,
            P3ErrorCategory::CapabilityDenied,
        )
    }

    /// HTTP service/middleware binding awaits a frozen host contract.
    pub fn defer_http_service(&self, operation: &str) -> P3AdapterError {
        P3AdapterError::new(
            "wasi:http/handler@0.3.1",
            operation,
            P3ErrorCategory::NotSupported,
        )
    }

    /// UDP operation tags are not frozen in the locked contract.
    pub fn defer_udp(&self, operation: &str) -> P3AdapterError {
        P3AdapterError::new(
            "wasi:sockets/types@0.3.1",
            operation,
            P3ErrorCategory::NotSupported,
        )
    }
}

fn allowed<T>(granted: bool, adapter: &Option<Arc<T>>) -> ImportDisposition
where
    T: ?Sized,
{
    if granted && adapter.is_some() {
        ImportDisposition::Allowed
    } else {
        ImportDisposition::Denied
    }
}

fn is_forbidden_extension(import: &str) -> bool {
    [
        "terminal",
        "raw-device",
        "process",
        "shell",
        "spawn",
        "listener",
        "secrets",
    ]
    .iter()
    .any(|token| import.contains(token))
}
