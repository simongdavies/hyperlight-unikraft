// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{
    Error, FetchBroker, InvocationCancellation, InvocationRequest, InvocationResponse,
    QueueRequest, QueueResponse, RequestEnvelope, ResponseEnvelope, Result, ScheduledRequest,
    ScheduledResponse, SnapshotBinding, TimerLimits, VerifiedSnapshot, WorkerBinding, WorkerBundle,
    WorkerVersionId, snapshot::kernel_for_rootfs, timer::TimerBroker,
};
use crate::{AppSandbox, Mount, MountLimits, Yield, broker_runtime::BrokerRuntime};
use hyperlight_host::func::Registerable;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct InitializationProfile {
    pub binding_ms: f64,
    pub assemble_ms: f64,
    pub boot_ms: f64,
    pub init_ms: f64,
    pub snapshot_ms: f64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ExecutionProfile {
    pub ready_wait_ms: f64,
    pub admission_wait_ms: f64,
    pub ready_owner_wait_ms: f64,
    pub replenishment_policy_wait_ms: f64,
    pub replenishment_wait_ms: f64,
    pub replenishment_restore_ms: f64,
    pub snapshot_restore_ms: f64,
    pub request_setup_ms: f64,
    pub guest_execution_ms: f64,
    pub response_finish_ms: f64,
    pub vm_teardown_ms: f64,
    pub total_ms: f64,
}

#[derive(Debug)]
pub struct InitializationFailure {
    pub stage: &'static str,
    pub message: String,
    pub profile: InitializationProfile,
}

impl std::fmt::Display for InitializationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} failed: {}", self.stage, self.message)
    }
}

impl std::error::Error for InitializationFailure {}

#[derive(Default)]
struct RequestState {
    id: Option<String>,
    response: Option<String>,
    violation: Option<String>,
    bytes: Vec<u8>,
}

#[derive(Clone, Default)]
struct Responses(Arc<Mutex<RequestState>>, super::ingress::IngressSession);

fn read_entropy(amount: u64) -> hyperlight_host::Result<Vec<u8>> {
    use ring::rand::SecureRandom;
    let amount = usize::try_from(amount)
        .ok()
        .filter(|amount| (1..=16_384).contains(amount))
        .ok_or_else(|| hyperlight_host::new_error!("entropy read must be 1..16384 bytes"))?;
    let mut bytes = vec![0; amount];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| hyperlight_host::new_error!("host OS entropy unavailable"))?;
    Ok(bytes)
}

fn register_entropy(target: &mut impl Registerable) -> Result<()> {
    target.register_host_function("WorkerdEntropyV1Read", read_entropy)?;
    Ok(())
}

fn register_provider_websockets(
    target: &mut impl Registerable,
    runtime: &BrokerRuntime,
) -> Result<()> {
    if runtime.has_provider_websockets() {
        for function in [
            "WorkerdWebSocketV1Open",
            "WorkerdWebSocketV1Send",
            "WorkerdWebSocketV1Receive",
            "WorkerdWebSocketV1Close",
        ] {
            let runtime = runtime.clone();
            target.register_host_function(
                function,
                move |payload: String| -> hyperlight_host::Result<String> {
                    runtime
                        .dispatch_provider_websocket(function, &payload)
                        .map_err(|error| hyperlight_host::new_error!("{error}"))
                },
            )?;
        }
    }
    Ok(())
}

fn drive_control_call<T: serde::de::DeserializeOwned>(
    app: &mut AppSandbox,
    responses: &Responses,
    sessions: (super::fetch::FetchSession, super::timer::TimerSession),
    function: &str,
    request_id: &str,
    deadline: Instant,
    cancellation: InvocationCancellation,
) -> Result<T> {
    let request = super::ControlRequest::new(request_id)?;
    let encoded = serde_json::to_string(&request)?;
    responses.begin(request_id)?;
    let result = with_watchdog_cancellable(
        app,
        deadline,
        Some(sessions),
        cancellation,
        |app, deadline| {
            app.resume()?;
            drive_call(app, function, encoded, deadline)
        },
    );
    match result {
        Ok(()) => super::control::decode(responses.finish_raw()?.as_bytes()),
        Err(error) => {
            responses.clear()?;
            match error {
                Error::Guest(crate::Error::CallFailed { status }) => Err(Error::State(format!(
                    "guest control {function} failed with status {status}"
                ))),
                error => Err(error),
            }
        }
    }
}

fn validate_guest_completion(
    app: &mut AppSandbox,
    responses: &Responses,
    sessions: (super::fetch::FetchSession, super::timer::TimerSession),
    deadline: Instant,
    cancellation: InvocationCancellation,
) -> Result<()> {
    let capabilities: super::GuestCapabilities = drive_control_call(
        app,
        responses,
        sessions.clone(),
        "runtime_capabilities",
        "completion-capabilities",
        deadline,
        cancellation.clone(),
    )?;
    capabilities.validate("completion-capabilities", "safe-point-v1")?;
    capabilities.validate("completion-capabilities", "tracked-work-drain-v1")?;
    let safe: super::SafePoint = drive_control_call(
        app,
        responses,
        sessions,
        "checkpoint",
        "invocation-completion",
        deadline,
        cancellation,
    )?;
    safe.validate("invocation-completion")
}

impl Responses {
    fn begin(&self, id: &str) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))?;
        if state.id.is_some() {
            return Err(Error::State("request already active".into()));
        }
        *state = RequestState {
            id: Some(id.into()),
            ..Default::default()
        };
        Ok::<(), Error>(())
    }

    fn submit(&self, json: &str) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))?;
        let result = (|| {
            let envelope: serde_json::Value = serde_json::from_str(json)?;
            let request_id = envelope
                .as_object()
                .and_then(|object| object.get("request_id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| Error::Protocol("response has no request ID".into()))?;
            if state.id.as_deref() != Some(request_id) {
                return Err(Error::State(
                    "stale response ID or no active request".into(),
                ));
            }

            if state.response.is_some() {
                return Err(Error::State("duplicate response".into()));
            }
            state.response = Some(json.into());
            Ok(())
        })();
        if let Err(error) = &result {
            state.violation = Some(error.to_string());
        }
        result
    }

    fn output(&self, chunk: &str) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))?;
        if state.id.is_none() {
            return Err(Error::State("output with no active request".into()));
        }
        if let Some(error) = &state.violation {
            return Err(Error::State(error.clone()));
        }
        let failure = if state.response.is_some() {
            Some("output after response terminator")
        } else if chunk.len() > super::MAX_ENVELOPE_BYTES + 2 - state.bytes.len() {
            Some("response stream exceeds size limit")
        } else {
            None
        };
        if let Some(error) = failure {
            state.violation = Some(error.into());
            return Err(Error::State(error.into()));
        }
        state.bytes.extend_from_slice(chunk.as_bytes());
        if let Some(end) = state.bytes.iter().position(|&b| b == b'\n') {
            // v0.14's console turns a guest LF into CRLF, including across
            // HostPrint chunks. Strip exactly that terminator, never trim().
            let json_end = end.saturating_sub(1);
            let error = if end != state.bytes.len() - 1 {
                Some("response has a suffix or additional line")
            } else if state.bytes.get(json_end) != Some(&b'\r') {
                Some("response terminator is not CRLF")
            } else if state.bytes.first() != Some(&b'{') {
                Some("response has a non-JSON prefix")
            } else if state.bytes.get(json_end.wrapping_sub(1)) != Some(&b'}') {
                Some("response has content after the JSON object")
            } else {
                None
            };
            if let Some(error) = error {
                state.violation = Some(error.into());
                return Err(Error::State(error.into()));
            }
            let json = String::from_utf8(std::mem::take(&mut state.bytes))
                .map_err(|e| Error::Protocol(e.to_string()))?;
            drop(state);
            return self.submit(&json[..json_end]);
        }
        Ok(())
    }

    fn finish_raw(&self) -> Result<String> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))?;
        let completed = std::mem::take(&mut *state);
        if let Some(error) = completed.violation {
            return Err(Error::State(error));
        }
        completed
            .response
            .ok_or_else(|| Error::State("guest returned without a response".into()))
    }

    fn finish_fetch(&self) -> Result<ResponseEnvelope> {
        ResponseEnvelope::from_json(self.finish_raw()?.as_bytes())
    }

    #[cfg(test)]
    fn finish(&self) -> Result<ResponseEnvelope> {
        self.finish_fetch()
    }

    fn finish_scheduled(&self) -> Result<ScheduledResponse> {
        ScheduledResponse::from_json(self.finish_raw()?.as_bytes())
    }

    fn finish_queue(&self) -> Result<QueueResponse> {
        QueueResponse::from_json(self.finish_raw()?.as_bytes())
    }

    fn finish_invocation(&self, function: &str) -> Result<InvocationResponse> {
        match function {
            "fetch" => self.finish_fetch().map(InvocationResponse::Fetch),
            "scheduled" => self.finish_scheduled().map(InvocationResponse::Scheduled),
            "queue" => self.finish_queue().map(InvocationResponse::Queue),
            _ => Err(Error::State("unsupported invocation response kind".into())),
        }
    }

    fn finish_stream(&self) -> Result<()> {
        let completed = std::mem::take(
            &mut *self
                .0
                .lock()
                .map_err(|_| Error::State("response mutex poisoned".into()))?,
        );
        if completed.violation.is_some()
            || completed.response.is_some()
            || !completed.bytes.is_empty()
        {
            return Err(Error::Protocol(
                "streaming invocation emitted buffered stdout response".into(),
            ));
        }
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        *self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))? = RequestState::default();
        Ok(())
    }

    fn register(&self, target: &mut impl Registerable) -> Result<()> {
        self.1.register(target)?;
        target.register_host_function("ReadStdin", || -> hyperlight_host::Result<String> {
            Err(hyperlight_host::new_error!(
                "workerd has no stdin capability"
            ))
        })?;
        let bytes = Arc::new(Mutex::new(0usize));
        let collector = self.clone();
        target.register_host_function(
            "HostPrint",
            move |message: String| -> hyperlight_host::Result<i32> {
                let active = collector
                    .0
                    .lock()
                    .map_err(|_| hyperlight_host::new_error!("response state poisoned"))?
                    .id
                    .is_some();
                if active {
                    return match collector.output(&message) {
                        Ok(()) => Ok(message.len() as i32),
                        Err(error) => {
                            tracing::warn!(%error, "workerd response rejected");
                            Ok(-1)
                        }
                    };
                }
                let mut bytes = bytes
                    .lock()
                    .map_err(|_| hyperlight_host::new_error!("console state poisoned"))?;
                *bytes = bytes.saturating_add(message.len());
                if *bytes > 64 * 1024 {
                    return Err(hyperlight_host::new_error!(
                        "workerd console limit exceeded"
                    ));
                }
                tracing::debug!(%message, "workerd console");
                Ok(message.len() as i32)
            },
        )?;
        Ok(())
    }
}

/// One immutable Worker version; no reassignment or raw sandbox escape hatch.
/// Every request starts from the initialized snapshot. After each call the
/// VM is dropped, including after a kill: a killed Hyperlight VM is not reused.
#[derive(Clone)]
pub struct WorkerVersionSandbox {
    image: VerifiedSnapshot,
    fetch_broker: FetchBroker,
    timer_broker: TimerBroker,
    storage_policy: StoragePolicy,
    broker_runtime: Option<BrokerRuntime>,
    capability_policy_sha256: String,
    logical_policy: Option<super::LogicalBindingsPolicy>,
    allow_instance_checkpoint: bool,
}

const STORAGE_GUEST_ROOT: &str = "/mnt/workerd-storage";
const MAX_STORAGE_BINDINGS: usize = 8;

#[derive(Clone, Debug)]
pub struct StorageBinding {
    name: String,
    host_path: std::path::PathBuf,
    readonly: bool,
    limits: MountLimits,
}

impl StorageBinding {
    pub fn read_only(
        name: impl Into<String>,
        host_path: impl AsRef<Path>,
        limits: MountLimits,
    ) -> Result<Self> {
        Self::new(name.into(), host_path.as_ref(), true, limits)
    }

    pub fn read_write(
        name: impl Into<String>,
        host_path: impl AsRef<Path>,
        limits: MountLimits,
    ) -> Result<Self> {
        Self::new(name.into(), host_path.as_ref(), false, limits)
    }

    fn new(name: String, host_path: &Path, readonly: bool, limits: MountLimits) -> Result<Self> {
        validate_storage_name(&name)?;
        validate_storage_limits(limits)?;
        let host_path = std::fs::canonicalize(host_path)?;
        if !host_path.is_dir() {
            return Err(Error::State(format!(
                "storage binding {name:?} host path is not a directory"
            )));
        }
        if host_path.to_str().is_none() {
            return Err(Error::State(format!(
                "storage binding {name:?} host path must be valid UTF-8"
            )));
        }
        Ok(Self {
            name,
            host_path,
            readonly,
            limits,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn guest_path(&self) -> String {
        format!("{STORAGE_GUEST_ROOT}/{}", self.name)
    }

    pub fn host_path(&self) -> &Path {
        &self.host_path
    }

    pub fn readonly(&self) -> bool {
        self.readonly
    }

    pub fn limits(&self) -> MountLimits {
        self.limits
    }

    fn mount(&self) -> Mount {
        let mount = if self.readonly {
            Mount::ro(&self.host_path, self.guest_path())
        } else {
            Mount::rw(&self.host_path, self.guest_path())
        };
        mount.with_limits(self.limits)
    }
}

#[derive(Clone, Debug, Default)]
pub struct StoragePolicy {
    bindings: Vec<StorageBinding>,
}

impl StoragePolicy {
    pub fn denied() -> Self {
        Self::default()
    }

    pub fn new(bindings: impl IntoIterator<Item = StorageBinding>) -> Result<Self> {
        let mut bindings: Vec<_> = bindings.into_iter().collect();
        bindings.sort_by(|left, right| left.name.cmp(&right.name));
        if bindings.len() > MAX_STORAGE_BINDINGS {
            return Err(Error::State(format!(
                "storage policy supports at most {MAX_STORAGE_BINDINGS} bindings"
            )));
        }
        let mut names = HashSet::with_capacity(bindings.len());
        for binding in &bindings {
            if !names.insert(binding.name.clone()) {
                return Err(Error::State(format!(
                    "duplicate storage binding {:?}",
                    binding.name
                )));
            }
        }
        Ok(Self { bindings })
    }

    pub fn bindings(&self) -> &[StorageBinding] {
        &self.bindings
    }

    fn mounts(&self) -> Vec<Mount> {
        self.bindings.iter().map(StorageBinding::mount).collect()
    }

    fn executor_bindings(&self) -> impl Iterator<Item = (&str, bool)> {
        self.bindings
            .iter()
            .map(|binding| (binding.name.as_str(), binding.readonly))
    }

    pub(super) fn sha256(&self) -> String {
        let mut entries: Vec<_> = self
            .bindings
            .iter()
            .map(|binding| {
                format!(
                    "{}\0{}\0{}\0{:?}\0{:?}\0{:?}",
                    binding.name,
                    binding.host_path.display(),
                    if binding.readonly { "ro" } else { "rw" },
                    binding.limits.max_operations,
                    binding.limits.max_read_bytes,
                    binding.limits.max_write_bytes
                )
            })
            .collect();
        entries.sort();
        let mut digest = Sha256::new();
        digest.update(b"workerd-storage-policy:v1\0");
        for entry in entries {
            digest.update(entry.as_bytes());
            digest.update(b"\0");
        }
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

#[derive(Clone)]
pub struct WorkerCapabilityPolicy {
    fetch_broker: FetchBroker,
    timer_limits: TimerLimits,
    storage_policy: StoragePolicy,
    broker_runtime: Option<BrokerRuntime>,
    bindings: Vec<WorkerBinding>,
    logical_policy: Option<super::LogicalBindingsPolicy>,
}

impl WorkerCapabilityPolicy {
    pub fn new(
        fetch_broker: FetchBroker,
        timer_limits: TimerLimits,
        storage_policy: StoragePolicy,
    ) -> Self {
        Self {
            fetch_broker,
            timer_limits: timer_limits.clone(),
            storage_policy,
            broker_runtime: None,
            bindings: Vec::new(),
            logical_policy: None,
        }
    }

    pub fn with_broker_runtime(
        mut self,
        broker_runtime: BrokerRuntime,
        bindings: impl IntoIterator<Item = WorkerBinding>,
    ) -> Result<Self> {
        if self.logical_policy.is_some() {
            return Err(Error::State(
                "cannot mix typed reconstruction policy with opaque callbacks".into(),
            ));
        }
        let mut bindings: Vec<_> = bindings.into_iter().collect();
        bindings.sort_by(|left, right| left.name.cmp(&right.name));
        if bindings.is_empty() {
            return Err(Error::State(
                "broker runtime requires at least one Workerd binding".into(),
            ));
        }
        if !broker_runtime.has_logical() {
            return Err(Error::State(
                "Workerd bindings require a logical broker runtime".into(),
            ));
        }
        let mut names = HashSet::new();
        for binding in &bindings {
            if !names.insert(binding.name.clone()) {
                return Err(Error::State(format!(
                    "duplicate Workerd binding {:?}",
                    binding.name
                )));
            }
        }
        self.broker_runtime = Some(broker_runtime);
        self.bindings = bindings;
        Ok(self)
    }

    pub fn with_logical_bindings(mut self, policy: super::LogicalBindingsPolicy) -> Result<Self> {
        if self.broker_runtime.is_some() {
            return Err(Error::State(
                "cannot mix opaque runtime with reconstructable logical policy".into(),
            ));
        }
        self.bindings = policy.worker_bindings();
        self.logical_policy = Some(policy);
        Ok(self)
    }

    pub(super) fn sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"workerd-capability-policy:v3\0");
        digest.update(self.fetch_broker.policy_identity().as_bytes());
        digest.update(b"\0");
        digest.update(self.timer_limits.max_active_timers.to_le_bytes());
        digest.update(self.timer_limits.max_unreleased_handles.to_le_bytes());
        digest.update(self.storage_policy.sha256().as_bytes());
        if let Some(policy) = &self.logical_policy {
            digest.update(b"\0logical-policy\0");
            digest.update(policy.authority().as_bytes());
        }
        if let Some(runtime) = &self.broker_runtime {
            // Opaque legacy callbacks are intentionally process-bound. They
            // cannot be persisted/reconstructed as an equivalent host policy.
            digest.update(b"\0opaque-runtime\0");
            digest.update(runtime.opaque_authority_identity().as_bytes());
            digest.update([
                u8::from(runtime.has_network()),
                u8::from(runtime.has_logical()),
            ]);
        }
        digest.update(b"\0");
        for binding in &self.bindings {
            digest.update(binding.name.as_bytes());
            digest.update(b"\0");
            digest.update(format!("{:?}", binding.kind).as_bytes());
            digest.update(b"\0");
        }
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

impl Default for WorkerCapabilityPolicy {
    fn default() -> Self {
        Self::new(
            FetchBroker::denied(),
            TimerLimits::default(),
            StoragePolicy::denied(),
        )
    }
}

fn validate_storage_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 32
        || !name.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || (index != 0 && (byte.is_ascii_digit() || byte == b'-'))
        })
    {
        return Err(Error::State(
            "storage binding names must be 1-32 lowercase ASCII characters, start with a letter, and contain only letters, digits, or '-'".into(),
        ));
    }
    Ok(())
}

fn validate_storage_limits(limits: MountLimits) -> Result<()> {
    if [
        limits.max_operations,
        limits.max_read_bytes,
        limits.max_write_bytes,
    ]
    .into_iter()
    .flatten()
    .any(|limit| limit == 0)
    {
        return Err(Error::State(
            "storage limits must be nonzero when configured".into(),
        ));
    }
    Ok(())
}

pub(super) struct RestoredWorkerVersionSandbox {
    app: AppSandbox,
    responses: Responses,
    broker_runtime: Option<BrokerRuntime>,
    use_invoke_v2: bool,
    reset_broker_per_invocation: bool,
    fetch_session: super::fetch::FetchSession,
    timer_session: super::timer::TimerSession,
}

impl WorkerVersionSandbox {
    /// Boot trusted artifacts, invoke `init(canonical_bundle_json)` once and snapshot.
    /// `timeout` bounds guest initialization, not filesystem/hypervisor creation.
    pub fn initialize(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
    ) -> Result<Self> {
        Self::initialize_profiled(bundle, rootfs, executor, scratch_mb, timeout)
            .map(|(worker, _)| worker)
            .map_err(|failure| Error::State(failure.to_string()))
    }

    pub fn initialize_profiled(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
    ) -> std::result::Result<(Self, InitializationProfile), InitializationFailure> {
        Self::initialize_profiled_with_fetch(
            bundle,
            rootfs,
            executor,
            scratch_mb,
            timeout,
            FetchBroker::denied(),
        )
    }

    pub fn initialize_with_fetch(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
        fetch_broker: FetchBroker,
    ) -> Result<Self> {
        Self::initialize_profiled_with_fetch(
            bundle,
            rootfs,
            executor,
            scratch_mb,
            timeout,
            fetch_broker,
        )
        .map(|(worker, _)| worker)
        .map_err(|failure| Error::State(failure.to_string()))
    }

    pub fn initialize_profiled_with_fetch(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
        fetch_broker: FetchBroker,
    ) -> std::result::Result<(Self, InitializationProfile), InitializationFailure> {
        Self::initialize_profiled_with_capabilities(
            bundle,
            rootfs,
            executor,
            scratch_mb,
            timeout,
            fetch_broker,
            TimerLimits::default(),
        )
    }

    pub fn initialize_with_capabilities(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
        fetch_broker: FetchBroker,
        timer_limits: TimerLimits,
    ) -> Result<Self> {
        Self::initialize_profiled_with_capabilities(
            bundle,
            rootfs,
            executor,
            scratch_mb,
            timeout,
            fetch_broker,
            timer_limits,
        )
        .map(|(worker, _)| worker)
        .map_err(|failure| Error::State(failure.to_string()))
    }

    pub fn initialize_profiled_with_capabilities(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
        fetch_broker: FetchBroker,
        timer_limits: TimerLimits,
    ) -> std::result::Result<(Self, InitializationProfile), InitializationFailure> {
        Self::initialize_profiled_with_policy(
            bundle,
            rootfs,
            executor,
            scratch_mb,
            timeout,
            WorkerCapabilityPolicy::new(fetch_broker, timer_limits, StoragePolicy::denied()),
        )
    }

    pub fn initialize_with_policy(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
        policy: WorkerCapabilityPolicy,
    ) -> Result<Self> {
        Self::initialize_profiled_with_policy(bundle, rootfs, executor, scratch_mb, timeout, policy)
            .map(|(worker, _)| worker)
            .map_err(|failure| Error::State(failure.to_string()))
    }

    pub fn initialize_profiled_with_policy(
        bundle: WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        scratch_mb: usize,
        timeout: Duration,
        policy: WorkerCapabilityPolicy,
    ) -> std::result::Result<(Self, InitializationProfile), InitializationFailure> {
        let WorkerCapabilityPolicy {
            fetch_broker,
            timer_limits,
            storage_policy,
            broker_runtime,
            bindings,
            logical_policy,
        } = policy;
        let policy_sha256 = WorkerCapabilityPolicy {
            fetch_broker: fetch_broker.clone(),
            timer_limits: timer_limits.clone(),
            storage_policy: storage_policy.clone(),
            broker_runtime: broker_runtime.clone(),
            bindings: bindings.clone(),
            logical_policy: logical_policy.clone(),
        }
        .sha256();
        let mut profile = InitializationProfile::default();
        let timer_broker = TimerBroker::new(timer_limits)
            .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        if scratch_mb == 0 || scratch_mb.checked_mul(1024 * 1024).is_none() {
            return Err(InitializationFailure {
                stage: "assemble",
                message: "invalid scratch memory size".into(),
                profile,
            });
        }
        let started = Instant::now();
        let kernel = kernel_for_rootfs(rootfs.as_ref())
            .map_err(|error| Self::initialization_failure("binding", error, &profile))?;
        let bundle_reader = super::bundle::BundleReader::new(&bundle)
            .map_err(|error| Self::initialization_failure("binding", error, &profile))?;
        let init_json = if bundle.protocol_version != super::PACKAGE_PROTOCOL_VERSION
            && storage_policy.bindings().is_empty()
            && bindings.is_empty()
        {
            bundle.to_canonical_json()
        } else {
            bundle.to_executor_init_json_with_bindings(
                storage_policy.executor_bindings(),
                bindings.iter(),
            )
        }
        .map_err(|error| Self::initialization_failure("binding", error, &profile))?;
        let binding = SnapshotBinding::from_artifacts_with_policy(
            &bundle,
            &rootfs,
            executor,
            policy_sha256.clone(),
        )
        .map_err(|error| Self::initialization_failure("binding", error, &profile))?;
        profile.binding_ms = Self::elapsed_ms(started);
        let responses = Responses::default();
        let started = Instant::now();
        let (mut uninitialized, config) = crate::assemble_sandbox_with_embedded_kernel(
            kernel,
            &Some(rootfs.as_ref().into()),
            &Some("/bin/workerd-executor".into()),
            scratch_mb,
            storage_policy.mounts(),
            None,
            None,
        )
        .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        responses
            .register(&mut uninitialized)
            .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        register_entropy(&mut uninitialized)
            .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        bundle_reader
            .register(&mut uninitialized)
            .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        let init_deadline =
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| InitializationFailure {
                    stage: "assemble",
                    message: "timeout too large".into(),
                    profile: profile.clone(),
                })?;
        fetch_broker
            .register(&mut uninitialized, fetch_broker.session(init_deadline))
            .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        timer_broker
            .register(&mut uninitialized, timer_broker.session())
            .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        profile.assemble_ms = Self::elapsed_ms(started);
        // Evolve's boot is trusted startup. The timed init call below is where
        // the executor loads the Worker; untrusted code must not run at boot.
        let started = Instant::now();
        let sandbox = uninitialized
            .evolve()
            .map_err(|error| Self::initialization_failure("boot", error, &profile))?;
        let boot = config
            .absorb()
            .map_err(|error| Self::initialization_failure("boot", error, &profile))?;
        if matches!(boot, Yield::Exited { .. }) {
            return Err(InitializationFailure {
                stage: "boot",
                message: "executor exited during boot".into(),
                profile,
            });
        }
        profile.boot_ms = Self::elapsed_ms(started);
        let mut app = AppSandbox {
            sandbox,
            config,
            exited: None,
            pending: None,
            restore_poisoned: false,
        };
        if !app.has_driver() {
            return Err(InitializationFailure {
                stage: "boot",
                message: "executor did not open /dev/hlcall".into(),
                profile,
            });
        }
        let started = Instant::now();
        timed_call(&mut app, "init", init_json, timeout)
            .map_err(|error| Self::initialization_failure("init", error, &profile))?;
        bundle_reader.close();
        profile.init_ms = Self::elapsed_ms(started);
        let started = Instant::now();
        let snapshot = app
            .snapshot()
            .map_err(|error| Self::initialization_failure("snapshot", error, &profile))?;
        profile.snapshot_ms = Self::elapsed_ms(started);
        let image = VerifiedSnapshot::initialized(snapshot, binding);
        Ok((
            Self {
                image,
                fetch_broker,
                timer_broker,
                storage_policy,
                broker_runtime,
                capability_policy_sha256: policy_sha256,
                logical_policy,
                allow_instance_checkpoint: false,
            },
            profile,
        ))
    }

    fn elapsed_ms(started: Instant) -> f64 {
        started.elapsed().as_secs_f64() * 1000.0
    }

    fn initialization_failure(
        stage: &'static str,
        error: impl std::fmt::Display,
        profile: &InitializationProfile,
    ) -> InitializationFailure {
        InitializationFailure {
            stage,
            message: error.to_string(),
            profile: profile.clone(),
        }
    }

    pub fn from_verified_snapshot(image: VerifiedSnapshot) -> Self {
        let capability_policy_sha256 = WorkerCapabilityPolicy::default().sha256();
        Self {
            image,
            fetch_broker: FetchBroker::denied(),
            timer_broker: TimerBroker::default(),
            storage_policy: StoragePolicy::denied(),
            broker_runtime: None,
            capability_policy_sha256,
            logical_policy: None,
            allow_instance_checkpoint: false,
        }
    }

    pub fn from_verified_snapshot_with_fetch(
        image: VerifiedSnapshot,
        fetch_broker: FetchBroker,
    ) -> Self {
        let capability_policy_sha256 = WorkerCapabilityPolicy::new(
            fetch_broker.clone(),
            TimerLimits::default(),
            StoragePolicy::denied(),
        )
        .sha256();
        Self {
            image,
            fetch_broker,
            timer_broker: TimerBroker::default(),
            storage_policy: StoragePolicy::denied(),
            broker_runtime: None,
            capability_policy_sha256,
            logical_policy: None,
            allow_instance_checkpoint: false,
        }
    }

    pub fn from_verified_snapshot_with_capabilities(
        image: VerifiedSnapshot,
        fetch_broker: FetchBroker,
        timer_limits: TimerLimits,
    ) -> Result<Self> {
        Self::from_verified_snapshot_with_storage(
            image,
            fetch_broker,
            timer_limits,
            StoragePolicy::denied(),
        )
    }

    pub fn from_verified_snapshot_with_storage(
        image: VerifiedSnapshot,
        fetch_broker: FetchBroker,
        timer_limits: TimerLimits,
        storage_policy: StoragePolicy,
    ) -> Result<Self> {
        let policy = WorkerCapabilityPolicy::new(
            fetch_broker.clone(),
            timer_limits.clone(),
            storage_policy.clone(),
        );
        if image.binding().capability_policy_sha256() != policy.sha256() {
            return Err(Error::Snapshot(
                "snapshot capability policy binding mismatch".into(),
            ));
        }
        Ok(Self {
            image,
            fetch_broker,
            timer_broker: TimerBroker::new(timer_limits)?,
            storage_policy,
            broker_runtime: None,
            capability_policy_sha256: policy.sha256(),
            logical_policy: None,
            allow_instance_checkpoint: false,
        })
    }

    /// Skip the expensive boot+init+snapshot sequence
    /// ([`initialize_with_policy`](Self::initialize_with_policy)) by
    /// restoring directly from an on-disk snapshot a prior call already
    /// built and saved (via [`Self::snapshot`]'s
    /// [`VerifiedSnapshot::save`]) — the same save/open/restore round trip
    /// [`tests/workerd_sandbox.rs`] already exercises, now reachable from a
    /// `bundle_path`/`rootfs`/`executor`/policy tuple instead of a
    /// caller-supplied [`VerifiedSnapshot`].
    ///
    /// `bundle`/`rootfs`/`executor`/`policy` must be the exact artifacts
    /// the snapshot at `snapshot_dir` was built from:
    /// [`VerifiedSnapshot::open`] hashes and compares every one of them
    /// (see [`SnapshotBinding::validate`]) and fails closed on any
    /// mismatch, so a stale or wrong snapshot directory is rejected rather
    /// than silently restoring the wrong code. `policy`'s `broker_runtime`
    /// and Workerd `bindings` are not supported here (matching every other
    /// `from_verified_snapshot*` constructor): a policy carrying either is
    /// rejected up front rather than silently dropped.
    pub fn initialize_from_snapshot_dir(
        snapshot_dir: impl AsRef<Path>,
        bundle: &WorkerBundle,
        rootfs: impl AsRef<Path>,
        executor: impl AsRef<Path>,
        policy: WorkerCapabilityPolicy,
    ) -> Result<Self> {
        if policy.broker_runtime.is_some()
            || (!policy.bindings.is_empty() && policy.logical_policy.is_none())
        {
            return Err(Error::Snapshot(
                "snapshot_dir restore does not support a broker runtime or Workerd bindings".into(),
            ));
        }
        let policy_sha256 = policy.sha256();
        let expected =
            SnapshotBinding::from_artifacts_with_policy(bundle, rootfs, executor, policy_sha256)?;
        let image = VerifiedSnapshot::open(snapshot_dir, &expected)?;
        if policy.logical_policy.is_some() {
            return Ok(Self {
                image,
                fetch_broker: policy.fetch_broker,
                timer_broker: TimerBroker::new(policy.timer_limits)?,
                storage_policy: policy.storage_policy,
                broker_runtime: None,
                capability_policy_sha256: expected.capability_policy_sha256().into(),
                logical_policy: policy.logical_policy,
                allow_instance_checkpoint: false,
            });
        }
        Self::from_verified_snapshot_with_storage(
            image,
            policy.fetch_broker,
            policy.timer_limits,
            policy.storage_policy,
        )
    }

    pub fn snapshot(&self) -> &VerifiedSnapshot {
        &self.image
    }

    pub fn negotiate_extensions(&self, required: &[&str], timeout: Duration) -> Result<()> {
        self.negotiated_extensions(required, timeout).map(|_| ())
    }

    pub(super) fn negotiated_extensions(
        &self,
        required: &[&str],
        timeout: Duration,
    ) -> Result<super::GuestCapabilities> {
        let started = Instant::now();
        let (mut restored, _) = self.restore()?;
        let deadline = started
            .checked_add(timeout)
            .ok_or_else(|| Error::State("capability admission deadline too large".into()))?;
        let capabilities: super::GuestCapabilities =
            restored.control_call("runtime_capabilities", "admitted-capabilities", deadline)?;
        for required in required {
            capabilities.validate("admitted-capabilities", required)?;
        }
        Ok(capabilities)
    }

    pub fn worker_version(&self) -> &WorkerVersionId {
        self.image.binding().worker_version()
    }

    pub fn execute(
        &self,
        version: &WorkerVersionId,
        request: RequestEnvelope,
        timeout: Duration,
    ) -> Result<ResponseEnvelope> {
        self.execute_profiled(version, request, timeout).0
    }

    pub fn execute_scheduled(
        &self,
        version: &WorkerVersionId,
        request: ScheduledRequest,
        timeout: Duration,
    ) -> Result<ScheduledResponse> {
        if self.image.binding().bundle_protocol_version() == super::PACKAGE_PROTOCOL_VERSION {
            return self
                .execute_invocation_profiled(
                    version,
                    InvocationRequest::Scheduled(request),
                    timeout,
                )
                .0
                .and_then(|response| match response {
                    InvocationResponse::Scheduled(response) => Ok(response),
                    _ => Err(Error::Protocol(
                        "wrong budgeted scheduled response kind".into(),
                    )),
                });
        }
        if version != self.worker_version() {
            return Err(Error::State(
                "sandbox cannot be reassigned across Worker versions".into(),
            ));
        }
        let (restored, _) = self.restore()?;
        restored.execute_scheduled(request, timeout)
    }

    pub fn execute_queue(
        &self,
        version: &WorkerVersionId,
        request: QueueRequest,
        timeout: Duration,
    ) -> Result<QueueResponse> {
        if self.image.binding().bundle_protocol_version() == super::PACKAGE_PROTOCOL_VERSION {
            return self
                .execute_invocation_profiled(version, InvocationRequest::Queue(request), timeout)
                .0
                .and_then(|response| match response {
                    InvocationResponse::Queue(response) => Ok(response),
                    _ => Err(Error::Protocol("wrong budgeted queue response kind".into())),
                });
        }
        if version != self.worker_version() {
            return Err(Error::State(
                "sandbox cannot be reassigned across Worker versions".into(),
            ));
        }
        let (restored, _) = self.restore()?;
        restored.execute_queue(request, timeout)
    }

    pub fn execute_profiled(
        &self,
        version: &WorkerVersionId,
        request: RequestEnvelope,
        timeout: Duration,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        if self.image.binding().bundle_protocol_version() == super::PACKAGE_PROTOCOL_VERSION {
            let (result, profile) =
                self.execute_invocation_profiled(version, request.into(), timeout);
            return (result.and_then(InvocationResponse::into_fetch), profile);
        }
        let total_started = Instant::now();
        let mut profile = ExecutionProfile::default();
        if version != self.worker_version() {
            profile.total_ms = Self::elapsed_ms(total_started);
            return (
                Err(Error::State(
                    "sandbox cannot be reassigned across Worker versions".into(),
                )),
                profile,
            );
        }
        let (restored, restore_ms) = match self.restore() {
            Ok(restored) => restored,
            Err(error) => {
                profile.total_ms = Self::elapsed_ms(total_started);
                return (Err(error), profile);
            }
        };
        let (result, mut execution_profile) =
            restored.execute_profiled(request, timeout, total_started);
        execution_profile.snapshot_restore_ms = restore_ms;
        execution_profile.total_ms = Self::elapsed_ms(total_started);
        (result, execution_profile)
    }

    pub fn execute_invocation_profiled(
        &self,
        version: &WorkerVersionId,
        request: InvocationRequest,
        timeout: Duration,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        self.execute_invocation_cancellable(
            version,
            request,
            timeout,
            InvocationCancellation::default(),
        )
    }

    pub fn execute_invocation_cancellable(
        &self,
        version: &WorkerVersionId,
        request: InvocationRequest,
        timeout: Duration,
        cancellation: InvocationCancellation,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        let started = Instant::now();
        if version != self.worker_version() {
            return (
                Err(Error::State(
                    "sandbox cannot be reassigned across Worker versions".into(),
                )),
                ExecutionProfile::default(),
            );
        }
        if cancellation.is_cancelled() {
            return (Err(Error::Cancelled), ExecutionProfile::default());
        }
        if timeout.is_zero() {
            return (Err(Error::Timeout), ExecutionProfile::default());
        }
        let (restored, restore_ms) = match self.restore() {
            Ok(restored) => restored,
            Err(error) => return (Err(error), ExecutionProfile::default()),
        };
        let (result, mut profile) = restored.execute_invocation_with_teardown_observer(
            request,
            timeout.saturating_sub(started.elapsed()),
            started,
            cancellation,
            || {},
            || {},
        );
        profile.snapshot_restore_ms = restore_ms;
        (result, profile)
    }

    pub fn execute_stream(
        &self,
        request: RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
    ) -> Result<()> {
        let started = Instant::now();
        let (mut restored, _) = self.restore()?;
        restored.execute_stream(
            request,
            websocket,
            ingress,
            timeout.saturating_sub(started.elapsed()),
        )
    }

    /// Restore one VM from the snapshot and hand it back as a
    /// [`super::resident::ResidentWorkerSandbox`] the caller can execute
    /// more than once against, instead of the disposable, single-call
    /// restored sandbox `restore()` returns. This does not change `restore()`
    /// or any disposable call path.
    pub fn restore_resident(&self) -> Result<super::resident::ResidentWorkerSandbox> {
        let (restored, restore_ms) = self.restore()?;
        super::resident::ResidentWorkerSandbox::new(
            restored,
            self.image.binding().clone(),
            restore_ms,
        )
    }

    pub(super) fn checkpoint_binding(&self) -> Result<SnapshotBinding> {
        if self.broker_runtime.is_some() {
            return Err(Error::Snapshot(
                "resident checkpoint requires an explicit reconstruction policy for an opaque broker runtime".into(),
            ));
        }
        Ok(self.image.binding().clone())
    }

    pub fn restore_resident_checkpoint(
        &self,
        checkpoint: &VerifiedSnapshot,
    ) -> Result<super::resident::ResidentWorkerSandbox> {
        self.checkpoint_binding()?;
        if checkpoint.binding() != self.image.binding() {
            return Err(Error::Snapshot(
                "instance checkpoint revision, target or capability policy mismatch".into(),
            ));
        }
        let mut worker = self.clone();
        worker.image = checkpoint.clone();
        worker.allow_instance_checkpoint = true;
        let mut resident = worker.restore_resident()?;
        let super::snapshot::SnapshotPurpose::Instance {
            identity,
            host_state,
        } = &checkpoint.purpose
        else {
            return Err(Error::Snapshot(
                "cannot resume an initialized template as a logical instance checkpoint".into(),
            ));
        };
        resident.restore_host_state(identity.clone(), host_state.requests_served);
        Ok(resident)
    }

    pub fn resume_checkpoint_claim(
        &self,
        store: &super::CheckpointStore,
        claim: super::CheckpointClaim,
    ) -> Result<super::ResidentWorkerSandbox> {
        let mut resident = match self.restore_resident_checkpoint(claim.snapshot()) {
            Ok(resident) => resident,
            Err(error) => {
                store.fail(&claim)?;
                return Err(error);
            }
        };
        resident.retain_checkpoint_layout(claim.layout());
        resident.set_identity(claim.identity.clone());
        store.activate(&claim)?;
        Ok(resident)
    }

    pub(super) fn restore(&self) -> Result<(RestoredWorkerVersionSandbox, f64)> {
        if !self.allow_instance_checkpoint
            && !matches!(
                self.image.purpose,
                super::snapshot::SnapshotPurpose::Template
            )
        {
            return Err(Error::Snapshot(
                "changed-instance checkpoint cannot be used as an initialized revision template"
                    .into(),
            ));
        }
        if self.image.binding().capability_policy_sha256() != self.capability_policy_sha256 {
            return Err(Error::Snapshot(
                "snapshot capability policy binding mismatch".into(),
            ));
        }
        let restore_started = Instant::now();
        let responses = Responses::default();
        let fetch_session = self.fetch_broker.session(Instant::now());
        let timer_session = self.timer_broker.session();
        if let super::snapshot::SnapshotPurpose::Instance { host_state, .. } = &self.image.purpose {
            fetch_session
                .restore_sequence(host_state.fetch_next_id, host_state.fetch_v2_next_id)?;
            timer_session.restore_sequence(host_state.timer_next_id)?;
        }
        let broker_runtime = if let Some(policy) = &self.logical_policy {
            Some(policy.runtime()?)
        } else {
            self.broker_runtime.clone()
        };
        if let Some(runtime) = &broker_runtime {
            runtime
                .reset_for_fresh_vm()
                .map_err(|error| Error::State(error.to_string()))?;
        }
        if let super::snapshot::SnapshotPurpose::Instance { host_state, .. } = &self.image.purpose {
            match &broker_runtime {
                Some(runtime) => runtime
                    .restore_next_handle(host_state.network_next_id)
                    .map_err(|error| Error::Snapshot(error.to_string()))?,
                None if host_state.network_next_id.is_none() => {}
                _ => {
                    return Err(Error::Snapshot(
                        "checkpoint network host-state binding mismatch".into(),
                    ));
                }
            }
        }
        let (sandbox, config) = crate::restore_snapshot_with(
            self.image.snapshot.clone(),
            self.storage_policy.mounts(),
            None,
            None,
            |functions| {
                responses.register(functions)?;
                register_entropy(functions)?;
                super::bundle::BundleReader::default().register(functions)?;
                self.fetch_broker
                    .register(functions, fetch_session.clone())?;
                self.timer_broker
                    .register(functions, timer_session.clone())?;
                if let Some(runtime) = &broker_runtime {
                    register_provider_websockets(functions, runtime)?;
                    if runtime.has_network() {
                        let runtime = runtime.clone();
                        functions.register_host_function(
                            crate::broker_runtime::NETWORK_BROKER_HOST_FUNCTION,
                            move |payload: Vec<u8>| -> hyperlight_host::Result<Vec<u8>> {
                                Ok(runtime.dispatch_network(&payload))
                            },
                        )?;
                    }
                    if runtime.has_logical() {
                        let runtime = runtime.clone();
                        functions.register_host_function(
                            crate::broker_runtime::LOGICAL_BROKER_HOST_FUNCTION,
                            move |payload: String| -> hyperlight_host::Result<String> {
                                String::from_utf8(runtime.dispatch_logical(payload.as_bytes()))
                                    .map_err(|_| {
                                        hyperlight_host::new_error!(
                                            "logical service broker returned a non-UTF-8 response"
                                        )
                                    })
                            },
                        )?;
                    }
                }
                Ok::<(), Error>(())
            },
        )?;
        Ok((
            RestoredWorkerVersionSandbox {
                app: AppSandbox {
                    sandbox,
                    config,
                    exited: None,
                    pending: None,
                    restore_poisoned: false,
                },
                responses,
                broker_runtime,
                use_invoke_v2: self.image.binding().bundle_protocol_version()
                    == super::PACKAGE_PROTOCOL_VERSION,
                reset_broker_per_invocation: self.logical_policy.as_ref().is_some_and(|policy| {
                    policy.budget_scope() == super::LogicalBudgetScope::Invocation
                }),
                fetch_session,
                timer_session,
            },
            Self::elapsed_ms(restore_started),
        ))
    }
}

impl RestoredWorkerVersionSandbox {
    pub(super) fn execute_stream(
        &mut self,
        request: RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
    ) -> Result<()> {
        let result = (|| {
            request.validate()?;
            if !request.body_base64.is_empty() || request.request_id != ingress.request_id() {
                return Err(Error::Protocol(
                    "streaming request must match transport and have an empty buffered body".into(),
                ));
            }
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| Error::State("streaming lifetime too large".into()))?
                .min(ingress.deadline());
            let capabilities_id = "ingress-capabilities";
            let capabilities: super::GuestCapabilities =
                self.control_call("runtime_capabilities", capabilities_id, deadline)?;
            capabilities.validate(capabilities_id, "ingress-stream-v1")?;
            capabilities.validate(capabilities_id, "tracked-work-drain-v1")?;
            let lifetime_budget_ms = u32::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .map_err(|_| {
                Error::State("streaming lifetime exceeds u32 millisecond budget".into())
            })?;
            if lifetime_budget_ms == 0 {
                return Err(Error::Timeout);
            }
            let request_id = request.request_id.clone();
            let encoded = serde_json::json!({
                "protocol_version":1,"request_id":request_id,"request":request,
                "websocket":websocket,"lifetime_budget_ms":lifetime_budget_ms,
            })
            .to_string();
            if encoded.len() > super::MAX_ENVELOPE_BYTES {
                return Err(Error::Protocol(
                    "ingress invocation exceeds envelope limit".into(),
                ));
            }
            self.fetch_session.set_deadline(deadline);
            begin_broker_invocation(
                &self.broker_runtime,
                self.reset_broker_per_invocation,
                deadline,
            )?;
            self.responses.begin(&request_id)?;
            self.responses.1.attach(ingress.clone())?;
            let call = with_watchdog_cancellable(
                &mut self.app,
                deadline,
                Some((self.fetch_session.clone(), self.timer_session.clone())),
                ingress.cancellation(),
                |app, deadline| {
                    app.resume()?;
                    drive_call(app, "ingress_stream", encoded, deadline)
                },
            );
            match call {
                Ok(()) => {
                    self.responses.finish_stream()?;
                    validate_guest_completion(
                        &mut self.app,
                        &self.responses,
                        (self.fetch_session.clone(), self.timer_session.clone()),
                        deadline,
                        ingress.cancellation(),
                    )?;
                    ingress.finish(true)
                }
                Err(error) => {
                    self.responses.clear()?;
                    Err(error)
                }
            }
        })();
        if result.is_err() {
            ingress.abort();
        }
        self.responses.1.detach()?;
        result
    }

    pub(super) fn checkpoint(
        &mut self,
        binding: SnapshotBinding,
        request_id: &str,
        timeout: Duration,
        identity: super::InstanceIdentity,
        requests_served: u64,
    ) -> Result<VerifiedSnapshot> {
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::State("checkpoint timeout too large".into()))?;
        let capabilities_id = "checkpoint-capabilities";
        let capabilities: super::GuestCapabilities =
            self.control_call("runtime_capabilities", capabilities_id, deadline)?;
        capabilities.validate(capabilities_id, "safe-point-v1")?;
        capabilities.validate(capabilities_id, "tracked-work-drain-v1")?;
        let safe: super::SafePoint = self.control_call("checkpoint", request_id, deadline)?;
        safe.validate(request_id)?;
        let broker_quiescent = match &self.broker_runtime {
            Some(runtime) => runtime
                .checkpoint_quiescent()
                .map_err(|error| Error::Snapshot(error.to_string()))?,
            None => true,
        };
        if !self.fetch_session.is_quiescent()?
            || !self.timer_session.is_quiescent()?
            || !broker_quiescent
        {
            return Err(Error::Snapshot(
                "host fetch/timer handles are not reconstructable at safe point".into(),
            ));
        }
        // Capture this VM after its handler mutations and safe-point call.
        // self.image (the initialized revision template) is never used here.
        let (fetch_next_id, fetch_v2_next_id) = self.fetch_session.sequence_state();
        let network_next_id = self
            .broker_runtime
            .as_ref()
            .map(BrokerRuntime::checkpoint_next_handle)
            .transpose()
            .map_err(|error| Error::Snapshot(error.to_string()))?
            .flatten();
        Ok(VerifiedSnapshot::changed_instance(
            self.app.snapshot()?,
            binding,
            identity,
            super::snapshot::HostCheckpointState {
                fetch_next_id,
                fetch_v2_next_id,
                timer_next_id: self.timer_session.sequence_state(),
                requests_served,
                network_next_id,
            },
        ))
    }

    fn control_call<T: serde::de::DeserializeOwned>(
        &mut self,
        function: &str,
        request_id: &str,
        deadline: Instant,
    ) -> Result<T> {
        drive_control_call(
            &mut self.app,
            &self.responses,
            (self.fetch_session.clone(), self.timer_session.clone()),
            function,
            request_id,
            deadline,
            InvocationCancellation::default(),
        )
    }

    pub(super) fn execute_invocation_with_teardown_observer(
        mut self,
        request: InvocationRequest,
        timeout: Duration,
        total_started: Instant,
        cancellation: InvocationCancellation,
        teardown_started: impl FnOnce(),
        teardown_finished: impl FnOnce(),
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        if !self.use_invoke_v2
            && let InvocationRequest::Fetch(request) = request
        {
            let (result, profile) = self.execute_profiled_cancellable_with_teardown_observer(
                request,
                timeout,
                total_started,
                cancellation,
                teardown_started,
                teardown_finished,
            );
            return (result.map(InvocationResponse::Fetch), profile);
        }
        let (result, mut profile) = self.execute_resident_invocation_cancellable(
            request,
            timeout,
            total_started,
            cancellation,
        );
        teardown_started();
        let teardown = Instant::now();
        drop(self);
        profile.vm_teardown_ms = WorkerVersionSandbox::elapsed_ms(teardown);
        teardown_finished();
        profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
        (result, profile)
    }

    pub(super) fn execute_resident_invocation_cancellable(
        &mut self,
        request: InvocationRequest,
        timeout: Duration,
        total_started: Instant,
        cancellation: InvocationCancellation,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        if self.use_invoke_v2 {
            return self.execute_budgeted_invocation(request, timeout, total_started, cancellation);
        }
        if let InvocationRequest::Fetch(request) = request {
            let (result, profile) =
                self.execute_resident_cancellable(request, timeout, total_started, cancellation);
            return (result.map(InvocationResponse::Fetch), profile);
        }
        let mut profile = ExecutionProfile::default();
        let setup = Instant::now();
        let result = (|| {
            let request_id = request.request_id().to_string();
            let (function, encoded) = request.encode()?;
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| Error::State("timeout too large".into()))?;
            self.fetch_session.set_deadline(deadline);
            begin_broker_invocation(
                &self.broker_runtime,
                self.reset_broker_per_invocation,
                deadline,
            )?;
            self.responses.begin(&request_id)?;
            profile.request_setup_ms = WorkerVersionSandbox::elapsed_ms(setup);
            let execution = Instant::now();
            let result = with_watchdog_cancellable(
                &mut self.app,
                deadline,
                Some((self.fetch_session.clone(), self.timer_session.clone())),
                cancellation.clone(),
                |app, deadline| {
                    app.resume()?;
                    drive_call(app, function, encoded, deadline)
                },
            );
            profile.guest_execution_ms = WorkerVersionSandbox::elapsed_ms(execution);
            let finish = Instant::now();
            let result = match result {
                Ok(()) => self
                    .responses
                    .finish_invocation(function)
                    .and_then(|response| {
                        validate_guest_completion(
                            &mut self.app,
                            &self.responses,
                            (self.fetch_session.clone(), self.timer_session.clone()),
                            deadline,
                            cancellation,
                        )?;
                        Ok(response)
                    }),
                Err(error) => {
                    self.responses.clear()?;
                    Err(error)
                }
            };
            profile.response_finish_ms = WorkerVersionSandbox::elapsed_ms(finish);
            result
        })();
        profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
        (result, profile)
    }

    fn execute_budgeted_invocation(
        &mut self,
        request: InvocationRequest,
        timeout: Duration,
        total_started: Instant,
        cancellation: InvocationCancellation,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        let mut profile = ExecutionProfile::default();
        let result = (|| {
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| Error::State("invocation budget too large".into()))?;
            let request_id = request.request_id().to_string();
            let (kind, _) = request.encode()?;
            let capabilities: super::GuestCapabilities =
                self.control_call("runtime_capabilities", "invocation-capabilities", deadline)?;
            capabilities.validate("invocation-capabilities", "invocation-budget-v2")?;
            capabilities.validate("invocation-capabilities", "tracked-work-drain-v1")?;
            capabilities.validate("invocation-capabilities", "safe-point-v1")?;
            let remaining = u32::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .map_err(|_| Error::State("invocation exceeds u32 millisecond budget".into()))?;
            if remaining == 0 {
                return Err(Error::Timeout);
            }
            let encoded = request.to_budgeted_json(remaining)?;
            self.responses.begin(&request_id)?;
            self.fetch_session.set_deadline(deadline);
            begin_broker_invocation(
                &self.broker_runtime,
                self.reset_broker_per_invocation,
                deadline,
            )?;
            let execution = Instant::now();
            let called = with_watchdog_cancellable(
                &mut self.app,
                deadline,
                Some((self.fetch_session.clone(), self.timer_session.clone())),
                cancellation.clone(),
                |app, deadline| {
                    app.resume()?;
                    drive_call(app, "invoke", encoded, deadline)
                },
            );
            profile.guest_execution_ms = WorkerVersionSandbox::elapsed_ms(execution);
            match called {
                Ok(()) => {
                    let response = self.responses.finish_invocation(kind)?;
                    validate_guest_completion(
                        &mut self.app,
                        &self.responses,
                        (self.fetch_session.clone(), self.timer_session.clone()),
                        deadline,
                        cancellation,
                    )?;
                    Ok(response)
                }
                Err(error) => {
                    self.responses.clear()?;
                    Err(error)
                }
            }
        })();
        profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
        (result, profile)
    }

    fn execute_scheduled(
        self,
        request: ScheduledRequest,
        timeout: Duration,
    ) -> Result<ScheduledResponse> {
        let request_id = request.request_id.clone();
        self.execute_event(
            "scheduled",
            request_id,
            request.to_json()?,
            timeout,
            |responses| responses.finish_scheduled(),
        )
    }

    fn execute_queue(self, request: QueueRequest, timeout: Duration) -> Result<QueueResponse> {
        let request_id = request.request_id.clone();
        self.execute_event(
            "queue",
            request_id,
            request.to_json()?,
            timeout,
            |responses| responses.finish_queue(),
        )
    }

    fn execute_event<T>(
        self,
        function: &str,
        request_id: String,
        encoded: String,
        timeout: Duration,
        finish: impl FnOnce(&Responses) -> Result<T>,
    ) -> Result<T> {
        let Self {
            mut app,
            responses,
            fetch_session,
            timer_session,
            broker_runtime,
            use_invoke_v2: _,
            reset_broker_per_invocation,
        } = self;
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::State("timeout too large".into()))?;
        fetch_session.set_deadline(deadline);
        begin_broker_invocation(&broker_runtime, reset_broker_per_invocation, deadline)?;
        responses.begin(&request_id)?;
        let result = with_watchdog(
            &mut app,
            deadline,
            Some((fetch_session.clone(), timer_session.clone())),
            |app, deadline| {
                app.resume()?;
                drive_call(app, function, encoded, deadline)
            },
        );
        match result {
            Ok(()) => finish(&responses).and_then(|response| {
                validate_guest_completion(
                    &mut app,
                    &responses,
                    (fetch_session, timer_session),
                    deadline,
                    InvocationCancellation::default(),
                )?;
                Ok(response)
            }),
            Err(error) => match responses.clear() {
                Ok(()) => Err(error),
                Err(clear_error) => Err(clear_error),
            },
        }
    }

    pub(super) fn execute_profiled(
        self,
        request: RequestEnvelope,
        timeout: Duration,
        total_started: Instant,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        self.execute_profiled_with_teardown_observer(request, timeout, total_started, || {}, || {})
    }

    pub(super) fn execute_profiled_with_teardown_observer(
        self,
        request: RequestEnvelope,
        timeout: Duration,
        total_started: Instant,
        teardown_started: impl FnOnce(),
        teardown_finished: impl FnOnce(),
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        self.execute_profiled_cancellable_with_teardown_observer(
            request,
            timeout,
            total_started,
            InvocationCancellation::default(),
            teardown_started,
            teardown_finished,
        )
    }

    fn execute_profiled_cancellable_with_teardown_observer(
        self,
        request: RequestEnvelope,
        timeout: Duration,
        total_started: Instant,
        cancellation: InvocationCancellation,
        teardown_started: impl FnOnce(),
        teardown_finished: impl FnOnce(),
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        let Self {
            app,
            responses,
            fetch_session,
            timer_session,
            broker_runtime,
            use_invoke_v2: _,
            reset_broker_per_invocation,
        } = self;
        let mut app = ObservedAppTeardown::new(app, teardown_started, teardown_finished);
        let mut profile = ExecutionProfile::default();
        macro_rules! fail {
            ($error:expr) => {{
                app.teardown();
                profile.vm_teardown_ms = app.elapsed_ms();
                profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
                return (Err($error), profile);
            }};
        }
        let setup_started = Instant::now();
        if cancellation.is_cancelled() {
            fail!(Error::Cancelled);
        }
        let encoded = match request.to_json() {
            Ok(encoded) => encoded,
            Err(error) => fail!(error),
        };
        if timeout.is_zero() {
            fail!(Error::Timeout);
        }
        let deadline = match Instant::now().checked_add(timeout) {
            Some(deadline) => deadline,
            None => fail!(Error::State("timeout too large".into())),
        };
        fetch_session.set_deadline(deadline);
        if let Err(error) =
            begin_broker_invocation(&broker_runtime, reset_broker_per_invocation, deadline)
        {
            fail!(error);
        }
        if let Err(error) = responses.begin(&request.request_id) {
            fail!(error);
        }
        profile.request_setup_ms = WorkerVersionSandbox::elapsed_ms(setup_started);

        let execution_started = Instant::now();
        let result = timed_request(
            app.app_mut(),
            encoded,
            deadline,
            fetch_session.clone(),
            timer_session.clone(),
            cancellation.clone(),
        );
        profile.guest_execution_ms = WorkerVersionSandbox::elapsed_ms(execution_started);

        let finish_started = Instant::now();
        let result = match result {
            Ok(()) => responses.finish_fetch().and_then(|response| {
                validate_guest_completion(
                    app.app_mut(),
                    &responses,
                    (fetch_session, timer_session),
                    deadline,
                    cancellation,
                )?;
                Ok(response)
            }),
            Err(error) => match responses.clear() {
                Ok(()) => Err(error),
                Err(clear_error) => Err(clear_error),
            },
        };
        profile.response_finish_ms = WorkerVersionSandbox::elapsed_ms(finish_started);
        app.teardown();
        profile.vm_teardown_ms = app.elapsed_ms();
        profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
        (result, profile)
    }

    /// Resident counterpart of [`Self::execute_profiled_with_teardown_observer`]:
    /// drives exactly one `fetch` call on this VM **without** tearing it down
    /// afterward, so the caller (`resident::ResidentWorkerSandbox`) can issue
    /// further requests against the same running VM. The VM is left running
    /// only when `result` is `Ok`; any error (including a watchdog-induced
    /// timeout kill or a guest exit) leaves the VM unusable and the caller
    /// must retire it rather than call this again.
    fn execute_resident_cancellable(
        &mut self,
        request: RequestEnvelope,
        timeout: Duration,
        total_started: Instant,
        cancellation: InvocationCancellation,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        let mut profile = ExecutionProfile::default();
        macro_rules! fail {
            ($error:expr) => {{
                profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
                return (Err($error), profile);
            }};
        }
        let setup_started = Instant::now();
        if cancellation.is_cancelled() {
            fail!(Error::Cancelled);
        }
        let encoded = match request.to_json() {
            Ok(encoded) => encoded,
            Err(error) => fail!(error),
        };
        if timeout.is_zero() {
            fail!(Error::Timeout);
        }
        let deadline = match Instant::now().checked_add(timeout) {
            Some(deadline) => deadline,
            None => fail!(Error::State("timeout too large".into())),
        };
        self.fetch_session.set_deadline(deadline);
        if let Err(error) = begin_broker_invocation(
            &self.broker_runtime,
            self.reset_broker_per_invocation,
            deadline,
        ) {
            fail!(error);
        }
        if let Err(error) = self.responses.begin(&request.request_id) {
            fail!(error);
        }

        profile.request_setup_ms = WorkerVersionSandbox::elapsed_ms(setup_started);

        let execution_started = Instant::now();
        let result = timed_request(
            &mut self.app,
            encoded,
            deadline,
            self.fetch_session.clone(),
            self.timer_session.clone(),
            cancellation.clone(),
        );
        profile.guest_execution_ms = WorkerVersionSandbox::elapsed_ms(execution_started);

        let finish_started = Instant::now();
        let result = match result {
            Ok(()) => self.responses.finish_fetch().and_then(|response| {
                validate_guest_completion(
                    &mut self.app,
                    &self.responses,
                    (self.fetch_session.clone(), self.timer_session.clone()),
                    deadline,
                    cancellation,
                )?;
                Ok(response)
            }),
            Err(error) => match self.responses.clear() {
                Ok(()) => Err(error),
                Err(clear_error) => Err(clear_error),
            },
        };
        profile.response_finish_ms = WorkerVersionSandbox::elapsed_ms(finish_started);
        profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
        (result, profile)
    }
}

struct ObservedAppTeardown<Started: FnOnce(), Finished: FnOnce()> {
    app: Option<AppSandbox>,
    started: Option<Started>,
    finished: Option<Finished>,
    elapsed: Duration,
}

impl<Started: FnOnce(), Finished: FnOnce()> ObservedAppTeardown<Started, Finished> {
    fn new(app: AppSandbox, started: Started, finished: Finished) -> Self {
        Self {
            app: Some(app),
            started: Some(started),
            finished: Some(finished),
            elapsed: Duration::ZERO,
        }
    }

    fn app_mut(&mut self) -> &mut AppSandbox {
        self.app.as_mut().expect("VM has not been torn down")
    }

    fn teardown(&mut self) {
        let Some(app) = self.app.take() else {
            return;
        };
        if let Some(started) = self.started.take() {
            started();
        }
        let teardown_started = Instant::now();
        let observer = ScopeExit::new(self.finished.take().expect("teardown observer is paired"));
        drop(app);
        self.elapsed = teardown_started.elapsed();
        drop(observer);
    }

    fn elapsed_ms(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1000.0
    }
}

impl<Started: FnOnce(), Finished: FnOnce()> Drop for ObservedAppTeardown<Started, Finished> {
    fn drop(&mut self) {
        self.teardown();
    }
}

struct ScopeExit<F: FnOnce()> {
    callback: Option<F>,
}

impl<F: FnOnce()> ScopeExit<F> {
    fn new(callback: F) -> Self {
        Self {
            callback: Some(callback),
        }
    }
}

impl<F: FnOnce()> Drop for ScopeExit<F> {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

fn begin_broker_invocation(
    runtime: &Option<BrokerRuntime>,
    reset: bool,
    deadline: Instant,
) -> Result<()> {
    if let Some(runtime) = runtime {
        if reset {
            if !runtime
                .checkpoint_quiescent()
                .map_err(|error| Error::State(error.to_string()))?
            {
                return Err(Error::State(
                    "cannot reset invocation budget while host resources remain live".into(),
                ));
            }
            runtime
                .reset_for_fresh_vm()
                .map_err(|error| Error::State(error.to_string()))?;
        }
        runtime
            .set_deadline(deadline)
            .map_err(|error| Error::State(error.to_string()))?;
    }
    Ok(())
}

fn timed_request(
    app: &mut AppSandbox,
    encoded: String,
    deadline: Instant,
    fetch_session: super::fetch::FetchSession,
    timer_session: super::timer::TimerSession,
    cancellation: InvocationCancellation,
) -> Result<()> {
    with_watchdog_cancellable(
        app,
        deadline,
        Some((fetch_session, timer_session)),
        cancellation,
        |app, deadline| {
            app.resume()?;
            drive_call(app, "fetch", encoded, deadline)
        },
    )
}

fn timed_call(app: &mut AppSandbox, name: &str, argument: String, timeout: Duration) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::State("timeout too large".into()))?;
    with_watchdog(app, deadline, None, |app, deadline| {
        drive_call(app, name, argument, deadline)
    })
}

fn drive_call(app: &mut AppSandbox, name: &str, argument: String, deadline: Instant) -> Result<()> {
    if !app.has_driver() {
        return Err(Error::State("snapshot has no executor driver".into()));
    }
    let mut yielded = app.config.enter(&mut app.sandbox, name, argument)?;
    loop {
        match yielded {
            Yield::CallDone => return Ok(()),
            Yield::CallFailed { status } => return Err(crate::Error::CallFailed { status }.into()),
            Yield::Exited { status } => return Err(crate::Error::GuestExited { status }.into()),
            Yield::Blocked { .. } => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(Error::Timeout);
                }
                yielded = app.step(remaining.min(Duration::from_millis(10)))?;
            }
        }
    }
}

fn with_watchdog(
    app: &mut AppSandbox,
    deadline: Instant,
    sessions: Option<(super::fetch::FetchSession, super::timer::TimerSession)>,
    run: impl FnOnce(&mut AppSandbox, Instant) -> Result<()>,
) -> Result<()> {
    with_watchdog_cancellable(
        app,
        deadline,
        sessions,
        InvocationCancellation::default(),
        run,
    )
}

fn with_watchdog_cancellable(
    app: &mut AppSandbox,
    deadline: Instant,
    sessions: Option<(super::fetch::FetchSession, super::timer::TimerSession)>,
    cancellation: InvocationCancellation,
    run: impl FnOnce(&mut AppSandbox, Instant) -> Result<()>,
) -> Result<()> {
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(Error::Timeout);
    }
    let interrupt = app.interrupt_handle();
    let (done, wait) = mpsc::channel();
    let watchdog_cancellation = cancellation.clone();
    let watchdog = std::thread::Builder::new()
        .name("workerd-watchdog".into())
        .spawn(move || {
            loop {
                if watchdog_cancellation.is_cancelled() || Instant::now() >= deadline {
                    break;
                }
                match wait.recv_timeout(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(10)),
                ) {
                    Ok(()) => return false,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            if let Some((fetch_session, timer_session)) = sessions {
                fetch_session.cancel_all();
                timer_session.cancel_all();
            }
            interrupt.kill();
            true
        })?;
    let result = run(app, deadline);
    // A disconnected receiver means it already timed out; join is authoritative.
    let _ = done.send(());
    let timed_out = watchdog
        .join()
        .map_err(|_| Error::State("watchdog panicked".into()))?;
    if cancellation.is_cancelled() {
        Err(Error::Cancelled)
    } else if timed_out || Instant::now() >= deadline {
        Err(Error::Timeout)
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_entropy_is_bounded_and_fresh() {
        for amount in [0, 16_385, u64::MAX] {
            assert!(read_entropy(amount).is_err());
        }
        for amount in [1, 32, 16_384] {
            assert_eq!(read_entropy(amount).unwrap().len(), amount as usize);
        }
        assert!(
            read_entropy(32).unwrap() != read_entropy(32).unwrap(),
            "host entropy unexpectedly repeated"
        );
    }

    fn response(id: &str) -> String {
        format!(
            r#"{{"protocol_version":1,"request_id":"{id}","status":200,"headers":[],"body_base64":""}}"#
        )
    }

    #[test]
    fn violations_are_latched_and_request_state_is_cleared() {
        let c = Responses::default();
        assert!(c.submit(&response("idle")).is_err());
        for bad in [
            response("stale"),
            "{".into(),
            " ".repeat(super::super::MAX_ENVELOPE_BYTES + 1),
        ] {
            c.begin("active").unwrap();
            assert!(c.submit(&bad).is_err());
            c.submit(&response("active")).unwrap();
            assert!(c.finish().is_err());
        }
        c.begin("active").unwrap();
        c.submit(&response("active")).unwrap();
        assert!(c.submit(&response("active")).is_err());
        assert!(c.finish().is_err());
        c.begin("killed").unwrap();
        c.clear().unwrap();
        c.begin("next").unwrap();
        c.submit(&response("next")).unwrap();
        assert_eq!(c.finish().unwrap().request_id, "next");
    }

    #[test]
    fn stream_rejects_prefix_suffix_missing_newline_and_split_duplicates() {
        let c = Responses::default();
        for bad in [
            format!("log\r\n{}\r\n", response("r")),
            format!(" {}\r\n", response("r")),
            format!("{} \r\n", response("r")),
            format!("{}\r\nextra", response("r")),
            format!("{}\r\n{}\r\n", response("r"), response("r")),
            format!("{}\n", response("r")),
            format!("{}\r\r\n", response("r")),
            "a".repeat(super::super::MAX_ENVELOPE_BYTES + 3),
        ] {
            c.begin("r").unwrap();
            assert!(c.output(&bad).is_err());
            assert!(c.output(&format!("{}\r\n", response("r"))).is_err());
            assert!(c.finish().is_err());
        }
        c.begin("r").unwrap();
        c.output(&response("r")).unwrap();
        assert!(c.finish().is_err());
        c.begin("r").unwrap();
        let valid = response("r");
        c.output(&valid[..10]).unwrap();
        c.output(&valid[10..]).unwrap();
        c.output("\r\n").unwrap();
        assert!(c.output("\n").is_err());
        assert!(c.finish().is_err());
        c.begin("next").unwrap();
        c.output(&format!("{}\r", response("next"))).unwrap();
        c.output("\n").unwrap();
        assert_eq!(c.finish().unwrap().request_id, "next");
    }

    #[test]
    fn storage_policy_rejects_executor_incompatible_names_and_excess_bindings() {
        let root = tempfile::tempdir().unwrap();
        for name in ["Upper", "under_score", "1starts-with-digit"] {
            assert!(
                StorageBinding::read_only(name, root.path(), MountLimits::default()).is_err(),
                "{name}"
            );
        }
        let bindings = (0..=MAX_STORAGE_BINDINGS)
            .map(|index| {
                StorageBinding::read_only(
                    format!("mount-{index}"),
                    root.path(),
                    MountLimits::default(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(StoragePolicy::new(bindings).is_err());
    }
}
