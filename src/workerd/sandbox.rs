// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{
    Error, RequestEnvelope, ResponseEnvelope, Result, SnapshotBinding, VerifiedSnapshot,
    WorkerBundle, WorkerVersionId, snapshot::kernel_for_rootfs,
};
use crate::{AppSandbox, Yield};
use hyperlight_host::func::Registerable;
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
    response: Option<ResponseEnvelope>,
    violation: Option<String>,
    bytes: Vec<u8>,
}

#[derive(Clone, Default)]
struct Responses(Arc<Mutex<RequestState>>);

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
        Ok(())
    }

    fn submit(&self, json: &str) -> Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))?;
        let result = (|| {
            let response = ResponseEnvelope::from_json(json.as_bytes())?;
            if state.id.as_deref() != Some(&response.request_id) {
                return Err(Error::State(
                    "stale response ID or no active request".into(),
                ));
            }

            if state.response.is_some() {
                return Err(Error::State("duplicate response".into()));
            }
            state.response = Some(response);
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

    fn finish(&self) -> Result<ResponseEnvelope> {
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

    fn clear(&self) -> Result<()> {
        *self
            .0
            .lock()
            .map_err(|_| Error::State("response mutex poisoned".into()))? = RequestState::default();
        Ok(())
    }

    fn register(&self, target: &mut impl Registerable) -> Result<()> {
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
}

pub(super) struct RestoredWorkerVersionSandbox {
    app: AppSandbox,
    responses: Responses,
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
        let mut profile = InitializationProfile::default();
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
        let init_json = bundle
            .to_canonical_json()
            .map_err(|error| Self::initialization_failure("binding", error, &profile))?;
        let binding = SnapshotBinding::from_artifacts(&bundle, &rootfs, executor)
            .map_err(|error| Self::initialization_failure("binding", error, &profile))?;
        profile.binding_ms = Self::elapsed_ms(started);
        let responses = Responses::default();
        let started = Instant::now();
        let (mut uninitialized, config) = crate::assemble_sandbox_with_embedded_kernel(
            kernel,
            &Some(rootfs.as_ref().into()),
            &Some("/bin/workerd-executor".into()),
            scratch_mb,
            vec![],
            None,
            None,
        )
        .map_err(|error| Self::initialization_failure("assemble", error, &profile))?;
        responses
            .register(&mut uninitialized)
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
        profile.init_ms = Self::elapsed_ms(started);
        let started = Instant::now();
        let snapshot = app
            .snapshot()
            .map_err(|error| Self::initialization_failure("snapshot", error, &profile))?;
        profile.snapshot_ms = Self::elapsed_ms(started);
        let image = VerifiedSnapshot::initialized(snapshot, binding);
        Ok((Self { image }, profile))
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
        Self { image }
    }

    pub fn snapshot(&self) -> &VerifiedSnapshot {
        &self.image
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

    pub fn execute_profiled(
        &self,
        version: &WorkerVersionId,
        request: RequestEnvelope,
        timeout: Duration,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
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

    pub(super) fn restore(&self) -> Result<(RestoredWorkerVersionSandbox, f64)> {
        let restore_started = Instant::now();
        let (mut sandbox, config) =
            crate::restore_snapshot(self.image.snapshot.clone(), vec![], None, None)?;
        let responses = Responses::default();
        responses.register(&mut sandbox)?;
        Ok((
            RestoredWorkerVersionSandbox {
                app: AppSandbox {
                    sandbox,
                    config,
                    exited: None,
                    pending: None,
                },
                responses,
            },
            Self::elapsed_ms(restore_started),
        ))
    }
}

impl RestoredWorkerVersionSandbox {
    pub(super) fn execute_profiled(
        mut self,
        request: RequestEnvelope,
        timeout: Duration,
        total_started: Instant,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        let mut profile = ExecutionProfile::default();
        macro_rules! fail {
            ($error:expr) => {{
                profile.total_ms = WorkerVersionSandbox::elapsed_ms(total_started);
                return (Err($error), profile);
            }};
        }
        let setup_started = Instant::now();
        let encoded = match request.to_json() {
            Ok(encoded) => encoded,
            Err(error) => fail!(error),
        };
        if timeout.is_zero() {
            fail!(Error::Timeout);
        }
        if let Err(error) = self.responses.begin(&request.request_id) {
            fail!(error);
        }
        profile.request_setup_ms = WorkerVersionSandbox::elapsed_ms(setup_started);

        let execution_started = Instant::now();
        let result = timed_request(&mut self.app, encoded, timeout);
        profile.guest_execution_ms = WorkerVersionSandbox::elapsed_ms(execution_started);

        // Join the watchdog before dropping the VM; no late kill can hit the
        // next request. Dropping also discards timers, threads and guest secrets.
        let teardown_started = Instant::now();
        drop(self.app);
        profile.vm_teardown_ms = WorkerVersionSandbox::elapsed_ms(teardown_started);

        let finish_started = Instant::now();
        let result = match result {
            Ok(()) => self.responses.finish(),
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

fn timed_request(app: &mut AppSandbox, encoded: String, timeout: Duration) -> Result<()> {
    with_watchdog(app, timeout, |app, deadline| {
        app.resume()?;
        drive_call(app, "fetch", encoded, deadline)
    })
}

fn timed_call(app: &mut AppSandbox, name: &str, argument: String, timeout: Duration) -> Result<()> {
    with_watchdog(app, timeout, |app, deadline| {
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
    timeout: Duration,
    run: impl FnOnce(&mut AppSandbox, Instant) -> Result<()>,
) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::State("timeout too large".into()))?;
    if timeout.is_zero() {
        return Err(Error::Timeout);
    }
    let interrupt = app.interrupt_handle();
    let (done, wait) = mpsc::channel();
    let watchdog = std::thread::Builder::new()
        .name("workerd-watchdog".into())
        .spawn(move || {
            if wait
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .is_ok()
            {
                return false;
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
    if timed_out || Instant::now() >= deadline {
        Err(Error::Timeout)
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
