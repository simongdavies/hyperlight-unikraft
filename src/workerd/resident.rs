// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Boundary 1 of the resident Workerd host: a reusable resident VM.
//!
//! Every existing restore/execute path in [`super::sandbox`] is disposable:
//! one restored VM serves exactly one request and is torn down. A
//! [`ResidentWorkerSandbox`] instead wraps a single restored VM that keeps
//! running across many sequential `execute()` calls, driven by
//! `sandbox::RestoredWorkerVersionSandbox::execute_resident`, which is a new,
//! additive, non-consuming sibling of the existing one-shot execution
//! methods. None of the existing disposable types, methods, or behavior are
//! changed by this module.
//!
//! A resident VM is retired (never reused again) the moment any `execute()`
//! call returns an error: a watchdog timeout kills the guest VM outright, and
//! any other failure is treated conservatively as having left the VM in an
//! unknown state. [`ResidentPolicy`] additionally allows proactively
//! retiring a healthy VM after a bounded number of requests or a bounded
//! lifetime, so guest-side state/memory growth in a long-lived VM remains
//! bounded even when no error ever occurs.

use super::sandbox::RestoredWorkerVersionSandbox;
use super::{
    Error, ExecutionProfile, InvocationCancellation, InvocationRequest, InvocationResponse,
    RequestEnvelope, ResponseEnvelope, Result, WorkerVersionId,
};
use std::time::{Duration, Instant};

/// Proactive recycling knobs for a resident VM. `None`/`None` means "recycle
/// only on error", i.e. reuse the VM indefinitely while it keeps succeeding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResidentPolicy {
    pub max_requests_per_vm: Option<u64>,
    pub max_lifetime: Option<Duration>,
}

/// One restored VM that survives across multiple `execute()` calls instead
/// of being torn down after the first. Not `Clone`: exactly one logical
/// owner drives it at a time (a bounded resident pool, boundary 2, owns one
/// per worker thread).
pub struct ResidentWorkerSandbox {
    restored: Option<RestoredWorkerVersionSandbox>,
    worker_version: WorkerVersionId,
    binding: super::SnapshotBinding,
    requests_served: u64,
    incarnation_requests_served: u64,
    created_at: Instant,
    checkpoint_layout: Option<std::sync::Arc<super::checkpoint::CheckpointLayout>>,
    identity: super::InstanceIdentity,
    live_observer: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

impl ResidentWorkerSandbox {
    pub(super) fn new(
        restored: RestoredWorkerVersionSandbox,
        binding: super::SnapshotBinding,
        _restore_ms: f64,
    ) -> Result<Self> {
        Ok(Self {
            restored: Some(restored),
            worker_version: binding.worker_version().clone(),
            binding,
            requests_served: 0,
            incarnation_requests_served: 0,
            created_at: Instant::now(),
            checkpoint_layout: None,
            identity: super::InstanceIdentity::new()?,
            live_observer: None,
        })
    }

    pub fn identity(&self) -> &super::InstanceIdentity {
        &self.identity
    }
    pub(super) fn set_identity(&mut self, identity: super::InstanceIdentity) {
        self.identity = identity;
    }
    pub(super) fn restore_host_state(
        &mut self,
        identity: super::InstanceIdentity,
        requests_served: u64,
    ) {
        self.identity = identity;
        self.requests_served = requests_served;
        self.incarnation_requests_served = 0;
    }

    pub(super) fn observe_live(&mut self, counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.live_observer = Some(counter);
    }

    fn release_live_observer(&mut self) {
        if let Some(counter) = self.live_observer.take() {
            counter.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    pub fn worker_version(&self) -> &WorkerVersionId {
        &self.worker_version
    }

    pub(super) fn retain_checkpoint_layout(
        &mut self,
        layout: std::sync::Arc<super::checkpoint::CheckpointLayout>,
    ) {
        self.checkpoint_layout = Some(layout);
    }

    /// Number of `execute()` calls this VM has served, including any call
    /// that failed (and therefore already retired it).
    pub fn requests_served(&self) -> u64 {
        self.requests_served
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// `false` once an `execute()` call has failed; the sandbox must be
    /// dropped/replaced rather than reused.
    pub fn is_alive(&self) -> bool {
        self.restored.is_some()
    }

    pub fn checkpoint(
        &mut self,
        worker: &super::WorkerVersionSandbox,
        request_id: &str,
        timeout: Duration,
    ) -> Result<super::VerifiedSnapshot> {
        if worker.snapshot().binding() != &self.binding {
            return Err(Error::Snapshot(
                "resident checkpoint actual artifact or capability policy binding mismatch".into(),
            ));
        }
        let binding = worker.checkpoint_binding()?;
        self.restored
            .as_mut()
            .ok_or_else(|| Error::State("cannot checkpoint a retired resident".into()))?
            .checkpoint(
                binding,
                request_id,
                timeout,
                self.identity.clone(),
                self.requests_served,
            )
    }

    /// Run exactly one request on this VM without tearing it down. On
    /// success the VM remains resident and ready for the next `execute()`
    /// call. On error the VM is retired immediately: `is_alive()` becomes
    /// `false` and every subsequent `execute()` call fails fast.
    pub fn execute(
        &mut self,
        request: RequestEnvelope,
        timeout: Duration,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        let (result, profile) = self.execute_invocation(request.into(), timeout);
        (result.and_then(InvocationResponse::into_fetch), profile)
    }

    pub fn execute_invocation(
        &mut self,
        request: InvocationRequest,
        timeout: Duration,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        self.execute_cancellable(request, timeout, InvocationCancellation::default())
    }

    pub fn execute_cancellable(
        &mut self,
        request: InvocationRequest,
        timeout: Duration,
        cancellation: InvocationCancellation,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        let total_started = Instant::now();
        let Some(restored) = self.restored.as_mut() else {
            return (
                Err(Error::State("resident VM already retired".into())),
                ExecutionProfile::default(),
            );
        };
        let (result, profile) = restored.execute_resident_invocation_cancellable(
            request,
            timeout,
            total_started,
            cancellation,
        );
        self.requests_served += 1;
        self.incarnation_requests_served += 1;
        if result.is_err() {
            // Conservative default: any failure (including a watchdog kill)
            // retires the VM rather than risking reuse of unknown state.
            self.restored = None;
            self.release_live_observer();
        }
        (result, profile)
    }

    pub fn execute_stream(
        &mut self,
        request: RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
    ) -> Result<()> {
        let result = self
            .restored
            .as_mut()
            .ok_or_else(|| Error::State("resident VM already retired".into()))?
            .execute_stream(request, websocket, ingress, timeout);
        self.requests_served += 1;
        self.incarnation_requests_served += 1;
        if result.is_err() {
            self.restored = None;
            self.release_live_observer();
        }
        result
    }

    /// Whether this VM should be retired proactively under `policy`, even
    /// though it is still healthy. Always `true` once `is_alive()` is
    /// `false`.
    pub fn should_retire(&self, policy: &ResidentPolicy) -> bool {
        if self.restored.is_none() {
            return true;
        }
        if let Some(max) = policy.max_requests_per_vm
            && self.incarnation_requests_served >= max
        {
            return true;
        }
        if let Some(max_lifetime) = policy.max_lifetime
            && self.created_at.elapsed() >= max_lifetime
        {
            return true;
        }
        false
    }

    /// Explicit, consuming teardown. Dropping a `ResidentWorkerSandbox`
    /// (e.g. going out of scope) has the same effect.
    pub fn retire(mut self) {
        self.restored = None;
    }
}

impl Drop for ResidentWorkerSandbox {
    fn drop(&mut self) {
        self.restored = None;
        self.release_live_observer();
    }
}

// Real-VM tests for this module live in `tests/workerd_resident.rs`,
// following this codebase's convention (see `tests/workerd_sandbox.rs`) of
// keeping hypervisor-dependent tests in top-level integration tests rather
// than colocated unit tests, since they require a built
// `workerd-executor-fixture` and a hypervisor and are treated as failures
// (not skips) when those are missing.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_never_retires_a_healthy_vm_proactively() {
        let policy = ResidentPolicy::default();
        assert_eq!(policy.max_requests_per_vm, None);
        assert_eq!(policy.max_lifetime, None);
    }
}
