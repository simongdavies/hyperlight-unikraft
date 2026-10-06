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
use super::{Error, ExecutionProfile, RequestEnvelope, ResponseEnvelope, Result, WorkerVersionId};
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
    requests_served: u64,
    created_at: Instant,
}

impl ResidentWorkerSandbox {
    pub(super) fn new(
        restored: RestoredWorkerVersionSandbox,
        worker_version: WorkerVersionId,
        _restore_ms: f64,
    ) -> Self {
        Self {
            restored: Some(restored),
            worker_version,
            requests_served: 0,
            created_at: Instant::now(),
        }
    }

    pub fn worker_version(&self) -> &WorkerVersionId {
        &self.worker_version
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

    /// Run exactly one request on this VM without tearing it down. On
    /// success the VM remains resident and ready for the next `execute()`
    /// call. On error the VM is retired immediately: `is_alive()` becomes
    /// `false` and every subsequent `execute()` call fails fast.
    pub fn execute(
        &mut self,
        request: RequestEnvelope,
        timeout: Duration,
    ) -> (Result<ResponseEnvelope>, ExecutionProfile) {
        let total_started = Instant::now();
        let Some(restored) = self.restored.as_mut() else {
            return (
                Err(Error::State("resident VM already retired".into())),
                ExecutionProfile::default(),
            );
        };
        let (result, profile) = restored.execute_resident(request, timeout, total_started);
        self.requests_served += 1;
        if result.is_err() {
            // Conservative default: any failure (including a watchdog kill)
            // retires the VM rather than risking reuse of unknown state.
            self.restored = None;
        }
        (result, profile)
    }

    /// Whether this VM should be retired proactively under `policy`, even
    /// though it is still healthy. Always `true` once `is_alive()` is
    /// `false`.
    pub fn should_retire(&self, policy: &ResidentPolicy) -> bool {
        if self.restored.is_none() {
            return true;
        }
        if let Some(max) = policy.max_requests_per_vm
            && self.requests_served >= max
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
