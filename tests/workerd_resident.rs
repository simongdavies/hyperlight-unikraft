// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor tests for the resident VM (boundary 1): missing
//! fixture/hypervisor is a failure, not a skip (see `tests/workerd_sandbox.rs`).

use hyperlight_unikraft::workerd::{
    Error, FetchBroker, PROTOCOL_VERSION, RequestEnvelope, ResidentPolicy, StoragePolicy,
    TimerLimits, WorkerBundle, WorkerCapabilityPolicy, WorkerVersionId, WorkerVersionSandbox,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn artifacts() -> (PathBuf, PathBuf) {
    let dir =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("build-elfloader/workerd-executor-fixture");
    let rootfs = dir.join("rootfs.cpio");
    let executor = dir.join("executor");
    assert!(
        rootfs.is_file() && executor.is_file(),
        "run just guests first"
    );
    (rootfs, executor)
}

fn bundle(version: WorkerVersionId, source: &str) -> WorkerBundle {
    WorkerBundle::single_script(version, "2025-01-01", "worker.js", source).unwrap()
}

fn request(id: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: id.into(),
        method: "GET".into(),
        url: "https://example.test/".into(),
        headers: vec![],
        body_base64: String::new(),
    }
}

fn worker(version: &str, rootfs: &Path, executor: &Path) -> WorkerVersionSandbox {
    WorkerVersionSandbox::initialize_with_policy(
        bundle(WorkerVersionId::new(version).unwrap(), "export default {}"),
        rootfs,
        executor,
        64,
        Duration::from_secs(10),
        WorkerCapabilityPolicy::new(
            FetchBroker::denied(),
            TimerLimits::default(),
            StoragePolicy::denied(),
        ),
    )
    .unwrap()
}

#[test]
fn resident_vm_serves_multiple_requests_without_teardown() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-multi-v1", &rootfs, &executor);
    let mut resident = worker.restore_resident().unwrap();
    assert_eq!(resident.requests_served(), 0);
    assert!(resident.is_alive());

    for sequence in 0..3u64 {
        let (result, _profile) =
            resident.execute(request(&format!("req-{sequence}")), Duration::from_secs(5));
        assert!(result.is_ok(), "request {sequence} failed: {result:?}");
        assert_eq!(resident.requests_served(), sequence + 1);
        assert!(resident.is_alive());
    }
}

#[test]
fn should_retire_honors_max_requests_per_vm() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-policy-v1", &rootfs, &executor);
    let mut resident = worker.restore_resident().unwrap();
    let policy = ResidentPolicy {
        max_requests_per_vm: Some(2),
        max_lifetime: None,
    };
    assert!(!resident.should_retire(&policy));
    resident
        .execute(request("a"), Duration::from_secs(5))
        .0
        .unwrap();
    assert!(!resident.should_retire(&policy));
    resident
        .execute(request("b"), Duration::from_secs(5))
        .0
        .unwrap();
    assert!(resident.should_retire(&policy));
}

#[test]
fn execute_after_a_failed_request_fails_fast_and_never_reuses_the_vm() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-retire-v1", &rootfs, &executor);
    let mut resident = worker.restore_resident().unwrap();

    // Zero timeout fails fast without ever touching the guest.
    let (result, _) = resident.execute(request("timeout"), Duration::ZERO);
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(!resident.is_alive());

    let (result, _) = resident.execute(request("after-retire"), Duration::from_secs(5));
    assert!(result.is_err());
    // The already-retired fast-fail path does not count as a served
    // request: only the one call that actually reached the VM does.
    assert_eq!(resident.requests_served(), 1);
}
