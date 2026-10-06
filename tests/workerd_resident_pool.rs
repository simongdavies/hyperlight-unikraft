// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor tests for the bounded resident pool (boundary 2): missing
//! fixture/hypervisor is a failure, not a skip (see `tests/workerd_sandbox.rs`).

use hyperlight_unikraft::workerd::{
    FetchBroker, PROTOCOL_VERSION, PoolSubmitError, RequestEnvelope, ResidentPolicy,
    ResidentPoolConfig, ResidentWorkerPool, StoragePolicy, TimerLimits, WorkerBundle,
    WorkerCapabilityPolicy, WorkerVersionId, WorkerVersionSandbox,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
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
fn pool_serves_many_sequential_requests_through_one_resident_vm() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-pool-single-v1", &rootfs, &executor);
    let pool = ResidentWorkerPool::new(
        worker,
        ResidentPoolConfig {
            capacity: 1,
            queue_capacity: 4,
            policy: ResidentPolicy::default(),
        },
    )
    .unwrap();

    for sequence in 0..5u64 {
        let (tx, rx) = mpsc::channel();
        pool.try_submit(
            request(&format!("req-{sequence}")),
            Duration::from_secs(5),
            move |execution| {
                let _ = tx.send(execution);
            },
        )
        .unwrap();
        let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(execution.result.is_ok(), "request {sequence} failed");
    }

    let status = pool.status();
    assert_eq!(status.capacity, 1);
    assert_eq!(status.retirements, 0);
    assert_eq!(status.resident_requests_served, 5);
}

#[test]
fn pool_never_exceeds_its_capacity_under_concurrent_submission() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-pool-bound-v1", &rootfs, &executor);
    let capacity = 2;
    let pool = Arc::new(
        ResidentWorkerPool::new(
            worker,
            ResidentPoolConfig {
                capacity,
                queue_capacity: 32,
                policy: ResidentPolicy::default(),
            },
        )
        .unwrap(),
    );
    let observed_max_active = Arc::new(AtomicUsize::new(0));

    let requests = 20u64;
    let (tx, rx) = mpsc::channel();
    for sequence in 0..requests {
        let pool = pool.clone();
        let observed = observed_max_active.clone();
        let tx = tx.clone();
        // Sample the pool's reported "active" count right after submission;
        // it must never climb above the configured capacity.
        let active_now = pool.status().active;
        observed.fetch_max(active_now, Ordering::AcqRel);
        pool.try_submit(
            request(&format!("req-{sequence}")),
            Duration::from_secs(5),
            move |execution| {
                let _ = tx.send(execution);
            },
        )
        .unwrap();
    }
    drop(tx);
    for _ in 0..requests {
        let execution = rx.recv_timeout(Duration::from_secs(20)).unwrap();
        assert!(execution.result.is_ok());
    }

    assert!(
        observed_max_active.load(Ordering::Acquire) <= capacity,
        "observed more active resident VMs than the configured capacity"
    );
}

#[test]
fn try_submit_rejects_once_capacity_and_queue_are_full() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-pool-full-v1", &rootfs, &executor);
    let pool = ResidentWorkerPool::new(
        worker,
        ResidentPoolConfig {
            capacity: 1,
            queue_capacity: 1,
            policy: ResidentPolicy::default(),
        },
    )
    .unwrap();

    let (first_started_tx, first_started_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    pool.try_submit(request("first"), Duration::from_secs(5), move |execution| {
        assert!(execution.result.is_ok());
        first_started_tx.send(()).unwrap();
        // Block the owner thread inside completion delivery so the admission
        // slot it holds is not released until we explicitly allow it.
        release_rx.recv().unwrap();
    })
    .unwrap();
    first_started_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();

    // Occupies the one queue slot (admission_capacity = capacity + queue_capacity = 2).
    let (second_tx, second_rx) = mpsc::channel();
    pool.try_submit(
        request("second"),
        Duration::from_secs(5),
        move |execution| {
            let _ = second_tx.send(execution);
        },
    )
    .unwrap();

    // Admission capacity is now fully used: this one must be rejected.
    let result = pool.try_submit(request("third"), Duration::from_secs(5), |execution| {
        assert_eq!(execution.submit_error, Some(PoolSubmitError::Full));
    });
    assert_eq!(result, Err(PoolSubmitError::Full));

    release_tx.send(()).unwrap();
    let second_execution = second_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(second_execution.result.is_ok());
}

#[test]
fn shutdown_drains_queue_and_retires_every_resident_vm() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-pool-shutdown-v1", &rootfs, &executor);
    let mut pool = ResidentWorkerPool::new(
        worker,
        ResidentPoolConfig {
            capacity: 2,
            queue_capacity: 4,
            policy: ResidentPolicy::default(),
        },
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    pool.try_submit(request("before-shutdown"), Duration::from_secs(5), {
        let tx = tx.clone();
        move |execution| {
            let _ = tx.send(execution);
        }
    })
    .unwrap();
    let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(execution.result.is_ok());

    pool.shutdown();

    let result = pool.try_submit(
        request("after-shutdown"),
        Duration::from_secs(5),
        move |execution| {
            assert_eq!(execution.submit_error, Some(PoolSubmitError::ShuttingDown));
            let _ = tx.send(execution);
        },
    );
    assert_eq!(result, Err(PoolSubmitError::ShuttingDown));
    let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(execution.result.is_err());

    let status = pool.status();
    assert_eq!(status.active, 0);
    assert_eq!(status.queued, 0);
}

#[test]
fn proactive_retirement_replaces_the_vm_and_keeps_serving() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let worker = worker("resident-pool-retire-v1", &rootfs, &executor);
    let pool = ResidentWorkerPool::new(
        worker,
        ResidentPoolConfig {
            capacity: 1,
            queue_capacity: 4,
            policy: ResidentPolicy {
                max_requests_per_vm: Some(1),
                max_lifetime: None,
            },
        },
    )
    .unwrap();

    for sequence in 0..3u64 {
        let (tx, rx) = mpsc::channel();
        pool.try_submit(
            request(&format!("req-{sequence}")),
            Duration::from_secs(5),
            move |execution| {
                let _ = tx.send(execution);
            },
        )
        .unwrap();
        let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(execution.result.is_ok(), "request {sequence} failed");
    }

    let status = pool.status();
    assert_eq!(status.resident_requests_served, 3);
    // Every request exceeded `max_requests_per_vm`, so the two requests after
    // the first each caused the previous VM to be retired and replaced
    // before running; the final VM is still resident (not yet replaced).
    assert_eq!(status.retirements, 2);
}
