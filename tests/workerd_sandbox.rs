// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor tests: missing fixture/hypervisor is a failure, not a skip.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    Error, FetchBroker, FetchBrokerConfig, FetchLimits, FetchPolicy, Header, MAX_BODY_BYTES,
    PROTOCOL_VERSION, RequestEnvelope, SnapshotBinding, StorageBinding, StoragePolicy, TimerLimits,
    VerifiedSnapshot, WorkerBundle, WorkerCapabilityPolicy, WorkerPoolRestoreMode,
    WorkerRequestPool, WorkerVersionId, WorkerVersionSandbox,
};
use hyperlight_unikraft::{AllowList, MountLimits, NetworkPolicy};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

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

fn request(id: &str, path: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: id.into(),
        method: "POST".into(),
        url: format!("https://example.test/{path}"),
        headers: vec![],
        body_base64: String::new(),
    }
}

fn bundle(version: WorkerVersionId, source: &str) -> WorkerBundle {
    WorkerBundle::single_script(version, "2025-01-01", "worker.js", source).unwrap()
}

fn outbound_request(id: &str, path: &str, target: String) -> RequestEnvelope {
    let mut request = request(id, path);
    request.headers.push(Header {
        name: "x-fetch-url".into(),
        value: target,
    });
    request
}

fn loopback_server() -> (u16, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = stop.clone();
    thread::spawn(move || {
        while !stop_thread.load(Ordering::Acquire) {
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(2));
                continue;
            };
            stream.set_nonblocking(false).unwrap();
            thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let response = if request_line.starts_with("POST ") {
                    body
                } else {
                    b"ok".to_vec()
                };
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response.len()
                )
                .unwrap();
                stream.write_all(&response).unwrap();
            });
        }
    });
    (port, stop)
}

fn wait_for_status(
    pool: &WorkerRequestPool,
    predicate: impl Fn(hyperlight_unikraft::workerd::WorkerPoolStatus) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !predicate(pool.status()) {
        assert!(Instant::now() < deadline, "pool status wait timed out");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn real_guest_storage_policy_enforces_modes_confinement_quotas_and_reset() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let root = tempfile::tempdir().unwrap();
    let readonly = root.path().join("readonly");
    let scratch = root.path().join("scratch");
    let outside = root.path().join("outside");
    fs::create_dir_all(&readonly).unwrap();
    fs::create_dir_all(&scratch).unwrap();
    fs::create_dir_all(&outside).unwrap();
    fs::write(readonly.join("message.txt"), b"fixture-read-ok\n").unwrap();
    fs::write(outside.join("secret.txt"), b"must-not-read").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, readonly.join("escape")).unwrap();

    let limits = MountLimits {
        max_operations: Some(128),
        max_read_bytes: Some(1024),
        max_write_bytes: Some(16),
    };
    let storage = StoragePolicy::new([
        StorageBinding::read_only("readonly", &readonly, limits).unwrap(),
        StorageBinding::read_write("scratch", &scratch, limits).unwrap(),
    ])
    .unwrap();
    let version = WorkerVersionId::new("storage-v1").unwrap();
    let worker = WorkerVersionSandbox::initialize_with_policy(
        bundle(version.clone(), "export default {}"),
        &rootfs,
        &executor,
        64,
        Duration::from_secs(10),
        WorkerCapabilityPolicy::new(FetchBroker::denied(), TimerLimits::default(), storage),
    )
    .unwrap();

    for (path, expected) in [
        ("storage-allowed-read", "allowed-read"),
        ("storage-ro-write-denied", "ro-write-denied"),
        ("storage-rw-write", "rw-write"),
        ("storage-traversal-denied", "traversal-denied"),
        ("storage-unlisted-denied", "unlisted-denied"),
        ("storage-quota-denied", "quota-denied"),
        ("storage-quota-denied", "quota-denied"),
    ] {
        let response = worker
            .execute(&version, request(path, path), Duration::from_secs(5))
            .unwrap();
        let body = STANDARD.decode(response.body_base64).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["outcome"], expected, "{path}");
    }
    assert_eq!(fs::read(scratch.join("allowed.txt")).unwrap(), b"rw-ok");
    assert_eq!(fs::metadata(scratch.join("quota.txt")).unwrap().len(), 16);
    assert!(!readonly.join("denied.txt").exists());

    let mismatch = WorkerVersionSandbox::from_verified_snapshot_with_storage(
        worker.snapshot().clone(),
        FetchBroker::denied(),
        TimerLimits::default(),
        StoragePolicy::denied(),
    );
    assert!(mismatch.is_err());

    let legacy_mismatch = WorkerVersionSandbox::from_verified_snapshot(worker.snapshot().clone());
    let error = legacy_mismatch
        .execute(
            &version,
            request("legacy-storage-mismatch", "storage-allowed-read"),
            Duration::from_secs(5),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("snapshot capability policy binding mismatch")
    );
}

#[test]
fn real_guest_protocol_timeout_recovery_and_version_binding() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let version = WorkerVersionId::new("worker-v1").unwrap();
    let other = WorkerVersionId::new("worker-v2").unwrap();
    let init_bundle = bundle(version.clone(), "export default {}");
    let worker = WorkerVersionSandbox::initialize(
        init_bundle.clone(),
        &rootfs,
        &executor,
        64,
        Duration::from_secs(10),
    )
    .expect("real hypervisor must boot the v0.14 fixture");
    assert!(
        worker
            .execute(&other, request("wrong", "ok"), Duration::from_secs(5))
            .is_err()
    );

    for path in [
        "malformed",
        "oversized",
        "stale",
        "duplicate",
        "prefix",
        "suffix",
    ] {
        assert!(
            worker
                .execute(&version, request("bad", path), Duration::from_secs(5))
                .is_err(),
            "{path}"
        );
        let response = worker
            .execute(&version, request("recovered", "ok"), Duration::from_secs(5))
            .unwrap();
        assert_eq!(response.request_id, "recovered");
        assert_eq!(response.body_base64, "b2s=");
    }
    let response = worker
        .execute(
            &version,
            request("fragmented", "fragmented"),
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(response.request_id, "fragmented");
    assert_eq!(STANDARD.decode(response.body_base64).unwrap().len(), 6144);

    for path in ["busy", "sleep"] {
        let started = Instant::now();
        let error = worker
            .execute(
                &version,
                request("killed", path),
                Duration::from_millis(100),
            )
            .unwrap_err();
        assert!(matches!(error, Error::Timeout), "{path}: {error}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{path} deadline failed"
        );
        let response = worker
            .execute(
                &version,
                request("after-kill", "ok"),
                Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(response.request_id, "after-kill");
    }
    let mut oversized = request("oversized-request", "ok");
    oversized.body_base64 = STANDARD.encode(vec![0; MAX_BODY_BYTES + 1]);
    assert!(
        worker
            .execute(&version, oversized, Duration::from_secs(5))
            .is_err()
    );
    let mut maximum = request("maximum", "ok");
    maximum.body_base64 = STANDARD.encode(vec![0; MAX_BODY_BYTES]);
    assert_eq!(
        worker
            .execute(&version, maximum, Duration::from_secs(5))
            .unwrap()
            .request_id,
        "maximum"
    );

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("worker");
    worker.snapshot().save(&layout).unwrap();
    assert!(
        worker.snapshot().save(&layout).is_err(),
        "must not overwrite a snapshot"
    );
    let expected = SnapshotBinding::from_artifacts(&init_bundle, &rootfs, &executor).unwrap();
    let wrong =
        SnapshotBinding::from_artifacts(&bundle(other, "export default {}"), &rootfs, &executor)
            .unwrap();
    assert!(VerifiedSnapshot::open(&layout, &wrong).is_err());
    let wrong_source = SnapshotBinding::from_artifacts(
        &bundle(version.clone(), "export default { fetch() {} }"),
        &rootfs,
        &executor,
    )
    .unwrap();
    assert!(VerifiedSnapshot::open(&layout, &wrong_source).is_err());
    let loaded = VerifiedSnapshot::open(&layout, &expected).unwrap();
    let restored = WorkerVersionSandbox::from_verified_snapshot(loaded);
    assert_eq!(
        restored
            .execute(&version, request("persisted", "ok"), Duration::from_secs(5))
            .unwrap()
            .request_id,
        "persisted"
    );
    drop(restored);
    drop(worker);

    // No live mappings remain when corrupting the layer for integrity coverage.
    let blobs = layout.join("blobs/sha256");
    let blob = fs::read_dir(&blobs)
        .unwrap()
        .map(|e| e.unwrap().path())
        .max_by_key(|p| fs::metadata(p).unwrap().len())
        .unwrap();
    let sealed = fs::metadata(&blob).unwrap().permissions();
    make_writable(&blob);
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&blob)
        .unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[byte[0] ^ 1]).unwrap();
    drop(file);
    fs::set_permissions(&blob, sealed).unwrap();
    assert!(
        VerifiedSnapshot::open(&layout, &expected).is_err(),
        "SHA-256 must reject corrupted OCI layer"
    );
    unseal(&layout);
}

#[test]
fn real_guest_uses_host_owned_loopback_fetch_policy() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let (port, stop) = loopback_server();
    let version = WorkerVersionId::new("fetch-worker-v1").unwrap();
    let fetch_broker = FetchBroker::new(FetchBrokerConfig {
        policy: FetchPolicy::new(
            NetworkPolicy::AllowList(AllowList::from_hosts(&["localhost"]).unwrap()),
            ["http"],
            [port],
        )
        .allow_loopback(true),
        limits: FetchLimits::default(),
    })
    .unwrap();
    let worker = WorkerVersionSandbox::initialize_with_fetch(
        bundle(version.clone(), "export default {}"),
        &rootfs,
        &executor,
        64,
        Duration::from_secs(10),
        fetch_broker,
    )
    .expect("real hypervisor must boot the fetch fixture");

    for (id, path, expected) in [
        ("broker-get", "broker", "b2s="),
        ("broker-post", "broker-post", "cG9zdA=="),
    ] {
        let response = worker
            .execute(
                &version,
                outbound_request(id, path, format!("http://localhost:{port}/")),
                Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(response.request_id, id);
        assert_eq!(response.status, 200);
        assert_eq!(response.body_base64, expected);
    }

    assert!(
        worker
            .execute(
                &version,
                outbound_request(
                    "denied-port",
                    "broker",
                    format!("http://localhost:{}/", port.saturating_add(1))
                ),
                Duration::from_secs(5),
            )
            .is_err()
    );
    stop.store(true, Ordering::Release);
}

#[test]
fn real_guest_uses_nonblocking_monotonic_timer_channel() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let version = WorkerVersionId::new("timer-worker-v1").unwrap();
    let worker = WorkerVersionSandbox::initialize(
        bundle(version.clone(), "export default {}"),
        &rootfs,
        &executor,
        64,
        Duration::from_secs(10),
    )
    .expect("real hypervisor must boot the timer fixture");

    for (id, path) in [
        ("timer-fired", "timer"),
        ("timer-cancelled", "timer-cancel"),
    ] {
        let started = Instant::now();
        let response = worker
            .execute(&version, request(id, path), Duration::from_secs(5))
            .unwrap();
        assert_eq!(response.request_id, id);
        assert_eq!(response.status, 200);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}

#[test]
fn request_pool_parallelism_bounds_queue_isolation_and_timeout_recovery() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let version = WorkerVersionId::new("parallel-worker-v1").unwrap();
    let worker = WorkerVersionSandbox::initialize(
        bundle(version, "export default {}"),
        &rootfs,
        &executor,
        64,
        Duration::from_secs(10),
    )
    .expect("real hypervisor must boot the v0.14 fixture");

    let pool = WorkerRequestPool::new(worker.clone(), 2, 2).unwrap();
    let (tx, rx) = mpsc::channel();
    for id in ["parallel-a", "parallel-b"] {
        let tx = tx.clone();
        pool.try_submit(
            request(id, "delay"),
            Duration::from_secs(5),
            move |result| {
                let _ = tx.send(result);
            },
        )
        .unwrap();
    }
    wait_for_status(&pool, |status| status.active == 2);
    for _ in 0..2 {
        let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let response = execution.result.unwrap();
        assert_eq!(response.request_id, execution.request_id);
        assert_eq!(response.status, 200);
    }

    for id in ["wave-a", "wave-b", "wave-c", "wave-d"] {
        let tx = tx.clone();
        pool.try_submit(
            request(id, "delay"),
            Duration::from_secs(5),
            move |result| {
                let _ = tx.send(result);
            },
        )
        .unwrap();
    }
    wait_for_status(&pool, |status| status.active == 2 && status.queued == 2);
    let mut seen = Vec::new();
    while seen.len() < 4 {
        let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(execution.submit_error.is_none());
        let response = execution.result.unwrap();
        assert_eq!(response.request_id, execution.request_id);
        seen.push(response.request_id);
        assert!(pool.status().active <= 2);
    }
    seen.sort();
    assert_eq!(seen, ["wave-a", "wave-b", "wave-c", "wave-d"]);
    drop(pool);

    let pool = WorkerRequestPool::new(worker, 1, 1).unwrap();
    let tx1 = tx.clone();
    pool.try_submit(
        request("timeout", "busy"),
        Duration::from_millis(300),
        move |result| {
            let _ = tx1.send(result);
        },
    )
    .unwrap();
    wait_for_status(&pool, |status| status.active == 1);
    let tx2 = tx.clone();
    pool.try_submit(
        request("queued", "instance"),
        Duration::from_secs(5),
        move |result| {
            let _ = tx2.send(result);
        },
    )
    .unwrap();
    wait_for_status(&pool, |status| status.active == 1 && status.queued == 1);
    let tx3 = tx.clone();
    assert_eq!(
        pool.try_submit(
            request("overflow", "instance"),
            Duration::from_secs(5),
            move |result| {
                let _ = tx3.send(result);
            },
        ),
        Err(hyperlight_unikraft::workerd::PoolSubmitError::Full)
    );

    let overflow = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(overflow.request_id, "overflow");
    assert_eq!(
        overflow.submit_error,
        Some(hyperlight_unikraft::workerd::PoolSubmitError::Full)
    );
    let timeout = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(timeout.request_id, "timeout");
    assert!(matches!(timeout.result, Err(Error::Timeout)));
    let queued = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(queued.request_id, "queued");
    let queued_response = queued.result.unwrap();
    assert_eq!(queued_response.status, 200);
    assert_eq!(STANDARD.decode(queued_response.body_base64).unwrap(), b"1");

    for id in ["fresh-one", "fresh-two"] {
        let tx = tx.clone();
        pool.try_submit(
            request(id, "instance"),
            Duration::from_secs(5),
            move |result| {
                let _ = tx.send(result);
            },
        )
        .unwrap();
        let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let response = execution.result.unwrap();
        assert_eq!(response.request_id, id);
        assert_eq!(response.status, 200, "request VM was reused");
        assert_eq!(STANDARD.decode(response.body_base64).unwrap(), b"1");
    }
}

#[test]
fn prewarmed_pool_is_ready_one_shot_bounded_and_replenishes() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let (port, stop) = loopback_server();
    let version = WorkerVersionId::new("prewarmed-worker-v1").unwrap();
    let fetch_broker = FetchBroker::new(FetchBrokerConfig {
        policy: FetchPolicy::new(
            NetworkPolicy::AllowList(AllowList::from_hosts(&["localhost"]).unwrap()),
            ["http"],
            [port],
        )
        .allow_loopback(true),
        limits: FetchLimits::default(),
    })
    .unwrap();
    let worker = WorkerVersionSandbox::initialize_with_fetch(
        bundle(version, "export default {}"),
        &rootfs,
        &executor,
        64,
        Duration::from_secs(10),
        fetch_broker,
    )
    .expect("real hypervisor must boot the v0.14 fixture");
    let pool = WorkerRequestPool::with_restore_mode(
        worker,
        1,
        2,
        WorkerPoolRestoreMode::Prewarmed {
            sandboxes: 2,
            max_concurrent_restores: 1,
            policy: hyperlight_unikraft::workerd::PrewarmPolicy {
                warm_floor: 1,
                ready_low_watermark: 2,
                ready_high_watermark: 2,
                max_replenish_batch: 1,
                diagnostic_no_refill_wave: None,
            },
        },
    )
    .unwrap();
    wait_for_status(&pool, |status| {
        status.prewarmed_inventory == 2 && status.ready == 2 && status.replenishing == 0
    });
    let initial_status = pool.status();
    assert_eq!(initial_status.execution_slots_in_use, 0);
    assert_eq!(initial_status.restore_slots_in_use, 0);
    assert_eq!(initial_status.admitted, 0);
    assert_eq!(initial_status.completion_queue_depth, 0);
    assert_eq!(initial_status.completion_in_flight, 0);
    assert_eq!(initial_status.completed_restores, 2);
    assert_eq!(initial_status.failed_restores, 0);
    assert_eq!(initial_status.restore_attempts, 2);
    assert_eq!(initial_status.ready_peak, 2);
    assert_eq!(initial_status.replenishing_peak, 1);

    let (tx, rx) = mpsc::channel();
    for id in ["burst-a", "burst-b", "burst-c"] {
        let tx = tx.clone();
        pool.try_submit(
            request(id, "delay"),
            Duration::from_secs(5),
            move |result| {
                let _ = tx.send(result);
            },
        )
        .unwrap();
    }
    wait_for_status(&pool, |status| status.active == 1 && status.queued == 2);
    assert_eq!(
        pool.try_submit(
            request("overflow", "instance"),
            Duration::from_secs(5),
            |_| {},
        ),
        Err(hyperlight_unikraft::workerd::PoolSubmitError::Full)
    );
    let mut replenishment_restore_ms = Vec::new();
    for _ in 0..3 {
        let execution = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(execution.result.is_ok());
        assert_eq!(execution.profile.snapshot_restore_ms, 0.0);
        replenishment_restore_ms.push(execution.profile.replenishment_restore_ms);
        assert!(pool.status().active <= 1);
        assert!(pool.status().replenishing <= 1);
    }
    assert_eq!(&replenishment_restore_ms[..2], &[0.0, 0.0]);
    assert!(replenishment_restore_ms[2] > 0.0);
    let status = pool.status();
    assert_eq!(status.prewarmed_hits, 2);
    assert_eq!(status.prewarmed_misses, 1);
    assert!(status.execution_slots_in_use <= 1);
    assert!(status.restore_slots_in_use <= 1);
    assert!(status.ready_min <= status.ready);
    assert!(status.restore_wait_total_ms >= status.restore_wait_max_ms);
    assert!(status.restore_total_ms >= status.restore_max_ms);
    wait_for_status(&pool, |status| status.completed_completions >= 3);
    wait_for_status(&pool, |status| status.completed_teardowns >= 3);
    wait_for_status(&pool, |status| {
        status.prewarmed_inventory == 2 && status.ready == 2 && status.replenishing == 0
    });
    let replenished_status = pool.status();
    assert!(replenished_status.completed_restores >= 5);
    assert_eq!(
        replenished_status.restore_attempts,
        replenished_status.completed_restores
    );
    assert_eq!(replenished_status.completion_in_flight, 0);
    assert_eq!(replenished_status.completion_queue_depth, 0);
    assert_eq!(replenished_status.admitted, 0);
    assert!(replenished_status.completion_queue_peak >= 1);
    assert_eq!(replenished_status.teardown_peak, 1);
    assert!(replenished_status.completion_total_ms >= replenished_status.completion_max_ms);
    assert!(replenished_status.teardown_total_ms >= replenished_status.teardown_max_ms);

    let teardowns_before_timeout = replenished_status.completed_teardowns;
    let tx_timeout = tx.clone();
    pool.try_submit(
        request("zero-timeout", "instance"),
        Duration::ZERO,
        move |result| {
            let _ = tx_timeout.send(result);
        },
    )
    .unwrap();
    let timeout = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(matches!(timeout.result, Err(Error::Timeout)));
    wait_for_status(&pool, |status| {
        status.completed_teardowns == teardowns_before_timeout + 1
    });
    wait_for_status(&pool, |status| {
        status.prewarmed_inventory == 2 && status.ready == 2 && status.replenishing == 0
    });

    for id in ["fresh-one", "fresh-two"] {
        let tx = tx.clone();
        pool.try_submit(
            request(id, "instance"),
            Duration::from_secs(5),
            move |result| {
                let _ = tx.send(result);
            },
        )
        .unwrap();
        let response = rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .result
            .unwrap();
        assert_eq!(STANDARD.decode(response.body_base64).unwrap(), b"1");
    }
    wait_for_status(&pool, |status| {
        status.prewarmed_inventory == 2 && status.ready == 2 && status.replenishing == 0
    });

    let tx = tx.clone();
    pool.try_submit(
        outbound_request(
            "prewarmed-fetch",
            "broker",
            format!("http://localhost:{port}/"),
        ),
        Duration::from_secs(5),
        move |result| {
            let _ = tx.send(result);
        },
    )
    .unwrap();
    let response = rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .result
        .unwrap();
    assert_eq!(STANDARD.decode(response.body_base64).unwrap(), b"ok");
    stop.store(true, Ordering::Release);
}

#[allow(clippy::permissions_set_readonly_false)]
fn make_writable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(permissions.mode() | 0o200);
    }
    #[cfg(windows)]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
}

fn unseal(path: &Path) {
    make_writable(path);
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            unseal(&entry.unwrap().path());
        }
    }
}
