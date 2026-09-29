// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor tests: missing fixture/hypervisor is a failure, not a skip.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    Error, MAX_BODY_BYTES, PROTOCOL_VERSION, RequestEnvelope, SnapshotBinding, VerifiedSnapshot,
    WorkerBundle, WorkerVersionId, WorkerVersionSandbox,
};
use std::fs;
use std::path::{Path, PathBuf};
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

#[test]
fn real_guest_protocol_timeout_recovery_and_version_binding() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(2);
    let (rootfs, executor) = artifacts();
    let version = WorkerVersionId::new("worker-v1").unwrap();
    let other = WorkerVersionId::new("worker-v2").unwrap();
    let init_bundle = bundle(version.clone(), "export default {}");
    let mut worker = WorkerVersionSandbox::initialize(
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
    let mut restored = WorkerVersionSandbox::from_verified_snapshot(loaded);
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
