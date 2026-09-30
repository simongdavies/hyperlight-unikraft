// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real Workerd/JSG/V8 bundle acceptance. Missing artifacts or hypervisor fail.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    Header, PROTOCOL_VERSION, RequestEnvelope, SnapshotBinding, VerifiedSnapshot, WorkerBundle,
    WorkerVersionSandbox,
};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const WORKERD_SCRATCH_MIB: usize = 768;

fn artifact(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("build-elfloader/workerd-executor")
        .join(name)
}

fn bundle(name: &str) -> WorkerBundle {
    WorkerBundle::from_path(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/workerd-bundles")
            .join(name),
    )
    .unwrap()
}

fn request(method: &str, url: &str, headers: Vec<Header>) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: "acceptance-1".into(),
        method: method.into(),
        url: url.into(),
        headers,
        body_base64: String::new(),
    }
}

fn execute(bundle: WorkerBundle, request: RequestEnvelope) -> (WorkerVersionSandbox, String) {
    let version = bundle.worker_version.clone();
    let worker = WorkerVersionSandbox::initialize(
        bundle,
        artifact("rootfs.img"),
        artifact("executor"),
        WORKERD_SCRATCH_MIB,
        Duration::from_secs(90),
    )
    .expect("real Workerd executor must initialize");
    let response = worker
        .execute(&version, request, Duration::from_secs(10))
        .expect("real Workerd fetch must succeed");
    assert_eq!(response.status, 200);
    (
        worker,
        String::from_utf8(STANDARD.decode(response.body_base64).unwrap()).unwrap(),
    )
}

#[test]
fn real_workerd_bundles_and_snapshot_identity() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(2);
    assert!(
        artifact("rootfs.img").is_file(),
        "package the real executor"
    );
    assert!(artifact("executor").is_file(), "package the real executor");

    let hello = bundle("helloworld_esm.json");
    let (worker, body) = execute(
        hello.clone(),
        request("GET", "https://example.test/", Vec::new()),
    );
    assert_eq!(body, "Hello World\n");

    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("hello");
    worker.snapshot().save(&layout).unwrap();
    let expected =
        SnapshotBinding::from_artifacts(&hello, artifact("rootfs.img"), artifact("executor"))
            .unwrap();
    VerifiedSnapshot::open(&layout, &expected).unwrap();
    let mut changed = hello.clone();
    changed.modules[0].source.push('\n');
    let changed =
        SnapshotBinding::from_artifacts(&changed, artifact("rootfs.img"), artifact("executor"))
            .unwrap();
    assert!(
        VerifiedSnapshot::open(&layout, &changed).is_err(),
        "snapshot must reject changed Worker source"
    );
    drop(worker);
    unseal(&layout);

    let (_, streams) = execute(
        bundle("web-streams.json"),
        request("GET", "https://example.test/sync", Vec::new()),
    );
    assert!(!streams.is_empty());
    assert!(
        streams
            .chars()
            .all(|character| !character.is_ascii_lowercase()),
        "synchronous transform must uppercase the generated stream"
    );

    let (_, smoke) = execute(
        bundle("api-smoke.json"),
        request(
            "POST",
            "https://example.test/wintertc-smoke",
            vec![Header {
                name: "x-smoke".into(),
                value: "yes".into(),
            }],
        ),
    );
    let smoke: Value = serde_json::from_str(&smoke).unwrap();
    for field in [
        "url",
        "urlPattern",
        "request",
        "response",
        "headers",
        "formData",
        "blob",
        "textCodec",
        "cryptoDigest",
        "cryptoRandom",
        "readableStream",
        "transformStream",
        "compression",
        "performance",
    ] {
        assert_eq!(smoke[field], true, "{field}");
    }
    assert_eq!(smoke["webAssembly"], "blocked by executor embedder policy");
}

#[allow(clippy::permissions_set_readonly_false)]
fn unseal(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            unseal(&entry.unwrap().path());
        }
    }
}
