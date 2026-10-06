// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor tests for multi-app routing (boundary 3): missing
//! fixture/hypervisor is a failure, not a skip (see `tests/workerd_sandbox.rs`).

use hyperlight_unikraft::workerd::{
    AppConfig, AppPoolConfig, AppRegistry, AppRegistryError, AppRoute, ConnectionAffinity,
    DisposablePoolConfig, HostConfig, PROTOCOL_VERSION, RequestEnvelope, ResidentPoolConfigJson,
    WorkerBundle, WorkerCapabilityPolicyConfig, WorkerVersionId,
};
use std::path::{Path, PathBuf};
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

fn bundles_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/workerd-bundles")
}

/// Writes a minimal single-script bundle to `dir` as JSON, matching the
/// on-disk shape `HostConfig`'s `bundle_path` expects (`WorkerBundle::from_path`).
/// The lightweight fixture executor used by these tests does not run real
/// JS (see `tests/workerd_resident.rs`): only the bundle/version identity and
/// routing are exercised here, not worker script semantics.
fn write_script_bundle(dir: &Path, file_name: &str, version: &str) -> PathBuf {
    let bundle = WorkerBundle::single_script(
        WorkerVersionId::new(version).unwrap(),
        "2025-01-01",
        "worker.js",
        "export default {}",
    )
    .unwrap();
    let path = dir.join(file_name);
    std::fs::write(&path, serde_json::to_vec(&bundle).unwrap()).unwrap();
    path
}

fn request(id: &str, url: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: id.into(),
        method: "GET".into(),
        url: url.into(),
        headers: vec![],
        body_base64: String::new(),
    }
}

fn disposable_app(app_id: &str, hostname: &str, bundle_path: PathBuf) -> AppConfig {
    AppConfig {
        route: AppRoute {
            app_id: app_id.into(),
            hostnames: vec![hostname.into()],
            path_prefix: None,
        },
        bundle_path,
        scratch_memory_mb: 64,
        execute_timeout_secs: 10,
        capability_policy: WorkerCapabilityPolicyConfig::default(),
        pool: AppPoolConfig::Disposable(DisposablePoolConfig {
            max_concurrent_sandboxes: 2,
            queue_capacity: 2,
        }),
        connection_affinity: ConnectionAffinity::None,
    }
}

fn resident_app(app_id: &str, hostname: &str, bundle_path: PathBuf) -> AppConfig {
    AppConfig {
        route: AppRoute {
            app_id: app_id.into(),
            hostnames: vec![hostname.into()],
            path_prefix: None,
        },
        bundle_path,
        scratch_memory_mb: 64,
        execute_timeout_secs: 10,
        capability_policy: WorkerCapabilityPolicyConfig::default(),
        pool: AppPoolConfig::Resident(ResidentPoolConfigJson {
            capacity: 1,
            queue_capacity: 2,
            max_requests_per_vm: None,
            max_lifetime_secs: None,
        }),
        connection_affinity: ConnectionAffinity::None,
    }
}

fn submit_and_wait(
    registry: &AppRegistry,
    host: &str,
    path: &str,
    request_id: &str,
) -> hyperlight_unikraft::workerd::Result<hyperlight_unikraft::workerd::ResponseEnvelope> {
    let handle = registry
        .route(Some(host), path)
        .unwrap_or_else(|| panic!("no route for host {host} path {path}"));
    let (tx, rx) = mpsc::channel();
    handle
        .try_submit(
            request(request_id, &format!("https://{host}{path}")),
            Duration::from_secs(10),
            move |execution| {
                let _ = tx.send(execution.result);
            },
        )
        .unwrap();
    rx.recv().unwrap()
}

#[test]
fn routes_mixed_resident_and_disposable_apps_in_isolation() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let hello_bundle = write_script_bundle(tmp.path(), "hello.json", "hello-app-v1");
    let streams_bundle = write_script_bundle(tmp.path(), "streams.json", "streams-app-v1");

    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![
            disposable_app("hello", "hello.test", hello_bundle),
            resident_app("streams", "streams.test", streams_bundle),
        ],
    };

    let registry = AppRegistry::from_host_config(config).unwrap();
    assert_eq!(registry.len(), 2);
    assert!(registry.route(Some("unknown.test"), "/").is_none());

    let hello = submit_and_wait(&registry, "hello.test", "/", "hello-1").unwrap();
    assert_eq!(hello.status, 200);

    let streams = submit_and_wait(&registry, "streams.test", "/sync", "streams-1").unwrap();
    assert_eq!(streams.status, 200);

    // Hitting the same resident app twice reuses its one resident VM.
    let streams_again = submit_and_wait(&registry, "streams.test", "/async", "streams-2").unwrap();
    assert_eq!(streams_again.status, 200);

    // A `Host` header with a port suffix still matches.
    let hello_with_port = submit_and_wait(&registry, "hello.test:8080", "/", "hello-2").unwrap();
    assert_eq!(hello_with_port.status, 200);
}

#[test]
fn ambiguous_routes_are_rejected_before_any_app_starts() {
    let (rootfs, executor) = artifacts();
    let bundles = bundles_dir();
    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![
            disposable_app("a", "shared.test", bundles.join("helloworld_esm.json")),
            disposable_app("b", "shared.test", bundles.join("web-streams.json")),
        ],
    };
    match AppRegistry::from_host_config(config) {
        Ok(_) => panic!("expected ambiguous route rejection"),
        Err(error) => assert!(matches!(error, AppRegistryError::AmbiguousRoute { .. })),
    }
}

#[test]
fn duplicate_app_ids_are_rejected_before_any_app_starts() {
    let (rootfs, executor) = artifacts();
    let bundles = bundles_dir();
    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![
            disposable_app("same", "a.test", bundles.join("helloworld_esm.json")),
            disposable_app("same", "b.test", bundles.join("web-streams.json")),
        ],
    };
    match AppRegistry::from_host_config(config) {
        Ok(_) => panic!("expected duplicate app id rejection"),
        Err(error) => {
            assert!(matches!(error, AppRegistryError::DuplicateAppId(id) if id == "same"))
        }
    }
}

#[test]
fn host_config_round_trips_through_json() {
    let (rootfs, executor) = artifacts();
    let bundles = bundles_dir();
    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![disposable_app(
            "hello",
            "hello.test",
            bundles.join("helloworld_esm.json"),
        )],
    };
    let json = serde_json::to_vec(&config).unwrap();
    let path = std::env::temp_dir().join(format!("host-config-{}.json", std::process::id()));
    std::fs::write(&path, &json).unwrap();
    let loaded = HostConfig::from_path(&path).unwrap();
    std::fs::remove_file(&path).ok();
    assert_eq!(loaded.apps.len(), 1);
    assert_eq!(loaded.apps[0].route.app_id, "hello");
}
