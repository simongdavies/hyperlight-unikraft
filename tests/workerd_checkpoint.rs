// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Actual changed-VM state through KVM + mock executor. Not real V8 proof.
use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::*;
use std::path::Path;
use std::time::Duration;

fn worker() -> WorkerVersionSandbox {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("build-elfloader/workerd-executor-fixture");
    WorkerVersionSandbox::initialize(
        WorkerBundle::single_script(
            WorkerVersionId::new("checkpoint-v1").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default {}",
        )
        .unwrap(),
        fixture.join("rootfs.cpio"),
        fixture.join("executor"),
        64,
        Duration::from_secs(10),
    )
    .unwrap()
}

fn request(id: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: 1,
        request_id: id.into(),
        method: "GET".into(),
        url: "https://example.test/instance".into(),
        headers: vec![],
        body_base64: String::new(),
    }
}

#[test]
fn encrypted_durable_file_recovery_preserves_changed_instance_and_fences_resume() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let worker = worker();
    let mut resident = worker.restore_resident().unwrap();
    assert_eq!(
        resident
            .execute(request("before"), Duration::from_secs(5))
            .0
            .unwrap()
            .status,
        200
    );
    let changed = resident
        .checkpoint(&worker, "park-1", Duration::from_secs(5))
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("checkpoints.db");
    let store = CheckpointStore::open(&db_path, &[7; 32], 128 * 1024 * 1024).unwrap();
    let identity = resident.identity().clone();
    store
        .register(&identity, worker.snapshot().binding())
        .unwrap();
    let checkpoint_id = store.commit(&identity, &changed).unwrap();
    assert!(
        store.claim(&identity, worker.snapshot().binding()).is_err(),
        "live VM must retain ownership"
    );
    resident.retire();
    store.publish_parked(&identity, &checkpoint_id).unwrap();
    drop(store);
    // Reopen only durable encrypted storage, not the pristine worker image.
    let recovered = CheckpointStore::open(&db_path, &[7; 32], 128 * 1024 * 1024).unwrap();
    let records = recovered.records(worker.snapshot().binding()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, InstanceState::Parked);
    assert_eq!(records[0].requests_served, 1);
    let wrong_key = CheckpointStore::open(&db_path, &[8; 32], 128 * 1024 * 1024).unwrap();
    assert!(
        wrong_key.records(worker.snapshot().binding()).is_err(),
        "counter telemetry must authenticate metadata, not return a silent zero"
    );
    let claim = recovered
        .claim(&identity, worker.snapshot().binding())
        .unwrap();
    assert_eq!(claim.identity.instance_id, identity.instance_id);
    assert_eq!(claim.identity.generation, 2);
    assert!(
        recovered
            .claim(&identity, worker.snapshot().binding())
            .is_err(),
        "racing old-generation claim must fail"
    );
    let resumed_identity = claim.identity.clone();
    let mut resumed = worker.resume_checkpoint_claim(&recovered, claim).unwrap();
    let changed_response = resumed
        .execute(request("after"), Duration::from_secs(5))
        .0
        .unwrap();
    assert_eq!(
        changed_response.status, 500,
        "mock counter mutation was lost or pristine fallback used"
    );
    assert_eq!(changed_response.body_base64, "cmV1c2Vk");
    assert!(
        recovered.delete(&resumed_identity).is_err(),
        "active instance deletion must fail"
    );
    let second = resumed
        .checkpoint(&worker, "park-2", Duration::from_secs(5))
        .unwrap();
    let checkpoint_id = recovered.commit(&resumed_identity, &second).unwrap();
    resumed.retire();
    recovered
        .publish_parked(&resumed_identity, &checkpoint_id)
        .unwrap();
    recovered.delete(&resumed_identity).unwrap();
    assert!(
        recovered
            .claim(&resumed_identity, worker.snapshot().binding())
            .is_err(),
        "deleted checkpoint must not fall back"
    );
}

#[test]
fn owner_thread_idle_policy_reaches_zero_live_vms_and_preserves_identity_and_changed_state() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let mut instance = ResidentInstance::create(
        worker(),
        CheckpointPolicy::Local,
        None,
        Some(Duration::from_millis(10)),
    )
    .unwrap();
    let identity = instance.identity().clone();
    assert_eq!(
        instance
            .invoke(
                identity.generation,
                request("before-idle").into(),
                Duration::from_secs(5),
                InvocationCancellation::default()
            )
            .0
            .unwrap(),
        InvocationResponse::Fetch(ResponseEnvelope {
            protocol_version: 1,
            request_id: "before-idle".into(),
            status: 200,
            headers: vec![],
            body_base64: "MQ==".into(),
        })
    );
    std::thread::sleep(Duration::from_millis(15));
    assert!(instance.idle_tick(Duration::from_secs(5)).unwrap());
    assert_eq!(instance.status().live_vms, 0);
    assert_eq!(instance.status().parked_instances, 1);
    instance.resume(identity.generation).unwrap();
    assert_eq!(instance.identity().instance_id, identity.instance_id);
    assert_eq!(instance.identity().generation, 2);
    assert_eq!(instance.status().live_vms, 1);
    assert!(
        instance
            .invoke(
                identity.generation,
                request("stale").into(),
                Duration::from_secs(5),
                InvocationCancellation::default()
            )
            .0
            .is_err()
    );
    let InvocationResponse::Fetch(response) = instance
        .invoke(
            2,
            request("after-idle").into(),
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .0
        .unwrap()
    else {
        panic!("wrong response kind")
    };
    assert_eq!(response.status, 500);
    instance.release(2).unwrap();
    assert_eq!(instance.status().state, InstanceState::Released);
    assert_eq!(instance.status().live_vms, 0);
}

#[test]
fn durable_policy_without_backend_and_corrupt_recovery_fail_closed() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let worker = worker();
    assert!(
        ResidentInstance::create(worker.clone(), CheckpointPolicy::default(), None, None).is_err()
    );
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("durable.db");
    let store =
        std::sync::Arc::new(CheckpointStore::open(&db_path, &[3; 32], 128 * 1024 * 1024).unwrap());
    let mut instance =
        ResidentInstance::create(worker.clone(), CheckpointPolicy::Durable, Some(store), None)
            .unwrap();
    let parked = instance.identity().clone();
    instance
        .park(parked.generation, Duration::from_secs(5))
        .unwrap();
    let wrong_key =
        std::sync::Arc::new(CheckpointStore::open(&db_path, &[4; 32], 128 * 1024 * 1024).unwrap());
    assert!(ResidentInstance::recover(worker.clone(), wrong_key, parked.clone(), None).is_err());
    let right_key =
        std::sync::Arc::new(CheckpointStore::open(&db_path, &[3; 32], 128 * 1024 * 1024).unwrap());
    assert!(
        ResidentInstance::recover(worker, right_key, parked, None).is_err(),
        "failed or fenced claim must not silently reset state"
    );
}

#[test]
fn typed_persistent_logical_policy_reconstructs_on_changed_instance_resume() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("build-elfloader/workerd-executor-fixture");
    let policy = WorkerCapabilityPolicy::default()
        .with_logical_bindings(
            LogicalBindingsPolicy::new(
                "tenant-hello",
                "checkpoint-kv-v1",
                vec![LogicalBindingConfig::Kv {
                    name: "kv".into(),
                    backing_path: tmp.path().join("data.db"),
                    read_only: false,
                    limits: Default::default(),
                }],
            )
            .unwrap(),
        )
        .unwrap();
    let worker = WorkerVersionSandbox::initialize_with_policy(
        WorkerBundle::single_script(
            WorkerVersionId::new("checkpoint-kv-v1").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default {}",
        )
        .unwrap(),
        fixture.join("rootfs.cpio"),
        fixture.join("executor"),
        64,
        Duration::from_secs(5),
        policy,
    )
    .unwrap();
    let store = std::sync::Arc::new(
        CheckpointStore::open(tmp.path().join("instance.db"), &[9; 32], 128 * 1024 * 1024).unwrap(),
    );
    let mut instance = ResidentInstance::create(
        worker.clone(),
        CheckpointPolicy::Durable,
        Some(store.clone()),
        None,
    )
    .unwrap();
    let first = instance.identity().clone();
    assert!(
        instance
            .invoke(
                first.generation,
                request("mutate").into(),
                Duration::from_secs(5),
                InvocationCancellation::default()
            )
            .0
            .is_ok()
    );
    instance
        .park(first.generation, Duration::from_secs(5))
        .unwrap();
    assert!(instance.status().checkpoint_id.is_some());
    assert_eq!(instance.status().live_vms, 0);
    instance.resume(first.generation).unwrap();
    let generation = instance.identity().generation;
    let InvocationResponse::Fetch(response) = instance
        .invoke(
            generation,
            request("resume").into(),
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .0
        .unwrap()
    else {
        panic!("wrong response kind");
    };
    assert_eq!(
        response.status, 500,
        "changed guest memory must survive reconstructed host logical callbacks"
    );
    instance.release(generation).unwrap();
}

#[test]
fn changed_checkpoint_is_not_a_template_and_reconstructs_stateless_directory_authority() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let readonly = tmp.path().join("readonly");
    let scratch = tmp.path().join("scratch");
    std::fs::create_dir(&readonly).unwrap();
    std::fs::create_dir(&scratch).unwrap();
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("build-elfloader/workerd-executor-fixture");
    let limits = hyperlight_unikraft::MountLimits {
        max_operations: Some(128),
        max_read_bytes: Some(1024),
        max_write_bytes: Some(16),
    };
    let storage = StoragePolicy::new([
        StorageBinding::read_write("scratch", &scratch, limits).unwrap(),
        StorageBinding::read_only("readonly", &readonly, limits).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        storage.bindings()[0].name(),
        "readonly",
        "mount indices must be canonical across reconstruction"
    );
    let worker = WorkerVersionSandbox::initialize_with_policy(
        WorkerBundle::single_script(
            WorkerVersionId::new("directory-checkpoint").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default {}",
        )
        .unwrap(),
        fixture.join("rootfs.cpio"),
        fixture.join("executor"),
        64,
        Duration::from_secs(5),
        WorkerCapabilityPolicy::new(
            FetchBroker::denied(),
            TimerLimits::default(),
            storage.clone(),
        ),
    )
    .unwrap();
    let mut resident = worker.restore_resident().unwrap();
    let mut invocation = request("write-before");
    invocation.url = "https://example.test/storage-rw-write".into();
    resident
        .execute(invocation, Duration::from_secs(5))
        .0
        .unwrap();
    let changed = resident
        .checkpoint(&worker, "park-dir", Duration::from_secs(5))
        .unwrap();
    let fake_template = WorkerVersionSandbox::from_verified_snapshot_with_storage(
        changed.clone(),
        FetchBroker::denied(),
        TimerLimits::default(),
        storage,
    )
    .unwrap();
    let error = fake_template
        .restore_resident()
        .err()
        .expect("changed image must never become a revision template");
    assert!(error.to_string().contains("revision template"), "{error}");
    resident.retire();
    let mut resumed = worker.restore_resident_checkpoint(&changed).unwrap();
    assert_eq!(resumed.requests_served(), 1);
    let mut invocation = request("write-after");
    invocation.url = "https://example.test/storage-rw-write".into();
    resumed
        .execute(invocation, Duration::from_secs(5))
        .0
        .unwrap();
    assert_eq!(resumed.requests_served(), 2);
    assert_eq!(
        std::fs::read(scratch.join("allowed.txt")).unwrap(),
        b"rw-ok"
    );
    let mut invocation = request("ro-after");
    invocation.url = "https://example.test/storage-ro-write-denied".into();
    let response = resumed
        .execute(invocation, Duration::from_secs(5))
        .0
        .unwrap();
    let body: serde_json::Value =
        serde_json::from_slice(&STANDARD.decode(response.body_base64).unwrap()).unwrap();
    assert_eq!(
        body["outcome"], "ro-write-denied",
        "read-only mode must be reconstructed"
    );
    assert!(!readonly.join("denied.txt").exists());
}

#[test]
fn parked_instance_frees_live_capacity_but_resume_cannot_overcommit_it() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let home = InstanceHome::new(
        worker(),
        InstanceHomeConfig {
            checkpoint_policy: CheckpointPolicy::Local,
            database_path: None,
            encryption_key_path: None,
            max_checkpoint_bytes: 128 * 1024 * 1024,
            idle_timeout_secs: None,
            checkpoint_timeout_secs: 5,
            scratch_directory: None,
        },
        1,
        2,
    )
    .unwrap();
    home.create("first", 0).unwrap();
    home.lifecycle("first", 1, LifecycleOperation::Park)
        .unwrap();
    home.create("second", 0).unwrap();
    assert!(
        home.lifecycle("first", 1, LifecycleOperation::Resume)
            .is_err()
    );
    let statuses = home.status().unwrap();
    assert_eq!(
        statuses.iter().map(|status| status.live_vms).sum::<usize>(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .find(|status| status.identity.instance_id == "first")
            .unwrap()
            .state,
        InstanceState::Parked
    );
    home.lifecycle("second", 1, LifecycleOperation::Release)
        .unwrap();
    assert_eq!(
        home.lifecycle("first", 1, LifecycleOperation::Resume)
            .unwrap()
            .identity
            .generation,
        2
    );
}
