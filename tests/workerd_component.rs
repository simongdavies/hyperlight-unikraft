// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::workerd::{
    ModuleType, PROTOCOL_VERSION, WorkerBundle, WorkerModule, WorkerVersionId,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("experiments/workerd-component-model")
}

fn bundle() -> WorkerBundle {
    let root = fixture();
    WorkerBundle {
        protocol_version: PROTOCOL_VERSION,
        worker_version: WorkerVersionId::new("component-model-hyperlight-v1").unwrap(),
        compatibility_date: "2026-09-25".into(),
        compatibility_flags: Vec::new(),
        main_module: "worker.mjs".into(),
        modules: vec![
            WorkerModule {
                name: "worker.mjs".into(),
                module_type: ModuleType::EsModule,
                source: fs::read_to_string(root.join("worker.mjs")).unwrap(),
            },
            WorkerModule::wasm(
                "generated/component.core.wasm",
                &fs::read(root.join("generated/component.core.wasm")).unwrap(),
            ),
            WorkerModule {
                name: "generated/component.js".into(),
                module_type: ModuleType::EsModule,
                source: fs::read_to_string(root.join("generated/component.js")).unwrap(),
            },
        ],
    }
}

#[test]
fn component_fixture_fits_the_canonical_guest_protocol() {
    let canonical = bundle().to_canonical_json().unwrap();
    let parsed = WorkerBundle::from_json(canonical.as_bytes()).unwrap();
    assert_eq!(parsed, bundle());
}

#[test]
fn component_fixture_identity_is_stable() {
    assert_eq!(
        bundle().sha256().unwrap(),
        "13499a5b6c88e082da9f53908b655452dced3bb8bc0d181aeadf1aeb215f520b"
    );
}

#[test]
fn component_bundle_lock_identity_is_stable() {
    assert_eq!(
        Sha256::digest(fs::read(fixture().join("bundle.lock.json")).unwrap())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        "fbd7e3688a88e647fd0816ccffbff58dcb923e2651093dcee7812e5f70097dff"
    );
}

#[test]
fn wasm_modules_reject_non_base64_source() {
    let mut invalid = bundle();
    invalid.modules[1].source = "not base64!".into();
    assert!(invalid.validate().is_err());
}
