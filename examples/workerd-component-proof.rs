// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    Header, ModuleType, PROTOCOL_VERSION, RequestEnvelope, ResponseEnvelope, WorkerBundle,
    WorkerModule, WorkerVersionId, WorkerVersionSandbox,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const FIXTURE_DIR: &str = "experiments/workerd-component-model";
const DEFAULT_ROOTFS: &str = "build-elfloader/workerd-executor/rootfs.img";
const DEFAULT_EXECUTOR: &str = "build-elfloader/workerd-executor/executor";
const DEFAULT_SCRATCH_MIB: usize = 344;
const ITERATIONS: usize = 100;
const EXPECTED_BUNDLE_LOCK_SHA256: &str =
    "fbd7e3688a88e647fd0816ccffbff58dcb923e2651093dcee7812e5f70097dff";
const EXPECTED_RESPONSE_SHA256: &str =
    "70f0274a7fe585abc7aca8e430ec2ba8a66d087486d20efac3647d8376ed4fa0";

#[derive(Debug, Serialize)]
struct RouteEvidence {
    url: String,
    status: u16,
    body: String,
}

#[derive(Debug, Serialize)]
struct ProofEvidence {
    schema_version: u16,
    worker_version: String,
    bundle_sha256: String,
    component_bundle_lock_sha256: String,
    component_wasm_sha256: String,
    iterations: usize,
    deterministic_response_sha256: String,
    routes: Vec<RouteEvidence>,
}

fn sha256(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn component_bundle(root: &Path) -> Result<WorkerBundle, Box<dyn std::error::Error>> {
    let read = |path: &str| fs::read_to_string(root.join(path));
    let mut bundle = WorkerBundle {
        protocol_version: PROTOCOL_VERSION,
        worker_version: WorkerVersionId::new("component-model-hyperlight-v1")?,
        compatibility_date: "2026-09-25".into(),
        compatibility_flags: Vec::new(),
        main_module: "worker.mjs".into(),
        modules: vec![
            WorkerModule {
                name: "worker.mjs".into(),
                module_type: ModuleType::EsModule,
                source: read("worker.mjs")?,
            },
            WorkerModule::wasm(
                "generated/component.core.wasm",
                &fs::read(root.join("generated/component.core.wasm"))?,
            ),
            WorkerModule {
                name: "generated/component.js".into(),
                module_type: ModuleType::EsModule,
                source: read("generated/component.js")?,
            },
        ],
    };
    bundle = WorkerBundle::from_json(bundle.to_canonical_json()?.as_bytes())?;
    Ok(bundle)
}

fn request(id: &str, url: String) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: id.into(),
        method: "GET".into(),
        url,
        headers: Vec::<Header>::new(),
        body_base64: String::new(),
    }
}

fn decode(response: ResponseEnvelope) -> Result<RouteEvidence, Box<dyn std::error::Error>> {
    Ok(RouteEvidence {
        url: String::new(),
        status: response.status,
        body: String::from_utf8(STANDARD.decode(response.body_base64)?)?,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let rootfs = PathBuf::from(args.next().unwrap_or_else(|| DEFAULT_ROOTFS.into()));
    let executor = PathBuf::from(args.next().unwrap_or_else(|| DEFAULT_EXECUTOR.into()));
    let scratch_mib = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_SCRATCH_MIB);
    if args.next().is_some() {
        return Err("usage: workerd-component-proof [ROOTFS] [EXECUTOR] [SCRATCH_MIB]".into());
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_DIR);
    let bundle = component_bundle(&root)?;
    let bundle_sha256 = bundle.sha256()?;
    let component_bundle_lock_sha256 = sha256(&fs::read(root.join("bundle.lock.json"))?);
    if component_bundle_lock_sha256 != EXPECTED_BUNDLE_LOCK_SHA256 {
        return Err(format!(
            "Component bundle-lock identity {component_bundle_lock_sha256}, expected {EXPECTED_BUNDLE_LOCK_SHA256}"
        )
        .into());
    }
    let component_wasm_sha256 = sha256(&fs::read(root.join("component.wasm"))?);
    let version = bundle.worker_version.clone();
    let worker = WorkerVersionSandbox::initialize(
        bundle,
        rootfs,
        executor,
        scratch_mib,
        Duration::from_secs(90),
    )?;

    let cases = [
        (
            "health",
            "https://component.test/health",
            200,
            Some(r#"{"ok":true,"path":"jco-transpiled-component"}"#),
        ),
        (
            "add",
            "https://component.test/add?left=20&right=22",
            200,
            Some(r#"{"result":42}"#),
        ),
        (
            "invalid",
            "https://component.test/add?left=x&right=22",
            400,
            Some(r#"{"error":"left must be a base-10 integer"}"#),
        ),
        ("network", "https://component.test/network-probe", 200, None),
    ];
    let mut routes = Vec::with_capacity(cases.len() + 1);
    for (id, url, status, expected_body) in cases {
        let mut evidence = decode(worker.execute(
            &version,
            request(id, url.into()),
            Duration::from_millis(50),
        )?)?;
        evidence.url = url.into();
        if evidence.status != status {
            return Err(format!("{id} returned {}, expected {status}", evidence.status).into());
        }
        if let Some(expected_body) = expected_body
            && evidence.body != expected_body
        {
            return Err(format!("{id} returned an unexpected body: {}", evidence.body).into());
        }
        if id == "network"
            && serde_json::from_str::<serde_json::Value>(&evidence.body)?["blocked"] != true
        {
            return Err("network probe did not prove deny-all egress".into());
        }
        routes.push(evidence);
    }

    let oversized_url = format!(
        "https://component.test/add?left=20&right=22&pad={}",
        "x".repeat(1024)
    );
    let mut oversized = decode(worker.execute(
        &version,
        request("oversized", oversized_url.clone()),
        Duration::from_millis(50),
    )?)?;
    oversized.url = oversized_url;
    if oversized.status != 413 {
        return Err(format!(
            "oversized request returned {}, expected 413",
            oversized.status
        )
        .into());
    }
    if oversized.body != r#"{"error":"request exceeds policy limit"}"# {
        return Err(format!(
            "oversized request returned an unexpected body: {}",
            oversized.body
        )
        .into());
    }
    routes.push(oversized);

    let mut deterministic = Sha256::new();
    for sequence in 0..ITERATIONS {
        let response = worker.execute(
            &version,
            request(
                &format!("determinism-{sequence}"),
                "https://component.test/add?left=20&right=22".into(),
            ),
            Duration::from_millis(50),
        )?;
        if response.status != 200 {
            return Err(format!(
                "determinism iteration {sequence} returned {}",
                response.status
            )
            .into());
        }
        let body = STANDARD.decode(response.body_base64)?;
        if body != br#"{"result":42}"# {
            return Err(format!("determinism iteration {sequence} returned the wrong body").into());
        }
        deterministic.update(body);
    }
    let deterministic_response_sha256 = hex_digest(deterministic.finalize());
    if deterministic_response_sha256 != EXPECTED_RESPONSE_SHA256 {
        return Err(format!(
            "100-response digest {deterministic_response_sha256}, expected {EXPECTED_RESPONSE_SHA256}"
        )
        .into());
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&ProofEvidence {
            schema_version: 1,
            worker_version: version.as_str().into(),
            bundle_sha256,
            component_bundle_lock_sha256,
            component_wasm_sha256,
            iterations: ITERATIONS,
            deterministic_response_sha256,
            routes,
        })?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalized_response_digest_is_encoded_without_rehashing() {
        let mut digest = Sha256::new();
        for _ in 0..ITERATIONS {
            digest.update(br#"{"result":42}"#);
        }
        assert_eq!(hex_digest(digest.finalize()), EXPECTED_RESPONSE_SHA256);
    }

    #[test]
    fn component_bundle_lock_identity_matches_the_acceptance_contract() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_DIR);
        assert_eq!(
            sha256(&fs::read(root.join("bundle.lock.json")).unwrap()),
            EXPECTED_BUNDLE_LOCK_SHA256
        );
    }
}
