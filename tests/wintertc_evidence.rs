// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::workerd::WorkerBundle;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

#[derive(Deserialize)]
struct Manifest {
    schema_version: u32,
    acceptance_threshold: AcceptanceThreshold,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct AcceptanceThreshold {
    compliance_rule: String,
    authoritative_sources: Vec<String>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    requirement: String,
    expected: String,
}

#[test]
fn wintertc_evidence_bundle_and_manifest_are_bounded_and_complete() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/workerd-bundles");
    let bundle = WorkerBundle::from_path(root.join("wintertc-evidence.json")).unwrap();
    bundle.to_canonical_json().unwrap();

    let manifest: Manifest =
        serde_json::from_slice(&fs::read(root.join("wintertc-evidence-manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest.schema_version, 1);
    assert!(!manifest.acceptance_threshold.compliance_rule.is_empty());
    assert!(
        manifest
            .acceptance_threshold
            .authoritative_sources
            .iter()
            .any(|source| source.contains("webmessaging/message-channels"))
    );
    let mut ids = HashSet::new();
    for case in &manifest.cases {
        assert!(ids.insert(case.id.clone()), "duplicate case {}", case.id);
        assert!(
            ["required", "optional", "policy", "external_pending"]
                .contains(&case.requirement.as_str())
        );
        assert!(
            [
                "pass",
                "pass_or_unsupported",
                "policy_denied",
                "dns_denied",
                "pending_timer_branch",
                "pending_external_artifact"
            ]
            .contains(&case.expected.as_str())
        );
    }
    for required in [
        "api-presence",
        "capability-free-behavior",
        "worker-global-handler-properties",
        "webassembly-eval-denial",
        "webassembly-hostile-limits-wpt",
        "message-port-wpt",
        "readable-byte-streams-wpt",
        "fetch-v1-get",
        "fetch-v1-post",
        "fetch-v1-ordered-duplicate-headers",
        "fetch-v2-large-upload",
        "fetch-v2-large-download",
        "fetch-v2-unknown-upload",
        "fetch-v2-unknown-download",
        "fetch-v2-slow-producer",
        "fetch-v2-slow-consumer",
        "fetch-v2-backpressure",
        "fetch-v2-early-response",
        "fetch-v2-cancellation",
        "fetch-redirect",
        "fetch-policy-denial",
        "fetch-dns-denial",
        "fetch-timeout",
        "fetch-overload",
        "webassembly",
        "message-channel",
        "file",
        "byob",
        "timer-presence",
        "timer-timeout-ordering",
        "timer-cancellation",
    ] {
        assert!(ids.contains(required), "missing case {required}");
    }
}
