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
    let source = &bundle.modules[0].source;
    assert!(source.contains("/case/"));
    assert!(source.matches("response.body.getReader()").count() >= 3);
    assert!(source.contains("too many concurrent"));

    let workerd_demo = WorkerBundle::from_path(root.join("workerd-wintertc-demo.json")).unwrap();
    workerd_demo.to_canonical_json().unwrap();
    assert!(workerd_demo.modules[0].source.contains("/evidence/"));
    assert!(!workerd_demo.modules[0].source.contains("/case/"));
    assert_eq!(workerd_demo.compatibility_date, "2025-12-31");
    assert!(
        workerd_demo
            .compatibility_flags
            .iter()
            .any(|flag| flag == "worker_global_scope_event_handlers")
    );
    assert!(
        workerd_demo
            .compatibility_flags
            .iter()
            .any(|flag| flag == "message_port_standard_semantics")
    );

    let pool_benchmark = WorkerBundle::from_path(root.join("workerd-pool-benchmark.json")).unwrap();
    pool_benchmark.to_canonical_json().unwrap();
    let pool_source = &pool_benchmark.modules[0].source;
    assert!(pool_source.contains("url.pathname === '/sync'"));
    assert!(pool_source.contains("'x'.repeat(9441)"));
    let pool_script = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/run-wintertc-pool-benchmark.sh"),
    )
    .unwrap();
    for expected in [
        "\"requests\": 320",
        "\"concurrency\": 32",
        "\"payload_bytes\": 9441",
        "\"throughput_requests_per_second\": 291.3588",
        "\"p50_ms\": 107.3",
        "\"p95_ms\": 121.3",
        "\"p99_ms\": 135.4",
        "[\"-z\", \"60s\", \"-c\", str(concurrency)]",
        "for concurrency in (32, 64, 128)",
        "performance FAIL: exact baseline was not beaten",
    ] {
        assert!(
            pool_script.contains(expected),
            "pool benchmark is missing frozen requirement {expected}"
        );
    }
    let archive_script = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/archive-wintertc-evidence.sh"),
    )
    .unwrap();
    for expected in [
        "find . -type f ! -path './SHA256SUMS' -print0",
        "sha256sum --check SHA256SUMS",
        "archive must be outside the export directory",
    ] {
        assert!(
            archive_script.contains(expected),
            "archive script is missing checksum safeguard {expected}"
        );
    }

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
        "wasm-component-or-core-fallback",
        "message-channel",
        "file",
        "byob",
        "timer-presence",
        "timer-timeout-ordering",
        "timer-cancellation",
        "offline-restore-cold-vs-restored-latency",
        "offline-restore-repeated-cycles",
        "offline-restore-pool-depletion-refill",
        "offline-restore-throughput-latency",
        "offline-restore-timer-after-restore",
        "offline-restore-network-after-restore",
        "offline-restore-tenant-isolation",
        "offline-restore-cancellation-recovery",
        "offline-restore-clean-shutdown",
    ] {
        assert!(ids.contains(required), "missing case {required}");
    }
}
