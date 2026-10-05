// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::wasi_p3::{
    AdapterFuture, CliAdapter, CliGrant, ClocksAdapter, DeterministicExecutor, ExecutorStep,
    FixedTimezone, HttpAdapter, ImportDisposition, P3AdapterError, P3Adapters, P3ErrorCategory,
    P3Policy, TaskState, future, stream,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct TestClocks;

fn ready<T>(value: T) -> AdapterFuture<T> {
    let (writer, reader) = future();
    writer.complete(Ok(value)).ok();
    reader
}

impl ClocksAdapter for TestClocks {
    fn system_now(&self) -> Result<Duration, P3AdapterError> {
        Ok(Duration::from_secs(1))
    }

    fn monotonic_now(&self) -> Result<Duration, P3AdapterError> {
        Ok(Duration::from_secs(2))
    }

    fn wait_until(&self, _deadline: Duration) -> AdapterFuture<()> {
        let (writer, reader) = future();
        writer.complete(Ok(())).expect("fresh future completes");
        reader
    }
}

struct TestCli;

impl CliAdapter for TestCli {
    fn read_stdin(&self, _max_bytes: u64) -> AdapterFuture<Vec<u8>> {
        ready(Vec::new())
    }

    fn write_stdout(&self, _bytes: Vec<u8>) -> AdapterFuture<()> {
        ready(())
    }

    fn write_stderr(&self, _bytes: Vec<u8>) -> AdapterFuture<()> {
        ready(())
    }

    fn run(&self) -> AdapterFuture<u8> {
        ready(0)
    }

    fn exit(&self, _status: u8) -> AdapterFuture<()> {
        ready(())
    }
}

struct TestHttp;

impl HttpAdapter for TestHttp {
    fn request(&self, request: Vec<u8>) -> AdapterFuture<Vec<u8>> {
        ready(request)
    }
}

#[test]
fn canonical_future_stays_pending_until_producer_completion() {
    let executor = DeterministicExecutor::new();
    let (writer, reader) = future();
    let result = Arc::new(Mutex::new(None));
    let task_result = result.clone();
    let task = executor.spawn(async move {
        *task_result.lock().unwrap() = Some(reader.await);
    });

    assert_eq!(executor.step(), ExecutorStep::Pending(task));
    assert_eq!(executor.state(task), Some(TaskState::Waiting));
    assert_eq!(*result.lock().unwrap(), None);

    writer.complete(42).unwrap();
    assert_eq!(executor.step(), ExecutorStep::Completed(task));
    assert_eq!(executor.state(task), Some(TaskState::Completed));
    assert_eq!(*result.lock().unwrap(), Some(Ok(42)));
}

#[test]
fn canonical_future_cancellation_reaches_the_producer() {
    let (writer, reader) = future::<u32>();
    reader.cancel();
    assert!(writer.is_cancelled());
    assert_eq!(writer.complete(7), Err(7));
}

#[test]
fn dropping_future_consumer_cancels_the_producer() {
    let (writer, reader) = future::<u32>();
    drop(reader);
    assert!(writer.is_cancelled());
}

#[test]
fn canonical_stream_applies_backpressure_and_preserves_order() {
    let executor = DeterministicExecutor::new();
    let (mut writer, mut reader) = stream(1);
    let received = Arc::new(Mutex::new(Vec::new()));
    let consumer_output = received.clone();

    let producer = executor.spawn(async move {
        writer.send(1).await.unwrap();
        writer.send(2).await.unwrap();
        writer.close();
    });
    let consumer = executor.spawn(async move {
        while let Some(value) = reader.read().await {
            consumer_output.lock().unwrap().push(value);
        }
    });

    assert!(executor.run_until_stalled() >= 4);
    assert_eq!(executor.state(producer), Some(TaskState::Completed));
    assert_eq!(executor.state(consumer), Some(TaskState::Completed));
    assert_eq!(*received.lock().unwrap(), vec![1, 2]);
}

#[test]
fn executor_cancellation_drops_a_waiting_task() {
    let executor = DeterministicExecutor::new();
    let (_writer, reader) = future::<()>();
    let task = executor.spawn(async move {
        let _ = reader.await;
    });

    assert_eq!(executor.step(), ExecutorStep::Pending(task));
    assert!(executor.cancel(task));
    assert_eq!(executor.state(task), Some(TaskState::Cancelled));
    assert_eq!(executor.step(), ExecutorStep::Idle);
}

#[test]
fn cancelling_a_ready_task_does_not_strand_later_tasks() {
    let executor = DeterministicExecutor::new();
    let cancelled = executor.spawn(async {});
    let completed = executor.spawn(async {});

    assert!(executor.cancel(cancelled));
    assert_eq!(executor.step(), ExecutorStep::Completed(completed));
    assert_eq!(executor.step(), ExecutorStep::Idle);
}

#[test]
fn dropping_stream_consumer_releases_a_backpressured_producer() {
    let executor = DeterministicExecutor::new();
    let (mut writer, reader) = stream(1);
    let result = Arc::new(Mutex::new(None));
    let task_result = result.clone();
    let producer = executor.spawn(async move {
        writer.send(1).await.unwrap();
        *task_result.lock().unwrap() = Some(writer.send(2).await.is_err());
    });

    assert_eq!(executor.step(), ExecutorStep::Pending(producer));
    drop(reader);
    assert_eq!(executor.step(), ExecutorStep::Completed(producer));
    assert_eq!(*result.lock().unwrap(), Some(true));
}

#[test]
fn import_mapping_is_version_locked_and_fail_closed() {
    let denied = P3Adapters::deny_all();
    assert_eq!(
        denied
            .import_disposition("wasi:cli/terminal-input@0.3.1")
            .unwrap(),
        ImportDisposition::Denied
    );
    assert_eq!(
        denied
            .import_disposition("wasi:http/handler@0.3.1")
            .unwrap(),
        ImportDisposition::Deferred
    );
    for import in [
        "wasi:http/client@0.3.1",
        "wasi:clocks/types@0.3.1",
        "wasi:cli/types@0.3.1",
        "wasi:cli/environment@0.3.1",
        "wasi:cli/stdin@0.3.1",
        "wasi:cli/stdout@0.3.1",
        "wasi:cli/stderr@0.3.1",
        "wasi:cli/run@0.3.1",
        "wasi:cli/exit@0.3.1",
    ] {
        assert_eq!(
            denied.import_disposition(import).unwrap(),
            ImportDisposition::Denied,
            "{import}"
        );
    }

    for import in [
        "wasi:clocks/system-clock",
        "wasi:clocks/system-clock@0.3.2",
        "vendor:process/spawn@0.3.1",
    ] {
        let error = denied.validate_imports([import]).unwrap_err();
        let expected = if import == "vendor:process/spawn@0.3.1" {
            P3ErrorCategory::CapabilityDenied
        } else {
            P3ErrorCategory::UnsupportedImport
        };
        assert_eq!(error.category(), expected);
        assert_eq!(error.interface(), import);
    }

    let http = P3Adapters::new(P3Policy::deny_all().with_http_client()).with_http(TestHttp);
    for import in ["wasi:http/types@0.3.1", "wasi:http/client@0.3.1"] {
        assert_eq!(
            http.import_disposition(import).unwrap(),
            ImportDisposition::Allowed
        );
    }
    assert_eq!(
        http.import_disposition("wasi:http/handler@0.3.1").unwrap(),
        ImportDisposition::Deferred
    );
}

#[test]
fn granted_clock_import_requires_a_typed_adapter() {
    let policy = P3Policy::deny_all().with_clocks(true, true, None).unwrap();
    let missing = P3Adapters::new(policy.clone());
    assert_eq!(
        missing
            .validate_imports(["wasi:clocks/monotonic-clock@0.3.1"])
            .unwrap_err()
            .category(),
        P3ErrorCategory::CapabilityDenied
    );

    let configured = P3Adapters::new(policy).with_clocks(TestClocks);
    configured
        .validate_imports([
            "wasi:clocks/system-clock@0.3.1",
            "wasi:clocks/monotonic-clock@0.3.1",
            "wasi:clocks/types@0.3.1",
        ])
        .unwrap();
}

#[test]
fn cli_environment_and_timezone_configuration_are_bounded() {
    let mut environment = BTreeMap::new();
    environment.insert("TOKEN".to_string(), "not-inherited".to_string());
    let valid = CliGrant {
        environment,
        max_environment_entries: 1,
        max_environment_key_bytes: 16,
        max_environment_value_bytes: 32,
        max_environment_bytes: 64,
        max_stdin_bytes: 1024,
        max_stdout_bytes: 1024,
        max_stderr_bytes: 1024,
        allow_exit: false,
    };
    let adapters = P3Adapters::new(P3Policy::deny_all().with_cli(valid).unwrap()).with_cli(TestCli);
    for import in [
        "wasi:cli/types@0.3.1",
        "wasi:cli/environment@0.3.1",
        "wasi:cli/stdin@0.3.1",
        "wasi:cli/stdout@0.3.1",
        "wasi:cli/stderr@0.3.1",
        "wasi:cli/run@0.3.1",
    ] {
        assert_eq!(
            adapters.import_disposition(import).unwrap(),
            ImportDisposition::Allowed,
            "{import}"
        );
    }
    assert_eq!(
        adapters.import_disposition("wasi:cli/exit@0.3.1").unwrap(),
        ImportDisposition::Denied
    );

    let invalid_timezone = FixedTimezone {
        name: "../host".to_string(),
        utc_offset_seconds: 0,
    };
    assert_eq!(
        P3Policy::deny_all()
            .with_clocks(true, true, Some(invalid_timezone))
            .unwrap_err()
            .category(),
        P3ErrorCategory::InvalidArgument
    );
}

#[test]
fn listener_raw_device_and_http_service_fail_with_stable_categories() {
    let adapters = P3Adapters::deny_all();
    assert_eq!(
        adapters.deny_listener("listen").category(),
        P3ErrorCategory::CapabilityDenied
    );
    assert_eq!(
        adapters.deny_raw_device("get-terminal").category(),
        P3ErrorCategory::CapabilityDenied
    );
    assert_eq!(
        adapters.defer_http_service("handle").category(),
        P3ErrorCategory::NotSupported
    );
    assert_eq!(
        adapters.defer_udp("send").category(),
        P3ErrorCategory::NotSupported
    );
}

#[test]
fn policy_abi_categories_use_stable_codes() {
    let categories = [
        (P3ErrorCategory::UnsupportedImport, "unsupported-import"),
        (P3ErrorCategory::CapabilityDenied, "capability-denied"),
        (P3ErrorCategory::InvalidArgument, "invalid-argument"),
        (P3ErrorCategory::QuotaExceeded, "quota-exceeded"),
        (P3ErrorCategory::NotSupported, "not-supported"),
        (P3ErrorCategory::HostFailure, "host-failure"),
        (P3ErrorCategory::InvalidHandle, "invalid-handle"),
        (P3ErrorCategory::SizeLimit, "size-limit"),
        (P3ErrorCategory::Timeout, "timeout"),
        (P3ErrorCategory::Canceled, "canceled"),
    ];
    for (category, code) in categories {
        assert_eq!(category.code(), code);
    }
}

#[test]
fn authoritative_lock_excludes_p3_2_and_records_contract_hashes() {
    let lock: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("experiments/workerd-component-model/wasi-p3-v0.3.1.lock.json"),
        )
        .unwrap(),
    )
    .unwrap();

    assert_eq!(
        lock["spec"]["commit"],
        "59e48bfe3fae9bf2480eb15abd8f55999eb3b395"
    );
    assert_eq!(lock["spec"]["excludedRelease"]["version"], "v0.3.2");
    assert_eq!(
        lock["authoritativeContracts"]["wasiP3Lock"]["sha256"],
        "afc52d6cf56e16f86d06f15cb7da801802127c7fa424b468478b1cfc74bf119c"
    );
    assert_eq!(
        lock["authoritativeContracts"]["sharedPolicySha256"],
        "d4f1316d59594640688a2d8a3875fea675dc51196f36e19024d275675e273447"
    );
    assert_eq!(
        lock["authoritativeContracts"]["exclusionsSha256"],
        "e43209923e1574ac7bd11f3edb1be2348ec39592e9f47d432b1a839b0bad1754"
    );
    assert_eq!(
        lock["authoritativeContracts"]["proofContractSha256"],
        "31338f5b571b2b4fa9e8631f2f248f470c6cca49876a63afc60c29561399247d"
    );
    assert_eq!(
        lock["authoritativeContracts"]["interfaceMatrixSha256"],
        "65027e05ad5322583e89094265bec1aafdef09028c18c3ddcc996f1bb3d674b2"
    );
    assert_eq!(
        lock["authoritativeContracts"]["contractVerifierSha256"],
        "2bca4b4af21aa76ae2193dcd37e334b563d06f9b3df0c8f9cc8cfc50162f56a5"
    );
    assert_eq!(
        lock["authoritativeContracts"]["contractVerifierResult"],
        "contracts-ok"
    );
    assert_eq!(
        lock["authoritativeContracts"]["complianceReviewSha256"],
        "276ddde19a8e0c99720409533460f3a73580999e218cc948a9f5890627edf763"
    );
    assert_eq!(
        lock["authoritativeContracts"]["complianceVerifierSha256"],
        "2bca4b4af21aa76ae2193dcd37e334b563d06f9b3df0c8f9cc8cfc50162f56a5"
    );
    assert!(
        lock["lowering"]["jcoArguments"]
            .as_array()
            .unwrap()
            .iter()
            .any(|argument| argument == "--strict")
    );
    assert_eq!(
        lock["toolchain"]["preview3Shim"]["installableVersion"],
        "0.7.0"
    );
    assert_eq!(
        lock["toolchain"]["preview3Shim"]["unavailableUpstreamTag"]["version"],
        "0.8.1"
    );
}
