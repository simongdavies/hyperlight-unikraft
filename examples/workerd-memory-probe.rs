// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    ExecutionProfile, Header, InitializationProfile, PROTOCOL_VERSION, RequestEnvelope,
    WorkerBundle, WorkerVersionId, WorkerVersionSandbox,
};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct MemorySample {
    current_working_set_bytes: u64,
    peak_working_set_bytes: u64,
}

#[derive(Serialize)]
struct ProbeResult {
    scratch_mb: usize,
    stage: String,
    result: String,
    error: Option<String>,
    initialization: InitializationProfile,
    snapshot_bytes: Option<u64>,
    snapshot_page_estimate: Option<u64>,
    rootfs_bytes: u64,
    executor_bytes: u64,
    bundle_sha256: String,
    memory_after_init: Option<MemorySample>,
    memory_after_hello: Option<MemorySample>,
    memory_after_busy: Option<MemorySample>,
    memory_after_recovery: Option<MemorySample>,
    hello_ms: Option<f64>,
    busy_ms: Option<f64>,
    busy_timed_out: Option<bool>,
    recovery_ms: Option<f64>,
    hello_profile: Option<ExecutionProfile>,
    busy_profile: Option<ExecutionProfile>,
    recovery_profile: Option<ExecutionProfile>,
    hello_status: Option<u16>,
    recovery_status: Option<u16>,
}

fn request(id: &str, path: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: id.into(),
        method: "POST".into(),
        url: format!("http://memory.test/{path}"),
        headers: vec![Header {
            name: "x-demo".into(),
            value: id.into(),
        }],
        body_base64: STANDARD.encode(format!("body-{id}")),
    }
}

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    let mut args = std::env::args().skip(1);
    let scratch_mb = args
        .next()
        .and_then(|value| value.parse().ok())
        .expect("usage: workerd-memory-probe SCRATCH_MIB [--bundle JSON | --script JS]");
    let mut bundle_path = PathBuf::from("examples/workerd-bundles/acceptance.json");
    let mut script_path = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--bundle" => {
                bundle_path = args.next().expect("missing --bundle path").into();
                script_path = None;
            }
            "--script" => {
                script_path = Some(PathBuf::from(args.next().expect("missing --script path")));
            }
            _ => panic!("unknown argument: {argument}"),
        }
    }
    let rootfs = PathBuf::from("build-elfloader/workerd-executor/rootfs.img");
    let executor = PathBuf::from("build-elfloader/workerd-executor/executor");
    let rootfs_bytes = fs::metadata(&rootfs).map(|m| m.len()).unwrap_or(0);
    let executor_bytes = fs::metadata(&executor).map(|m| m.len()).unwrap_or(0);
    let bundle = if let Some(path) = script_path {
        WorkerBundle::single_script(
            WorkerVersionId::new("memory-v1").unwrap(),
            "2025-01-01",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("worker.js"),
            fs::read_to_string(&path).unwrap(),
        )
        .unwrap()
    } else {
        WorkerBundle::from_path(bundle_path).unwrap()
    };
    let bundle_sha256 = bundle.sha256().unwrap();
    let version = bundle.worker_version.clone();
    let initialized = WorkerVersionSandbox::initialize_profiled(
        bundle,
        &rootfs,
        &executor,
        scratch_mb,
        Duration::from_secs(90),
    );
    let (worker, initialization) = match initialized {
        Ok(value) => value,
        Err(failure) => {
            emit(ProbeResult {
                scratch_mb,
                stage: failure.stage.into(),
                result: "failed".into(),
                error: Some(failure.message),
                initialization: failure.profile,
                snapshot_bytes: None,
                snapshot_page_estimate: None,
                rootfs_bytes,
                executor_bytes,
                bundle_sha256,
                memory_after_init: Some(memory()),
                memory_after_hello: None,
                memory_after_busy: None,
                memory_after_recovery: None,
                hello_ms: None,
                busy_ms: None,
                busy_timed_out: None,
                recovery_ms: None,
                hello_profile: None,
                busy_profile: None,
                recovery_profile: None,
                hello_status: None,
                recovery_status: None,
            });
            return;
        }
    };
    let memory_after_init = memory();
    let snapshot_dir = std::env::temp_dir().join(format!(
        "hluk-workerd-memory-{}-{}",
        std::process::id(),
        scratch_mb
    ));
    let (snapshot_bytes, snapshot_page_estimate) = match worker.snapshot().save(&snapshot_dir) {
        Ok(()) => {
            let bytes = directory_bytes(&snapshot_dir).ok();
            unseal(&snapshot_dir);
            let _ = fs::remove_dir_all(&snapshot_dir);
            (bytes, bytes.map(|value| value.div_ceil(4096)))
        }
        Err(error) => {
            emit(failed(
                scratch_mb,
                "snapshot-save",
                error.to_string(),
                initialization,
                rootfs_bytes,
                executor_bytes,
                bundle_sha256,
                memory_after_init,
            ));
            return;
        }
    };
    let started = Instant::now();
    let (hello, hello_profile) =
        worker.execute_profiled(&version, request("hello", "hello"), Duration::from_secs(30));
    let hello_ms = elapsed_ms(started);
    let hello_status = match hello {
        Ok(response) => Some(response.status),
        Err(error) => {
            emit(failed_with_snapshot(
                scratch_mb,
                "hello",
                error.to_string(),
                initialization,
                rootfs_bytes,
                executor_bytes,
                bundle_sha256.clone(),
                memory_after_init,
                snapshot_bytes,
                snapshot_page_estimate,
                hello_ms,
            ));
            return;
        }
    };
    let memory_after_hello = memory();
    let started = Instant::now();
    let (busy, busy_profile) = worker.execute_profiled(
        &version,
        request("busy", "busy"),
        Duration::from_millis(500),
    );
    let busy_ms = elapsed_ms(started);
    let busy_timed_out = matches!(busy, Err(hyperlight_unikraft::workerd::Error::Timeout));
    if !busy_timed_out {
        emit(failed_with_snapshot(
            scratch_mb,
            "busy",
            format!("expected timeout, got {busy:?}"),
            initialization,
            rootfs_bytes,
            executor_bytes,
            bundle_sha256.clone(),
            memory_after_init,
            snapshot_bytes,
            snapshot_page_estimate,
            hello_ms,
        ));
        return;
    }
    let memory_after_busy = memory();
    let started = Instant::now();
    let (recovery, recovery_profile) =
        worker.execute_profiled(&version, request("after", "after"), Duration::from_secs(30));
    let recovery_ms = elapsed_ms(started);
    let recovery_status = match recovery {
        Ok(response) => Some(response.status),
        Err(error) => {
            emit(failed_with_snapshot(
                scratch_mb,
                "after",
                error.to_string(),
                initialization,
                rootfs_bytes,
                executor_bytes,
                bundle_sha256.clone(),
                memory_after_init,
                snapshot_bytes,
                snapshot_page_estimate,
                hello_ms,
            ));
            return;
        }
    };
    emit(ProbeResult {
        scratch_mb,
        stage: "after".into(),
        result: "passed".into(),
        error: None,
        initialization,
        snapshot_bytes,
        snapshot_page_estimate,
        rootfs_bytes,
        executor_bytes,
        bundle_sha256,
        memory_after_init: Some(memory_after_init),
        memory_after_hello: Some(memory_after_hello),
        memory_after_busy: Some(memory_after_busy),
        memory_after_recovery: Some(memory()),
        hello_ms: Some(hello_ms),
        busy_ms: Some(busy_ms),
        busy_timed_out: Some(busy_timed_out),
        recovery_ms: Some(recovery_ms),
        hello_profile: Some(hello_profile),
        busy_profile: Some(busy_profile),
        recovery_profile: Some(recovery_profile),
        hello_status,
        recovery_status,
    });
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn emit(result: ProbeResult) {
    println!("{}", serde_json::to_string(&result).unwrap());
}

#[allow(clippy::too_many_arguments)]
fn failed(
    scratch_mb: usize,
    stage: &str,
    error: String,
    initialization: InitializationProfile,
    rootfs_bytes: u64,
    executor_bytes: u64,
    bundle_sha256: String,
    memory_after_init: MemorySample,
) -> ProbeResult {
    ProbeResult {
        scratch_mb,
        stage: stage.into(),
        result: "failed".into(),
        error: Some(error),
        initialization,
        snapshot_bytes: None,
        snapshot_page_estimate: None,
        rootfs_bytes,
        executor_bytes,
        bundle_sha256,
        memory_after_init: Some(memory_after_init),
        memory_after_hello: None,
        memory_after_busy: None,
        memory_after_recovery: None,
        hello_ms: None,
        busy_ms: None,
        busy_timed_out: None,
        recovery_ms: None,
        hello_profile: None,
        busy_profile: None,
        recovery_profile: None,
        hello_status: None,
        recovery_status: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn failed_with_snapshot(
    scratch_mb: usize,
    stage: &str,
    error: String,
    initialization: InitializationProfile,
    rootfs_bytes: u64,
    executor_bytes: u64,
    bundle_sha256: String,
    memory_after_init: MemorySample,
    snapshot_bytes: Option<u64>,
    snapshot_page_estimate: Option<u64>,
    hello_ms: f64,
) -> ProbeResult {
    let mut result = failed(
        scratch_mb,
        stage,
        error,
        initialization,
        rootfs_bytes,
        executor_bytes,
        bundle_sha256,
        memory_after_init,
    );
    result.snapshot_bytes = snapshot_bytes;
    result.snapshot_page_estimate = snapshot_page_estimate;
    result.hello_ms = Some(hello_ms);
    result
}

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    let mut bytes = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            bytes += directory_bytes(&entry.path())?;
        } else {
            bytes += metadata.len();
        }
    }
    Ok(bytes)
}

#[allow(clippy::permissions_set_readonly_false)]
fn unseal(path: &Path) {
    if let Ok(metadata) = fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_readonly(false);
        let _ = fs::set_permissions(path, permissions);
        if metadata.is_dir()
            && let Ok(entries) = fs::read_dir(path)
        {
            for entry in entries.flatten() {
                unseal(&entry.path());
            }
        }
    }
}

#[cfg(windows)]
fn memory() -> MemorySample {
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    let ok = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &raw mut counters, size) };
    assert_ne!(ok, 0, "K32GetProcessMemoryInfo failed");
    MemorySample {
        current_working_set_bytes: counters.WorkingSetSize as u64,
        peak_working_set_bytes: counters.PeakWorkingSetSize as u64,
    }
}

#[cfg(not(windows))]
fn memory() -> MemorySample {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    MemorySample {
        current_working_set_bytes: proc_status_bytes(&status, "VmRSS:"),
        peak_working_set_bytes: proc_status_bytes(&status, "VmHWM:"),
    }
}

#[cfg(not(windows))]
fn proc_status_bytes(status: &str, key: &str) -> u64 {
    status
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix(key)?.trim();
            let kib = value.strip_suffix(" kB")?.trim().parse::<u64>().ok()?;
            kib.checked_mul(1024)
        })
        .unwrap_or(0)
}
