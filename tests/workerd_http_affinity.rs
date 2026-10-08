// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor, real-binary tests for connection affinity (boundary 5):
//! missing fixture/hypervisor is a failure, not a skip (see
//! `tests/workerd_sandbox.rs`). These spawn the actual `hluk` binary, same
//! convention as `tests/workerd_host_contract.rs`.
//!
//! The acceptance criterion under test is: two requests sent on one
//! keep-alive connection to a `Sticky` app observe the same resident VM.
//! Rather than requiring a guest-visible identity signal, this is proven
//! deterministically via the existing `/__hyperlight/status` instrumentation
//! (`retirements`, `resident_requests_served`): both apps here use a
//! `max_requests_per_vm: 1` policy, which (per `ResidentPolicy`) would
//! retire-and-replace the VM after *every single request* on the normal
//! shared-pool path. A `Sticky` app's reservation deliberately bypasses that
//! proactive-retirement policy for its lifetime (see
//! `resident_pool.rs::run_reservation`), so observing `retirements == 0`
//! after several requests on one keep-alive connection is only possible if
//! every one of them hit the *same* resident VM — exactly the behavior
//! boundary 5 adds. The `None`-affinity app is the contrasting control: the
//! very same keep-alive HTTP connection handling, same policy, but no
//! reservation, so it retires on every request as boundary 1-4 already do.

use hyperlight_unikraft::workerd::{
    AppConfig, AppPoolConfig, AppRoute, ConnectionAffinity, HostConfig, ResidentPoolConfigJson,
    WorkerCapabilityPolicyConfig,
};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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

fn write_script_bundle(dir: &Path) -> PathBuf {
    let bundle = hyperlight_unikraft::workerd::WorkerBundle::single_script(
        hyperlight_unikraft::workerd::WorkerVersionId::new("http-affinity-v1").unwrap(),
        "2025-01-01",
        "worker.js",
        "export default {}",
    )
    .unwrap();
    let path = dir.join("bundle.json");
    std::fs::write(&path, serde_json::to_vec(&bundle).unwrap()).unwrap();
    path
}

fn resident_app(
    app_id: &str,
    hostname: &str,
    bundle_path: PathBuf,
    affinity: ConnectionAffinity,
) -> AppConfig {
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
            queue_capacity: 4,
            // Every request "should" retire the VM on the normal shared-pool
            // path; only a `Sticky` reservation is exempt from this policy.
            max_requests_per_vm: Some(1),
            max_lifetime_secs: None,
        }),
        connection_affinity: affinity,
        snapshot_dir: None,
        instance_home: None,
        streaming: false,
    }
}

fn hluk_bin() -> &'static str {
    env!("CARGO_BIN_EXE_hluk")
}

fn wait_for_listen_address(stderr: &mut BufReader<impl Read>) -> String {
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = stderr.read_line(&mut line).expect("read stderr");
        assert!(bytes > 0, "process exited before printing a listen line");
        if let Some(rest) = line
            .trim_end()
            .strip_prefix("workerd-host listening on http://")
        {
            return rest.split_whitespace().next().unwrap().to_string();
        }
    }
}

fn connect_with_retry(addr: &str) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => return stream,
            Err(error) if Instant::now() < deadline => {
                eprintln!("connect retry after {error}");
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("failed to connect to {addr}: {error}"),
        }
    }
}

/// Reads exactly one HTTP/1.1 response off `stream` (head + Content-Length
/// body) without consuming bytes belonging to a next response on the same
/// connection, so the caller can send another request afterwards.
fn read_one_response(stream: &mut TcpStream) -> (u16, String) {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let count = stream.read(&mut chunk).expect("read response");
        assert!(count > 0, "connection closed before a full response");
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = std::str::from_utf8(&bytes[..head_end]).unwrap().to_owned();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let content_length: usize = head
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        })
        .map(|(_, value)| value.trim().parse().unwrap())
        .unwrap_or(0);
    while bytes.len() - head_end < content_length {
        let count = stream.read(&mut chunk).expect("read response body");
        assert!(count > 0, "connection closed before response body");
        bytes.extend_from_slice(&chunk[..count]);
    }
    let body = String::from_utf8_lossy(&bytes[head_end..head_end + content_length]).into_owned();
    (status, body)
}

/// Sends one request on an already-open keep-alive connection and reads its
/// response, leaving the stream open for another request.
fn http_get_keepalive(stream: &mut TcpStream, path: &str, host: &str) -> (u16, String) {
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: keep-alive\r\n\r\n"
    )
    .unwrap();
    read_one_response(stream)
}

/// Sends a single request with `Connection: close` on a fresh connection
/// (matches `tests/workerd_host_contract.rs`'s `http_get`).
fn http_get_close(addr: &str, path: &str, host: &str) -> (u16, String) {
    let mut stream = connect_with_retry(addr);
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = parts.next().unwrap_or_default().to_string();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (status, body)
}

fn status_for_app(body: &str, app_id: &str) -> serde_json::Value {
    let parsed: serde_json::Value = serde_json::from_str(body).expect("status body is JSON");
    parsed
        .as_array()
        .expect("status body is a JSON array")
        .iter()
        .find(|entry| entry["app_id"] == app_id)
        .unwrap_or_else(|| panic!("no status entry for app {app_id}"))
        .clone()
}

#[cfg(windows)]
mod signal {
    pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const CTRL_BREAK_EVENT: u32 = 1;

    unsafe extern "system" {
        fn GenerateConsoleCtrlEvent(dw_ctrl_event: u32, dw_process_group_id: u32) -> i32;
    }

    pub fn send_graceful_shutdown(pid: u32) {
        let ok = unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid) };
        let last_error = std::io::Error::last_os_error();
        assert!(ok != 0, "GenerateConsoleCtrlEvent failed: {last_error}");
    }
}

#[cfg(unix)]
mod signal {
    pub fn send_graceful_shutdown(pid: u32) {
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("failed to invoke kill");
        assert!(status.success(), "kill -TERM failed: {status}");
    }
}

#[test]
fn smoke_two_resident_apps_serve_one_close_request_each() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let bundle_path = write_script_bundle(tmp.path());
    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![
            resident_app(
                "sticky",
                "sticky.test",
                bundle_path.clone(),
                ConnectionAffinity::Sticky,
            ),
            resident_app("plain", "plain.test", bundle_path, ConnectionAffinity::None),
        ],
    };
    let config_path = tmp.path().join("host-config.json");
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    #[cfg(windows)]
    use std::os::windows::process::CommandExt;
    let mut command = Command::new(hluk_bin());
    command
        .args([
            "workerd-host",
            "--config",
            config_path.to_str().unwrap(),
            "--bind",
            "127.0.0.1:0",
            "--drain-timeout-ms",
            "5000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    command.creation_flags(signal::CREATE_NEW_PROCESS_GROUP);
    let mut child = command.spawn().expect("failed to spawn hluk workerd-host");
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let addr = wait_for_listen_address(&mut stderr);
    eprintln!("[smoke] listening on {addr}");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, _) = http_get_close(&addr, "/__hyperlight/readyz", "sticky.test");
        eprintln!("[smoke] readyz -> {status}");
        if status == 200 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readyz never became ready in time"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!("[smoke] sending close request to sticky.test");
    let (status, _) = http_get_close(&addr, "/", "sticky.test");
    eprintln!("[smoke] sticky.test -> {status}");
    assert_eq!(status, 200);

    eprintln!("[smoke] sending close request to plain.test");
    let (status, _) = http_get_close(&addr, "/", "plain.test");
    eprintln!("[smoke] plain.test -> {status}");
    assert_eq!(status, 200);

    signal::send_graceful_shutdown(child.id());
    let deadline = Instant::now() + Duration::from_secs(15);
    let exit_status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        assert!(Instant::now() < deadline, "process did not exit in time");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(exit_status.code(), Some(0));
}

#[test]
fn sticky_connection_pins_one_resident_vm_and_plain_connection_does_not() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let bundle_path = write_script_bundle(tmp.path());
    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![
            resident_app(
                "sticky",
                "sticky.test",
                bundle_path.clone(),
                ConnectionAffinity::Sticky,
            ),
            resident_app("plain", "plain.test", bundle_path, ConnectionAffinity::None),
        ],
    };
    let config_path = tmp.path().join("host-config.json");
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();

    #[cfg(windows)]
    use std::os::windows::process::CommandExt;
    let mut command = Command::new(hluk_bin());
    command
        .args([
            "workerd-host",
            "--config",
            config_path.to_str().unwrap(),
            "--bind",
            "127.0.0.1:0",
            "--drain-timeout-ms",
            "5000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    command.creation_flags(signal::CREATE_NEW_PROCESS_GROUP);
    let mut child = command.spawn().expect("failed to spawn hluk workerd-host");
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    let addr = wait_for_listen_address(&mut stderr);

    // Poll readyz before exercising either app.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, _) = http_get_close(&addr, "/__hyperlight/readyz", "sticky.test");
        if status == 200 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readyz never became ready in time"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // --- Sticky app: three requests on one keep-alive connection. ---
    {
        let mut stream = connect_with_retry(&addr);
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        for sequence in 0..3u32 {
            let (status, _) = http_get_keepalive(&mut stream, "/", "sticky.test");
            assert_eq!(status, 200, "sticky request {sequence} failed");
        }
        // Stream drops here: the reservation is released with the
        // connection, same as a client disconnecting.
    }

    let (status, body) = http_get_close(&addr, "/__hyperlight/status", "sticky.test");
    assert_eq!(status, 200);
    let sticky_status = status_for_app(&body, "sticky");
    assert_eq!(
        sticky_status["resident_requests_served"], 3,
        "all three requests should have been served: {sticky_status}"
    );
    assert_eq!(
        sticky_status["retirements"], 0,
        "a Sticky reservation must not retire the VM mid-connection even \
         though max_requests_per_vm=1 would on the shared pool path: {sticky_status}"
    );

    // --- Plain (None-affinity) app: same policy, same keep-alive HTTP
    // connection handling, but no reservation, so every request still
    // retires the VM exactly like boundaries 1-4's existing behavior. ---
    {
        let mut stream = connect_with_retry(&addr);
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        for sequence in 0..3u32 {
            let (status, _) = http_get_keepalive(&mut stream, "/", "plain.test");
            assert_eq!(status, 200, "plain request {sequence} failed");
        }
    }

    let (status, body) = http_get_close(&addr, "/__hyperlight/status", "plain.test");
    assert_eq!(status, 200);
    let plain_status = status_for_app(&body, "plain");
    assert_eq!(plain_status["resident_requests_served"], 3);
    assert_eq!(
        plain_status["retirements"], 2,
        "without a reservation every request should retire the previous \
         VM per max_requests_per_vm=1 (the first request does not retire \
         anything; the following two each replace the prior VM): {plain_status}"
    );

    // The sticky app's admission slot (capacity 1) was released when its
    // first connection closed: a fresh connection must still succeed
    // rather than hang/timeout waiting on a leaked reservation.
    let (status, _) = http_get_close(&addr, "/", "sticky.test");
    assert_eq!(
        status, 200,
        "capacity must be released once the reserving connection closes"
    );

    signal::send_graceful_shutdown(child.id());
    let deadline = Instant::now() + Duration::from_secs(15);
    let exit_status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        assert!(Instant::now() < deadline, "process did not exit in time");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(exit_status.code(), Some(0), "clean shutdown should exit 0");
}
