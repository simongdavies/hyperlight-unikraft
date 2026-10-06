// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Real hypervisor, real-binary tests for `hluk workerd-host`'s
//! orchestrator contract (boundary 4): missing fixture/hypervisor is a
//! failure, not a skip (see `tests/workerd_sandbox.rs`). These spawn the
//! actual `hluk` binary (`std::process::Command`, matching `tests/cli.rs`'s
//! existing convention — no new test-harness dependency needed) rather than
//! calling library code directly, since the contract under test is the CLI
//! subcommand's process lifecycle: listener bind order, exit codes, and
//! signal-driven drain.

use hyperlight_unikraft::workerd::{
    AppConfig, AppPoolConfig, AppRoute, ConnectionAffinity, DisposablePoolConfig, HostConfig,
    WorkerCapabilityPolicyConfig,
};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
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

/// Writes a minimal single-script bundle to `dir`, matching the on-disk
/// shape `HostConfig`'s `bundle_path` expects (see
/// `tests/workerd_app_registry.rs`). The lightweight fixture executor does
/// not run real JS: only routing/lifecycle are exercised here.
fn write_script_bundle(dir: &Path) -> PathBuf {
    let bundle = hyperlight_unikraft::workerd::WorkerBundle::single_script(
        hyperlight_unikraft::workerd::WorkerVersionId::new("host-contract-v1").unwrap(),
        "2025-01-01",
        "worker.js",
        "export default {}",
    )
    .unwrap();
    let path = dir.join("bundle.json");
    std::fs::write(&path, serde_json::to_vec(&bundle).unwrap()).unwrap();
    path
}

fn write_host_config(
    dir: &Path,
    apps: Vec<AppConfig>,
    rootfs: PathBuf,
    executor: PathBuf,
) -> PathBuf {
    let config = HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps,
    };
    let path = dir.join("host-config.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    path
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

fn hluk_bin() -> &'static str {
    env!("CARGO_BIN_EXE_hluk")
}

/// Reads the child's stderr until the `workerd-host listening on
/// http://ADDR ...` line, returning `ADDR`. Panics (not skips) if the
/// process exits or logs something else first: a missing/garbled listen
/// line is a real contract failure.
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

/// Minimal blocking HTTP/1.1 client: connects, sends one request with
/// `Connection: close`, and returns `(status, body)`. No new dependency:
/// `reqwest` in this crate only has the non-blocking `rustls-tls`/`stream`
/// features enabled (see `Cargo.toml`), so a raw socket is simplest here.
fn http_get(addr: &str, path: &str, host: &str) -> (u16, String) {
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

/// Like [`http_get`], but never panics: used to probe a connection opened
/// *after* a shutdown signal has been sent, where "no response" (reset or
/// read timeout) is the expected, successful outcome rather than an error.
/// Returns `None` if the connect fails or no full status line is read
/// before `timeout` elapses.
fn try_http_get_with_timeout(addr: &str, path: &str, host: &str, timeout: Duration) -> Option<u16> {
    let mut stream = TcpStream::connect(addr).ok()?;
    stream.set_read_timeout(Some(timeout)).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut buf = Vec::new();
    // A reset or a read timeout both surface as an `Err` here (or an `Ok`
    // with a truncated/empty buffer); either way, fall through to parsing
    // and let a missing/garbled status line become `None`.
    let _ = stream.read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf);
    text.lines().next()?.split_whitespace().nth(1)?.parse().ok()
}

#[cfg(windows)]
mod signal {
    // Sends CTRL_BREAK_EVENT to a child spawned with
    // `CREATE_NEW_PROCESS_GROUP`, which `ctrlc::set_handler` (used by
    // `workerd-host`) treats the same as SIGTERM/SIGINT on Unix. No new
    // dependency: `GenerateConsoleCtrlEvent` is kernel32, already linked
    // into every Windows std binary.
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
    pub const CREATE_NEW_PROCESS_GROUP: u32 = 0;

    pub fn send_graceful_shutdown(pid: u32) {
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("failed to invoke kill");
        assert!(status.success(), "kill -TERM failed: {status}");
    }
}

#[test]
fn healthz_readyz_status_and_routing_then_clean_shutdown() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let bundle_path = write_script_bundle(tmp.path());
    let config_path = write_host_config(
        tmp.path(),
        vec![disposable_app("hello", "hello.test", bundle_path)],
        rootfs,
        executor,
    );

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

    // healthz must serve before the registry is necessarily ready.
    let (status, _) = http_get(&addr, "/__hyperlight/healthz", "hello.test");
    assert_eq!(status, 200, "healthz should be 200 once the process is up");

    // readyz: poll until it flips from 503 (initializing) to 200 (ready).
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut ready_status = 0;
    while Instant::now() < deadline {
        let (status, _) = http_get(&addr, "/__hyperlight/readyz", "hello.test");
        ready_status = status;
        if status == 200 {
            break;
        }
        assert_eq!(status, 503, "readyz should be 503 while initializing");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(ready_status, 200, "readyz never became ready in time");

    // status: one app, tagged disposable.
    let (status, body) = http_get(&addr, "/__hyperlight/status", "hello.test");
    assert_eq!(status, 200);
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("status body is JSON");
    let apps = parsed.as_array().expect("status body is a JSON array");
    assert_eq!(apps.len(), 1);
    assert_eq!(apps[0]["app_id"], "hello");
    assert_eq!(apps[0]["kind"], "disposable");

    // Routed app traffic succeeds...
    let (status, _) = http_get(&addr, "/", "hello.test");
    assert_eq!(status, 200);
    // ...and an unmatched Host is a 404, not routed to any app.
    let (status, _) = http_get(&addr, "/", "unknown.test");
    assert_eq!(status, 404);

    // Clean shutdown: graceful signal, bounded wait, exit code 0.
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

#[test]
fn bad_config_exits_2_without_binding_a_port() {
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let bundle_path = write_script_bundle(tmp.path());
    // Two apps sharing the same app id is a config-shape error
    // (`AppRegistryError::DuplicateAppId`), not a bundle/worker-init
    // failure: this must fail before any listener is bound.
    let config_path = write_host_config(
        tmp.path(),
        vec![
            disposable_app("same", "a.test", bundle_path.clone()),
            disposable_app("same", "b.test", bundle_path),
        ],
        rootfs,
        executor,
    );

    let output = Command::new(hluk_bin())
        .args([
            "workerd-host",
            "--config",
            config_path.to_str().unwrap(),
            "--bind",
            "127.0.0.1:0",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run hluk workerd-host");
    assert_eq!(output.status.code(), Some(2), "expected exit code 2");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("listening on http://"),
        "a config error must not bind a port: {stderr}"
    );
}

/// Boundary-7 benchmark support: proves the drain contract the
/// `benchmark-orchestrator-contract` guided demo (`tools/hyperlight-demo`)
/// exercises under real `hey` load. A request already admitted before
/// `SIGTERM`/`CTRL_BREAK` must still complete with 200, while a brand-new
/// connection opened immediately after the signal must never see a 200:
/// the accept loop stops polling as soon as `shutting_down` is set (see
/// `src/main.rs`), so such a connection either times out waiting for a
/// response or is reset once the listener is dropped at process exit.
#[test]
fn admitted_request_drains_while_new_connections_get_no_response_after_sigterm() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let bundle_path = write_script_bundle(tmp.path());
    let config_path = write_host_config(
        tmp.path(),
        vec![disposable_app("hello", "hello.test", bundle_path)],
        rootfs,
        executor,
    );

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

    // Wait for readiness before admitting the "in flight" request.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, _) = http_get(&addr, "/__hyperlight/readyz", "hello.test");
        if status == 200 {
            break;
        }
        assert!(Instant::now() < deadline, "readyz never became ready");
        std::thread::sleep(Duration::from_millis(100));
    }

    // Admit one request on a background thread, give it a short head start
    // to connect and be dispatched, then signal shutdown while it is still
    // outstanding.
    let addr_for_thread = addr.clone();
    let admitted = std::thread::spawn(move || http_get(&addr_for_thread, "/", "hello.test"));
    std::thread::sleep(Duration::from_millis(30));

    signal::send_graceful_shutdown(child.id());

    // A connection opened right after the signal must never get a 200:
    // the accept loop has already stopped polling, so this either times
    // out or is reset once the listener closes at process exit.
    let post_signal = try_http_get_with_timeout(&addr, "/", "hello.test", Duration::from_secs(2));
    assert_ne!(
        post_signal,
        Some(200),
        "a connection opened after SIGTERM must not be served: got {post_signal:?}"
    );

    // The request admitted before the signal must still complete.
    let (admitted_status, _) = admitted.join().expect("admitted request thread panicked");
    assert_eq!(
        admitted_status, 200,
        "a request already in flight at signal time must drain successfully"
    );

    // And the process must still exit cleanly within the drain timeout.
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

#[test]
fn bind_failure_exits_4() {
    let (rootfs, executor) = artifacts();
    let tmp = tempfile::tempdir().unwrap();
    let bundle_path = write_script_bundle(tmp.path());
    let config_path = write_host_config(
        tmp.path(),
        vec![disposable_app("hello", "hello.test", bundle_path)],
        rootfs,
        executor,
    );

    // Hold a real port open so the bind in the child process fails.
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap().to_string();

    let output = Command::new(hluk_bin())
        .args([
            "workerd-host",
            "--config",
            config_path.to_str().unwrap(),
            "--bind",
            &addr,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run hluk workerd-host");
    drop(occupied);
    assert_eq!(output.status.code(), Some(4), "expected exit code 4");
}
