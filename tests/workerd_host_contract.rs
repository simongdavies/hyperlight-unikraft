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
        snapshot_dir: None,
        instance_home: None,
        streaming: false,
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

struct HostProcess(std::process::Child, Option<std::process::ChildStderr>);

impl Drop for HostProcess {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
            self.0.wait().unwrap();
        }
    }
}

fn start_separated_host(config: &Path, max_connections: &str) -> (HostProcess, String, String) {
    #[cfg(windows)]
    use std::os::windows::process::CommandExt;
    let mut command = Command::new(hluk_bin());
    command
        .args([
            "workerd-host",
            "--config",
            config.to_str().unwrap(),
            "--bind",
            "127.0.0.1:0",
            "--admin-bind",
            "127.0.0.1:0",
            "--max-connections",
            max_connections,
            "--drain-timeout-ms",
            "10000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    command.creation_flags(signal::CREATE_NEW_PROCESS_GROUP);
    let mut host = HostProcess(command.spawn().unwrap(), None);
    let mut stderr = BufReader::new(host.0.stderr.take().unwrap());
    let mut line = String::new();
    let (app, admin) = loop {
        line.clear();
        assert!(
            stderr.read_line(&mut line).unwrap() > 0,
            "host exited before listening"
        );
        if let Some(rest) = line
            .trim()
            .strip_prefix("workerd-host listening on http://")
        {
            let (app, admin) = rest.split_once(" (admin ").unwrap();
            break (app.to_string(), admin.trim_end_matches(')').to_string());
        }
    };
    host.1 = Some(stderr.into_inner());
    let deadline = Instant::now() + Duration::from_secs(30);
    while http_get(&admin, "/__hyperlight/readyz", "hello.test").0 != 200 {
        assert!(Instant::now() < deadline, "host did not become ready");
        std::thread::sleep(Duration::from_millis(50));
    }
    (host, app, admin)
}

fn read_response(reader: &mut BufReader<TcpStream>) -> u16 {
    let mut line = String::new();
    assert!(
        reader.read_line(&mut line).unwrap() > 0,
        "missing HTTP status"
    );
    let status = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut length = 0;
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap();
        }
    }
    reader.read_exact(&mut vec![0; length]).unwrap();
    status
}

fn contract_config(tmp: &Path) -> PathBuf {
    let (rootfs, executor) = artifacts();
    let bundle = write_script_bundle(tmp);
    write_host_config(
        tmp,
        vec![disposable_app("hello", "hello.test", bundle)],
        rootfs,
        executor,
    )
}

#[test]
fn admin_listener_never_invokes_apps_and_app_listener_never_exposes_probes() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let config = contract_config(tmp.path());
    let (_host, app, admin) = start_separated_host(&config, "8");
    assert_eq!(http_get(&admin, "/", "hello.test").0, 404);
    assert_eq!(http_get(&app, "/__hyperlight/healthz", "hello.test").0, 404);
    assert_eq!(
        http_get(&admin, "/__hyperlight/healthz", "hello.test").0,
        200
    );
    assert_eq!(http_get(&app, "/", "hello.test").0, 200);
    let (_, body) = http_get(&admin, "/__hyperlight/status", "hello.test");
    let status: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(status[0]["admitted"], 0);
}

#[test]
fn drain_rejects_new_invocations_on_keepalive_and_preaccepted_probe_connections() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let config = contract_config(tmp.path());
    let (mut host, app, admin) = start_separated_host(&config, "8");
    let mut keepalive = BufReader::new(connect_with_retry(&app));
    keepalive
        .get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        keepalive.get_mut(),
        "GET / HTTP/1.1\r\nHost: hello.test\r\nConnection: keep-alive\r\n\r\n"
    )
    .unwrap();
    assert_eq!(read_response(&mut keepalive), 200);
    let mut probe = BufReader::new(connect_with_retry(&admin));
    probe
        .get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    // Send a partial head to ensure this connection is accepted before drain.
    write!(probe.get_mut(), "GET /__hyperlight/readyz HTTP/1.1\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(100));
    signal::send_graceful_shutdown(host.0.id());
    std::thread::sleep(Duration::from_millis(100));
    write!(
        probe.get_mut(),
        "Host: hello.test\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    assert_eq!(read_response(&mut probe), 503);
    write!(
        keepalive.get_mut(),
        "GET / HTTP/1.1\r\nHost: hello.test\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    assert_eq!(read_response(&mut keepalive), 503);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = host.0.try_wait().unwrap() {
            assert_eq!(status.code(), Some(0));
            break;
        }
        assert!(
            Instant::now() < deadline,
            "host failed to drain connections"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn app_connection_limit_does_not_starve_admin_probes() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let config = contract_config(tmp.path());
    let (_host, app, admin) = start_separated_host(&config, "1");
    let mut occupied = connect_with_retry(&app);
    write!(occupied, "GET / HTTP/1.1\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert_ne!(
        try_http_get_with_timeout(&app, "/", "hello.test", Duration::from_secs(2)),
        Some(200),
    );
    assert_eq!(
        http_get(&admin, "/__hyperlight/readyz", "hello.test").0,
        200
    );
}

#[test]
fn disconnected_client_cancels_cpu_bound_guest_and_releases_capacity() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let config = contract_config(tmp.path());
    let (_host, app, admin) = start_separated_host(&config, "8");
    let mut abandoned = connect_with_retry(&app);
    write!(
        abandoned,
        "GET /busy HTTP/1.1\r\nHost: hello.test\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, body) = http_get(&admin, "/__hyperlight/status", "hello.test");
        let status: serde_json::Value = serde_json::from_str(&body).unwrap();
        if status[0]["active"] == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "busy request was not admitted");
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(abandoned);
    let cancelled_at = Instant::now();
    loop {
        let (_, body) = http_get(&admin, "/__hyperlight/status", "hello.test");
        let status: serde_json::Value = serde_json::from_str(&body).unwrap();
        if status[0]["admitted"] == 0 {
            break;
        }
        assert!(
            cancelled_at.elapsed() < Duration::from_secs(2),
            "disconnected invocation retained capacity"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(http_get(&app, "/", "hello.test").0, 200);
}

#[test]
fn pipelined_requests_do_not_consume_each_others_bytes() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let config = contract_config(tmp.path());
    let (_host, app, _) = start_separated_host(&config, "8");
    let mut stream = BufReader::new(connect_with_retry(&app));
    stream
        .get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(stream.get_mut(),
        "GET / HTTP/1.1\r\nHost: hello.test\r\nConnection: keep-alive\r\n\r\nGET / HTTP/1.1\r\nHost: hello.test\r\nConnection: close\r\n\r\n"
    ).unwrap();
    assert_eq!(read_response(&mut stream), 200);
    assert_eq!(read_response(&mut stream), 200);
}

#[test]
fn control_envelope_can_carry_the_full_32k_guest_body_without_raising_v1_limits() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let tmp = tempfile::tempdir().unwrap();
    let config = contract_config(tmp.path());
    let (_host, app, admin) = start_separated_host(&config, "8");
    use base64::{Engine, engine::general_purpose::STANDARD};
    let payload = serde_json::json!({
        "protocol_version":1,"kind":"fetch",
        "request":{"protocol_version":1,"request_id":"exact-body","method":"POST",
            "url":"https://hello.test/","headers":[],"body_base64":STANDARD.encode(vec![0;32768])},
    })
    .to_string();
    assert!(payload.len() > 32768 && payload.len() < 61440);
    let mut stream = BufReader::new(connect_with_retry(&admin));
    stream
        .get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(stream.get_mut(),
        "POST /v1/apps/hello/invoke HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        payload.len(), payload,
    ).unwrap();
    assert_eq!(read_response(&mut stream), 200);
    assert_eq!(http_get(&app, "/v1/capabilities", "hello.test").0, 404);
}

fn http_post(addr: &str, path: &str, payload: &serde_json::Value) -> (u16, serde_json::Value) {
    let payload = payload.to_string();
    let mut stream = connect_with_retry(addr);
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    write!(stream, "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", payload.len(), payload).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, serde_json::from_str(body).unwrap())
}

#[test]
fn instance_home_has_one_vm_per_slot_fenced_invocation_and_changed_state_park_resume() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    use hyperlight_unikraft::workerd::{
        CheckpointPolicy, InstanceHomeConfig, ResidentPoolConfigJson,
    };
    let tmp = tempfile::tempdir().unwrap();
    let (rootfs, executor) = artifacts();
    let mut app = disposable_app("hello", "hello.test", write_script_bundle(tmp.path()));
    app.pool = AppPoolConfig::Resident(ResidentPoolConfigJson {
        capacity: 1,
        queue_capacity: 2,
        max_requests_per_vm: None,
        max_lifetime_secs: None,
    });
    app.instance_home = Some(InstanceHomeConfig {
        checkpoint_policy: CheckpointPolicy::Local,
        database_path: None,
        encryption_key_path: None,
        max_checkpoint_bytes: 128 * 1024 * 1024,
        idle_timeout_secs: None,
        checkpoint_timeout_secs: 5,
        scratch_directory: None,
    });
    let config = write_host_config(tmp.path(), vec![app], rootfs, executor);
    let (_host, app_addr, admin) = start_separated_host(&config, "8");
    let (status, empty) = http_get(
        &admin,
        "/v1/instances?app_id=hello&revision=host-contract-v1",
        "hello.test",
    );
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&empty).unwrap()["instances"],
        serde_json::json!([])
    );
    let mut lifecycle = serde_json::json!({
        "protocol_version":1,"request_id":"lifecycle-1","app_id":"hello",
        "revision":"host-contract-v1","expected_generation":0,"checkpoint_policy":"local",
    });
    let (status, created) = http_post(&admin, "/v1/instances/instance-1/create", &lifecycle);
    assert_eq!(status, 200, "{created}");
    assert_eq!(created["state"], "active");
    assert_eq!(created["generation"], 1);
    assert!(
        created["endpoint"]
            .as_str()
            .unwrap()
            .ends_with("/v1/instances/instance-1/invoke")
    );
    assert_eq!(
        created["http_endpoint"],
        format!("http://{app_addr}/v1/instances/instance-1/http")
    );
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-2/create", &lifecycle).0,
        409
    );
    let mut invocation = serde_json::json!({
        "protocol_version":1,"request_id":"instance-fetch","app_id":"hello",
        "revision":"host-contract-v1","instance_id":"instance-1","expected_generation":1,
        "lifetime_budget_ms":5000,"invocation":{"kind":"fetch","request":{
            "protocol_version":1,"request_id":"instance-fetch","method":"GET",
            "url":"https://hello.test/instance","headers":[],"body_base64":"",
        }},
    });
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/invoke", &invocation).1["response"]["status"],
        200
    );
    assert_eq!(
        http_get(&app_addr, "/v1/instances/instance-1/invoke", "hello.test").0,
        404
    );
    lifecycle["expected_generation"] = serde_json::json!(1);
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/park", &lifecycle).1["state"],
        "parked"
    );
    let (_, status_body) = http_get(&admin, "/__hyperlight/status", "hello.test");
    let status: serde_json::Value = serde_json::from_str(&status_body).unwrap();
    assert_eq!(status[0]["instances"][0]["live_vms"], 0);
    let (_, listed) = http_get(
        &admin,
        "/v1/instances/instance-1?app_id=hello&revision=host-contract-v1",
        "hello.test",
    );
    let listed: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert_eq!(listed["instance"]["state"], "parked");
    assert_eq!(listed["instance"]["identity"]["generation"], 1);
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/invoke", &invocation).0,
        503
    );
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/resume", &lifecycle).1["generation"],
        2
    );
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/invoke", &invocation).0,
        409
    );
    invocation["expected_generation"] = serde_json::json!(2);
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/invoke", &invocation).1["response"]["status"],
        500
    );
    let mut data = BufReader::new(connect_with_retry(&app_addr));
    data.get_mut()
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(data.get_mut(),"GET /v1/instances/instance-1/http HTTP/1.1\r\nHost: fixed.test\r\nx-hyperloom-app: hello\r\nx-hyperloom-revision: host-contract-v1\r\nx-hyperloom-generation: 2\r\nx-hyperloom-request-url: https://hello.test/instance\r\nConnection: close\r\n\r\n").unwrap();
    assert_eq!(
        read_response(&mut data),
        500,
        "raw data route must select this same fenced changed instance"
    );
    lifecycle["expected_generation"] = serde_json::json!(2);
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/park", &lifecycle).1["state"],
        "parked"
    );
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/resume", &lifecycle).1["generation"],
        3
    );
    lifecycle["expected_generation"] = serde_json::json!(3);
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-1/release", &lifecycle).1["state"],
        "released"
    );
    lifecycle["expected_generation"] = serde_json::json!(0);
    assert_eq!(
        http_post(&admin, "/v1/instances/instance-2/create", &lifecycle).0,
        200
    );
}
