// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    FetchBroker, FetchBrokerConfig, FetchLimits, FetchPolicy, Header, PROTOCOL_VERSION,
    RequestEnvelope, WorkerBundle, WorkerVersionSandbox,
};
use hyperlight_unikraft::{BlockList, NetworkPolicy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const WORKER_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION;
const FETCH_V1_PROTOCOL_VERSION: u32 = 1;
const FETCH_V2_PROTOCOL_VERSION: u32 = 2;

#[derive(Deserialize)]
struct Manifest {
    schema_version: u32,
    suite: String,
    qualification: String,
    acceptance_threshold: AcceptanceThreshold,
    cases: Vec<ManifestCase>,
}

#[derive(Deserialize, Serialize)]
struct AcceptanceThreshold {
    rule: String,
    compliance_rule: String,
    authoritative_sources: Vec<String>,
}

#[derive(Deserialize)]
struct ManifestCase {
    id: String,
    category: String,
    requirement: String,
    expected: String,
}

#[derive(Deserialize)]
struct ProbeResult {
    outcome: String,
    detail: String,
    #[serde(default)]
    observations: Value,
}

#[derive(Serialize)]
struct Evidence {
    schema_version: u32,
    suite: String,
    qualification: String,
    generated_unix_seconds: u64,
    accepted: bool,
    compliance_ready: bool,
    acceptance_threshold: AcceptanceThreshold,
    revisions: Revisions,
    protocols: Protocols,
    artifacts: Artifacts,
    environment: Environment,
    matrix: Vec<CaseEvidence>,
}

#[derive(Serialize)]
struct Revisions {
    host_git: String,
    host_worktree_dirty: bool,
    unikraft_guest_git: String,
    executor_git: String,
}

#[derive(Serialize)]
struct Protocols {
    worker_envelope: u16,
    outbound_fetch_v1: u32,
    outbound_fetch_v2: u32,
}

#[derive(Serialize)]
struct Artifacts {
    kernel: Artifact,
    rootfs: Artifact,
    executor: Artifact,
    bundle_sha256: String,
}

#[derive(Serialize)]
struct Artifact {
    path: String,
    sha256: String,
    bytes: u64,
}

#[derive(Serialize)]
struct Environment {
    os: String,
    arch: String,
    scratch_mib: u64,
    fresh_vm_per_case: bool,
}

#[derive(Serialize)]
struct CaseEvidence {
    id: String,
    category: String,
    requirement: String,
    expected: String,
    outcome: String,
    accepted: bool,
    detail: String,
    observations: Value,
}

struct Options {
    executor_revision: String,
    artifact_dir: PathBuf,
    output: PathBuf,
    scratch_mib: usize,
}

struct ScenarioServer {
    port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for ScenarioServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest_path = root.join("examples/workerd-bundles/wintertc-evidence-manifest.json");
    let bundle_path = root.join("examples/workerd-bundles/wintertc-evidence.json");
    let kernel_path = root.join("kernel/workerd_hyperlight-x86_64");
    let rootfs_path = options.artifact_dir.join("rootfs.img");
    let executor_path = options.artifact_dir.join("executor");
    for path in [&kernel_path, &rootfs_path, &executor_path] {
        if !path.is_file() {
            return Err(format!("required real-VM artifact is missing: {}", path.display()).into());
        }
    }

    let manifest: Manifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.schema_version != 1 {
        return Err("unsupported evidence manifest schema".into());
    }
    let bundle = WorkerBundle::from_path(&bundle_path)?;
    let bundle_sha256 = bundle.sha256()?;
    let worker_version = bundle.worker_version.clone();
    let server = start_scenario_server()?;
    let policy = FetchPolicy::new(
        NetworkPolicy::BlockList(BlockList::from_hosts(&[] as &[&str])?),
        ["http"],
        [server.port],
    )
    .allow_loopback(true);
    let broker = FetchBroker::new(FetchBrokerConfig {
        policy,
        limits: FetchLimits {
            connect_timeout: Duration::from_secs(1),
            total_timeout: Duration::from_secs(2),
            ..FetchLimits::default()
        },
    })?;
    let worker = WorkerVersionSandbox::initialize_with_fetch(
        bundle,
        &rootfs_path,
        &executor_path,
        options.scratch_mib,
        Duration::from_secs(90),
        broker,
    )?;

    let origin = format!("http://localhost:{}", server.port);
    let denied_origin = format!("http://localhost:{}", server.port.saturating_add(1));
    let dns_origin = format!("http://wintertc.invalid:{}", server.port);
    let mut matrix = Vec::with_capacity(manifest.cases.len());
    for case in manifest.cases {
        let probe = if case.requirement == "external_pending" {
            let dependency = if case.expected == "pending_timer_branch" {
                "timer branch"
            } else {
                "authoritative upstream test artifact"
            };
            ProbeResult {
                outcome: "pending".into(),
                detail: format!(
                    "Compliance-blocking evidence is pending; rerun after the {dependency} is available."
                ),
                observations: json!({ "dependency": dependency }),
            }
        } else {
            execute_case(
                &worker,
                &worker_version,
                &case.id,
                &origin,
                &denied_origin,
                &dns_origin,
            )
        };
        let accepted = case_accepted(&case, &probe);
        matrix.push(CaseEvidence {
            id: case.id,
            category: case.category,
            requirement: case.requirement,
            expected: case.expected,
            outcome: probe.outcome,
            accepted,
            detail: probe.detail,
            observations: probe.observations,
        });
    }

    let accepted = matrix.iter().all(|case| case.accepted);
    let compliance_ready = accepted
        && matrix
            .iter()
            .all(|case| case.category != "compliance-authoritative" || case.outcome == "pass");
    let evidence = Evidence {
        schema_version: 1,
        suite: manifest.suite,
        qualification: manifest.qualification,
        generated_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        accepted,
        compliance_ready,
        acceptance_threshold: manifest.acceptance_threshold,
        revisions: Revisions {
            host_git: git(root, &["rev-parse", "HEAD"])?,
            host_worktree_dirty: !git(root, &["status", "--porcelain"])?.is_empty(),
            unikraft_guest_git: git(root, &["rev-parse", "HEAD:kernel/unikraft"])?,
            executor_git: options.executor_revision,
        },
        protocols: Protocols {
            worker_envelope: WORKER_PROTOCOL_VERSION,
            outbound_fetch_v1: FETCH_V1_PROTOCOL_VERSION,
            outbound_fetch_v2: FETCH_V2_PROTOCOL_VERSION,
        },
        artifacts: Artifacts {
            kernel: artifact(&kernel_path)?,
            rootfs: artifact(&rootfs_path)?,
            executor: artifact(&executor_path)?,
            bundle_sha256,
        },
        environment: Environment {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            scratch_mib: options.scratch_mib as u64,
            fresh_vm_per_case: true,
        },
        matrix,
    };
    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_vec_pretty(&evidence)?;
    fs::write(&options.output, &json)?;
    println!("{}", String::from_utf8(json)?);
    if accepted {
        Ok(())
    } else {
        Err(format!(
            "evidence threshold failed; matrix written to {}",
            options.output.display()
        )
        .into())
    }
}

fn parse_options() -> Result<Options, Box<dyn std::error::Error>> {
    let mut executor_revision = None;
    let mut artifact_dir = PathBuf::from("build-elfloader/workerd-executor");
    let mut output = PathBuf::from("build-elfloader/wintertc-evidence.json");
    let mut scratch_mib = 512;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--executor-revision" => executor_revision = args.next(),
            "--artifact-dir" => {
                artifact_dir = PathBuf::from(args.next().ok_or("missing --artifact-dir value")?)
            }
            "--output" => output = PathBuf::from(args.next().ok_or("missing --output value")?),
            "--scratch-mib" => {
                scratch_mib = args.next().ok_or("missing --scratch-mib value")?.parse()?
            }
            "--help" | "-h" => {
                println!(
                    "usage: cargo run --release --locked --example wintertc-vm-evidence -- \\\n+  --executor-revision GIT_SHA [--artifact-dir DIR] [--output FILE] [--scratch-mib MIB]"
                );
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let executor_revision = executor_revision
        .or_else(|| std::env::var("WORKERD_EXECUTOR_REVISION").ok())
        .ok_or("--executor-revision or WORKERD_EXECUTOR_REVISION is required")?;
    if executor_revision.trim().is_empty() {
        return Err("executor revision must not be empty".into());
    }
    Ok(Options {
        executor_revision,
        artifact_dir,
        output,
        scratch_mib,
    })
}

fn execute_case(
    worker: &WorkerVersionSandbox,
    version: &hyperlight_unikraft::workerd::WorkerVersionId,
    id: &str,
    origin: &str,
    denied_origin: &str,
    dns_origin: &str,
) -> ProbeResult {
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: format!("evidence-{id}"),
        method: "GET".into(),
        url: format!("https://evidence.invalid/case/{id}"),
        headers: vec![
            Header {
                name: "x-evidence-origin".into(),
                value: origin.into(),
            },
            Header {
                name: "x-evidence-denied-origin".into(),
                value: denied_origin.into(),
            },
            Header {
                name: "x-evidence-dns-origin".into(),
                value: dns_origin.into(),
            },
        ],
        body_base64: String::new(),
    };
    match worker.execute(version, request, Duration::from_secs(15)) {
        Ok(response) if response.status == 200 => STANDARD
            .decode(response.body_base64)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_else(|| ProbeResult {
                outcome: "fail".into(),
                detail: "executor returned malformed probe JSON".into(),
                observations: Value::Null,
            }),
        Ok(response) => ProbeResult {
            outcome: "fail".into(),
            detail: format!("executor returned HTTP {}", response.status),
            observations: json!({ "headers": response.headers }),
        },
        Err(error) => ProbeResult {
            outcome: "fail".into(),
            detail: format!("fresh VM execution failed: {error}"),
            observations: Value::Null,
        },
    }
}

fn case_accepted(case: &ManifestCase, result: &ProbeResult) -> bool {
    match case.expected.as_str() {
        "pass" => result.outcome == "pass",
        "pass_or_unsupported" => matches!(result.outcome.as_str(), "pass" | "unsupported"),
        "policy_denied" => result.outcome == "policy_denied",
        "dns_denied" => result.outcome == "dns_denied",
        "pending_timer_branch" | "pending_external_artifact" => result.outcome == "pending",
        _ => false,
    }
}

fn artifact(path: &Path) -> Result<Artifact, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes += read as u64;
    }
    Ok(Artifact {
        path: path.display().to_string(),
        sha256: digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        bytes,
    })
}

fn git(root: &Path, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git").current_dir(root).args(args).output()?;
    if !output.status.success() {
        return Err(format!("git {} failed", args.join(" ")).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().into())
}

fn start_scenario_server() -> Result<ScenarioServer, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let thread = thread::spawn(move || {
        while !thread_stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    thread::spawn(move || {
                        let _ = serve(stream);
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(_) => break,
            }
        }
    });
    Ok(ScenarioServer {
        port,
        stop,
        thread: Some(thread),
    })
}

fn serve(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");
    let mut content_length = None;
    let mut chunked = false;
    let mut duplicate_headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse::<usize>().ok();
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && value.eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            } else if name.eq_ignore_ascii_case("x-evidence-duplicate") {
                duplicate_headers.push(value.to_string());
            }
        }
    }

    if path == "/early" {
        return response(&mut stream, 202, "text/plain", b"early");
    }
    if path == "/timeout" {
        thread::sleep(Duration::from_secs(4));
        return response(&mut stream, 200, "text/plain", b"late");
    }
    if path == "/slow" {
        thread::sleep(Duration::from_millis(750));
        return response(&mut stream, 200, "text/plain", b"slow");
    }

    let body = if chunked {
        read_chunked(&mut reader)?
    } else {
        let mut body = vec![0; content_length.unwrap_or(0)];
        reader.read_exact(&mut body)?;
        body
    };
    match path {
        "/get" => response(&mut stream, 200, "text/plain", b"get-ok"),
        "/post" => response(&mut stream, 200, "application/octet-stream", &body),
        "/headers" => response(
            &mut stream,
            200,
            "application/json",
            &serde_json::to_vec(&duplicate_headers)?,
        ),
        "/upload" => response(
            &mut stream,
            200,
            "application/json",
            &serde_json::to_vec(&json!({ "bytes": body.len() }))?,
        ),
        "/download-large" => response(
            &mut stream,
            200,
            "application/octet-stream",
            &vec![b'd'; 5 * 1024 * 1024],
        ),
        "/download-unknown" => chunked_response(&mut stream, 5 * 1024 * 1024, false),
        "/download-slow" => chunked_response(&mut stream, 1024 * 1024, true),
        "/redirect" => {
            write!(
                stream,
                "HTTP/1.1 302 Found\r\nLocation: /get\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
        }
        _ => response(&mut stream, 404, "text/plain", b"not-found"),
    }
}

fn response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        404 => "Not Found",
        _ => "Response",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

fn chunked_response(stream: &mut TcpStream, total: usize, slow: bool) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    )?;
    let chunk = vec![b'c'; 32 * 1024];
    let mut written = 0;
    while written < total {
        let size = chunk.len().min(total - written);
        write!(stream, "{size:x}\r\n")?;
        stream.write_all(&chunk[..size])?;
        stream.write_all(b"\r\n")?;
        stream.flush()?;
        written += size;
        if slow {
            thread::sleep(Duration::from_millis(5));
        }
    }
    stream.write_all(b"0\r\n\r\n")
}

fn read_chunked(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad chunk size"))?;
        if size == 0 {
            let mut trailer = String::new();
            reader.read_line(&mut trailer)?;
            return Ok(body);
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..])?;
        let mut crlf = [0; 2];
        reader.read_exact(&mut crlf)?;
        if crlf != *b"\r\n" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bad chunk terminator",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acceptance_distinguishes_optional_policy_and_pending() {
        let case = |expected: &str| ManifestCase {
            id: "case".into(),
            category: "test".into(),
            requirement: "required".into(),
            expected: expected.into(),
        };
        let result = |outcome: &str| ProbeResult {
            outcome: outcome.into(),
            detail: String::new(),
            observations: Value::Null,
        };
        assert!(case_accepted(&case("pass"), &result("pass")));
        assert!(!case_accepted(&case("pass"), &result("unsupported")));
        assert!(case_accepted(
            &case("pass_or_unsupported"),
            &result("unsupported")
        ));
        assert!(case_accepted(
            &case("policy_denied"),
            &result("policy_denied")
        ));
        assert!(case_accepted(
            &case("pending_timer_branch"),
            &result("pending")
        ));
    }
}
