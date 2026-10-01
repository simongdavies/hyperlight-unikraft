// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    ExecutionProfile, FetchBroker, FetchBrokerConfig, FetchLimits, FetchPolicy, Header,
    PROTOCOL_VERSION, RequestEnvelope, WorkerBundle, WorkerRequestPool, WorkerVersionSandbox,
};
use hyperlight_unikraft::{BlockList, NetworkPolicy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WORKER_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION;
const FETCH_V1_PROTOCOL_VERSION: u32 = 1;
const FETCH_V2_PROTOCOL_VERSION: u32 = 2;
const DEFAULT_SCRATCH_MIB: usize = 344;
const RESTORE_CYCLES: usize = 10;
const POOL_CONCURRENCY: usize = 2;
const POOL_QUEUE_CAPACITY: usize = 2;

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

#[derive(Clone, Deserialize)]
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
    performance: PerformanceEvidence,
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
struct PerformanceEvidence {
    cold_boot_ms: f64,
    repeated_restore_cycles: usize,
    sequential_profiles: Vec<ExecutionProfile>,
    pool: PoolPerformance,
}

#[derive(Serialize)]
struct PoolPerformance {
    max_concurrent_sandboxes: usize,
    queue_capacity: usize,
    submitted: usize,
    completed: usize,
    rejected: usize,
    wall_ms: f64,
    throughput_requests_per_second: f64,
    p50_total_ms: f64,
    p95_total_ms: f64,
    peak_active: usize,
    peak_queued: usize,
    final_active: usize,
    final_queued: usize,
    shutdown_ms: f64,
    refill_completed: bool,
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
    bundle: PathBuf,
    host_git: Option<String>,
    host_worktree_dirty: Option<bool>,
    unikraft_guest_git: Option<String>,
    output: PathBuf,
    performance_output: PathBuf,
    scratch_mib: usize,
    focus_typed_errors: bool,
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
    let bundle_path = if options.bundle.is_absolute() {
        options.bundle.clone()
    } else {
        root.join(&options.bundle)
    };
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
    let cold_boot_started = Instant::now();
    let worker = WorkerVersionSandbox::initialize_with_fetch(
        bundle,
        &rootfs_path,
        &executor_path,
        options.scratch_mib,
        Duration::from_secs(90),
        broker,
    )?;
    let cold_boot_ms = elapsed_ms(cold_boot_started);

    let origin = format!("http://localhost:{}", server.port);
    let denied_origin = format!("http://localhost:{}", server.port.saturating_add(1));
    let dns_origin = format!("http://wintertc.invalid:{}", server.port);
    let (host_results, performance) = if options.focus_typed_errors {
        (
            BTreeMap::new(),
            PerformanceEvidence {
                cold_boot_ms,
                repeated_restore_cycles: 0,
                sequential_profiles: Vec::new(),
                pool: PoolPerformance {
                    max_concurrent_sandboxes: 0,
                    queue_capacity: 0,
                    submitted: 0,
                    completed: 0,
                    rejected: 0,
                    wall_ms: 0.0,
                    throughput_requests_per_second: 0.0,
                    p50_total_ms: 0.0,
                    p95_total_ms: 0.0,
                    peak_active: 0,
                    peak_queued: 0,
                    final_active: 0,
                    final_queued: 0,
                    shutdown_ms: 0.0,
                    refill_completed: false,
                },
            },
        )
    } else {
        run_host_evidence(
            &worker,
            &worker_version,
            &origin,
            &denied_origin,
            &dns_origin,
            cold_boot_ms,
        )
    };
    let mut matrix = Vec::with_capacity(manifest.cases.len());
    for case in manifest.cases.into_iter().filter(|case| {
        !options.focus_typed_errors
            || matches!(
                case.id.as_str(),
                "fetch-policy-denial" | "fetch-dns-denial" | "fetch-timeout" | "fetch-overload"
            )
    }) {
        let probe = if let Some(probe) = host_results.get(&case.id) {
            probe.clone()
        } else if case.requirement == "external_pending" {
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
    let compliance_ready = !options.focus_typed_errors
        && accepted
        && matrix
            .iter()
            .all(|case| case.category != "compliance-authoritative" || case.outcome == "pass");
    let host_git = match options.host_git {
        Some(value) => value,
        None => git(root, &["rev-parse", "HEAD"])?,
    };
    let host_worktree_dirty = match options.host_worktree_dirty {
        Some(value) => value,
        None => !git(root, &["status", "--porcelain"])?.is_empty(),
    };
    let unikraft_guest_git = match options.unikraft_guest_git {
        Some(value) => value,
        None => git(root, &["rev-parse", "HEAD:kernel/unikraft"])?,
    };
    let evidence = Evidence {
        schema_version: 1,
        suite: manifest.suite,
        qualification: manifest.qualification,
        generated_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        accepted,
        compliance_ready,
        acceptance_threshold: manifest.acceptance_threshold,
        revisions: Revisions {
            host_git,
            host_worktree_dirty,
            unikraft_guest_git,
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
        performance,
        matrix,
    };
    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = options.performance_output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        &options.performance_output,
        serde_json::to_vec_pretty(&evidence.performance)?,
    )?;
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
    let mut bundle = PathBuf::from("examples/workerd-bundles/wintertc-evidence.json");
    let mut host_git = None;
    let mut host_worktree_dirty = None;
    let mut unikraft_guest_git = None;
    let mut output = PathBuf::from("build-elfloader/wintertc-evidence.json");
    let mut performance_output = PathBuf::from("build-elfloader/wintertc-performance.json");
    let mut scratch_mib = DEFAULT_SCRATCH_MIB;
    let mut focus_typed_errors = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--executor-revision" => executor_revision = args.next(),
            "--artifact-dir" => {
                artifact_dir = PathBuf::from(args.next().ok_or("missing --artifact-dir value")?)
            }
            "--bundle" => bundle = PathBuf::from(args.next().ok_or("missing --bundle value")?),
            "--host-git" => host_git = args.next(),
            "--host-worktree-dirty" => {
                host_worktree_dirty = Some(
                    args.next()
                        .ok_or("missing --host-worktree-dirty value")?
                        .parse()?,
                )
            }
            "--unikraft-guest-git" => unikraft_guest_git = args.next(),
            "--output" => output = PathBuf::from(args.next().ok_or("missing --output value")?),
            "--performance-output" => {
                performance_output =
                    PathBuf::from(args.next().ok_or("missing --performance-output value")?)
            }
            "--scratch-mib" => {
                scratch_mib = args.next().ok_or("missing --scratch-mib value")?.parse()?
            }
            "--focus-typed-errors" => focus_typed_errors = true,
            "--help" | "-h" => {
                println!(
                    "usage: cargo run --release --locked --example wintertc-vm-evidence -- \\\n+  --executor-revision GIT_SHA [--artifact-dir DIR] [--bundle JSON] \\\n+  [--host-git SHA --host-worktree-dirty BOOL --unikraft-guest-git SHA] \\\n                    +  [--output FILE] [--performance-output FILE] [--scratch-mib MIB] \\\n+  [--focus-typed-errors]"
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
        bundle,
        host_git,
        host_worktree_dirty,
        unikraft_guest_git,
        output,
        performance_output,
        scratch_mib,
        focus_typed_errors,
    })
}

fn run_host_evidence(
    worker: &WorkerVersionSandbox,
    version: &hyperlight_unikraft::workerd::WorkerVersionId,
    origin: &str,
    denied_origin: &str,
    dns_origin: &str,
    cold_boot_ms: f64,
) -> (BTreeMap<String, ProbeResult>, PerformanceEvidence) {
    let mut results = BTreeMap::new();
    let mut sequential_profiles = Vec::with_capacity(RESTORE_CYCLES);
    let mut repeated_passed = 0usize;
    for index in 0..RESTORE_CYCLES {
        let (probe, profile) = execute_profiled_case(
            worker,
            version,
            &format!("restore-cycle-{index}"),
            "capability-free-behavior",
            origin,
            denied_origin,
            dns_origin,
        );
        if probe.outcome == "pass" {
            repeated_passed += 1;
        }
        sequential_profiles.push(profile);
    }
    let restored_ms: Vec<f64> = sequential_profiles
        .iter()
        .map(|profile| profile.snapshot_restore_ms)
        .collect();
    let mut sorted_restored_ms = restored_ms.clone();
    sorted_restored_ms.sort_by(f64::total_cmp);
    let restored_p95_ms = percentile(&sorted_restored_ms, 95);
    results.insert(
        "offline-restore-cold-vs-restored-latency".into(),
        if repeated_passed == RESTORE_CYCLES && restored_p95_ms < cold_boot_ms {
            pass(
                "restored acquisition p95 was lower than cold boot latency",
                json!({
                    "cold_boot_ms": cold_boot_ms,
                    "restored_acquisition_ms": restored_ms,
                    "restored_p95_ms": restored_p95_ms,
                }),
            )
        } else {
            failure(
                "restored acquisition did not beat cold boot or a sample failed",
                json!({
                    "passed": repeated_passed,
                    "attempted": RESTORE_CYCLES,
                    "cold_boot_ms": cold_boot_ms,
                    "restored_p95_ms": restored_p95_ms,
                }),
            )
        },
    );
    results.insert(
        "offline-restore-repeated-cycles".into(),
        if repeated_passed == RESTORE_CYCLES {
            pass(
                "every repeated restore cycle completed in a fresh VM",
                json!({ "cycles": RESTORE_CYCLES }),
            )
        } else {
            failure(
                "one or more repeated restore cycles failed",
                json!({ "passed": repeated_passed, "attempted": RESTORE_CYCLES }),
            )
        },
    );

    let pool_started = Instant::now();
    let pool = WorkerRequestPool::new(worker.clone(), POOL_CONCURRENCY, POOL_QUEUE_CAPACITY);
    let mut pool_profiles = Vec::new();
    let mut completed = 0usize;
    let mut rejected = 0usize;
    let mut refill_completed = false;
    let mut peak_active = 0usize;
    let mut peak_queued = 0usize;
    let mut final_active = 0usize;
    let mut final_queued = 0usize;
    let mut shutdown_ms = 0.0;
    if let Ok(pool) = pool {
        let submitted = POOL_CONCURRENCY + POOL_QUEUE_CAPACITY;
        let (sender, receiver) = mpsc::channel();
        for index in 0..submitted {
            let request = probe_request(
                &format!("pool-{index}"),
                "capability-free-behavior",
                origin,
                denied_origin,
                dns_origin,
            );
            let sender = sender.clone();
            if pool
                .try_submit(request, Duration::from_secs(30), move |execution| {
                    let _ = sender.send(execution);
                })
                .is_err()
            {
                rejected += 1;
            }
        }
        drop(sender);
        let observe_until = Instant::now() + Duration::from_secs(1);
        while Instant::now() < observe_until {
            let status = pool.status();
            peak_active = peak_active.max(status.active);
            peak_queued = peak_queued.max(status.queued);
            if peak_active > 0 && peak_queued > 0 {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        for _ in 0..submitted {
            match receiver.recv_timeout(Duration::from_secs(45)) {
                Ok(execution) => {
                    if execution.submit_error.is_some() {
                        rejected += 1;
                    } else if probe_from_execution(execution.result).outcome == "pass" {
                        completed += 1;
                    }
                    pool_profiles.push(execution.profile);
                    let status = pool.status();
                    peak_active = peak_active.max(status.active);
                    peak_queued = peak_queued.max(status.queued);
                }
                Err(_) => break,
            }
        }
        let (refill_sender, refill_receiver) = mpsc::channel();
        let refill = pool.try_submit(
            probe_request(
                "pool-refill",
                "capability-free-behavior",
                origin,
                denied_origin,
                dns_origin,
            ),
            Duration::from_secs(15),
            move |execution| {
                let _ = refill_sender.send(execution);
            },
        );
        if refill.is_ok() {
            refill_completed = refill_receiver
                .recv_timeout(Duration::from_secs(30))
                .map(|execution| probe_from_execution(execution.result).outcome == "pass")
                .unwrap_or(false);
        }
        let status = pool.status();
        final_active = status.active;
        final_queued = status.queued;
        let shutdown_started = Instant::now();
        drop(pool);
        shutdown_ms = elapsed_ms(shutdown_started);
    }
    let wall_ms = elapsed_ms(pool_started);
    let mut totals: Vec<f64> = pool_profiles
        .iter()
        .filter_map(|profile| profile.total_ms.is_finite().then_some(profile.total_ms))
        .collect();
    totals.sort_by(f64::total_cmp);
    let p50_total_ms = percentile(&totals, 50);
    let p95_total_ms = percentile(&totals, 95);
    let throughput_requests_per_second = if wall_ms > 0.0 {
        completed as f64 * 1_000.0 / wall_ms
    } else {
        0.0
    };
    let pool_observations = json!({
        "max_concurrent_sandboxes": POOL_CONCURRENCY,
        "queue_capacity": POOL_QUEUE_CAPACITY,
        "submitted": POOL_CONCURRENCY + POOL_QUEUE_CAPACITY,
        "completed": completed,
        "rejected": rejected,
        "refill_completed": refill_completed,
        "wall_ms": wall_ms,
        "throughput_requests_per_second": throughput_requests_per_second,
        "p50_total_ms": p50_total_ms,
        "p95_total_ms": p95_total_ms,
        "peak_active": peak_active,
        "peak_queued": peak_queued,
        "final_active": final_active,
        "final_queued": final_queued,
        "shutdown_ms": shutdown_ms,
    });
    let pool_passed = completed == POOL_CONCURRENCY + POOL_QUEUE_CAPACITY
        && rejected == 0
        && refill_completed
        && peak_active > 0
        && peak_queued > 0;
    results.insert(
        "offline-restore-pool-depletion-refill".into(),
        if pool_passed {
            pass(
                "bounded pool drained its admitted work and accepted a refill",
                pool_observations.clone(),
            )
        } else {
            failure(
                "pool depletion or refill validation failed",
                pool_observations.clone(),
            )
        },
    );
    results.insert(
        "offline-restore-throughput-latency".into(),
        if pool_passed && throughput_requests_per_second > 0.0 {
            pass(
                "concurrent restored-request throughput and latency recorded",
                pool_observations.clone(),
            )
        } else {
            failure(
                "concurrent restored-request performance run did not complete",
                pool_observations.clone(),
            )
        },
    );
    results.insert(
        "offline-restore-clean-shutdown".into(),
        if pool_passed && final_active == 0 && final_queued == 0 {
            pass(
                "pool drained before shutdown with no active or queued requests",
                pool_observations,
            )
        } else {
            failure(
                "pool resource accounting was nonzero before shutdown",
                pool_observations,
            )
        },
    );

    let timer_cases = [
        "timer-presence",
        "timer-timeout-ordering",
        "timer-cancellation",
    ];
    let timer_results: Vec<ProbeResult> = timer_cases
        .iter()
        .map(|case| execute_case(worker, version, case, origin, denied_origin, dns_origin))
        .collect();
    results.insert(
        "offline-restore-timer-after-restore".into(),
        if timer_results.iter().all(|probe| probe.outcome == "pass") {
            pass(
                "timer presence, ordering, and cancellation passed after restore",
                json!({ "cases": timer_cases }),
            )
        } else {
            failure(
                "timer behavior failed after restore",
                json!({
                    "cases": timer_cases.iter().zip(timer_results.iter()).map(
                        |(case, probe)| json!({ "case": case, "outcome": probe.outcome, "detail": probe.detail })
                    ).collect::<Vec<_>>()
                }),
            )
        },
    );

    let network_before = execute_case(
        worker,
        version,
        "fetch-v1-get",
        origin,
        denied_origin,
        dns_origin,
    );
    let cancellation = execute_case(
        worker,
        version,
        "fetch-v2-cancellation",
        origin,
        denied_origin,
        dns_origin,
    );
    let network_after = execute_case(
        worker,
        version,
        "fetch-v1-get",
        origin,
        denied_origin,
        dns_origin,
    );
    let network_passed = [&network_before, &cancellation, &network_after]
        .iter()
        .all(|probe| probe.outcome == "pass");
    results.insert(
        "offline-restore-network-after-restore".into(),
        if network_passed {
            pass(
                "network behavior passed across fresh restored VMs",
                Value::Null,
            )
        } else {
            failure(
                "network behavior failed across fresh restored VMs",
                json!({
                    "before": network_before.outcome,
                    "cancellation": cancellation.outcome,
                    "after": network_after.outcome,
                }),
            )
        },
    );
    results.insert(
        "offline-restore-cancellation-recovery".into(),
        if cancellation.outcome == "pass" && network_after.outcome == "pass" {
            pass(
                "a cancelled request was followed by a successful fresh-VM request",
                Value::Null,
            )
        } else {
            failure(
                "request cancellation did not recover cleanly",
                json!({
                    "cancellation": cancellation.outcome,
                    "recovery": network_after.outcome,
                }),
            )
        },
    );

    let tenant_one = execute_case(
        worker,
        version,
        "tenant-isolation",
        origin,
        denied_origin,
        dns_origin,
    );
    let tenant_two = execute_case(
        worker,
        version,
        "tenant-isolation",
        origin,
        denied_origin,
        dns_origin,
    );
    let isolated = [&tenant_one, &tenant_two].iter().all(|probe| {
        probe.outcome == "pass"
            && probe
                .observations
                .get("request_count")
                .and_then(Value::as_u64)
                == Some(1)
    });
    results.insert(
        "offline-restore-tenant-isolation".into(),
        if isolated {
            pass(
                "module state was reset for each tenant request VM",
                json!({
                    "first": tenant_one.observations,
                    "second": tenant_two.observations,
                }),
            )
        } else {
            failure(
                "module state leaked across restored request VMs",
                json!({
                    "first": tenant_one.observations,
                    "second": tenant_two.observations,
                }),
            )
        },
    );

    (
        results,
        PerformanceEvidence {
            cold_boot_ms,
            repeated_restore_cycles: RESTORE_CYCLES,
            sequential_profiles,
            pool: PoolPerformance {
                max_concurrent_sandboxes: POOL_CONCURRENCY,
                queue_capacity: POOL_QUEUE_CAPACITY,
                submitted: POOL_CONCURRENCY + POOL_QUEUE_CAPACITY,
                completed,
                rejected,
                wall_ms,
                throughput_requests_per_second,
                p50_total_ms,
                p95_total_ms,
                peak_active,
                peak_queued,
                final_active,
                final_queued,
                shutdown_ms,
                refill_completed,
            },
        },
    )
}

fn pass(detail: &str, observations: Value) -> ProbeResult {
    ProbeResult {
        outcome: "pass".into(),
        detail: detail.into(),
        observations,
    }
}

fn failure(detail: &str, observations: Value) -> ProbeResult {
    ProbeResult {
        outcome: "fail".into(),
        detail: detail.into(),
        observations,
    }
}

fn percentile(sorted: &[f64], percentile: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn probe_request(
    request_id: &str,
    case_id: &str,
    origin: &str,
    denied_origin: &str,
    dns_origin: &str,
) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: request_id.into(),
        method: "GET".into(),
        url: format!("https://evidence.invalid/case/{case_id}"),
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
    }
}

fn execute_profiled_case(
    worker: &WorkerVersionSandbox,
    version: &hyperlight_unikraft::workerd::WorkerVersionId,
    request_id: &str,
    case_id: &str,
    origin: &str,
    denied_origin: &str,
    dns_origin: &str,
) -> (ProbeResult, ExecutionProfile) {
    let (result, profile) = worker.execute_profiled(
        version,
        probe_request(request_id, case_id, origin, denied_origin, dns_origin),
        Duration::from_secs(15),
    );
    (probe_from_execution(result), profile)
}

fn probe_from_execution(
    result: hyperlight_unikraft::workerd::Result<hyperlight_unikraft::workerd::ResponseEnvelope>,
) -> ProbeResult {
    match result {
        Ok(response) if response.status == 200 => STANDARD
            .decode(response.body_base64)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_else(|| failure("executor returned malformed probe JSON", Value::Null)),
        Ok(response) => failure(
            &format!("executor returned HTTP {}", response.status),
            json!({ "headers": response.headers }),
        ),
        Err(error) => failure(&format!("fresh VM execution failed: {error}"), Value::Null),
    }
}

fn execute_case(
    worker: &WorkerVersionSandbox,
    version: &hyperlight_unikraft::workerd::WorkerVersionId,
    id: &str,
    origin: &str,
    denied_origin: &str,
    dns_origin: &str,
) -> ProbeResult {
    probe_from_execution(worker.execute(
        version,
        probe_request(
            &format!("evidence-{id}"),
            id,
            origin,
            denied_origin,
            dns_origin,
        ),
        Duration::from_secs(15),
    ))
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
