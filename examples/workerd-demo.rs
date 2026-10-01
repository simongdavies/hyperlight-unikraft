// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.
#![recursion_limit = "256"]

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    FetchBroker, FetchBrokerConfig, FetchLimits, FetchPolicy, Header, MAX_BODY_BYTES,
    MAX_HEADER_BYTES, PROTOCOL_VERSION, PrewarmPolicy, RequestEnvelope, WorkerBundle,
    WorkerPoolRestoreMode, WorkerRequestPool, WorkerVersionId, WorkerVersionSandbox,
};
use hyperlight_unikraft::{AllowList, NetworkPolicy};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const DEFAULT_ROOTFS: &str = "build-elfloader/workerd-executor/rootfs.img";
const DEFAULT_EXECUTOR: &str = "build-elfloader/workerd-executor/executor";
const DEFAULT_BUNDLE: &str = "examples/workerd-bundles/helloworld_esm.json";
const MAX_REQUEST_HEAD_BYTES: usize = MAX_HEADER_BYTES + 8 * 1024;
const DEFAULT_MAX_CONCURRENT_SANDBOXES: usize = 4;
const DEFAULT_PREWARMED_SANDBOXES: usize = 4;
const DEFAULT_MAX_CONCURRENT_RESTORES: usize = 1;
const DEFAULT_WARM_FLOOR: usize = 1;
const DEFAULT_MAX_REPLENISH_BATCH: usize = 2;
const DEFAULT_QUEUE_CAPACITY: usize = 64;
const DEFAULT_SCRATCH_MIB: usize = 344;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestoreMode {
    OnDemand,
    Prewarmed,
}

struct Options {
    bind: String,
    rootfs: PathBuf,
    executor: PathBuf,
    bundle: Option<PathBuf>,
    script: Option<PathBuf>,
    version: WorkerVersionId,
    compatibility_date: String,
    scratch_mb: usize,
    init_timeout: Duration,
    request_timeout: Duration,
    restore_mode: RestoreMode,
    prewarmed_sandboxes: Option<usize>,
    max_concurrent_restores: Option<usize>,
    warm_floor: Option<usize>,
    ready_low_watermark: Option<usize>,
    ready_high_watermark: Option<usize>,
    max_replenish_batch: Option<usize>,
    diagnostic_no_refill_wave: Option<usize>,
    max_concurrent_sandboxes: usize,
    queue_capacity: usize,
    profile_log_every: usize,
    fetch_loopback_port: Option<u16>,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            bind: "0.0.0.0:8787".into(),
            rootfs: DEFAULT_ROOTFS.into(),
            executor: DEFAULT_EXECUTOR.into(),
            bundle: Some(DEFAULT_BUNDLE.into()),
            script: None,
            version: WorkerVersionId::new("demo-v1").map_err(|e| e.to_string())?,
            compatibility_date: "2025-01-01".into(),
            scratch_mb: DEFAULT_SCRATCH_MIB,
            init_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(2),
            restore_mode: RestoreMode::OnDemand,
            prewarmed_sandboxes: None,
            max_concurrent_restores: None,
            warm_floor: None,
            ready_low_watermark: None,
            ready_high_watermark: None,
            max_replenish_batch: None,
            diagnostic_no_refill_wave: None,
            max_concurrent_sandboxes: DEFAULT_MAX_CONCURRENT_SANDBOXES,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            profile_log_every: 1,
            fetch_loopback_port: None,
        };
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = || {
                args.next()
                    .ok_or_else(|| format!("missing value for {arg}"))
            };
            match arg.as_str() {
                "--bind" => options.bind = value()?,
                "--rootfs" => options.rootfs = value()?.into(),
                "--executor" => options.executor = value()?.into(),
                "--bundle" => {
                    options.bundle = Some(value()?.into());
                    options.script = None;
                }
                "--script" => {
                    options.script = Some(value()?.into());
                    options.bundle = None;
                }
                "--version" => {
                    options.version = WorkerVersionId::new(value()?).map_err(|e| e.to_string())?
                }
                "--compatibility-date" => options.compatibility_date = value()?,
                "--scratch-mb" => {
                    options.scratch_mb = value()?
                        .parse()
                        .map_err(|_| "invalid --scratch-mb".to_string())?
                }
                "--init-timeout-ms" => {
                    options.init_timeout = duration(value()?, "--init-timeout-ms")?
                }
                "--request-timeout-ms" => {
                    options.request_timeout = duration(value()?, "--request-timeout-ms")?
                }
                "--restore-mode" => options.restore_mode = parse_restore_mode(&value()?)?,
                "--prewarmed-sandboxes" => {
                    options.prewarmed_sandboxes =
                        Some(nonzero_usize(value()?, "--prewarmed-sandboxes")?)
                }
                "--max-concurrent-restores" => {
                    options.max_concurrent_restores =
                        Some(nonzero_usize(value()?, "--max-concurrent-restores")?)
                }
                "--warm-floor" => {
                    options.warm_floor = Some(nonzero_usize(value()?, "--warm-floor")?)
                }
                "--ready-low-watermark" => {
                    options.ready_low_watermark =
                        Some(nonzero_usize(value()?, "--ready-low-watermark")?)
                }
                "--ready-high-watermark" => {
                    options.ready_high_watermark =
                        Some(nonzero_usize(value()?, "--ready-high-watermark")?)
                }
                "--max-replenish-batch" => {
                    options.max_replenish_batch =
                        Some(nonzero_usize(value()?, "--max-replenish-batch")?)
                }
                "--diagnostic-no-refill-wave" => {
                    options.diagnostic_no_refill_wave =
                        Some(nonzero_usize(value()?, "--diagnostic-no-refill-wave")?)
                }
                "--max-concurrent-sandboxes" => {
                    options.max_concurrent_sandboxes =
                        nonzero_usize(value()?, "--max-concurrent-sandboxes")?
                }
                "--queue-capacity" => {
                    options.queue_capacity = nonzero_usize(value()?, "--queue-capacity")?
                }
                "--profile-log-every" => {
                    options.profile_log_every = usize_value(value()?, "--profile-log-every")?
                }
                "--fetch-loopback-port" => {
                    options.fetch_loopback_port = Some(
                        value()?
                            .parse()
                            .map_err(|_| "invalid --fetch-loopback-port".to_string())?,
                    )
                }
                "--help" | "-h" => {
                    return Err(format!(
                        "usage: workerd-demo [--bind ADDR] [--rootfs CPIO] \
                         [--executor ELF] [--version ID] [--scratch-mb MIB] \
                         [--bundle JSON | --script JS] [--compatibility-date YYYY-MM-DD] \
                         [--init-timeout-ms MS] [--request-timeout-ms MS] \
                         [--restore-mode on-demand|prewarmed] [--prewarmed-sandboxes N] \
                         [--max-concurrent-restores N] \
                         [--warm-floor N] [--ready-low-watermark N] \
                         [--ready-high-watermark N] [--max-replenish-batch N] \
                         [--diagnostic-no-refill-wave N] \
                         [--max-concurrent-sandboxes N] [--queue-capacity N] \
                         [--profile-log-every N]\n\
                         [--fetch-loopback-port PORT]\n\
                         defaults: --bind 0.0.0.0:8787 --rootfs {DEFAULT_ROOTFS} \
                         --executor {DEFAULT_EXECUTOR} --version demo-v1 \
                         --bundle {DEFAULT_BUNDLE} \
                         --scratch-mb {DEFAULT_SCRATCH_MIB} --init-timeout-ms 30000 \
                         --request-timeout-ms 2000 \
                         --restore-mode on-demand \
                         --prewarmed-sandboxes {DEFAULT_PREWARMED_SANDBOXES} \
                         --max-concurrent-restores {DEFAULT_MAX_CONCURRENT_RESTORES} (prewarmed) \
                         --warm-floor {DEFAULT_WARM_FLOOR} \
                         --ready-low-watermark active/2 \
                         --ready-high-watermark active \
                         --max-replenish-batch {DEFAULT_MAX_REPLENISH_BATCH} \
                         --max-concurrent-sandboxes {DEFAULT_MAX_CONCURRENT_SANDBOXES} \
                         --queue-capacity {DEFAULT_QUEUE_CAPACITY} \
                         --profile-log-every 1\n\
                         Outbound fetch is denied by default. --fetch-loopback-port explicitly \
                         allows HTTP fetches to localhost on exactly PORT for the demo.\n\
                         On-demand owners restore one fresh VM per request. Prewarmed owners restore \
                         before advertising readiness, execute at most one request, drop the VM, and \
                         replenish adaptively below explicit ready watermarks. The warm floor is \
                         never dispatched. --max-concurrent-restores bounds restore CPU pressure and \
                         --max-concurrent-sandboxes remains the active execution cap; \
                         requests above it queue up to --queue-capacity and overflow receives HTTP 503."
                    ));
                }
                _ => return Err(format!("unknown argument: {arg}")),
            }
        }
        validate_restore_options(
            options.restore_mode,
            &[
                ("--prewarmed-sandboxes", options.prewarmed_sandboxes),
                ("--max-concurrent-restores", options.max_concurrent_restores),
                ("--warm-floor", options.warm_floor),
                ("--ready-low-watermark", options.ready_low_watermark),
                ("--ready-high-watermark", options.ready_high_watermark),
                ("--max-replenish-batch", options.max_replenish_batch),
                (
                    "--diagnostic-no-refill-wave",
                    options.diagnostic_no_refill_wave,
                ),
            ],
        )?;
        Ok(options)
    }
}

fn parse_restore_mode(value: &str) -> Result<RestoreMode, String> {
    match value {
        "on-demand" => Ok(RestoreMode::OnDemand),
        "prewarmed" => Ok(RestoreMode::Prewarmed),
        _ => Err("--restore-mode must be on-demand or prewarmed".to_string()),
    }
}

fn validate_restore_options(
    mode: RestoreMode,
    prewarm_options: &[(&str, Option<usize>)],
) -> Result<(), String> {
    if mode == RestoreMode::OnDemand {
        for (flag, value) in prewarm_options {
            if value.is_some() {
                return Err(format!(
                    "{flag} is only valid with --restore-mode prewarmed"
                ));
            }
        }
    }
    Ok(())
}

fn nonzero_usize(value: String, flag: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| format!("invalid {flag}"))?;
    if value == 0 {
        return Err(format!("{flag} must be nonzero"));
    }
    Ok(value)
}

fn usize_value(value: String, flag: &str) -> Result<usize, String> {
    value.parse().map_err(|_| format!("invalid {flag}"))
}

fn duration(value: String, flag: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<u64>()
        .map_err(|_| format!("invalid {flag}"))?;
    if millis == 0 {
        return Err(format!("{flag} must be nonzero"));
    }
    Ok(Duration::from_millis(millis))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = match Options::parse() {
        Ok(options) => options,
        Err(message) if env::args().any(|arg| arg == "--help" || arg == "-h") => {
            println!("{message}");
            return Ok(());
        }
        Err(message) => return Err(message.into()),
    };
    let bundle = if let Some(path) = &options.bundle {
        WorkerBundle::from_path(path)?
    } else {
        let path = options
            .script
            .as_ref()
            .expect("bundle or script is required");
        WorkerBundle::single_script(
            options.version.clone(),
            &options.compatibility_date,
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("worker.js"),
            std::fs::read_to_string(path)?,
        )?
    };
    let bundle_sha256 = bundle.sha256()?;
    let version = bundle.worker_version.clone();
    let fetch_broker = if let Some(port) = options.fetch_loopback_port {
        FetchBroker::new(FetchBrokerConfig {
            policy: FetchPolicy::new(
                NetworkPolicy::AllowList(AllowList::from_hosts(&["localhost"])?),
                ["http"],
                [port],
            )
            .allow_loopback(true),
            limits: FetchLimits::default(),
        })?
    } else {
        FetchBroker::denied()
    };
    let worker = WorkerVersionSandbox::initialize_with_fetch(
        bundle,
        &options.rootfs,
        &options.executor,
        options.scratch_mb,
        options.init_timeout,
        fetch_broker,
    )?;
    let owner_count = match options.restore_mode {
        RestoreMode::OnDemand => options.max_concurrent_sandboxes,
        RestoreMode::Prewarmed => options
            .prewarmed_sandboxes
            .unwrap_or(DEFAULT_PREWARMED_SANDBOXES),
    };
    let prewarmed_sandboxes =
        (options.restore_mode == RestoreMode::Prewarmed).then_some(owner_count);
    let max_concurrent_restores = match options.restore_mode {
        RestoreMode::OnDemand => None,
        RestoreMode::Prewarmed => Some(
            options
                .max_concurrent_restores
                .unwrap_or(DEFAULT_MAX_CONCURRENT_RESTORES),
        ),
    };
    let prewarm_policy = (options.restore_mode == RestoreMode::Prewarmed).then(|| {
        let warm_floor = options.warm_floor.unwrap_or(DEFAULT_WARM_FLOOR);
        let ready_high_watermark = options
            .ready_high_watermark
            .unwrap_or(options.max_concurrent_sandboxes)
            .min(owner_count);
        PrewarmPolicy {
            warm_floor,
            ready_low_watermark: options.ready_low_watermark.unwrap_or(
                (ready_high_watermark / 2)
                    .max(warm_floor.saturating_add(1))
                    .min(ready_high_watermark),
            ),
            ready_high_watermark,
            max_replenish_batch: options
                .max_replenish_batch
                .unwrap_or(DEFAULT_MAX_REPLENISH_BATCH),
            diagnostic_no_refill_wave: options.diagnostic_no_refill_wave,
        }
    });
    let effective_concurrency = prewarm_policy.map_or(options.max_concurrent_sandboxes, |policy| {
        owner_count
            .saturating_sub(policy.warm_floor)
            .min(options.max_concurrent_sandboxes)
    });
    let restore_mode = match options.restore_mode {
        RestoreMode::OnDemand => WorkerPoolRestoreMode::OnDemand,
        RestoreMode::Prewarmed => WorkerPoolRestoreMode::Prewarmed {
            sandboxes: owner_count,
            max_concurrent_restores: max_concurrent_restores.unwrap(),
            policy: prewarm_policy.unwrap(),
        },
    };
    let pool = WorkerRequestPool::with_restore_mode(
        worker,
        options.max_concurrent_sandboxes,
        options.queue_capacity,
        restore_mode,
    )?;
    let listener = TcpListener::bind(&options.bind)?;
    eprintln!(
        "workerd demo listening on http://{} (Worker {}, bundle {}, restore {}, {} active cap, {} owners, {} effective concurrency, {} restores, warm floor {}, ready low/high {}/{}, refill batch {}, queue {})",
        listener.local_addr()?,
        version.as_str(),
        bundle_sha256,
        match options.restore_mode {
            RestoreMode::OnDemand => "on-demand",
            RestoreMode::Prewarmed => "prewarmed",
        },
        options.max_concurrent_sandboxes,
        owner_count,
        effective_concurrency,
        max_concurrent_restores.unwrap_or(options.max_concurrent_sandboxes),
        prewarm_policy.map_or(0, |policy| policy.warm_floor),
        prewarm_policy.map_or(0, |policy| policy.ready_low_watermark),
        prewarm_policy.map_or(0, |policy| policy.ready_high_watermark),
        prewarm_policy.map_or(0, |policy| policy.max_replenish_batch),
        options.queue_capacity
    );
    let sequence = AtomicU64::new(1);
    let profile_sequence = AtomicU64::new(1);
    let profile_samples = Arc::new(AtomicU64::new(0));
    for connection in listener.incoming() {
        let mut stream = match connection {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("accept failed: {error}");
                continue;
            }
        };
        let request_id = format!("http-{}", sequence.fetch_add(1, Ordering::Relaxed));
        if let Err(error) = stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .and_then(|()| stream.set_write_timeout(Some(Duration::from_secs(5))))
        {
            eprintln!("connection setup failed: {error}");
            continue;
        }
        let request = match read_request(&mut stream, request_id) {
            Ok(request) => request,
            Err(error) => {
                if let Err(write_error) = write_error(&mut stream, 400, &error) {
                    eprintln!("bad request response failed: {write_error}");
                }
                continue;
            }
        };
        if request_path(&request.url) == "/__hyperlight/pool-status" {
            let status = pool.status();
            let body = serde_json::to_vec(&serde_json::json!({
                "restore_mode": match options.restore_mode {
                    RestoreMode::OnDemand => "on-demand",
                    RestoreMode::Prewarmed => "prewarmed",
                },
                "admitted": status.admitted,
                "active": status.active,
                "queued": status.queued,
                "queue_capacity": status.queue_capacity,
                "execution_slots_in_use": status.execution_slots_in_use,
                "restore_slots_in_use": status.restore_slots_in_use,
                "recycle_queue_depth": status.recycle_queue_depth,
                "recycle_queue_peak": status.recycle_queue_peak,
                "teardown_in_flight": status.teardown_in_flight,
                "teardown_peak": status.teardown_peak,
                "completed_teardowns": status.completed_teardowns,
                "teardown_total_ms": status.teardown_total_ms,
                "teardown_average_ms": average_ms(
                    status.teardown_total_ms,
                    status.completed_teardowns,
                ),
                "teardown_max_ms": status.teardown_max_ms,
                "completion_queue_depth": status.completion_queue_depth,
                "completion_queue_peak": status.completion_queue_peak,
                "completion_in_flight": status.completion_in_flight,
                "completion_peak": status.completion_peak,
                "completed_completions": status.completed_completions,
                "completion_total_ms": status.completion_total_ms,
                "completion_average_ms": average_ms(
                    status.completion_total_ms,
                    status.completed_completions,
                ),
                "completion_max_ms": status.completion_max_ms,
                "max_concurrent_sandboxes": options.max_concurrent_sandboxes,
                "owner_count": owner_count,
                "effective_concurrency": effective_concurrency,
                "prewarmed_sandboxes": prewarmed_sandboxes,
                "max_concurrent_restores": max_concurrent_restores,
                "prewarmed_inventory": status.prewarmed_inventory,
                "warm_floor": status.warm_floor,
                "ready_low_watermark": status.ready_low_watermark,
                "ready_high_watermark": status.ready_high_watermark,
                "max_replenish_batch": status.max_replenish_batch,
                "replenishment_paused": status.replenishment_paused,
                "replenishment_pause_reason": status.replenishment_pause_reason,
                "refill_active": status.refill_active,
                "idle_owners": status.idle_owners,
                "restore_permits_outstanding": status.restore_permits_outstanding,
                "diagnostic_wave_dispatched": status.diagnostic_wave_dispatched,
                "diagnostic_wave_completed": status.diagnostic_wave_completed,
                "prewarmed_ready": status.ready,
                "prewarmed_ready_min": status.ready_min,
                "prewarmed_ready_peak": status.ready_peak,
                "prewarmed_replenishing": status.replenishing,
                "prewarmed_replenishing_min": status.replenishing_min,
                "prewarmed_replenishing_peak": status.replenishing_peak,
                "restore_attempts": status.restore_attempts,
                "completed_restores": status.completed_restores,
                "failed_restores": status.failed_restores,
                "completed_replenishment_policy_waits": status.completed_replenishment_policy_waits,
                "replenishment_policy_wait_total_ms": status.replenishment_policy_wait_total_ms,
                "replenishment_policy_wait_average_ms": average_ms(
                    status.replenishment_policy_wait_total_ms,
                    status.completed_replenishment_policy_waits,
                ),
                "replenishment_policy_wait_max_ms": status.replenishment_policy_wait_max_ms,
                "restore_wait_total_ms": status.restore_wait_total_ms,
                "restore_wait_average_ms": average_ms(
                    status.restore_wait_total_ms,
                    status.restore_attempts,
                ),
                "restore_wait_max_ms": status.restore_wait_max_ms,
                "restore_total_ms": status.restore_total_ms,
                "restore_average_ms": average_ms(
                    status.restore_total_ms,
                    status.restore_attempts,
                ),
                "restore_max_ms": status.restore_max_ms,
                "prewarmed_hits": status.prewarmed_hits,
                "prewarmed_misses": status.prewarmed_misses,
                "profile_log_every": options.profile_log_every,
                "profile_samples_logged": profile_samples.load(Ordering::Relaxed),
            }))?;
            write_response(&mut stream, 200, "application/json", &body)?;
            continue;
        }
        let request_sequence = profile_sequence.fetch_add(1, Ordering::Relaxed);
        let profile_log_every = options.profile_log_every;
        let profile_samples = profile_samples.clone();
        let _ = pool.try_submit(request, options.request_timeout, move |execution| {
            if let Err(error) = finish_request(
                &mut stream,
                execution,
                request_sequence,
                profile_log_every,
                &profile_samples,
            ) {
                eprintln!("request response failed: {error}");
            }
        });
    }
    Ok(())
}

fn request_path(url: &str) -> &str {
    let authority_and_path = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = authority_and_path
        .find('/')
        .map_or("/", |index| &authority_and_path[index..]);
    path.split_once('?').map_or(path, |(path, _)| path)
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        reason(status),
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}

fn finish_request(
    stream: &mut TcpStream,
    execution: hyperlight_unikraft::workerd::RequestExecution,
    request_sequence: u64,
    profile_log_every: usize,
    profile_samples: &AtomicU64,
) -> Result<(), Box<dyn std::error::Error>> {
    let request_id = execution.request_id;
    if should_log_profile(request_sequence, profile_log_every) {
        let sample = profile_samples.fetch_add(1, Ordering::Relaxed) + 1;
        eprintln!(
            "workerd profile sample={sample} sequence={request_sequence} request_id={request_id}: {}",
            serde_json::to_string(&execution.profile)?
        );
    }
    if let Some(error) = execution.submit_error {
        write_error(stream, 503, submit_error_message(error))?;
        return Ok(());
    }
    match execution.result {
        Ok(response) => {
            let body = STANDARD.decode(response.body_base64)?;
            write!(
                stream,
                "HTTP/1.1 {} {}\r\n",
                response.status,
                reason(response.status)
            )?;
            for header in response.headers {
                if !header.name.eq_ignore_ascii_case("content-length")
                    && !header.name.eq_ignore_ascii_case("connection")
                {
                    write!(stream, "{}: {}\r\n", header.name, header.value)?;
                }
            }
            write!(
                stream,
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )?;
            stream.write_all(&body)?;
        }
        Err(hyperlight_unikraft::workerd::Error::Timeout) => {
            write_error(stream, 504, "Worker timed out")?
        }
        Err(error) => {
            eprintln!("Worker {request_id} failed: {error}");
            write_error(stream, 502, "Worker execution failed")?;
        }
    }
    Ok(())
}

fn should_log_profile(request_sequence: u64, profile_log_every: usize) -> bool {
    profile_log_every != 0 && (request_sequence - 1).is_multiple_of(profile_log_every as u64)
}

fn average_ms(total_ms: f64, count: usize) -> Option<f64> {
    (count != 0).then(|| total_ms / count as f64)
}

fn submit_error_message(error: hyperlight_unikraft::workerd::PoolSubmitError) -> &'static str {
    match error {
        hyperlight_unikraft::workerd::PoolSubmitError::Full => "Worker request queue is full",
        hyperlight_unikraft::workerd::PoolSubmitError::ShuttingDown => {
            "Worker request pool is shutting down"
        }
        hyperlight_unikraft::workerd::PoolSubmitError::Unavailable => {
            "Worker request pool is unavailable"
        }
    }
}

fn read_request(stream: &mut TcpStream, request_id: String) -> Result<RequestEnvelope, String> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if bytes.len() > MAX_REQUEST_HEAD_BYTES {
            return Err("request headers exceed limit".into());
        }
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request headers".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = std::str::from_utf8(&bytes[..head_end])
        .map_err(|_| "headers are not UTF-8")?
        .to_owned();
    let mut lines = head[..head.len() - 4].split("\r\n");
    let mut request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?
        .split_ascii_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| "missing method".to_string())?;
    let target = request_line
        .next()
        .ok_or_else(|| "missing request target".to_string())?;
    if request_line.next() != Some("HTTP/1.1") || request_line.next().is_some() {
        return Err("only HTTP/1.1 is supported".into());
    }
    let mut headers = Vec::new();
    let mut host = None;
    let mut content_length = 0usize;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "malformed header".to_string())?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("host") {
            host = Some(value);
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse()
                .map_err(|_| "invalid Content-Length".to_string())?;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("Transfer-Encoding is not supported".into());
        }
        headers.push(Header {
            name: name.into(),
            value: value.into(),
        });
    }
    if content_length > MAX_BODY_BYTES {
        return Err("request body exceeds limit".into());
    }
    while bytes.len() - head_end < content_length {
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request body".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() - head_end > content_length {
            return Err("bytes after request body".into());
        }
    }
    if bytes.len() - head_end != content_length {
        return Err("bytes after request body".into());
    }
    let url = if target.starts_with("http://") || target.starts_with("https://") {
        target.into()
    } else {
        let host = host.ok_or_else(|| "missing Host header".to_string())?;
        format!("http://{host}{target}")
    };
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id,
        method: method.into(),
        url,
        headers,
        body_base64: STANDARD.encode(&bytes[head_end..]),
    };
    request.validate().map_err(|error| error.to_string())?;
    Ok(request)
}

fn write_error(stream: &mut TcpStream, status: u16, message: &str) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{message}",
        reason(status),
        message.len()
    )
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Worker Response",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_mode_switch_accepts_only_documented_values() {
        assert_eq!(parse_restore_mode("on-demand"), Ok(RestoreMode::OnDemand));
        assert_eq!(parse_restore_mode("prewarmed"), Ok(RestoreMode::Prewarmed));
        assert!(parse_restore_mode("warm").is_err());
    }

    #[test]
    fn restore_limit_is_prewarmed_only() {
        assert!(
            validate_restore_options(
                RestoreMode::OnDemand,
                &[("--max-concurrent-restores", None)]
            )
            .is_ok()
        );
        assert!(
            validate_restore_options(
                RestoreMode::Prewarmed,
                &[("--max-concurrent-restores", Some(2))],
            )
            .is_ok()
        );
        assert!(
            validate_restore_options(
                RestoreMode::OnDemand,
                &[("--max-concurrent-restores", Some(1))],
            )
            .is_err()
        );
    }

    #[test]
    fn prewarmed_sandbox_count_is_prewarmed_only() {
        assert!(
            validate_restore_options(
                RestoreMode::Prewarmed,
                &[("--prewarmed-sandboxes", Some(2))],
            )
            .is_ok()
        );
        assert!(
            validate_restore_options(RestoreMode::OnDemand, &[("--prewarmed-sandboxes", Some(2))],)
                .is_err()
        );
    }

    #[test]
    fn profile_logging_interval_is_explicit_and_deterministic() {
        assert_eq!(usize_value("0".into(), "--profile-log-every"), Ok(0));
        assert_eq!(usize_value("64".into(), "--profile-log-every"), Ok(64));
        assert!(usize_value("all".into(), "--profile-log-every").is_err());
        assert!(should_log_profile(1, 1));
        assert!(should_log_profile(1, 64));
        assert!(!should_log_profile(2, 64));
        assert!(should_log_profile(65, 64));
        assert!(!should_log_profile(1, 0));
    }

    #[test]
    fn metric_averages_are_null_until_observed() {
        assert_eq!(average_ms(0.0, 0), None);
        assert_eq!(average_ms(12.0, 3), Some(4.0));
    }

    #[test]
    fn submit_errors_have_distinct_service_unavailable_messages() {
        use hyperlight_unikraft::workerd::PoolSubmitError;

        assert_eq!(
            submit_error_message(PoolSubmitError::Full),
            "Worker request queue is full"
        );
        assert_eq!(
            submit_error_message(PoolSubmitError::Unavailable),
            "Worker request pool is unavailable"
        );
        assert_eq!(
            submit_error_message(PoolSubmitError::ShuttingDown),
            "Worker request pool is shutting down"
        );
    }
}
