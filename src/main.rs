// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;

use hyperlight_unikraft::{
    AllowList, AppSandbox, BlockList, DEFAULT_SCRATCH_MB, Error, Exec, ListenPorts, Mount,
    NetworkPolicy, SandboxBuilder, load_snapshot,
};

/// The CLI's own errors are messages for the terminal; the library's
/// come through as they are.
type CliResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Minimal Hyperlight host for Unikraft unikernels.
#[derive(Parser)]
#[command(name = "hluk")]
struct Cli {
    /// Log level for hluk diagnostics: error, warn, info, debug, trace.
    /// Off by default; pass --log-level info to see timing.
    #[arg(long, global = true)]
    log_level: Option<tracing::Level>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Boot a guest and run a script, inline code, a guest command, or
    /// its entry point.
    Run(RunArgs),

    /// Snapshot operations: save a post-evolve snapshot to disk,
    /// or restore from a saved snapshot and dispatch.
    #[command(subcommand)]
    Snapshot(SnapshotCommand),

    /// Benchmark modes with structured timing output.
    #[command(subcommand)]
    Bench(BenchCommand),

    /// Initialize, snapshot, fresh-restore and fetch a trusted Worker bundle.
    Workerd(WorkerdArgs),

    /// Orchestrator-ready resident Workerd host: a long-running,
    /// multi-app, multi-process-friendly server with liveness/readiness/
    /// status contract endpoints and a graceful signal-driven drain. Not a
    /// replacement for `workerd` (single-request CLI): this is the host
    /// Boundaries 1-3's resident VM, bounded pool, and multi-app routing
    /// are meant to run under.
    WorkerdHost(WorkerdHostArgs),

    /// One-shot "prewarm": boot a trusted Worker bundle, run its guest
    /// `init` call, and save the post-init snapshot to disk — the
    /// load -> init -> snapshot half of the load -> init -> snapshot ->
    /// restore x N pattern. Boots with the same deny-all capability
    /// policy `AppRegistry::from_host_config` always builds today, so the
    /// saved directory is directly usable from a `HostConfig`'s
    /// `AppConfig.snapshot_dir` to restore N apps without paying this
    /// boot+init cost again in each of their processes.
    WorkerdPrewarmSnapshot(WorkerdPrewarmSnapshotArgs),

    /// Print canonical bundle and actual configured policy identities without booting a VM.
    WorkerdIdentity(WorkerdIdentityArgs),
}

#[derive(clap::Args)]
#[command(group(clap::ArgGroup::new("input").required(true).args(["bundle", "config"])))]
struct WorkerdIdentityArgs {
    #[arg(long, conflicts_with = "config")]
    bundle: Option<PathBuf>,
    #[arg(long, conflicts_with = "bundle")]
    config: Option<PathBuf>,
}

fn cmd_workerd_identity(args: WorkerdIdentityArgs) -> CliResult<()> {
    use hyperlight_unikraft::workerd::{HostConfig, WorkerBundle, WorkerCapabilityPolicyConfig};
    let identity = |path: &PathBuf,
                    policy: &WorkerCapabilityPolicyConfig,
                    app_id: &str|
     -> CliResult<serde_json::Value> {
        let bundle = WorkerBundle::from_path(path)?;
        Ok(serde_json::json!({
            "worker_version": bundle.worker_version,
            "bundle_protocol_version": bundle.protocol_version,
            "bundle_sha256": bundle.sha256()?,
            "capability_policy_sha256": policy.sha256_for(app_id,bundle.worker_version.as_str())?,
        }))
    };
    if let Some(bundle) = args.bundle {
        println!(
            "{}",
            identity(
                &bundle,
                &WorkerCapabilityPolicyConfig::default(),
                "standalone"
            )?
        );
    } else if let Some(config) = args.config {
        let config = HostConfig::from_path(config)?;
        let identities = config
            .apps
            .iter()
            .map(|app| {
                let mut value =
                    identity(&app.bundle_path, &app.capability_policy, &app.route.app_id)?;
                value["app_id"] = serde_json::json!(app.route.app_id);
                Ok(value)
            })
            .collect::<CliResult<Vec<_>>>()?;
        println!("{}", serde_json::to_string(&identities)?);
    }
    Ok(())
}

#[derive(Subcommand)]
enum SnapshotCommand {
    /// Boot the guest, then save a snapshot to disk.
    Save(SaveArgs),

    /// Restore a guest from a saved snapshot and dispatch commands.
    Run(SnapshotRunArgs),
}

#[derive(clap::Args)]
struct WorkerdArgs {
    /// Trusted protocol-v1 Worker bundle JSON.
    #[arg(long, conflicts_with = "script")]
    bundle: Option<PathBuf>,

    /// Convenience single ES module; source is sent only during initialization.
    #[arg(long, conflicts_with = "bundle")]
    script: Option<PathBuf>,

    /// Worker version for --script.
    #[arg(long, default_value = "cli-v1")]
    version: String,

    /// Compatibility date for --script.
    #[arg(long, default_value = "2025-01-01")]
    compatibility_date: String,

    /// Packaged executor image/rootfs.
    #[arg(long, default_value = "build-elfloader/workerd-executor/rootfs.img")]
    rootfs: PathBuf,

    /// Matching trusted executor artifact.
    #[arg(long, default_value = "build-elfloader/workerd-executor/executor")]
    executor: PathBuf,

    /// Request URL.
    #[arg(long, default_value = "https://example.test/")]
    url: String,

    /// Scratch memory in MiB.
    #[arg(long, default_value_t = 512)]
    scratch_mb: usize,

    /// Initialization timeout in milliseconds.
    #[arg(long, default_value_t = 90_000)]
    init_timeout_ms: u64,

    /// Fetch timeout in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    request_timeout_ms: u64,
}

#[derive(clap::Args)]
struct WorkerdPrewarmSnapshotArgs {
    /// Trusted protocol-v1 Worker bundle JSON.
    #[arg(long, conflicts_with = "script")]
    bundle: Option<PathBuf>,

    /// Convenience single ES module; source is sent only during initialization.
    #[arg(long, conflicts_with = "bundle")]
    script: Option<PathBuf>,

    /// Worker version for --script.
    #[arg(long, default_value = "cli-v1")]
    version: String,

    /// Compatibility date for --script.
    #[arg(long, default_value = "2025-01-01")]
    compatibility_date: String,

    /// Packaged executor image/rootfs.
    #[arg(long, default_value = "build-elfloader/workerd-executor/rootfs.img")]
    rootfs: PathBuf,

    /// Matching trusted executor artifact.
    #[arg(long, default_value = "build-elfloader/workerd-executor/executor")]
    executor: PathBuf,

    /// Scratch memory in MiB. Must match the `scratch_memory_mb` every
    /// `AppConfig` restoring from this snapshot will use (the snapshot's
    /// own memory layout applies on restore; this only governs the fresh
    /// boot happening here).
    #[arg(long, default_value_t = 512)]
    scratch_mb: usize,

    /// Initialization timeout in milliseconds.
    #[arg(long, default_value_t = 90_000)]
    init_timeout_ms: u64,

    /// Directory to save the verified snapshot to. Must not already
    /// exist (snapshots are write-once, matching
    /// `workerd::VerifiedSnapshot::save`'s own guarantee).
    #[arg(long)]
    snapshot_dir: PathBuf,
}

/// Builds a `WorkerBundle` from `--bundle`/`--script` (or, if neither is
/// given, the demo helloworld bundle), shared by `workerd` and
/// `workerd-prewarm-snapshot` so both parse identical CLI shapes the same way.
fn build_workerd_bundle(
    bundle: Option<PathBuf>,
    script: Option<PathBuf>,
    version: String,
    compatibility_date: String,
) -> CliResult<hyperlight_unikraft::workerd::WorkerBundle> {
    use hyperlight_unikraft::workerd::{WorkerBundle, WorkerVersionId};
    match (bundle, script) {
        (Some(path), None) => Ok(WorkerBundle::from_path(path)?),
        (None, Some(path)) => Ok(WorkerBundle::single_script(
            WorkerVersionId::new(version)?,
            compatibility_date,
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("worker.js"),
            std::fs::read_to_string(&path)?,
        )?),
        (None, None) => Ok(WorkerBundle::from_path(
            "examples/workerd-bundles/helloworld_esm.json",
        )?),
        (Some(_), Some(_)) => unreachable!("clap rejects conflicting arguments"),
    }
}

/// Arguments for `workerd-host` — the orchestrator-ready multi-app,
/// multi-process-friendly resident Workerd host (Boundary 4).
#[derive(clap::Args)]
struct WorkerdHostArgs {
    /// Host configuration JSON: shared rootfs/executor plus every app's
    /// route, bundle, and pool (disposable or resident). See
    /// `hyperlight_unikraft::workerd::HostConfig`.
    #[arg(long)]
    config: PathBuf,

    /// Address the host listens on for app traffic, and — unless
    /// `--admin-bind` is set — the `/__hyperlight/*` contract endpoints.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,

    /// Optional separate address serving only the `/__hyperlight/*`
    /// contract endpoints (liveness/readiness/status), so an orchestrator
    /// can probe them on a port never exposed to app traffic. Defaults to
    /// sharing `--bind`.
    #[arg(long)]
    admin_bind: Option<String>,

    /// How long to wait for in-flight requests to finish after SIGTERM/
    /// SIGINT before giving up and exiting with a drain-timeout error
    /// (exit code 1).
    #[arg(long, default_value_t = 10_000)]
    drain_timeout_ms: u64,

    /// Maximum simultaneous app connections, including idle keep-alive sockets.
    #[arg(long, default_value_t = 256, value_parser = clap::value_parser!(u32).range(1..))]
    max_connections: u32,

    /// Separate connection budget for management probes.
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..))]
    max_admin_connections: u32,
}

/// Arguments for `run` — boot the embedded kernel + initrd and dispatch.
#[derive(clap::Args)]
struct RunArgs {
    /// Script file (.py, .js, …) to execute in the guest.
    #[arg(conflicts_with = "exec")]
    script: Option<PathBuf>,

    /// Path to a CPIO initrd to map into the guest.
    #[arg(long)]
    initrd: Option<PathBuf>,

    /// Entry point binary path inside the initrd VFS.
    /// Auto-detected from the initrd if not specified.
    #[arg(long)]
    entry: Option<String>,

    /// Advanced: boot a kernel from this path instead of the embedded one.
    /// Must match the host ABI this build expects, or the guest will fault.
    /// Intended for kernel development.
    #[arg(long, value_name = "PATH")]
    kernel: Option<PathBuf>,

    /// Scratch memory in MiB (default 256; increase for large rootfs).
    #[arg(long, default_value_t = DEFAULT_SCRATCH_MB)]
    scratch_mb: usize,

    /// Inline code to execute (alternative to a script file).
    #[arg(long, conflicts_with = "script")]
    exec: Option<String>,

    /// Run a command that already lives in the guest filesystem: a path plus
    /// optional args (e.g. "/app/server --port 8080"). Unlike a script file
    /// (read from the host) or --exec (host code), this runs a file baked into
    /// the initrd. This is how urunc drives the guest. With no workload given,
    /// the guest's conventional entrypoint (/entrypoint.py, /entrypoint, …) runs.
    #[arg(long = "guest-exec", value_name = "COMMAND", conflicts_with_all = ["script", "exec"])]
    guest_exec: Option<String>,

    /// Mount a host directory into the guest filesystem.
    /// Format: HOST:GUEST[:ro] (e.g. /tmp/share:/mnt or /data:/mnt/data:ro).
    #[arg(long = "mount", value_name = "HOST:GUEST[:ro]")]
    mounts: Vec<String>,

    /// Enable host networking with no policy (all destinations allowed).
    #[arg(long, conflicts_with_all = ["net_allow", "net_block"])]
    net: bool,

    /// Allow-list: only permit connections to these hosts/IPs.
    /// Implies --net. Mutually exclusive with --net-block.
    #[arg(long = "net-allow", value_name = "HOST", conflicts_with = "net_block")]
    net_allow: Vec<String>,

    /// Block-list: deny connections to these hosts/IPs, allow everything else.
    /// Implies --net. Mutually exclusive with --net-allow.
    #[arg(long = "net-block", value_name = "HOST", conflicts_with = "net_allow")]
    net_block: Vec<String>,

    /// Ports the guest may bind to for inbound connections.
    /// Without this flag, bind() is rejected (outbound-only).
    #[arg(long = "port", value_name = "PORT")]
    ports: Vec<u16>,

    /// Set an environment variable in the guest (repeatable).
    /// Format: KEY=VALUE (e.g. --env MY_VAR=hello --env DEBUG=1).
    #[arg(long = "env", value_name = "KEY=VALUE")]
    envs: Vec<String>,
}

/// Arguments for `snapshot save`.
#[derive(clap::Args)]
struct SaveArgs {
    /// Path to a CPIO initrd to map into the guest.
    #[arg(long)]
    initrd: Option<PathBuf>,

    /// Entry point binary path inside the initrd VFS.
    /// Auto-detected from the initrd if not specified.
    #[arg(long)]
    entry: Option<String>,

    /// Advanced: boot a kernel from this path instead of the embedded one.
    /// Must match the host ABI this build expects, or the guest will fault.
    /// Intended for kernel development.
    #[arg(long, value_name = "PATH")]
    kernel: Option<PathBuf>,

    /// Scratch memory in MiB (default 256; increase for large rootfs).
    #[arg(long, default_value_t = DEFAULT_SCRATCH_MB)]
    scratch_mb: usize,

    /// Directory to save the snapshot (OCI Image Layout).
    #[arg(short, long)]
    output: PathBuf,

    /// Mount a host directory into the guest filesystem.
    /// Format: HOST:GUEST[:ro] (e.g. /tmp/share:/mnt or /data:/mnt/data:ro).
    #[arg(long = "mount", value_name = "HOST:GUEST[:ro]")]
    mounts: Vec<String>,

    /// Enable host networking with no policy (all destinations allowed).
    #[arg(long, conflicts_with_all = ["net_allow", "net_block"])]
    net: bool,

    /// Allow-list: only permit connections to these hosts/IPs.
    #[arg(long = "net-allow", value_name = "HOST", conflicts_with = "net_block")]
    net_allow: Vec<String>,

    /// Block-list: deny connections to these hosts/IPs, allow everything else.
    #[arg(long = "net-block", value_name = "HOST", conflicts_with = "net_allow")]
    net_block: Vec<String>,

    /// Ports the guest may bind to for inbound connections.
    #[arg(long = "port", value_name = "PORT")]
    ports: Vec<u16>,
}

/// Arguments for `snapshot run`.
#[derive(clap::Args)]
struct SnapshotRunArgs {
    /// Path to a saved snapshot directory (OCI Image Layout).
    snapshot: PathBuf,

    /// Script file (.py, .js, …) to execute in the guest.
    #[arg(conflicts_with = "exec")]
    script: Option<PathBuf>,

    /// Inline code to execute (alternative to a script file).
    #[arg(long, conflicts_with = "script")]
    exec: Option<String>,

    /// Run a command that already lives in the guest filesystem (path plus
    /// optional args). With no workload given, the guest's conventional
    /// entrypoint runs. See `hluk run --help`.
    #[arg(long = "guest-exec", value_name = "COMMAND", conflicts_with_all = ["script", "exec"])]
    guest_exec: Option<String>,

    /// Mount a host directory into the guest filesystem.
    /// Format: HOST:GUEST[:ro] (e.g. /tmp/share:/mnt or /data:/mnt/data:ro).
    #[arg(long = "mount", value_name = "HOST:GUEST[:ro]")]
    mounts: Vec<String>,

    /// Enable host networking with no policy (all destinations allowed).
    #[arg(long, conflicts_with_all = ["net_allow", "net_block"])]
    net: bool,

    /// Allow-list: only permit connections to these hosts/IPs.
    #[arg(long = "net-allow", value_name = "HOST", conflicts_with = "net_block")]
    net_allow: Vec<String>,

    /// Block-list: deny connections to these hosts/IPs, allow everything else.
    #[arg(long = "net-block", value_name = "HOST", conflicts_with = "net_allow")]
    net_block: Vec<String>,

    /// Ports the guest may bind to for inbound connections.
    #[arg(long = "port", value_name = "PORT")]
    ports: Vec<u16>,

    /// Set an environment variable in the guest (repeatable).
    /// Format: KEY=VALUE (e.g. --env MY_VAR=hello --env DEBUG=1).
    #[arg(long = "env", value_name = "KEY=VALUE")]
    envs: Vec<String>,
}

#[derive(Subcommand)]
enum BenchCommand {
    /// Cold start: fresh boot (evolve) + dispatch, no snapshot.
    Cold(BenchColdArgs),

    /// Cold snapshot start: load snapshot from disk + restore + dispatch.
    ColdSnap(BenchSnapArgs),

    /// Warm with restore: load snapshot once, then loop (dispatch + restore).
    WarmRestore(BenchSnapArgs),

    /// Warm stateful: load snapshot once, then loop (dispatch only, no restore).
    WarmStateful(BenchSnapArgs),

    /// Parallel VMs: spawn N VMs concurrently from the same snapshot.
    Parallel(BenchParallelArgs),
}

/// Arguments for `bench cold`.
#[derive(clap::Args)]
struct BenchColdArgs {
    /// Path to a CPIO initrd.
    #[arg(long)]
    initrd: PathBuf,

    /// Script file to execute.
    script: PathBuf,

    /// Scratch memory in MiB.
    #[arg(long, default_value_t = DEFAULT_SCRATCH_MB)]
    scratch_mb: usize,

    /// Number of samples to run.
    #[arg(long, default_value_t = 20)]
    samples: usize,
}

/// Arguments for snapshot-based bench modes (cold-snap, warm-restore, warm-stateful).
#[derive(clap::Args)]
struct BenchSnapArgs {
    /// Path to a saved snapshot directory.
    snapshot: PathBuf,

    /// Script file to execute.
    script: PathBuf,

    /// Number of iterations / samples.
    #[arg(long, default_value_t = 20)]
    samples: usize,
}

/// Arguments for `bench parallel`.
#[derive(clap::Args)]
struct BenchParallelArgs {
    /// Path to a saved snapshot directory.
    snapshot: PathBuf,

    /// Script file to execute.
    script: PathBuf,

    /// Number of concurrent VMs.
    #[arg(long, default_value_t = 4)]
    vms: usize,

    /// Iterations per VM.
    #[arg(long, default_value_t = 10)]
    iterations: usize,
}

// ── Helpers ──────────────────────────────────────────────────────

/// Parse `--env KEY=VALUE` strings into `(key, value)` pairs.  The first
/// `=` is the split point, so values may contain `=`; an entry without
/// one is an error rather than a variable silently not set.
fn parse_envs(raw: &[String]) -> Result<Vec<(&str, &str)>, String> {
    raw.iter()
        .map(|e| {
            e.split_once('=')
                .ok_or_else(|| format!("invalid --env {e:?}: expected KEY=VALUE"))
        })
        .collect()
}

/// Parse `--mount HOST:GUEST[:ro]` strings.  An entry with no `:` is an
/// error rather than a mount silently left out.
fn parse_mounts(raw: &[String]) -> Result<Vec<Mount>, String> {
    raw.iter()
        .map(|m| {
            parse_mount(m).ok_or_else(|| format!("invalid --mount {m:?}: expected HOST:GUEST[:ro]"))
        })
        .collect()
}

/// One `HOST:GUEST[:ro]` entry, or `None` if it has no `:` to split on.
fn parse_mount(m: &str) -> Option<Mount> {
    // On Windows, "C:\foo:/mnt" would split wrong at the drive
    // letter colon.  Detect "X:\" prefix and split after it.
    let (host, rest) = if m.len() >= 3
        && m.as_bytes()[0].is_ascii_alphabetic()
        && m.as_bytes()[1] == b':'
        && (m.as_bytes()[2] == b'\\' || m.as_bytes()[2] == b'/')
    {
        // Drive-letter prefix — split at the NEXT colon.
        let after_drive = &m[2..];
        let colon = after_drive.find(':')?;
        (&m[..2 + colon], &after_drive[colon + 1..])
    } else {
        m.split_once(':')?
    };
    let (guest, readonly) = match rest.rsplit_once(':') {
        Some((g, "ro")) => (g, true),
        _ => (rest, false),
    };
    Some(Mount {
        host_path: PathBuf::from(host),
        guest_path: guest.to_string(),
        readonly,
        limits: Default::default(),
    })
}

/// Convert CLI net flags into `(Option<NetworkPolicy>, Option<ListenPorts>)`.
fn parse_net_policy(
    net: bool,
    net_allow: &[String],
    net_block: &[String],
    ports: &[u16],
) -> Result<(Option<NetworkPolicy>, Option<ListenPorts>), String> {
    let policy = if !net_allow.is_empty() {
        Some(NetworkPolicy::AllowList(
            AllowList::from_hosts(net_allow).map_err(|e| e.to_string())?,
        ))
    } else if !net_block.is_empty() {
        Some(NetworkPolicy::BlockList(
            BlockList::from_hosts(net_block).map_err(|e| e.to_string())?,
        ))
    } else if net {
        Some(NetworkPolicy::AllowAll)
    } else {
        None
    };
    // Listen ports are meaningless without networking: `hostnet` is only
    // registered when a policy is present, so `--port` on its own would be a
    // silent no-op (the guest gets no networking at all).  Reject it up front
    // rather than let the user believe the guest can bind.
    if !ports.is_empty() && policy.is_none() {
        return Err(
            "--port requires networking to be enabled; pass --net (or --net-allow/--net-block) too"
                .to_string(),
        );
    }
    let listen = if !ports.is_empty() {
        Some(ListenPorts::from_ports(ports.iter().copied()))
    } else {
        None
    };
    Ok((policy, listen))
}

/// Resolve script/exec args into an Exec value.
///
/// Text files are passed as scripts.  Compiled binaries are rejected
/// with a helpful error — use `--mount` + `--exec` for those.
fn resolve_exec(script: Option<PathBuf>, exec: Option<String>) -> CliResult<Option<Exec>> {
    match (script, exec) {
        (Some(path), _) => {
            // Verify the file is valid UTF-8 (i.e. a script, not a binary)
            if std::fs::read_to_string(&path).is_err() && path.exists() {
                let dir = path
                    .parent()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| ".".into());
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "binary".into());
                return Err(format!(
                    "{} is a compiled binary, not a script.\n\
                     To run compiled binaries, mount a directory containing the binary:\n  \
                     hluk run --initrd <rootfs.cpio> --mount {dir}:/mnt/bin --exec /mnt/bin/{name}",
                    path.display(),
                )
                .into());
            }
            Ok(Some(Exec::File(path)))
        }
        (_, Some(code)) => Ok(Some(Exec::Code(code))),
        _ => Ok(None),
    }
}

// ── Commands ─────────────────────────────────────────────────────

/// Start a [`SandboxBuilder`] from the CLI's `--kernel` / `--initrd`.  A run
/// needs a workload — an external kernel, a rootfs, or both — so neither is an
/// error rather than a guest with nothing to boot.
fn base_builder(kernel: Option<PathBuf>, initrd: Option<PathBuf>) -> CliResult<SandboxBuilder> {
    match (kernel, initrd) {
        (Some(kernel), Some(initrd)) => Ok(SandboxBuilder::from_kernel(kernel).initrd(initrd)),
        (Some(kernel), None) => Ok(SandboxBuilder::from_kernel(kernel)),
        (None, Some(initrd)) => Ok(SandboxBuilder::from_initrd(initrd)),
        (None, None) => Err("no workload: pass --initrd <rootfs.cpio> or --kernel <kernel>".into()),
    }
}

fn cmd_run(args: RunArgs) -> CliResult<()> {
    let mounts = parse_mounts(&args.mounts)?;
    let (policy, listen) =
        parse_net_policy(args.net, &args.net_allow, &args.net_block, &args.ports)?;

    // Precedence: a host script or --exec code; else a guest command
    // (--guest-exec); else, with no workload at all, the rootfs's conventional
    // entrypoint. The last two are the model a container runtime (urunc) uses.
    let no_workload = args.script.is_none() && args.exec.is_none() && args.guest_exec.is_none();
    let exec = resolve_exec(args.script, args.exec)?
        .unwrap_or_else(|| Exec::Guest(args.guest_exec.unwrap_or_default()));
    let envs = parse_envs(&args.envs)?;

    let mut builder = base_builder(args.kernel, args.initrd)?
        .scratch_mb(args.scratch_mb)
        .mounts(mounts);
    if let Some(entry) = args.entry {
        builder = builder.entry(entry);
    }

    if let Some(policy) = policy {
        builder = builder.network(policy);
    }
    if let Some(listen) = listen {
        builder = builder.listen_ports(listen);
    }
    for (key, value) in envs {
        builder = builder.env(key, value);
    }
    let t = Instant::now();
    let mut sandbox = builder.boot()?;
    info!(elapsed_ms = t.elapsed().as_secs_f64() * 1000.0, "boot");

    drive(&mut sandbox, no_workload, exec)
}

fn cmd_workerd(args: WorkerdArgs) -> CliResult<()> {
    use hyperlight_unikraft::workerd::{PROTOCOL_VERSION, RequestEnvelope, WorkerVersionSandbox};

    let bundle = build_workerd_bundle(
        args.bundle,
        args.script,
        args.version,
        args.compatibility_date,
    )?;
    let version = bundle.worker_version.clone();
    let bundle_sha256 = bundle.sha256()?;
    let worker = WorkerVersionSandbox::initialize(
        bundle,
        args.rootfs,
        args.executor,
        args.scratch_mb,
        Duration::from_millis(args.init_timeout_ms),
    )?;
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: "cli-1".into(),
        method: "GET".into(),
        url: args.url,
        headers: Vec::new(),
        body_base64: String::new(),
    };
    let (response, profile) = worker.execute_profiled(
        &version,
        request,
        Duration::from_millis(args.request_timeout_ms),
    );
    eprintln!(
        "worker={} bundle={} profile={}",
        version.as_str(),
        bundle_sha256,
        serde_json::to_string(&profile)?
    );
    let response = response?;
    std::io::Write::write_all(
        &mut std::io::stdout(),
        &STANDARD.decode(response.body_base64)?,
    )?;
    Ok(())
}

fn cmd_workerd_prewarm_snapshot(args: WorkerdPrewarmSnapshotArgs) -> CliResult<()> {
    use hyperlight_unikraft::workerd::WorkerVersionSandbox;

    let bundle = build_workerd_bundle(
        args.bundle,
        args.script,
        args.version,
        args.compatibility_date,
    )?;
    let version = bundle.worker_version.clone();
    let bundle_sha256 = bundle.sha256()?;
    // Same deny-all policy `AppConfig`'s default `capability_policy` always
    // builds (`WorkerCapabilityPolicyConfig::build()`), so the saved
    // snapshot's `capability_policy_sha256` binds to exactly what
    // `AppRegistry::from_host_config` will check against when an
    // `AppConfig.snapshot_dir` restores from it.
    let (worker, profile) = WorkerVersionSandbox::initialize_profiled(
        bundle,
        args.rootfs,
        args.executor,
        args.scratch_mb,
        Duration::from_millis(args.init_timeout_ms),
    )
    .map_err(|failure| failure.to_string())?;
    worker.snapshot().save(&args.snapshot_dir)?;
    eprintln!(
        "worker={} bundle={} snapshot_dir={} profile={}",
        version.as_str(),
        bundle_sha256,
        args.snapshot_dir.display(),
        serde_json::to_string(&profile)?
    );
    Ok(())
}

/// Shared state for `workerd-host`'s background app-initialization thread
/// and its accept loop: apps are loaded/initialized off the accept thread
/// so `/__hyperlight/healthz` can serve immediately while VMs are still
/// warming up, and `/__hyperlight/readyz` only flips once every app is up.
enum WorkerdHostState {
    Initializing,
    Ready(hyperlight_unikraft::workerd::AppRegistry),
    Failed(String),
}

/// `workerd-host` — Boundary 4 of the resident Workerd host: an
/// orchestrator-ready, long-running multi-app server.
///
/// Exit codes are deterministic and checked by orchestrators, not just
/// logged: 0 clean shutdown, 1 drain timeout or panic, 2 config
/// load/validation failure, 3 app initialization failure, 4 listener bind
/// failure. Config/bind failures happen before any port is bound or any
/// app is started; an app-initialization failure can only be observed
/// after the listener is already serving `/__hyperlight/healthz`, since
/// every app's bundle/VM is loaded on a background thread so one slow app
/// cannot delay the others or the health endpoint.
///
/// The HTTP read/write primitives below come from `src/workerd/http.rs`
/// (boundary 5), the same functions `examples/workerd-demo.rs` uses, so the
/// parsing/writing logic is no longer duplicated between the two call
/// sites.
/// How many WHP surrogate processes (Windows only) `workerd-host` needs for
/// `args.config`'s apps: the sum of each resident app's pool `capacity` and
/// each disposable app's `max_concurrent_sandboxes`, since every app's pool
/// can have that many sandboxes alive at once and every app runs
/// concurrently in one process (unlike every other `hluk` subcommand, which
/// uses exactly one sandbox at a time). Called before `cmd_workerd_host`
/// loads the config itself, so on any read/parse failure this falls back to
/// `0` (today's single-guest default) and lets `cmd_workerd_host`'s own
/// load report the real error and exit code.
#[cfg(windows)]
fn workerd_host_surrogate_capacity(args: &WorkerdHostArgs) -> usize {
    use hyperlight_unikraft::workerd::{AppPoolConfig, HostConfig};

    let Ok(config) = HostConfig::from_path(&args.config) else {
        return 0;
    };
    config
        .apps
        .iter()
        .map(|app| match &app.pool {
            AppPoolConfig::Disposable(pool) => pool.max_concurrent_sandboxes,
            AppPoolConfig::Resident(pool) => pool.capacity,
        })
        .sum()
}

fn cmd_workerd_host(args: WorkerdHostArgs) -> CliResult<()> {
    use hyperlight_unikraft::workerd::{
        AppHandle, AppRegistry, ConnectionAffinity, ConnectionMode, HostConfig, MAX_HEADER_BYTES,
        RequestExecution, ResidentHandle, read_http_request_with_control, wants_keep_alive,
        write_http_error, write_http_response,
    };
    use std::io::Write as _;
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Mutex, RwLock};

    const MAX_REQUEST_HEAD_BYTES: usize = MAX_HEADER_BYTES + 8 * 1024;
    const CONNECTION_IO_TIMEOUT: Duration = Duration::from_secs(5);
    const POLL_INTERVAL: Duration = Duration::from_millis(20);

    #[derive(Clone, Copy, PartialEq)]
    enum ListenerRole {
        Shared,
        Application,
        Admin,
    }

    struct HostConnection<'a> {
        state: &'a Arc<RwLock<WorkerdHostState>>,
        in_flight: &'a Arc<AtomicUsize>,
        connection_sequence: u64,
        role: ListenerRole,
        shutting_down: &'a AtomicBool,
        admission: &'a Mutex<()>,
        data_port: u16,
    }

    struct ConnectionGuard(Arc<AtomicUsize>);

    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Keeps a connection's `in_flight` accounting raised for the full
    /// request lifecycle — pool execution *and* writing the response back
    /// to the client — by decrementing on `Drop` instead of right after
    /// `execute_request` returns. Bracketing only the pool-execution call
    /// (the prior behavior) let the drain loop observe `in_flight == 0`
    /// and exit the process while connection threads were still blocked
    /// writing already-computed responses: under a burst of simultaneously
    /// admitted requests, every pool completion (and thus every
    /// `fetch_sub`) could land within the same `POLL_INTERVAL` tick, so
    /// `drop(state)` ran — tearing down the listener and every in-process
    /// resource — before those threads finished `stream.write_all`,
    /// producing a client-visible `EOF` instead of the `200` the request
    /// had already legitimately earned. Holding the counter up until the
    /// write completes (success or failure) closes that race.
    struct InFlightGuard<'a>(&'a Arc<AtomicUsize>);

    impl<'a> InFlightGuard<'a> {
        fn new(in_flight: &'a Arc<AtomicUsize>) -> Self {
            in_flight.fetch_add(1, Ordering::AcqRel);
            Self(in_flight)
        }
    }

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Runs one request against `handle`'s pool and blocks until the result
    /// is known. When `reserved` holds a connection-affine
    /// [`ResidentHandle`] (boundary 5 sticky affinity) the request bypasses
    /// the shared pool entirely and runs on that handle's dedicated
    /// resident VM instead. Callers are responsible for keeping `in_flight`
    /// raised (via [`InFlightGuard`]) for as long as the eventual response
    /// write takes too — this function itself no longer touches it.
    fn execute_request(
        handle: &AppHandle,
        reserved: Option<&ResidentHandle>,
        envelope: hyperlight_unikraft::workerd::RequestEnvelope,
        timeout: Duration,
        stream: &TcpStream,
    ) -> RequestExecution {
        let request_id = envelope.request_id.clone();
        let cancellation = hyperlight_unikraft::workerd::InvocationCancellation::default();
        let (tx, rx) = mpsc::channel();
        let completion = move |execution| {
            if tx.send(execution).is_err() {
                tracing::debug!("HTTP invocation receiver dropped");
            }
        };
        let request = envelope.into();
        if let Some(resident) = reserved {
            let _ =
                resident.try_submit_cancellable(request, timeout, cancellation.clone(), completion);
        } else {
            let _ =
                handle.try_submit_cancellable(request, timeout, cancellation.clone(), completion);
        }
        match wait_for_invocation(rx, stream, cancellation) {
            Ok(execution) => RequestExecution {
                request_id: execution.request_id,
                result: execution.result.and_then(|response| match response {
                    hyperlight_unikraft::workerd::InvocationResponse::Fetch(response) => {
                        Ok(response)
                    }
                    _ => Err(hyperlight_unikraft::workerd::Error::State(
                        "non-fetch HTTP response".into(),
                    )),
                }),
                profile: execution.profile,
                submit_error: execution.submit_error,
            },
            Err(error) => RequestExecution {
                request_id,
                result: Err(error),
                profile: Default::default(),
                submit_error: Some(hyperlight_unikraft::workerd::PoolSubmitError::ShuttingDown),
            },
        }
    }

    fn wait_for_invocation(
        rx: mpsc::Receiver<hyperlight_unikraft::workerd::InvocationExecution>,
        stream: &TcpStream,
        cancellation: hyperlight_unikraft::workerd::InvocationCancellation,
    ) -> hyperlight_unikraft::workerd::Result<hyperlight_unikraft::workerd::InvocationExecution>
    {
        if let Err(error) = stream.set_read_timeout(Some(Duration::from_millis(1))) {
            cancellation.cancel();
            return Err(error.into());
        }
        let result = loop {
            match rx.recv_timeout(POLL_INTERVAL) {
                Ok(execution) => break Ok(execution),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    cancellation.cancel();
                    break Err(hyperlight_unikraft::workerd::Error::State(
                        "invocation owner unavailable".into(),
                    ));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let mut byte = [0];
                    match stream.peek(&mut byte) {
                        Ok(0) => cancellation.cancel(),
                        Ok(_) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                            ) => {}
                        Err(error) => {
                            tracing::debug!(%error, "HTTP disconnect cancels invocation");
                            cancellation.cancel();
                        }
                    }
                }
            }
        };
        stream.set_read_timeout(Some(CONNECTION_IO_TIMEOUT))?;
        result
    }
    /// Writes `execution`'s outcome as an HTTP response with `connection`'s
    /// `Connection` header. Mirrors `examples/workerd-demo.rs`'s
    /// `finish_request` passthrough (header filtering, Content-Length) —
    /// deliberately not extracted into `http.rs` alongside the other four
    /// functions, since the plan names only those four and the two call
    /// sites map submit errors to different messages.
    fn write_execution_response(
        stream: &mut TcpStream,
        execution: RequestExecution,
        connection: ConnectionMode,
    ) -> std::io::Result<()> {
        if execution.submit_error.is_some() {
            return write_http_error(stream, 503, "worker request pool unavailable", connection);
        }
        match execution.result {
            Ok(response) => {
                let body = STANDARD
                    .decode(response.body_base64)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
                write!(
                    stream,
                    "HTTP/1.1 {} {}\r\n",
                    response.status,
                    hyperlight_unikraft::workerd::http_reason(response.status)
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
                    "Content-Length: {}\r\nConnection: {}\r\n\r\n",
                    body.len(),
                    match connection {
                        ConnectionMode::Close => "close",
                        ConnectionMode::KeepAlive => "keep-alive",
                    }
                )?;
                stream.write_all(&body)
            }
            Err(hyperlight_unikraft::workerd::Error::Timeout) => {
                write_http_error(stream, 504, "Worker timed out", connection)
            }
            Err(hyperlight_unikraft::workerd::Error::Fence(_)) => {
                write_http_error(stream, 409, "stale instance generation", connection)
            }
            Err(hyperlight_unikraft::workerd::Error::NotReady(_)) => {
                write_http_error(stream, 503, "instance is not ready", connection)
            }
            Err(_) => write_http_error(stream, 502, "Worker execution failed", connection),
        }
    }

    /// Serves every request on one accepted connection, including a
    /// keep-alive loop (boundary 5): while the client keeps asking for
    /// `Connection: keep-alive` on an app-routed request, the same thread
    /// keeps reading further requests off the same `stream` instead of
    /// returning after one. The `/__hyperlight/*` contract endpoints always
    /// close after responding regardless of what the client asked for —
    /// they are infrequent control-plane calls, not the affinity feature's
    /// target, so keeping them simple avoids complicating the already-
    /// validated Boundary 4 contract tests.
    ///
    /// `reserved` holds a connection-affine [`ResidentHandle`] once a
    /// `Sticky` resident app has been routed to on this connection, tagged
    /// with the app id it belongs to so a later request that routes to a
    /// *different* app (unusual, but HTTP permits a different `Host` header
    /// per request on a keep-alive connection) drops the stale reservation
    /// instead of misusing it.
    fn handle_host_connection(mut stream: TcpStream, context: HostConnection<'_>) {
        let HostConnection {
            state,
            in_flight,
            connection_sequence,
            role,
            shutting_down,
            admission,
            data_port,
        } = context;
        if let Err(error) = stream
            .set_read_timeout(Some(CONNECTION_IO_TIMEOUT))
            .and_then(|()| stream.set_write_timeout(Some(CONNECTION_IO_TIMEOUT)))
        {
            eprintln!("workerd-host: connection timeout setup failed: {error}");
            return;
        }
        let mut reserved: Option<(String, ResidentHandle)> = None;
        let mut request_index: u64 = 0;
        loop {
            request_index += 1;
            let request_id = format!("host-{connection_sequence}-{request_index}");
            let parsed = match read_http_request_with_control(
                &mut stream,
                request_id,
                MAX_REQUEST_HEAD_BYTES,
                role != ListenerRole::Application,
            ) {
                Ok(parsed) => parsed,
                Err(error) => {
                    // Past the first request, a read failure almost always
                    // just means the client closed the keep-alive
                    // connection; only the first request's parse failure is
                    // a client-visible 400.
                    if request_index == 1 {
                        let _ = write_http_error(&mut stream, 400, &error, ConnectionMode::Close);
                    }
                    return;
                }
            };
            let keep_alive = wants_keep_alive(&parsed.envelope);
            let connection_mode = if keep_alive {
                ConnectionMode::KeepAlive
            } else {
                ConnectionMode::Close
            };

            let fenced_http =
                parsed.path.starts_with("/v1/instances/") && parsed.path.ends_with("/http");
            if role == ListenerRole::Application
                && (parsed.path.starts_with("/__hyperlight/")
                    || parsed.path == "/v1/capabilities"
                    || parsed.path.starts_with("/v1/apps/")
                    || ((parsed.path == "/v1/instances"
                        || parsed.path.starts_with("/v1/instances/"))
                        && !fenced_http))
            {
                let _ = write_http_error(
                    &mut stream,
                    404,
                    "management endpoint requires the admin listener",
                    ConnectionMode::Close,
                );
                return;
            }
            match parsed.path.as_str() {
                "/v1/capabilities" if role != ListenerRole::Application => {
                    let guard = state.read().expect("workerd-host state lock poisoned");
                    let (status, body) = match &*guard {
                        WorkerdHostState::Ready(registry)
                            if !shutting_down.load(Ordering::SeqCst) =>
                        {
                            (200, registry.capabilities_json())
                        }
                        _ => (
                            503,
                            serde_json::json!({"protocol_version":1,"error":"runtime not ready for admission","capabilities":[]}),
                        ),
                    };
                    let body = body.to_string();
                    let _ = write_http_response(
                        &mut stream,
                        status,
                        "application/json",
                        body.as_bytes(),
                        ConnectionMode::Close,
                    );
                    return;
                }
                "/__hyperlight/healthz" => {
                    let _ = write_http_response(
                        &mut stream,
                        200,
                        "application/json",
                        b"{\"status\":\"ok\"}",
                        ConnectionMode::Close,
                    );
                    return;
                }
                "/__hyperlight/readyz" => {
                    let guard = state.read().expect("workerd-host state lock poisoned");
                    let (status, body) = if shutting_down.load(Ordering::SeqCst) {
                        (503, serde_json::json!({"status":"draining"}))
                    } else {
                        match &*guard {
                            WorkerdHostState::Ready(registry) => (
                                200,
                                serde_json::json!({
                                    "status": "ready", "apps": registry.status_json(),
                                }),
                            ),
                            WorkerdHostState::Initializing => {
                                (503, serde_json::json!({"status":"initializing"}))
                            }
                            WorkerdHostState::Failed(message) => {
                                (503, serde_json::json!({"status":"failed","error":message}))
                            }
                        }
                    };
                    let body = body.to_string();
                    let _ = write_http_response(
                        &mut stream,
                        status,
                        "application/json",
                        body.as_bytes(),
                        ConnectionMode::Close,
                    );
                    return;
                }
                "/__hyperlight/status" => {
                    if shutting_down.load(Ordering::SeqCst) {
                        let _ = write_http_response(
                            &mut stream,
                            503,
                            "application/json",
                            b"{\"status\":\"draining\"}",
                            ConnectionMode::Close,
                        );
                        return;
                    }
                    let guard = state.read().expect("workerd-host state lock poisoned");
                    let (status, body) = match &*guard {
                        WorkerdHostState::Ready(registry) => {
                            (200, serde_json::json!(registry.status_json()))
                        }
                        WorkerdHostState::Initializing => {
                            (503, serde_json::json!({"status": "initializing"}))
                        }
                        WorkerdHostState::Failed(message) => (
                            503,
                            serde_json::json!({"status": "failed", "error": message}),
                        ),
                    };
                    drop(guard);
                    let body = body.to_string();
                    let _ = write_http_response(
                        &mut stream,
                        status,
                        "application/json",
                        body.as_bytes(),
                        ConnectionMode::Close,
                    );
                    return;
                }
                _ => {}
            }

            if fenced_http {
                if role == ListenerRole::Admin {
                    let _ = write_http_error(
                        &mut stream,
                        404,
                        "instance HTTP transport requires the data listener",
                        ConnectionMode::Close,
                    );
                    return;
                }
                let mut transport_admitted = false;
                let result = (|| -> hyperlight_unikraft::workerd::Result<()> {
                    use hyperlight_unikraft::workerd::{
                        Error, HostIngress, InvocationCancellation, decode_buffered_input,
                        pump_http_stream, websocket_requested,
                    };
                    let instance_id = parsed
                        .path
                        .strip_prefix("/v1/instances/")
                        .and_then(|path| path.strip_suffix("/http"))
                        .ok_or_else(|| Error::Protocol("invalid fenced HTTP route".into()))?;
                    if instance_id.is_empty() || instance_id.contains('/') {
                        return Err(Error::Protocol("invalid fenced instance ID".into()));
                    }
                    let header = |name: &str| -> hyperlight_unikraft::workerd::Result<String> {
                        let values = parsed
                            .envelope
                            .headers
                            .iter()
                            .filter(|header| header.name.eq_ignore_ascii_case(name))
                            .map(|header| header.value.clone())
                            .collect::<Vec<_>>();
                        if values.len() != 1 {
                            return Err(Error::Protocol(format!(
                                "exactly one trusted {name} header required"
                            )));
                        }
                        Ok(values[0].clone())
                    };
                    let app_id = header("x-hyperloom-app")?;
                    let revision = header("x-hyperloom-revision")?;
                    let generation = header("x-hyperloom-generation")?
                        .parse::<u64>()
                        .map_err(|_| Error::Protocol("invalid instance generation".into()))?;
                    if generation == 0 {
                        return Err(Error::Protocol(
                            "instance generation must be positive".into(),
                        ));
                    }
                    let target = header("x-hyperloom-request-url")?;
                    let admission_guard = admission.lock().expect("admission lock poisoned");
                    if shutting_down.load(Ordering::SeqCst) {
                        return Err(Error::State("host is draining".into()));
                    }
                    let _in_flight_guard = InFlightGuard::new(in_flight);
                    drop(admission_guard);
                    let guard = state.read().expect("workerd-host state lock poisoned");
                    let WorkerdHostState::Ready(registry) = &*guard else {
                        return Err(Error::State("host is not ready".into()));
                    };
                    let app = registry
                        .app(&app_id)
                        .ok_or_else(|| Error::State("unknown application".into()))?;
                    if app.identity().worker_version.as_str() != revision {
                        return Err(Error::Protocol("fenced HTTP revision mismatch".into()));
                    }
                    let AppHandle::Instances { home, .. } = app else {
                        return Err(Error::State("application has no fenced home".into()));
                    };
                    let timeout = app.execute_timeout();
                    let mut request = parsed.envelope.clone();
                    request.url = target;
                    request.headers.retain(|header| {
                        !header.name.to_ascii_lowercase().starts_with("x-hyperloom-")
                    });
                    let (tx, rx) = mpsc::channel();
                    if app.identity().streaming {
                        let body = decode_buffered_input(&request)?;
                        request.body_base64.clear();
                        request.validate()?;
                        let websocket = websocket_requested(&request);
                        let (host, guest) = HostIngress::pair(
                            &request.request_id,
                            websocket,
                            timeout,
                            InvocationCancellation::default(),
                        )?;
                        home.try_submit_stream(
                            &hyperlight_unikraft::workerd::InstanceIdentity {
                                instance_id: instance_id.into(),
                                generation,
                            },
                            request.clone(),
                            websocket,
                            guest,
                            timeout,
                            move |execution| {
                                if tx.send(execution).is_err() {
                                    tracing::debug!(
                                        "fenced stream completion receiver disconnected"
                                    );
                                }
                            },
                        )?;
                        transport_admitted = true;
                        pump_http_stream(stream.try_clone()?, &request, &body, host, rx)?;
                    } else {
                        request.validate()?;
                        let cancellation = InvocationCancellation::default();
                        home.try_submit(
                            instance_id,
                            generation,
                            request.into(),
                            timeout,
                            cancellation.clone(),
                            move |execution| {
                                if tx.send(execution).is_err() {
                                    tracing::debug!("fenced HTTP completion receiver disconnected");
                                }
                            },
                        )?;
                        let execution = wait_for_invocation(rx, &stream, cancellation)?;
                        let execution = RequestExecution {
                            request_id: execution.request_id,
                            result: execution.result.and_then(|response| match response {
                                hyperlight_unikraft::workerd::InvocationResponse::Fetch(
                                    response,
                                ) => Ok(response),
                                _ => Err(Error::Protocol(
                                    "non-fetch result on HTTP instance route".into(),
                                )),
                            }),
                            profile: execution.profile,
                            submit_error: execution.submit_error,
                        };
                        write_execution_response(&mut stream, execution, ConnectionMode::Close)?;
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    eprintln!("workerd-host: fenced HTTP transport rejected:{error}");
                    let status = match error {
                        hyperlight_unikraft::workerd::Error::Protocol(_) => 400,
                        hyperlight_unikraft::workerd::Error::Fence(_) => 409,
                        _ => 503,
                    };
                    if !transport_admitted {
                        let _ = write_http_error(
                            &mut stream,
                            status,
                            &error.to_string(),
                            ConnectionMode::Close,
                        );
                    }
                }
                return;
            }
            if role != ListenerRole::Application
                && parsed.envelope.method == "GET"
                && (parsed.path == "/v1/instances"
                    || parsed
                        .path
                        .strip_prefix("/v1/instances/")
                        .is_some_and(|id| !id.is_empty() && !id.contains('/')))
            {
                let result = (|| -> hyperlight_unikraft::workerd::Result<serde_json::Value> {
                    use hyperlight_unikraft::workerd::Error;
                    let url = reqwest::Url::parse(&parsed.envelope.url)
                        .map_err(|error| Error::Protocol(error.to_string()))?;
                    let query = url.query_pairs().collect::<Vec<_>>();
                    if query.len() != 2 {
                        return Err(Error::Protocol(
                            "instance status requires exact app_id and revision query".into(),
                        ));
                    }
                    let app = query
                        .iter()
                        .filter(|(name, _)| name == "app_id")
                        .map(|(_, value)| value.as_ref())
                        .collect::<Vec<_>>();
                    let revision = query
                        .iter()
                        .filter(|(name, _)| name == "revision")
                        .map(|(_, value)| value.as_ref())
                        .collect::<Vec<_>>();
                    if app.len() != 1 || revision.len() != 1 {
                        return Err(Error::Protocol(
                            "duplicate or missing status authority".into(),
                        ));
                    }
                    let guard = state.read().expect("workerd-host state lock poisoned");
                    let WorkerdHostState::Ready(registry) = &*guard else {
                        return Err(Error::State("host is not ready".into()));
                    };
                    let app = registry
                        .app(app[0])
                        .ok_or_else(|| Error::State("unknown application".into()))?;
                    if app.identity().worker_version.as_str() != revision[0] {
                        return Err(Error::Protocol("status revision authority mismatch".into()));
                    }
                    let AppHandle::Instances { home, .. } = app else {
                        return Err(Error::State("application has no fenced home".into()));
                    };
                    let statuses = home.status()?;
                    if let Some(id) = parsed.path.strip_prefix("/v1/instances/") {
                        let status = statuses
                            .into_iter()
                            .find(|status| status.identity.instance_id == id)
                            .ok_or_else(|| Error::State("unknown instance".into()))?;
                        Ok(
                            serde_json::json!({"protocol_version":1,"app_id":app.app_id(),"revision":app.identity().worker_version,"instance":status}),
                        )
                    } else {
                        Ok(
                            serde_json::json!({"protocol_version":1,"app_id":app.app_id(),"revision":app.identity().worker_version,"instances":statuses}),
                        )
                    }
                })();
                let (status, body) = match result {
                    Ok(body) => (200, body.to_string()),
                    Err(error) => {
                        eprintln!("workerd-host: instance status rejected:{error}");
                        (
                            if matches!(error, hyperlight_unikraft::workerd::Error::Protocol(_)) {
                                400
                            } else {
                                503
                            },
                            serde_json::json!({"protocol_version":1,"error":error.to_string()})
                                .to_string(),
                        )
                    }
                };
                let _ = write_http_response(
                    &mut stream,
                    status,
                    "application/json",
                    body.as_bytes(),
                    ConnectionMode::Close,
                );
                return;
            }
            if role != ListenerRole::Application
                && let Some(route) = parsed.path.strip_prefix("/v1/instances/")
            {
                let handle = (|| -> hyperlight_unikraft::workerd::Result<(u16, String)> {
                    use hyperlight_unikraft::workerd::{
                        InstanceIdentity, InstanceInvocation, LifecycleOperation, LifecycleRequest,
                    };
                    if parsed.envelope.method != "POST" {
                        return Ok((
                            405,
                            serde_json::json!({"error":"instance operation requires POST"})
                                .to_string(),
                        ));
                    }
                    let Some((instance_id, operation)) = route.split_once('/') else {
                        return Err(hyperlight_unikraft::workerd::Error::Protocol(
                            "invalid instance route".into(),
                        ));
                    };
                    if instance_id.is_empty() || operation.contains('/') {
                        return Err(hyperlight_unikraft::workerd::Error::Protocol(
                            "invalid instance route".into(),
                        ));
                    }
                    let body = STANDARD
                        .decode(&parsed.envelope.body_base64)
                        .map_err(|error| {
                            hyperlight_unikraft::workerd::Error::Protocol(error.to_string())
                        })?;
                    let admission_guard = admission.lock().expect("admission lock poisoned");
                    if shutting_down.load(Ordering::SeqCst) {
                        return Ok((
                            503,
                            serde_json::json!({"error":"host is draining"}).to_string(),
                        ));
                    }
                    let _in_flight_guard = InFlightGuard::new(in_flight);
                    drop(admission_guard);
                    let guard = state.read().expect("workerd-host state lock poisoned");
                    let WorkerdHostState::Ready(registry) = &*guard else {
                        return Ok((
                            503,
                            serde_json::json!({"error":"host is not ready"}).to_string(),
                        ));
                    };
                    if operation == "invoke" {
                        let invocation: InstanceInvocation = serde_json::from_slice(&body)?;
                        let app = registry.app(&invocation.app_id).ok_or_else(|| {
                            hyperlight_unikraft::workerd::Error::State("unknown application".into())
                        })?;
                        invocation.validate(app, instance_id)?;
                        let AppHandle::Instances { home, .. } = app else {
                            return Ok((503, serde_json::json!({"error":"application has no fenced instance home"}).to_string()));
                        };
                        let timeout = Duration::from_millis(invocation.lifetime_budget_ms as u64)
                            .min(app.execute_timeout());
                        let cancellation =
                            hyperlight_unikraft::workerd::InvocationCancellation::default();
                        let (tx, rx) = mpsc::channel();
                        home.try_submit(
                            instance_id,
                            invocation.expected_generation,
                            invocation.invocation,
                            timeout,
                            cancellation.clone(),
                            move |execution| {
                                if tx.send(execution).is_err() {
                                    tracing::debug!("fenced invocation receiver disconnected");
                                }
                            },
                        )?;
                        let execution = wait_for_invocation(rx, &stream, cancellation)?;
                        let response = execution.result?;
                        let mut response = serde_json::to_value(response)?;
                        response["protocol_version"] = serde_json::json!(1);
                        return Ok((200, response.to_string()));
                    }
                    let request: LifecycleRequest = serde_json::from_slice(&body)?;
                    let app = registry.app(&request.app_id).ok_or_else(|| {
                        hyperlight_unikraft::workerd::Error::State("unknown application".into())
                    })?;
                    request.validate(app)?;
                    let AppHandle::Instances { home, .. } = app else {
                        return Ok((
                            503,
                            serde_json::json!({"error":"application has no fenced instance home"})
                                .to_string(),
                        ));
                    };
                    if home.policy() != request.checkpoint_policy {
                        return Err(hyperlight_unikraft::workerd::Error::Protocol(
                            "checkpoint policy differs from admitted app".into(),
                        ));
                    }
                    let status = match operation {
                        "create" => home.create(instance_id, request.expected_generation)?,
                        "park" => home.lifecycle(
                            instance_id,
                            request.expected_generation,
                            LifecycleOperation::Park,
                        )?,
                        "resume" => {
                            if home.contains(instance_id)? {
                                home.lifecycle(
                                    instance_id,
                                    request.expected_generation,
                                    LifecycleOperation::Resume,
                                )?
                            } else {
                                home.recover(InstanceIdentity {
                                    instance_id: instance_id.into(),
                                    generation: request.expected_generation,
                                })?
                            }
                        }
                        "release" => home.lifecycle(
                            instance_id,
                            request.expected_generation,
                            LifecycleOperation::Release,
                        )?,
                        _ => {
                            return Err(hyperlight_unikraft::workerd::Error::Protocol(
                                "unsupported lifecycle operation".into(),
                            ));
                        }
                    };
                    let mut body = serde_json::json!({
                        "protocol_version":1,"request_id":request.request_id,
                        "instance_id":status.identity.instance_id,"generation":status.identity.generation,"state":status.state,
                    });
                    if let Some(checkpoint_id) = status.checkpoint_id {
                        body["checkpoint_id"] = serde_json::json!(checkpoint_id);
                    }
                    if status.state == hyperlight_unikraft::workerd::InstanceState::Active {
                        body["endpoint"] = serde_json::json!(format!(
                            "http://{}/v1/instances/{instance_id}/invoke",
                            stream.local_addr()?,
                        ));
                        body["http_endpoint"] = serde_json::json!(format!(
                            "http://{}/v1/instances/{instance_id}/http",
                            std::net::SocketAddr::new(stream.local_addr()?.ip(), data_port),
                        ));
                    }
                    Ok((200, body.to_string()))
                })();
                let (status, body) = match handle {
                    Ok(response) => response,
                    Err(error) => {
                        eprintln!("workerd-host: instance operation rejected: {error}");
                        let status = match error {
                            hyperlight_unikraft::workerd::Error::Timeout => 504,
                            hyperlight_unikraft::workerd::Error::Protocol(_)
                            | hyperlight_unikraft::workerd::Error::Json(_) => 400,
                            hyperlight_unikraft::workerd::Error::State(_) => 409,
                            hyperlight_unikraft::workerd::Error::Fence(_) => 409,
                            _ => 503,
                        };
                        (
                            status,
                            serde_json::json!({"protocol_version":1,"error":error.to_string()})
                                .to_string(),
                        )
                    }
                };
                if let Err(error) = write_http_response(
                    &mut stream,
                    status,
                    "application/json",
                    body.as_bytes(),
                    ConnectionMode::Close,
                ) {
                    eprintln!("workerd-host: instance response write failed: {error}");
                }
                return;
            }
            if role != ListenerRole::Application
                && let Some(app_id) = parsed
                    .path
                    .strip_prefix("/v1/apps/")
                    .and_then(|path| path.strip_suffix("/invoke"))
            {
                if parsed.envelope.method != "POST" {
                    let _ = write_http_error(
                        &mut stream,
                        405,
                        "invocation requires POST",
                        ConnectionMode::Close,
                    );
                    return;
                }
                let invocation = STANDARD
                    .decode(&parsed.envelope.body_base64)
                    .map_err(|error| error.to_string())
                    .and_then(|bytes| {
                        hyperlight_unikraft::workerd::InvocationRequest::from_control_json(&bytes)
                            .map_err(|error| error.to_string())
                    });
                let invocation = match invocation {
                    Ok(invocation) => invocation,
                    Err(error) => {
                        let _ = write_http_error(&mut stream, 400, &error, ConnectionMode::Close);
                        return;
                    }
                };
                let admission_guard = admission.lock().expect("admission lock poisoned");
                if shutting_down.load(Ordering::SeqCst) {
                    drop(admission_guard);
                    let _ = write_http_error(
                        &mut stream,
                        503,
                        "host is draining",
                        ConnectionMode::Close,
                    );
                    return;
                }
                let _in_flight_guard = InFlightGuard::new(in_flight);
                drop(admission_guard);
                let guard = state.read().expect("workerd-host state lock poisoned");
                let WorkerdHostState::Ready(registry) = &*guard else {
                    let _ = write_http_error(
                        &mut stream,
                        503,
                        "host is not ready",
                        ConnectionMode::Close,
                    );
                    return;
                };
                let Some(app) = registry.app(app_id) else {
                    let _ = write_http_error(
                        &mut stream,
                        404,
                        "unknown application",
                        ConnectionMode::Close,
                    );
                    return;
                };
                let (tx, rx) = mpsc::channel();
                let cancellation = hyperlight_unikraft::workerd::InvocationCancellation::default();
                let _ = app.try_submit_cancellable(
                    invocation,
                    app.execute_timeout(),
                    cancellation.clone(),
                    move |execution| {
                        if tx.send(execution).is_err() {
                            tracing::debug!("invocation client disconnected before completion");
                        }
                    },
                );
                let execution = match wait_for_invocation(rx, &stream, cancellation) {
                    Ok(execution) => execution,
                    Err(error) => {
                        eprintln!("workerd-host: invocation completion unavailable: {error}");
                        let _ = write_http_error(
                            &mut stream,
                            503,
                            "invocation unavailable",
                            ConnectionMode::Close,
                        );
                        return;
                    }
                };
                drop(guard);
                let (status, body) = match execution.result {
                    Ok(response) => {
                        let mut body = serde_json::to_value(response)
                            .expect("invocation response is serializable");
                        body["protocol_version"] = serde_json::json!(1);
                        (200, body.to_string())
                    }
                    Err(error) => {
                        let status = if execution.submit_error.is_some() {
                            503
                        } else if matches!(error, hyperlight_unikraft::workerd::Error::Timeout) {
                            504
                        } else {
                            502
                        };
                        eprintln!("workerd-host: invocation failed: {error}");
                        (
                            status,
                            serde_json::json!({"protocol_version":1,"error":error.to_string()})
                                .to_string(),
                        )
                    }
                };
                if let Err(error) = write_http_response(
                    &mut stream,
                    status,
                    "application/json",
                    body.as_bytes(),
                    ConnectionMode::Close,
                ) {
                    eprintln!("workerd-host: invocation response write failed: {error}");
                }
                return;
            }
            if role == ListenerRole::Admin {
                let _ = write_http_error(
                    &mut stream,
                    404,
                    "admin listener does not serve applications",
                    ConnectionMode::Close,
                );
                return;
            }
            // Linearize each invocation against shutdown, including requests
            // arriving on sockets accepted before the signal.
            let admission_guard = admission.lock().expect("admission lock poisoned");
            if shutting_down.load(Ordering::SeqCst) {
                drop(admission_guard);
                let _ =
                    write_http_error(&mut stream, 503, "host is draining", ConnectionMode::Close);
                return;
            }
            let _in_flight_guard = InFlightGuard::new(in_flight);
            drop(admission_guard);
            let host_header = parsed
                .envelope
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("host"))
                .map(|header| header.value.as_str());
            let guard = state.read().expect("workerd-host state lock poisoned");
            let registry = match &*guard {
                WorkerdHostState::Ready(registry) => registry,
                _ => {
                    drop(guard);
                    let _ =
                        write_http_error(&mut stream, 503, "host is not ready", connection_mode);
                    if !keep_alive {
                        return;
                    }
                    continue;
                }
            };
            let handle = match registry.route(host_header, &parsed.path) {
                Some(handle) => handle,
                None => {
                    drop(guard);
                    let _ = write_http_error(
                        &mut stream,
                        404,
                        "no app matches this request",
                        connection_mode,
                    );
                    if !keep_alive {
                        return;
                    }
                    continue;
                }
            };
            if handle.identity().streaming {
                use hyperlight_unikraft::workerd::{
                    HostIngress, InvocationCancellation, decode_buffered_input, pump_http_stream,
                    websocket_requested,
                };
                let timeout = handle.execute_timeout();
                let websocket = websocket_requested(&parsed.envelope);
                let body = match decode_buffered_input(&parsed.envelope) {
                    Ok(body) => body,
                    Err(error) => {
                        let _ = write_http_error(
                            &mut stream,
                            400,
                            &error.to_string(),
                            ConnectionMode::Close,
                        );
                        return;
                    }
                };
                let mut request = parsed.envelope;
                request.body_base64.clear();
                let (host_ingress, guest_ingress) = match HostIngress::pair(
                    &request.request_id,
                    websocket,
                    timeout,
                    InvocationCancellation::default(),
                ) {
                    Ok(pair) => pair,
                    Err(error) => {
                        let _ = write_http_error(
                            &mut stream,
                            400,
                            &error.to_string(),
                            ConnectionMode::Close,
                        );
                        return;
                    }
                };
                let (tx, rx) = mpsc::channel();
                let _ = handle.try_submit_stream(
                    request.clone(),
                    websocket,
                    guest_ingress,
                    timeout,
                    move |execution| {
                        if tx.send(execution).is_err() {
                            tracing::debug!("stream completion receiver disconnected");
                        }
                    },
                );
                if let Err(error) = pump_http_stream(stream, &request, &body, host_ingress, rx) {
                    eprintln!("workerd-host: streaming transport failed:{error}");
                }
                return;
            }
            if reserved
                .as_ref()
                .is_some_and(|(app_id, _)| app_id != handle.app_id())
            {
                // The connection's requests now route to a different app
                // than the one this reservation was made for; releasing it
                // (dropping the `ResidentHandle`) frees the owner thread
                // back to its pool immediately rather than holding it idle
                // for the rest of this connection.
                reserved = None;
            }
            if reserved.is_none() && keep_alive && handle.affinity() == ConnectionAffinity::Sticky {
                // Only worth reserving a whole resident VM when the
                // connection might actually send more than one request;
                // a pool-full reservation attempt silently falls back to
                // the shared pool below rather than failing the request.
                if let Some(resident) = handle.reserve() {
                    reserved = Some((handle.app_id().to_string(), resident));
                }
            }
            // Raised before `execute_request` runs and held until this
            // iteration's scope ends (after the response write below,
            // including on early `return`/`continue` paths) — see
            // `InFlightGuard`'s doc comment for why this must cover the
            // write too, not just pool execution.
            let execution = execute_request(
                handle,
                reserved.as_ref().map(|(_, resident)| resident),
                parsed.envelope,
                handle.execute_timeout(),
                &stream,
            );
            drop(guard);
            if let Err(error) = write_execution_response(&mut stream, execution, connection_mode) {
                eprintln!("workerd-host: response write failed: {error}");
                return;
            }
            if !keep_alive {
                return;
            }
        }
    }

    // Step 1: load + validate the config's *shape* before binding anything
    // (duplicate app ids, missing hostnames, ambiguous routes) — exit 2.
    // Loading each app's bundle/VM happens later, off the accept thread.
    let config = match HostConfig::from_path(&args.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(2);
        }
    };
    if let Err(error) = AppRegistry::validate(&config) {
        eprintln!("error: {error}");
        std::process::exit(2);
    }

    // Step 2: bind before any app finishes initializing, so healthz can
    // serve while a slow app's VM is still warming up — exit 4 on failure.
    let listener = match TcpListener::bind(&args.bind) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("error: failed to bind {}: {error}", args.bind);
            std::process::exit(4);
        }
    };
    let data_port = listener.local_addr()?.port();
    listener.set_nonblocking(true)?;
    let admin_listener = match &args.admin_bind {
        Some(addr) => match TcpListener::bind(addr) {
            Ok(listener) => {
                listener.set_nonblocking(true)?;
                Some(listener)
            }
            Err(error) => {
                eprintln!("error: failed to bind admin {addr}: {error}");
                std::process::exit(4);
            }
        },
        None => None,
    };
    eprintln!(
        "workerd-host listening on http://{} (admin {})",
        listener.local_addr()?,
        admin_listener
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "shared with app traffic".into())
    );

    // Step 3: build every app's worker + pool off the accept thread.
    let state = Arc::new(RwLock::new(WorkerdHostState::Initializing));
    let initializer = {
        let state = state.clone();
        std::thread::spawn(move || {
            let built = AppRegistry::from_host_config(config);
            let mut guard = state.write().expect("workerd-host state lock poisoned");
            *guard = match built {
                Ok(registry) => WorkerdHostState::Ready(registry),
                Err(error) => WorkerdHostState::Failed(error.to_string()),
            };
        })
    };

    let shutting_down = Arc::new(AtomicBool::new(false));
    let admission = Arc::new(Mutex::new(()));
    {
        let shutting_down = shutting_down.clone();
        let admission = admission.clone();
        ctrlc::set_handler(move || {
            let _guard = admission.lock().expect("admission lock poisoned");
            shutting_down.store(true, Ordering::SeqCst);
        })?;
    }

    let in_flight = Arc::new(AtomicUsize::new(0));
    let sequence = Arc::new(AtomicUsize::new(1));
    let app_connections = Arc::new(AtomicUsize::new(0));
    let admin_connections = Arc::new(AtomicUsize::new(0));
    let mut connections: Vec<std::thread::JoinHandle<()>> = Vec::new();
    let drain_timeout = Duration::from_millis(args.drain_timeout_ms);
    let mut fatal_exit_code: Option<i32> = None;
    let mut logged_ready = false;
    loop {
        if shutting_down.load(Ordering::SeqCst) {
            break;
        }
        {
            let guard = state.read().expect("workerd-host state lock poisoned");
            match &*guard {
                WorkerdHostState::Failed(message) => {
                    eprintln!("error: {message}");
                    fatal_exit_code = Some(3);
                }
                WorkerdHostState::Ready(registry) if !logged_ready => {
                    let apps: Vec<&str> = registry.app_ids().collect();
                    println!("{}", serde_json::json!({"event": "ready", "apps": apps}));
                    logged_ready = true;
                }
                _ => {}
            }
        }
        if fatal_exit_code.is_some() {
            break;
        }
        let mut index = 0;
        while index < connections.len() {
            if connections[index].is_finished() {
                if connections.swap_remove(index).join().is_err() {
                    eprintln!("error: workerd-host connection handler panicked");
                    fatal_exit_code = Some(1);
                }
            } else {
                index += 1;
            }
        }
        let app_role = if admin_listener.is_some() {
            ListenerRole::Application
        } else {
            ListenerRole::Shared
        };
        let listeners = [
            Some((&listener, app_role, &app_connections, args.max_connections)),
            admin_listener.as_ref().map(|listener| {
                (
                    listener,
                    ListenerRole::Admin,
                    &admin_connections,
                    args.max_admin_connections,
                )
            }),
        ];
        for (incoming, role, counter, limit) in listeners.into_iter().flatten() {
            match incoming.accept() {
                Ok((stream, _addr)) => {
                    if counter.load(Ordering::Acquire) >= limit as usize {
                        // Close rather than block the accept thread writing to
                        // a slow peer. Admin probes have an independent budget.
                        drop(stream);
                        continue;
                    }
                    // The accepted socket inherits the listening socket's
                    // non-blocking mode on Windows (unlike POSIX, where a
                    // fresh accepted socket starts blocking regardless of
                    // the listener's mode); revert it so the per-connection
                    // thread's `set_read_timeout`/`set_write_timeout` below
                    // actually block up to their timeout instead of
                    // returning `WouldBlock` immediately whenever no bytes
                    // are queued yet — needed once boundary 5's keep-alive
                    // loop reads a second request that genuinely has to
                    // wait for the client. Same fix as
                    // `workerd::fetch`'s loopback listener and the
                    // `dotnet_jit`/`workerd_sandbox` test harnesses use.
                    if let Err(error) = stream.set_nonblocking(false) {
                        eprintln!(
                            "workerd-host: failed to clear non-blocking mode on accepted connection: {error}"
                        );
                        continue;
                    }
                    let state = state.clone();
                    let in_flight = in_flight.clone();
                    let shutting_down = shutting_down.clone();
                    let admission = admission.clone();
                    counter.fetch_add(1, Ordering::AcqRel);
                    let connection_guard = ConnectionGuard(counter.clone());
                    let request_sequence = sequence.fetch_add(1, Ordering::Relaxed) as u64;
                    match std::thread::Builder::new()
                        .name("workerd-connection".into())
                        .spawn(move || {
                            let _connection_guard = connection_guard;
                            handle_host_connection(
                                stream,
                                HostConnection {
                                    state: &state,
                                    in_flight: &in_flight,
                                    connection_sequence: request_sequence,
                                    role,
                                    shutting_down: &shutting_down,
                                    admission: &admission,
                                    data_port,
                                },
                            );
                        }) {
                        Ok(handle) => connections.push(handle),
                        Err(error) => {
                            eprintln!("error: workerd-host connection spawn failed: {error}");
                            fatal_exit_code = Some(1);
                        }
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => eprintln!("workerd-host: accept failed: {error}"),
            }
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    // Drain: no new connections are accepted once we reach here; wait for
    // every in-flight request's response to finish.
    let drain_start = Instant::now();
    {
        let _guard = admission.lock().expect("admission lock poisoned");
        shutting_down.store(true, Ordering::SeqCst);
    }
    while (in_flight.load(Ordering::SeqCst) > 0
        || connections.iter().any(|handle| !handle.is_finished())
        || !initializer.is_finished())
        && drain_start.elapsed() < drain_timeout
    {
        std::thread::sleep(POLL_INTERVAL);
    }
    let drained = in_flight.load(Ordering::SeqCst) == 0
        && connections.iter().all(std::thread::JoinHandle::is_finished)
        && initializer.is_finished();
    if drained {
        for handle in connections {
            if handle.join().is_err() {
                eprintln!("error: workerd-host connection handler panicked during drain");
                fatal_exit_code = Some(1);
            }
        }
        if initializer.join().is_err() {
            eprintln!("error: workerd-host initializer panicked");
            fatal_exit_code = Some(1);
        }
    }
    // Dropping every app's pool runs its own `Drop` (already exercised and
    // validated by Boundaries 1-3): resident pools retire every resident
    // VM, disposable pools stop admitting and join their owners.
    drop(state);

    if let Some(code) = fatal_exit_code {
        std::process::exit(code);
    }
    if !drained {
        eprintln!(
            "error: drain timeout exceeded with live requests, connections or initialization"
        );
        std::process::exit(1);
    }
    println!(
        "{}",
        serde_json::json!({"event": "shutdown", "reason": "signal"})
    );
    Ok(())
}

/// Run the workload in a booted guest.  With nothing to run and no driver
/// to run it, the entry point is a plain program (`--entry /bin/server`):
/// drive it to its exit the way a container runtime would, and exit with
/// its status, as running it directly would.
fn drive(sandbox: &mut AppSandbox, no_workload: bool, exec: Exec) -> CliResult<()> {
    let t = Instant::now();
    if no_workload && !sandbox.has_driver() {
        info!("no driver in the guest; driving its entry point to exit");
        let status = sandbox.join()?;
        info!(
            elapsed_ms = t.elapsed().as_secs_f64() * 1000.0,
            status, "exec"
        );
        if status != 0 {
            std::process::exit(status);
        }
        return Ok(());
    }
    sandbox.run(exec)?;
    info!(elapsed_ms = t.elapsed().as_secs_f64() * 1000.0, "exec");
    Ok(())
}

fn cmd_snapshot_save(args: SaveArgs) -> CliResult<()> {
    let mounts = parse_mounts(&args.mounts)?;
    let (policy, listen) =
        parse_net_policy(args.net, &args.net_allow, &args.net_block, &args.ports)?;
    let mut builder = base_builder(args.kernel, args.initrd)?
        .scratch_mb(args.scratch_mb)
        .mounts(mounts);
    if let Some(entry) = args.entry {
        builder = builder.entry(entry);
    }
    if let Some(policy) = policy {
        builder = builder.network(policy);
    }
    if let Some(listen) = listen {
        builder = builder.listen_ports(listen);
    }
    let mut sandbox = builder.boot()?;

    let t = Instant::now();
    sandbox.snapshot_to(&args.output)?;
    let save_ms = t.elapsed().as_secs_f64() * 1000.0;
    info!(
        path = %args.output.display(),
        elapsed_ms = save_ms,
        "snapshot saved",
    );

    eprintln!(
        "Snapshot saved to {} ({:.1} ms)",
        args.output.display(),
        save_ms,
    );

    Ok(())
}

fn cmd_snapshot_run(args: SnapshotRunArgs) -> CliResult<()> {
    let mounts = parse_mounts(&args.mounts)?;
    let (policy, listen) =
        parse_net_policy(args.net, &args.net_allow, &args.net_block, &args.ports)?;

    let t = Instant::now();
    let builder = SandboxBuilder::from_snapshot_dir(&args.snapshot)?;
    info!(
        path = %args.snapshot.display(),
        elapsed_ms = t.elapsed().as_secs_f64() * 1000.0,
        "snapshot loaded",
    );

    let envs = parse_envs(&args.envs)?;

    let mut builder = builder.mounts(mounts);
    if let Some(policy) = policy {
        builder = builder.network(policy);
    }
    if let Some(listen) = listen {
        builder = builder.listen_ports(listen);
    }
    for (key, value) in envs {
        builder = builder.env(key, value);
    }

    let t = Instant::now();
    let mut sandbox = builder.boot()?;
    info!(
        elapsed_ms = t.elapsed().as_secs_f64() * 1000.0,
        "restored from snapshot",
    );

    // Precedence: host script / --exec code; else --guest-exec; else the
    // rootfs's conventional entrypoint.
    let no_workload = args.script.is_none() && args.exec.is_none() && args.guest_exec.is_none();
    let exec = resolve_exec(args.script, args.exec)?
        .unwrap_or_else(|| Exec::Guest(args.guest_exec.unwrap_or_default()));
    drive(&mut sandbox, no_workload, exec)
}

// ── Bench helpers ────────────────────────────────────────────────

/// Read a script file and return its source for dispatch.
fn read_script(path: &std::path::Path) -> CliResult<String> {
    Ok(std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?)
}

/// Percentile from a **sorted** slice (linear interpolation).
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = p / 100.0 * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    let frac = idx - lo as f64;
    sorted[lo] * (1.0 - frac) + sorted[hi] * frac
}

/// Print summary line: median, p95, min, max.
fn print_summary(label: &str, field: &str, values: &[f64]) {
    if values.is_empty() {
        return;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = percentile(&sorted, 50.0);
    let p95 = percentile(&sorted, 95.0);
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    println!(
        "BENCH {label} {field} median={median:.3} p95={p95:.3} min={min:.3} max={max:.3} samples={}",
        values.len(),
    );
}

/// Print snapshot size on disk as a BENCH line.
fn print_snapshot_size(label: &str, snap_dir: &std::path::Path) {
    fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
        let mut total = 0;
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_file() {
                total += meta.len();
            } else if meta.is_dir() {
                total += dir_size(&entry.path())?;
            }
        }
        Ok(total)
    }
    if let Ok(bytes) = dir_size(snap_dir) {
        let mib = bytes as f64 / (1024.0 * 1024.0);
        println!("BENCH {label} snapshot_mib={mib:.1}");
    }
}

/// Print resident memory as a BENCH line.
/// This is the density-relevant metric — it scales linearly with VM count.
/// Linux reports `RssAnon` (anonymous resident pages).  Windows reports the
/// working set: guest memory there is a section mapping, which the
/// private-commit counters do not attribute to the process.  The two are
/// comparable within an OS, not across them.
fn print_rss(label: &str) {
    #[cfg(target_os = "linux")]
    if let Ok(status) = std::fs::read_to_string("/proc/self/status")
        && let Some(kb) = status
            .lines()
            .find(|l| l.starts_with("RssAnon:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u64>().ok())
    {
        println!("BENCH {label} rss_mb={}", kb / 1024);
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::ProcessStatus::{
            K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;

        let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        // SAFETY: `counters` is a valid, writable struct of the size passed
        // in `cb`.
        let ok = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &raw mut counters, size) };
        if ok != 0 {
            println!(
                "BENCH {label} rss_mb={}",
                counters.WorkingSetSize / (1024 * 1024)
            );
        }
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    let _ = label;
}

// ── Bench commands ───────────────────────────────────────────────

/// Cold start: fresh boot + dispatch, N independent samples.
fn bench_cold(args: BenchColdArgs) -> CliResult<()> {
    let source = read_script(&args.script)?;
    let mut boots = Vec::with_capacity(args.samples);
    let mut execs = Vec::with_capacity(args.samples);
    let mut totals = Vec::with_capacity(args.samples);

    for i in 0..args.samples {
        let t0 = Instant::now();
        let mut sandbox = SandboxBuilder::from_initrd(args.initrd.clone())
            .scratch_mb(args.scratch_mb)
            .boot()?;
        let boot_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let t1 = Instant::now();
        sandbox.run(source.as_str())?;
        let exec_ms = t1.elapsed().as_secs_f64() * 1000.0;

        let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "BENCH cold sample={i} boot_ms={boot_ms:.3} exec_ms={exec_ms:.3} total_ms={total_ms:.3}"
        );
        boots.push(boot_ms);
        execs.push(exec_ms);
        totals.push(total_ms);
    }

    print_summary("cold", "boot_ms", &boots);
    print_summary("cold", "exec_ms", &execs);
    print_summary("cold", "total_ms", &totals);
    print_rss("cold");
    Ok(())
}

/// Cold snapshot: load from disk + restore + dispatch, N independent samples.
fn bench_cold_snap(args: BenchSnapArgs) -> CliResult<()> {
    let source = read_script(&args.script)?;
    let mut loads = Vec::with_capacity(args.samples);
    let mut restores = Vec::with_capacity(args.samples);
    let mut execs = Vec::with_capacity(args.samples);
    let mut totals = Vec::with_capacity(args.samples);

    for i in 0..args.samples {
        let t0 = Instant::now();
        let builder = SandboxBuilder::from_snapshot_dir(&args.snapshot)?;
        let load_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let t1 = Instant::now();
        let mut sandbox = builder.boot()?;
        let restore_ms = t1.elapsed().as_secs_f64() * 1000.0;

        let t2 = Instant::now();
        sandbox.run(source.as_str())?;
        let exec_ms = t2.elapsed().as_secs_f64() * 1000.0;

        let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
        println!(
            "BENCH cold-snap sample={i} load_ms={load_ms:.3} restore_ms={restore_ms:.3} \
             exec_ms={exec_ms:.3} total_ms={total_ms:.3}"
        );
        loads.push(load_ms);
        restores.push(restore_ms);
        execs.push(exec_ms);
        totals.push(total_ms);
    }

    print_summary("cold-snap", "load_ms", &loads);
    print_summary("cold-snap", "restore_ms", &restores);
    print_summary("cold-snap", "exec_ms", &execs);
    print_summary("cold-snap", "total_ms", &totals);
    print_snapshot_size("cold-snap", &args.snapshot);
    print_rss("cold-snap");
    Ok(())
}

/// Warm with restore: load snapshot once, then loop dispatch + restore.
fn bench_warm_restore(args: BenchSnapArgs) -> CliResult<()> {
    let source = read_script(&args.script)?;

    let t0 = Instant::now();
    let snap = load_snapshot(&args.snapshot)?;
    let mut sandbox = SandboxBuilder::from_snapshot(snap.clone()).boot()?;
    let setup_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!("BENCH warm-restore setup_ms={setup_ms:.3}");

    let mut execs = Vec::with_capacity(args.samples);
    let mut restores = Vec::with_capacity(args.samples);

    for i in 0..args.samples {
        let t1 = Instant::now();
        sandbox.run(source.as_str())?;
        let exec_ms = t1.elapsed().as_secs_f64() * 1000.0;

        let t2 = Instant::now();
        sandbox.restore(snap.clone())?;
        let restore_ms = t2.elapsed().as_secs_f64() * 1000.0;

        println!("BENCH warm-restore sample={i} exec_ms={exec_ms:.3} restore_ms={restore_ms:.3}");
        execs.push(exec_ms);
        restores.push(restore_ms);
    }

    print_summary("warm-restore", "exec_ms", &execs);
    print_summary("warm-restore", "restore_ms", &restores);
    print_snapshot_size("warm-restore", &args.snapshot);
    print_rss("warm-restore");
    Ok(())
}

/// Warm stateful: load snapshot once, then loop dispatch without restore.
fn bench_warm_stateful(args: BenchSnapArgs) -> CliResult<()> {
    let source = read_script(&args.script)?;

    let t0 = Instant::now();
    let snap = load_snapshot(&args.snapshot)?;
    let mut sandbox = SandboxBuilder::from_snapshot(snap).boot()?;
    let setup_ms = t0.elapsed().as_secs_f64() * 1000.0;
    println!("BENCH warm-stateful setup_ms={setup_ms:.3}");

    let mut execs = Vec::with_capacity(args.samples);

    for i in 0..args.samples {
        let t1 = Instant::now();
        sandbox.run(source.as_str())?;
        let exec_ms = t1.elapsed().as_secs_f64() * 1000.0;

        println!("BENCH warm-stateful sample={i} exec_ms={exec_ms:.3}");
        execs.push(exec_ms);
    }

    print_summary("warm-stateful", "exec_ms", &execs);
    print_snapshot_size("warm-stateful", &args.snapshot);
    print_rss("warm-stateful");
    Ok(())
}

/// Parallel VMs: spawn N threads, each restoring from the same snapshot.
fn bench_parallel(args: BenchParallelArgs) -> CliResult<()> {
    let source = Arc::new(read_script(&args.script)?);
    let snap = load_snapshot(&args.snapshot)?;

    // Barrier so all VMs start at the same time.
    let barrier = Arc::new(Barrier::new(args.vms));
    let wall_start = Instant::now();

    let handles: Vec<_> = (0..args.vms)
        .map(|vm_id| {
            let snap = snap.clone();
            let source = source.clone();
            let barrier = barrier.clone();
            let iterations = args.iterations;

            std::thread::spawn(move || -> Result<Vec<f64>, String> {
                barrier.wait();
                let vm_start = Instant::now();

                let mut sandbox = SandboxBuilder::from_snapshot(snap.clone())
                    .boot()
                    .map_err(|e| e.to_string())?;
                let mut execs = Vec::with_capacity(iterations);

                for iter in 0..iterations {
                    let t = Instant::now();
                    sandbox.run(source.as_str()).map_err(|e| e.to_string())?;
                    let exec_ms = t.elapsed().as_secs_f64() * 1000.0;

                    let t = Instant::now();
                    sandbox.restore(snap.clone()).map_err(|e| e.to_string())?;
                    let restore_ms = t.elapsed().as_secs_f64() * 1000.0;

                    println!(
                        "BENCH parallel vm={vm_id} iter={iter} \
                         exec_ms={exec_ms:.3} restore_ms={restore_ms:.3}"
                    );
                    execs.push(exec_ms);
                }

                let vm_total_ms = vm_start.elapsed().as_secs_f64() * 1000.0;
                let vm_throughput = iterations as f64 / (vm_total_ms / 1000.0);
                println!(
                    "BENCH parallel vm={vm_id} total_ms={vm_total_ms:.3} \
                     iterations={iterations} throughput={vm_throughput:.1}/s"
                );
                Ok(execs)
            })
        })
        .collect();

    let mut errors = Vec::new();
    let mut all_execs = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        match h.join().unwrap_or_else(|_| Err("thread panicked".into())) {
            Ok(execs) => all_execs.extend(execs),
            Err(e) => errors.push(format!("vm {i}: {e}")),
        }
    }

    let wall_ms = wall_start.elapsed().as_secs_f64() * 1000.0;
    let total_calls = args.vms * args.iterations;
    let throughput = total_calls as f64 / (wall_ms / 1000.0);
    println!(
        "BENCH parallel summary vms={} iterations={} total_calls={total_calls} \
         wall_ms={wall_ms:.3} throughput={throughput:.1}/s",
        args.vms, args.iterations,
    );
    if !all_execs.is_empty() {
        print_summary("parallel", "exec_ms", &all_execs);
    }
    print_snapshot_size("parallel", &args.snapshot);
    print_rss("parallel");

    if !errors.is_empty() {
        return Err(errors.join("; ").into());
    }
    Ok(())
}

// ── Main ─────────────────────────────────────────────────────────

fn main() {
    let Err(e) = cli_main() else { return };
    // A script that failed has said why in the guest's output, and its
    // status is the command's, as when running it directly; a guest that
    // exited under a call is reported, with its status likewise.
    match e.downcast_ref::<Error>() {
        Some(Error::CallFailed { status }) => std::process::exit(*status),
        Some(Error::GuestExited { status }) => {
            eprintln!("error: {e}");
            std::process::exit(*status);
        }
        _ => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

fn cli_main() -> CliResult<()> {
    let cli = Cli::parse();

    if let Some(level) = cli.log_level {
        // RUST_LOG overrides --log-level when set; otherwise scope to
        // our crate only so library noise doesn't leak through.
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(format!("hyperlight_unikraft={level}")));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_writer(std::io::stderr)
            .init();
    }

    // One guest at a time, so skip Hyperlight's 512 pre-spawned helper
    // processes on Windows; `bench parallel` needs one per VM, and
    // `workerd-host` needs one per concurrently-alive sandbox across every
    // configured app (each resident app's pool capacity, plus each
    // disposable app's `max_concurrent_sandboxes`) since its apps' pools
    // run concurrently in one process.
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(match &cli.command {
        Command::Bench(BenchCommand::Parallel(args)) => args.vms,
        Command::WorkerdHost(args) => workerd_host_surrogate_capacity(args),
        _ => 0,
    });

    match cli.command {
        Command::Run(args) => cmd_run(args),
        Command::Workerd(args) => cmd_workerd(args),
        Command::WorkerdHost(args) => cmd_workerd_host(args),
        Command::WorkerdPrewarmSnapshot(args) => cmd_workerd_prewarm_snapshot(args),
        Command::WorkerdIdentity(args) => cmd_workerd_identity(args),
        Command::Snapshot(cmd) => match cmd {
            SnapshotCommand::Save(args) => cmd_snapshot_save(args),
            SnapshotCommand::Run(args) => cmd_snapshot_run(args),
        },
        Command::Bench(cmd) => match cmd {
            BenchCommand::Cold(args) => bench_cold(args),
            BenchCommand::ColdSnap(args) => bench_cold_snap(args),
            BenchCommand::WarmRestore(args) => bench_warm_restore(args),
            BenchCommand::WarmStateful(args) => bench_warm_stateful(args),
            BenchCommand::Parallel(args) => bench_parallel(args),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workerd_bundle_and_script_are_exclusive() {
        assert!(
            Cli::try_parse_from([
                "hluk",
                "workerd",
                "--bundle",
                "bundle.json",
                "--script",
                "worker.js"
            ])
            .is_err()
        );
        let cli = Cli::try_parse_from(["hluk", "workerd", "--bundle", "bundle.json"]).unwrap();
        assert!(matches!(cli.command, Command::Workerd(_)));
    }

    #[test]
    fn parse_envs_basic() {
        let input = vec!["KEY=value".into(), "DEBUG=1".into()];
        let envs = parse_envs(&input).unwrap();
        assert_eq!(envs, vec![("KEY", "value"), ("DEBUG", "1")]);
    }

    #[test]
    fn parse_envs_value_with_equals() {
        let input = vec!["CONN=host=db;port=5432".into()];
        let envs = parse_envs(&input).unwrap();
        assert_eq!(envs, vec![("CONN", "host=db;port=5432")]);
    }

    #[test]
    fn parse_envs_empty_value() {
        let input = vec!["EMPTY=".into()];
        let envs = parse_envs(&input).unwrap();
        assert_eq!(envs, vec![("EMPTY", "")]);
    }

    #[test]
    fn parse_envs_rejects_an_entry_without_equals() {
        let input = vec!["GOOD=1".into(), "no_equals".into()];
        let err = parse_envs(&input).unwrap_err();
        assert!(err.contains("no_equals"), "got: {err}");
    }

    #[test]
    fn parse_envs_empty_input() {
        let envs = parse_envs(&[]).unwrap();
        assert!(envs.is_empty());
    }

    #[test]
    fn parse_unix_rw_mount() {
        let mounts = parse_mounts(&["/tmp/share:/mnt/host".into()]).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].host_path, PathBuf::from("/tmp/share"));
        assert_eq!(mounts[0].guest_path, "/mnt/host");
        assert!(!mounts[0].readonly);
    }

    #[test]
    fn parse_unix_ro_mount() {
        let mounts = parse_mounts(&["/data:/mnt/data:ro".into()]).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].host_path, PathBuf::from("/data"));
        assert_eq!(mounts[0].guest_path, "/mnt/data");
        assert!(mounts[0].readonly);
    }

    #[test]
    fn parse_multiple_mounts() {
        let mounts = parse_mounts(&["/a:/mnt/a".into(), "/b:/mnt/b:ro".into()]).unwrap();
        assert_eq!(mounts.len(), 2);
        assert!(!mounts[0].readonly);
        assert!(mounts[1].readonly);
    }

    #[test]
    fn parse_windows_drive_rw() {
        let mounts = parse_mounts(&[r"C:\Users\data:/mnt/data".into()]).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].host_path, PathBuf::from(r"C:\Users\data"));
        assert_eq!(mounts[0].guest_path, "/mnt/data");
        assert!(!mounts[0].readonly);
    }

    #[test]
    fn parse_windows_drive_ro() {
        let mounts = parse_mounts(&[r"D:\share:/mnt/host:ro".into()]).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].host_path, PathBuf::from(r"D:\share"));
        assert_eq!(mounts[0].guest_path, "/mnt/host");
        assert!(mounts[0].readonly);
    }

    #[test]
    fn parse_relative_path() {
        let mounts = parse_mounts(&["./data:/mnt".into()]).unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].host_path, PathBuf::from("./data"));
        assert_eq!(mounts[0].guest_path, "/mnt");
    }

    #[test]
    fn parse_mounts_rejects_an_entry_without_colon() {
        let err = parse_mounts(&["invalid".into()]).unwrap_err();
        assert!(err.contains("invalid"), "got: {err}");
    }

    #[test]
    fn net_policy_none_by_default() {
        let (policy, listen) = parse_net_policy(false, &[], &[], &[]).unwrap();
        assert!(policy.is_none());
        assert!(listen.is_none());
    }

    #[test]
    fn net_policy_bare_net_is_allow_all() {
        let (policy, listen) = parse_net_policy(true, &[], &[], &[]).unwrap();
        assert!(matches!(policy, Some(NetworkPolicy::AllowAll)));
        assert!(listen.is_none());
    }

    #[test]
    fn net_policy_port_with_net_sets_listen() {
        let (policy, listen) = parse_net_policy(true, &[], &[], &[8080]).unwrap();
        assert!(matches!(policy, Some(NetworkPolicy::AllowAll)));
        assert!(listen.is_some());
    }

    #[test]
    fn net_policy_port_with_allow_list_sets_listen() {
        let (policy, listen) =
            parse_net_policy(false, &["example.com".into()], &[], &[8080]).unwrap();
        assert!(matches!(policy, Some(NetworkPolicy::AllowList(_))));
        assert!(listen.is_some());
    }

    #[test]
    fn net_policy_port_without_net_is_rejected() {
        // --port alone would be a silent no-op (hostnet is only registered
        // when a policy is present), so it must be an error, not accepted.
        let err = parse_net_policy(false, &[], &[], &[8080]).unwrap_err();
        assert!(err.contains("--port requires networking"), "got: {err}");
    }
}
