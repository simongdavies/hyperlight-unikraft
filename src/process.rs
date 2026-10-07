// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Host-owned virtual processes backed by distinct restored HLUK VMs.
//!
//! This module provides lifecycle plumbing for warm-snapshot process creation
//! and the experimental running-process snapshot path. It does not implement
//! a POSIX `fork(2)` guest ABI.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::{AppSandbox, Error, SandboxBuilder, Snapshot};

const FIRST_VIRTUAL_PID: u64 = 10_000;
const CANCEL_HOST_FUNCTION: &str = "process.cancelled";
const DEFAULT_STDOUT_CHUNKS: NonZeroUsize =
    NonZeroUsize::new(8).expect("default stdout capacity is non-zero");

/// A host-owned process identifier. Values are monotonically allocated and
/// are never reused by a [`VmProcessHost`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VirtualPid(u64);

impl VirtualPid {
    /// Return the numeric process identifier.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for VirtualPid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why a trusted snapshot is being restored as a new logical process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessSnapshotKind {
    /// Start a new process from an application snapshot prepared for repeated
    /// warm restores.
    WarmRestore,
    /// Continue a running process image captured at the experimental
    /// post-prepare boundary.
    RunningProcessFork {
        /// Live application threads represented by the snapshot.
        live_application_threads: usize,
    },
}

/// A snapshot produced by the current host contract and selected for process
/// creation.
#[derive(Clone)]
pub struct TrustedProcessSnapshot {
    snapshot: Arc<Snapshot>,
    kind: ProcessSnapshotKind,
}

impl fmt::Debug for TrustedProcessSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrustedProcessSnapshot")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl TrustedProcessSnapshot {
    /// Use a trusted snapshot as a warm process template.
    pub fn warm(snapshot: Arc<Snapshot>) -> Self {
        Self {
            snapshot,
            kind: ProcessSnapshotKind::WarmRestore,
        }
    }

    /// Use a trusted snapshot captured from a running process.
    ///
    /// The current experiment accepts exactly one live application thread.
    pub fn running_process_fork(snapshot: Arc<Snapshot>, live_application_threads: usize) -> Self {
        Self {
            snapshot,
            kind: ProcessSnapshotKind::RunningProcessFork {
                live_application_threads,
            },
        }
    }

    fn validate(&self) -> Result<(), ProcessError> {
        Self::validate_kind(self.kind)
    }

    fn validate_kind(kind: ProcessSnapshotKind) -> Result<(), ProcessError> {
        if let ProcessSnapshotKind::RunningProcessFork {
            live_application_threads,
        } = kind
            && live_application_threads != 1
        {
            return Err(ProcessError::UnsupportedApplicationThreadCount {
                live_application_threads,
            });
        }
        Ok(())
    }
}

/// Requested treatment of a host-owned capability when creating a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityDisposition {
    /// Omit the capability from the child.
    CloseInChild,
    /// Parent and child would share one host open-description state.
    ShareOpenDescription,
    /// The child would receive a separate view of the same resource.
    DuplicateView,
    /// The child would receive an independent copy.
    PrivateCopy,
    /// The child would receive a newly established equivalent resource.
    Recreate,
    /// The capability explicitly prevents process creation.
    Unsupported,
}

/// One live capability considered during process creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InheritedCapability {
    name: String,
    disposition: CapabilityDisposition,
}

impl InheritedCapability {
    /// Describe a capability and its required child disposition.
    pub fn new(name: impl Into<String>, disposition: CapabilityDisposition) -> Self {
        Self {
            name: name.into(),
            disposition,
        }
    }
}

/// Host-side settings for one virtual process.
#[derive(Debug, Clone)]
pub struct ProcessOptions {
    stdout_chunks: NonZeroUsize,
    capabilities: Vec<InheritedCapability>,
}

impl Default for ProcessOptions {
    fn default() -> Self {
        Self {
            stdout_chunks: DEFAULT_STDOUT_CHUNKS,
            capabilities: Vec::new(),
        }
    }
}

impl ProcessOptions {
    /// Bound stdout by the number of guest write chunks that may wait for the
    /// consumer. The guest write blocks once this capacity is full.
    pub fn stdout_chunks(mut self, capacity: NonZeroUsize) -> Self {
        self.stdout_chunks = capacity;
        self
    }

    /// Declare the complete set of live capabilities considered for child
    /// inheritance.
    pub fn capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = InheritedCapability>,
    ) -> Self {
        self.capabilities = capabilities.into_iter().collect();
        self
    }

    fn validate(&self) -> Result<(), ProcessError> {
        if let Some(capability) = self
            .capabilities
            .iter()
            .find(|capability| capability.disposition != CapabilityDisposition::CloseInChild)
        {
            return Err(ProcessError::UnsupportedCapabilityInheritance {
                capability: capability.name.clone(),
                disposition: capability.disposition,
            });
        }
        Ok(())
    }
}

/// The terminal status reported by the host process registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessExitStatus {
    /// The logical process completed with this status.
    Exited(i32),
    /// Cooperative cancellation did not finish in time and the VM was killed.
    Cancelled,
    /// Process setup or execution failed before a normal status was produced.
    Failed(String),
}

/// The typed lifecycle state retained in the process registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessState {
    /// The restore worker has been created but the VM is not ready yet.
    Starting,
    /// The restored VM is running.
    Running,
    /// Cooperative cancellation has been requested.
    CancellationRequested,
    /// The VM completed without becoming poisoned.
    Exited(ProcessExitStatus),
    /// The VM failed or was hard-stopped and must only be dropped.
    Poisoned(ProcessExitStatus),
}

impl ProcessState {
    fn terminal_status(&self) -> Option<ProcessExitStatus> {
        match self {
            Self::Exited(status) | Self::Poisoned(status) => Some(status.clone()),
            Self::Starting | Self::Running | Self::CancellationRequested => None,
        }
    }
}

/// How a cancellation request completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The process finished after the cooperative request, without a hard stop.
    Cooperative,
    /// The process was still running and `InterruptHandle::kill()` stopped it.
    HardKilled,
    /// The process had already completed.
    AlreadyExited,
}

/// Process creation, control, or wait failure.
#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    /// Running-process snapshot fork is currently restricted to one live
    /// application thread.
    #[error(
        "running-process snapshot fork requires exactly one live application thread, got \
         {live_application_threads}"
    )]
    UnsupportedApplicationThreadCount { live_application_threads: usize },
    /// This slice does not implement the requested capability inheritance.
    #[error(
        "capability {capability:?} requests unsupported process inheritance disposition \
         {disposition:?}"
    )]
    UnsupportedCapabilityInheritance {
        capability: String,
        disposition: CapabilityDisposition,
    },
    /// The monotonically increasing virtual PID space was exhausted.
    #[error("virtual PID space exhausted")]
    VirtualPidExhausted,
    /// The host could not create the process worker.
    #[error("failed to create worker for virtual PID {pid}: {source}")]
    SpawnWorker {
        pid: VirtualPid,
        #[source]
        source: std::io::Error,
    },
    /// The hard-stop handle reported that no running VM entry was interrupted.
    #[error("virtual PID {pid} did not accept the required hard stop")]
    HardStopFailed { pid: VirtualPid },
    /// The process worker ended without publishing a terminal state.
    #[error("virtual PID {pid} ended without a terminal status")]
    MissingExitStatus { pid: VirtualPid },
    /// The stdout stream was already taken from the process handle.
    #[error("stdout for virtual PID {pid} was already taken")]
    StdoutAlreadyTaken { pid: VirtualPid },
    /// The worker thread panicked.
    #[error("worker for virtual PID {pid} panicked")]
    WorkerPanicked { pid: VirtualPid },
}

/// Context available to the VM worker and to a cooperative guest capability.
#[derive(Debug, Clone)]
pub struct ProcessContext {
    pid: VirtualPid,
    cancellation_requested: Arc<AtomicBool>,
}

impl ProcessContext {
    /// The virtual PID allocated before the VM restore begins.
    pub fn pid(&self) -> VirtualPid {
        self.pid
    }

    /// Whether cooperative cancellation has been requested.
    pub fn cancellation_requested(&self) -> bool {
        self.cancellation_requested.load(Ordering::Acquire)
    }
}

/// The receiving end of a bounded stdout transport.
#[derive(Debug)]
pub struct ProcessStdout {
    receiver: Receiver<Vec<u8>>,
}

impl ProcessStdout {
    /// Receive one guest write. `None` is EOF after the VM and its stdout
    /// handler have been dropped.
    pub fn recv(&self) -> Option<Vec<u8>> {
        self.receiver.recv().ok()
    }

    /// Receive one guest write until `timeout`.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>, mpsc::RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    /// Drain all writes through EOF.
    pub fn read_to_end(self) -> Vec<u8> {
        self.receiver.into_iter().flatten().collect()
    }
}

struct ProcessRecord {
    state: Mutex<ProcessState>,
    changed: Condvar,
    interrupt: Mutex<Option<Arc<dyn crate::hyperlight_host::hypervisor::InterruptHandle>>>,
    cancellation_requested: Arc<AtomicBool>,
    hard_killed: AtomicBool,
}

impl ProcessRecord {
    fn new(cancellation_requested: Arc<AtomicBool>) -> Self {
        Self {
            state: Mutex::new(ProcessState::Starting),
            changed: Condvar::new(),
            interrupt: Mutex::new(None),
            cancellation_requested,
            hard_killed: AtomicBool::new(false),
        }
    }

    fn set_state(&self, state: ProcessState) {
        *lock(&self.state) = state;
        self.changed.notify_all();
    }
}

struct ProcessRegistry {
    next_pid: AtomicU64,
    records: Mutex<BTreeMap<VirtualPid, Arc<ProcessRecord>>>,
}

/// Allocates virtual PIDs and owns the lifecycle registry for restored VMs.
#[derive(Clone)]
pub struct VmProcessHost {
    registry: Arc<ProcessRegistry>,
}

impl fmt::Debug for VmProcessHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VmProcessHost")
            .field("processes", &lock(&self.registry.records).len())
            .finish_non_exhaustive()
    }
}

impl Default for VmProcessHost {
    fn default() -> Self {
        Self {
            registry: Arc::new(ProcessRegistry {
                next_pid: AtomicU64::new(FIRST_VIRTUAL_PID),
                records: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

impl VmProcessHost {
    /// Create an empty host process registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restore a distinct VM, register it under a new virtual PID, and run
    /// `work` on its worker thread.
    ///
    /// `configure` installs per-process host capabilities. The library then
    /// installs its reserved cooperative-cancellation query and bounded stdout
    /// transport. The returned handle owns the wait/reap operation.
    pub fn spawn<Configure, Work>(
        &self,
        source: TrustedProcessSnapshot,
        options: ProcessOptions,
        configure: Configure,
        work: Work,
    ) -> Result<VmProcess, ProcessError>
    where
        Configure: FnOnce(SandboxBuilder) -> SandboxBuilder + Send + 'static,
        Work: FnOnce(&mut AppSandbox, &ProcessContext) -> Result<i32, Error> + Send + 'static,
    {
        source.validate()?;
        options.validate()?;
        let pid = self.allocate_pid()?;
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let context = ProcessContext {
            pid,
            cancellation_requested: Arc::clone(&cancellation_requested),
        };
        let record = Arc::new(ProcessRecord::new(cancellation_requested));
        lock(&self.registry.records).insert(pid, Arc::clone(&record));

        let (stdout_sender, stdout_receiver) = mpsc::sync_channel(options.stdout_chunks.get());
        let registry = Arc::clone(&self.registry);
        let worker_record = Arc::clone(&record);
        let worker = thread::Builder::new()
            .name(format!("hluk-vm-process-{pid}"))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_process(
                        source,
                        configure,
                        work,
                        context,
                        stdout_sender,
                        &worker_record,
                    );
                }));
                if result.is_err() {
                    worker_record.set_state(ProcessState::Poisoned(ProcessExitStatus::Failed(
                        "process worker panicked".to_string(),
                    )));
                }
            })
            .map_err(|source| {
                lock(&registry.records).remove(&pid);
                ProcessError::SpawnWorker { pid, source }
            })?;

        Ok(VmProcess {
            pid,
            registry: Arc::clone(&self.registry),
            record,
            stdout: Some(ProcessStdout {
                receiver: stdout_receiver,
            }),
            worker: Some(worker),
        })
    }

    /// Return the current registered lifecycle state, or `None` after wait
    /// has reaped the process.
    pub fn state(&self, pid: VirtualPid) -> Option<ProcessState> {
        lock(&self.registry.records)
            .get(&pid)
            .map(|record| lock(&record.state).clone())
    }

    fn allocate_pid(&self) -> Result<VirtualPid, ProcessError> {
        self.registry
            .next_pid
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pid| {
                pid.checked_add(1)
            })
            .map(VirtualPid)
            .map_err(|_| ProcessError::VirtualPidExhausted)
    }
}

/// Handle for one registered VM-backed process.
pub struct VmProcess {
    pid: VirtualPid,
    registry: Arc<ProcessRegistry>,
    record: Arc<ProcessRecord>,
    stdout: Option<ProcessStdout>,
    worker: Option<JoinHandle<()>>,
}

impl fmt::Debug for VmProcess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VmProcess")
            .field("pid", &self.pid)
            .field("state", &lock(&self.record.state))
            .finish_non_exhaustive()
    }
}

impl VmProcess {
    /// The host-owned PID allocated for this process.
    pub fn pid(&self) -> VirtualPid {
        self.pid
    }

    /// Return the process's current lifecycle state.
    pub fn state(&self) -> ProcessState {
        lock(&self.record.state).clone()
    }

    /// Take the bounded stdout stream. The caller must drain it while the
    /// process runs to avoid intentionally applying backpressure forever.
    pub fn take_stdout(&mut self) -> Result<ProcessStdout, ProcessError> {
        self.stdout
            .take()
            .ok_or(ProcessError::StdoutAlreadyTaken { pid: self.pid })
    }

    /// Request cooperative cancellation, wait up to `grace`, then use
    /// `InterruptHandle::kill()` as the hard-stop path.
    pub fn cancel(&self, grace: Duration) -> Result<CancelOutcome, ProcessError> {
        {
            let mut state = lock(&self.record.state);
            if state.terminal_status().is_some() {
                return Ok(CancelOutcome::AlreadyExited);
            }
            self.record
                .cancellation_requested
                .store(true, Ordering::Release);
            *state = ProcessState::CancellationRequested;
            self.record.changed.notify_all();
        }

        let deadline = Instant::now() + grace;
        let mut state = lock(&self.record.state);
        while state.terminal_status().is_none() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let (next, _) = wait_timeout(&self.record.changed, state, deadline - now);
            state = next;
        }
        if state.terminal_status().is_some() {
            return Ok(CancelOutcome::Cooperative);
        }
        drop(state);

        let interrupt = lock(&self.record.interrupt).clone();
        let Some(interrupt) = interrupt else {
            return Err(ProcessError::HardStopFailed { pid: self.pid });
        };
        if !interrupt.kill() {
            let state = lock(&self.record.state);
            return if state.terminal_status().is_some() {
                Ok(CancelOutcome::Cooperative)
            } else {
                Err(ProcessError::HardStopFailed { pid: self.pid })
            };
        }
        self.record.hard_killed.store(true, Ordering::Release);
        Ok(CancelOutcome::HardKilled)
    }

    /// Wait, discard any stdout not taken by the caller, and reap the process.
    pub fn wait(mut self) -> Result<ProcessExitStatus, ProcessError> {
        if let Some(stdout) = self.stdout.take() {
            stdout.read_to_end();
        }
        self.finish_wait()
    }

    /// Drain stdout through EOF, wait, and reap the process.
    pub fn wait_with_output(mut self) -> Result<(ProcessExitStatus, Vec<u8>), ProcessError> {
        let stdout = self
            .stdout
            .take()
            .ok_or(ProcessError::StdoutAlreadyTaken { pid: self.pid })?;
        let output = stdout.read_to_end();
        let status = self.finish_wait()?;
        Ok((status, output))
    }

    fn finish_wait(&mut self) -> Result<ProcessExitStatus, ProcessError> {
        let mut state = lock(&self.record.state);
        while state.terminal_status().is_none() {
            state = wait(&self.record.changed, state);
        }
        let status = state
            .terminal_status()
            .ok_or(ProcessError::MissingExitStatus { pid: self.pid })?;
        drop(state);
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            return Err(ProcessError::WorkerPanicked { pid: self.pid });
        }
        lock(&self.registry.records).remove(&self.pid);
        Ok(status)
    }
}

fn run_process<Configure, Work>(
    source: TrustedProcessSnapshot,
    configure: Configure,
    work: Work,
    context: ProcessContext,
    stdout_sender: SyncSender<Vec<u8>>,
    record: &ProcessRecord,
) where
    Configure: FnOnce(SandboxBuilder) -> SandboxBuilder,
    Work: FnOnce(&mut AppSandbox, &ProcessContext) -> Result<i32, Error>,
{
    let cancelled = Arc::clone(&context.cancellation_requested);
    let stdout_handler = move |bytes: &[u8]| {
        stdout_sender.send(bytes.to_vec()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "process stdout reader dropped",
            )
        })
    };
    let builder = configure(SandboxBuilder::from_snapshot(source.snapshot))
        .host_function(CANCEL_HOST_FUNCTION, move |_| {
            Ok(cancelled.load(Ordering::Acquire).to_string())
        })
        .stdout_handler(stdout_handler);
    let mut sandbox = match builder.boot() {
        Ok(sandbox) => sandbox,
        Err(error) => {
            record.set_state(ProcessState::Poisoned(ProcessExitStatus::Failed(
                error.to_string(),
            )));
            return;
        }
    };

    *lock(&record.interrupt) = Some(sandbox.interrupt_handle());
    if record.cancellation_requested.load(Ordering::Acquire) {
        record.set_state(ProcessState::CancellationRequested);
    } else {
        record.set_state(ProcessState::Running);
    }

    let result = work(&mut sandbox, &context);
    drop(sandbox);
    *lock(&record.interrupt) = None;

    let state = if record.hard_killed.load(Ordering::Acquire) {
        ProcessState::Poisoned(ProcessExitStatus::Cancelled)
    } else {
        match result {
            Ok(status) => ProcessState::Exited(ProcessExitStatus::Exited(status)),
            Err(_) if record.cancellation_requested.load(Ordering::Acquire) => {
                ProcessState::Poisoned(ProcessExitStatus::Cancelled)
            }
            Err(error) => ProcessState::Poisoned(ProcessExitStatus::Failed(error.to_string())),
        }
    };
    record.set_state(state);
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait_timeout<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    timeout: Duration,
) -> (MutexGuard<'a, T>, std::sync::WaitTimeoutResult) {
    condvar
        .wait_timeout(guard, timeout)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_process_fork_requires_one_application_thread() {
        let result =
            TrustedProcessSnapshot::validate_kind(ProcessSnapshotKind::RunningProcessFork {
                live_application_threads: 2,
            });

        assert!(matches!(
            result,
            Err(ProcessError::UnsupportedApplicationThreadCount {
                live_application_threads: 2
            })
        ));
    }

    #[test]
    fn unsupported_capability_inheritance_fails_closed() {
        let options = ProcessOptions::default().capabilities([InheritedCapability::new(
            "workspace",
            CapabilityDisposition::ShareOpenDescription,
        )]);

        assert!(matches!(
            options.validate(),
            Err(ProcessError::UnsupportedCapabilityInheritance {
                capability,
                disposition: CapabilityDisposition::ShareOpenDescription,
            }) if capability == "workspace"
        ));
    }

    #[test]
    fn close_in_child_capability_is_supported() {
        let options = ProcessOptions::default().capabilities([InheritedCapability::new(
            "control-channel",
            CapabilityDisposition::CloseInChild,
        )]);

        assert!(options.validate().is_ok());
    }

    #[test]
    fn bounded_stdout_applies_backpressure_and_reports_eof() {
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(vec![1]).expect("first chunk fits");
        let writer = thread::spawn(move || {
            sender.send(vec![2]).expect("reader remains connected");
        });

        assert_eq!(receiver.recv().expect("first chunk"), vec![1]);
        assert_eq!(receiver.recv().expect("second chunk"), vec![2]);
        writer.join().expect("writer");
        assert_eq!(receiver.recv(), Err(mpsc::RecvError));
    }
}
