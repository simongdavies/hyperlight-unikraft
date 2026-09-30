// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{
    Error, ExecutionProfile, RequestEnvelope, Result, WorkerVersionId, WorkerVersionSandbox,
    sandbox::RestoredWorkerVersionSandbox,
};
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type Completion = Box<dyn FnOnce(RequestExecution) + Send + 'static>;

struct RequestJob {
    request: RequestEnvelope,
    timeout: Duration,
    completion: Completion,
    admitted_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerPoolRestoreMode {
    OnDemand,
    Prewarmed {
        sandboxes: usize,
        max_concurrent_restores: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolSubmitError {
    Full,
    ShuttingDown,
    Unavailable,
}

impl std::fmt::Display for PoolSubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => formatter.write_str("request queue is full"),
            Self::ShuttingDown => formatter.write_str("request pool is shutting down"),
            Self::Unavailable => formatter.write_str("request pool is unavailable"),
        }
    }
}

pub struct RequestExecution {
    pub request_id: String,
    pub result: Result<super::ResponseEnvelope>,
    pub profile: ExecutionProfile,
    pub submit_error: Option<PoolSubmitError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPoolStatus {
    pub active: usize,
    pub queued: usize,
    pub queue_capacity: usize,
    pub ready: usize,
    pub replenishing: usize,
}

struct QueueState {
    jobs: VecDeque<RequestJob>,
    terminal_error: Option<PoolSubmitError>,
}

struct RequestQueue {
    state: Mutex<QueueState>,
    available: Condvar,
    capacity: usize,
}

impl RequestQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(QueueState {
                jobs: VecDeque::with_capacity(capacity),
                terminal_error: None,
            }),
            available: Condvar::new(),
            capacity,
        }
    }

    fn try_push(
        &self,
        job: RequestJob,
    ) -> std::result::Result<(), (PoolSubmitError, Box<RequestJob>)> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(error) = state.terminal_error {
            return Err((error, Box::new(job)));
        }
        if state.jobs.len() == self.capacity {
            return Err((PoolSubmitError::Full, Box::new(job)));
        }
        state.jobs.push_back(job);
        self.available.notify_one();
        Ok(())
    }

    fn pop(&self) -> Option<RequestJob> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if let Some(job) = state.jobs.pop_front() {
                return Some(job);
            }
            if state.terminal_error.is_some() {
                return None;
            }
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .terminal_error
            .get_or_insert(PoolSubmitError::ShuttingDown);
        self.available.notify_all();
    }

    fn shutdown(&self) -> Vec<RequestJob> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .terminal_error
            .get_or_insert(PoolSubmitError::ShuttingDown);
        let jobs = state.jobs.drain(..).collect();
        self.available.notify_all();
        jobs
    }

    fn is_closed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal_error
            .is_some()
    }

    fn terminal_error(&self) -> Option<PoolSubmitError> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal_error
    }

    fn fail(&self) -> Vec<RequestJob> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.terminal_error = Some(PoolSubmitError::Unavailable);
        let jobs = state.jobs.drain(..).collect();
        self.available.notify_all();
        jobs
    }

    fn wait_for_retry_or_close(&self, duration: Duration) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.terminal_error.is_some() {
            return true;
        }
        let (state, _) = self
            .available
            .wait_timeout(state, duration)
            .unwrap_or_else(|error| error.into_inner());
        state.terminal_error.is_some()
    }
}

struct ExecutionSlots {
    state: Mutex<ExecutionSlotState>,
    available: Condvar,
}

struct ExecutionSlotState {
    in_use: usize,
    capacity: usize,
    closed: bool,
}

struct PrewarmedWorkerState {
    queue: Arc<RequestQueue>,
    active: Arc<AtomicUsize>,
    admitted: Arc<AtomicUsize>,
    ready: Arc<AtomicUsize>,
    replenishing: Arc<AtomicUsize>,
    execution_slots: Arc<ExecutionSlots>,
    restore_slots: Arc<ExecutionSlots>,
    owner_health: Arc<OwnerHealth>,
}

struct OwnerHealth {
    remaining: AtomicUsize,
}

impl ExecutionSlots {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(ExecutionSlotState {
                in_use: 0,
                capacity,
                closed: false,
            }),
            available: Condvar::new(),
        }
    }

    fn acquire(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.in_use == state.capacity && !state.closed {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        if state.closed {
            return false;
        }
        state.in_use += 1;
        true
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.in_use -= 1;
        self.available.notify_one();
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        self.available.notify_all();
    }
}

/// Fixed, thread-affine request executors for one immutable Worker version.
///
/// Each worker creates, executes and drops every request VM on its own thread.
/// Only the immutable verified snapshot is cloned across workers. The watchdog
/// may signal an executing vCPU through Hyperlight's `InterruptHandle`, but all
/// normal VM lifecycle operations remain on the owning worker.
pub struct WorkerRequestPool {
    queue: Arc<RequestQueue>,
    active: Arc<AtomicUsize>,
    admitted: Arc<AtomicUsize>,
    admission_capacity: usize,
    queue_capacity: usize,
    ready: Arc<AtomicUsize>,
    replenishing: Arc<AtomicUsize>,
    execution_slots: Arc<ExecutionSlots>,
    restore_slots: Arc<ExecutionSlots>,
    workers: Vec<JoinHandle<()>>,
}

impl WorkerRequestPool {
    pub fn new(
        worker: WorkerVersionSandbox,
        max_concurrent_sandboxes: usize,
        queue_capacity: usize,
    ) -> Result<Self> {
        Self::with_restore_mode(
            worker,
            max_concurrent_sandboxes,
            queue_capacity,
            WorkerPoolRestoreMode::OnDemand,
        )
    }

    pub fn with_restore_mode(
        worker: WorkerVersionSandbox,
        max_concurrent_sandboxes: usize,
        queue_capacity: usize,
        restore_mode: WorkerPoolRestoreMode,
    ) -> Result<Self> {
        validate_configuration(max_concurrent_sandboxes, queue_capacity, restore_mode)?;
        let admission_capacity = max_concurrent_sandboxes
            .checked_add(queue_capacity)
            .ok_or_else(|| Error::State("request pool capacity is too large".into()))?;
        let queue = Arc::new(RequestQueue::new(admission_capacity));
        let active = Arc::new(AtomicUsize::new(0));
        let admitted = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicUsize::new(0));
        let replenishing = Arc::new(AtomicUsize::new(0));
        let execution_slots = Arc::new(ExecutionSlots::new(max_concurrent_sandboxes));
        let version = worker.worker_version().clone();
        let owner_count = match restore_mode {
            WorkerPoolRestoreMode::OnDemand => max_concurrent_sandboxes,
            WorkerPoolRestoreMode::Prewarmed { sandboxes, .. } => sandboxes,
        };
        let restore_slots = Arc::new(ExecutionSlots::new(match restore_mode {
            WorkerPoolRestoreMode::OnDemand => max_concurrent_sandboxes,
            WorkerPoolRestoreMode::Prewarmed {
                max_concurrent_restores,
                ..
            } => max_concurrent_restores,
        }));
        let owner_health = Arc::new(OwnerHealth {
            remaining: AtomicUsize::new(owner_count),
        });
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(owner_count);
        for index in 0..owner_count {
            let worker_queue = queue.clone();
            let worker_active = active.clone();
            let worker_admitted = admitted.clone();
            let worker_ready = ready.clone();
            let worker_replenishing = replenishing.clone();
            let worker_execution_slots = execution_slots.clone();
            let worker_restore_slots = restore_slots.clone();
            let worker_owner_health = owner_health.clone();
            let worker_sandbox = worker.clone();
            let worker_version = version.clone();
            let handle = match thread::Builder::new()
                .name(format!("workerd-sandbox-{index}"))
                .spawn(move || match restore_mode {
                    WorkerPoolRestoreMode::OnDemand => run_on_demand_worker(
                        worker_queue,
                        worker_active,
                        worker_admitted,
                        worker_sandbox,
                        worker_version,
                    ),
                    WorkerPoolRestoreMode::Prewarmed { .. } => run_prewarmed_worker(
                        PrewarmedWorkerState {
                            queue: worker_queue,
                            active: worker_active,
                            admitted: worker_admitted,
                            ready: worker_ready,
                            replenishing: worker_replenishing,
                            execution_slots: worker_execution_slots,
                            restore_slots: worker_restore_slots,
                            owner_health: worker_owner_health,
                        },
                        worker_sandbox,
                    ),
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    queue.close();
                    execution_slots.close();
                    restore_slots.close();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(error.into());
                }
            };
            workers.push(handle);
        }
        Ok(Self {
            queue,
            active,
            admitted,
            admission_capacity,
            queue_capacity,
            ready,
            replenishing,
            execution_slots,
            restore_slots,
            workers,
        })
    }

    /// Admit one request without waiting for queue space.
    ///
    /// On rejection the completion runs synchronously on the submitting thread,
    /// allowing a listener to return an immediate HTTP 503 while retaining
    /// ownership of its connection.
    pub fn try_submit(
        &self,
        request: RequestEnvelope,
        timeout: Duration,
        completion: impl FnOnce(RequestExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        let request_id = request.request_id.clone();
        if self
            .admitted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |admitted| {
                (admitted < self.admission_capacity).then_some(admitted + 1)
            })
            .is_err()
        {
            let error = self.queue.terminal_error().unwrap_or(PoolSubmitError::Full);
            completion(RequestExecution {
                request_id,
                result: Err(Error::State(error.to_string())),
                profile: ExecutionProfile::default(),
                submit_error: Some(error),
            });
            return Err(error);
        }
        let job = RequestJob {
            request,
            timeout,
            completion: Box::new(completion),
            admitted_at: Instant::now(),
        };
        match self.queue.try_push(job) {
            Ok(()) => Ok(()),
            Err((error, job)) => {
                self.admitted.fetch_sub(1, Ordering::AcqRel);
                (job.completion)(RequestExecution {
                    request_id,
                    result: Err(Error::State(error.to_string())),
                    profile: ExecutionProfile::default(),
                    submit_error: Some(error),
                });
                Err(error)
            }
        }
    }

    pub fn status(&self) -> WorkerPoolStatus {
        let active = self.active.load(Ordering::Acquire);
        let admitted = self.admitted.load(Ordering::Acquire);
        WorkerPoolStatus {
            active,
            queued: admitted.saturating_sub(active).min(self.queue_capacity),
            queue_capacity: self.queue_capacity,
            ready: self.ready.load(Ordering::Acquire),
            replenishing: self.replenishing.load(Ordering::Acquire),
        }
    }
}

impl Drop for WorkerRequestPool {
    fn drop(&mut self) {
        complete_jobs(
            self.queue.shutdown(),
            &self.admitted,
            PoolSubmitError::ShuttingDown,
        );
        self.execution_slots.close();
        self.restore_slots.close();
        for worker in self.workers.drain(..) {
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}

fn run_on_demand_worker(
    queue: Arc<RequestQueue>,
    active: Arc<AtomicUsize>,
    admitted: Arc<AtomicUsize>,
    worker: WorkerVersionSandbox,
    version: WorkerVersionId,
) {
    while let Some(job) = queue.pop() {
        let request_id = job.request.request_id.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let execution = catch_unwind(AssertUnwindSafe(|| {
            let mut execution = worker.execute_profiled(&version, job.request, job.timeout);
            execution.1.ready_wait_ms = elapsed_ms(job.admitted_at);
            execution
        }));
        active.fetch_sub(1, Ordering::AcqRel);
        admitted.fetch_sub(1, Ordering::AcqRel);
        let (result, profile) = match execution {
            Ok(execution) => execution,
            Err(_) => (
                Err(Error::State("request worker panicked".into())),
                ExecutionProfile::default(),
            ),
        };
        let _ = catch_unwind(AssertUnwindSafe(|| {
            (job.completion)(RequestExecution {
                request_id,
                result,
                profile,
                submit_error: None,
            });
        }));
    }
}

fn run_prewarmed_worker(state: PrewarmedWorkerState, worker: WorkerVersionSandbox) {
    const RESTORE_RETRY_DELAYS: [Duration; 3] = [
        Duration::from_millis(10),
        Duration::from_millis(25),
        Duration::from_millis(50),
    ];
    let mut consecutive_failures = 0usize;
    loop {
        if state.queue.is_closed() {
            return;
        }
        let restore_wait_started = Instant::now();
        if !state.restore_slots.acquire() {
            return;
        }
        let restore_wait_ms = elapsed_ms(restore_wait_started);
        state.replenishing.fetch_add(1, Ordering::AcqRel);
        let restored = catch_unwind(AssertUnwindSafe(|| worker.restore()));
        state.replenishing.fetch_sub(1, Ordering::AcqRel);
        state.restore_slots.release();
        let (restored, restore_ms) = match restored {
            Ok(Ok(restored)) => restored,
            Ok(Err(error)) => {
                tracing::warn!(%error, "workerd sandbox replenishment failed");
                if retry_restore(
                    &state.queue,
                    &state.admitted,
                    &state.owner_health,
                    &mut consecutive_failures,
                    &RESTORE_RETRY_DELAYS,
                ) {
                    return;
                }
                continue;
            }
            Err(_) => {
                tracing::warn!("workerd sandbox replenishment panicked");
                if retry_restore(
                    &state.queue,
                    &state.admitted,
                    &state.owner_health,
                    &mut consecutive_failures,
                    &RESTORE_RETRY_DELAYS,
                ) {
                    return;
                }
                continue;
            }
        };
        consecutive_failures = 0;
        state.ready.fetch_add(1, Ordering::AcqRel);
        if !state.execution_slots.acquire() {
            state.ready.fetch_sub(1, Ordering::AcqRel);
            drop(restored);
            return;
        }
        let Some(job) = state.queue.pop() else {
            state.execution_slots.release();
            state.ready.fetch_sub(1, Ordering::AcqRel);
            drop(restored);
            return;
        };
        state.ready.fetch_sub(1, Ordering::AcqRel);
        execute_prewarmed_job(
            job,
            restored,
            restore_wait_ms,
            restore_ms,
            &state.active,
            &state.admitted,
        );
        state.execution_slots.release();
    }
}

fn retry_restore(
    queue: &RequestQueue,
    admitted: &AtomicUsize,
    owner_health: &OwnerHealth,
    consecutive_failures: &mut usize,
    delays: &[Duration],
) -> bool {
    if *consecutive_failures == delays.len() {
        mark_owner_failed(queue, admitted, owner_health);
        return true;
    }
    let delay = delays[*consecutive_failures];
    *consecutive_failures += 1;
    queue.wait_for_retry_or_close(delay)
}

fn mark_owner_failed(queue: &RequestQueue, admitted: &AtomicUsize, owner_health: &OwnerHealth) {
    if owner_health.remaining.fetch_sub(1, Ordering::AcqRel) != 1 {
        return;
    }
    complete_jobs(queue.fail(), admitted, PoolSubmitError::Unavailable);
}

fn complete_jobs(jobs: Vec<RequestJob>, admitted: &AtomicUsize, error: PoolSubmitError) {
    for job in jobs {
        admitted.fetch_sub(1, Ordering::AcqRel);
        let request_id = job.request.request_id.clone();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            (job.completion)(RequestExecution {
                request_id,
                result: Err(Error::State(error.to_string())),
                profile: ExecutionProfile::default(),
                submit_error: Some(error),
            });
        }));
    }
}

fn execute_prewarmed_job(
    job: RequestJob,
    restored: RestoredWorkerVersionSandbox,
    restore_wait_ms: f64,
    restore_ms: f64,
    active: &AtomicUsize,
    admitted: &AtomicUsize,
) {
    let request_id = job.request.request_id.clone();
    active.fetch_add(1, Ordering::AcqRel);
    let execution = catch_unwind(AssertUnwindSafe(|| {
        let total_started = Instant::now();
        let mut execution = restored.execute_profiled(job.request, job.timeout, total_started);
        execution.1.ready_wait_ms = elapsed_ms(job.admitted_at);
        execution.1.replenishment_wait_ms = restore_wait_ms;
        execution.1.replenishment_restore_ms = restore_ms;
        execution
    }));
    active.fetch_sub(1, Ordering::AcqRel);
    admitted.fetch_sub(1, Ordering::AcqRel);
    let (result, profile) = match execution {
        Ok(execution) => execution,
        Err(_) => (
            Err(Error::State("request worker panicked".into())),
            ExecutionProfile {
                ready_wait_ms: elapsed_ms(job.admitted_at),
                replenishment_wait_ms: restore_wait_ms,
                replenishment_restore_ms: restore_ms,
                ..ExecutionProfile::default()
            },
        ),
    };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        (job.completion)(RequestExecution {
            request_id,
            result,
            profile,
            submit_error: None,
        });
    }));
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn validate_configuration(
    max_concurrent_sandboxes: usize,
    queue_capacity: usize,
    restore_mode: WorkerPoolRestoreMode,
) -> Result<()> {
    if max_concurrent_sandboxes == 0 {
        return Err(Error::State(
            "max concurrent sandboxes must be nonzero".into(),
        ));
    }
    if queue_capacity == 0 {
        return Err(Error::State(
            "request queue capacity must be nonzero".into(),
        ));
    }
    if matches!(
        restore_mode,
        WorkerPoolRestoreMode::Prewarmed { sandboxes: 0, .. }
    ) {
        return Err(Error::State(
            "prewarmed sandbox count must be nonzero".into(),
        ));
    }
    if matches!(
        restore_mode,
        WorkerPoolRestoreMode::Prewarmed {
            max_concurrent_restores: 0,
            ..
        }
    ) {
        return Err(Error::State(
            "max concurrent restores must be nonzero".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_is_bounded_fifo_and_drains_on_close() {
        let queue = RequestQueue::new(2);
        let job = |id: &str| RequestJob {
            request: RequestEnvelope {
                protocol_version: super::super::PROTOCOL_VERSION,
                request_id: id.into(),
                method: "GET".into(),
                url: "https://example.test/".into(),
                headers: vec![],
                body_base64: String::new(),
            },
            timeout: Duration::from_secs(1),
            completion: Box::new(|_| {}),
            admitted_at: Instant::now(),
        };
        assert!(queue.try_push(job("one")).is_ok());
        assert!(queue.try_push(job("two")).is_ok());
        assert_eq!(
            queue.try_push(job("overflow")).unwrap_err().0,
            PoolSubmitError::Full
        );
        queue.close();
        assert_eq!(queue.pop().unwrap().request.request_id, "one");
        assert_eq!(queue.pop().unwrap().request.request_id, "two");
        assert!(queue.pop().is_none());
        assert_eq!(
            queue.try_push(job("closed")).unwrap_err().0,
            PoolSubmitError::ShuttingDown
        );
    }

    #[test]
    fn permanent_restore_failure_drains_admission_and_rejects_new_jobs() {
        let queue = RequestQueue::new(3);
        let admitted = AtomicUsize::new(2);
        let owner_health = OwnerHealth {
            remaining: AtomicUsize::new(1),
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let job = |id: &str| {
            let tx = tx.clone();
            RequestJob {
                request: RequestEnvelope {
                    protocol_version: super::super::PROTOCOL_VERSION,
                    request_id: id.into(),
                    method: "GET".into(),
                    url: "https://example.test/".into(),
                    headers: vec![],
                    body_base64: String::new(),
                },
                timeout: Duration::from_secs(1),
                completion: Box::new(move |execution| {
                    tx.send(execution.submit_error).unwrap();
                }),
                admitted_at: Instant::now(),
            }
        };
        assert!(queue.try_push(job("one")).is_ok());
        assert!(queue.try_push(job("two")).is_ok());

        let mut failures = 0;
        assert!(retry_restore(
            &queue,
            &admitted,
            &owner_health,
            &mut failures,
            &[]
        ));

        assert_eq!(admitted.load(Ordering::Acquire), 0);
        assert_eq!(rx.recv().unwrap(), Some(PoolSubmitError::Unavailable));
        assert_eq!(rx.recv().unwrap(), Some(PoolSubmitError::Unavailable));
        assert_eq!(
            queue.try_push(job("rejected")).unwrap_err().0,
            PoolSubmitError::Unavailable
        );
        assert!(queue.pop().is_none());
    }

    #[test]
    fn shutdown_drains_queued_jobs_and_releases_admission() {
        let queue = RequestQueue::new(3);
        let admitted = AtomicUsize::new(2);
        let (tx, rx) = std::sync::mpsc::channel();
        let job = |id: &str| {
            let tx = tx.clone();
            RequestJob {
                request: RequestEnvelope {
                    protocol_version: super::super::PROTOCOL_VERSION,
                    request_id: id.into(),
                    method: "GET".into(),
                    url: "https://example.test/".into(),
                    headers: vec![],
                    body_base64: String::new(),
                },
                timeout: Duration::from_secs(1),
                completion: Box::new(move |execution| {
                    tx.send(execution.submit_error).unwrap();
                }),
                admitted_at: Instant::now(),
            }
        };
        assert!(queue.try_push(job("one")).is_ok());
        assert!(queue.try_push(job("two")).is_ok());

        complete_jobs(queue.shutdown(), &admitted, PoolSubmitError::ShuttingDown);

        assert_eq!(admitted.load(Ordering::Acquire), 0);
        assert_eq!(rx.recv().unwrap(), Some(PoolSubmitError::ShuttingDown));
        assert_eq!(rx.recv().unwrap(), Some(PoolSubmitError::ShuttingDown));
        assert_eq!(
            queue.try_push(job("rejected")).unwrap_err().0,
            PoolSubmitError::ShuttingDown
        );
        assert!(queue.pop().is_none());
    }

    #[test]
    fn pool_configuration_rejects_zero_values_before_spawning() {
        assert!(validate_configuration(0, 1, WorkerPoolRestoreMode::OnDemand).is_err());
        assert!(validate_configuration(1, 0, WorkerPoolRestoreMode::OnDemand).is_err());
        assert!(validate_configuration(1, 1, WorkerPoolRestoreMode::OnDemand).is_ok());
        assert!(
            validate_configuration(
                1,
                1,
                WorkerPoolRestoreMode::Prewarmed {
                    sandboxes: 0,
                    max_concurrent_restores: 1,
                }
            )
            .is_err()
        );
        assert!(
            validate_configuration(
                1,
                1,
                WorkerPoolRestoreMode::Prewarmed {
                    sandboxes: 2,
                    max_concurrent_restores: 1,
                }
            )
            .is_ok()
        );
    }
}
