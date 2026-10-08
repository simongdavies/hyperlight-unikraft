// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{
    Error, ExecutionProfile, InvocationCancellation, InvocationRequest, InvocationResponse,
    RequestEnvelope, Result, WorkerVersionId, WorkerVersionSandbox,
    sandbox::RestoredWorkerVersionSandbox,
};
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type Completion = Box<dyn FnOnce(InvocationExecution) + Send + 'static>;

struct RequestJob {
    request: super::ingress::PoolInvocation,
    timeout: Duration,
    completion: Completion,
    admitted_at: Instant,
    ready_wait_started_at: Option<Instant>,
    diagnostic_wave_member: bool,
    cancellation: InvocationCancellation,
}

struct CompletedJob {
    completion: Completion,
    execution: InvocationExecution,
}

struct PrewarmExecutionTiming {
    ready_wait_ms: f64,
    admission_wait_ms: f64,
    ready_owner_wait_ms: f64,
    policy_wait_ms: f64,
    restore_wait_ms: f64,
    restore_ms: f64,
}

enum OwnerMessage {
    Execute(Box<RequestJob>),
    Shutdown,
}

#[derive(Clone, Copy)]
struct ReadyOwner {
    index: usize,
    ready_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrewarmPolicy {
    pub warm_floor: usize,
    pub ready_low_watermark: usize,
    pub ready_high_watermark: usize,
    pub max_replenish_batch: usize,
    pub diagnostic_no_refill_wave: Option<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerPoolRestoreMode {
    OnDemand,
    Prewarmed {
        sandboxes: usize,
        max_concurrent_restores: usize,
        policy: PrewarmPolicy,
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

pub struct InvocationExecution {
    pub request_id: String,
    pub result: Result<InvocationResponse>,
    pub profile: ExecutionProfile,
    pub submit_error: Option<PoolSubmitError>,
}

impl InvocationExecution {
    pub(super) fn into_fetch(self) -> RequestExecution {
        RequestExecution {
            request_id: self.request_id,
            result: self.result.and_then(InvocationResponse::into_fetch),
            profile: self.profile,
            submit_error: self.submit_error,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WorkerPoolStatus {
    pub admitted: usize,
    pub active: usize,
    pub queued: usize,
    pub queue_capacity: usize,
    pub execution_slots_in_use: usize,
    pub restore_slots_in_use: usize,
    pub recycle_queue_depth: usize,
    pub recycle_queue_peak: usize,
    pub teardown_in_flight: usize,
    pub teardown_peak: usize,
    pub completed_teardowns: usize,
    pub teardown_total_ms: f64,
    pub teardown_max_ms: f64,
    pub completion_queue_depth: usize,
    pub completion_queue_peak: usize,
    pub completion_in_flight: usize,
    pub completion_peak: usize,
    pub completed_completions: usize,
    pub completion_total_ms: f64,
    pub completion_max_ms: f64,
    pub prewarmed_inventory: usize,
    pub warm_floor: usize,
    pub ready_low_watermark: usize,
    pub ready_high_watermark: usize,
    pub max_replenish_batch: usize,
    pub replenishment_paused: bool,
    pub replenishment_pause_reason: Option<&'static str>,
    pub refill_active: bool,
    pub idle_owners: usize,
    pub restore_permits_outstanding: usize,
    pub diagnostic_wave_dispatched: usize,
    pub diagnostic_wave_completed: usize,
    pub ready: usize,
    pub ready_min: usize,
    pub ready_peak: usize,
    pub replenishing: usize,
    pub replenishing_min: usize,
    pub replenishing_peak: usize,
    pub restore_attempts: usize,
    pub completed_restores: usize,
    pub failed_restores: usize,
    pub completed_replenishment_policy_waits: usize,
    pub replenishment_policy_wait_total_ms: f64,
    pub replenishment_policy_wait_max_ms: f64,
    pub restore_wait_total_ms: f64,
    pub restore_wait_max_ms: f64,
    pub restore_total_ms: f64,
    pub restore_max_ms: f64,
    pub prewarmed_hits: usize,
    pub prewarmed_misses: usize,
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

struct PrewarmScheduler {
    state: Mutex<PrewarmSchedulerState>,
    available: Condvar,
    capacity: usize,
    max_active: usize,
    policy: PrewarmPolicy,
    metrics: Arc<PoolMetrics>,
    prewarmed_hits: Arc<AtomicUsize>,
    prewarmed_misses: Arc<AtomicUsize>,
}

struct PrewarmSchedulerState {
    jobs: VecDeque<RequestJob>,
    ready_owners: VecDeque<ReadyOwner>,
    active: usize,
    owners_remaining: usize,
    idle_owners: VecDeque<usize>,
    restore_granted: Vec<bool>,
    restore_permits_outstanding: usize,
    refill_active: bool,
    diagnostic_wave_started: bool,
    diagnostic_wave_dispatched: usize,
    diagnostic_wave_completed: usize,
    terminal_error: Option<PoolSubmitError>,
}

#[derive(Clone, Copy)]
struct SchedulerStatus {
    active: usize,
    queued: usize,
    ready: usize,
    replenishment_paused: bool,
    refill_active: bool,
    idle_owners: usize,
    restore_permits_outstanding: usize,
    diagnostic_wave_dispatched: usize,
    diagnostic_wave_completed: usize,
}

impl PrewarmScheduler {
    fn new(
        capacity: usize,
        max_active: usize,
        owner_count: usize,
        policy: PrewarmPolicy,
        metrics: Arc<PoolMetrics>,
        prewarmed_hits: Arc<AtomicUsize>,
        prewarmed_misses: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            state: Mutex::new(PrewarmSchedulerState {
                jobs: VecDeque::with_capacity(capacity),
                ready_owners: VecDeque::with_capacity(owner_count),
                active: 0,
                owners_remaining: owner_count,
                idle_owners: VecDeque::with_capacity(owner_count),
                restore_granted: vec![false; owner_count],
                restore_permits_outstanding: 0,
                refill_active: false,
                diagnostic_wave_started: false,
                diagnostic_wave_dispatched: 0,
                diagnostic_wave_completed: 0,
                terminal_error: None,
            }),
            available: Condvar::new(),
            capacity,
            max_active,
            policy,
            metrics,
            prewarmed_hits,
            prewarmed_misses,
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
        self.available.notify_all();
        Ok(())
    }

    fn publish_ready(&self, owner: ReadyOwner, used_replenishment_permit: bool) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.terminal_error.is_some() {
            return false;
        }
        if used_replenishment_permit {
            debug_assert!(state.restore_permits_outstanding != 0);
            state.restore_permits_outstanding -= 1;
        }
        state.ready_owners.push_back(owner);
        record_observation(
            state.ready_owners.len(),
            &self.metrics.ready_min,
            &self.metrics.ready_peak,
        );
        self.schedule_replenishment(&mut state);
        self.available.notify_all();
        true
    }

    fn next_dispatch(&self) -> Option<(ReadyOwner, RequestJob)> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if state.terminal_error.is_some() {
                return None;
            }
            if state.active < self.max_active && !state.jobs.is_empty() {
                let now = Instant::now();
                let job = state.jobs.front_mut().unwrap();
                job.ready_wait_started_at.get_or_insert(now);
            }
            if state.active < self.max_active
                && !state.jobs.is_empty()
                && state.ready_owners.len() > self.policy.warm_floor
            {
                let owner = state.ready_owners.pop_front().unwrap();
                record_observation(
                    state.ready_owners.len(),
                    &self.metrics.ready_min,
                    &self.metrics.ready_peak,
                );
                let mut job = state.jobs.pop_front().unwrap();
                state.active += 1;
                if let Some(wave_size) = self.policy.diagnostic_no_refill_wave {
                    state.diagnostic_wave_started = true;
                    if state.diagnostic_wave_dispatched < wave_size {
                        state.diagnostic_wave_dispatched += 1;
                        job.diagnostic_wave_member = true;
                    }
                }
                self.schedule_replenishment(&mut state);
                record_prewarmed_pairing(
                    owner.ready_at,
                    job.admitted_at,
                    &self.prewarmed_hits,
                    &self.prewarmed_misses,
                );
                return Some((owner, job));
            }
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn finish_execution(&self, owner: usize, diagnostic_wave_member: bool) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.active -= 1;
        state.idle_owners.push_back(owner);
        if diagnostic_wave_member {
            state.diagnostic_wave_completed += 1;
        }
        self.schedule_replenishment(&mut state);
        self.available.notify_all();
    }

    fn finish_failed_dispatch(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.active -= 1;
        self.available.notify_all();
    }

    fn wait_for_replenishment(&self, owner: usize) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if state.terminal_error.is_some() {
                return false;
            }
            if state.restore_granted[owner] {
                state.restore_granted[owner] = false;
                return true;
            }
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn schedule_replenishment(&self, state: &mut PrewarmSchedulerState) {
        let paused = self.replenishment_paused(state);
        let ready_supply = state.ready_owners.len() + state.restore_permits_outstanding;
        let total_inventory = ready_supply + state.active;
        let floor_deficit = self.policy.warm_floor.saturating_sub(total_inventory);
        self.grant_replenishment(state, floor_deficit);
        if paused {
            return;
        }
        let ready_supply = state.ready_owners.len() + state.restore_permits_outstanding;
        if ready_supply < self.policy.ready_low_watermark {
            state.refill_active = true;
        }
        if ready_supply >= self.policy.ready_high_watermark {
            state.refill_active = false;
        }
        if state.refill_active
            && state.restore_permits_outstanding < self.policy.max_replenish_batch
        {
            let refill = self
                .policy
                .ready_high_watermark
                .saturating_sub(ready_supply)
                .min(
                    self.policy
                        .max_replenish_batch
                        .saturating_sub(state.restore_permits_outstanding),
                );
            self.grant_replenishment(state, refill);
        }
    }

    fn grant_replenishment(&self, state: &mut PrewarmSchedulerState, count: usize) {
        for _ in 0..count {
            let Some(owner) = state.idle_owners.pop_front() else {
                break;
            };
            state.restore_granted[owner] = true;
            state.restore_permits_outstanding += 1;
        }
    }

    fn replenishment_paused(&self, state: &PrewarmSchedulerState) -> bool {
        self.policy.diagnostic_no_refill_wave.is_some()
            && state.diagnostic_wave_started
            && state.diagnostic_wave_completed
                < self.policy.diagnostic_no_refill_wave.unwrap_or_default()
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

    fn owner_failed(&self, used_replenishment_permit: bool) -> Vec<RequestJob> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if used_replenishment_permit {
            debug_assert!(state.restore_permits_outstanding != 0);
            state.restore_permits_outstanding -= 1;
        }
        state.owners_remaining -= 1;
        if state.owners_remaining != 0 {
            self.schedule_replenishment(&mut state);
            self.available.notify_all();
            return Vec::new();
        }
        state.terminal_error = Some(PoolSubmitError::Unavailable);
        let jobs = state.jobs.drain(..).collect();
        self.available.notify_all();
        jobs
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

    fn terminal_error(&self) -> Option<PoolSubmitError> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal_error
    }

    fn status(&self) -> SchedulerStatus {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        SchedulerStatus {
            active: state.active,
            queued: state.jobs.len(),
            ready: state.ready_owners.len(),
            replenishment_paused: self.replenishment_paused(&state),
            refill_active: state.refill_active,
            idle_owners: state.idle_owners.len(),
            restore_permits_outstanding: state.restore_permits_outstanding,
            diagnostic_wave_dispatched: state.diagnostic_wave_dispatched,
            diagnostic_wave_completed: state.diagnostic_wave_completed,
        }
    }
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

    fn terminal_error(&self) -> Option<PoolSubmitError> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal_error
    }

    fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .jobs
            .len()
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

struct PoolMetrics {
    completion_queue_depth: AtomicUsize,
    completion_queue_peak: AtomicUsize,
    completion_in_flight: AtomicUsize,
    completion_peak: AtomicUsize,
    completed_completions: AtomicUsize,
    completion_total_ns: AtomicU64,
    completion_max_ns: AtomicU64,
    recycle_queue_depth: AtomicUsize,
    recycle_queue_peak: AtomicUsize,
    teardown_in_flight: AtomicUsize,
    teardown_peak: AtomicUsize,
    completed_teardowns: AtomicUsize,
    teardown_total_ns: AtomicU64,
    teardown_max_ns: AtomicU64,
    ready_min: AtomicUsize,
    ready_peak: AtomicUsize,
    replenishing_min: AtomicUsize,
    replenishing_peak: AtomicUsize,
    restore_attempts: AtomicUsize,
    completed_restores: AtomicUsize,
    failed_restores: AtomicUsize,
    completed_replenishment_policy_waits: AtomicUsize,
    replenishment_policy_wait_total_ns: AtomicU64,
    replenishment_policy_wait_max_ns: AtomicU64,
    restore_wait_total_ns: AtomicU64,
    restore_wait_max_ns: AtomicU64,
    restore_total_ns: AtomicU64,
    restore_max_ns: AtomicU64,
}

impl PoolMetrics {
    fn new() -> Self {
        Self {
            completion_queue_depth: AtomicUsize::new(0),
            completion_queue_peak: AtomicUsize::new(0),
            completion_in_flight: AtomicUsize::new(0),
            completion_peak: AtomicUsize::new(0),
            completed_completions: AtomicUsize::new(0),
            completion_total_ns: AtomicU64::new(0),
            completion_max_ns: AtomicU64::new(0),
            recycle_queue_depth: AtomicUsize::new(0),
            recycle_queue_peak: AtomicUsize::new(0),
            teardown_in_flight: AtomicUsize::new(0),
            teardown_peak: AtomicUsize::new(0),
            completed_teardowns: AtomicUsize::new(0),
            teardown_total_ns: AtomicU64::new(0),
            teardown_max_ns: AtomicU64::new(0),
            ready_min: AtomicUsize::new(usize::MAX),
            ready_peak: AtomicUsize::new(0),
            replenishing_min: AtomicUsize::new(usize::MAX),
            replenishing_peak: AtomicUsize::new(0),
            restore_attempts: AtomicUsize::new(0),
            completed_restores: AtomicUsize::new(0),
            failed_restores: AtomicUsize::new(0),
            completed_replenishment_policy_waits: AtomicUsize::new(0),
            replenishment_policy_wait_total_ns: AtomicU64::new(0),
            replenishment_policy_wait_max_ns: AtomicU64::new(0),
            restore_wait_total_ns: AtomicU64::new(0),
            restore_wait_max_ns: AtomicU64::new(0),
            restore_total_ns: AtomicU64::new(0),
            restore_max_ns: AtomicU64::new(0),
        }
    }
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
        self.acquire_with_wait_observer(|| {}, || {})
    }

    fn acquire_with_wait_observer(
        &self,
        wait_started: impl FnOnce(),
        wait_finished: impl FnOnce(),
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut wait_started = Some(wait_started);
        let mut wait_finished = Some(wait_finished);
        let mut waited = false;
        while state.in_use == state.capacity && !state.closed {
            if !waited {
                waited = true;
                wait_started.take().unwrap()();
            }
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        if waited {
            wait_finished.take().unwrap()();
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

    fn in_use(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .in_use
    }
}

/// Fixed, thread-affine request executors for one immutable Worker version.
///
/// Each worker creates, executes and drops every request VM on its own thread.
/// Only the immutable verified snapshot is cloned across workers. The watchdog
/// may signal an executing vCPU through Hyperlight's `InterruptHandle`, but all
/// normal VM lifecycle operations remain on the owning worker.
enum PoolDispatch {
    OnDemand(Arc<RequestQueue>),
    Prewarmed(Arc<PrewarmScheduler>),
}

pub struct WorkerRequestPool {
    dispatch: PoolDispatch,
    active: Arc<AtomicUsize>,
    admitted: Arc<AtomicUsize>,
    admission_capacity: usize,
    queue_capacity: usize,
    inventory: Arc<AtomicUsize>,
    replenishing: Arc<AtomicUsize>,
    prewarmed_hits: Arc<AtomicUsize>,
    prewarmed_misses: Arc<AtomicUsize>,
    prewarm_policy: Option<PrewarmPolicy>,
    restore_slots: Option<Arc<ExecutionSlots>>,
    metrics: Arc<PoolMetrics>,
    owner_senders: Vec<mpsc::Sender<OwnerMessage>>,
    dispatcher: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
    completion_sender: Option<mpsc::Sender<CompletedJob>>,
    completion_workers: Vec<JoinHandle<()>>,
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
        let admission_capacity = effective_concurrency(max_concurrent_sandboxes, restore_mode)
            .checked_add(queue_capacity)
            .ok_or_else(|| Error::State("request pool capacity is too large".into()))?;
        let active = Arc::new(AtomicUsize::new(0));
        let admitted = Arc::new(AtomicUsize::new(0));
        let inventory = Arc::new(AtomicUsize::new(0));
        let replenishing = Arc::new(AtomicUsize::new(0));
        let prewarmed_hits = Arc::new(AtomicUsize::new(0));
        let prewarmed_misses = Arc::new(AtomicUsize::new(0));
        let metrics = Arc::new(PoolMetrics::new());
        let version = worker.worker_version().clone();
        let owner_count = match restore_mode {
            WorkerPoolRestoreMode::OnDemand => max_concurrent_sandboxes,
            WorkerPoolRestoreMode::Prewarmed { sandboxes, .. } => sandboxes,
        };
        let completion_count = effective_concurrency(max_concurrent_sandboxes, restore_mode);
        let (completion_sender, completion_workers) =
            spawn_completion_workers(completion_count, metrics.clone(), admitted.clone())?;

        match restore_mode {
            WorkerPoolRestoreMode::OnDemand => {
                let queue = Arc::new(RequestQueue::new(admission_capacity));
                let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(owner_count);
                for index in 0..owner_count {
                    let worker_queue = queue.clone();
                    let worker_active = active.clone();
                    let worker_admitted = admitted.clone();
                    let worker_sandbox = worker.clone();
                    let worker_version = version.clone();
                    let worker_completion_sender = completion_sender.clone();
                    let worker_metrics = metrics.clone();
                    let handle = match thread::Builder::new()
                        .name(format!("workerd-sandbox-{index}"))
                        .spawn(move || {
                            run_on_demand_worker(
                                worker_queue,
                                worker_active,
                                worker_admitted,
                                worker_sandbox,
                                worker_version,
                                worker_completion_sender,
                                worker_metrics,
                            )
                        }) {
                        Ok(handle) => handle,
                        Err(error) => {
                            queue.close();
                            for worker in workers {
                                let _ = worker.join();
                            }
                            drop(completion_sender);
                            for worker in completion_workers {
                                let _ = worker.join();
                            }
                            return Err(error.into());
                        }
                    };
                    workers.push(handle);
                }
                Ok(Self {
                    dispatch: PoolDispatch::OnDemand(queue),
                    active,
                    admitted,
                    admission_capacity,
                    queue_capacity,
                    inventory,
                    replenishing,
                    prewarmed_hits,
                    prewarmed_misses,
                    prewarm_policy: None,
                    restore_slots: None,
                    metrics,
                    owner_senders: Vec::new(),
                    dispatcher: None,
                    workers,
                    completion_sender: Some(completion_sender),
                    completion_workers,
                })
            }
            WorkerPoolRestoreMode::Prewarmed {
                max_concurrent_restores,
                policy,
                ..
            } => {
                let scheduler = Arc::new(PrewarmScheduler::new(
                    admission_capacity,
                    max_concurrent_sandboxes,
                    owner_count,
                    policy,
                    metrics.clone(),
                    prewarmed_hits.clone(),
                    prewarmed_misses.clone(),
                ));
                let restore_slots = Arc::new(ExecutionSlots::new(max_concurrent_restores));
                let mut owner_senders = Vec::with_capacity(owner_count);
                let mut owner_receivers = Vec::with_capacity(owner_count);
                for _ in 0..owner_count {
                    let (sender, receiver) = mpsc::channel();
                    owner_senders.push(sender);
                    owner_receivers.push(receiver);
                }
                let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(owner_count);
                for (index, receiver) in owner_receivers.into_iter().enumerate() {
                    let owner_scheduler = scheduler.clone();
                    let owner_admitted = admitted.clone();
                    let owner_inventory = inventory.clone();
                    let owner_replenishing = replenishing.clone();
                    let owner_restore_slots = restore_slots.clone();
                    let owner_metrics = metrics.clone();
                    let owner_sandbox = worker.clone();
                    let owner_completion_sender = completion_sender.clone();
                    let handle = match thread::Builder::new()
                        .name(format!("workerd-sandbox-{index}"))
                        .spawn(move || {
                            run_prewarmed_owner(
                                index,
                                receiver,
                                owner_scheduler,
                                owner_admitted,
                                owner_inventory,
                                owner_replenishing,
                                owner_restore_slots,
                                owner_metrics,
                                owner_sandbox,
                                owner_completion_sender,
                            )
                        }) {
                        Ok(handle) => handle,
                        Err(error) => {
                            scheduler.shutdown();
                            restore_slots.close();
                            for sender in &owner_senders {
                                let _ = sender.send(OwnerMessage::Shutdown);
                            }
                            for worker in workers {
                                let _ = worker.join();
                            }
                            drop(completion_sender);
                            for worker in completion_workers {
                                let _ = worker.join();
                            }
                            return Err(error.into());
                        }
                    };
                    workers.push(handle);
                }
                let dispatcher_scheduler = scheduler.clone();
                let dispatcher_senders = owner_senders.clone();
                let dispatcher_admitted = admitted.clone();
                let dispatcher_completion_sender = completion_sender.clone();
                let dispatcher_metrics = metrics.clone();
                let dispatcher = match thread::Builder::new()
                    .name("workerd-dispatch".into())
                    .spawn(move || {
                        run_prewarmed_dispatcher(
                            dispatcher_scheduler,
                            dispatcher_senders,
                            dispatcher_admitted,
                            dispatcher_completion_sender,
                            dispatcher_metrics,
                        )
                    }) {
                    Ok(handle) => handle,
                    Err(error) => {
                        scheduler.shutdown();
                        restore_slots.close();
                        for sender in &owner_senders {
                            let _ = sender.send(OwnerMessage::Shutdown);
                        }
                        for worker in workers {
                            let _ = worker.join();
                        }
                        drop(completion_sender);
                        for worker in completion_workers {
                            let _ = worker.join();
                        }
                        return Err(error.into());
                    }
                };
                Ok(Self {
                    dispatch: PoolDispatch::Prewarmed(scheduler),
                    active,
                    admitted,
                    admission_capacity,
                    queue_capacity,
                    inventory,
                    replenishing,
                    prewarmed_hits,
                    prewarmed_misses,
                    prewarm_policy: Some(policy),
                    restore_slots: Some(restore_slots),
                    metrics,
                    owner_senders,
                    dispatcher: Some(dispatcher),
                    workers,
                    completion_sender: Some(completion_sender),
                    completion_workers,
                })
            }
        }
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
        self.try_submit_invocation(request.into(), timeout, move |execution| {
            completion(execution.into_fetch())
        })
    }

    pub fn try_submit_invocation(
        &self,
        request: InvocationRequest,
        timeout: Duration,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        self.try_submit_cancellable(
            request,
            timeout,
            InvocationCancellation::default(),
            completion,
        )
    }

    pub fn try_submit_cancellable(
        &self,
        request: InvocationRequest,
        timeout: Duration,
        cancellation: InvocationCancellation,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        self.try_submit_job(request.into(), timeout, cancellation, completion)
    }

    pub fn try_submit_stream(
        &self,
        request: RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        let cancellation = ingress.cancellation();
        self.try_submit_job(
            super::ingress::PoolInvocation::Stream {
                request,
                websocket,
                ingress,
            },
            timeout,
            cancellation,
            completion,
        )
    }

    fn try_submit_job(
        &self,
        request: super::ingress::PoolInvocation,
        timeout: Duration,
        cancellation: InvocationCancellation,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> std::result::Result<(), PoolSubmitError> {
        let request_id = request.request_id().to_string();
        if self
            .admitted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |admitted| {
                (admitted < self.admission_capacity).then_some(admitted + 1)
            })
            .is_err()
        {
            let error = self.terminal_error().unwrap_or(PoolSubmitError::Full);
            completion(InvocationExecution {
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
            ready_wait_started_at: None,
            diagnostic_wave_member: false,
            cancellation,
        };
        let pushed = match &self.dispatch {
            PoolDispatch::OnDemand(queue) => queue.try_push(job),
            PoolDispatch::Prewarmed(scheduler) => scheduler.try_push(job),
        };
        match pushed {
            Ok(()) => Ok(()),
            Err((error, job)) => {
                self.admitted.fetch_sub(1, Ordering::AcqRel);
                (job.completion)(InvocationExecution {
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
        let scheduler_status = match &self.dispatch {
            PoolDispatch::OnDemand(_) => None,
            PoolDispatch::Prewarmed(scheduler) => Some(scheduler.status()),
        };
        let (active, queued, ready) = match (&self.dispatch, scheduler_status) {
            (PoolDispatch::OnDemand(queue), None) => {
                (self.active.load(Ordering::Acquire), queue.len(), 0)
            }
            (PoolDispatch::Prewarmed(_), Some(status)) => {
                (status.active, status.queued, status.ready)
            }
            _ => unreachable!("dispatch and scheduler status must agree"),
        };
        let policy = self.prewarm_policy.unwrap_or(PrewarmPolicy {
            warm_floor: 0,
            ready_low_watermark: 0,
            ready_high_watermark: 0,
            max_replenish_batch: 0,
            diagnostic_no_refill_wave: None,
        });
        WorkerPoolStatus {
            admitted: self.admitted.load(Ordering::Acquire),
            active,
            queued,
            queue_capacity: self.queue_capacity,
            execution_slots_in_use: active,
            restore_slots_in_use: self
                .restore_slots
                .as_ref()
                .map_or(0, |slots| slots.in_use()),
            recycle_queue_depth: self.metrics.recycle_queue_depth.load(Ordering::Acquire),
            recycle_queue_peak: self.metrics.recycle_queue_peak.load(Ordering::Acquire),
            teardown_in_flight: self.metrics.teardown_in_flight.load(Ordering::Acquire),
            teardown_peak: self.metrics.teardown_peak.load(Ordering::Acquire),
            completed_teardowns: self.metrics.completed_teardowns.load(Ordering::Acquire),
            teardown_total_ms: nanoseconds_to_milliseconds(
                self.metrics.teardown_total_ns.load(Ordering::Acquire),
            ),
            teardown_max_ms: nanoseconds_to_milliseconds(
                self.metrics.teardown_max_ns.load(Ordering::Acquire),
            ),
            completion_queue_depth: self.metrics.completion_queue_depth.load(Ordering::Acquire),
            completion_queue_peak: self.metrics.completion_queue_peak.load(Ordering::Acquire),
            completion_in_flight: self.metrics.completion_in_flight.load(Ordering::Acquire),
            completion_peak: self.metrics.completion_peak.load(Ordering::Acquire),
            completed_completions: self.metrics.completed_completions.load(Ordering::Acquire),
            completion_total_ms: nanoseconds_to_milliseconds(
                self.metrics.completion_total_ns.load(Ordering::Acquire),
            ),
            completion_max_ms: nanoseconds_to_milliseconds(
                self.metrics.completion_max_ns.load(Ordering::Acquire),
            ),
            prewarmed_inventory: self.inventory.load(Ordering::Acquire),
            warm_floor: policy.warm_floor,
            ready_low_watermark: policy.ready_low_watermark,
            ready_high_watermark: policy.ready_high_watermark,
            max_replenish_batch: policy.max_replenish_batch,
            replenishment_paused: scheduler_status
                .is_some_and(|status| status.replenishment_paused),
            replenishment_pause_reason: scheduler_status
                .is_some_and(|status| status.replenishment_paused)
                .then_some("diagnostic-wave"),
            refill_active: scheduler_status.is_some_and(|status| status.refill_active),
            idle_owners: scheduler_status.map_or(0, |status| status.idle_owners),
            restore_permits_outstanding: scheduler_status
                .map_or(0, |status| status.restore_permits_outstanding),
            diagnostic_wave_dispatched: scheduler_status
                .map_or(0, |status| status.diagnostic_wave_dispatched),
            diagnostic_wave_completed: scheduler_status
                .map_or(0, |status| status.diagnostic_wave_completed),
            ready,
            ready_min: observed_min(&self.metrics.ready_min),
            ready_peak: self.metrics.ready_peak.load(Ordering::Acquire),
            replenishing: self.replenishing.load(Ordering::Acquire),
            replenishing_min: observed_min(&self.metrics.replenishing_min),
            replenishing_peak: self.metrics.replenishing_peak.load(Ordering::Acquire),
            restore_attempts: self.metrics.restore_attempts.load(Ordering::Acquire),
            completed_restores: self.metrics.completed_restores.load(Ordering::Acquire),
            failed_restores: self.metrics.failed_restores.load(Ordering::Acquire),
            completed_replenishment_policy_waits: self
                .metrics
                .completed_replenishment_policy_waits
                .load(Ordering::Acquire),
            replenishment_policy_wait_total_ms: nanoseconds_to_milliseconds(
                self.metrics
                    .replenishment_policy_wait_total_ns
                    .load(Ordering::Acquire),
            ),
            replenishment_policy_wait_max_ms: nanoseconds_to_milliseconds(
                self.metrics
                    .replenishment_policy_wait_max_ns
                    .load(Ordering::Acquire),
            ),
            restore_wait_total_ms: nanoseconds_to_milliseconds(
                self.metrics.restore_wait_total_ns.load(Ordering::Acquire),
            ),
            restore_wait_max_ms: nanoseconds_to_milliseconds(
                self.metrics.restore_wait_max_ns.load(Ordering::Acquire),
            ),
            restore_total_ms: nanoseconds_to_milliseconds(
                self.metrics.restore_total_ns.load(Ordering::Acquire),
            ),
            restore_max_ms: nanoseconds_to_milliseconds(
                self.metrics.restore_max_ns.load(Ordering::Acquire),
            ),
            prewarmed_hits: self.prewarmed_hits.load(Ordering::Acquire),
            prewarmed_misses: self.prewarmed_misses.load(Ordering::Acquire),
        }
    }

    fn terminal_error(&self) -> Option<PoolSubmitError> {
        match &self.dispatch {
            PoolDispatch::OnDemand(queue) => queue.terminal_error(),
            PoolDispatch::Prewarmed(scheduler) => scheduler.terminal_error(),
        }
    }
}

impl Drop for WorkerRequestPool {
    fn drop(&mut self) {
        let jobs = match &self.dispatch {
            PoolDispatch::OnDemand(queue) => queue.shutdown(),
            PoolDispatch::Prewarmed(scheduler) => scheduler.shutdown(),
        };
        enqueue_jobs(
            jobs,
            &self.admitted,
            PoolSubmitError::ShuttingDown,
            self.completion_sender.as_ref(),
            &self.metrics,
        );
        if let Some(restore_slots) = &self.restore_slots {
            restore_slots.close();
        }
        for sender in &self.owner_senders {
            let _ = sender.send(OwnerMessage::Shutdown);
        }
        if let Some(dispatcher) = self.dispatcher.take() {
            let _ = dispatcher.join();
        }
        for worker in self.workers.drain(..) {
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
        self.completion_sender.take();
        for worker in self.completion_workers.drain(..) {
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
    completion_sender: mpsc::Sender<CompletedJob>,
    metrics: Arc<PoolMetrics>,
) {
    while let Some(job) = queue.pop() {
        let request_id = job.request.request_id().to_string();
        let ready_wait_ms = elapsed_ms(job.admitted_at);
        active.fetch_add(1, Ordering::AcqRel);
        let execution = catch_unwind(AssertUnwindSafe(|| {
            job.request.execute_worker(
                &worker,
                &version,
                job.timeout.saturating_sub(job.admitted_at.elapsed()),
                job.cancellation,
            )
        }));
        active.fetch_sub(1, Ordering::AcqRel);
        let (result, profile) = match execution {
            Ok(execution) => execution,
            Err(_) => (
                Err(Error::State("request worker panicked".into())),
                ExecutionProfile {
                    ready_wait_ms,
                    ..ExecutionProfile::default()
                },
            ),
        };
        let mut profile = profile;
        profile.ready_wait_ms = ready_wait_ms;
        profile.admission_wait_ms = ready_wait_ms;
        enqueue_completion(
            &completion_sender,
            CompletedJob {
                completion: job.completion,
                execution: InvocationExecution {
                    request_id,
                    result,
                    profile,
                    submit_error: None,
                },
            },
            &metrics,
            &admitted,
        );
    }
}

fn run_prewarmed_dispatcher(
    scheduler: Arc<PrewarmScheduler>,
    owner_senders: Vec<mpsc::Sender<OwnerMessage>>,
    admitted: Arc<AtomicUsize>,
    completion_sender: mpsc::Sender<CompletedJob>,
    metrics: Arc<PoolMetrics>,
) {
    while let Some((owner, job)) = scheduler.next_dispatch() {
        if let Err(error) = owner_senders[owner.index].send(OwnerMessage::Execute(Box::new(job))) {
            scheduler.finish_failed_dispatch();
            let OwnerMessage::Execute(job) = error.0 else {
                unreachable!("dispatcher only sends execute messages")
            };
            enqueue_job_error(
                *job,
                PoolSubmitError::Unavailable,
                &completion_sender,
                &metrics,
                &admitted,
            );
            enqueue_jobs(
                scheduler.owner_failed(false),
                &admitted,
                PoolSubmitError::Unavailable,
                Some(&completion_sender),
                &metrics,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_prewarmed_owner(
    index: usize,
    receiver: mpsc::Receiver<OwnerMessage>,
    scheduler: Arc<PrewarmScheduler>,
    admitted: Arc<AtomicUsize>,
    inventory: Arc<AtomicUsize>,
    replenishing: Arc<AtomicUsize>,
    restore_slots: Arc<ExecutionSlots>,
    metrics: Arc<PoolMetrics>,
    worker: WorkerVersionSandbox,
    completion_sender: mpsc::Sender<CompletedJob>,
) {
    const RESTORE_RETRY_DELAYS: [Duration; 3] = [
        Duration::from_millis(10),
        Duration::from_millis(25),
        Duration::from_millis(50),
    ];
    let mut consecutive_failures = 0usize;
    let mut recycling = false;
    loop {
        let policy_wait_started = Instant::now();
        if recycling && !scheduler.wait_for_replenishment(index) {
            return;
        }
        let policy_wait_ms = if recycling {
            let duration = policy_wait_started.elapsed();
            record_duration(
                &metrics.replenishment_policy_wait_total_ns,
                &metrics.replenishment_policy_wait_max_ns,
                duration,
            );
            metrics
                .completed_replenishment_policy_waits
                .fetch_add(1, Ordering::AcqRel);
            duration.as_secs_f64() * 1000.0
        } else {
            0.0
        };
        let restore_wait_started = Instant::now();
        let acquired = if recycling {
            restore_slots.acquire_with_wait_observer(
                || {
                    let queued = metrics.recycle_queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
                    update_max(&metrics.recycle_queue_peak, queued);
                },
                || {
                    metrics.recycle_queue_depth.fetch_sub(1, Ordering::AcqRel);
                },
            )
        } else {
            restore_slots.acquire()
        };
        if !acquired {
            return;
        }
        let restore_wait_ms = elapsed_ms(restore_wait_started);
        record_duration(
            &metrics.restore_wait_total_ns,
            &metrics.restore_wait_max_ns,
            restore_wait_started.elapsed(),
        );
        metrics.restore_attempts.fetch_add(1, Ordering::AcqRel);
        let replenishing_now = replenishing.fetch_add(1, Ordering::AcqRel) + 1;
        record_observation(
            replenishing_now,
            &metrics.replenishing_min,
            &metrics.replenishing_peak,
        );
        let restore_started = Instant::now();
        let restored = catch_unwind(AssertUnwindSafe(|| worker.restore()));
        record_duration(
            &metrics.restore_total_ns,
            &metrics.restore_max_ns,
            restore_started.elapsed(),
        );
        let replenishing_now = replenishing.fetch_sub(1, Ordering::AcqRel) - 1;
        record_observation(
            replenishing_now,
            &metrics.replenishing_min,
            &metrics.replenishing_peak,
        );
        restore_slots.release();
        let (restored, restore_ms) = match restored {
            Ok(Ok(restored)) => {
                metrics.completed_restores.fetch_add(1, Ordering::AcqRel);
                restored
            }
            Ok(Err(error)) => {
                metrics.failed_restores.fetch_add(1, Ordering::AcqRel);
                tracing::warn!(owner = index, %error, "workerd sandbox replenishment failed");
                if retry_prewarmed_restore(
                    &scheduler,
                    &admitted,
                    &completion_sender,
                    &metrics,
                    &mut consecutive_failures,
                    &RESTORE_RETRY_DELAYS,
                    recycling,
                ) {
                    return;
                }
                continue;
            }
            Err(_) => {
                metrics.failed_restores.fetch_add(1, Ordering::AcqRel);
                tracing::warn!(owner = index, "workerd sandbox replenishment panicked");
                if retry_prewarmed_restore(
                    &scheduler,
                    &admitted,
                    &completion_sender,
                    &metrics,
                    &mut consecutive_failures,
                    &RESTORE_RETRY_DELAYS,
                    recycling,
                ) {
                    return;
                }
                continue;
            }
        };
        consecutive_failures = 0;
        inventory.fetch_add(1, Ordering::AcqRel);
        if !scheduler.publish_ready(
            ReadyOwner {
                index,
                ready_at: Instant::now(),
            },
            recycling,
        ) {
            inventory.fetch_sub(1, Ordering::AcqRel);
            drop(restored);
            return;
        }
        let job = match receiver.recv() {
            Ok(OwnerMessage::Execute(job)) => *job,
            Ok(OwnerMessage::Shutdown) | Err(_) => {
                inventory.fetch_sub(1, Ordering::AcqRel);
                drop(restored);
                return;
            }
        };
        let dispatched_at = Instant::now();
        let ready_wait_started_at = job.ready_wait_started_at.unwrap_or(job.admitted_at);
        let admission_wait_ms = ready_wait_started_at
            .saturating_duration_since(job.admitted_at)
            .as_secs_f64()
            * 1000.0;
        let ready_owner_wait_ms = dispatched_at
            .saturating_duration_since(ready_wait_started_at)
            .as_secs_f64()
            * 1000.0;
        let ready_wait_ms = admission_wait_ms + ready_owner_wait_ms;
        let diagnostic_wave_member = job.diagnostic_wave_member;
        let completed = execute_prewarmed_job(
            job,
            restored,
            PrewarmExecutionTiming {
                ready_wait_ms,
                admission_wait_ms,
                ready_owner_wait_ms,
                policy_wait_ms,
                restore_wait_ms: if recycling { restore_wait_ms } else { 0.0 },
                restore_ms: if recycling { restore_ms } else { 0.0 },
            },
            &metrics,
        );
        inventory.fetch_sub(1, Ordering::AcqRel);
        scheduler.finish_execution(index, diagnostic_wave_member);
        enqueue_completion(&completion_sender, completed, &metrics, &admitted);
        recycling = true;
    }
}

fn retry_prewarmed_restore(
    scheduler: &PrewarmScheduler,
    admitted: &AtomicUsize,
    completion_sender: &mpsc::Sender<CompletedJob>,
    metrics: &PoolMetrics,
    consecutive_failures: &mut usize,
    delays: &[Duration],
    used_replenishment_permit: bool,
) -> bool {
    if *consecutive_failures == delays.len() {
        enqueue_jobs(
            scheduler.owner_failed(used_replenishment_permit),
            admitted,
            PoolSubmitError::Unavailable,
            Some(completion_sender),
            metrics,
        );
        return true;
    }
    let delay = delays[*consecutive_failures];
    *consecutive_failures += 1;
    scheduler.wait_for_retry_or_close(delay)
}

fn execute_prewarmed_job(
    job: RequestJob,
    restored: RestoredWorkerVersionSandbox,
    timing: PrewarmExecutionTiming,
    metrics: &PoolMetrics,
) -> CompletedJob {
    let request_id = job.request.request_id().to_string();
    let execution = catch_unwind(AssertUnwindSafe(|| {
        let total_started = Instant::now();
        let teardown_started = std::cell::Cell::new(None);
        let mut execution = job.request.execute_restored(
            restored,
            job.timeout.saturating_sub(job.admitted_at.elapsed()),
            total_started,
            job.cancellation,
            || {
                let in_flight = metrics.teardown_in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                update_max(&metrics.teardown_peak, in_flight);
                teardown_started.set(Some(Instant::now()));
            },
            || {
                if let Some(started) = teardown_started.take() {
                    record_duration(
                        &metrics.teardown_total_ns,
                        &metrics.teardown_max_ns,
                        started.elapsed(),
                    );
                }
                metrics.teardown_in_flight.fetch_sub(1, Ordering::AcqRel);
                metrics.completed_teardowns.fetch_add(1, Ordering::AcqRel);
            },
        );
        execution.1.ready_wait_ms = timing.ready_wait_ms;
        execution.1.admission_wait_ms = timing.admission_wait_ms;
        execution.1.ready_owner_wait_ms = timing.ready_owner_wait_ms;
        execution.1.replenishment_policy_wait_ms = timing.policy_wait_ms;
        execution.1.replenishment_wait_ms = timing.restore_wait_ms;
        execution.1.replenishment_restore_ms = timing.restore_ms;
        execution
    }));
    let (result, profile) = match execution {
        Ok(execution) => execution,
        Err(_) => (
            Err(Error::State("request worker panicked".into())),
            ExecutionProfile {
                ready_wait_ms: timing.ready_wait_ms,
                admission_wait_ms: timing.admission_wait_ms,
                ready_owner_wait_ms: timing.ready_owner_wait_ms,
                replenishment_policy_wait_ms: timing.policy_wait_ms,
                replenishment_wait_ms: timing.restore_wait_ms,
                replenishment_restore_ms: timing.restore_ms,
                ..ExecutionProfile::default()
            },
        ),
    };
    CompletedJob {
        completion: job.completion,
        execution: InvocationExecution {
            request_id,
            result,
            profile,
            submit_error: None,
        },
    }
}

fn spawn_completion_workers(
    count: usize,
    metrics: Arc<PoolMetrics>,
    admitted: Arc<AtomicUsize>,
) -> Result<(mpsc::Sender<CompletedJob>, Vec<JoinHandle<()>>)> {
    let (sender, receiver) = mpsc::channel::<CompletedJob>();
    let receiver = Arc::new(Mutex::new(receiver));
    let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(count);
    for index in 0..count {
        let worker_receiver = receiver.clone();
        let worker_metrics = metrics.clone();
        let worker_admitted = admitted.clone();
        let handle = match thread::Builder::new()
            .name(format!("workerd-completion-{index}"))
            .spawn(move || {
                loop {
                    let completed = {
                        let receiver = worker_receiver
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        receiver.recv()
                    };
                    let Ok(completed) = completed else {
                        return;
                    };
                    worker_metrics
                        .completion_queue_depth
                        .fetch_sub(1, Ordering::AcqRel);
                    run_completion(completed, &worker_metrics, &worker_admitted);
                }
            }) {
            Ok(handle) => handle,
            Err(error) => {
                drop(sender);
                for worker in workers {
                    let _ = worker.join();
                }
                return Err(error.into());
            }
        };
        workers.push(handle);
    }
    Ok((sender, workers))
}

fn enqueue_completion(
    sender: &mpsc::Sender<CompletedJob>,
    completed: CompletedJob,
    metrics: &PoolMetrics,
    admitted: &AtomicUsize,
) {
    let queued = metrics
        .completion_queue_depth
        .fetch_add(1, Ordering::AcqRel)
        + 1;
    update_max(&metrics.completion_queue_peak, queued);
    if let Err(error) = sender.send(completed) {
        metrics
            .completion_queue_depth
            .fetch_sub(1, Ordering::AcqRel);
        run_completion(error.0, metrics, admitted);
    }
}

fn enqueue_job_error(
    job: RequestJob,
    error: PoolSubmitError,
    sender: &mpsc::Sender<CompletedJob>,
    metrics: &PoolMetrics,
    admitted: &AtomicUsize,
) {
    let request_id = job.request.request_id().to_string();
    enqueue_completion(
        sender,
        CompletedJob {
            completion: job.completion,
            execution: InvocationExecution {
                request_id,
                result: Err(Error::State(error.to_string())),
                profile: ExecutionProfile::default(),
                submit_error: Some(error),
            },
        },
        metrics,
        admitted,
    );
}

fn enqueue_jobs(
    jobs: Vec<RequestJob>,
    admitted: &AtomicUsize,
    error: PoolSubmitError,
    sender: Option<&mpsc::Sender<CompletedJob>>,
    metrics: &PoolMetrics,
) {
    for job in jobs {
        if let Some(sender) = sender {
            enqueue_job_error(job, error, sender, metrics, admitted);
        } else {
            let request_id = job.request.request_id().to_string();
            let _ = catch_unwind(AssertUnwindSafe(|| {
                (job.completion)(InvocationExecution {
                    request_id,
                    result: Err(Error::State(error.to_string())),
                    profile: ExecutionProfile::default(),
                    submit_error: Some(error),
                });
            }));
            admitted.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn run_completion(completed: CompletedJob, metrics: &PoolMetrics, admitted: &AtomicUsize) {
    let in_flight = metrics.completion_in_flight.fetch_add(1, Ordering::AcqRel) + 1;
    update_max(&metrics.completion_peak, in_flight);
    let started = Instant::now();
    let _ = catch_unwind(AssertUnwindSafe(|| {
        (completed.completion)(completed.execution)
    }));
    record_duration(
        &metrics.completion_total_ns,
        &metrics.completion_max_ns,
        started.elapsed(),
    );
    metrics.completed_completions.fetch_add(1, Ordering::AcqRel);
    metrics.completion_in_flight.fetch_sub(1, Ordering::AcqRel);
    admitted.fetch_sub(1, Ordering::AcqRel);
}

fn record_observation(value: usize, min: &AtomicUsize, max: &AtomicUsize) {
    min.fetch_min(value, Ordering::AcqRel);
    update_max(max, value);
}

fn update_max(max: &AtomicUsize, value: usize) {
    max.fetch_max(value, Ordering::AcqRel);
}

fn record_duration(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let nanoseconds = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    total.fetch_add(nanoseconds, Ordering::AcqRel);
    max.fetch_max(nanoseconds, Ordering::AcqRel);
}

fn observed_min(min: &AtomicUsize) -> usize {
    match min.load(Ordering::Acquire) {
        usize::MAX => 0,
        value => value,
    }
}

fn nanoseconds_to_milliseconds(nanoseconds: u64) -> f64 {
    Duration::from_nanos(nanoseconds).as_secs_f64() * 1000.0
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn record_prewarmed_pairing(
    ready_at: Instant,
    admitted_at: Instant,
    hits: &AtomicUsize,
    misses: &AtomicUsize,
) {
    if ready_at <= admitted_at {
        hits.fetch_add(1, Ordering::AcqRel);
    } else {
        misses.fetch_add(1, Ordering::AcqRel);
    }
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
    if let WorkerPoolRestoreMode::Prewarmed {
        sandboxes, policy, ..
    } = restore_mode
    {
        if policy.warm_floor == 0 {
            return Err(Error::State("prewarm warm floor must be nonzero".into()));
        }
        if policy.warm_floor >= sandboxes {
            return Err(Error::State(
                "prewarm warm floor must be smaller than the owner count".into(),
            ));
        }
        if policy.ready_low_watermark <= policy.warm_floor {
            return Err(Error::State(
                "ready low watermark must exceed the warm floor".into(),
            ));
        }
        if policy.ready_low_watermark > policy.ready_high_watermark {
            return Err(Error::State(
                "ready low watermark must not exceed the high watermark".into(),
            ));
        }
        if policy.ready_high_watermark > sandboxes {
            return Err(Error::State(
                "ready high watermark must not exceed the owner count".into(),
            ));
        }
        if policy.max_replenish_batch == 0 {
            return Err(Error::State(
                "maximum replenish batch must be nonzero".into(),
            ));
        }
        if policy.diagnostic_no_refill_wave == Some(0) {
            return Err(Error::State(
                "diagnostic no-refill wave must be nonzero".into(),
            ));
        }
    }
    Ok(())
}

fn effective_concurrency(
    max_concurrent_sandboxes: usize,
    restore_mode: WorkerPoolRestoreMode,
) -> usize {
    match restore_mode {
        WorkerPoolRestoreMode::OnDemand => max_concurrent_sandboxes,
        WorkerPoolRestoreMode::Prewarmed {
            sandboxes, policy, ..
        } => sandboxes
            .saturating_sub(policy.warm_floor)
            .min(max_concurrent_sandboxes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_is_bounded_fifo_and_drains_on_close() {
        let queue = RequestQueue::new(2);
        assert!(queue.try_push(test_job("one", |_| {})).is_ok());
        assert!(queue.try_push(test_job("two", |_| {})).is_ok());
        assert_eq!(queue.len(), 2);
        assert_eq!(
            queue.try_push(test_job("overflow", |_| {})).unwrap_err().0,
            PoolSubmitError::Full
        );
        queue.close();
        assert_eq!(queue.pop().unwrap().request.request_id(), "one");
        assert_eq!(queue.pop().unwrap().request.request_id(), "two");
        assert!(queue.pop().is_none());
        assert_eq!(
            queue.try_push(test_job("closed", |_| {})).unwrap_err().0,
            PoolSubmitError::ShuttingDown
        );
    }

    #[test]
    fn shutdown_drains_queued_jobs_and_releases_admission() {
        let queue = RequestQueue::new(3);
        let admitted = AtomicUsize::new(2);
        let metrics = PoolMetrics::new();
        let (tx, rx) = std::sync::mpsc::channel();
        let job = |id: &str| {
            let tx = tx.clone();
            test_job(id, move |execution| {
                tx.send(execution.submit_error).unwrap();
            })
        };
        assert!(queue.try_push(job("one")).is_ok());
        assert!(queue.try_push(job("two")).is_ok());

        enqueue_jobs(
            queue.shutdown(),
            &admitted,
            PoolSubmitError::ShuttingDown,
            None,
            &metrics,
        );

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
    fn scheduler_directly_pairs_ready_owner_without_owner_queue_waiters() {
        let scheduler = test_scheduler(2, 1);
        assert!(scheduler.try_push(test_job("one", |_| {})).is_ok());
        assert!(scheduler.try_push(test_job("two", |_| {})).is_ok());
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 0,
                ready_at: Instant::now(),
            },
            false,
        ));
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 1,
                ready_at: Instant::now(),
            },
            false,
        ));

        let (owner, job) = scheduler.next_dispatch().unwrap();
        assert_eq!(owner.index, 0);
        assert_eq!(job.request.request_id(), "one");
        assert_eq!(scheduler.status().active, 1);
        assert_eq!(scheduler.status().queued, 1);
        assert_eq!(scheduler.status().ready, 1);

        let scheduler = Arc::new(scheduler);
        let dispatch_scheduler = scheduler.clone();
        let (tx, rx) = mpsc::channel();
        let blocked = thread::spawn(move || {
            tx.send(
                dispatch_scheduler
                    .next_dispatch()
                    .unwrap()
                    .1
                    .request
                    .request_id()
                    .to_string(),
            )
            .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        scheduler.finish_execution(0, false);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), "two");
        blocked.join().unwrap();
    }

    #[test]
    fn warm_floor_is_never_dispatched() {
        let scheduler = PrewarmScheduler::new(
            4,
            2,
            2,
            PrewarmPolicy {
                warm_floor: 1,
                ready_low_watermark: 1,
                ready_high_watermark: 2,
                max_replenish_batch: 1,
                diagnostic_no_refill_wave: None,
            },
            Arc::new(PoolMetrics::new()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        assert!(scheduler.try_push(test_job("one", |_| {})).is_ok());
        assert!(scheduler.try_push(test_job("two", |_| {})).is_ok());
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 0,
                ready_at: Instant::now(),
            },
            false,
        ));
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 1,
                ready_at: Instant::now(),
            },
            false,
        ));

        assert_eq!(
            scheduler.next_dispatch().unwrap().1.request.request_id(),
            "one"
        );
        let status = scheduler.status();
        assert_eq!(status.active, 1);
        assert_eq!(status.queued, 1);
        assert_eq!(status.ready, 1);
    }

    #[test]
    fn adaptive_replenishment_refills_in_bounded_batches_toward_high() {
        let scheduler = PrewarmScheduler::new(
            8,
            4,
            5,
            PrewarmPolicy {
                warm_floor: 1,
                ready_low_watermark: 2,
                ready_high_watermark: 4,
                max_replenish_batch: 2,
                diagnostic_no_refill_wave: None,
            },
            Arc::new(PoolMetrics::new()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        for index in 0..5 {
            assert!(scheduler.publish_ready(
                ReadyOwner {
                    index,
                    ready_at: Instant::now(),
                },
                false,
            ));
            assert!(
                scheduler
                    .try_push(test_job(&format!("job-{index}"), |_| {}))
                    .is_ok()
            );
        }
        let first = scheduler.next_dispatch().unwrap().0.index;
        let second = scheduler.next_dispatch().unwrap().0.index;
        let third = scheduler.next_dispatch().unwrap().0.index;
        let fourth = scheduler.next_dispatch().unwrap().0.index;
        scheduler.finish_execution(first, false);
        scheduler.finish_execution(second, false);
        scheduler.finish_execution(third, false);
        scheduler.finish_execution(fourth, false);

        let status = scheduler.status();
        assert!(status.refill_active);
        assert_eq!(status.ready, 1);
        assert_eq!(status.restore_permits_outstanding, 2);
        assert_eq!(status.idle_owners, 2);
        assert!(scheduler.wait_for_replenishment(first));
        assert!(scheduler.wait_for_replenishment(second));
    }

    #[test]
    fn diagnostic_wave_pauses_refill_until_every_wave_member_finishes() {
        let scheduler = PrewarmScheduler::new(
            8,
            4,
            5,
            PrewarmPolicy {
                warm_floor: 1,
                ready_low_watermark: 2,
                ready_high_watermark: 4,
                max_replenish_batch: 2,
                diagnostic_no_refill_wave: Some(4),
            },
            Arc::new(PoolMetrics::new()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        for index in 0..5 {
            assert!(scheduler.publish_ready(
                ReadyOwner {
                    index,
                    ready_at: Instant::now(),
                },
                false,
            ));
            assert!(
                scheduler
                    .try_push(test_job(&format!("job-{index}"), |_| {}))
                    .is_ok()
            );
        }
        let dispatches = (0..4)
            .map(|_| scheduler.next_dispatch().unwrap())
            .collect::<Vec<_>>();
        for (index, (owner, job)) in dispatches.into_iter().enumerate() {
            assert!(job.diagnostic_wave_member);
            scheduler.finish_execution(owner.index, true);
            let status = scheduler.status();
            if index < 3 {
                assert!(status.replenishment_paused);
                assert_eq!(status.restore_permits_outstanding, 0);
            }
        }
        let status = scheduler.status();
        assert!(!status.replenishment_paused);
        assert_eq!(status.diagnostic_wave_dispatched, 4);
        assert_eq!(status.diagnostic_wave_completed, 4);
        assert_eq!(status.restore_permits_outstanding, 2);
    }

    #[test]
    fn request_wakes_dispatcher_when_idle_owners_are_waiting() {
        let scheduler = Arc::new(PrewarmScheduler::new(
            8,
            2,
            4,
            PrewarmPolicy {
                warm_floor: 1,
                ready_low_watermark: 2,
                ready_high_watermark: 2,
                max_replenish_batch: 1,
                diagnostic_no_refill_wave: Some(2),
            },
            Arc::new(PoolMetrics::new()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        ));
        for index in 0..4 {
            assert!(scheduler.publish_ready(
                ReadyOwner {
                    index,
                    ready_at: Instant::now(),
                },
                false,
            ));
        }
        for id in ["wave-one", "wave-two"] {
            assert!(scheduler.try_push(test_job(id, |_| {})).is_ok());
        }
        let dispatches = (0..2)
            .map(|_| scheduler.next_dispatch().unwrap())
            .collect::<Vec<_>>();
        let (waiting_tx, waiting_rx) = mpsc::channel();
        let mut owners = Vec::new();
        for (owner, job) in dispatches {
            assert!(job.diagnostic_wave_member);
            scheduler.finish_execution(owner.index, true);
            let waiter = scheduler.clone();
            let waiting_tx = waiting_tx.clone();
            owners.push(thread::spawn(move || {
                waiting_tx.send(()).unwrap();
                let _ = waiter.wait_for_replenishment(owner.index);
            }));
        }
        waiting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        waiting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(20));

        let dispatcher = scheduler.clone();
        let (dispatch_tx, dispatch_rx) = mpsc::channel();
        let dispatch = thread::spawn(move || {
            dispatch_tx.send(dispatcher.next_dispatch()).unwrap();
        });
        thread::sleep(Duration::from_millis(20));
        assert!(scheduler.try_push(test_job("after-wave", |_| {})).is_ok());
        let (_, job) = dispatch_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(job.request.request_id(), "after-wave");

        scheduler.shutdown();
        dispatch.join().unwrap();
        for owner in owners {
            owner.join().unwrap();
        }
    }

    #[test]
    fn owner_is_not_ready_again_until_replacement_is_published() {
        let scheduler = test_scheduler(1, 1);
        assert!(scheduler.try_push(test_job("one", |_| {})).is_ok());
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 0,
                ready_at: Instant::now(),
            },
            false,
        ));
        let _ = scheduler.next_dispatch().unwrap();
        scheduler.finish_execution(0, false);

        let status = scheduler.status();
        assert_eq!(status.active, 0);
        assert_eq!(status.ready, 0);
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 0,
                ready_at: Instant::now(),
            },
            false,
        ));
        assert_eq!(scheduler.status().ready, 1);
    }

    #[test]
    fn permanent_owner_failure_drains_and_rejects_admission() {
        let scheduler = test_scheduler(1, 1);
        assert!(scheduler.try_push(test_job("one", |_| {})).is_ok());
        assert!(scheduler.try_push(test_job("two", |_| {})).is_ok());
        let drained = scheduler.owner_failed(false);

        assert_eq!(drained.len(), 2);
        assert_eq!(
            scheduler.terminal_error(),
            Some(PoolSubmitError::Unavailable)
        );
        assert_eq!(
            scheduler
                .try_push(test_job("rejected", |_| {}))
                .unwrap_err()
                .0,
            PoolSubmitError::Unavailable
        );
        assert!(scheduler.next_dispatch().is_none());
    }

    #[test]
    fn restore_slots_bound_recycle_concurrency() {
        let slots = Arc::new(ExecutionSlots::new(1));
        let wait_started = Arc::new(AtomicUsize::new(0));
        let wait_finished = Arc::new(AtomicUsize::new(0));
        assert!(slots.acquire_with_wait_observer(
            || panic!("immediate acquisition must not report a waiter"),
            || panic!("immediate acquisition must not finish a waiter"),
        ));
        let other_slots = slots.clone();
        let other_started = wait_started.clone();
        let other_finished = wait_finished.clone();
        let (tx, rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            tx.send(other_slots.acquire_with_wait_observer(
                || {
                    other_started.fetch_add(1, Ordering::AcqRel);
                },
                || {
                    other_finished.fetch_add(1, Ordering::AcqRel);
                },
            ))
            .unwrap();
            other_slots.release();
        });
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert_eq!(slots.in_use(), 1);
        assert_eq!(wait_started.load(Ordering::Acquire), 1);
        assert_eq!(wait_finished.load(Ordering::Acquire), 0);
        slots.release();
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        waiter.join().unwrap();
        assert_eq!(slots.in_use(), 0);
        assert_eq!(wait_finished.load(Ordering::Acquire), 1);
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
                    policy: test_policy(1),
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
                    policy: test_policy(2),
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn prewarmed_admission_uses_effective_owner_concurrency() {
        assert_eq!(
            effective_concurrency(
                8,
                WorkerPoolRestoreMode::Prewarmed {
                    sandboxes: 2,
                    max_concurrent_restores: 1,
                    policy: test_policy(2),
                }
            ),
            1
        );
        assert_eq!(
            effective_concurrency(
                2,
                WorkerPoolRestoreMode::Prewarmed {
                    sandboxes: 8,
                    max_concurrent_restores: 1,
                    policy: test_policy(8),
                }
            ),
            2
        );
        assert_eq!(effective_concurrency(8, WorkerPoolRestoreMode::OnDemand), 8);
    }

    #[test]
    fn queue_length_reports_actual_waiting_jobs_without_status_cap() {
        let queue = RequestQueue::new(3);
        assert!(queue.try_push(test_job("one", |_| {})).is_ok());
        assert!(queue.try_push(test_job("two", |_| {})).is_ok());
        assert!(queue.try_push(test_job("three", |_| {})).is_ok());
        assert_eq!(queue.len(), 3);
    }

    #[test]
    fn one_ready_vm_records_one_hit_and_queued_job_records_one_miss() {
        let hits = AtomicUsize::new(0);
        let misses = AtomicUsize::new(0);
        let first_ready = Instant::now();
        let first_admitted = first_ready + Duration::from_millis(1);
        record_prewarmed_pairing(first_ready, first_admitted, &hits, &misses);

        let queued_admitted = Instant::now();
        let replenished_ready = queued_admitted + Duration::from_millis(1);
        record_prewarmed_pairing(replenished_ready, queued_admitted, &hits, &misses);

        assert_eq!(hits.load(Ordering::Acquire), 1);
        assert_eq!(misses.load(Ordering::Acquire), 1);
    }

    #[test]
    fn active_capacity_is_released_before_blocked_completion() {
        let scheduler = Arc::new(test_scheduler(1, 1));
        assert!(scheduler.try_push(test_job("one", |_| {})).is_ok());
        assert!(scheduler.publish_ready(
            ReadyOwner {
                index: 0,
                ready_at: Instant::now(),
            },
            false,
        ));
        let _ = scheduler.next_dispatch().unwrap();
        let metrics = scheduler.metrics.clone();
        let admitted = Arc::new(AtomicUsize::new(1));
        let (completion_sender, completion_workers) =
            spawn_completion_workers(1, metrics.clone(), admitted.clone()).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let completed = CompletedJob {
            completion: Box::new(move |_| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
            execution: test_execution("blocked"),
        };
        scheduler.finish_execution(0, false);
        enqueue_completion(&completion_sender, completed, &metrics, &admitted);

        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(scheduler.status().active, 0);
        assert_eq!(metrics.completion_in_flight.load(Ordering::Acquire), 1);

        release_tx.send(()).unwrap();
        drop(completion_sender);
        for worker in completion_workers {
            worker.join().unwrap();
        }
        assert_eq!(metrics.completion_in_flight.load(Ordering::Acquire), 0);
        assert_eq!(metrics.completed_completions.load(Ordering::Acquire), 1);
        assert_eq!(admitted.load(Ordering::Acquire), 0);
        assert!(metrics.completion_total_ns.load(Ordering::Acquire) > 0);
    }

    #[test]
    fn completion_concurrency_is_observable_and_unwinds_after_panics() {
        let owner_count = 2;
        let metrics = Arc::new(PoolMetrics::new());
        let admitted = Arc::new(AtomicUsize::new(3));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let mut workers = Vec::new();
        for id in ["one", "two"] {
            let metrics = metrics.clone();
            let entered_tx = entered_tx.clone();
            let release = release.clone();
            let admitted = admitted.clone();
            workers.push(thread::spawn(move || {
                run_completion(
                    CompletedJob {
                        completion: Box::new(move |_| {
                            entered_tx.send(()).unwrap();
                            let (lock, available) = &*release;
                            let mut released = lock.lock().unwrap();
                            while !*released {
                                released = available.wait(released).unwrap();
                            }
                        }),
                        execution: test_execution(id),
                    },
                    &metrics,
                    &admitted,
                );
            }));
        }
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(metrics.completion_in_flight.load(Ordering::Acquire), 2);
        assert_eq!(metrics.completion_peak.load(Ordering::Acquire), 2);
        assert!(metrics.completion_peak.load(Ordering::Acquire) <= owner_count);

        {
            let (lock, available) = &*release;
            *lock.lock().unwrap() = true;
            available.notify_all();
        }
        for worker in workers {
            worker.join().unwrap();
        }
        run_completion(
            CompletedJob {
                completion: Box::new(|_| panic!("completion panic is contained")),
                execution: test_execution("panic"),
            },
            &metrics,
            &admitted,
        );
        assert_eq!(metrics.completion_in_flight.load(Ordering::Acquire), 0);
        assert_eq!(metrics.completed_completions.load(Ordering::Acquire), 3);
        assert_eq!(metrics.completion_peak.load(Ordering::Acquire), 2);
        assert_eq!(admitted.load(Ordering::Acquire), 0);
    }

    #[test]
    fn extrema_and_duration_metrics_report_observed_values() {
        let metrics = PoolMetrics::new();
        record_observation(3, &metrics.ready_min, &metrics.ready_peak);
        record_observation(1, &metrics.ready_min, &metrics.ready_peak);
        record_observation(2, &metrics.ready_min, &metrics.ready_peak);
        record_duration(
            &metrics.restore_total_ns,
            &metrics.restore_max_ns,
            Duration::from_millis(2),
        );
        record_duration(
            &metrics.restore_total_ns,
            &metrics.restore_max_ns,
            Duration::from_millis(5),
        );

        assert_eq!(observed_min(&metrics.ready_min), 1);
        assert_eq!(metrics.ready_peak.load(Ordering::Acquire), 3);
        assert_eq!(
            nanoseconds_to_milliseconds(metrics.restore_total_ns.load(Ordering::Acquire)),
            7.0
        );
        assert_eq!(
            nanoseconds_to_milliseconds(metrics.restore_max_ns.load(Ordering::Acquire)),
            5.0
        );
    }

    fn test_execution(request_id: &str) -> InvocationExecution {
        InvocationExecution {
            request_id: request_id.into(),
            result: Err(Error::State("test".into())),
            profile: ExecutionProfile::default(),
            submit_error: None,
        }
    }

    fn test_scheduler(owner_count: usize, max_active: usize) -> PrewarmScheduler {
        PrewarmScheduler::new(
            owner_count + 2,
            max_active,
            owner_count,
            PrewarmPolicy {
                warm_floor: 0,
                ready_low_watermark: 1,
                ready_high_watermark: owner_count,
                max_replenish_batch: 1,
                diagnostic_no_refill_wave: None,
            },
            Arc::new(PoolMetrics::new()),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    fn test_job(
        request_id: &str,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> RequestJob {
        RequestJob {
            request: RequestEnvelope {
                protocol_version: super::super::PROTOCOL_VERSION,
                request_id: request_id.into(),
                method: "GET".into(),
                url: "https://example.test/".into(),
                headers: vec![],
                body_base64: String::new(),
            }
            .into(),
            timeout: Duration::from_secs(1),
            completion: Box::new(completion),
            admitted_at: Instant::now(),
            ready_wait_started_at: None,
            diagnostic_wave_member: false,
            cancellation: InvocationCancellation::default(),
        }
    }

    fn test_policy(owner_count: usize) -> PrewarmPolicy {
        PrewarmPolicy {
            warm_floor: 1,
            ready_low_watermark: owner_count.min(2),
            ready_high_watermark: owner_count,
            max_replenish_batch: 1,
            diagnostic_no_refill_wave: None,
        }
    }
}
