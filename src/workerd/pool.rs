// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{
    Error, ExecutionProfile, RequestEnvelope, Result, WorkerVersionId, WorkerVersionSandbox,
};
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

type Completion = Box<dyn FnOnce(RequestExecution) + Send + 'static>;

struct RequestJob {
    request: RequestEnvelope,
    timeout: Duration,
    completion: Completion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolSubmitError {
    Full,
    ShuttingDown,
}

impl std::fmt::Display for PoolSubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => formatter.write_str("request queue is full"),
            Self::ShuttingDown => formatter.write_str("request pool is shutting down"),
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
}

struct QueueState {
    jobs: VecDeque<RequestJob>,
    closed: bool,
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
                closed: false,
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
        if state.closed {
            return Err((PoolSubmitError::ShuttingDown, Box::new(job)));
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
            if state.closed {
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
    workers: Vec<JoinHandle<()>>,
}

impl WorkerRequestPool {
    pub fn new(
        worker: WorkerVersionSandbox,
        max_concurrent_sandboxes: usize,
        queue_capacity: usize,
    ) -> Result<Self> {
        validate_configuration(max_concurrent_sandboxes, queue_capacity)?;
        let admission_capacity = max_concurrent_sandboxes
            .checked_add(queue_capacity)
            .ok_or_else(|| Error::State("request pool capacity is too large".into()))?;
        let queue = Arc::new(RequestQueue::new(admission_capacity));
        let active = Arc::new(AtomicUsize::new(0));
        let admitted = Arc::new(AtomicUsize::new(0));
        let version = worker.worker_version().clone();
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(max_concurrent_sandboxes);
        for index in 0..max_concurrent_sandboxes {
            let worker_queue = queue.clone();
            let worker_active = active.clone();
            let worker_admitted = admitted.clone();
            let worker_sandbox = worker.clone();
            let worker_version = version.clone();
            let handle = match thread::Builder::new()
                .name(format!("workerd-sandbox-{index}"))
                .spawn(move || {
                    run_worker(
                        worker_queue,
                        worker_active,
                        worker_admitted,
                        worker_sandbox,
                        worker_version,
                    )
                }) {
                Ok(handle) => handle,
                Err(error) => {
                    queue.close();
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
            completion(RequestExecution {
                request_id,
                result: Err(Error::State(PoolSubmitError::Full.to_string())),
                profile: ExecutionProfile::default(),
                submit_error: Some(PoolSubmitError::Full),
            });
            return Err(PoolSubmitError::Full);
        }
        let job = RequestJob {
            request,
            timeout,
            completion: Box::new(completion),
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
        }
    }
}

impl Drop for WorkerRequestPool {
    fn drop(&mut self) {
        self.queue.close();
        for worker in self.workers.drain(..) {
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}

fn run_worker(
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
            worker.execute_profiled(&version, job.request, job.timeout)
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

fn validate_configuration(max_concurrent_sandboxes: usize, queue_capacity: usize) -> Result<()> {
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
    fn pool_configuration_rejects_zero_values_before_spawning() {
        assert!(validate_configuration(0, 1).is_err());
        assert!(validate_configuration(1, 0).is_err());
        assert!(validate_configuration(1, 1).is_ok());
    }
}
