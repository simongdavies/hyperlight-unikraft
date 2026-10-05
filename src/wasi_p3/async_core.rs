// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Canonical asynchronous primitives used by the WASI Preview 3 adapter lane.
//!
//! These types remain pending until their producer or the deterministic
//! executor wakes them. They never block a host thread and never turn an
//! asynchronous WIT operation into a synchronous call.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};

/// A canonical future was cancelled before producing its value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("canonical future cancelled")]
pub struct FutureCancelled;

enum FutureState<T> {
    Pending,
    Ready(T),
    Cancelled,
    Consumed,
}

struct FutureShared<T> {
    state: FutureState<T>,
    reader: Option<Waker>,
}

/// Producer half of a canonical `future<T>`.
pub struct FutureWriter<T> {
    shared: Arc<Mutex<FutureShared<T>>>,
    finished: bool,
}

/// Consumer half of a canonical `future<T>`.
pub struct FutureReader<T> {
    shared: Arc<Mutex<FutureShared<T>>>,
}

/// Construct the producer and consumer halves of a canonical `future<T>`.
pub fn future<T>() -> (FutureWriter<T>, FutureReader<T>) {
    let shared = Arc::new(Mutex::new(FutureShared {
        state: FutureState::Pending,
        reader: None,
    }));
    (
        FutureWriter {
            shared: shared.clone(),
            finished: false,
        },
        FutureReader { shared },
    )
}

impl<T> FutureWriter<T> {
    /// Complete the future and wake its consumer.
    pub fn complete(mut self, value: T) -> Result<(), T> {
        let mut shared = self.shared.lock().expect("future state lock poisoned");
        match shared.state {
            FutureState::Pending => {
                shared.state = FutureState::Ready(value);
                self.finished = true;
                if let Some(reader) = shared.reader.take() {
                    reader.wake();
                }
                Ok(())
            }
            FutureState::Ready(_) | FutureState::Cancelled | FutureState::Consumed => Err(value),
        }
    }

    /// Report whether the consumer cancelled this future.
    pub fn is_cancelled(&self) -> bool {
        matches!(
            self.shared
                .lock()
                .expect("future state lock poisoned")
                .state,
            FutureState::Cancelled
        )
    }
}

impl<T> Drop for FutureWriter<T> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut shared = self.shared.lock().expect("future state lock poisoned");
        if matches!(shared.state, FutureState::Pending) {
            shared.state = FutureState::Cancelled;
            if let Some(reader) = shared.reader.take() {
                reader.wake();
            }
        }
    }
}

impl<T> FutureReader<T> {
    /// Cancel the future without waiting for the producer.
    pub fn cancel(&self) {
        let mut shared = self.shared.lock().expect("future state lock poisoned");
        if matches!(shared.state, FutureState::Pending) {
            shared.state = FutureState::Cancelled;
            shared.reader = None;
        }
    }
}

impl<T> Drop for FutureReader<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl<T> Future for FutureReader<T> {
    type Output = Result<T, FutureCancelled>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.shared.lock().expect("future state lock poisoned");
        match std::mem::replace(&mut shared.state, FutureState::Consumed) {
            FutureState::Pending => {
                shared.state = FutureState::Pending;
                shared.reader = Some(context.waker().clone());
                Poll::Pending
            }
            FutureState::Ready(value) => Poll::Ready(Ok(value)),
            FutureState::Cancelled => Poll::Ready(Err(FutureCancelled)),
            FutureState::Consumed => {
                shared.state = FutureState::Consumed;
                Poll::Ready(Err(FutureCancelled))
            }
        }
    }
}

/// A canonical stream was closed while a producer still had an item to send.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
#[error("canonical stream closed")]
pub struct StreamClosed<T>(pub T);

struct StreamState<T> {
    queue: VecDeque<T>,
    capacity: usize,
    closed: bool,
    reader: Option<Waker>,
    writers: Vec<Waker>,
}

/// Producer half of a bounded canonical `stream<T>`.
pub struct StreamWriter<T> {
    shared: Arc<Mutex<StreamState<T>>>,
}

/// Consumer half of a bounded canonical `stream<T>`.
pub struct StreamReader<T> {
    shared: Arc<Mutex<StreamState<T>>>,
}

/// Construct a bounded canonical `stream<T>`.
///
/// The capacity must be non-zero so a stalled consumer applies real
/// backpressure to its producer.
pub fn stream<T>(capacity: usize) -> (StreamWriter<T>, StreamReader<T>) {
    assert!(capacity > 0, "canonical stream capacity must be non-zero");
    let shared = Arc::new(Mutex::new(StreamState {
        queue: VecDeque::with_capacity(capacity),
        capacity,
        closed: false,
        reader: None,
        writers: Vec::new(),
    }));
    (
        StreamWriter {
            shared: shared.clone(),
        },
        StreamReader { shared },
    )
}

impl<T> StreamWriter<T> {
    /// Send one item, remaining pending while the bounded queue is full.
    pub fn send(&mut self, item: T) -> SendFuture<'_, T> {
        SendFuture {
            writer: self,
            item: Some(item),
        }
    }

    /// Close the stream after all queued items have been consumed.
    pub fn close(&self) {
        let mut shared = self.shared.lock().expect("stream state lock poisoned");
        shared.closed = true;
        if let Some(reader) = shared.reader.take() {
            reader.wake();
        }
    }
}

impl<T> Drop for StreamWriter<T> {
    fn drop(&mut self) {
        self.close();
    }
}

/// Future returned by [`StreamWriter::send`].
pub struct SendFuture<'a, T> {
    writer: &'a mut StreamWriter<T>,
    item: Option<T>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), StreamClosed<T>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let shared = this.writer.shared.clone();
        let mut shared = shared.lock().expect("stream state lock poisoned");
        if shared.closed {
            return Poll::Ready(Err(StreamClosed(
                this.item
                    .take()
                    .expect("send future polled after completion"),
            )));
        }
        if shared.queue.len() == shared.capacity {
            if !shared
                .writers
                .iter()
                .any(|writer| writer.will_wake(context.waker()))
            {
                shared.writers.push(context.waker().clone());
            }
            return Poll::Pending;
        }
        shared.queue.push_back(
            this.item
                .take()
                .expect("send future polled after completion"),
        );
        if let Some(reader) = shared.reader.take() {
            reader.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl<T> StreamReader<T> {
    /// Receive the next item, remaining pending while the stream is open and
    /// empty.
    pub fn read(&mut self) -> NextFuture<'_, T> {
        NextFuture { reader: self }
    }

    /// Cancel the stream and discard queued values.
    pub fn cancel(&self) {
        let mut shared = self.shared.lock().expect("stream state lock poisoned");
        shared.closed = true;
        shared.queue.clear();
        for writer in shared.writers.drain(..) {
            writer.wake();
        }
    }
}

impl<T> Drop for StreamReader<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Future returned by [`StreamReader::read`].
pub struct NextFuture<'a, T> {
    reader: &'a mut StreamReader<T>,
}

impl<T> Future for NextFuture<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self
            .reader
            .shared
            .lock()
            .expect("stream state lock poisoned");
        if let Some(item) = shared.queue.pop_front() {
            for writer in shared.writers.drain(..) {
                writer.wake();
            }
            return Poll::Ready(Some(item));
        }
        if shared.closed {
            return Poll::Ready(None);
        }
        shared.reader = Some(context.waker().clone());
        Poll::Pending
    }
}

/// Stable identity of one executor task.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(u64);

impl TaskId {
    /// Numeric task identity for deterministic evidence.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Observable task lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Runnable,
    Running,
    Waiting,
    Completed,
    Cancelled,
}

struct ReadyState {
    queue: VecDeque<TaskId>,
    queued: HashSet<TaskId>,
}

type TaskFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

struct ExecutorInner {
    tasks: Mutex<BTreeMap<TaskId, TaskFuture>>,
    states: Mutex<BTreeMap<TaskId, TaskState>>,
    ready: Mutex<ReadyState>,
    next_task: AtomicU64,
}

impl ExecutorInner {
    fn schedule(&self, task: TaskId) {
        let terminal = self
            .states
            .lock()
            .expect("executor state lock poisoned")
            .get(&task)
            .is_none_or(|state| matches!(state, TaskState::Completed | TaskState::Cancelled));
        if terminal {
            return;
        }
        let mut ready = self.ready.lock().expect("executor ready lock poisoned");
        if ready.queued.insert(task) {
            ready.queue.push_back(task);
        }
        drop(ready);
        self.states
            .lock()
            .expect("executor state lock poisoned")
            .insert(task, TaskState::Runnable);
    }

    fn pop_ready(&self) -> Option<TaskId> {
        let mut ready = self.ready.lock().expect("executor ready lock poisoned");
        let task = ready.queue.pop_front()?;
        ready.queued.remove(&task);
        Some(task)
    }

    fn is_ready(&self, task: TaskId) -> bool {
        self.ready
            .lock()
            .expect("executor ready lock poisoned")
            .queued
            .contains(&task)
    }
}

struct TaskWake {
    task: TaskId,
    executor: Weak<ExecutorInner>,
}

impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(executor) = self.executor.upgrade() {
            executor.schedule(self.task);
        }
    }
}

/// Result of one deterministic executor step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorStep {
    Idle,
    Pending(TaskId),
    Completed(TaskId),
}

/// Single-threaded, wake-driven executor with deterministic FIFO scheduling.
///
/// Futures may perform asynchronous work on other threads, but polling order
/// and task lifecycle are controlled here and never rely on a blocking
/// `block_on` path.
pub struct DeterministicExecutor {
    inner: Arc<ExecutorInner>,
}

impl Default for DeterministicExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DeterministicExecutor {
    /// Create an empty executor.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ExecutorInner {
                tasks: Mutex::new(BTreeMap::new()),
                states: Mutex::new(BTreeMap::new()),
                ready: Mutex::new(ReadyState {
                    queue: VecDeque::new(),
                    queued: HashSet::new(),
                }),
                next_task: AtomicU64::new(1),
            }),
        }
    }

    /// Spawn one canonical async task.
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> TaskId {
        let task = TaskId(self.inner.next_task.fetch_add(1, Ordering::Relaxed));
        self.inner
            .tasks
            .lock()
            .expect("executor task lock poisoned")
            .insert(task, Box::pin(future));
        self.inner
            .states
            .lock()
            .expect("executor state lock poisoned")
            .insert(task, TaskState::Runnable);
        self.inner.schedule(task);
        task
    }

    /// Poll one ready task.
    pub fn step(&self) -> ExecutorStep {
        let Some(task) = self.inner.pop_ready() else {
            return ExecutorStep::Idle;
        };
        let Some(mut future) = self
            .inner
            .tasks
            .lock()
            .expect("executor task lock poisoned")
            .remove(&task)
        else {
            return ExecutorStep::Idle;
        };
        self.inner
            .states
            .lock()
            .expect("executor state lock poisoned")
            .insert(task, TaskState::Running);
        let waker = Waker::from(Arc::new(TaskWake {
            task,
            executor: Arc::downgrade(&self.inner),
        }));
        let mut context = Context::from_waker(&waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(()) => {
                self.inner
                    .states
                    .lock()
                    .expect("executor state lock poisoned")
                    .insert(task, TaskState::Completed);
                ExecutorStep::Completed(task)
            }
            Poll::Pending => {
                self.inner
                    .tasks
                    .lock()
                    .expect("executor task lock poisoned")
                    .insert(task, future);
                let state = if self.inner.is_ready(task) {
                    TaskState::Runnable
                } else {
                    TaskState::Waiting
                };
                self.inner
                    .states
                    .lock()
                    .expect("executor state lock poisoned")
                    .insert(task, state);
                ExecutorStep::Pending(task)
            }
        }
    }

    /// Poll ready tasks until every remaining task is waiting.
    pub fn run_until_stalled(&self) -> usize {
        let mut polls = 0;
        while !matches!(self.step(), ExecutorStep::Idle) {
            polls += 1;
        }
        polls
    }

    /// Cancel one task. Its future is dropped without another poll.
    pub fn cancel(&self, task: TaskId) -> bool {
        let removed = self
            .inner
            .tasks
            .lock()
            .expect("executor task lock poisoned")
            .remove(&task)
            .is_some();
        if removed {
            let mut ready = self
                .inner
                .ready
                .lock()
                .expect("executor ready lock poisoned");
            ready.queued.remove(&task);
            ready.queue.retain(|queued| *queued != task);
            drop(ready);
            self.inner
                .states
                .lock()
                .expect("executor state lock poisoned")
                .insert(task, TaskState::Cancelled);
        }
        removed
    }

    /// Read the current lifecycle state of a task.
    pub fn state(&self, task: TaskId) -> Option<TaskState> {
        self.inner
            .states
            .lock()
            .expect("executor state lock poisoned")
            .get(&task)
            .copied()
    }
}
