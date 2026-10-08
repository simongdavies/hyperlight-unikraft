// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const TIMER_PROTOCOL_VERSION: u32 = 1;
const MAX_TIMER_ID: u64 = (1u64 << 53) - 1;

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TimerLimits {
    pub max_active_timers: usize,
    pub max_unreleased_handles: usize,
}

impl Default for TimerLimits {
    fn default() -> Self {
        Self {
            max_active_timers: 1024,
            max_unreleased_handles: 4096,
        }
    }
}

#[derive(Clone)]
pub(crate) struct TimerBroker {
    inner: Arc<TimerBrokerInner>,
}

struct TimerBrokerInner {
    limits: TimerLimits,
    active: AtomicUsize,
    clock: Arc<dyn Clock>,
}

trait Clock: Send + Sync {
    fn now(&self) -> Instant;

    fn checked_deadline(&self, delay: Duration) -> Option<Instant> {
        self.now().checked_add(delay)
    }
}

struct MonotonicClock;

impl Clock for MonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

impl std::fmt::Debug for TimerBroker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TimerBroker")
            .field("limits", &self.inner.limits)
            .field("active", &self.inner.active.load(Ordering::Acquire))
            .finish()
    }
}

impl Default for TimerBroker {
    fn default() -> Self {
        Self::new(TimerLimits::default()).expect("default timer limits are valid")
    }
}

impl TimerBroker {
    pub(crate) fn new(limits: TimerLimits) -> Result<Self> {
        Self::with_clock(limits, Arc::new(MonotonicClock))
    }

    fn with_clock(limits: TimerLimits, clock: Arc<dyn Clock>) -> Result<Self> {
        if limits.max_active_timers == 0
            || limits.max_unreleased_handles == 0
            || limits.max_unreleased_handles < limits.max_active_timers
        {
            return Err(Error::State(
                "timer limits must be nonzero and unreleased handles must cover active timers"
                    .into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(TimerBrokerInner {
                limits,
                active: AtomicUsize::new(0),
                clock,
            }),
        })
    }

    pub(crate) fn session(&self) -> TimerSession {
        TimerSession {
            inner: Arc::new(TimerSessionInner {
                broker: self.clone(),
                next_id: AtomicU64::new(1),
                closed: AtomicBool::new(false),
                timers: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn register(
        &self,
        target: &mut impl hyperlight_host::func::Registerable,
        session: TimerSession,
    ) -> Result<()> {
        let start = session.clone();
        target.register_host_function(
            "WorkerdTimerV1Start",
            move |delay_ns: u64| -> hyperlight_host::Result<String> {
                tracing::debug!(delay_ns, "WorkerdTimerV1Start host operation");
                let response = start
                    .start_json(delay_ns)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))?;
                tracing::debug!(%response, "WorkerdTimerV1Start host result");
                Ok(response)
            },
        )?;
        let read = session.clone();
        target.register_host_function(
            "WorkerdTimerV1Read",
            move |timer_id: u64| -> hyperlight_host::Result<String> {
                tracing::debug!(timer_id, "WorkerdTimerV1Read host operation");
                let response = read
                    .read_json(timer_id)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))?;
                tracing::debug!(%response, "WorkerdTimerV1Read host result");
                Ok(response)
            },
        )?;
        target.register_host_function(
            "WorkerdTimerV1Cancel",
            move |timer_id: u64| -> hyperlight_host::Result<i32> {
                tracing::debug!(timer_id, "WorkerdTimerV1Cancel host operation");
                let response = session.cancel(timer_id);
                tracing::debug!(timer_id, response, "WorkerdTimerV1Cancel host result");
                Ok(response)
            },
        )?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TimerSession {
    inner: Arc<TimerSessionInner>,
}

struct TimerSessionInner {
    broker: TimerBroker,
    next_id: AtomicU64,
    closed: AtomicBool,
    timers: Mutex<HashMap<u64, TimerEntry>>,
}

struct TimerEntry {
    deadline: Instant,
    state: TimerState,
    admission: Option<TimerAdmission>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimerState {
    Pending,
    Cancelled,
}

struct TimerAdmission {
    broker: Arc<TimerBrokerInner>,
}

impl TimerAdmission {
    fn acquire(broker: Arc<TimerBrokerInner>) -> std::result::Result<Self, TimerFailure> {
        broker
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < broker.limits.max_active_timers).then_some(active + 1)
            })
            .map_err(|_| TimerFailure::overloaded("active timer limit reached"))?;
        Ok(Self { broker })
    }
}

impl Drop for TimerAdmission {
    fn drop(&mut self) {
        self.broker.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum TimerErrorCode {
    InvalidDuration,
    Overloaded,
    UnknownTimer,
}

#[derive(Debug)]
struct TimerFailure {
    code: TimerErrorCode,
    message: String,
}

impl TimerFailure {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: TimerErrorCode::InvalidDuration,
            message: message.into(),
        }
    }

    fn overloaded(message: impl Into<String>) -> Self {
        Self {
            code: TimerErrorCode::Overloaded,
            message: message.into(),
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TimerResponse {
    protocol_version: u32,
    timer_id: u64,
    state: TimerResponseState,
    error: Option<TimerError>,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TimerResponseState {
    Pending,
    Fired,
    Cancelled,
    Error,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TimerError {
    code: TimerErrorCode,
    message: String,
}

impl TimerResponse {
    fn state(timer_id: u64, state: TimerResponseState) -> Self {
        Self {
            protocol_version: TIMER_PROTOCOL_VERSION,
            timer_id,
            state,
            error: None,
        }
    }

    fn error(timer_id: u64, failure: TimerFailure) -> Self {
        Self {
            protocol_version: TIMER_PROTOCOL_VERSION,
            timer_id,
            state: TimerResponseState::Error,
            error: Some(TimerError {
                code: failure.code,
                message: failure.message,
            }),
        }
    }
}

impl TimerSession {
    pub(super) fn sequence_state(&self) -> u64 {
        self.inner.next_id.load(Ordering::Acquire)
    }
    pub(super) fn restore_sequence(&self, next: u64) -> Result<()> {
        if next == 0 || next > MAX_TIMER_ID {
            return Err(Error::Snapshot(
                "invalid checkpoint timer handle watermark".into(),
            ));
        }
        self.inner.next_id.store(next, Ordering::Release);
        Ok(())
    }

    pub(crate) fn is_quiescent(&self) -> Result<bool> {
        Ok(self
            .inner
            .timers
            .lock()
            .map_err(|_| Error::State("timer state poisoned".into()))?
            .is_empty())
    }

    fn start_json(&self, delay_ns: u64) -> Result<String> {
        let response = match self.start(delay_ns) {
            Ok(timer_id) => TimerResponse::state(timer_id, TimerResponseState::Pending),
            Err(error) => TimerResponse::error(0, error),
        };
        Ok(serde_json::to_string(&response)?)
    }

    fn start(&self, delay_ns: u64) -> std::result::Result<u64, TimerFailure> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TimerFailure::overloaded("timer session is closing"));
        }
        let deadline = self
            .inner
            .broker
            .inner
            .clock
            .checked_deadline(Duration::from_nanos(delay_ns))
            .ok_or_else(|| TimerFailure::invalid("timer duration overflows monotonic time"))?;
        let mut timers = self
            .inner
            .timers
            .lock()
            .map_err(|_| TimerFailure::overloaded("timer state is unavailable"))?;
        if timers.len() >= self.inner.broker.inner.limits.max_unreleased_handles {
            return Err(TimerFailure::overloaded(
                "unreleased timer handle limit reached",
            ));
        }
        let admission = TimerAdmission::acquire(self.inner.broker.inner.clone())?;
        let timer_id = self.inner.next_id.fetch_add(1, Ordering::AcqRel);
        if timer_id == 0 || timer_id > MAX_TIMER_ID {
            return Err(TimerFailure::overloaded("timer ID space exhausted"));
        }
        timers.insert(
            timer_id,
            TimerEntry {
                deadline,
                state: TimerState::Pending,
                admission: Some(admission),
            },
        );
        Ok(timer_id)
    }

    fn read_json(&self, timer_id: u64) -> Result<String> {
        let response = self.read(timer_id)?;
        Ok(serde_json::to_string(&response)?)
    }

    fn read(&self, timer_id: u64) -> Result<TimerResponse> {
        let mut timers = self
            .inner
            .timers
            .lock()
            .map_err(|_| Error::State("timer state poisoned".into()))?;
        let Some(timer) = timers.get_mut(&timer_id) else {
            return Ok(TimerResponse::error(
                timer_id,
                TimerFailure {
                    code: TimerErrorCode::UnknownTimer,
                    message: "unknown or released timer".into(),
                },
            ));
        };
        if timer.state == TimerState::Cancelled {
            timers.remove(&timer_id);
            return Ok(TimerResponse::state(
                timer_id,
                TimerResponseState::Cancelled,
            ));
        }
        if self.inner.broker.inner.clock.now() >= timer.deadline {
            timer.admission.take();
            timers.remove(&timer_id);
            return Ok(TimerResponse::state(timer_id, TimerResponseState::Fired));
        }
        Ok(TimerResponse::state(timer_id, TimerResponseState::Pending))
    }

    fn cancel(&self, timer_id: u64) -> i32 {
        let Ok(mut timers) = self.inner.timers.lock() else {
            return -crate::errno::EIO;
        };
        let Some(timer) = timers.get_mut(&timer_id) else {
            return -crate::errno::ENOENT;
        };
        timer.state = TimerState::Cancelled;
        timer.admission.take();
        0
    }

    pub(crate) fn cancel_all(&self) {
        self.inner.closed.store(true, Ordering::Release);
        if let Ok(mut timers) = self.inner.timers.lock() {
            timers.clear();
        }
    }
}

impl Drop for TimerSessionInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(timers) = self.timers.get_mut() {
            timers.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeClock {
        origin: Instant,
        elapsed_ns: AtomicU64,
        reject_deadline: AtomicBool,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                origin: Instant::now(),
                elapsed_ns: AtomicU64::new(0),
                reject_deadline: AtomicBool::new(false),
            }
        }

        fn advance(&self, duration: Duration) {
            self.elapsed_ns
                .fetch_add(duration.as_nanos() as u64, Ordering::AcqRel);
        }

        fn reject_next_deadline(&self) {
            self.reject_deadline.store(true, Ordering::Release);
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            self.origin + Duration::from_nanos(self.elapsed_ns.load(Ordering::Acquire))
        }

        fn checked_deadline(&self, delay: Duration) -> Option<Instant> {
            if self.reject_deadline.swap(false, Ordering::AcqRel) {
                None
            } else {
                self.now().checked_add(delay)
            }
        }
    }

    fn parsed(json: String) -> serde_json::Value {
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn limits_reject_zero_and_invalid_relationships() {
        assert!(
            TimerBroker::new(TimerLimits {
                max_active_timers: 0,
                max_unreleased_handles: 1,
            })
            .is_err()
        );
        assert!(
            TimerBroker::new(TimerLimits {
                max_active_timers: 2,
                max_unreleased_handles: 1,
            })
            .is_err()
        );
        assert!(TimerBroker::new(TimerLimits::default()).is_ok());
    }

    #[test]
    fn fake_clock_drives_pending_fired_release_and_zero_delay() {
        let clock = Arc::new(FakeClock::new());
        let broker = TimerBroker::with_clock(TimerLimits::default(), clock.clone()).unwrap();
        let session = broker.session();
        let timer_id = parsed(session.start_json(10).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        assert_eq!(
            parsed(session.read_json(timer_id).unwrap())["state"],
            "pending"
        );
        clock.advance(Duration::from_nanos(10));
        assert_eq!(
            parsed(session.read_json(timer_id).unwrap())["state"],
            "fired"
        );
        let released = parsed(session.read_json(timer_id).unwrap());
        assert_eq!(released["state"], "error");
        assert_eq!(released["error"]["code"], "unknown_timer");

        let immediate = parsed(session.start_json(0).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        assert_eq!(
            parsed(session.read_json(immediate).unwrap())["state"],
            "fired"
        );
    }

    #[test]
    fn cancellation_wins_until_terminal_read_and_is_idempotent() {
        let clock = Arc::new(FakeClock::new());
        let broker = TimerBroker::with_clock(TimerLimits::default(), clock.clone()).unwrap();
        let session = broker.session();
        let timer_id = parsed(session.start_json(1).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        clock.advance(Duration::from_nanos(1));
        assert_eq!(session.cancel(timer_id), 0);
        assert_eq!(session.cancel(timer_id), 0);
        assert_eq!(
            parsed(session.read_json(timer_id).unwrap())["state"],
            "cancelled"
        );
        assert_eq!(session.cancel(timer_id), -crate::errno::ENOENT);
    }

    #[test]
    fn active_and_unreleased_limits_are_independent_and_drop_releases() {
        let clock = Arc::new(FakeClock::new());
        let broker = TimerBroker::with_clock(
            TimerLimits {
                max_active_timers: 1,
                max_unreleased_handles: 2,
            },
            clock,
        )
        .unwrap();
        let first = broker.session();
        let second = broker.session();
        let first_id = parsed(first.start_json(10).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        assert_eq!(
            parsed(second.start_json(10).unwrap())["error"]["code"],
            "overloaded"
        );
        assert_eq!(first.cancel(first_id), 0);
        let second_id = parsed(second.start_json(10).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        assert_ne!(second_id, 0);
        assert_eq!(second.cancel(second_id), 0);

        let another = parsed(first.start_json(10).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        assert_eq!(first.cancel(another), 0);
        assert_eq!(
            parsed(first.start_json(10).unwrap())["error"]["code"],
            "overloaded"
        );
        drop(first);
        assert_eq!(broker.inner.active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn overflow_is_invalid_and_sessions_are_isolated() {
        let clock = Arc::new(FakeClock::new());
        let broker = TimerBroker::with_clock(TimerLimits::default(), clock.clone()).unwrap();
        let first = broker.session();
        let second = broker.session();
        clock.reject_next_deadline();
        let overflow = parsed(first.start_json(u64::MAX).unwrap());
        assert_eq!(overflow["timer_id"], 0);
        assert_eq!(overflow["state"], "error");
        assert_eq!(overflow["error"]["code"], "invalid_duration");

        let timer_id = parsed(first.start_json(1).unwrap())["timer_id"]
            .as_u64()
            .unwrap();
        assert_eq!(
            parsed(second.read_json(timer_id).unwrap())["error"]["code"],
            "unknown_timer"
        );
        first.cancel_all();
        assert_eq!(broker.inner.active.load(Ordering::Acquire), 0);
        assert_eq!(
            parsed(first.start_json(1).unwrap())["error"]["code"],
            "overloaded"
        );
    }

    #[test]
    fn json_shapes_and_json_safe_id_limit_are_exact() {
        let clock = Arc::new(FakeClock::new());
        let broker = TimerBroker::with_clock(TimerLimits::default(), clock.clone()).unwrap();
        let session = broker.session();
        session.inner.next_id.store(MAX_TIMER_ID, Ordering::Release);

        let started = parsed(session.start_json(1).unwrap());
        assert_eq!(
            started,
            serde_json::json!({
                "protocol_version": TIMER_PROTOCOL_VERSION,
                "timer_id": MAX_TIMER_ID,
                "state": "pending",
                "error": null
            })
        );
        assert_eq!(
            parsed(session.start_json(1).unwrap()),
            serde_json::json!({
                "protocol_version": TIMER_PROTOCOL_VERSION,
                "timer_id": 0,
                "state": "error",
                "error": {
                    "code": "overloaded",
                    "message": "timer ID space exhausted"
                }
            })
        );

        let unknown = parsed(session.read_json(7).unwrap());
        assert_eq!(
            unknown,
            serde_json::json!({
                "protocol_version": TIMER_PROTOCOL_VERSION,
                "timer_id": 7,
                "state": "error",
                "error": {
                    "code": "unknown_timer",
                    "message": "unknown or released timer"
                }
            })
        );
        clock.advance(Duration::from_nanos(1));
        assert_eq!(
            parsed(session.read_json(MAX_TIMER_ID).unwrap()),
            serde_json::json!({
                "protocol_version": TIMER_PROTOCOL_VERSION,
                "timer_id": MAX_TIMER_ID,
                "state": "fired",
                "error": null
            })
        );
    }
}
