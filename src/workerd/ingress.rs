// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Bounded, nonblocking guest transport. A network adapter must retain its
//! instance/capacity admission until guest CallDone, not merely an end frame.
use super::{
    ControlRequest, Error, FrameKind, InvocationCancellation, MAX_FRAME_BYTES, Result, StreamFrame,
    StreamState,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_host::func::Registerable;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

const FRAME_QUEUE: usize = 4;
const MAX_STREAM_BYTES: u64 = 8 * 1024 * 1024;

pub(super) enum PoolInvocation {
    Buffered(super::InvocationRequest),
    Stream {
        request: super::RequestEnvelope,
        websocket: bool,
        ingress: GuestIngress,
    },
}

impl PoolInvocation {
    pub(super) fn request_id(&self) -> &str {
        match self {
            Self::Buffered(request) => request.request_id(),
            Self::Stream { request, .. } => &request.request_id,
        }
    }

    pub(super) fn execute_worker(
        self,
        worker: &super::WorkerVersionSandbox,
        version: &super::WorkerVersionId,
        timeout: Duration,
        cancellation: InvocationCancellation,
    ) -> (Result<super::InvocationResponse>, super::ExecutionProfile) {
        match self {
            Self::Buffered(request) => {
                worker.execute_invocation_cancellable(version, request, timeout, cancellation)
            }
            Self::Stream {
                request,
                websocket,
                ingress,
            } => (
                worker
                    .execute_stream(request, websocket, ingress, timeout)
                    .map(|()| super::InvocationResponse::StreamComplete),
                Default::default(),
            ),
        }
    }

    pub(super) fn execute_resident(
        self,
        resident: &mut super::ResidentWorkerSandbox,
        timeout: Duration,
        cancellation: InvocationCancellation,
    ) -> (Result<super::InvocationResponse>, super::ExecutionProfile) {
        match self {
            Self::Buffered(request) => resident.execute_cancellable(request, timeout, cancellation),
            Self::Stream {
                request,
                websocket,
                ingress,
            } => (
                resident
                    .execute_stream(request, websocket, ingress, timeout)
                    .map(|()| super::InvocationResponse::StreamComplete),
                Default::default(),
            ),
        }
    }

    pub(super) fn execute_restored(
        self,
        mut restored: super::sandbox::RestoredWorkerVersionSandbox,
        timeout: Duration,
        total_started: Instant,
        cancellation: InvocationCancellation,
        teardown_started: impl FnOnce(),
        teardown_finished: impl FnOnce(),
    ) -> (Result<super::InvocationResponse>, super::ExecutionProfile) {
        match self {
            Self::Buffered(request) => restored.execute_invocation_with_teardown_observer(
                request,
                timeout,
                total_started,
                cancellation,
                teardown_started,
                teardown_finished,
            ),
            Self::Stream {
                request,
                websocket,
                ingress,
            } => {
                let result = restored.execute_stream(request, websocket, ingress, timeout);
                teardown_started();
                drop(restored);
                teardown_finished();
                (
                    result.map(|()| super::InvocationResponse::StreamComplete),
                    Default::default(),
                )
            }
        }
    }
}

impl From<super::InvocationRequest> for PoolInvocation {
    fn from(request: super::InvocationRequest) -> Self {
        Self::Buffered(request)
    }
}

impl From<super::RequestEnvelope> for PoolInvocation {
    fn from(request: super::RequestEnvelope) -> Self {
        Self::Buffered(request.into())
    }
}

#[derive(Serialize)]
struct SendAck<'a> {
    protocol_version: u16,
    request_id: &'a str,
    sequence: u64,
    accepted: bool,
    cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
}

struct Transport {
    request_id: String,
    deadline: Instant,
    cancellation: InvocationCancellation,
    completed: AtomicBool,
    outbound: Mutex<(StreamState, mpsc::SyncSender<StreamFrame>, u64)>,
    inbound: Mutex<(mpsc::Receiver<StreamFrame>, u64)>,
}

/// Move to the sandbox's single owner thread; host-call polling stays local
/// to that owner rather than creating a thread/task per frame.
#[derive(Clone)]
pub struct GuestIngress(Arc<Transport>);

pub struct HostIngress {
    transport: Arc<Transport>,
    outbound: mpsc::Receiver<StreamFrame>,
    inbound: mpsc::SyncSender<StreamFrame>,
    next_sequence: u64,
    input_bytes: u64,
    ended: bool,
}

impl HostIngress {
    pub(super) fn is_completed(&self) -> bool {
        self.transport.completed.load(Ordering::Acquire)
    }

    pub(super) fn remaining(&self) -> Duration {
        self.transport
            .deadline
            .saturating_duration_since(Instant::now())
    }
    pub fn pair(
        request_id: impl Into<String>,
        websocket: bool,
        timeout: Duration,
        cancellation: InvocationCancellation,
    ) -> Result<(Self, GuestIngress)> {
        let request_id = request_id.into();
        ControlRequest::new(&request_id)?;
        if timeout.is_zero() {
            return Err(Error::Timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::State("ingress lifetime too large".into()))?;
        let (output_tx, output_rx) = mpsc::sync_channel(FRAME_QUEUE);
        let (input_tx, input_rx) = mpsc::sync_channel(FRAME_QUEUE);
        let transport = Arc::new(Transport {
            request_id: request_id.clone(),
            deadline,
            cancellation: cancellation.clone(),
            completed: AtomicBool::new(false),
            outbound: Mutex::new((
                StreamState::new(request_id, websocket, cancellation)?,
                output_tx,
                0,
            )),
            inbound: Mutex::new((input_rx, 0)),
        });
        Ok((
            Self {
                transport: transport.clone(),
                outbound: output_rx,
                inbound: input_tx,
                next_sequence: 0,
                input_bytes: 0,
                ended: false,
            },
            GuestIngress(transport),
        ))
    }

    pub fn receive(&self, wait: Duration) -> Result<Option<StreamFrame>> {
        let remaining = self
            .transport
            .deadline
            .saturating_duration_since(Instant::now());
        match self.outbound.recv_timeout(wait.min(remaining)) {
            Ok(frame) => Ok(Some(frame)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if remaining.is_zero() {
                    self.cancel();
                    return Err(Error::Timeout);
                }
                if self.transport.cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                Ok(None)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(Error::State("guest ingress owner disconnected".into()))
            }
        }
    }

    /// false means backpressure or completed guest input; consumes no accounting.
    pub fn try_send(&mut self, kind: FrameKind, bytes: &[u8], opcode: Option<u8>) -> Result<bool> {
        if self.transport.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if self.is_completed() {
            return Ok(false);
        }
        if self.ended {
            return Err(Error::State("ingress already completed".into()));
        }
        if Instant::now() >= self.transport.deadline {
            self.cancel();
            return Err(Error::Timeout);
        }
        if bytes.len() > MAX_FRAME_BYTES
            || self.input_bytes + bytes.len() as u64 > MAX_STREAM_BYTES
            || !matches!(
                kind,
                FrameKind::Data | FrameKind::Websocket | FrameKind::End
            )
            || (kind == FrameKind::End && (!bytes.is_empty() || opcode.is_some()))
        {
            return Err(Error::Protocol(
                "invalid inbound stream frame or byte budget exceeded".into(),
            ));
        }
        let frame = StreamFrame {
            protocol_version: 1,
            request_id: self.transport.request_id.clone(),
            sequence: self.next_sequence,
            kind,
            status: None,
            headers: None,
            body_base64: (kind != FrameKind::End).then(|| STANDARD.encode(bytes)),
            opcode,
            error: None,
        };
        frame.validate(&self.transport.request_id)?;
        match self.inbound.try_send(frame) {
            Ok(()) => {
                self.input_bytes += bytes.len() as u64;
                self.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Protocol("inbound sequence exhausted".into()))?;
                self.ended = kind == FrameKind::End;
                Ok(true)
            }
            Err(mpsc::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::State("guest ingress receiver disconnected".into()))
            }
        }
    }

    pub fn cancel(&self) {
        self.transport.cancellation.cancel();
    }
}

impl Drop for HostIngress {
    fn drop(&mut self) {
        if !self.transport.completed.load(Ordering::Acquire) {
            self.cancel();
        }
    }
}

impl GuestIngress {
    pub(super) fn request_id(&self) -> &str {
        &self.0.request_id
    }
    pub(super) fn deadline(&self) -> Instant {
        self.0.deadline
    }
    pub(super) fn cancellation(&self) -> InvocationCancellation {
        self.0.cancellation.clone()
    }
    pub(super) fn finish(&self, drained: bool) -> Result<()> {
        self.0.completed.store(true, Ordering::Release);
        self.0
            .outbound
            .lock()
            .map_err(|_| Error::State("ingress output poisoned".into()))?
            .0
            .finish(drained)
    }
    pub(super) fn abort(&self) {
        self.0.cancellation.cancel();
        self.0.completed.store(true, Ordering::Release);
    }

    pub(super) fn send(&self, json: &str) -> Result<String> {
        let frame: StreamFrame = super::control::decode(json.as_bytes())?;
        let sequence = frame.sequence;
        let mut state = self
            .0
            .outbound
            .lock()
            .map_err(|_| Error::State("ingress output poisoned".into()))?;
        let mut candidate = state.0.clone();
        let error = candidate.accept(&frame).err();
        let (accepted, cancelled, error) =
            if self.0.cancellation.is_cancelled() || Instant::now() >= self.0.deadline {
                self.0.cancellation.cancel();
                (false, true, None)
            } else if let Some(error) = error {
                (false, false, Some(error.to_string()))
            } else if state.2 + frame.decoded_body()?.len() as u64 > MAX_STREAM_BYTES {
                (false, false, Some("stream byte budget exceeded".into()))
            } else {
                let byte_count = frame.decoded_body()?.len() as u64;
                match state.1.try_send(frame) {
                    Ok(()) => {
                        state.0 = candidate;
                        state.2 += byte_count;
                        (true, false, None)
                    }
                    Err(mpsc::TrySendError::Full(_)) => (false, false, Some("backpressure".into())),
                    Err(mpsc::TrySendError::Disconnected(_)) => {
                        self.0.cancellation.cancel();
                        (false, true, None)
                    }
                }
            };
        Ok(serde_json::to_string(&SendAck {
            protocol_version: 1,
            request_id: &self.0.request_id,
            sequence,
            accepted,
            cancelled,
            error: error.as_deref(),
        })?)
    }

    pub(super) fn receive(&self, json: &str) -> Result<String> {
        let query: ControlRequest = super::control::decode(json.as_bytes())?;
        if query.protocol_version != 1 || query.request_id != self.0.request_id {
            return Err(Error::Protocol("ingress receive request mismatch".into()));
        }
        let mut input = self
            .0
            .inbound
            .lock()
            .map_err(|_| Error::State("ingress input poisoned".into()))?;
        let frame = if self.0.cancellation.is_cancelled() || Instant::now() >= self.0.deadline {
            self.0.cancellation.cancel();
            terminal(&self.0.request_id, input.1, FrameKind::Cancel)
        } else {
            match input.0.try_recv() {
                Ok(frame) => {
                    if frame.sequence != input.1 {
                        return Err(Error::Protocol("inbound stream sequence mismatch".into()));
                    }
                    input.1 += 1;
                    frame
                }
                Err(mpsc::TryRecvError::Empty) => {
                    terminal(&self.0.request_id, input.1, FrameKind::Pending)
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.0.cancellation.cancel();
                    terminal(&self.0.request_id, input.1, FrameKind::Cancel)
                }
            }
        };
        Ok(serde_json::to_string(&frame)?)
    }
}

#[derive(Clone, Default)]
pub(super) struct IngressSession(Arc<Mutex<Option<GuestIngress>>>);

impl IngressSession {
    pub(super) fn attach(&self, ingress: GuestIngress) -> Result<()> {
        let mut session = self
            .0
            .lock()
            .map_err(|_| Error::State("ingress session poisoned".into()))?;
        if session.is_some() {
            return Err(Error::State("ingress session already active".into()));
        }
        *session = Some(ingress);
        Ok(())
    }
    pub(super) fn detach(&self) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| Error::State("ingress session poisoned".into()))?
            .take();
        Ok(())
    }
    pub(super) fn register(&self, target: &mut impl Registerable) -> Result<()> {
        let sender = self.clone();
        target.register_host_function(
            "WorkerdIngressV1Send",
            move |json: String| -> hyperlight_host::Result<String> {
                sender
                    .active()
                    .and_then(|ingress| ingress.send(&json))
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        let receiver = self.clone();
        target.register_host_function(
            "WorkerdIngressV1Receive",
            move |json: String| -> hyperlight_host::Result<String> {
                receiver
                    .active()
                    .and_then(|ingress| ingress.receive(&json))
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        Ok(())
    }
    fn active(&self) -> Result<GuestIngress> {
        self.0
            .lock()
            .map_err(|_| Error::State("ingress session poisoned".into()))?
            .clone()
            .ok_or_else(|| Error::State("ingress transport not admitted".into()))
    }
}

fn terminal(id: &str, sequence: u64, kind: FrameKind) -> StreamFrame {
    StreamFrame {
        protocol_version: 1,
        request_id: id.into(),
        sequence,
        kind,
        status: None,
        headers: None,
        body_base64: None,
        opcode: None,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_and_backpressure_do_not_consume_sequences_and_cancel_is_visible() {
        let (host, guest) = HostIngress::pair(
            "stream-1",
            false,
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .unwrap();
        let query = r#"{"protocol_version":1,"request_id":"stream-1"}"#;
        assert!(
            guest
                .receive(query)
                .unwrap()
                .contains("\"kind\":\"pending\"")
        );
        let headers = r#"{"protocol_version":1,"request_id":"stream-1","sequence":0,"kind":"headers","status":200,"headers":[]}"#;
        assert!(guest.send(headers).unwrap().contains("\"accepted\":true"));
        for sequence in 1..FRAME_QUEUE {
            let frame = serde_json::json!({"protocol_version":1,"request_id":"stream-1","sequence":sequence,"kind":"data","body_base64":"aGk="});
            assert!(
                guest
                    .send(&frame.to_string())
                    .unwrap()
                    .contains("\"accepted\":true")
            );
        }
        let blocked = serde_json::json!({"protocol_version":1,"request_id":"stream-1","sequence":FRAME_QUEUE,"kind":"data","body_base64":"aGk="});
        assert!(
            guest
                .send(&blocked.to_string())
                .unwrap()
                .contains("\"error\":\"backpressure\"")
        );
        assert!(guest.receive(query).unwrap().contains("\"sequence\":0"));
        host.receive(Duration::from_millis(1)).unwrap().unwrap();
        assert!(
            guest
                .send(&blocked.to_string())
                .unwrap()
                .contains("\"accepted\":true")
        );
        host.cancel();
        assert!(
            guest
                .receive(query)
                .unwrap()
                .contains("\"kind\":\"cancel\"")
        );
        assert!(
            guest
                .send(&blocked.to_string())
                .unwrap()
                .contains("\"cancelled\":true")
        );
    }
}
