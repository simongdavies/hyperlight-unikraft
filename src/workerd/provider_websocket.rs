// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, FetchCredential, FetchPolicy, Result};
use crate::broker::{
    BrokerEndpoint, BrokerLimits, BrokerPolicy, BrokerProtocol, DnsPolicy, EgressRule,
    EndpointHost, HostRule, PortRange,
};
use crate::broker_network::{NetworkBrokerConfig, default_tls_roots, provider_tls_transport};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Shutdown};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tungstenite::{Message, client::IntoClientRequest};

const MAX_HANDLE: u64 = 9_007_199_254_740_991;
const MAX_FRAME: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWebSocketLimits {
    pub max_connections: usize,
    pub queue_frames: usize,
    pub max_frame_bytes: usize,
    pub max_total_bytes: u64,
    pub connect_timeout_ms: u64,
}

impl ProviderWebSocketLimits {
    fn validate(&self) -> Result<()> {
        if !(1..=4).contains(&self.max_connections)
            || !(1..=4).contains(&self.queue_frames)
            || !(1..=MAX_FRAME).contains(&self.max_frame_bytes)
            || self.max_total_bytes == 0
            || self.max_total_bytes > 8 * 1024 * 1024
            || !(1..=5000).contains(&self.connect_timeout_ms)
        {
            return Err(Error::State(
                "invalid finite provider WebSocket limits".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWebSocketConfig {
    pub name: String,
    pub hosts: Vec<String>,
    pub ports: Vec<u16>,
    pub paths: Vec<String>,
    pub query_parameters: BTreeMap<String, Vec<String>>,
    pub subprotocols: Vec<String>,
    pub ip_ranges: Vec<ipnet::IpNet>,
    pub resolver_identity: IpAddr,
    #[serde(default)]
    pub allow_loopback: bool,
    #[serde(default)]
    pub allow_private: bool,
    pub credential: FetchCredential,
    pub limits: ProviderWebSocketLimits,
    pub session_policy: Option<ProviderWebSocketSessionPolicy>,
    pub trusted_ca_file: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWebSocketSessionPolicy {
    pub discriminator_pointer: String,
    pub control_type: String,
    pub required_values: BTreeMap<String, serde_json::Value>,
}

impl ProviderWebSocketSessionPolicy {
    fn validate(&self) -> Result<()> {
        if !self.discriminator_pointer.starts_with('/')
            || self.control_type.is_empty()
            || self.required_values.is_empty()
            || self.required_values.len() > 16
            || self.required_values.iter().any(|(pointer, value)| {
                !pointer.starts_with('/')
                    || pointer.len() > 256
                    || value.is_array()
                    || value.is_object()
                    || value.is_null()
            })
        {
            return Err(Error::State(
                "invalid provider initial-control scalar grants".into(),
            ));
        }
        Ok(())
    }

    fn check(&self, opcode: u8, bytes: &[u8], initialized: bool) -> Result<bool> {
        if opcode != 1 {
            return if initialized {
                Ok(false)
            } else {
                Err(Error::State(
                    "provider requires initial control before audio".into(),
                ))
            };
        }
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value
            .pointer(&self.discriminator_pointer)
            .and_then(serde_json::Value::as_str)
            != Some(self.control_type.as_str())
        {
            return if initialized {
                Ok(false)
            } else {
                Err(Error::State("provider initial control missing".into()))
            };
        }
        if self
            .required_values
            .iter()
            .any(|(pointer, expected)| value.pointer(pointer) != Some(expected))
        {
            return Err(Error::State(
                "provider control violates admitted resource/model".into(),
            ));
        }
        Ok(true)
    }
}

impl ProviderWebSocketConfig {
    pub(super) fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if let Some(policy) = &self.session_policy {
            policy.validate()?;
        }
        self.credential.validate()?;
        if self.name.is_empty()
            || self.name.len() > 64
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || self.hosts.is_empty()
            || self.paths.is_empty()
            || self.ip_ranges.is_empty()
            || self.ports.is_empty()
            || self.ports.contains(&0)
            || self.credential.scheme != "https"
            || !self.hosts.contains(&self.credential.host)
            || !self.ports.contains(&self.credential.port)
            || self.subprotocols.len() > 8
            || self.subprotocols.iter().any(|value| !valid_protocol(value))
            || self.query_parameters.keys().any(|name| {
                matches!(
                    name.to_ascii_lowercase().as_str(),
                    "api-key" | "api_key" | "token" | "access_token" | "authorization" | "sig"
                )
            })
        {
            return Err(Error::State("provider WebSocket requires a named TLS resource, destination-bound host credential and explicit IP/path grants".into()));
        }
        self.policy()?;
        Ok(())
    }

    fn policy(&self) -> Result<FetchPolicy> {
        let hosts = crate::AllowList::from_hosts(&self.hosts)
            .map_err(|error| Error::State(format!("invalid provider host grant: {error}")))?;
        FetchPolicy::new(
            crate::NetworkPolicy::AllowList(hosts),
            ["https"],
            self.ports.clone(),
        )
        .allow_loopback(self.allow_loopback)
        .allow_private(self.allow_private)
        .with_methods(["GET".into()])?
        .with_ip_ranges(self.ip_ranges.clone())
        .with_resource_scope(self.paths.clone(), self.query_parameters.clone(), None)
    }

    fn network(&self, roots: Arc<rustls::RootCertStore>) -> Result<NetworkBrokerConfig> {
        let mut names = Vec::new();
        let mut rules = Vec::new();
        for name in &self.hosts {
            let host = match name.parse::<IpAddr>() {
                Ok(ip) => HostRule::Ip(ip),
                Err(_) => {
                    let parsed =
                        name.parse()
                            .map_err(|error: crate::broker::BrokerContractError| {
                                Error::State(error.to_string())
                            })?;
                    names.push(parsed);
                    HostRule::ExactDns(name.parse().map_err(
                        |error: crate::broker::BrokerContractError| Error::State(error.to_string()),
                    )?)
                }
            };
            let ports = self
                .ports
                .iter()
                .map(|port| PortRange::new(*port, *port))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| Error::State(error.to_string()))?;
            rules.push(
                EgressRule::new(host, ports, vec![BrokerProtocol::WebSocket])
                    .map_err(|error| Error::State(error.to_string()))?,
            );
        }
        let policy = BrokerPolicy::new(
            rules,
            DnsPolicy::Allow {
                names,
                resolvers: vec![self.resolver_identity],
            },
        )
        .with_address_policy(
            self.ip_ranges.clone(),
            self.allow_loopback,
            self.allow_private,
            false,
        )
        .map_err(|error| Error::State(error.to_string()))?;
        Ok(NetworkBrokerConfig {
            policy,
            limits: BrokerLimits {
                max_connections: 4,
                max_sockets: 4,
                max_datagrams: 1,
                max_datagram_bytes: 1,
                max_stream_bytes: MAX_FRAME as u64,
                max_messages: 4096,
                max_message_bytes: MAX_FRAME as u64,
                max_bytes: self.limits.max_total_bytes,
                max_elapsed: Duration::from_secs(60),
                max_operations: 8192,
                max_operations_per_window: 8192,
                rate_window: Duration::from_secs(1),
                max_concurrency: 4,
            },
            resolver: self.resolver_identity,
            connect_timeout: Duration::from_millis(self.limits.connect_timeout_ms),
            io_timeout: Duration::from_millis(self.limits.connect_timeout_ms),
            tls_roots: roots,
        })
    }
}

fn valid_protocol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Open {
    protocol_version: u16,
    request_id: String,
    binding: String,
    url: String,
    subprotocols: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Send {
    protocol_version: u16,
    request_id: String,
    binding: String,
    handle_id: u64,
    sequence: u64,
    opcode: u8,
    body_base64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Receive {
    protocol_version: u16,
    request_id: String,
    binding: String,
    handle_id: u64,
    sequence: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Close {
    protocol_version: u16,
    request_id: String,
    binding: String,
    handle_id: u64,
    mode: CloseMode,
    #[serde(default, deserialize_with = "present_value")]
    code: Option<u16>,
    #[serde(default, deserialize_with = "present_value")]
    reason: Option<String>,
}

fn present_value<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CloseMode {
    Graceful,
    Cancel,
}

enum Command {
    Message(Message),
    Close(u16, String),
}
enum Event {
    Open,
    Message(u8, Vec<u8>),
    Close(u16, String),
}

struct Connection {
    binding: String,
    commands: mpsc::SyncSender<Command>,
    events: mpsc::Receiver<Event>,
    cancel: Arc<AtomicBool>,
    worker: Option<JoinHandle<std::result::Result<(), &'static str>>>,
    pending_event: Option<Event>,
    send_sequence: u64,
    receive_sequence: u64,
    bytes: Arc<AtomicU64>,
    queued_send: Arc<AtomicU64>,
    queued_receive: Arc<AtomicU64>,
    deadline: Instant,
    initialized: bool,
}

struct State {
    policies: HashMap<String, ProviderWebSocketConfig>,
    connections: HashMap<u64, Connection>,
    next_handle: u64,
    deadline: Option<Instant>,
}

pub(crate) struct ProviderWebSockets {
    state: Mutex<State>,
    roots: Arc<rustls::RootCertStore>,
}

impl ProviderWebSockets {
    pub(crate) fn new(config: &[ProviderWebSocketConfig]) -> Result<Self> {
        let mut policies = HashMap::new();
        for policy in config {
            policy.validate()?;
            if policies
                .insert(policy.name.clone(), policy.clone())
                .is_some()
            {
                return Err(Error::State("duplicate provider WebSocket binding".into()));
            }
        }
        Ok(Self {
            state: Mutex::new(State {
                policies,
                connections: HashMap::new(),
                next_handle: 1,
                deadline: None,
            }),
            roots: Arc::new(default_tls_roots()),
        })
    }

    pub(crate) fn set_deadline(&self, deadline: Instant) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::State("provider WebSocket state poisoned".into()))?;
        if !state.connections.is_empty() {
            return Err(Error::State(
                "provider WebSocket session still owns live handles".into(),
            ));
        }
        state.deadline = Some(deadline);
        Ok(())
    }

    pub(crate) fn is_quiescent(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| Error::State("provider WebSocket state poisoned".into()))?
            .connections
            .is_empty())
    }

    pub(crate) fn next_handle(&self) -> Result<u64> {
        Ok(self
            .state
            .lock()
            .map_err(|_| Error::State("provider WebSocket state poisoned".into()))?
            .next_handle)
    }

    pub(crate) fn restore_next_handle(&self, next: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::State("provider WebSocket state poisoned".into()))?;
        if next == 0 || next > MAX_HANDLE || !state.connections.is_empty() {
            return Err(Error::State(
                "invalid provider WebSocket handle watermark".into(),
            ));
        }
        state.next_handle = next;
        Ok(())
    }

    pub(crate) fn dispatch(&self, function: &str, json: &str) -> Result<String> {
        if json.len() > super::MAX_ENVELOPE_BYTES {
            return Err(Error::Protocol(
                "provider WebSocket query exceeds envelope bound".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::State("provider WebSocket state poisoned".into()))?;
        let outcome = match function {
            "WorkerdWebSocketV1Open" => super::control::decode::<Open>(json.as_bytes())
                .map(|query| self.open(&mut state, query)),
            "WorkerdWebSocketV1Send" => {
                super::control::decode::<Send>(json.as_bytes()).map(|query| send(&mut state, query))
            }
            "WorkerdWebSocketV1Receive" => super::control::decode::<Receive>(json.as_bytes())
                .map(|query| receive(&mut state, query)),
            "WorkerdWebSocketV1Close" => super::control::decode::<Close>(json.as_bytes())
                .map(|query| close(&mut state, query)),
            _ => return Err(Error::State("unknown provider WebSocket callback".into())),
        };
        let value = match outcome {
            Ok(value) => value,
            Err(error) => {
                let header: serde_json::Value = serde_json::from_str(json)?;
                let request_id = header
                    .get("request_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| Error::Protocol("provider query has no request ID".into()))?;
                super::ControlRequest::new(request_id)?;
                tracing::debug!(%error, "provider WebSocket query rejected");
                reply(request_id, "invalid_request", "invalid_query")
            }
        };
        Ok(serde_json::to_string(&value)?)
    }

    fn open(&self, state: &mut State, query: Open) -> serde_json::Value {
        let Some(policy) = state.policies.get(&query.binding).cloned() else {
            return reply(&query.request_id, "denied", "binding_denied");
        };
        let valid = super::ControlRequest::new(&query.request_id).is_ok()
            && query.protocol_version == 1
            && query.url.len() <= 8192
            && query.subprotocols.len() <= 8
            && query
                .subprotocols
                .iter()
                .all(|value| valid_protocol(value) && policy.subprotocols.contains(value));
        if !valid {
            return reply(&query.request_id, "invalid_request", "invalid_open");
        }
        let mut url = match reqwest::Url::parse(&query.url) {
            Ok(url) if url.scheme() == "wss" => url,
            _ => return reply(&query.request_id, "denied", "url_denied"),
        };
        if url.set_scheme("https").is_err() {
            return reply(&query.request_id, "denied", "url_denied");
        }
        let destination = match policy.policy().and_then(|policy| {
            policy
                .authorize_url(&url)
                .map_err(|_| Error::State("provider URL denied".into()))
        }) {
            Ok(destination) => destination,
            Err(_) => return reply(&query.request_id, "denied", "resource_denied"),
        };
        if destination.0 != policy.credential.host || destination.1 != policy.credential.port {
            return reply(&query.request_id, "denied", "credential_scope_denied");
        }
        let Some(deadline) = state.deadline.filter(|deadline| *deadline > Instant::now()) else {
            return reply(&query.request_id, "host_error", "deadline");
        };
        if state.connections.len() >= policy.limits.max_connections
            || state.next_handle > MAX_HANDLE
        {
            return reply(&query.request_id, "denied", "connection_limit");
        }
        let (commands, input) = mpsc::sync_channel(policy.limits.queue_frames);
        let (output, events) = mpsc::sync_channel(policy.limits.queue_frames);
        let cancel = Arc::new(AtomicBool::new(false));
        let worker_cancel = cancel.clone();
        let bytes = Arc::new(AtomicU64::new(0));
        let worker_bytes = bytes.clone();
        let queued_send = Arc::new(AtomicU64::new(0));
        let worker_send = queued_send.clone();
        let queued_receive = Arc::new(AtomicU64::new(0));
        let worker_receive = queued_receive.clone();
        let roots = self.roots.clone();
        let protocols = query.subprotocols;
        let worker = match std::thread::Builder::new()
            .name("provider-websocket".into())
            .spawn(move || {
                run(Worker {
                    policy,
                    url,
                    protocols,
                    deadline,
                    roots,
                    cancel: worker_cancel,
                    commands: input,
                    events: output,
                    budget: worker_bytes,
                    queued_send: worker_send,
                    queued_receive: worker_receive,
                })
            }) {
            Ok(worker) => worker,
            Err(_) => return reply(&query.request_id, "host_error", "worker_unavailable"),
        };
        let handle = state.next_handle;
        state.next_handle += 1;
        state.connections.insert(
            handle,
            Connection {
                binding: query.binding,
                commands,
                events,
                cancel,
                worker: Some(worker),
                pending_event: None,
                send_sequence: 0,
                receive_sequence: 0,
                bytes,
                queued_send,
                queued_receive,
                deadline,
                initialized: false,
            },
        );
        let mut value = reply(&query.request_id, "pending", "connecting");
        value["handle_id"] = handle.into();
        value
    }
}

fn reply(id: &str, status: &str, code: &str) -> serde_json::Value {
    serde_json::json!({"protocol_version":1,"request_id":id,"status":status,"code":code})
}

fn acknowledge(id: &str, accepted: bool, code: &str) -> serde_json::Value {
    let mut value = reply(
        id,
        if accepted && code == "accepted" {
            "ok"
        } else {
            "pending"
        },
        code,
    );
    value["accepted"] = accepted.into();
    value
}

fn connection<'a>(state: &'a mut State, binding: &str, handle: u64) -> Option<&'a mut Connection> {
    state
        .connections
        .get_mut(&handle)
        .filter(|connection| connection.binding == binding)
}

fn send(state: &mut State, query: Send) -> serde_json::Value {
    let policy = state.policies.get(&query.binding).cloned();
    let Some(policy) = policy else {
        return reply(&query.request_id, "denied", "binding_denied");
    };
    let limit = &policy.limits;
    let Some(connection) = connection(state, &query.binding, query.handle_id) else {
        return reply(&query.request_id, "denied", "handle_denied");
    };
    if query.protocol_version != 1
        || query.sequence != connection.send_sequence
        || super::ControlRequest::new(&query.request_id).is_err()
        || connection.cancel.load(Ordering::Acquire)
        || Instant::now() >= connection.deadline
    {
        return reply(&query.request_id, "invalid_request", "invalid_send");
    }
    let bytes = match STANDARD.decode(&query.body_base64) {
        Ok(bytes)
            if bytes.len() <= limit.max_frame_bytes
                && connection.bytes.load(Ordering::Acquire) + bytes.len() as u64
                    <= limit.max_total_bytes =>
        {
            bytes
        }
        _ => return reply(&query.request_id, "invalid_request", "frame_limit"),
    };
    let control = match &policy.session_policy {
        Some(policy) => match policy.check(query.opcode, &bytes, connection.initialized) {
            Ok(control) => control,
            Err(_) => return reply(&query.request_id, "denied", "session_policy_denied"),
        },
        None => false,
    };
    let message = match query.opcode {
        1 => match String::from_utf8(bytes) {
            Ok(text) => Message::Text(text.into()),
            Err(_) => return reply(&query.request_id, "invalid_request", "invalid_utf8"),
        },
        2 => Message::Binary(bytes.into()),
        _ => return reply(&query.request_id, "invalid_request", "invalid_opcode"),
    };
    let bytes = message.len() as u64;
    if !reserve_bytes(&connection.queued_send, 1, limit.queue_frames as u64) {
        return acknowledge(&query.request_id, false, "backpressure");
    }
    if !reserve_bytes(&connection.bytes, bytes, limit.max_total_bytes) {
        connection.queued_send.fetch_sub(1, Ordering::AcqRel);
        return reply(&query.request_id, "denied", "byte_limit");
    }
    match connection.commands.try_send(Command::Message(message)) {
        Ok(()) => {
            connection.send_sequence += 1;
            connection.initialized |= control;
            acknowledge(&query.request_id, true, "accepted")
        }
        Err(mpsc::TrySendError::Full(_)) => {
            connection.queued_send.fetch_sub(1, Ordering::AcqRel);
            connection.bytes.fetch_sub(bytes, Ordering::AcqRel);
            acknowledge(&query.request_id, false, "backpressure")
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            connection.queued_send.fetch_sub(1, Ordering::AcqRel);
            connection.bytes.fetch_sub(bytes, Ordering::AcqRel);
            reply(&query.request_id, "host_error", "transport_failed")
        }
    }
}

fn receive(state: &mut State, query: Receive) -> serde_json::Value {
    let Some(connection) = connection(state, &query.binding, query.handle_id) else {
        return reply(&query.request_id, "denied", "handle_denied");
    };
    if query.protocol_version != 1
        || query.sequence != connection.receive_sequence
        || super::ControlRequest::new(&query.request_id).is_err()
    {
        return reply(&query.request_id, "invalid_request", "invalid_receive");
    }
    let event = match connection.pending_event.take() {
        Some(event) => event,
        None => match connection.events.try_recv() {
            Ok(event) => event,
            Err(mpsc::TryRecvError::Empty) => {
                return reply(&query.request_id, "pending", "pending");
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                return reply(&query.request_id, "host_error", "transport_failed");
            }
        },
    };
    if matches!(event, Event::Close(_, _))
        && connection
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
    {
        connection.pending_event = Some(event);
        return reply(&query.request_id, "pending", "pending");
    }
    let mut value = reply(&query.request_id, "ok", "event");
    value["handle_id"] = query.handle_id.into();
    value["sequence"] = query.sequence.into();
    connection.receive_sequence += 1;
    match event {
        Event::Open => value["kind"] = "open".into(),
        Event::Message(opcode, bytes) => {
            connection.queued_receive.fetch_sub(1, Ordering::AcqRel);
            value["kind"] = "message".into();
            value["opcode"] = opcode.into();
            value["body_base64"] = STANDARD.encode(bytes).into();
        }
        Event::Close(code, reason) => {
            let Some(mut connection) = state.connections.remove(&query.handle_id) else {
                return reply(&query.request_id, "host_error", "state_failed");
            };
            if !matches!(
                connection.worker.take().map(JoinHandle::join),
                Some(Ok(Ok(())))
            ) {
                return reply(&query.request_id, "host_error", "transport_failed");
            }
            value["kind"] = "close".into();
            value["close_code"] = code.into();
            value["reason"] = reason.into();
        }
    }
    value
}

fn close(state: &mut State, query: Close) -> serde_json::Value {
    let Some(connection) = connection(state, &query.binding, query.handle_id) else {
        return reply(&query.request_id, "denied", "handle_denied");
    };
    if query.protocol_version != 1 || super::ControlRequest::new(&query.request_id).is_err() {
        return reply(&query.request_id, "invalid_request", "invalid_close");
    }
    match query.mode {
        CloseMode::Cancel => {
            if query.code.is_some() || query.reason.is_some() {
                return reply(&query.request_id, "invalid_request", "invalid_cancel");
            }
            connection.cancel.store(true, Ordering::Release);
            if connection
                .worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
            {
                return acknowledge(&query.request_id, true, "cancelling");
            }
            let Some(mut connection) = state.connections.remove(&query.handle_id) else {
                return reply(&query.request_id, "host_error", "state_failed");
            };
            if connection
                .worker
                .take()
                .map(JoinHandle::join)
                .is_some_and(|result| result.is_err())
            {
                return reply(&query.request_id, "host_error", "worker_failed");
            }
            reply(&query.request_id, "ok", "cancelled")
        }
        CloseMode::Graceful => {
            let (Some(code), Some(reason)) = (query.code, query.reason) else {
                return reply(&query.request_id, "invalid_request", "invalid_close");
            };
            if !valid_close(code) || reason.len() > 123 {
                return reply(&query.request_id, "invalid_request", "invalid_close");
            }
            match connection.commands.try_send(Command::Close(code, reason)) {
                Ok(()) => acknowledge(&query.request_id, true, "closing"),
                Err(mpsc::TrySendError::Full(_)) => {
                    acknowledge(&query.request_id, false, "backpressure")
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    reply(&query.request_id, "host_error", "transport_failed")
                }
            }
        }
    }
}

fn valid_close(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

fn reserve_bytes(budget: &AtomicU64, bytes: u64, maximum: u64) -> bool {
    budget
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|total| *total <= maximum)
        })
        .is_ok()
}

struct Worker {
    policy: ProviderWebSocketConfig,
    url: reqwest::Url,
    protocols: Vec<String>,
    deadline: Instant,
    roots: Arc<rustls::RootCertStore>,
    cancel: Arc<AtomicBool>,
    commands: mpsc::Receiver<Command>,
    events: mpsc::SyncSender<Event>,
    budget: Arc<AtomicU64>,
    queued_send: Arc<AtomicU64>,
    queued_receive: Arc<AtomicU64>,
}

fn run(worker: Worker) -> std::result::Result<(), &'static str> {
    let Worker {
        policy,
        url,
        protocols,
        deadline,
        roots,
        cancel,
        commands,
        events,
        budget,
        queued_send,
        queued_receive,
    } = worker;
    let host = match policy.credential.host.parse::<IpAddr>() {
        Ok(ip) => EndpointHost::Ip(ip),
        Err(_) => EndpointHost::Dns(
            policy
                .credential
                .host
                .parse()
                .map_err(|_| "endpoint_failed")?,
        ),
    };
    let endpoint =
        BrokerEndpoint::new(host, policy.credential.port).map_err(|_| "endpoint_failed")?;
    let roots = match &policy.trusted_ca_file {
        Some(path) => {
            use rustls::pki_types::pem::PemObject;
            use std::io::Read;
            let mut bytes = Vec::new();
            std::fs::File::open(path)
                .map_err(|_| "tls_roots_failed")?
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| "tls_roots_failed")?;
            if bytes.len() > 1024 * 1024 {
                return Err("tls_roots_failed");
            }
            let mut trusted = (*roots).clone();
            let mut count = 0;
            for certificate in rustls::pki_types::CertificateDer::pem_slice_iter(&bytes) {
                trusted
                    .add(certificate.map_err(|_| "tls_roots_failed")?)
                    .map_err(|_| "tls_roots_failed")?;
                count += 1;
            }
            if count == 0 {
                return Err("tls_roots_failed");
            }
            Arc::new(trusted)
        }
        None => roots,
    };
    let network = policy.network(roots).map_err(|_| "policy_failed")?;
    let connect_deadline =
        deadline.min(Instant::now() + Duration::from_millis(policy.limits.connect_timeout_ms));
    let transport = provider_tls_transport(&network, &endpoint, connect_deadline)
        .map_err(|_| "connect_failed")?;
    if cancel.load(Ordering::Acquire) {
        return Err("cancelled");
    }
    let mut public_url = url.clone();
    public_url.set_scheme("wss").map_err(|_| "url_failed")?;
    let mut request = public_url
        .as_str()
        .into_client_request()
        .map_err(|_| "request_failed")?;
    if !protocols.is_empty() {
        request.headers_mut().insert(
            "sec-websocket-protocol",
            protocols
                .join(", ")
                .parse()
                .map_err(|_| "protocol_failed")?,
        );
    }
    policy
        .credential
        .inject(&url, request.headers_mut())
        .map_err(|_| "credential_failed")?;
    let mut config = tungstenite::protocol::WebSocketConfig::default();
    config.write_buffer_size = 0;
    config.max_write_buffer_size =
        policy.limits.queue_frames * policy.limits.max_frame_bytes + 1024;
    config.max_message_size = Some(policy.limits.max_frame_bytes);
    config.max_frame_size = Some(policy.limits.max_frame_bytes);
    let (mut socket, response) =
        tungstenite::client::client_with_config(request, transport, Some(config))
            .map_err(|_| "handshake_failed")?;
    if response
        .headers()
        .get("sec-websocket-protocol")
        .is_some_and(|value| {
            !value
                .to_str()
                .is_ok_and(|value| protocols.iter().any(|allowed| allowed == value))
        })
        || response.headers().contains_key("sec-websocket-extensions")
    {
        return Err("protocol_failed");
    }
    socket.get_mut().polling(Duration::from_millis(10));
    socket
        .get_mut()
        .set_deadline(deadline)
        .map_err(|_| "deadline_failed")?;
    let mut pending = Some(Event::Open);
    let mut closing = false;
    let mut pending_write = false;
    let mut pending_message = false;
    loop {
        if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
            socket
                .get_ref()
                .tcp()
                .shutdown(Shutdown::Both)
                .map_err(|_| "shutdown_failed")?;
            return Err("cancelled");
        }
        if let Some(event) = pending.take() {
            match events.try_send(event) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(event)) => {
                    pending = Some(event);
                }
                Err(mpsc::TrySendError::Disconnected(_)) => return Err("revoked"),
            }
        }
        if pending_write {
            match socket.flush() {
                Ok(()) => {
                    pending_write = false;
                    if pending_message {
                        queued_send.fetch_sub(1, Ordering::AcqRel);
                        pending_message = false;
                    }
                }
                Err(error) if retryable_io(&error) => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(_) => return Err("send_failed"),
            }
        }
        match commands.try_recv() {
            Ok(Command::Message(message)) if !closing => match socket.send(message) {
                Ok(()) => {
                    queued_send.fetch_sub(1, Ordering::AcqRel);
                }
                Err(error) if retryable_io(&error) => {
                    pending_write = true;
                    pending_message = true;
                }
                Err(_) => return Err("send_failed"),
            },
            Ok(Command::Message(_)) => return Err("send_after_close"),
            Ok(Command::Close(code, reason)) => {
                closing = true;
                match socket.close(Some(tungstenite::protocol::CloseFrame {
                    code: code.into(),
                    reason: reason.into(),
                })) {
                    Ok(()) => {}
                    Err(error) if retryable_io(&error) => pending_write = true,
                    Err(_) => return Err("close_failed"),
                }
            }
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => return Err("revoked"),
        }
        if pending.is_some() {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }
        if queued_receive.load(Ordering::Acquire) >= policy.limits.queue_frames as u64 {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }
        match socket.read() {
            Ok(Message::Binary(bytes)) => {
                if !reserve_bytes(&budget, bytes.len() as u64, policy.limits.max_total_bytes) {
                    return Err("byte_limit");
                }
                queued_receive.fetch_add(1, Ordering::AcqRel);
                pending = Some(Event::Message(2, bytes.to_vec()));
            }
            Ok(Message::Text(text)) => {
                if !reserve_bytes(&budget, text.len() as u64, policy.limits.max_total_bytes) {
                    return Err("byte_limit");
                }
                queued_receive.fetch_add(1, Ordering::AcqRel);
                pending = Some(Event::Message(1, text.as_bytes().to_vec()));
            }
            Ok(Message::Close(close)) => {
                let (code, reason) = close.map_or((1000, String::new()), |close| {
                    (u16::from(close.code), close.reason.to_string())
                });
                loop {
                    if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
                        return Err("cancelled");
                    }
                    match socket.flush() {
                        Ok(()) | Err(tungstenite::Error::ConnectionClosed) => break,
                        Err(error) if retryable_io(&error) => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => return Err("close_failed"),
                    }
                }
                loop {
                    match events.try_send(Event::Close(code, reason.clone())) {
                        Ok(()) => return Ok(()),
                        Err(mpsc::TrySendError::Disconnected(_)) => return Err("revoked"),
                        Err(mpsc::TrySendError::Full(_)) => {
                            if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
                                return Err("cancelled");
                            }
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            }
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return Err("receive_failed"),
        }
    }
}

fn retryable_io(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(error) if matches!(
        error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ))
}

impl Drop for ProviderWebSockets {
    fn drop(&mut self) {
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        for (_, mut connection) in state.connections.drain() {
            connection.cancel.store(true, Ordering::Release);
            if connection
                .worker
                .take()
                .is_some_and(|worker| worker.join().is_err())
            {
                tracing::error!("provider WebSocket worker failed during teardown");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::net::{Ipv4Addr, TcpListener};

    fn config(port: u16, file: std::path::PathBuf) -> ProviderWebSocketConfig {
        ProviderWebSocketConfig {
            name: "transcribe".into(),
            hosts: vec!["127.0.0.1".into()],
            ports: vec![port],
            paths: vec!["/realtime".into()],
            query_parameters: BTreeMap::from([("intent".into(), vec!["transcription".into()])]),
            subprotocols: vec![],
            ip_ranges: vec!["127.0.0.0/8".parse().unwrap()],
            resolver_identity: Ipv4Addr::LOCALHOST.into(),
            allow_loopback: true,
            allow_private: false,
            credential: FetchCredential {
                reference: "test-provider".into(),
                value_file: file,
                header: "authorization".into(),
                scheme: "https".into(),
                host: "127.0.0.1".into(),
                port,
            },
            limits: ProviderWebSocketLimits {
                max_connections: 4,
                queue_frames: 4,
                max_frame_bytes: MAX_FRAME,
                max_total_bytes: 8 * 1024 * 1024,
                connect_timeout_ms: 2000,
            },
            session_policy: None,
            trusted_ca_file: None,
        }
    }

    fn call(
        broker: &ProviderWebSockets,
        function: &str,
        value: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::from_str(&broker.dispatch(function, &value.to_string()).unwrap()).unwrap()
    }

    fn read(broker: &ProviderWebSockets, handle: u64, sequence: u64) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let value = call(
                broker,
                "WorkerdWebSocketV1Receive",
                serde_json::json!({
                    "protocol_version":1,"request_id":"read-1","binding":"transcribe","handle_id":handle,"sequence":sequence
                }),
            );
            if value["status"] != "pending" {
                return value;
            }
            assert!(Instant::now() < deadline, "provider event timed out");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    #[expect(
        clippy::result_large_err,
        reason = "Tungstenite handshake callback requires its unboxed HTTP error response"
    )]
    fn tls_resource_auth_duplex_and_joined_close_are_real_and_bounded() {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let certificate: CertificateDer<'static> = cert.der().clone();
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der())),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let fixture = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let tls = rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(server)).unwrap(),
                stream,
            );
            let mut socket = tungstenite::accept_hdr(
                tls,
                |request: &tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri(), "/realtime?intent=transcription");
                    assert_eq!(request.headers()["authorization"], "Bearer synthetic-token");
                    Ok(response)
                },
            )
            .unwrap();
            let message = socket.read().unwrap();
            assert_eq!(message, Message::Binary(vec![7; MAX_FRAME].into()));
            socket.send(message).unwrap();
            socket
                .close(Some(tungstenite::protocol::CloseFrame {
                    code: 1000.into(),
                    reason: "complete".into(),
                }))
                .unwrap();
        });
        let temporary = tempfile::tempdir().unwrap();
        let secret = temporary.path().join("credential");
        std::fs::write(&secret, b"Bearer synthetic-token").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut broker = ProviderWebSockets::new(&[config(port, secret)]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        broker.roots = Arc::new(roots);
        broker
            .set_deadline(Instant::now() + Duration::from_secs(8))
            .unwrap();
        let opened = call(
            &broker,
            "WorkerdWebSocketV1Open",
            serde_json::json!({
                "protocol_version":1,"request_id":"open-1","binding":"transcribe",
                "url":format!("wss://127.0.0.1:{port}/realtime?intent=transcription"),"subprotocols":[]
            }),
        );
        assert_eq!(opened["status"], "pending");
        let handle = opened["handle_id"].as_u64().unwrap();
        assert!(!broker.is_quiescent().unwrap());
        assert_eq!(read(&broker, handle, 0)["kind"], "open");
        let sent = call(
            &broker,
            "WorkerdWebSocketV1Send",
            serde_json::json!({
                "protocol_version":1,"request_id":"send-1","binding":"transcribe","handle_id":handle,
                "sequence":0,"opcode":2,"body_base64":STANDARD.encode(vec![7; MAX_FRAME])
            }),
        );
        assert_eq!(sent["accepted"], true);
        let received = read(&broker, handle, 1);
        assert_eq!(received["kind"], "message", "{received}");
        assert_eq!(
            STANDARD
                .decode(received["body_base64"].as_str().unwrap())
                .unwrap(),
            vec![7; MAX_FRAME]
        );
        assert_eq!(read(&broker, handle, 2)["kind"], "close");
        assert!(broker.is_quiescent().unwrap());
        fixture.join().unwrap();
    }

    #[test]
    fn resource_query_binding_and_plaintext_denials_do_not_allocate_handles() {
        let temporary = tempfile::tempdir().unwrap();
        let broker =
            ProviderWebSockets::new(&[config(443, temporary.path().join("absent-key"))]).unwrap();
        broker
            .set_deadline(Instant::now() + Duration::from_secs(5))
            .unwrap();
        for url in [
            "wss://127.0.0.1/other?intent=transcription",
            "wss://127.0.0.1/realtime?intent=wrong",
            "wss://127.0.0.1/realtime?api-key=forbidden",
            "wss://other.example/realtime?intent=transcription",
            "ws://127.0.0.1/realtime?intent=transcription",
            "wss://user:secret@127.0.0.1/realtime?intent=transcription",
        ] {
            let denied = call(
                &broker,
                "WorkerdWebSocketV1Open",
                serde_json::json!({
                    "protocol_version":1,"request_id":"deny-1","binding":"transcribe","url":url,"subprotocols":[]
                }),
            );
            assert_eq!(denied["status"], "denied", "{denied}");
            assert!(broker.is_quiescent().unwrap());
        }
    }

    #[test]
    fn forbidden_guest_auth_fields_and_unoffered_protocols_fail_before_allocation() {
        let temporary = tempfile::tempdir().unwrap();
        let broker =
            ProviderWebSockets::new(&[config(443, temporary.path().join("absent"))]).unwrap();
        broker
            .set_deadline(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let mut value = serde_json::json!({
            "protocol_version":1,"request_id":"spoof-1","binding":"transcribe",
            "url":"wss://127.0.0.1/realtime?intent=transcription","subprotocols":[]
        });
        value["headers"] = serde_json::json!({"Authorization":"caller-must-not-supply"});
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Open", value.clone())["status"],
            "invalid_request"
        );
        value.as_object_mut().unwrap().remove("headers");
        value["subprotocols"] = serde_json::json!(["unadmitted"]);
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Open", value)["status"],
            "invalid_request"
        );
        assert!(broker.is_quiescent().unwrap());
    }

    #[test]
    fn queue_backpressure_preserves_sequence_and_combined_byte_budget() {
        let temporary = tempfile::tempdir().unwrap();
        let broker =
            ProviderWebSockets::new(&[config(443, temporary.path().join("absent"))]).unwrap();
        let (commands, queue) = mpsc::sync_channel(4);
        let (_events, receiver) = mpsc::sync_channel(4);
        broker.state.lock().unwrap().connections.insert(
            1,
            Connection {
                binding: "transcribe".into(),
                commands,
                events: receiver,
                cancel: Arc::new(AtomicBool::new(false)),
                worker: None,
                pending_event: None,
                send_sequence: 0,
                receive_sequence: 0,
                bytes: Arc::new(AtomicU64::new(0)),
                queued_send: Arc::new(AtomicU64::new(0)),
                queued_receive: Arc::new(AtomicU64::new(0)),
                deadline: Instant::now() + Duration::from_secs(5),
                initialized: false,
            },
        );
        let packet = |sequence| {
            serde_json::json!({
                "protocol_version":1,"request_id":"send-1","binding":"transcribe","handle_id":1,
                "sequence":sequence,"opcode":2,"body_base64":STANDARD.encode(vec![1; MAX_FRAME])
            })
        };
        for sequence in 0..4 {
            assert_eq!(
                call(&broker, "WorkerdWebSocketV1Send", packet(sequence))["accepted"],
                true
            );
        }
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Send", packet(4))["accepted"],
            false
        );
        assert_eq!(
            broker.state.lock().unwrap().connections[&1].send_sequence,
            4
        );
        assert_eq!(
            broker.state.lock().unwrap().connections[&1]
                .bytes
                .load(Ordering::Acquire),
            (4 * MAX_FRAME) as u64
        );
        queue.recv().unwrap();
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Send", packet(4))["accepted"],
            false
        );
        broker.state.lock().unwrap().connections[&1]
            .queued_send
            .fetch_sub(1, Ordering::AcqRel);
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Send", packet(4))["accepted"],
            true
        );
        let budget = AtomicU64::new(8 * 1024 * 1024 - 1);
        assert!(reserve_bytes(&budget, 1, 8 * 1024 * 1024));
        assert!(!reserve_bytes(&budget, 1, 8 * 1024 * 1024));
        assert_eq!(budget.load(Ordering::Acquire), 8 * 1024 * 1024);
    }

    #[test]
    fn initial_control_model_and_format_are_validated_without_transport_branding() {
        let policy = ProviderWebSocketSessionPolicy {
            discriminator_pointer: "/type".into(),
            control_type: "session.update".into(),
            required_values: BTreeMap::from([
                (
                    "/session/audio/input/transcription/model".into(),
                    serde_json::json!("approved-model"),
                ),
                (
                    "/session/audio/input/format/type".into(),
                    serde_json::json!("audio/pcm"),
                ),
                (
                    "/session/audio/input/format/rate".into(),
                    serde_json::json!(16000),
                ),
            ]),
        };
        policy.validate().unwrap();
        let mut value = serde_json::json!({"type":"session.update","session":{"audio":{"input":{
            "transcription":{"model":"approved-model"},"format":{"type":"audio/pcm","rate":16000}
        }}}});
        assert!(
            policy
                .check(1, &serde_json::to_vec(&value).unwrap(), false)
                .unwrap()
        );
        assert!(policy.check(2, &[0; 16], false).is_err());
        assert!(!policy.check(2, &[0; 16], true).unwrap());
        value["session"]["audio"]["input"]["transcription"]["model"] = "unapproved-model".into();
        assert!(
            policy
                .check(1, &serde_json::to_vec(&value).unwrap(), true)
                .is_err()
        );
    }

    fn held_connection(broker: &ProviderWebSockets, handle: u64) -> mpsc::Sender<()> {
        let (commands, _input) = mpsc::sync_channel(4);
        let (_output, events) = mpsc::sync_channel(4);
        let (release, wait) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            wait.recv().unwrap();
            Ok(())
        });
        broker.state.lock().unwrap().connections.insert(
            handle,
            Connection {
                binding: "transcribe".into(),
                commands,
                events,
                cancel: Arc::new(AtomicBool::new(false)),
                worker: Some(worker),
                pending_event: None,
                send_sequence: 0,
                receive_sequence: 0,
                bytes: Arc::new(AtomicU64::new(0)),
                queued_send: Arc::new(AtomicU64::new(0)),
                queued_receive: Arc::new(AtomicU64::new(0)),
                deadline: Instant::now() + Duration::from_secs(5),
                initialized: false,
            },
        );
        release
    }

    fn cancel_query(handle: u64) -> serde_json::Value {
        serde_json::json!({"protocol_version":1,"request_id":"cancel-1",
            "binding":"transcribe","handle_id":handle,"mode":"cancel"})
    }

    fn joined_cancel(broker: &ProviderWebSockets, handle: u64) {
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let value = call(broker, "WorkerdWebSocketV1Close", cancel_query(handle));
            if value["status"] == "ok" {
                assert_eq!(value, reply("cancel-1", "ok", "cancelled"));
                return;
            }
            assert_eq!(value["code"], "cancelling", "{value}");
            assert!(Instant::now() < deadline, "cancel did not join");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn pending_cancel_keeps_ownership_until_actual_worker_join() {
        let tmp = tempfile::tempdir().unwrap();
        let broker = ProviderWebSockets::new(&[config(443, tmp.path().join("absent"))]).unwrap();
        let release = held_connection(&broker, 1);
        let pending = call(&broker, "WorkerdWebSocketV1Close", cancel_query(1));
        assert_eq!(pending, acknowledge("cancel-1", true, "cancelling"));
        assert!(!broker.is_quiescent().unwrap());
        assert!(
            broker
                .set_deadline(Instant::now() + Duration::from_secs(1))
                .is_err()
        );
        release.send(()).unwrap();
        joined_cancel(&broker, 1);
        assert!(broker.is_quiescent().unwrap());
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Close", cancel_query(1))["code"],
            "handle_denied"
        );
    }

    #[test]
    fn cancel_rejects_present_null_fields_without_releasing_live_handle() {
        let tmp = tempfile::tempdir().unwrap();
        let broker = ProviderWebSockets::new(&[config(443, tmp.path().join("absent"))]).unwrap();
        let release = held_connection(&broker, 1);
        for field in ["code", "reason"] {
            let mut query = cancel_query(1);
            query[field] = serde_json::Value::Null;
            assert_eq!(
                call(&broker, "WorkerdWebSocketV1Close", query)["status"],
                "invalid_request"
            );
        }
        assert!(!broker.is_quiescent().unwrap());
        release.send(()).unwrap();
        joined_cancel(&broker, 1);
    }

    #[test]
    fn handles_are_binding_and_broker_scoped_and_four_sockets_is_the_total_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let broker = ProviderWebSockets::new(&[config(443, tmp.path().join("absent"))]).unwrap();
        let other = ProviderWebSockets::new(&[config(443, tmp.path().join("other"))]).unwrap();
        let releases: Vec<_> = (1..=4)
            .map(|handle| held_connection(&broker, handle))
            .collect();
        broker.state.lock().unwrap().deadline = Some(Instant::now() + Duration::from_secs(5));
        let open = serde_json::json!({"protocol_version":1,"request_id":"limit-1","binding":"transcribe",
            "url":"wss://127.0.0.1/realtime?intent=transcription","subprotocols":[]});
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Open", open)["code"],
            "connection_limit"
        );
        assert_eq!(
            call(&other, "WorkerdWebSocketV1Close", cancel_query(1))["code"],
            "handle_denied"
        );
        let mut forged = cancel_query(1);
        forged["binding"] = "other-app".into();
        assert_eq!(
            call(&broker, "WorkerdWebSocketV1Close", forged)["code"],
            "handle_denied"
        );
        for (index, release) in releases.into_iter().enumerate() {
            release.send(()).unwrap();
            joined_cancel(&broker, index as u64 + 1);
        }
        assert!(broker.is_quiescent().unwrap());
    }

    #[test]
    fn resolved_cidr_private_loopback_and_metadata_addresses_remain_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let mut policy = config(443, tmp.path().join("absent"));
        policy.allow_loopback = false;
        policy.ip_ranges = vec!["0.0.0.0/0".parse().unwrap(), "::/0".parse().unwrap()];
        let network = policy.network(Arc::new(default_tls_roots())).unwrap();
        for address in [
            "127.0.0.1",
            "10.1.2.3",
            "169.254.169.254",
            "::ffff:127.0.0.1",
            "fd00:ec2::254",
        ] {
            assert!(
                !network.policy.address_allowed(address.parse().unwrap()),
                "{address}"
            );
        }
        policy.allow_loopback = true;
        policy.ip_ranges = vec!["127.0.0.1/32".parse().unwrap()];
        let network = policy.network(Arc::new(default_tls_roots())).unwrap();
        assert!(network.policy.address_allowed("127.0.0.1".parse().unwrap()));
        assert!(!network.policy.address_allowed("127.0.0.2".parse().unwrap()));
        assert!(!network.policy.address_allowed("8.8.8.8".parse().unwrap()));
    }

    #[test]
    #[expect(
        clippy::result_large_err,
        reason = "Tungstenite requires its fixed handshake error type"
    )]
    fn tls_credential_rotation_and_abrupt_eof_require_explicit_joined_cancel() {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der())),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let fixture = std::thread::spawn(move || {
            for generation in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let transport = rustls::StreamOwned::new(
                    rustls::ServerConnection::new(Arc::new(tls.clone())).unwrap(),
                    stream,
                );
                let socket = tungstenite::accept_hdr(
                    transport,
                    |request: &tungstenite::handshake::server::Request, response| {
                        assert_eq!(
                            request.headers()["authorization"],
                            format!("Bearer synthetic-rotation-{generation}")
                        );
                        Ok(response)
                    },
                )
                .unwrap();
                drop(socket);
            }
        });
        let tmp = tempfile::tempdir().unwrap();
        let credential = tmp.path().join("credential");
        std::fs::write(&credential, b"Bearer synthetic-rotation-0").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut broker = ProviderWebSockets::new(&[config(port, credential.clone())]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        broker.roots = Arc::new(roots);
        broker
            .set_deadline(Instant::now() + Duration::from_secs(10))
            .unwrap();
        for generation in 0..2 {
            std::fs::write(
                &credential,
                format!("Bearer synthetic-rotation-{generation}"),
            )
            .unwrap();
            let open = call(
                &broker,
                "WorkerdWebSocketV1Open",
                serde_json::json!({
                    "protocol_version":1,"request_id":"rotation-1","binding":"transcribe",
                    "url":format!("wss://127.0.0.1:{port}/realtime?intent=transcription"),"subprotocols":[]
                }),
            );
            let handle = open["handle_id"].as_u64().unwrap();
            assert_eq!(read(&broker, handle, 0)["kind"], "open");
            assert_eq!(read(&broker, handle, 1)["code"], "transport_failed");
            assert!(!broker.is_quiescent().unwrap());
            joined_cancel(&broker, handle);
            assert!(broker.is_quiescent().unwrap());
        }
        fixture.join().unwrap();
    }
}
