// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{Error, Result};
use crate::net_policy::{AddressClassOptIns, NetworkPolicy};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Body, Client, Method, Response, Url};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

pub const FETCH_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct FetchLimits {
    pub connect_timeout: Duration,
    pub total_timeout: Duration,
    pub max_headers: usize,
    pub max_header_bytes: usize,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_concurrent_requests: usize,
    pub max_write_chunk: usize,
    pub max_read_chunk: usize,
}

impl Default for FetchLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            total_timeout: Duration::from_secs(10),
            max_headers: 128,
            max_header_bytes: 64 * 1024,
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 4 * 1024 * 1024,
            max_concurrent_requests: 16,
            max_write_chunk: FETCH_ABI_SAFE_CHUNK,
            max_read_chunk: FETCH_ABI_SAFE_CHUNK - 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FetchPolicy {
    network: NetworkPolicy,
    schemes: HashSet<String>,
    ports: HashSet<u16>,
    address_opt_ins: AddressClassOptIns,
}

impl FetchPolicy {
    pub fn deny_all() -> Self {
        Self {
            network: NetworkPolicy::AllowList(
                crate::AllowList::from_hosts(&[] as &[&str]).expect("empty allow list is valid"),
            ),
            schemes: HashSet::new(),
            ports: HashSet::new(),
            address_opt_ins: AddressClassOptIns::default(),
        }
    }

    pub fn new(
        network: NetworkPolicy,
        schemes: impl IntoIterator<Item = impl Into<String>>,
        ports: impl IntoIterator<Item = u16>,
    ) -> Self {
        Self {
            network,
            schemes: schemes.into_iter().map(Into::into).collect(),
            ports: ports.into_iter().collect(),
            address_opt_ins: AddressClassOptIns::default(),
        }
    }

    pub fn allow_loopback(mut self, allow: bool) -> Self {
        self.address_opt_ins.loopback = allow;
        self
    }

    pub fn allow_private(mut self, allow: bool) -> Self {
        self.address_opt_ins.private = allow;
        self
    }

    pub fn allow_metadata(mut self, allow: bool) -> Self {
        self.address_opt_ins.metadata = allow;
        self
    }

    fn authorize_url(&self, url: &Url) -> std::result::Result<(String, u16), FetchFailure> {
        if !self.schemes.contains(url.scheme()) {
            return Err(FetchFailure::policy("URL scheme is not allowed"));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(FetchFailure::invalid("URL credentials are not allowed"));
        }
        if url.fragment().is_some() {
            return Err(FetchFailure::invalid("URL fragments are not allowed"));
        }
        let host = url
            .host_str()
            .ok_or_else(|| FetchFailure::invalid("URL has no host"))?
            .to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| FetchFailure::invalid("URL has no effective port"))?;
        if !self.ports.contains(&port) {
            return Err(FetchFailure::policy("destination port is not allowed"));
        }
        if host.parse::<IpAddr>().is_err() && !self.network.allows_hostname(&host) {
            return Err(FetchFailure::policy("destination host is not allowed"));
        }
        Ok((host, port))
    }

    fn authorize_addresses(
        &self,
        addresses: &[SocketAddr],
    ) -> std::result::Result<(), FetchFailure> {
        if addresses.is_empty() {
            return Err(FetchFailure::dns("host resolved to no addresses"));
        }
        if addresses.iter().any(|address| {
            !self
                .network
                .allows_with(address, false, self.address_opt_ins)
        }) {
            return Err(FetchFailure::policy(
                "one or more resolved addresses are not allowed",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FetchBrokerConfig {
    pub policy: FetchPolicy,
    pub limits: FetchLimits,
}

impl Default for FetchBrokerConfig {
    fn default() -> Self {
        Self {
            policy: FetchPolicy::deny_all(),
            limits: FetchLimits::default(),
        }
    }
}

#[derive(Clone)]
pub struct FetchBroker {
    inner: Arc<FetchBrokerInner>,
}

struct FetchBrokerInner {
    config: FetchBrokerConfig,
    active: AtomicUsize,
    dns_active: AtomicUsize,
    resolver: Arc<dyn Resolver>,
}

trait Resolver: Send + Sync {
    fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>>;
}

struct SystemResolver;

impl Resolver for SystemResolver {
    fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
        (host, port).to_socket_addrs().map(|items| items.collect())
    }
}

impl std::fmt::Debug for FetchBroker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FetchBroker")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl FetchBroker {
    pub fn new(config: FetchBrokerConfig) -> Result<Self> {
        if config.limits.max_concurrent_requests == 0
            || config.limits.connect_timeout.is_zero()
            || config.limits.total_timeout.is_zero()
            || config.limits.max_headers == 0
            || config.limits.max_header_bytes == 0
            || config.limits.max_request_bytes == 0
            || config.limits.max_response_bytes == 0
            || config.limits.max_write_chunk == 0
            || config.limits.max_write_chunk > FETCH_ABI_SAFE_CHUNK
            || config.limits.max_read_chunk == 0
            || config.limits.max_read_chunk >= FETCH_ABI_SAFE_CHUNK
        {
            return Err(Error::State("fetch broker limits must be nonzero".into()));
        }
        Ok(Self {
            inner: Arc::new(FetchBrokerInner {
                config,
                active: AtomicUsize::new(0),
                dns_active: AtomicUsize::new(0),
                resolver: Arc::new(SystemResolver),
            }),
        })
    }

    pub fn denied() -> Self {
        Self::new(FetchBrokerConfig::default()).expect("default fetch broker is valid")
    }

    pub(crate) fn session(&self, deadline: Instant) -> FetchSession {
        FetchSession {
            inner: Arc::new(FetchSessionInner {
                broker: self.clone(),
                deadline: Mutex::new(deadline),
                next_id: AtomicU64::new(1),
                v2_next_id: AtomicU64::new(1),
                closed: AtomicBool::new(false),
                operations: Mutex::new(HashMap::new()),
                v2_operations: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub(crate) fn register(
        &self,
        target: &mut impl hyperlight_host::func::Registerable,
        session: FetchSession,
    ) -> Result<()> {
        let start = session.clone();
        target.register_host_function(
            "WorkerdFetchV1Start",
            move |json: String| -> hyperlight_host::Result<String> {
                start
                    .start_json(&json)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        let write = session.clone();
        target.register_host_function(
            "WorkerdFetchV1Write",
            move |operation_id: u64, chunk: Vec<u8>| -> hyperlight_host::Result<i32> {
                Ok(write.write(operation_id, chunk))
            },
        )?;
        let finish = session.clone();
        target.register_host_function(
            "WorkerdFetchV1Finish",
            move |operation_id: u64| -> hyperlight_host::Result<i32> {
                Ok(finish.finish(operation_id))
            },
        )?;
        let poll = session.clone();
        target.register_host_function(
            "WorkerdFetchV1Poll",
            move |operation_id: u64| -> hyperlight_host::Result<String> {
                poll.poll_json(operation_id)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        let read = session.clone();
        target.register_host_function(
            "WorkerdFetchV1Read",
            move |operation_id: u64, max_bytes: u64| -> hyperlight_host::Result<Vec<u8>> {
                read.read(operation_id, max_bytes)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        let cancel = session.clone();
        target.register_host_function(
            "WorkerdFetchV1Cancel",
            move |operation_id: u64| -> hyperlight_host::Result<i32> {
                Ok(cancel.cancel(operation_id))
            },
        )?;
        let v2_start = session.clone();
        target.register_host_function(
            "WorkerdFetchV2Start",
            move |json: String| -> hyperlight_host::Result<String> {
                v2_start
                    .v2_start_json(&json)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        let v2_write = session.clone();
        target.register_host_function(
            "WorkerdFetchV2Write",
            move |operation_id: u64, chunk: Vec<u8>| -> hyperlight_host::Result<i32> {
                Ok(v2_write.v2_write(operation_id, chunk))
            },
        )?;
        let v2_finish = session.clone();
        target.register_host_function(
            "WorkerdFetchV2Finish",
            move |operation_id: u64| -> hyperlight_host::Result<i32> {
                Ok(v2_finish.v2_finish(operation_id))
            },
        )?;
        let v2_poll = session.clone();
        target.register_host_function(
            "WorkerdFetchV2Poll",
            move |operation_id: u64| -> hyperlight_host::Result<String> {
                v2_poll
                    .v2_poll_json(operation_id)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        let v2_read = session.clone();
        target.register_host_function(
            "WorkerdFetchV2Read",
            move |operation_id: u64, max_bytes: u64| -> hyperlight_host::Result<Vec<u8>> {
                v2_read
                    .v2_read(operation_id, max_bytes)
                    .map_err(|error| hyperlight_host::new_error!("{error}"))
            },
        )?;
        target.register_host_function(
            "WorkerdFetchV2Cancel",
            move |operation_id: u64| -> hyperlight_host::Result<i32> {
                Ok(session.v2_cancel(operation_id))
            },
        )?;
        Ok(())
    }

    async fn execute_inner(
        self,
        request: PreparedFetchRequest,
        body: Vec<u8>,
        request_deadline: Instant,
    ) -> std::result::Result<CompletedFetch, FetchFailure> {
        request.validate(&self.inner.config.limits)?;
        let (response, deadline) = self
            .send_request(&request, Body::from(body), request_deadline)
            .await?;
        self.finish_response(request.request_id, response, deadline)
            .await
    }

    async fn send_request(
        &self,
        request: &PreparedFetchRequest,
        body: Body,
        request_deadline: Instant,
    ) -> std::result::Result<(Response, Instant), FetchFailure> {
        let broker_deadline = Instant::now()
            .checked_add(self.inner.config.limits.total_timeout)
            .unwrap_or(request_deadline)
            .min(request_deadline);
        check_deadline(broker_deadline)?;

        let url = Url::parse(&request.url)
            .map_err(|error| FetchFailure::invalid(format!("invalid URL: {error}")))?;
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|_| FetchFailure::invalid("invalid HTTP method"))?;
        if method == Method::CONNECT || method == Method::TRACE {
            return Err(FetchFailure::invalid("HTTP method is not supported"));
        }
        let mut headers = request.header_map()?;
        if let Some(body_length) = request.body_length {
            headers.insert(
                reqwest::header::CONTENT_LENGTH,
                HeaderValue::from_str(&body_length.to_string())
                    .map_err(|_| FetchFailure::invalid("invalid request body length"))?,
            );
        }
        let (host, port) = self.inner.config.policy.authorize_url(&url)?;
        let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
            vec![SocketAddr::new(ip, port)]
        } else {
            let resolver = self.inner.resolver.clone();
            let _dns_admission = DnsAdmission::acquire(self.inner.clone())?;
            let lookup_host = host.clone();
            let remaining = broker_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(FetchFailure::timeout());
            }
            tokio::time::timeout(
                remaining,
                tokio::task::spawn_blocking(move || resolver.resolve(&lookup_host, port)),
            )
            .await
            .map_err(|_| FetchFailure::timeout())?
            .map_err(|_| FetchFailure::cancelled("DNS lookup was cancelled"))?
            .map_err(|error| FetchFailure::dns(format!("DNS lookup failed: {error}")))?
        };
        self.inner.config.policy.authorize_addresses(&addresses)?;
        let remaining = broker_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(FetchFailure::timeout());
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(self.inner.config.limits.connect_timeout.min(remaining))
            .timeout(remaining)
            .resolve_to_addrs(&host, &addresses)
            .build()
            .map_err(|error| FetchFailure::connect(error.to_string()))?;
        let response = client
            .request(method, url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest_error)?;
        check_deadline(broker_deadline)?;
        Ok((response, broker_deadline))
    }

    async fn finish_response(
        &self,
        request_id: String,
        mut response: Response,
        deadline: Instant,
    ) -> std::result::Result<CompletedFetch, FetchFailure> {
        let headers = response_headers(response.headers(), &self.inner.config.limits)?;
        if response
            .content_length()
            .is_some_and(|length| length > self.inner.config.limits.max_response_bytes as u64)
        {
            return Err(FetchFailure::too_large("response body exceeds limit"));
        }
        let header_block = serde_json::to_vec(&headers)
            .map_err(|error| FetchFailure::connect(format!("response headers: {error}")))?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
            check_deadline(deadline)?;
            if body.len().saturating_add(chunk.len()) > self.inner.config.limits.max_response_bytes
            {
                return Err(FetchFailure::too_large("response body exceeds limit"));
            }
            body.extend_from_slice(&chunk);
        }
        let body_length = body.len() as u64;
        let mut output = Vec::with_capacity(header_block.len().saturating_add(body.len()));
        output.extend_from_slice(&header_block);
        output.extend_from_slice(&body);
        Ok(CompletedFetch {
            response: FetchResponse {
                protocol_version: FETCH_PROTOCOL_VERSION,
                request_id,
                status: response.status().as_u16(),
                header_block_length: header_block.len() as u64,
                body_length,
            },
            output,
        })
    }

    async fn execute_stream(
        self,
        request: PreparedFetchRequest,
        body_rx: mpsc::Receiver<std::io::Result<Vec<u8>>>,
        response_tx: mpsc::Sender<Vec<u8>>,
        shared: Arc<Mutex<V2Shared>>,
        deadline: Instant,
        max_read_chunk: usize,
    ) {
        let body = Body::wrap_stream(ReceiverStream::new(body_rx));
        let result = self.send_request(&request, body, deadline).await;
        let (mut response, deadline) = match result {
            Ok(result) => result,
            Err(error) => {
                set_v2_error(&shared, error);
                return;
            }
        };
        let headers = match response_headers(response.headers(), &self.inner.config.limits) {
            Ok(headers) => headers,
            Err(error) => {
                set_v2_error(&shared, error);
                return;
            }
        };
        let header_block = match serde_json::to_vec(&headers) {
            Ok(header_block) => header_block,
            Err(error) => {
                set_v2_error(
                    &shared,
                    FetchFailure::connect(format!("response headers: {error}")),
                );
                return;
            }
        };
        if let Ok(mut state) = shared.lock() {
            state.response = Some(V2Response {
                protocol_version: V2_PROTOCOL_VERSION,
                request_id: request.request_id,
                status: response.status().as_u16(),
                header_block_length: header_block.len() as u64,
                body_length: response.content_length(),
            });
        } else {
            return;
        }
        if send_v2_bytes(&response_tx, &header_block, max_read_chunk)
            .await
            .is_err()
        {
            return;
        }
        loop {
            if let Err(error) = check_deadline(deadline) {
                set_v2_error(&shared, error);
                return;
            }
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    if send_v2_bytes(&response_tx, &chunk, max_read_chunk)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(None) => {
                    if let Ok(mut state) = shared.lock() {
                        state.eof = true;
                    }
                    return;
                }
                Err(error) => {
                    set_v2_error(&shared, map_reqwest_error(error));
                    return;
                }
            }
        }
    }
}

async fn send_v2_bytes(
    sender: &mpsc::Sender<Vec<u8>>,
    bytes: &[u8],
    chunk_size: usize,
) -> std::result::Result<(), ()> {
    for chunk in bytes.chunks(chunk_size) {
        sender.send(chunk.to_vec()).await.map_err(|_| ())?;
    }
    Ok(())
}

fn set_v2_error(shared: &Mutex<V2Shared>, error: FetchFailure) {
    if let Ok(mut state) = shared.lock() {
        state.error = Some(error);
    }
}

const FETCH_ABI_SAFE_CHUNK: usize = 60 * 1024;
const FETCH_CHUNK_BYTES: usize = FETCH_ABI_SAFE_CHUNK;
const MAX_OPERATION_ID: u64 = (1u64 << 53) - 1;

#[derive(Clone)]
pub(crate) struct FetchSession {
    inner: Arc<FetchSessionInner>,
}

struct FetchSessionInner {
    broker: FetchBroker,
    deadline: Mutex<Instant>,
    next_id: AtomicU64,
    v2_next_id: AtomicU64,
    closed: AtomicBool,
    operations: Mutex<HashMap<u64, FetchOperation>>,
    v2_operations: Mutex<HashMap<u64, V2Operation>>,
}

struct FetchOperation {
    admission: Option<Admission>,
    state: FetchOperationState,
}

enum FetchOperationState {
    Receiving {
        request: FetchRequest,
        header_block: Vec<u8>,
        body: Vec<u8>,
    },
    Running {
        result: Arc<Mutex<Option<std::result::Result<CompletedFetch, FetchFailure>>>>,
        task: JoinHandle<()>,
    },
    Failed(FetchFailure),
    Complete {
        response: FetchResponse,
        output: Vec<u8>,
        offset: usize,
    },
}

struct CompletedFetch {
    response: FetchResponse,
    output: Vec<u8>,
}

const V2_PROTOCOL_VERSION: u32 = 2;
const V2_QUEUE_CHUNKS: usize = 4;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V2StartRequest {
    protocol_version: u32,
    request_id: String,
    method: String,
    url: String,
    header_block_length: u64,
    body_length: Option<u64>,
    preferred_write_chunk: u64,
    preferred_read_chunk: u64,
}

#[derive(Clone, Debug, Serialize)]
struct V2Response {
    protocol_version: u32,
    request_id: String,
    status: u16,
    header_block_length: u64,
    body_length: Option<u64>,
}

struct V2Operation {
    admission: Option<Admission>,
    max_write_chunk: usize,
    max_read_chunk: usize,
    state: V2OperationState,
}

enum V2OperationState {
    ReceivingHeaders {
        request: V2StartRequest,
        header_block: Vec<u8>,
    },
    Streaming {
        request_tx: Option<mpsc::Sender<std::io::Result<Vec<u8>>>>,
        request_bytes: u64,
        expected_body_length: Option<u64>,
        response_rx: mpsc::Receiver<Vec<u8>>,
        pending_response: VecDeque<u8>,
        shared: Arc<Mutex<V2Shared>>,
        task: JoinHandle<()>,
    },
    Failed(FetchFailure),
}

#[derive(Default)]
struct V2Shared {
    response: Option<V2Response>,
    error: Option<FetchFailure>,
    eof: bool,
}

impl V2StartRequest {
    fn validate(&self, limits: &FetchLimits) -> std::result::Result<(), FetchFailure> {
        if self.protocol_version != V2_PROTOCOL_VERSION {
            return Err(FetchFailure::invalid("unsupported fetch protocol version"));
        }
        if self.request_id.is_empty() || self.request_id.len() > 256 {
            return Err(FetchFailure::invalid("invalid request ID"));
        }
        if self.method.is_empty() || self.method.len() > 32 {
            return Err(FetchFailure::invalid("invalid HTTP method"));
        }
        if self.url.len() > 16 * 1024 {
            return Err(FetchFailure::invalid("URL exceeds limit"));
        }
        let max_header_block = limits
            .max_header_bytes
            .saturating_mul(6)
            .saturating_add(limits.max_headers.saturating_mul(32));
        if self.header_block_length > max_header_block as u64 {
            return Err(FetchFailure::too_large("request headers exceed limit"));
        }
        if self.preferred_write_chunk == 0
            || self.preferred_write_chunk > FETCH_ABI_SAFE_CHUNK as u64
            || self.preferred_read_chunk == 0
            || self.preferred_read_chunk >= FETCH_ABI_SAFE_CHUNK as u64
        {
            return Err(FetchFailure::invalid("invalid fetch chunk preference"));
        }
        Ok(())
    }
}

impl PreparedFetchRequest {
    fn from_v2(
        request: &V2StartRequest,
        headers: Vec<FetchHeader>,
        limits: &FetchLimits,
    ) -> std::result::Result<Self, FetchFailure> {
        if headers.len() > limits.max_headers {
            return Err(FetchFailure::too_large(
                "request header count exceeds limit",
            ));
        }
        let header_bytes = headers.iter().try_fold(0usize, |total, header| {
            total
                .checked_add(header.name.len())
                .and_then(|value| value.checked_add(header.value.len()))
        });
        if header_bytes.is_none_or(|bytes| bytes > limits.max_header_bytes) {
            return Err(FetchFailure::too_large("request headers exceed limit"));
        }
        let prepared = Self {
            protocol_version: V2_PROTOCOL_VERSION,
            request_id: request.request_id.clone(),
            method: request.method.clone(),
            url: request.url.clone(),
            headers,
            body_length: request.body_length,
        };
        // Validate method, URL and forbidden headers without applying v1 body limits.
        Method::from_bytes(prepared.method.as_bytes())
            .map_err(|_| FetchFailure::invalid("invalid HTTP method"))?;
        Url::parse(&prepared.url)
            .map_err(|error| FetchFailure::invalid(format!("invalid URL: {error}")))?;
        prepared.header_map()?;
        Ok(prepared)
    }
}

impl FetchSession {
    pub(crate) fn set_deadline(&self, deadline: Instant) {
        *self
            .inner
            .deadline
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = deadline;
    }

    fn deadline(&self) -> Instant {
        *self
            .inner
            .deadline
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn start_json(&self, json: &str) -> Result<String> {
        let result = self.start(json);
        let value = match result {
            Ok(operation_id) => serde_json::json!({
                "protocol_version": FETCH_PROTOCOL_VERSION,
                "operation_id": operation_id,
                "error": null
            }),
            Err(error) => serde_json::json!({
                "protocol_version": FETCH_PROTOCOL_VERSION,
                "operation_id": 0,
                "error": {
                    "code": error.code,
                    "message": error.message
                }
            }),
        };
        Ok(serde_json::to_string(&value)?)
    }

    fn start(&self, json: &str) -> std::result::Result<u64, FetchFailure> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(FetchFailure::cancelled("request VM is closing"));
        }
        if json.len() > crate::HOST_CALL_MAX {
            return Err(FetchFailure::invalid(
                "fetch metadata exceeds host-call limit",
            ));
        }
        let request: FetchRequest = serde_json::from_str(json)
            .map_err(|error| FetchFailure::invalid(format!("malformed fetch envelope: {error}")))?;
        request.validate(&self.inner.broker.inner.config.limits)?;
        let admission = Admission::acquire(self.inner.broker.inner.clone())?;
        let operation_id = self.inner.next_id.fetch_add(1, Ordering::AcqRel);
        if operation_id == 0 || operation_id > MAX_OPERATION_ID {
            return Err(FetchFailure::overloaded(
                "fetch operation ID space exhausted",
            ));
        }
        let header_capacity = usize::try_from(request.header_block_length)
            .map_err(|_| FetchFailure::too_large("request headers exceed limit"))?;
        let body_capacity = usize::try_from(request.body_length)
            .map_err(|_| FetchFailure::too_large("request body exceeds limit"))?;
        self.inner
            .operations
            .lock()
            .map_err(|_| FetchFailure::cancelled("fetch operation state poisoned"))?
            .insert(
                operation_id,
                FetchOperation {
                    admission: Some(admission),
                    state: FetchOperationState::Receiving {
                        request,
                        header_block: Vec::with_capacity(header_capacity),
                        body: Vec::with_capacity(body_capacity),
                    },
                },
            );
        Ok(operation_id)
    }

    fn write(&self, operation_id: u64, chunk: Vec<u8>) -> i32 {
        if chunk.len() > FETCH_CHUNK_BYTES {
            return -crate::errno::EFBIG;
        }
        let Ok(mut operations) = self.inner.operations.lock() else {
            return -crate::errno::EIO;
        };
        let Some(operation) = operations.get_mut(&operation_id) else {
            return -crate::errno::ENOENT;
        };
        let FetchOperationState::Receiving {
            request,
            header_block,
            body,
        } = &mut operation.state
        else {
            return -crate::errno::EINVAL;
        };
        let expected_total = request
            .header_block_length
            .saturating_add(request.body_length);
        let received_total = (header_block.len() as u64).saturating_add(body.len() as u64);
        if received_total.saturating_add(chunk.len() as u64) > expected_total {
            return -crate::errno::EFBIG;
        }
        let header_remaining =
            (request.header_block_length as usize).saturating_sub(header_block.len());
        let header_bytes = header_remaining.min(chunk.len());
        header_block.extend_from_slice(&chunk[..header_bytes]);
        body.extend_from_slice(&chunk[header_bytes..]);
        i32::try_from(chunk.len()).unwrap_or(i32::MAX)
    }

    fn finish(&self, operation_id: u64) -> i32 {
        let Ok(mut operations) = self.inner.operations.lock() else {
            return -crate::errno::EIO;
        };
        let Some(operation) = operations.get_mut(&operation_id) else {
            return -crate::errno::ENOENT;
        };
        let old = std::mem::replace(
            &mut operation.state,
            FetchOperationState::Complete {
                response: FetchResponse {
                    protocol_version: FETCH_PROTOCOL_VERSION,
                    request_id: String::new(),
                    status: 0,
                    header_block_length: 0,
                    body_length: 0,
                },
                output: Vec::new(),
                offset: 0,
            },
        );
        let FetchOperationState::Receiving {
            request,
            header_block,
            body,
        } = old
        else {
            operation.state = old;
            return -crate::errno::EINVAL;
        };
        if header_block.len() as u64 != request.header_block_length
            || body.len() as u64 != request.body_length
        {
            operation.state = FetchOperationState::Receiving {
                request,
                header_block,
                body,
            };
            return -crate::errno::EINVAL;
        }
        let headers: Vec<FetchHeader> = match serde_json::from_slice(&header_block) {
            Ok(headers) => headers,
            Err(error) => {
                operation.state = FetchOperationState::Failed(FetchFailure::invalid(format!(
                    "malformed header block: {error}"
                )));
                operation.admission.take();
                return 0;
            }
        };
        let request = match PreparedFetchRequest::from_parts(
            request,
            headers,
            &self.inner.broker.inner.config.limits,
        ) {
            Ok(request) => request,
            Err(error) => {
                operation.state = FetchOperationState::Failed(error);
                operation.admission.take();
                return 0;
            }
        };
        let result = Arc::new(Mutex::new(None));
        let task_result = result.clone();
        let broker = self.inner.broker.clone();
        let deadline = self.deadline();
        let task = fetch_runtime().spawn(async move {
            let request_id = request.request_id.clone();
            let completed = broker.execute_inner(request, body, deadline).await;
            let completed = completed.inspect_err(|error| {
                tracing::debug!(%request_id, message = %error.message, "outbound fetch failed");
            });
            if let Ok(mut slot) = task_result.lock() {
                *slot = Some(completed);
            }
        });
        operation.state = FetchOperationState::Running { result, task };
        0
    }

    fn poll_json(&self, operation_id: u64) -> Result<String> {
        let mut operations = self
            .inner
            .operations
            .lock()
            .map_err(|_| Error::State("fetch operation state poisoned".into()))?;
        let Some(mut operation) = operations.remove(&operation_id) else {
            return Ok(serde_json::to_string(&serde_json::json!({
                "protocol_version": FETCH_PROTOCOL_VERSION,
                "operation_id": operation_id,
                "state": "complete",
                "response": null,
                "error": {
                    "code": FetchErrorCode::InvalidRequest,
                    "message": "unknown fetch operation"
                }
            }))?);
        };
        let value = match &mut operation.state {
            FetchOperationState::Receiving { .. } => serde_json::json!({
                "protocol_version": FETCH_PROTOCOL_VERSION,
                "operation_id": operation_id,
                "state": "receiving",
                "response": null,
                "error": null
            }),
            FetchOperationState::Running { result, task } => {
                let completed = result
                    .lock()
                    .map_err(|_| Error::State("fetch result state poisoned".into()))?
                    .take();
                match completed {
                    None if task.is_finished() => {
                        return Ok(serde_json::to_string(&serde_json::json!({
                            "protocol_version": FETCH_PROTOCOL_VERSION,
                            "operation_id": operation_id,
                            "state": "complete",
                            "response": null,
                            "error": {
                                "code": FetchErrorCode::Cancelled,
                                "message": "outbound task ended without a result"
                            }
                        }))?);
                    }
                    None => serde_json::json!({
                        "protocol_version": FETCH_PROTOCOL_VERSION,
                        "operation_id": operation_id,
                        "state": "pending",
                        "response": null,
                        "error": null
                    }),
                    Some(Ok(completed)) => {
                        operation.state = FetchOperationState::Complete {
                            response: completed.response,
                            output: completed.output,
                            offset: 0,
                        };
                        let FetchOperationState::Complete { response, .. } = &operation.state
                        else {
                            unreachable!()
                        };
                        serde_json::json!({
                            "protocol_version": FETCH_PROTOCOL_VERSION,
                            "operation_id": operation_id,
                            "state": "complete",
                            "response": response,
                            "error": null
                        })
                    }
                    Some(Err(error)) => {
                        task.abort();
                        operation.admission.take();
                        return Ok(serde_json::to_string(&serde_json::json!({
                            "protocol_version": FETCH_PROTOCOL_VERSION,
                            "operation_id": operation_id,
                            "state": "complete",
                            "response": null,
                            "error": {
                                "code": error.code,
                                "message": error.message
                            }
                        }))?);
                    }
                }
            }
            FetchOperationState::Failed(error) => {
                operation.admission.take();
                return Ok(serde_json::to_string(&serde_json::json!({
                    "protocol_version": FETCH_PROTOCOL_VERSION,
                    "operation_id": operation_id,
                    "state": "complete",
                    "response": null,
                    "error": {
                        "code": error.code,
                        "message": error.message
                    }
                }))?);
            }
            FetchOperationState::Complete { response, .. } => serde_json::json!({
                "protocol_version": FETCH_PROTOCOL_VERSION,
                "operation_id": operation_id,
                "state": "complete",
                "response": response,
                "error": null
            }),
        };
        operations.insert(operation_id, operation);
        Ok(serde_json::to_string(&value)?)
    }

    fn read(&self, operation_id: u64, max_bytes: u64) -> Result<Vec<u8>> {
        let requested = usize::try_from(max_bytes)
            .unwrap_or(usize::MAX)
            .min(FETCH_CHUNK_BYTES);
        if requested == 0 {
            return Err(Error::State("fetch read size must be nonzero".into()));
        }
        let mut operations = self
            .inner
            .operations
            .lock()
            .map_err(|_| Error::State("fetch operation state poisoned".into()))?;
        let Some(operation) = operations.get_mut(&operation_id) else {
            return Err(Error::State("unknown fetch operation".into()));
        };
        let FetchOperationState::Complete {
            output,
            offset,
            response: _,
        } = &mut operation.state
        else {
            return Err(Error::State("fetch response is not complete".into()));
        };
        let end = offset.saturating_add(requested).min(output.len());
        let chunk = output[*offset..end].to_vec();
        *offset = end;
        if *offset == output.len() {
            operations.remove(&operation_id);
        }
        Ok(chunk)
    }

    fn cancel(&self, operation_id: u64) -> i32 {
        let Ok(mut operations) = self.inner.operations.lock() else {
            return -crate::errno::EIO;
        };
        let Some(operation) = operations.get_mut(&operation_id) else {
            return if operation_id > 0 && operation_id < self.inner.next_id.load(Ordering::Acquire)
            {
                0
            } else {
                -crate::errno::ENOENT
            };
        };
        if let FetchOperationState::Running { task, .. } = &operation.state {
            task.abort();
        }
        operation.state =
            FetchOperationState::Failed(FetchFailure::cancelled("outbound request was cancelled"));
        operation.admission.take();
        0
    }

    fn v2_start_json(&self, json: &str) -> Result<String> {
        let result = self.v2_start(json);
        let value = match result {
            Ok((operation_id, max_write_chunk, max_read_chunk)) => serde_json::json!({
                "protocol_version": V2_PROTOCOL_VERSION,
                "operation_id": operation_id,
                "max_write_chunk": max_write_chunk,
                "max_read_chunk": max_read_chunk,
                "error": null
            }),
            Err(error) => serde_json::json!({
                "protocol_version": V2_PROTOCOL_VERSION,
                "operation_id": 0,
                "max_write_chunk": 0,
                "max_read_chunk": 0,
                "error": {
                    "code": error.code,
                    "message": error.message
                }
            }),
        };
        Ok(serde_json::to_string(&value)?)
    }

    fn v2_start(&self, json: &str) -> std::result::Result<(u64, usize, usize), FetchFailure> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(FetchFailure::cancelled("request VM is closing"));
        }
        if json.len() > FETCH_ABI_SAFE_CHUNK {
            return Err(FetchFailure::invalid(
                "fetch metadata exceeds host-call limit",
            ));
        }
        let request: V2StartRequest = serde_json::from_str(json)
            .map_err(|error| FetchFailure::invalid(format!("malformed fetch envelope: {error}")))?;
        request.validate(&self.inner.broker.inner.config.limits)?;
        if request.header_block_length == 0 {
            return Err(FetchFailure::invalid(
                "header block must contain a JSON array",
            ));
        }
        let admission = Admission::acquire(self.inner.broker.inner.clone())?;
        let operation_id = self.inner.v2_next_id.fetch_add(1, Ordering::AcqRel);
        if operation_id == 0 || operation_id > MAX_OPERATION_ID {
            return Err(FetchFailure::overloaded(
                "fetch operation ID space exhausted",
            ));
        }
        let max_write_chunk = (request.preferred_write_chunk as usize)
            .min(self.inner.broker.inner.config.limits.max_write_chunk)
            .min(FETCH_ABI_SAFE_CHUNK);
        let max_read_chunk = (request.preferred_read_chunk as usize)
            .min(self.inner.broker.inner.config.limits.max_read_chunk)
            .min(FETCH_ABI_SAFE_CHUNK - 1);
        let header_capacity = usize::try_from(request.header_block_length)
            .map_err(|_| FetchFailure::too_large("request headers exceed limit"))?;
        self.inner
            .v2_operations
            .lock()
            .map_err(|_| FetchFailure::cancelled("fetch operation state poisoned"))?
            .insert(
                operation_id,
                V2Operation {
                    admission: Some(admission),
                    max_write_chunk,
                    max_read_chunk,
                    state: V2OperationState::ReceivingHeaders {
                        request,
                        header_block: Vec::with_capacity(header_capacity),
                    },
                },
            );
        Ok((operation_id, max_write_chunk, max_read_chunk))
    }

    fn v2_write(&self, operation_id: u64, chunk: Vec<u8>) -> i32 {
        let Ok(mut operations) = self.inner.v2_operations.lock() else {
            return -crate::errno::EIO;
        };
        let Some(mut operation) = operations.remove(&operation_id) else {
            return -crate::errno::ENOENT;
        };
        if chunk.len() > operation.max_write_chunk {
            operations.insert(operation_id, operation);
            return -crate::errno::EFBIG;
        }
        let accepted = i32::try_from(chunk.len()).unwrap_or(i32::MAX);
        match operation.state {
            V2OperationState::ReceivingHeaders {
                request,
                mut header_block,
            } => {
                let remaining =
                    (request.header_block_length as usize).saturating_sub(header_block.len());
                let header_bytes = remaining.min(chunk.len());
                let body = &chunk[header_bytes..];
                if request
                    .body_length
                    .is_some_and(|length| body.len() as u64 > length)
                {
                    operation.state = V2OperationState::ReceivingHeaders {
                        request,
                        header_block,
                    };
                    operations.insert(operation_id, operation);
                    return -crate::errno::EFBIG;
                }
                header_block.extend_from_slice(&chunk[..header_bytes]);
                if header_block.len() < request.header_block_length as usize {
                    operation.state = V2OperationState::ReceivingHeaders {
                        request,
                        header_block,
                    };
                    operations.insert(operation_id, operation);
                    return accepted;
                }
                let headers: Vec<FetchHeader> = match serde_json::from_slice(&header_block) {
                    Ok(headers) => headers,
                    Err(error) => {
                        operation.admission.take();
                        operation.state = V2OperationState::Failed(FetchFailure::invalid(format!(
                            "malformed header block: {error}"
                        )));
                        operations.insert(operation_id, operation);
                        return accepted;
                    }
                };
                let prepared = match PreparedFetchRequest::from_v2(
                    &request,
                    headers,
                    &self.inner.broker.inner.config.limits,
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        operation.admission.take();
                        operation.state = V2OperationState::Failed(error);
                        operations.insert(operation_id, operation);
                        return accepted;
                    }
                };
                let (request_tx, request_rx) = mpsc::channel(V2_QUEUE_CHUNKS);
                let (response_tx, response_rx) = mpsc::channel(V2_QUEUE_CHUNKS);
                if !body.is_empty() && request_tx.try_send(Ok(body.to_vec())).is_err() {
                    operation.state = V2OperationState::Failed(FetchFailure::overloaded(
                        "request body queue unavailable",
                    ));
                    operation.admission.take();
                    operations.insert(operation_id, operation);
                    return accepted;
                }
                let shared = Arc::new(Mutex::new(V2Shared::default()));
                let broker = self.inner.broker.clone();
                let task_shared = shared.clone();
                let deadline = self.deadline();
                let max_read_chunk = operation.max_read_chunk;
                let task = fetch_runtime().spawn(async move {
                    broker
                        .execute_stream(
                            prepared,
                            request_rx,
                            response_tx,
                            task_shared,
                            deadline,
                            max_read_chunk,
                        )
                        .await;
                });
                operation.state = V2OperationState::Streaming {
                    request_tx: Some(request_tx),
                    request_bytes: body.len() as u64,
                    expected_body_length: request.body_length,
                    response_rx,
                    pending_response: VecDeque::new(),
                    shared,
                    task,
                };
                operations.insert(operation_id, operation);
                accepted
            }
            V2OperationState::Streaming {
                request_tx,
                request_bytes,
                expected_body_length,
                response_rx,
                pending_response,
                shared,
                task,
            } => {
                let response_started = shared
                    .lock()
                    .map(|state| state.response.is_some() || state.error.is_some())
                    .unwrap_or(true);
                if response_started || request_tx.is_none() {
                    operation.state = V2OperationState::Streaming {
                        request_tx,
                        request_bytes,
                        expected_body_length,
                        response_rx,
                        pending_response,
                        shared,
                        task,
                    };
                    operations.insert(operation_id, operation);
                    return -crate::errno::EPIPE;
                }
                if expected_body_length
                    .is_some_and(|length| request_bytes + chunk.len() as u64 > length)
                {
                    operation.state = V2OperationState::Streaming {
                        request_tx,
                        request_bytes,
                        expected_body_length,
                        response_rx,
                        pending_response,
                        shared,
                        task,
                    };
                    operations.insert(operation_id, operation);
                    return -crate::errno::EFBIG;
                }
                let send = request_tx
                    .as_ref()
                    .expect("checked above")
                    .try_send(Ok(chunk));
                let result = match send {
                    Ok(()) => accepted,
                    Err(mpsc::error::TrySendError::Full(_)) => -crate::errno::EAGAIN,
                    Err(mpsc::error::TrySendError::Closed(_)) => -crate::errno::EPIPE,
                };
                operation.state = V2OperationState::Streaming {
                    request_tx,
                    request_bytes: if result >= 0 {
                        request_bytes + accepted as u64
                    } else {
                        request_bytes
                    },
                    expected_body_length,
                    response_rx,
                    pending_response,
                    shared,
                    task,
                };
                operations.insert(operation_id, operation);
                result
            }
            state @ V2OperationState::Failed(_) => {
                operation.state = state;
                operations.insert(operation_id, operation);
                -crate::errno::EPIPE
            }
        }
    }

    fn v2_finish(&self, operation_id: u64) -> i32 {
        let Ok(mut operations) = self.inner.v2_operations.lock() else {
            return -crate::errno::EIO;
        };
        let Some(operation) = operations.get_mut(&operation_id) else {
            return -crate::errno::ENOENT;
        };
        match &mut operation.state {
            V2OperationState::ReceivingHeaders { .. } => -crate::errno::EINVAL,
            V2OperationState::Streaming {
                request_tx,
                request_bytes,
                expected_body_length,
                ..
            } => {
                if expected_body_length.is_some_and(|length| *request_bytes != length) {
                    return -crate::errno::EINVAL;
                }
                request_tx.take();
                0
            }
            V2OperationState::Failed(_) => -crate::errno::EPIPE,
        }
    }

    fn v2_poll_json(&self, operation_id: u64) -> Result<String> {
        let mut operations = self
            .inner
            .v2_operations
            .lock()
            .map_err(|_| Error::State("fetch operation state poisoned".into()))?;
        let Some(mut operation) = operations.remove(&operation_id) else {
            return v2_poll_error(
                operation_id,
                FetchFailure::invalid("unknown fetch operation"),
            );
        };
        let value = match &mut operation.state {
            V2OperationState::ReceivingHeaders { .. } => serde_json::json!({
                "protocol_version": V2_PROTOCOL_VERSION,
                "operation_id": operation_id,
                "state": "receiving_headers",
                "response": null,
                "error": null
            }),
            V2OperationState::Failed(error) => {
                operation.admission.take();
                return v2_poll_error(operation_id, error.clone());
            }
            V2OperationState::Streaming { shared, task, .. } => {
                let state = shared
                    .lock()
                    .map_err(|_| Error::State("fetch stream state poisoned".into()))?;
                if let Some(error) = &state.error {
                    let error = error.clone();
                    drop(state);
                    task.abort();
                    operation.admission.take();
                    return v2_poll_error(operation_id, error);
                }
                if let Some(response) = &state.response {
                    serde_json::json!({
                        "protocol_version": V2_PROTOCOL_VERSION,
                        "operation_id": operation_id,
                        "state": if state.eof { "complete" } else { "response" },
                        "response": response,
                        "error": null
                    })
                } else {
                    serde_json::json!({
                        "protocol_version": V2_PROTOCOL_VERSION,
                        "operation_id": operation_id,
                        "state": "uploading",
                        "response": null,
                        "error": null
                    })
                }
            }
        };
        operations.insert(operation_id, operation);
        Ok(serde_json::to_string(&value)?)
    }

    fn v2_read(&self, operation_id: u64, max_bytes: u64) -> Result<Vec<u8>> {
        let mut operations = self
            .inner
            .v2_operations
            .lock()
            .map_err(|_| Error::State("fetch operation state poisoned".into()))?;
        let Some(mut operation) = operations.remove(&operation_id) else {
            return Err(Error::State("unknown fetch operation".into()));
        };
        let requested = usize::try_from(max_bytes).unwrap_or(usize::MAX);
        if requested == 0 || requested > operation.max_read_chunk {
            operations.insert(operation_id, operation);
            return Err(Error::State("invalid fetch read size".into()));
        }
        let result = match &mut operation.state {
            V2OperationState::ReceivingHeaders { .. } => vec![0],
            V2OperationState::Failed(_) => {
                operations.insert(operation_id, operation);
                return Err(Error::State(
                    "fetch operation failed; poll for the error".into(),
                ));
            }
            V2OperationState::Streaming {
                response_rx,
                pending_response,
                shared,
                ..
            } => {
                if !pending_response.is_empty() {
                    let count = requested.min(pending_response.len());
                    let mut result = Vec::with_capacity(count + 1);
                    result.push(1);
                    result.extend(pending_response.drain(..count));
                    result
                } else {
                    match response_rx.try_recv() {
                        Ok(chunk) => {
                            let count = requested.min(chunk.len());
                            let mut result = Vec::with_capacity(count + 1);
                            result.push(1);
                            result.extend_from_slice(&chunk[..count]);
                            pending_response.extend(&chunk[count..]);
                            result
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            let eof = shared.lock().is_ok_and(|state| state.eof);
                            if eof { vec![2] } else { vec![0] }
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            if shared.lock().is_ok_and(|state| state.eof) {
                                vec![2]
                            } else {
                                vec![0]
                            }
                        }
                    }
                }
            }
        };
        if result == [2] {
            // Reading EOF is the successful operation's collection point.
            return Ok(result);
        }
        operations.insert(operation_id, operation);
        Ok(result)
    }

    fn v2_cancel(&self, operation_id: u64) -> i32 {
        let Ok(mut operations) = self.inner.v2_operations.lock() else {
            return -crate::errno::EIO;
        };
        let Some(operation) = operations.get_mut(&operation_id) else {
            return if operation_id > 0
                && operation_id < self.inner.v2_next_id.load(Ordering::Acquire)
            {
                0
            } else {
                -crate::errno::ENOENT
            };
        };
        if let V2OperationState::Streaming { task, .. } = &operation.state {
            task.abort();
        }
        operation.state =
            V2OperationState::Failed(FetchFailure::cancelled("outbound request was cancelled"));
        operation.admission.take();
        0
    }

    pub(crate) fn cancel_all(&self) {
        self.inner.closed.store(true, Ordering::Release);
        if let Ok(mut operations) = self.inner.operations.lock() {
            for (_, operation) in operations.drain() {
                if let FetchOperationState::Running { task, .. } = operation.state {
                    task.abort();
                }
            }
        }
        if let Ok(mut operations) = self.inner.v2_operations.lock() {
            for (_, operation) in operations.drain() {
                if let V2OperationState::Streaming { task, .. } = operation.state {
                    task.abort();
                }
            }
        }
    }
}

impl Drop for FetchSessionInner {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(operations) = self.operations.get_mut() {
            for (_, operation) in operations.drain() {
                if let FetchOperationState::Running { task, .. } = operation.state {
                    task.abort();
                }
            }
        }
        if let Ok(operations) = self.v2_operations.get_mut() {
            for (_, operation) in operations.drain() {
                if let V2OperationState::Streaming { task, .. } = operation.state {
                    task.abort();
                }
            }
        }
    }
}

fn v2_poll_error(operation_id: u64, error: FetchFailure) -> Result<String> {
    Ok(serde_json::to_string(&serde_json::json!({
        "protocol_version": V2_PROTOCOL_VERSION,
        "operation_id": operation_id,
        "state": "complete",
        "response": null,
        "error": {
            "code": error.code,
            "message": error.message
        }
    }))?)
}

struct Admission {
    inner: Arc<FetchBrokerInner>,
}

struct DnsAdmission {
    inner: Arc<FetchBrokerInner>,
}

impl DnsAdmission {
    fn acquire(inner: Arc<FetchBrokerInner>) -> std::result::Result<Self, FetchFailure> {
        inner
            .dns_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < inner.config.limits.max_concurrent_requests).then_some(active + 1)
            })
            .map_err(|_| FetchFailure::overloaded("DNS resolver limit reached"))?;
        Ok(Self { inner })
    }
}

impl Drop for DnsAdmission {
    fn drop(&mut self) {
        self.inner.dns_active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn fetch_runtime() -> &'static Runtime {
    static RUNTIME: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("workerd-fetch")
            .build()
            .expect("fetch runtime configuration is valid")
    })
}

impl Admission {
    fn acquire(inner: Arc<FetchBrokerInner>) -> std::result::Result<Self, FetchFailure> {
        inner
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < inner.config.limits.max_concurrent_requests).then_some(active + 1)
            })
            .map_err(|_| FetchFailure::overloaded("outbound request limit reached"))?;
        Ok(Self { inner })
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FetchRequest {
    pub protocol_version: u32,
    pub request_id: String,
    pub method: String,
    pub url: String,
    pub header_block_length: u64,
    pub body_length: u64,
}

impl FetchRequest {
    fn validate(&self, limits: &FetchLimits) -> std::result::Result<(), FetchFailure> {
        if self.protocol_version != FETCH_PROTOCOL_VERSION {
            return Err(FetchFailure::invalid("unsupported fetch protocol version"));
        }
        if self.request_id.is_empty() || self.request_id.len() > 256 {
            return Err(FetchFailure::invalid("invalid request ID"));
        }
        if self.url.len() > 16 * 1024 {
            return Err(FetchFailure::invalid("URL exceeds limit"));
        }
        if self.method.is_empty() || self.method.len() > 32 {
            return Err(FetchFailure::invalid("invalid HTTP method"));
        }
        let max_header_block = limits
            .max_header_bytes
            .saturating_mul(6)
            .saturating_add(limits.max_headers.saturating_mul(32));
        if self.header_block_length > max_header_block as u64 {
            return Err(FetchFailure::too_large("request headers exceed limit"));
        }
        if self.body_length > limits.max_request_bytes as u64 {
            return Err(FetchFailure::too_large("request body exceeds limit"));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PreparedFetchRequest {
    protocol_version: u32,
    request_id: String,
    method: String,
    url: String,
    headers: Vec<FetchHeader>,
    body_length: Option<u64>,
}

impl PreparedFetchRequest {
    fn from_parts(
        request: FetchRequest,
        headers: Vec<FetchHeader>,
        limits: &FetchLimits,
    ) -> std::result::Result<Self, FetchFailure> {
        if headers.len() > limits.max_headers {
            return Err(FetchFailure::too_large(
                "request header count exceeds limit",
            ));
        }
        let header_bytes = headers.iter().try_fold(0usize, |total, header| {
            total
                .checked_add(header.name.len())
                .and_then(|value| value.checked_add(header.value.len()))
        });
        if header_bytes.is_none_or(|bytes| bytes > limits.max_header_bytes) {
            return Err(FetchFailure::too_large("request headers exceed limit"));
        }
        Ok(Self {
            protocol_version: request.protocol_version,
            request_id: request.request_id,
            method: request.method,
            url: request.url,
            headers,
            body_length: Some(request.body_length),
        })
    }

    fn validate(&self, limits: &FetchLimits) -> std::result::Result<(), FetchFailure> {
        FetchRequest {
            protocol_version: self.protocol_version,
            request_id: self.request_id.clone(),
            method: self.method.clone(),
            url: self.url.clone(),
            header_block_length: 0,
            body_length: self.body_length.unwrap_or(0),
        }
        .validate(limits)
    }

    fn header_map(&self) -> std::result::Result<HeaderMap, FetchFailure> {
        let mut result = HeaderMap::new();
        for header in &self.headers {
            let name = HeaderName::from_str(&header.name)
                .map_err(|_| FetchFailure::invalid("invalid request header name"))?;
            if is_forbidden_request_header(&name) {
                return Err(FetchFailure::invalid("forbidden request header"));
            }
            let value = HeaderValue::from_str(&header.value)
                .map_err(|_| FetchFailure::invalid("invalid request header value"))?;
            result.append(name, value);
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FetchHeader {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FetchResponse {
    pub protocol_version: u32,
    pub request_id: String,
    pub status: u16,
    pub header_block_length: u64,
    pub body_length: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchErrorCode {
    InvalidRequest,
    PolicyDenied,
    DnsFailed,
    ConnectFailed,
    Timeout,
    Cancelled,
    ResponseTooLarge,
    RedirectLimit,
    Overloaded,
}

#[derive(Clone)]
struct FetchFailure {
    code: FetchErrorCode,
    message: String,
}

impl FetchFailure {
    fn new(code: FetchErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::InvalidRequest, message)
    }
    fn policy(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::PolicyDenied, message)
    }
    fn dns(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::DnsFailed, message)
    }
    fn connect(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::ConnectFailed, message)
    }
    fn timeout() -> Self {
        Self::new(FetchErrorCode::Timeout, "outbound request timed out")
    }
    fn cancelled(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::Cancelled, message)
    }
    fn too_large(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::ResponseTooLarge, message)
    }
    fn overloaded(message: impl Into<String>) -> Self {
        Self::new(FetchErrorCode::Overloaded, message)
    }
}

fn check_deadline(deadline: Instant) -> std::result::Result<(), FetchFailure> {
    if Instant::now() >= deadline {
        return Err(FetchFailure::timeout());
    }
    Ok(())
}

fn is_forbidden_request_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "content-length"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn response_headers(
    headers: &HeaderMap,
    limits: &FetchLimits,
) -> std::result::Result<Vec<FetchHeader>, FetchFailure> {
    if headers.len() > limits.max_headers {
        return Err(FetchFailure::too_large(
            "response header count exceeds limit",
        ));
    }
    let mut bytes = 0usize;
    let mut result = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|total| total.checked_add(value.as_bytes().len()))
            .ok_or_else(|| FetchFailure::too_large("response headers exceed limit"))?;
        if bytes > limits.max_header_bytes {
            return Err(FetchFailure::too_large("response headers exceed limit"));
        }
        if is_hop_by_hop(name) {
            continue;
        }
        result.push(FetchHeader {
            name: name.as_str().into(),
            value: value
                .to_str()
                .map_err(|_| FetchFailure::invalid("response header is not valid text"))?
                .into(),
        });
    }
    Ok(result)
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn map_reqwest_error(error: reqwest::Error) -> FetchFailure {
    if error.is_timeout() {
        FetchFailure::timeout()
    } else {
        FetchFailure::connect(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    struct StaticResolver {
        addresses: Mutex<Vec<SocketAddr>>,
    }

    impl Resolver for StaticResolver {
        fn resolve(&self, _host: &str, _port: u16) -> std::io::Result<Vec<SocketAddr>> {
            Ok(self.addresses.lock().unwrap().clone())
        }
    }

    fn broker(port: u16, max_response_bytes: usize, max_concurrent_requests: usize) -> FetchBroker {
        let config = FetchBrokerConfig {
            policy: FetchPolicy::new(
                NetworkPolicy::AllowList(crate::AllowList::from_hosts(&["localhost"]).unwrap()),
                ["http"],
                [port],
            )
            .allow_loopback(true),
            limits: FetchLimits {
                max_response_bytes,
                max_concurrent_requests,
                ..FetchLimits::default()
            },
        };
        let mut broker = FetchBroker::new(config).unwrap();
        Arc::get_mut(&mut broker.inner).unwrap().resolver = Arc::new(StaticResolver {
            addresses: Mutex::new(vec![SocketAddr::from(([127, 0, 0, 1], port))]),
        });
        broker
    }

    fn request(port: u16, method: &str, path: &str, body: &[u8]) -> FetchRequest {
        FetchRequest {
            protocol_version: FETCH_PROTOCOL_VERSION,
            request_id: "fetch-1".into(),
            method: method.into(),
            url: format!("http://localhost:{port}{path}"),
            header_block_length: 2,
            body_length: body.len() as u64,
        }
    }

    fn serve(
        handler: impl Fn(String, Vec<u8>) -> (u16, Vec<(String, String)>, Vec<u8>)
        + Send
        + Sync
        + 'static,
    ) -> (u16, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let handler = Arc::new(handler);
        thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            while !stop_thread.load(Ordering::Acquire) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                };
                let handler = handler.clone();
                thread::spawn(move || {
                    stream.set_nonblocking(false).unwrap();
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut first = String::new();
                    reader.read_line(&mut first).unwrap();
                    let mut length = 0usize;
                    loop {
                        let mut line = String::new();
                        reader.read_line(&mut line).unwrap();
                        if line == "\r\n" {
                            break;
                        }
                        if let Some(value) =
                            line.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            length = value.trim().parse().unwrap();
                        }
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).unwrap();
                    let (status, headers, response) = handler(first, body);
                    write!(stream, "HTTP/1.1 {status} Test\r\n").unwrap();
                    for (name, value) in headers {
                        write!(stream, "{name}: {value}\r\n").unwrap();
                    }
                    write!(
                        stream,
                        "Content-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .unwrap();
                    stream.write_all(&response).unwrap();
                });
            }
        });
        (port, stop)
    }

    fn run(session: &FetchSession, request: FetchRequest, body: &[u8]) -> serde_json::Value {
        run_with_headers(session, request, b"[]", body)
    }

    fn run_with_headers(
        session: &FetchSession,
        request: FetchRequest,
        header_block: &[u8],
        body: &[u8],
    ) -> serde_json::Value {
        let started: serde_json::Value = serde_json::from_str(
            &session
                .start_json(&serde_json::to_string(&request).unwrap())
                .unwrap(),
        )
        .unwrap();
        let operation_id = started["operation_id"].as_u64().unwrap();
        for chunk in header_block.chunks(FETCH_CHUNK_BYTES) {
            assert_eq!(
                session.write(operation_id, chunk.to_vec()),
                chunk.len() as i32
            );
        }
        for chunk in body.chunks(FETCH_CHUNK_BYTES) {
            assert_eq!(
                session.write(operation_id, chunk.to_vec()),
                chunk.len() as i32
            );
        }
        assert_eq!(session.finish(operation_id), 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let poll: serde_json::Value =
                serde_json::from_str(&session.poll_json(operation_id).unwrap()).unwrap();
            if poll["state"] == "complete" {
                return poll;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn read_response(
        session: &FetchSession,
        poll: &serde_json::Value,
    ) -> (Vec<FetchHeader>, Vec<u8>) {
        let operation_id = poll["operation_id"].as_u64().unwrap();
        let header_length = poll["response"]["header_block_length"].as_u64().unwrap() as usize;
        let body_length = poll["response"]["body_length"].as_u64().unwrap() as usize;
        let mut output = Vec::with_capacity(header_length + body_length);
        while output.len() < header_length + body_length {
            output.extend(session.read(operation_id, 1024).unwrap());
        }
        let headers = serde_json::from_slice(&output[..header_length]).unwrap();
        (headers, output[header_length..].to_vec())
    }

    fn v2_start(
        session: &FetchSession,
        port: u16,
        method: &str,
        body_length: Option<u64>,
        preferred_write_chunk: u64,
        preferred_read_chunk: u64,
    ) -> (u64, usize, usize) {
        let metadata = serde_json::json!({
            "protocol_version": 2,
            "request_id": "fetch-v2",
            "method": method,
            "url": format!("http://localhost:{port}/"),
            "header_block_length": 2,
            "body_length": body_length,
            "preferred_write_chunk": preferred_write_chunk,
            "preferred_read_chunk": preferred_read_chunk
        });
        let started: serde_json::Value =
            serde_json::from_str(&session.v2_start_json(&metadata.to_string()).unwrap()).unwrap();
        assert!(started["error"].is_null(), "{started}");
        assert_eq!(
            started
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<HashSet<_>>(),
            HashSet::from([
                "protocol_version",
                "operation_id",
                "max_write_chunk",
                "max_read_chunk",
                "error",
            ])
        );
        (
            started["operation_id"].as_u64().unwrap(),
            started["max_write_chunk"].as_u64().unwrap() as usize,
            started["max_read_chunk"].as_u64().unwrap() as usize,
        )
    }

    fn v2_run(
        session: &FetchSession,
        port: u16,
        method: &str,
        body: &[u8],
        preferred_write_chunk: u64,
        preferred_read_chunk: u64,
    ) -> (serde_json::Value, Vec<FetchHeader>, Vec<u8>) {
        let (operation_id, max_write_chunk, max_read_chunk) = v2_start(
            session,
            port,
            method,
            Some(body.len() as u64),
            preferred_write_chunk,
            preferred_read_chunk,
        );
        assert_eq!(max_write_chunk, preferred_write_chunk as usize);
        assert_eq!(max_read_chunk, preferred_read_chunk as usize);
        assert_eq!(session.v2_write(operation_id, b"[]".to_vec()), 2);
        for chunk in body.chunks(max_write_chunk) {
            loop {
                let result = session.v2_write(operation_id, chunk.to_vec());
                if result == -crate::errno::EAGAIN {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                assert_eq!(result, chunk.len() as i32);
                break;
            }
        }
        assert_eq!(session.v2_finish(operation_id), 0);

        let deadline = Instant::now() + Duration::from_secs(3);
        let response = loop {
            let poll: serde_json::Value =
                serde_json::from_str(&session.v2_poll_json(operation_id).unwrap()).unwrap();
            assert!(poll["error"].is_null(), "{poll}");
            if !poll["response"].is_null() {
                break poll;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        };
        let header_length = response["response"]["header_block_length"]
            .as_u64()
            .unwrap() as usize;
        let mut output = Vec::new();
        loop {
            let chunk = session
                .v2_read(operation_id, max_read_chunk as u64)
                .unwrap();
            assert!(chunk.len() <= max_read_chunk + 1);
            match chunk[0] {
                0 => {
                    assert!(Instant::now() < deadline);
                    thread::sleep(Duration::from_millis(1));
                }
                1 => output.extend_from_slice(&chunk[1..]),
                2 => break,
                tag => panic!("unexpected v2 read tag {tag}"),
            }
        }
        let headers = serde_json::from_slice(&output[..header_length]).unwrap();
        (response, headers, output[header_length..].to_vec())
    }

    #[test]
    fn allowed_loopback_get_post_and_fresh_operations() {
        let (port, stop) = serve(|line, body| {
            if line.starts_with("POST ") {
                (200, vec![], body)
            } else {
                (200, vec![], b"get".to_vec())
            }
        });
        let broker = broker(port, 1024, 2);
        let session = broker.session(Instant::now() + Duration::from_secs(5));
        for (method, body, expected) in [
            ("GET", &b""[..], &b"get"[..]),
            ("POST", &b"post"[..], &b"post"[..]),
        ] {
            let poll = run(&session, request(port, method, "/", body), body);
            assert_eq!(poll["response"]["status"], 200);
            assert_eq!(read_response(&session, &poll).1, expected);
        }
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn policy_denies_host_port_address_private_and_metadata() {
        let (port, stop) = serve(|_, _| (200, vec![], vec![]));
        let broker = broker(port, 1024, 1);
        let session = broker.session(Instant::now() + Duration::from_secs(5));
        let mut denied = request(port, "GET", "/", &[]);
        denied.url = format!("http://example.test:{port}/");
        assert_eq!(run(&session, denied, &[])["error"]["code"], "policy_denied");
        for url in [
            "http://localhost:1/",
            "http://127.0.0.2:80/",
            "http://10.0.0.1:80/",
            "http://169.254.169.254:80/",
        ] {
            let mut denied = request(port, "GET", "/", &[]);
            denied.url = url.into();
            assert!(denied.url != format!("http://localhost:{port}/"));
            assert!(!run(&session, denied, &[])["error"].is_null());
        }
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn redirects_are_returned_and_size_limit_is_enforced() {
        let (port, stop) = serve(|line, _| {
            if line.contains("/redirect ") {
                (
                    302,
                    vec![("Location".into(), "http://example.test/".into())],
                    vec![],
                )
            } else {
                (200, vec![], vec![b'x'; 32])
            }
        });
        let broker = broker(port, 8, 1);
        let session = broker.session(Instant::now() + Duration::from_secs(5));
        let redirect = run(&session, request(port, "GET", "/redirect", &[]), &[]);
        assert_eq!(redirect["response"]["status"], 302);
        assert!(redirect["error"].is_null());
        let (headers, body) = read_response(&session, &redirect);
        assert!(body.is_empty());
        assert!(headers.iter().any(|header| header.name == "location"));
        let mut escaped = request(port, "GET", "/", &[]);
        escaped.url = "http://example.test/".into();
        assert_eq!(
            run(&session, escaped, &[])["error"]["code"],
            "policy_denied"
        );
        let large = run(&session, request(port, "GET", "/large", &[]), &[]);
        assert_eq!(large["error"]["code"], "response_too_large");
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn malformed_cancellation_overload_and_chunk_bounds() {
        let (port, stop) = serve(|_, _| {
            thread::sleep(Duration::from_millis(150));
            (200, vec![], vec![])
        });
        let broker = broker(port, 1024, 1);
        let session = broker.session(Instant::now() + Duration::from_secs(5));
        let malformed: serde_json::Value =
            serde_json::from_str(&session.start_json("{").unwrap()).unwrap();
        assert_eq!(malformed["error"]["code"], "invalid_request");

        let body_request = request(port, "POST", "/", &[0; 1]);
        let started: serde_json::Value = serde_json::from_str(
            &session
                .start_json(&serde_json::to_string(&body_request).unwrap())
                .unwrap(),
        )
        .unwrap();
        let operation_id = started["operation_id"].as_u64().unwrap();
        assert_eq!(
            session.write(operation_id, vec![0; FETCH_CHUNK_BYTES + 1]),
            -crate::errno::EFBIG
        );
        assert_eq!(session.cancel(operation_id), 0);
        assert_eq!(session.cancel(operation_id), 0);

        let first = request(port, "GET", "/", &[]);
        let first: serde_json::Value = serde_json::from_str(
            &session
                .start_json(&serde_json::to_string(&first).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert!(first["error"].is_null());
        let second = request(port, "GET", "/", &[]);
        let second: serde_json::Value = serde_json::from_str(
            &session
                .start_json(&serde_json::to_string(&second).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second["error"]["code"], "overloaded");
        assert_eq!(session.cancel(first["operation_id"].as_u64().unwrap()), 0);
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn timeout_running_cancellation_limits_and_session_isolation() {
        let (port, stop) = serve(|_, _| {
            thread::sleep(Duration::from_millis(150));
            (200, vec![], b"late".to_vec())
        });
        let broker = broker(port, 1024, 2);

        let timeout_session = broker.session(Instant::now() + Duration::from_millis(20));
        let timeout = run(&timeout_session, request(port, "GET", "/", &[]), &[]);
        assert_eq!(timeout["error"]["code"], "timeout");

        let cancelled_session = broker.session(Instant::now() + Duration::from_secs(2));
        let metadata = request(port, "GET", "/", &[]);
        let started: serde_json::Value = serde_json::from_str(
            &cancelled_session
                .start_json(&serde_json::to_string(&metadata).unwrap())
                .unwrap(),
        )
        .unwrap();
        let operation_id = started["operation_id"].as_u64().unwrap();
        assert_eq!(cancelled_session.write(operation_id, b"[]".to_vec()), 2);
        assert_eq!(cancelled_session.finish(operation_id), 0);
        thread::sleep(Duration::from_millis(10));
        cancelled_session.cancel_all();
        assert_eq!(cancelled_session.cancel(operation_id), 0);

        let isolated_one = broker.session(Instant::now() + Duration::from_secs(1));
        let isolated_two = broker.session(Instant::now() + Duration::from_secs(1));
        let first: serde_json::Value = serde_json::from_str(
            &isolated_one
                .start_json(&serde_json::to_string(&request(port, "GET", "/", &[])).unwrap())
                .unwrap(),
        )
        .unwrap();
        let second: serde_json::Value = serde_json::from_str(
            &isolated_two
                .start_json(&serde_json::to_string(&request(port, "GET", "/", &[])).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first["operation_id"], 1);
        assert_eq!(second["operation_id"], 1);
        assert_eq!(isolated_one.cancel(1), 0);
        assert_eq!(isolated_two.cancel(1), 0);

        let limit_session = broker.session(Instant::now() + Duration::from_secs(1));
        let mut oversized = request(port, "POST", "/", &[]);
        oversized.body_length = FetchLimits::default().max_request_bytes as u64 + 1;
        let oversized: serde_json::Value = serde_json::from_str(
            &limit_session
                .start_json(&serde_json::to_string(&oversized).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(oversized["error"]["code"], "response_too_large");
        let headers: Vec<_> = (0..129)
            .map(|index| FetchHeader {
                name: format!("x-{index}"),
                value: "v".into(),
            })
            .collect();
        let header_block = serde_json::to_vec(&headers).unwrap();
        let mut too_many_headers = request(port, "GET", "/", &[]);
        too_many_headers.header_block_length = header_block.len() as u64;
        let too_many_headers =
            run_with_headers(&limit_session, too_many_headers, &header_block, &[]);
        assert_eq!(too_many_headers["error"]["code"], "response_too_large");
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn dns_rebinding_fails_if_any_answer_is_unauthorized() {
        let (port, stop) = serve(|_, _| (200, vec![], vec![]));
        let mut broker = broker(port, 1024, 1);
        Arc::get_mut(&mut broker.inner).unwrap().resolver = Arc::new(StaticResolver {
            addresses: Mutex::new(vec![
                SocketAddr::from(([127, 0, 0, 1], port)),
                SocketAddr::from(([10, 0, 0, 1], port)),
            ]),
        });
        let session = broker.session(Instant::now() + Duration::from_secs(2));
        let response = run(&session, request(port, "GET", "/", &[]), &[]);
        assert_eq!(response["error"]["code"], "policy_denied");
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn poll_json_has_exact_v1_shapes() {
        let (port, stop) = serve(|_, _| (200, vec![], b"ok".to_vec()));
        let broker = broker(port, 1024, 2);
        let session = broker.session(Instant::now() + Duration::from_secs(2));
        let metadata = request(port, "GET", "/", &[]);
        let started: serde_json::Value = serde_json::from_str(
            &session
                .start_json(&serde_json::to_string(&metadata).unwrap())
                .unwrap(),
        )
        .unwrap();
        let operation_id = started["operation_id"].as_u64().unwrap();
        let receiving: serde_json::Value =
            serde_json::from_str(&session.poll_json(operation_id).unwrap()).unwrap();
        assert_eq!(
            receiving,
            serde_json::json!({
                "protocol_version": 1,
                "operation_id": operation_id,
                "state": "receiving",
                "response": null,
                "error": null
            })
        );

        assert_eq!(session.write(operation_id, b"[]".to_vec()), 2);
        assert_eq!(session.finish(operation_id), 0);
        let complete = loop {
            let poll: serde_json::Value =
                serde_json::from_str(&session.poll_json(operation_id).unwrap()).unwrap();
            if poll["state"] == "complete" {
                break poll;
            }
            assert_eq!(
                poll,
                serde_json::json!({
                    "protocol_version": 1,
                    "operation_id": operation_id,
                    "state": "pending",
                    "response": null,
                    "error": null
                })
            );
            thread::sleep(Duration::from_millis(2));
        };
        assert_eq!(complete["protocol_version"], 1);
        assert_eq!(complete["operation_id"], operation_id);
        assert_eq!(complete["state"], "complete");
        assert!(complete["error"].is_null());
        let response = complete["response"].as_object().unwrap();
        assert_eq!(
            response.keys().map(String::as_str).collect::<HashSet<_>>(),
            HashSet::from([
                "protocol_version",
                "request_id",
                "status",
                "header_block_length",
                "body_length",
            ])
        );
        read_response(&session, &complete);

        let cancelled: serde_json::Value = serde_json::from_str(
            &session
                .start_json(&serde_json::to_string(&request(port, "GET", "/", &[])).unwrap())
                .unwrap(),
        )
        .unwrap();
        let cancelled_id = cancelled["operation_id"].as_u64().unwrap();
        assert_eq!(session.cancel(cancelled_id), 0);
        let cancelled: serde_json::Value =
            serde_json::from_str(&session.poll_json(cancelled_id).unwrap()).unwrap();
        assert_eq!(
            cancelled,
            serde_json::json!({
                "protocol_version": 1,
                "operation_id": cancelled_id,
                "state": "complete",
                "response": null,
                "error": {
                    "code": "cancelled",
                    "message": "outbound request was cancelled"
                }
            })
        );
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn v2_streams_get_post_with_negotiated_chunks_and_progressive_reads() {
        let response_body = vec![b'x'; 256];
        let expected = response_body.clone();
        let (port, stop) = serve(move |line, body| {
            if line.starts_with("POST ") {
                (200, vec![("X-Mode".into(), "post".into())], body)
            } else {
                (
                    200,
                    vec![("X-Mode".into(), "get".into())],
                    response_body.clone(),
                )
            }
        });
        let mut broker = broker(port, 8, 2);
        Arc::get_mut(&mut broker.inner)
            .unwrap()
            .config
            .limits
            .max_write_chunk = 11;
        Arc::get_mut(&mut broker.inner)
            .unwrap()
            .config
            .limits
            .max_read_chunk = 7;
        let session = broker.session(Instant::now() + Duration::from_secs(5));

        let (get, get_headers, get_body) = v2_run(&session, port, "GET", &[], 11, 7);
        assert_eq!(get["response"]["status"], 200);
        assert_eq!(get_body, expected);
        assert!(
            get_headers
                .iter()
                .any(|header| header.name == "x-mode" && header.value == "get")
        );

        let post_body = vec![b'p'; 128];
        let (post, post_headers, echoed) = v2_run(&session, port, "POST", &post_body, 11, 7);
        assert_eq!(post["response"]["status"], 200);
        assert_eq!(echoed, post_body);
        assert!(
            post_headers
                .iter()
                .any(|header| header.name == "x-mode" && header.value == "post")
        );
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn v2_rejects_malformed_envelopes_and_enforces_backpressure() {
        let (port, stop) = serve(|_, _| (200, vec![], vec![]));
        let broker = broker(port, 1024, 1);
        let session = broker.session(Instant::now() + Duration::from_secs(5));
        let malformed: serde_json::Value =
            serde_json::from_str(&session.v2_start_json("{").unwrap()).unwrap();
        assert_eq!(malformed["error"]["code"], "invalid_request");
        assert_eq!(malformed["operation_id"], 0);
        assert_eq!(malformed["max_write_chunk"], 0);
        assert_eq!(malformed["max_read_chunk"], 0);
        let invalid_preference = serde_json::json!({
            "protocol_version": 2,
            "request_id": "bad",
            "method": "GET",
            "url": format!("http://localhost:{port}/"),
            "header_block_length": 2,
            "body_length": 0,
            "preferred_write_chunk": FETCH_ABI_SAFE_CHUNK + 1,
            "preferred_read_chunk": 1
        });
        let invalid_preference: serde_json::Value = serde_json::from_str(
            &session
                .v2_start_json(&invalid_preference.to_string())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(invalid_preference["error"]["code"], "invalid_request");

        let (request_tx, mut request_rx) = mpsc::channel(V2_QUEUE_CHUNKS);
        for _ in 0..V2_QUEUE_CHUNKS {
            request_tx.try_send(Ok(vec![1])).unwrap();
        }
        let (_response_tx, response_rx) = mpsc::channel(V2_QUEUE_CHUNKS);
        let task = fetch_runtime().spawn(std::future::pending());
        let admission = match Admission::acquire(broker.inner.clone()) {
            Ok(admission) => admission,
            Err(_) => panic!("test broker admission should be available"),
        };
        session.inner.v2_operations.lock().unwrap().insert(
            1,
            V2Operation {
                admission: Some(admission),
                max_write_chunk: 8,
                max_read_chunk: 8,
                state: V2OperationState::Streaming {
                    request_tx: Some(request_tx),
                    request_bytes: 4,
                    expected_body_length: None,
                    response_rx,
                    pending_response: VecDeque::new(),
                    shared: Arc::new(Mutex::new(V2Shared::default())),
                    task,
                },
            },
        );
        assert_eq!(session.v2_write(1, vec![2]), -crate::errno::EAGAIN);
        let _ = request_rx.try_recv().unwrap();
        assert_eq!(session.v2_write(1, vec![2]), 1);
        assert_eq!(session.v2_cancel(1), 0);
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn v2_cancellation_timeout_redirect_and_fresh_session_isolation() {
        let (port, stop) = serve(|line, _| {
            if line.contains("/redirect ") {
                (
                    302,
                    vec![("Location".into(), "http://example.test/".into())],
                    vec![],
                )
            } else {
                thread::sleep(Duration::from_millis(100));
                (200, vec![], b"late".to_vec())
            }
        });
        let broker = broker(port, 1024, 2);
        let first = broker.session(Instant::now() + Duration::from_secs(2));
        let second = broker.session(Instant::now() + Duration::from_secs(2));
        let (first_id, _, _) = v2_start(&first, port, "GET", Some(0), 16, 16);
        let (second_id, _, _) = v2_start(&second, port, "GET", Some(0), 16, 16);
        assert_eq!(first_id, 1);
        assert_eq!(second_id, 1);
        assert_eq!(first.v2_cancel(first_id), 0);
        assert_eq!(second.v2_cancel(second_id), 0);

        let timeout = broker.session(Instant::now() + Duration::from_millis(10));
        let (operation_id, _, _) = v2_start(&timeout, port, "GET", Some(0), 16, 16);
        assert_eq!(timeout.v2_write(operation_id, b"[]".to_vec()), 2);
        assert_eq!(timeout.v2_finish(operation_id), 0);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let poll: serde_json::Value =
                serde_json::from_str(&timeout.v2_poll_json(operation_id).unwrap()).unwrap();
            if !poll["error"].is_null() {
                assert_eq!(poll["error"]["code"], "timeout");
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        stop.store(true, Ordering::Release);
    }

    #[test]
    fn completed_responses_hold_admission_until_collected() {
        let (port, stop) = serve(|_, _| (200, vec![], b"ok".to_vec()));
        let broker = broker(port, 1024, 1);

        let v1 = broker.session(Instant::now() + Duration::from_secs(2));
        let complete = run(&v1, request(port, "GET", "/", &[]), &[]);
        let blocked: serde_json::Value = serde_json::from_str(
            &v1.start_json(&serde_json::to_string(&request(port, "GET", "/", &[])).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(blocked["error"]["code"], "overloaded");
        read_response(&v1, &complete);
        let admitted: serde_json::Value = serde_json::from_str(
            &v1.start_json(&serde_json::to_string(&request(port, "GET", "/", &[])).unwrap())
                .unwrap(),
        )
        .unwrap();
        assert!(admitted["error"].is_null());
        assert_eq!(v1.cancel(admitted["operation_id"].as_u64().unwrap()), 0);

        let v2 = broker.session(Instant::now() + Duration::from_secs(2));
        let (operation_id, _, max_read_chunk) = v2_start(&v2, port, "GET", Some(0), 16, 16);
        assert_eq!(v2.v2_write(operation_id, b"[]".to_vec()), 2);
        assert_eq!(v2.v2_finish(operation_id), 0);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let poll: serde_json::Value =
                serde_json::from_str(&v2.v2_poll_json(operation_id).unwrap()).unwrap();
            if poll["state"] == "complete" {
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let blocked = v2_start_json_value(&v2, port);
        assert_eq!(blocked["error"]["code"], "overloaded");
        loop {
            let chunk = v2.v2_read(operation_id, max_read_chunk as u64).unwrap();
            if chunk == [2] {
                break;
            }
        }
        let admitted = v2_start_json_value(&v2, port);
        assert!(admitted["error"].is_null());
        assert_eq!(v2.v2_cancel(admitted["operation_id"].as_u64().unwrap()), 0);
        stop.store(true, Ordering::Release);
    }

    fn v2_start_json_value(session: &FetchSession, port: u16) -> serde_json::Value {
        serde_json::from_str(
            &session
                .v2_start_json(
                    &serde_json::json!({
                        "protocol_version": 2,
                        "request_id": "admission",
                        "method": "GET",
                        "url": format!("http://localhost:{port}/"),
                        "header_block_length": 2,
                        "body_length": 0,
                        "preferred_write_chunk": 16,
                        "preferred_read_chunk": 16
                    })
                    .to_string(),
                )
                .unwrap(),
        )
        .unwrap()
    }
}
