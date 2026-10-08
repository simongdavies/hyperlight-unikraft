// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Version-scoped Workerd execution on the existing driver ABI.
//!
//! The executor reads named `init` and `fetch` calls from `/dev/hlcall`.
//! It writes exactly one newline-terminated JSON response to stdout, carried
//! by the existing `HostPrint` host call. Output during fetch is protocol-only.
//! Host filesystem, outbound network, timers, SQL/SQLite, webhook and stream
//! capabilities require explicit typed per-app policy. Optional host-owned
//! adapters use the generic host-call bridge and reconstruct host-only state
//! rather than copying live resources into checkpoints. See
//! `examples/workerd-executor/README.md` for the ABI and trust assumptions;
//! this is not stock-workerd or production support.

mod app_registry;
mod bindings;
mod bundle;
mod cancellation;
mod checkpoint;
mod control;
mod extensions;
mod fetch;
mod fetch_scope;
mod home;
mod http;
mod ingress;
mod instance;
mod network;
mod pool;
mod protocol;
pub(crate) mod provider_websocket;
mod resident;
mod resident_pool;
mod sandbox;
mod secret;
mod snapshot;
mod timer;
mod transport;
mod webhook;

pub use app_registry::{
    AppConfig, AppHandle, AppIdentity, AppPoolConfig, AppRegistry, AppRegistryError, AppRoute,
    ConnectionAffinity, DisposablePoolConfig, FetchPolicyConfig, HostConfig,
    ResidentPoolConfigJson, StorageBindingConfig, StorageMode, WorkerCapabilityPolicyConfig,
};
pub use bindings::{LogicalBindingConfig, LogicalBindingsPolicy, LogicalBudgetScope};
pub use cancellation::InvocationCancellation;
pub use checkpoint::{
    CheckpointClaim, CheckpointPolicy, CheckpointRecord, CheckpointStore, InstanceIdentity,
};
pub use control::{
    ControlRequest, FrameKind, GuestCapabilities, InstanceInvocation, LifecycleRequest,
    MAX_FRAME_BYTES, SafePoint, StreamFrame, StreamState,
};
pub use extensions::*;
pub use fetch::{
    FetchBroker, FetchBrokerConfig, FetchCredential, FetchErrorCode, FetchLimits, FetchPolicy,
    FetchRequest, FetchResponse,
};
pub use fetch_scope::{AzureChatPolicy, FetchBodyPolicy};
pub use home::{InstanceHome, InstanceHomeConfig, LifecycleOperation};
pub use http::{
    ConnectionMode, ParsedHttpRequest, http_reason, read_http_request,
    read_http_request_with_control, request_path, wants_keep_alive, write_http_error,
    write_http_response,
};
pub use ingress::{GuestIngress, HostIngress};
pub use instance::{InstanceState, InstanceStatus, ResidentInstance};
pub use network::{RawEgressRule, RawNetworkPolicyConfig};
pub use pool::{
    InvocationExecution, PoolSubmitError, PrewarmPolicy, RequestExecution, WorkerPoolRestoreMode,
    WorkerPoolStatus, WorkerRequestPool,
};
pub use protocol::*;
pub use provider_websocket::{
    ProviderWebSocketConfig, ProviderWebSocketLimits, ProviderWebSocketSessionPolicy,
};
pub use resident::{ResidentPolicy, ResidentWorkerSandbox};
pub use resident_pool::{
    ResidentHandle, ResidentPoolConfig, ResidentPoolStatus, ResidentWorkerPool,
};
pub use sandbox::{
    ExecutionProfile, InitializationFailure, InitializationProfile, StorageBinding, StoragePolicy,
    WorkerCapabilityPolicy, WorkerVersionSandbox,
};
pub use snapshot::{SnapshotBinding, VerifiedSnapshot};
pub use timer::{TIMER_PROTOCOL_VERSION, TimerLimits};
pub use transport::{decode_buffered_input, pump_http_stream, websocket_requested};
pub use webhook::WebhookPolicy;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Protocol(String),
    #[error("{0}")]
    Snapshot(String),
    #[error("{0}")]
    State(String),
    #[error("Worker request timed out")]
    Timeout,
    #[error("Worker invocation cancelled")]
    Cancelled,
    #[error("stale instance fence: {0}")]
    Fence(String),
    #[error("instance not ready: {0}")]
    NotReady(String),
    #[error(transparent)]
    Guest(#[from] crate::Error),
    #[error(transparent)]
    Hyperlight(#[from] hyperlight_host::HyperlightError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
