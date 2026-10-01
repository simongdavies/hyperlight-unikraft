// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Experimental, one-Worker-version executor on the v0.14 driver ABI.
//!
//! The executor reads named `init` and `fetch` calls from `/dev/hlcall`.
//! It writes exactly one newline-terminated JSON response to stdout, carried
//! by the existing `HostPrint` host call. Output during fetch is protocol-only.
//! No host filesystem or raw socket capabilities are granted. Optional
//! host-owned outbound fetch and monotonic timer adapters use the generic
//! host-call bridge. D1 remains an interface only. See
//! `examples/workerd-executor/README.md` for the ABI and trust assumptions;
//! this is not stock-workerd or production support.

mod extensions;
mod fetch;
mod pool;
mod protocol;
mod sandbox;
mod snapshot;
mod timer;

pub use extensions::*;
pub use fetch::{
    FetchBroker, FetchBrokerConfig, FetchErrorCode, FetchLimits, FetchPolicy, FetchRequest,
    FetchResponse,
};
pub use pool::{
    PoolSubmitError, RequestExecution, WorkerPoolRestoreMode, WorkerPoolStatus, WorkerRequestPool,
};
pub use protocol::*;
pub use sandbox::{
    ExecutionProfile, InitializationFailure, InitializationProfile, WorkerVersionSandbox,
};
pub use snapshot::{SnapshotBinding, VerifiedSnapshot};
pub use timer::{TIMER_PROTOCOL_VERSION, TimerLimits};

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
