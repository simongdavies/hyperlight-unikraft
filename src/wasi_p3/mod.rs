// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Locked WASI Preview 3 v0.3.1 asynchronous adapter lane.
//!
//! The lane is intentionally separate from the P2, storage, network and
//! ingress implementations. It provides canonical asynchronous primitives
//! and policy-facing adapter contracts, but does not claim generated binding
//! conformance until the locked WIT source artifacts and preview3-shim digest
//! are available.

pub mod adapters;
pub mod async_core;

pub use adapters::{
    AdapterFuture, CliAdapter, CliGrant, ClocksAdapter, FilesystemAdapter, FixedTimezone,
    HttpAdapter, ImportDisposition, P3AdapterError, P3Adapters, P3ErrorCategory, P3Policy,
    RandomAdapter, SocketsAdapter, WASI_P3_RELEASE_COMMIT, WASI_P3_VERSION,
};
pub use async_core::{
    DeterministicExecutor, ExecutorStep, FutureCancelled, FutureReader, FutureWriter, NextFuture,
    SendFuture, StreamClosed, StreamReader, StreamWriter, TaskId, TaskState, future, stream,
};
