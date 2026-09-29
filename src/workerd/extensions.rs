// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use super::{RequestEnvelope, ResponseEnvelope, Result, WorkerVersionId};

/// Host-owned authorization context; never trust guest-supplied tenant identity.
pub struct CapabilityContext {
    pub worker_version: WorkerVersionId,
    pub capability_id: String,
}

/// Interface only: no implementation or guest registration is provided.
/// A broker must authorize the capability, constrain redirects, resolve DNS
/// on the host and reject private, local and metadata destinations.
pub trait OutboundFetchCapability: Send + Sync {
    fn fetch(
        &self,
        context: &CapabilityContext,
        request: RequestEnvelope,
    ) -> Result<ResponseEnvelope>;
}

pub struct D1Request {
    pub transaction_id: String,
    pub statements: Vec<String>,
}

pub struct D1Response {
    pub rows_json: Vec<String>,
    pub rows_affected: u64,
}

/// Interface only; implementations must authorize and bound each transaction.
pub trait D1Capability: Send + Sync {
    fn transact(&self, context: &CapabilityContext, request: D1Request) -> Result<D1Response>;
}
