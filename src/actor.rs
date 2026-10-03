//! Minimal Durable Objects-style actor routing and lifecycle seam.
//!
//! This module is intentionally separate from [`crate::broker`]. Request VMs
//! are disposable execution contexts; actors require stable identity, routing,
//! serialized delivery, durable state ownership, and explicit activation.

use crate::broker::RequestIdentity;
use std::fmt;

/// Stable namespace assigned by the trusted host control plane.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActorNamespace(String);

impl ActorNamespace {
    /// Create a non-empty namespace.
    pub fn new(value: impl Into<String>) -> Result<Self, ActorContractError> {
        let value = value.into();
        validate_actor_component("namespace", &value)?;
        Ok(Self(value))
    }

    /// Namespace value used by routing and storage partitioning.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable actor key within a namespace.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActorKey(String);

impl ActorKey {
    /// Create a non-empty actor key.
    pub fn new(value: impl Into<String>) -> Result<Self, ActorContractError> {
        let value = value.into();
        validate_actor_component("key", &value)?;
        Ok(Self(value))
    }

    /// Actor key within its namespace.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Globally stable actor identity used for routing and storage ownership.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActorId {
    namespace: ActorNamespace,
    key: ActorKey,
}

impl ActorId {
    /// Create an actor identity.
    pub fn new(namespace: ActorNamespace, key: ActorKey) -> Self {
        Self { namespace, key }
    }

    /// Actor namespace.
    pub fn namespace(&self) -> &ActorNamespace {
        &self.namespace
    }

    /// Actor key within the namespace.
    pub fn key(&self) -> &ActorKey {
        &self.key
    }
}

/// Host-selected placement for one actor activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorRoute {
    actor: ActorId,
    placement: String,
    generation: u64,
}

impl ActorRoute {
    /// Create a route whose generation changes whenever ownership moves.
    pub fn new(
        actor: ActorId,
        placement: impl Into<String>,
        generation: u64,
    ) -> Result<Self, ActorContractError> {
        let placement = placement.into();
        validate_actor_component("placement", &placement)?;
        Ok(Self {
            actor,
            placement,
            generation,
        })
    }

    /// Stable actor identity.
    pub fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// Placement generation used to reject stale deliveries.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Host placement identifier.
    pub fn placement(&self) -> &str {
        &self.placement
    }
}

/// Lifecycle event owned by the actor runtime, not a request VM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorLifecycle {
    Activate,
    Deliver,
    Alarm,
    Passivate,
}

/// One serialized actor delivery correlated with request identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorDelivery {
    route: ActorRoute,
    request: RequestIdentity,
    payload_bytes: u64,
}

impl ActorDelivery {
    /// Create a bounded delivery descriptor. Payload bytes are accounted by the
    /// actor runtime before any request VM is entered.
    pub fn new(route: ActorRoute, request: RequestIdentity, payload_bytes: u64) -> Self {
        Self {
            route,
            request,
            payload_bytes,
        }
    }

    /// Route selected for this delivery.
    pub fn route(&self) -> &ActorRoute {
        &self.route
    }

    /// Request identity used for audit and retry correlation.
    pub fn request(&self) -> &RequestIdentity {
        &self.request
    }

    /// Payload bytes charged before actor execution.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
}

/// Minimal routing seam required before actor storage or lifecycle execution.
pub trait ActorRouter {
    /// Resolve current ownership. Implementations must reject stale generations
    /// and serialize delivery per [`ActorId`].
    fn route(&self, actor: &ActorId) -> Result<ActorRoute, ActorRouteError>;
}

/// Actor route lookup failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActorRouteError {
    Unavailable,
    StaleGeneration { expected: u64, actual: u64 },
}

impl fmt::Display for ActorRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "actor route failed: {:?}", self)
    }
}

impl std::error::Error for ActorRouteError {}

/// Invalid actor routing contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorContractError {
    field: &'static str,
}

impl fmt::Display for ActorContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid actor {}", self.field)
    }
}

impl std::error::Error for ActorContractError {}

fn validate_actor_component(field: &'static str, value: &str) -> Result<(), ActorContractError> {
    if value.is_empty() || value.len() > 128 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        Err(ActorContractError { field })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_generation_is_explicit_and_separate_from_request_identity() {
        let actor = ActorId::new(
            ActorNamespace::new("chat").unwrap(),
            ActorKey::new("room-7").unwrap(),
        );
        let route = ActorRoute::new(actor, "cell-a", 3).unwrap();

        assert_eq!(route.generation(), 3);
    }
}
