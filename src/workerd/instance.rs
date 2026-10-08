// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Owner-thread lifecycle for one logical resident instance. The caller owns
//! admission/capacity and drives idle_tick while no invocation is active.
//! This is not an additional pool, network authorization layer or scheduler.

use super::{
    CheckpointPolicy, CheckpointStore, Error, ExecutionProfile, InstanceIdentity,
    InvocationCancellation, InvocationRequest, InvocationResponse, ResidentWorkerSandbox, Result,
    VerifiedSnapshot, WorkerVersionSandbox,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceState {
    Active,
    Draining,
    Checkpointing,
    Parked,
    Resuming,
    Failed,
    Released,
}

#[derive(Clone, Debug, Serialize)]
pub struct InstanceStatus {
    pub identity: InstanceIdentity,
    pub state: InstanceState,
    pub live_vms: usize,
    pub parked_instances: usize,
    pub resuming_instances: usize,
    pub requests_served: u64,
    pub checkpoint_policy: CheckpointPolicy,
    pub checkpoint_id: Option<String>,
    pub owner_loaded: bool,
    pub ownership_reconciled: bool,
}

pub struct ResidentInstance {
    worker: WorkerVersionSandbox,
    identity: InstanceIdentity,
    state: InstanceState,
    resident: Option<ResidentWorkerSandbox>,
    checkpoint: Option<VerifiedSnapshot>,
    store: Option<Arc<CheckpointStore>>,
    policy: CheckpointPolicy,
    idle_timeout: Option<Duration>,
    last_used: Instant,
    requests_served: u64,
    checkpoint_id: Option<String>,
}

impl ResidentInstance {
    pub fn create(
        worker: WorkerVersionSandbox,
        policy: CheckpointPolicy,
        store: Option<Arc<CheckpointStore>>,
        idle_timeout: Option<Duration>,
    ) -> Result<Self> {
        Self::create_named(
            worker,
            policy,
            store,
            idle_timeout,
            InstanceIdentity::new()?,
        )
    }

    pub fn create_named(
        worker: WorkerVersionSandbox,
        policy: CheckpointPolicy,
        store: Option<Arc<CheckpointStore>>,
        idle_timeout: Option<Duration>,
        identity: InstanceIdentity,
    ) -> Result<Self> {
        validate_policy(policy, store.as_ref(), idle_timeout)?;
        worker.checkpoint_binding()?;
        if identity.generation != 1 {
            return Err(Error::State("new instance requires generation one".into()));
        }
        let mut resident = worker.restore_resident()?;
        resident.set_identity(identity.clone());
        if let Some(store) = &store {
            store.register(&identity, worker.snapshot().binding())?;
        }
        Ok(Self {
            worker,
            identity,
            state: InstanceState::Active,
            resident: Some(resident),
            checkpoint: None,
            store,
            policy,
            idle_timeout,
            last_used: Instant::now(),
            requests_served: 0,
            checkpoint_id: None,
        })
    }

    /// Recover an already parked instance from persistent storage. Artifact,
    /// policy and generation mismatches never fall back to a new VM.
    pub fn recover(
        worker: WorkerVersionSandbox,
        store: Arc<CheckpointStore>,
        parked: InstanceIdentity,
        idle_timeout: Option<Duration>,
    ) -> Result<Self> {
        validate_policy(CheckpointPolicy::Durable, Some(&store), idle_timeout)?;
        worker.checkpoint_binding()?;
        let claim = store.claim(&parked, worker.snapshot().binding())?;
        let identity = claim.identity.clone();
        let checkpoint_id = Some(claim.checkpoint_id.clone());
        let resident = worker.resume_checkpoint_claim(&store, claim)?;
        let requests_served = resident.requests_served();
        Ok(Self {
            worker,
            identity,
            state: InstanceState::Active,
            resident: Some(resident),
            checkpoint: None,
            store: Some(store),
            policy: CheckpointPolicy::Durable,
            idle_timeout,
            last_used: Instant::now(),
            requests_served,
            checkpoint_id,
        })
    }

    pub fn identity(&self) -> &InstanceIdentity {
        &self.identity
    }

    pub fn status(&self) -> InstanceStatus {
        InstanceStatus {
            identity: self.identity.clone(),
            state: self.state,
            live_vms: usize::from(
                self.resident
                    .as_ref()
                    .is_some_and(ResidentWorkerSandbox::is_alive),
            ),
            parked_instances: usize::from(self.state == InstanceState::Parked),
            resuming_instances: usize::from(self.state == InstanceState::Resuming),
            requests_served: self.requests_served,
            checkpoint_policy: self.policy,
            checkpoint_id: self.checkpoint_id.clone(),
            owner_loaded: true,
            ownership_reconciled: true,
        }
    }

    fn fence(&self, expected_generation: u64) -> Result<()> {
        if expected_generation != self.identity.generation {
            return Err(Error::Fence("logical instance generation differs".into()));
        }
        Ok(())
    }

    pub fn invoke(
        &mut self,
        expected_generation: u64,
        request: InvocationRequest,
        timeout: Duration,
        cancellation: InvocationCancellation,
    ) -> (Result<InvocationResponse>, ExecutionProfile) {
        let admission = self.fence(expected_generation).and_then(|()| {
            if self.state != InstanceState::Active {
                return Err(Error::NotReady("instance is not active".into()));
            }
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            if cancellation.is_cancelled() {
                return Err(Error::Cancelled);
            }
            Ok(())
        });
        if let Err(error) = admission {
            return (Err(error), ExecutionProfile::default());
        }
        let Some(resident) = &mut self.resident else {
            return (
                Err(Error::NotReady("active instance has no live VM".into())),
                ExecutionProfile::default(),
            );
        };
        let result = resident.execute_cancellable(request, timeout, cancellation);
        self.requests_served += 1;
        self.last_used = Instant::now();
        if result.0.is_err() {
            self.resident = None;
            self.state = InstanceState::Failed;
        }
        result
    }

    pub fn park(&mut self, expected_generation: u64, timeout: Duration) -> Result<()> {
        self.fence(expected_generation)?;
        if self.state != InstanceState::Active {
            return Err(Error::State("only an active instance can park".into()));
        }
        // &mut self and the single owner thread close admission for the entire
        // drain/capture/commit/termination sequence.
        self.state = InstanceState::Draining;
        let checkpoint_id = format!("park:{}", self.identity.generation);
        self.state = InstanceState::Checkpointing;
        let checkpoint = match self
            .resident
            .as_mut()
            .ok_or_else(|| Error::State("instance VM missing".into()))?
            .checkpoint(&self.worker, &checkpoint_id, timeout)
        {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                if let Some(resident) = self.resident.take() {
                    resident.retire();
                }
                self.state = InstanceState::Failed;
                return Err(error);
            }
        };
        let committed = if let Some(store) = &self.store {
            match store.commit(&self.identity, &checkpoint) {
                Ok(id) => Some(id),
                Err(error) => {
                    // Persistence failed before live teardown: keep this exact
                    // changed instance, rather than reporting parked success.
                    self.state = InstanceState::Active;
                    self.last_used = Instant::now();
                    return Err(error);
                }
            }
        } else {
            None
        };
        self.checkpoint = Some(checkpoint);
        self.checkpoint_id = committed.clone();
        if let Some(resident) = self.resident.take() {
            resident.retire();
        }
        if let (Some(store), Some(committed)) = (&self.store, committed)
            && let Err(error) = store.publish_parked(&self.identity, &committed)
        {
            self.state = InstanceState::Failed;
            return Err(error);
        }
        self.state = InstanceState::Parked;
        Ok(())
    }

    pub fn resume(&mut self, expected_generation: u64) -> Result<()> {
        self.fence(expected_generation)?;
        if self.state != InstanceState::Parked {
            return Err(Error::State("only a parked instance can resume".into()));
        }
        self.state = InstanceState::Resuming;
        let result = if let Some(store) = &self.store {
            store
                .claim(&self.identity, self.worker.snapshot().binding())
                .and_then(|claim| {
                    let identity = claim.identity.clone();
                    self.worker
                        .resume_checkpoint_claim(store, claim)
                        .map(|resident| (identity, resident))
                })
        } else {
            let generation = self
                .identity
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::State("instance generation exhausted".into()));
            generation.and_then(|generation| {
                let checkpoint = self
                    .checkpoint
                    .as_ref()
                    .ok_or_else(|| Error::Snapshot("local instance checkpoint missing".into()))?;
                self.worker
                    .restore_resident_checkpoint(checkpoint)
                    .map(|resident| {
                        (
                            InstanceIdentity {
                                instance_id: self.identity.instance_id.clone(),
                                generation,
                            },
                            resident,
                        )
                    })
            })
        };
        match result {
            Ok((identity, mut resident)) => {
                resident.set_identity(identity.clone());
                self.identity = identity;
                self.resident = Some(resident);
                self.checkpoint = None;
                self.state = InstanceState::Active;
                self.last_used = Instant::now();
                Ok(())
            }
            Err(error) => {
                self.state = InstanceState::Failed;
                Err(error)
            }
        }
    }

    pub fn idle_tick(&mut self, checkpoint_timeout: Duration) -> Result<bool> {
        if self.state == InstanceState::Active
            && self
                .idle_timeout
                .is_some_and(|timeout| self.last_used.elapsed() >= timeout)
        {
            self.park(self.identity.generation, checkpoint_timeout)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn release(&mut self, expected_generation: u64) -> Result<()> {
        self.fence(expected_generation)?;
        if matches!(
            self.state,
            InstanceState::Resuming
                | InstanceState::Checkpointing
                | InstanceState::Draining
                | InstanceState::Released
        ) {
            return Err(Error::State(
                "instance cannot release in this lifecycle state".into(),
            ));
        }
        if let Some(resident) = self.resident.take() {
            resident.retire();
        }
        if let Some(store) = &self.store {
            store.release(&self.identity)?;
        }
        self.checkpoint = None;
        self.state = InstanceState::Released;
        Ok(())
    }

    pub fn invoke_stream(
        &mut self,
        expected_generation: u64,
        request: super::RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
    ) -> Result<()> {
        self.fence(expected_generation)?;
        if self.state != InstanceState::Active {
            return Err(Error::NotReady("instance is not active".into()));
        }
        let result = self
            .resident
            .as_mut()
            .ok_or_else(|| Error::State("active instance VM missing".into()))?
            .execute_stream(request, websocket, ingress, timeout);
        self.requests_served += 1;
        self.last_used = Instant::now();
        if result.is_err() {
            self.resident = None;
            self.state = InstanceState::Failed;
        }
        result
    }
}

fn validate_policy(
    policy: CheckpointPolicy,
    store: Option<&Arc<CheckpointStore>>,
    idle: Option<Duration>,
) -> Result<()> {
    if policy == CheckpointPolicy::Durable && store.is_none() {
        return Err(Error::Snapshot(
            "durable checkpoint policy requires persistent encrypted storage".into(),
        ));
    }
    if policy == CheckpointPolicy::Local && store.is_some() {
        return Err(Error::Snapshot(
            "local checkpoint policy cannot silently use a durable backend".into(),
        ));
    }
    if idle.is_some_and(|timeout| timeout.is_zero()) {
        return Err(Error::State("idle timeout must be positive".into()));
    }
    Ok(())
}
