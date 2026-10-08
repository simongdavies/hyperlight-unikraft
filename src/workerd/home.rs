// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! One configured revision's bounded owner-thread instance home. There is no
//! hidden resident pool behind a logical instance: one instance is one VM.
use super::{
    CheckpointPolicy, CheckpointStore, Error, InstanceIdentity, InstanceState, InstanceStatus,
    InvocationCancellation, InvocationExecution, InvocationRequest, ResidentInstance, Result,
    WorkerVersionSandbox,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceHomeConfig {
    #[serde(default)]
    pub checkpoint_policy: CheckpointPolicy,
    pub database_path: Option<PathBuf>,
    pub encryption_key_path: Option<PathBuf>,
    pub max_checkpoint_bytes: u64,
    pub idle_timeout_secs: Option<u64>,
    pub checkpoint_timeout_secs: u64,
    pub scratch_directory: Option<PathBuf>,
}

impl InstanceHomeConfig {
    fn validate(&self) -> Result<()> {
        if self.max_checkpoint_bytes == 0
            || self.checkpoint_timeout_secs == 0
            || self.idle_timeout_secs == Some(0)
        {
            return Err(Error::State(
                "instance home byte/deadline limits must be positive".into(),
            ));
        }
        match self.checkpoint_policy {
            CheckpointPolicy::Durable if self.database_path.is_none() || self.encryption_key_path.is_none() =>
                Err(Error::Snapshot("durable instance home requires persistent database and external encryption key paths".into())),
            CheckpointPolicy::Local if self.database_path.is_some() || self.encryption_key_path.is_some() =>
                Err(Error::Snapshot("local checkpoint policy cannot accept durable paths".into())),
            _ => Ok(()),
        }
    }
}

type Completion = Box<dyn FnOnce(InvocationExecution) + Send>;

enum Command {
    Stream {
        generation: u64,
        request: super::RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
        admitted_at: Instant,
        completion: Completion,
    },
    Invoke {
        generation: u64,
        request: InvocationRequest,
        timeout: Duration,
        admitted_at: Instant,
        cancellation: InvocationCancellation,
        completion: Completion,
    },
    Lifecycle {
        operation: LifecycleOperation,
        generation: u64,
        reply: mpsc::Sender<Result<InstanceStatus>>,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug)]
pub enum LifecycleOperation {
    Park,
    Resume,
    Release,
}

struct Slot {
    sender: mpsc::SyncSender<Command>,
    owner: Option<JoinHandle<()>>,
    status: Arc<Mutex<Option<InstanceStatus>>>,
}

struct LivePermit(Arc<AtomicUsize>);
impl LivePermit {
    fn acquire(counter: &Arc<AtomicUsize>, capacity: usize) -> Result<Self> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < capacity).then_some(live + 1)
            })
            .map_err(|_| Error::State("instance live VM capacity exhausted".into()))?;
        Ok(Self(counter.clone()))
    }
}
impl Drop for LivePermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct InstanceHome {
    worker: WorkerVersionSandbox,
    config: InstanceHomeConfig,
    store: Option<Arc<CheckpointStore>>,
    capacity: usize,
    queue_capacity: usize,
    slots: Mutex<HashMap<String, Slot>>,
    live_allocations: Arc<AtomicUsize>,
}

impl InstanceHome {
    pub fn new(
        worker: WorkerVersionSandbox,
        config: InstanceHomeConfig,
        capacity: usize,
        queue_capacity: usize,
    ) -> Result<Self> {
        config.validate()?;
        worker.checkpoint_binding()?;
        if capacity == 0 || queue_capacity == 0 {
            return Err(Error::State(
                "instance home capacity and queue must be positive".into(),
            ));
        }
        let store = if let (Some(path), Some(key_path)) =
            (&config.database_path, &config.encryption_key_path)
        {
            let metadata = std::fs::symlink_metadata(key_path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(Error::Snapshot(
                    "checkpoint encryption key must be a regular secret file".into(),
                ));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(Error::Snapshot(
                        "checkpoint secret permissions must exclude group/other".into(),
                    ));
                }
            }
            let mut bytes = Vec::new();
            std::fs::File::open(key_path)?
                .take(33)
                .read_to_end(&mut bytes)?;
            let key: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                Error::Snapshot("checkpoint encryption key must be exactly32rawbytes".into())
            })?;
            Some(Arc::new(CheckpointStore::open_with_scratch(
                path,
                &key,
                config.max_checkpoint_bytes,
                config.scratch_directory.as_deref(),
            )?))
        } else {
            None
        };
        capacity
            .checked_add(queue_capacity)
            .ok_or_else(|| Error::State("instance capacity overflow".into()))?;
        Ok(Self {
            worker,
            config,
            store,
            capacity,
            queue_capacity,
            slots: Mutex::new(HashMap::new()),
            live_allocations: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn policy(&self) -> CheckpointPolicy {
        self.config.checkpoint_policy
    }

    pub fn contains(&self, instance_id: &str) -> Result<bool> {
        Ok(self
            .slots
            .lock()
            .map_err(|_| Error::State("instance slots poisoned".into()))?
            .contains_key(instance_id))
    }

    pub fn create(&self, instance_id: &str, expected_generation: u64) -> Result<InstanceStatus> {
        if expected_generation != 0 {
            return Err(Error::State("create requires generation zero".into()));
        }
        super::ControlRequest::new(instance_id)?;
        self.start(instance_id, None)
    }

    /// Explicit recovery consumes exactly the persisted parked generation.
    pub fn recover(&self, identity: InstanceIdentity) -> Result<InstanceStatus> {
        if self.store.is_none() {
            return Err(Error::Snapshot(
                "local instance cannot recover after home replacement".into(),
            ));
        }
        self.start(&identity.instance_id.clone(), Some(identity))
    }

    fn start(&self, instance_id: &str, parked: Option<InstanceIdentity>) -> Result<InstanceStatus> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| Error::State("instance slots poisoned".into()))?;
        if slots.contains_key(instance_id) {
            return Err(Error::State("logical instance already owned".into()));
        }
        if slots.len() >= self.capacity + self.queue_capacity {
            return Err(Error::State(
                "instance logical retention capacity exhausted".into(),
            ));
        }
        let permit = LivePermit::acquire(&self.live_allocations, self.capacity)?;
        let live_allocations = self.live_allocations.clone();
        let capacity = self.capacity;
        let worker = self.worker.clone();
        let config = self.config.clone();
        let store = self.store.clone();
        let instance_id = instance_id.to_string();
        let (sender, receiver) = mpsc::sync_channel(self.queue_capacity);
        let (initialized_tx, initialized_rx) = mpsc::channel();
        let shared_status = Arc::new(Mutex::new(None));
        let owner_status = shared_status.clone();
        let owner = thread::Builder::new()
            .name(format!("instance-{instance_id}"))
            .spawn(move || {
                let mut live_permit = Some(permit);
                let idle = config.idle_timeout_secs.map(Duration::from_secs);
                let result = if let Some(parked) = parked {
                    ResidentInstance::recover(
                        worker,
                        store.clone().expect("recover requires store"),
                        parked,
                        idle,
                    )
                } else {
                    ResidentInstance::create_named(
                        worker,
                        config.checkpoint_policy,
                        store,
                        idle,
                        InstanceIdentity {
                            instance_id,
                            generation: 1,
                        },
                    )
                };
                let mut instance = match result {
                    Ok(instance) => instance,
                    Err(error) => {
                        if initialized_tx.send(Err(error)).is_err() {
                            tracing::error!("instance initialization receiver dropped");
                        }
                        return;
                    }
                };
                *owner_status.lock().expect("instance status poisoned") = Some(instance.status());
                if initialized_tx.send(Ok(instance.status())).is_err() {
                    tracing::error!("instance initialization receiver dropped after VM creation");
                    return;
                }
                let checkpoint_timeout = Duration::from_secs(config.checkpoint_timeout_secs);
                loop {
                    match receiver.recv_timeout(Duration::from_millis(50)) {
                        Ok(Command::Stream {
                            generation,
                            request,
                            websocket,
                            ingress,
                            timeout,
                            admitted_at,
                            completion,
                        }) => {
                            let request_id = request.request_id.clone();
                            let result = instance.invoke_stream(
                                generation,
                                request,
                                websocket,
                                ingress,
                                timeout.saturating_sub(admitted_at.elapsed()),
                            );
                            if instance.status().live_vms == 0 {
                                live_permit = None;
                            }
                            *owner_status.lock().expect("instance status poisoned") =
                                Some(instance.status());
                            completion(InvocationExecution {
                                request_id,
                                result: result.map(|()| super::InvocationResponse::StreamComplete),
                                profile: Default::default(),
                                submit_error: None,
                            });
                        }
                        Ok(Command::Invoke {
                            generation,
                            request,
                            timeout,
                            admitted_at,
                            cancellation,
                            completion,
                        }) => {
                            let id = request.request_id().to_string();
                            let remaining = timeout.saturating_sub(admitted_at.elapsed());
                            let (result, profile) =
                                instance.invoke(generation, request, remaining, cancellation);
                            if instance.status().live_vms == 0 {
                                live_permit = None;
                            }
                            *owner_status.lock().expect("instance status poisoned") =
                                Some(instance.status());
                            completion(InvocationExecution {
                                request_id: id,
                                result,
                                profile,
                                submit_error: None,
                            });
                        }
                        Ok(Command::Lifecycle {
                            operation,
                            generation,
                            reply,
                        }) => {
                            let admission = if matches!(operation, LifecycleOperation::Resume)
                                && live_permit.is_none()
                            {
                                match LivePermit::acquire(&live_allocations, capacity) {
                                    Ok(permit) => {
                                        live_permit = Some(permit);
                                        Ok(())
                                    }
                                    Err(error) => Err(error),
                                }
                            } else {
                                Ok(())
                            };
                            let result = admission
                                .and_then(|()| match operation {
                                    LifecycleOperation::Park => {
                                        instance.park(generation, checkpoint_timeout)
                                    }
                                    LifecycleOperation::Resume => instance.resume(generation),
                                    LifecycleOperation::Release => instance.release(generation),
                                })
                                .map(|()| instance.status());
                            if instance.status().live_vms == 0 {
                                live_permit = None;
                            }
                            *owner_status.lock().expect("instance status poisoned") =
                                Some(instance.status());
                            let released = result
                                .as_ref()
                                .is_ok_and(|status| status.state == InstanceState::Released);
                            if reply.send(result).is_err() {
                                tracing::warn!("instance lifecycle receiver disconnected");
                            }
                            if released {
                                break;
                            }
                        }
                        Ok(Command::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if let Err(error) = instance.idle_tick(checkpoint_timeout) {
                                tracing::error!(%error, "instance idle checkpoint failed");
                            }
                            if instance.status().live_vms == 0 {
                                live_permit = None;
                            }
                        }
                    }
                    *owner_status.lock().expect("instance status poisoned") =
                        Some(instance.status());
                }
            })?;
        let status = match initialized_rx.recv() {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                owner
                    .join()
                    .map_err(|_| Error::State("instance initializer panicked".into()))?;
                return Err(error);
            }
            Err(error) => {
                owner
                    .join()
                    .map_err(|_| Error::State("instance initializer panicked".into()))?;
                return Err(Error::State(format!(
                    "instance initialization unavailable:{error}"
                )));
            }
        };
        slots.insert(
            status.identity.instance_id.clone(),
            Slot {
                sender,
                owner: Some(owner),
                status: shared_status,
            },
        );
        Ok(status)
    }

    pub fn lifecycle(
        &self,
        instance_id: &str,
        generation: u64,
        operation: LifecycleOperation,
    ) -> Result<InstanceStatus> {
        let (reply, receive) = mpsc::channel();
        {
            let slots = self
                .slots
                .lock()
                .map_err(|_| Error::State("instance slots poisoned".into()))?;
            let slot = slots
                .get(instance_id)
                .ok_or_else(|| Error::State("unknown instance".into()))?;
            slot.sender
                .try_send(Command::Lifecycle {
                    operation,
                    generation,
                    reply,
                })
                .map_err(|error| {
                    Error::State(format!("instance lifecycle admission unavailable:{error}"))
                })?;
        }
        let status = receive.recv().map_err(|error| {
            Error::State(format!("instance lifecycle owner unavailable:{error}"))
        })??;
        if status.state == InstanceState::Released {
            let mut slots = self
                .slots
                .lock()
                .map_err(|_| Error::State("instance slots poisoned".into()))?;
            let slot = slots
                .get_mut(instance_id)
                .ok_or_else(|| Error::State("released instance slot missing".into()))?;
            if let Some(owner) = slot.owner.take() {
                owner.join().map_err(|_| {
                    Error::State("instance termination owner panicked; capacity quarantined".into())
                })?;
            }
            slots.remove(instance_id);
        }
        Ok(status)
    }

    pub fn try_submit(
        &self,
        instance_id: &str,
        generation: u64,
        request: InvocationRequest,
        timeout: Duration,
        cancellation: InvocationCancellation,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> Result<()> {
        let slots = self
            .slots
            .lock()
            .map_err(|_| Error::State("instance slots poisoned".into()))?;
        let slot = slots
            .get(instance_id)
            .ok_or_else(|| Error::State("unknown instance".into()))?;
        slot.sender
            .try_send(Command::Invoke {
                generation,
                request,
                timeout,
                admitted_at: Instant::now(),
                cancellation,
                completion: Box::new(completion),
            })
            .map_err(|error| {
                Error::State(format!("instance invocation admission unavailable:{error}"))
            })?;
        Ok(())
    }

    pub fn status(&self) -> Result<Vec<InstanceStatus>> {
        let slots = self
            .slots
            .lock()
            .map_err(|_| Error::State("instance slots poisoned".into()))?;
        let mut statuses = slots
            .values()
            .map(|slot| {
                slot.status
                    .lock()
                    .map_err(|_| Error::State("instance status poisoned".into()))?
                    .clone()
                    .ok_or_else(|| Error::State("instance status not initialized".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(store) = &self.store {
            for record in store.records(self.worker.snapshot().binding())? {
                if !slots.contains_key(&record.identity.instance_id) {
                    statuses.push(InstanceStatus {
                        identity: record.identity,
                        state: record.state,
                        live_vms: 0,
                        parked_instances: usize::from(record.state == InstanceState::Parked),
                        resuming_instances: usize::from(record.state == InstanceState::Resuming),
                        requests_served: record.requests_served,
                        checkpoint_policy: self.config.checkpoint_policy,
                        checkpoint_id: record.checkpoint_id,
                        owner_loaded: false,
                        ownership_reconciled: record.state == InstanceState::Parked,
                    });
                }
            }
        }
        statuses.sort_by(|left, right| left.identity.instance_id.cmp(&right.identity.instance_id));
        Ok(statuses)
    }

    pub fn try_submit_stream(
        &self,
        identity: &InstanceIdentity,
        request: super::RequestEnvelope,
        websocket: bool,
        ingress: super::GuestIngress,
        timeout: Duration,
        completion: impl FnOnce(InvocationExecution) + Send + 'static,
    ) -> Result<()> {
        let slots = self
            .slots
            .lock()
            .map_err(|_| Error::State("instance slots poisoned".into()))?;
        let slot = slots
            .get(&identity.instance_id)
            .ok_or_else(|| Error::State("unknown instance".into()))?;
        slot.sender
            .try_send(Command::Stream {
                generation: identity.generation,
                request,
                websocket,
                ingress,
                timeout,
                admitted_at: Instant::now(),
                completion: Box::new(completion),
            })
            .map_err(|error| {
                Error::State(format!("fenced stream admission unavailable:{error}"))
            })?;
        Ok(())
    }
}

impl Drop for InstanceHome {
    fn drop(&mut self) {
        let slots = self
            .slots
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for slot in slots.values_mut() {
            if slot.sender.send(Command::Shutdown).is_err() {
                tracing::debug!("instance owner already stopped");
            }
            if let Some(owner) = slot.owner.take()
                && owner.join().is_err()
            {
                tracing::error!("instance shutdown owner panicked");
            }
        }
    }
}
