// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

//! Executable evidence for the experimental VM-per-process snapshot fork.
//!
//! This is intentionally a focused prototype, not a public process API.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use hyperlight_unikraft::process::{
    CancelOutcome, CapabilityDisposition, InheritedCapability, ProcessExitStatus, ProcessOptions,
    ProcessState, TrustedProcessSnapshot, VmProcessHost,
};
use hyperlight_unikraft::{AppSandbox, Error, SandboxBuilder, Yield};

const RESULT_PENDING: i64 = -1;

const FORK_SCRIPT: &str = r#"
import hyperlight
import time

fork_state = {"private": "seed"}

def SleepCancel(_event):
    hyperlight.call("cancel.started")
    while True:
        pass

def ReadPrivate(_event):
    return fork_state["private"]

hyperlight.call("fork.prepare")
time.sleep(0.2)
fork_result = hyperlight.call("fork.result")
fork_state["private"] = "child" if fork_result == 0 else "parent"
print(f"fork-result={fork_result} private={fork_state['private']}", flush=True)
hyperlight.call("fork.report", fork_result, fork_state["private"])
"#;

struct ForkEndpoint {
    result: Arc<AtomicI64>,
    prepared: Arc<AtomicBool>,
    reports: Arc<Mutex<Vec<(i64, String)>>>,
    cancel_started: Option<SyncSender<()>>,
}

impl ForkEndpoint {
    fn new(result: i64) -> Self {
        Self {
            result: Arc::new(AtomicI64::new(result)),
            prepared: Arc::new(AtomicBool::new(false)),
            reports: Arc::new(Mutex::new(Vec::new())),
            cancel_started: None,
        }
    }

    fn with_cancel_started(mut self, sender: SyncSender<()>) -> Self {
        self.cancel_started = Some(sender);
        self
    }

    fn apply(self, builder: SandboxBuilder) -> SandboxBuilder {
        let prepared = self.prepared.clone();
        let result = self.result.clone();
        let reports = self.reports.clone();
        let cancel_started = self.cancel_started;
        builder
            .host_function("fork.prepare", move |_| {
                prepared.store(true, Ordering::Release);
                Ok("null".to_string())
            })
            .host_function("fork.result", move |_| {
                let value = result.load(Ordering::Acquire);
                if value == RESULT_PENDING {
                    Err("fork result requested before clone".to_string())
                } else {
                    Ok(value.to_string())
                }
            })
            .host_function("fork.report", move |args| {
                let (result, private): (i64, String) =
                    serde_json::from_str(args).map_err(|e| e.to_string())?;
                reports.lock().unwrap().push((result, private));
                Ok("null".to_string())
            })
            .host_function("cancel.started", move |_| {
                if let Some(sender) = &cancel_started {
                    sender
                        .send(())
                        .map_err(|_| "cancellation observer dropped".to_string())?;
                }
                Ok("null".to_string())
            })
    }
}

fn finish_fork_call(sandbox: &mut AppSandbox) -> Result<(), Error> {
    loop {
        match sandbox.step(Duration::from_secs(2))? {
            Yield::CallDone => return Ok(()),
            Yield::CallFailed { status } => return Err(Error::CallFailed { status }),
            Yield::Exited { status } => return Err(Error::GuestExited { status }),
            Yield::Blocked { .. } => {}
        }
    }
}

#[test]
fn vm_per_process_snapshot_fork_prototype() {
    let rootfs = common::require_rootfs("python");
    let parent = ForkEndpoint::new(RESULT_PENDING);
    let parent_result = parent.result.clone();
    let parent_prepared = parent.prepared.clone();
    let parent_reports = parent.reports.clone();

    let mut parent_vm = parent
        .apply(SandboxBuilder::from_initrd(rootfs).scratch_mb(256))
        .boot()
        .expect("boot parent");
    parent_vm.submit(FORK_SCRIPT).expect("submit fork script");
    assert!(
        parent_prepared.load(Ordering::Acquire),
        "snapshot boundary must follow the completed fork.prepare host call"
    );

    let snapshot = parent_vm.snapshot().expect("snapshot clean fork boundary");
    let process_host = VmProcessHost::new();

    let child_endpoint = ForkEndpoint::new(0);
    let child_reports = child_endpoint.reports.clone();
    let mut child = process_host
        .spawn(
            TrustedProcessSnapshot::running_process_fork(snapshot.clone(), 1),
            ProcessOptions::default()
                .stdout_chunks(std::num::NonZeroUsize::new(1).expect("one is non-zero")),
            move |builder| child_endpoint.apply(builder),
            |sandbox, _| {
                finish_fork_call(sandbox)?;
                Ok(0)
            },
        )
        .expect("spawn restored child");
    let child_pid = child.pid();
    parent_result.store(
        i64::try_from(child_pid.get()).expect("virtual PID fits fork prototype ABI"),
        Ordering::Release,
    );
    let child_stdout = child.take_stdout().expect("take child stdout");
    let child_output = thread::spawn(move || child_stdout.read_to_end());

    finish_fork_call(&mut parent_vm).expect("finish parent fork continuation");
    assert_eq!(
        parent_reports.lock().unwrap().as_slice(),
        &[(
            i64::try_from(child_pid.get()).expect("virtual PID fits fork prototype ABI"),
            "parent".to_string()
        )]
    );

    let status = child.wait().expect("wait for child");
    let output = child_output.join().expect("stdout collector");
    assert_eq!(status, ProcessExitStatus::Exited(0));
    assert_eq!(process_host.state(child_pid), None, "wait reaps the PID");
    assert_eq!(
        child_reports.lock().unwrap().as_slice(),
        &[(0, "child".to_string())]
    );
    assert!(
        String::from_utf8(output)
            .unwrap()
            .contains("fork-result=0 private=child")
    );
    assert_eq!(
        parent_vm
            .call("ReadPrivate", "null")
            .expect("read parent-private memory"),
        "\"parent\"",
        "the child's write must not leak into the parent's private memory"
    );

    let (cancel_started_tx, cancel_started_rx) = sync_channel(1);
    let cancelled_endpoint = ForkEndpoint::new(0).with_cancel_started(cancel_started_tx);
    let cancelled = process_host
        .spawn(
            TrustedProcessSnapshot::running_process_fork(snapshot.clone(), 1),
            ProcessOptions::default(),
            move |builder| cancelled_endpoint.apply(builder),
            |sandbox, _| {
                finish_fork_call(sandbox)?;
                sandbox.call("SleepCancel", "null")?;
                Ok(0)
            },
        )
        .expect("spawn cancellable child");
    let cancelled_pid = cancelled.pid();
    cancel_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("SleepCancel entered");
    assert_eq!(
        cancelled
            .cancel(Duration::from_millis(20))
            .expect("cancel child"),
        CancelOutcome::HardKilled
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if matches!(
            process_host.state(cancelled_pid),
            Some(ProcessState::Poisoned(ProcessExitStatus::Cancelled))
        ) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "killed VM did not become poisoned"
        );
        thread::yield_now();
    }
    assert_eq!(
        cancelled.wait().expect("wait for cancelled child"),
        ProcessExitStatus::Cancelled
    );
    assert_eq!(
        process_host.state(cancelled_pid),
        None,
        "poisoned VM is dropped and reaped"
    );

    let replacement_endpoint = ForkEndpoint::new(0);
    let replacement = process_host
        .spawn(
            TrustedProcessSnapshot::running_process_fork(snapshot.clone(), 1),
            ProcessOptions::default(),
            move |builder| replacement_endpoint.apply(builder),
            |sandbox, _| {
                finish_fork_call(sandbox)?;
                Ok(0)
            },
        )
        .expect("restore a fresh VM after poison");
    assert!(
        replacement.pid() > cancelled_pid,
        "virtual PIDs and killed VMs are never reused"
    );
    assert_eq!(
        replacement.wait().expect("wait for replacement"),
        ProcessExitStatus::Exited(0)
    );

    let unsupported = process_host.spawn(
        TrustedProcessSnapshot::running_process_fork(snapshot, 1),
        ProcessOptions::default().capabilities([InheritedCapability::new(
            "workspace",
            CapabilityDisposition::ShareOpenDescription,
        )]),
        |builder| builder,
        |_, _| Ok(0),
    );
    assert!(
        unsupported.is_err(),
        "unsupported inheritance must fail before restoring a VM"
    );
}
