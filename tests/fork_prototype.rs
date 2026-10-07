// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

//! Executable evidence for the experimental VM-per-process snapshot fork.
//!
//! This is intentionally a focused prototype, not a public process API.

mod common;

use std::io;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hyperlight_unikraft::{AppSandbox, Error, SandboxBuilder, Snapshot, Yield};

const CHILD_PID: i64 = 10_001;
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum ChildStatus {
    Exited(i32),
    Terminated,
}

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

struct VirtualChild {
    pid: i64,
    stdout: thread::JoinHandle<Vec<u8>>,
    status: Receiver<ChildStatus>,
    interrupt: Receiver<Arc<dyn hyperlight_unikraft::hyperlight_host::hypervisor::InterruptHandle>>,
    worker: thread::JoinHandle<()>,
}

impl VirtualChild {
    fn wait(self) -> (ChildStatus, Vec<u8>) {
        let status = self.status.recv().expect("child status");
        self.worker.join().expect("child worker");
        let output = self.stdout.join().expect("stdout collector");
        (status, output)
    }
}

fn stdout_pipe() -> (
    impl Fn(&[u8]) -> io::Result<()> + Send + Sync + 'static,
    Receiver<Vec<u8>>,
) {
    let (sender, receiver) = sync_channel::<Vec<u8>>(1);
    let handler = move |bytes: &[u8]| {
        sender
            .send(bytes.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "stdout reader dropped"))
    };
    (handler, receiver)
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

fn spawn_child(
    pid: i64,
    snapshot: Arc<Snapshot>,
    endpoint: ForkEndpoint,
    cancel_after_fork: bool,
) -> VirtualChild {
    let (stdout_handler, stdout) = stdout_pipe();
    let (status_tx, status) = sync_channel(1);
    let (interrupt_tx, interrupt) = sync_channel(1);
    let stdout = thread::spawn(move || {
        let mut output = Vec::new();
        while let Ok(chunk) = stdout.recv() {
            output.extend_from_slice(&chunk);
        }
        output
    });
    let worker = thread::spawn(move || {
        let builder = endpoint
            .apply(SandboxBuilder::from_snapshot(snapshot))
            .stdout_handler(stdout_handler);
        let mut sandbox = builder.boot().expect("restore child");
        finish_fork_call(&mut sandbox).expect("finish child fork continuation");

        let handle = sandbox.interrupt_handle();
        interrupt_tx.send(handle).expect("publish interrupt handle");

        if cancel_after_fork {
            let result = sandbox.call("SleepCancel", "null");
            assert!(result.is_err(), "SleepCancel should be interrupted");
            drop(sandbox);
            status_tx
                .send(ChildStatus::Terminated)
                .expect("publish terminated status");
        } else {
            drop(sandbox);
            status_tx
                .send(ChildStatus::Exited(0))
                .expect("publish exit status");
        }
    });
    VirtualChild {
        pid,
        stdout,
        status,
        interrupt,
        worker,
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
    parent_result.store(CHILD_PID, Ordering::Release);

    let child_endpoint = ForkEndpoint::new(0);
    let child_reports = child_endpoint.reports.clone();
    let child = spawn_child(CHILD_PID, snapshot.clone(), child_endpoint, false);

    finish_fork_call(&mut parent_vm).expect("finish parent fork continuation");
    assert_eq!(
        parent_reports.lock().unwrap().as_slice(),
        &[(CHILD_PID, "parent".to_string())]
    );

    let child_pid = child.pid;
    let (status, output) = child.wait();
    assert_eq!(child_pid, CHILD_PID);
    assert_eq!(status, ChildStatus::Exited(0));
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
    let cancelled = spawn_child(CHILD_PID + 1, snapshot, cancelled_endpoint, true);
    let interrupt = cancelled.interrupt.recv().expect("interrupt handle");
    cancel_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("SleepCancel entered");
    thread::sleep(Duration::from_millis(20));
    assert!(interrupt.kill(), "SleepCancel must be running when killed");
    let (status, _) = cancelled.wait();
    assert_eq!(status, ChildStatus::Terminated);
}
