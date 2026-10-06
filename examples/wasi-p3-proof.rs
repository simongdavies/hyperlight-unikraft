// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::wasi_p3::{
    DeterministicExecutor, ExecutorStep, ImportDisposition, P3Adapters, TaskState, future, stream,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = DeterministicExecutor::new();
    let (future_writer, future_reader) = future();
    let future_value = Arc::new(Mutex::new(None));
    let task_value = future_value.clone();
    let future_task = executor.spawn(async move {
        *task_value.lock().unwrap() = Some(future_reader.await);
    });
    let pending = executor.step();
    future_writer.complete(42).unwrap();
    let completed = executor.step();

    let (cancel_writer, cancel_reader) = future::<u32>();
    cancel_reader.cancel();
    let producer_cancelled = cancel_writer.is_cancelled();

    let (mut stream_writer, mut stream_reader) = stream(1);
    let received = Arc::new(Mutex::new(Vec::new()));
    let stream_output = received.clone();
    let producer = executor.spawn(async move {
        stream_writer.send(1).await.unwrap();
        stream_writer.send(2).await.unwrap();
        stream_writer.close();
    });
    let consumer = executor.spawn(async move {
        while let Some(value) = stream_reader.read().await {
            stream_output.lock().unwrap().push(value);
        }
    });
    let stream_steps = executor.run_until_stalled();

    let denied = P3Adapters::deny_all();
    let terminal = denied.import_disposition("wasi:cli/terminal-input@0.3.1")?;
    let deferred = denied.import_disposition("wasi:http/handler@0.3.1")?;
    let unsupported = denied
        .validate_imports(["wasi:clocks/system-clock@0.3.2"])
        .unwrap_err();
    let capability_denied = denied
        .validate_imports(["vendor:process/spawn@0.3.1"])
        .unwrap_err();

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "proof_path": "executable async host-interface contract",
            "future": {
                "pending_step": format!("{pending:?}"),
                "completed_step": format!("{completed:?}"),
                "task_state": format!("{:?}", executor.state(future_task)),
                "value": format!("{:?}", *future_value.lock().unwrap()),
                "producer_cancelled": producer_cancelled
            },
            "stream": {
                "steps": stream_steps,
                "producer_state": format!("{:?}", executor.state(producer)),
                "consumer_state": format!("{:?}", executor.state(consumer)),
                "received": *received.lock().unwrap()
            },
            "imports": {
                "terminal": disposition(terminal),
                "http_handler": disposition(deferred),
                "unsupported": {
                    "category": format!("{:?}", unsupported.category()),
                    "interface": unsupported.interface()
                },
                "capability_denied": {
                    "category": format!("{:?}", capability_denied.category()),
                    "interface": capability_denied.interface()
                }
            },
            "expected": {
                "pending": format!("{:?}", ExecutorStep::Pending(future_task)),
                "completed": format!("{:?}", ExecutorStep::Completed(future_task)),
                "task_completed": format!("{:?}", Some(TaskState::Completed))
            }
        }))?
    );
    Ok(())
}

fn disposition(value: ImportDisposition) -> &'static str {
    match value {
        ImportDisposition::Allowed => "allowed",
        ImportDisposition::Deferred => "deferred",
        ImportDisposition::Denied => "denied",
    }
}
