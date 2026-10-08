// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! KVM/fixture plumbing proof, not Workerd JavaScript handler qualification.
use hyperlight_unikraft::workerd::*;
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

fn worker() -> WorkerVersionSandbox {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("build-elfloader/workerd-executor-fixture");
    WorkerVersionSandbox::initialize(
        WorkerBundle::single_script(
            WorkerVersionId::new("invocation-parity-v1").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default {}",
        )
        .unwrap(),
        fixture.join("rootfs.cpio"),
        fixture.join("executor"),
        64,
        Duration::from_secs(10),
    )
    .unwrap()
}

fn invocations() -> Vec<InvocationRequest> {
    vec![
        InvocationRequest::Fetch(RequestEnvelope {
            protocol_version: 1,
            request_id: "fetch-1".into(),
            method: "GET".into(),
            url: "https://example.test/".into(),
            headers: vec![],
            body_base64: String::new(),
        }),
        InvocationRequest::Scheduled(ScheduledRequest {
            protocol_version: 1,
            request_id: "scheduled-1".into(),
            scheduled_time_unix_ms: 1767225600000,
            cron: "0 0 * * *".into(),
        }),
        InvocationRequest::Queue(QueueRequest {
            protocol_version: 1,
            request_id: "queue-1".into(),
            queue: "jobs".into(),
            messages: vec![QueueMessage {
                id: "message-1".into(),
                timestamp_unix_ms: 1767225600000,
                body_base64: "aGVsbG8=".into(),
                content_type: Some("text".into()),
                attempts: 1,
            }],
            metadata: QueueMetadata {
                backlog_count: 1.0,
                backlog_bytes: 5.0,
                oldest_message_timestamp_unix_ms: None,
            },
        }),
    ]
}

fn assert_response(request: &InvocationRequest, result: InvocationResponse) {
    match (request, result) {
        (InvocationRequest::Fetch(request), InvocationResponse::Fetch(response)) => {
            assert_eq!(response.request_id, request.request_id);
            assert_eq!(response.status, 200);
        }
        (InvocationRequest::Scheduled(request), InvocationResponse::Scheduled(response)) => {
            assert_eq!(response.request_id, request.request_id);
            assert_eq!(response.outcome, "ok");
            assert!(!response.retry);
        }
        (InvocationRequest::Queue(request), InvocationResponse::Queue(response)) => {
            assert_eq!(response.request_id, request.request_id);
            assert!(response.ack_all);
        }
        (_, response) => panic!("wrong response kind: {response:?}"),
    }
}

#[test]
fn every_invocation_kind_uses_both_bounded_pools_and_resident_affinity() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(8);
    let worker = worker();
    let disposable = WorkerRequestPool::new(worker.clone(), 1, 4).unwrap();
    let resident = ResidentWorkerPool::new(
        worker,
        ResidentPoolConfig {
            capacity: 1,
            queue_capacity: 4,
            policy: ResidentPolicy::default(),
        },
    )
    .unwrap();
    for request in invocations() {
        let (tx, rx) = mpsc::channel();
        disposable
            .try_submit_invocation(request.clone(), Duration::from_secs(5), move |execution| {
                tx.send(execution).unwrap();
            })
            .unwrap();
        assert_response(
            &request,
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .result
                .unwrap(),
        );
        let (tx, rx) = mpsc::channel();
        resident
            .try_submit_invocation(request.clone(), Duration::from_secs(5), move |execution| {
                tx.send(execution).unwrap();
            })
            .unwrap();
        assert_response(
            &request,
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .result
                .unwrap(),
        );
    }
    let reserved = resident.reserve().unwrap();
    for request in invocations() {
        assert_response(
            &request,
            reserved
                .execute_invocation(request.clone(), Duration::from_secs(5))
                .result
                .unwrap(),
        );
    }
    assert_eq!(resident.status().retirements, 0);
    assert_eq!(resident.status().resident_requests_served, 6);
}

#[test]
fn queued_invocation_cannot_receive_a_fresh_deadline_after_its_budget_expires() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(4);
    let pool = ResidentWorkerPool::new(
        worker(),
        ResidentPoolConfig {
            capacity: 1,
            queue_capacity: 2,
            policy: ResidentPolicy::default(),
        },
    )
    .unwrap();
    let (first_tx, first_rx) = mpsc::channel();
    let InvocationRequest::Fetch(mut slow) = invocations().remove(0) else {
        unreachable!()
    };
    slow.url = "https://example.test/delay".into();
    pool.try_submit(slow, Duration::from_secs(5), move |execution| {
        first_tx.send(execution).unwrap();
    })
    .unwrap();
    let (tx, rx) = mpsc::channel();
    pool.try_submit_invocation(
        invocations().remove(1),
        Duration::from_millis(1),
        move |execution| {
            tx.send(execution).unwrap();
        },
    )
    .unwrap();
    assert!(
        first_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .result
            .is_ok()
    );
    assert!(matches!(
        rx.recv_timeout(Duration::from_secs(10)).unwrap().result,
        Err(Error::Timeout)
    ));
}

#[test]
fn cancellation_kills_running_guests_in_both_modes_without_reusing_failed_residents() {
    #[cfg(windows)]
    hyperlight_unikraft::configure_surrogates(8);
    let worker = worker();
    let disposable = WorkerRequestPool::new(worker.clone(), 1, 2).unwrap();
    let resident = ResidentWorkerPool::new(
        worker,
        ResidentPoolConfig {
            capacity: 1,
            queue_capacity: 2,
            policy: ResidentPolicy::default(),
        },
    )
    .unwrap();
    for resident_mode in [false, true] {
        let InvocationRequest::Fetch(mut request) = invocations().remove(0) else {
            unreachable!()
        };
        request.url = "https://example.test/busy".into();
        let cancellation = InvocationCancellation::default();
        let (tx, rx) = mpsc::channel();
        if resident_mode {
            resident
                .try_submit_cancellable(
                    request.into(),
                    Duration::from_secs(30),
                    cancellation.clone(),
                    move |execution| {
                        tx.send(execution).unwrap();
                    },
                )
                .unwrap();
        } else {
            disposable
                .try_submit_cancellable(
                    request.into(),
                    Duration::from_secs(30),
                    cancellation.clone(),
                    move |execution| {
                        tx.send(execution).unwrap();
                    },
                )
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(100));
        let cancelled_at = std::time::Instant::now();
        cancellation.cancel();
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap().result,
            Err(Error::Cancelled)
        ));
        assert!(cancelled_at.elapsed() < Duration::from_secs(2));
    }
    let reserved = resident.reserve().unwrap();
    assert_response(
        &invocations()[0],
        reserved
            .execute_invocation(invocations().remove(0), Duration::from_secs(5))
            .result
            .unwrap(),
    );
    assert_eq!(resident.status().retirements, 1);
}
