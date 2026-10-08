// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

//! Explicit real Workerd/KVM qualification. Fixture tests cannot substitute.
//! WORKERD_REAL_EXECUTOR and WORKERD_REAL_ROOTFS identify frozen matching
//! artifacts supplied by the guest build owner; diagnostics are not releases.
use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::*;
use std::path::PathBuf;
use std::time::Duration;

fn artifacts() -> (PathBuf, PathBuf) {
    if std::env::var_os("WORKERD_REAL_LOG").is_some() {
        static LOGGER: std::sync::Once = std::sync::Once::new();
        LOGGER.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter("hyperlight_unikraft=debug")
                .with_writer(std::io::stderr)
                .try_init()
                .expect("real test logger initializes once");
        });
    }
    let executor = PathBuf::from(
        std::env::var("WORKERD_REAL_EXECUTOR").expect("set frozen actual Workerd executor path"),
    );
    let rootfs = PathBuf::from(
        std::env::var("WORKERD_REAL_ROOTFS").expect("set matching rootfs/direct ELF path"),
    );
    assert!(executor.is_file() && rootfs.is_file());
    (rootfs, executor)
}

fn request(id: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: 1,
        request_id: id.into(),
        method: "GET".into(),
        url: "https://example.test/counter".into(),
        headers: vec![],
        body_base64: String::new(),
    }
}

fn registry(
    rootfs: PathBuf,
    executor: PathBuf,
    bundle_path: PathBuf,
    resident: bool,
    streaming: bool,
) -> AppRegistry {
    let pool = if resident {
        AppPoolConfig::Resident(ResidentPoolConfigJson {
            capacity: 1,
            queue_capacity: 2,
            max_requests_per_vm: None,
            max_lifetime_secs: None,
        })
    } else {
        AppPoolConfig::Disposable(DisposablePoolConfig {
            max_concurrent_sandboxes: 1,
            queue_capacity: 2,
        })
    };
    AppRegistry::from_host_config(HostConfig {
        rootfs_path: rootfs,
        executor_path: executor,
        apps: vec![AppConfig {
            route: AppRoute {
                app_id: "legacy-app".into(),
                hostnames: vec!["example.test".into()],
                path_prefix: None,
            },
            bundle_path,
            scratch_memory_mb: 512,
            execute_timeout_secs: 30,
            capability_policy: WorkerCapabilityPolicyConfig::default(),
            pool,
            connection_affinity: ConnectionAffinity::None,
            snapshot_dir: None,
            instance_home: None,
            streaming,
        }],
    })
    .unwrap()
}

fn worker() -> WorkerVersionSandbox {
    let (rootfs, executor) = artifacts();
    let mut bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-large-v4").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"let count=0;let completed=0;
export default {
 async fetch(request,env,ctx) {
  count++;
  ctx.waitUntil(new Promise(resolve=>setTimeout(()=>{completed++;resolve()},25)));
  return new Response(JSON.stringify({count,completed}));
 },
 async scheduled(event,env,ctx) {
  count+=10;
  ctx.waitUntil(new Promise(resolve=>setTimeout(()=>{completed++;resolve()},25)));
 },
 async queue(batch,env,ctx) {
  count+=100;
  for (const message of batch.messages) message.ack();
  ctx.waitUntil(new Promise(resolve=>setTimeout(()=>{completed++;resolve()},25)));
 }
};"#,
    )
    .unwrap();
    bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
    let padding = 757597 - bundle.modules[0].source.len() - 4;
    bundle.modules[0].source.push_str("/*");
    bundle.modules[0].source.push_str(&"x".repeat(padding));
    bundle.modules[0].source.push_str("*/");
    assert_eq!(bundle.modules[0].source.len(), 757597);
    let scratch = std::env::var("WORKERD_REAL_SCRATCH_MB")
        .map(|value| value.parse::<usize>().expect("valid explicit scratch MiB"))
        .unwrap_or(512);
    WorkerVersionSandbox::initialize(bundle, rootfs, executor, scratch, Duration::from_secs(30))
        .unwrap()
}

fn json(response: ResponseEnvelope) -> serde_json::Value {
    serde_json::from_slice(&STANDARD.decode(response.body_base64).unwrap()).unwrap()
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_legacy_bundles_discover_guest_capabilities_in_both_execution_modes() {
    use std::sync::mpsc;
    let (rootfs, executor) = artifacts();
    let temporary = tempfile::tempdir().unwrap();
    let bundle = WorkerBundle::single_script(
        WorkerVersionId::new("legacy-capability-probe").unwrap(),
        "2025-01-01",
        "worker.js",
        "export default {fetch(){return new Response('ok')}}",
    )
    .unwrap();
    assert_eq!(bundle.protocol_version, PROTOCOL_VERSION);
    let path = temporary.path().join("bundle.json");
    std::fs::write(&path, serde_json::to_vec(&bundle).unwrap()).unwrap();
    for resident in [false, true] {
        let registry = registry(
            rootfs.clone(),
            executor.clone(),
            path.clone(),
            resident,
            false,
        );
        let app = registry.app("legacy-app").unwrap();
        for required in ["secure-entropy-v1", "tracked-work-drain"] {
            assert!(
                app.identity()
                    .capabilities
                    .iter()
                    .any(|capability| capability == required),
                "legacy bundle in resident={resident} did not discover {required}"
            );
        }
        let (reply, completion) = mpsc::channel();
        app.try_submit(
            request("legacy-fetch"),
            Duration::from_secs(10),
            move |execution| {
                reply.send(execution.result).unwrap();
            },
        )
        .unwrap();
        let response = completion
            .recv_timeout(Duration::from_secs(15))
            .unwrap()
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(STANDARD.decode(response.body_base64).unwrap(), b"ok");
    }
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_request_scoped_abort_signal_timer_probe() {
    use std::sync::mpsc;
    let (rootfs, executor) = artifacts();
    let bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-abort-signal-probe").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"let finished=0;
export default {fetch(request,env,ctx){
 if(request.url.endsWith('/read')) return new Response(String(finished));
 if(request.url.endsWith('/timeout')) AbortSignal.timeout(5000);
 if(request.url.endsWith('/any')) AbortSignal.any([AbortSignal.timeout(5000)]);
 if(request.url.endsWith('/cleared')) {const timer=setTimeout(()=>{},5000);clearTimeout(timer);}
 if(request.url.endsWith('/user')) setTimeout(()=>{},5000);
 if(request.url.endsWith('/interval')) setInterval(()=>{},5000);
 if(request.url.endsWith('/tracked')) {
  const signal=AbortSignal.any([AbortSignal.timeout(5000)]);
  ctx.waitUntil(new Promise(resolve=>setTimeout(()=>{
   if(signal.aborted) throw new Error('request deadline retired before tracked completion');
   finished++;resolve();
  },5)));
 }
 return new Response('ok');
}};"#,
    )
    .unwrap();
    let worker =
        WorkerVersionSandbox::initialize(bundle, rootfs, executor, 512, Duration::from_secs(30))
            .unwrap();
    let mut successful = true;
    for (path, expected) in [
        ("plain", true),
        ("cleared", true),
        ("timeout", true),
        ("any", true),
        ("tracked", true),
        ("user", false),
        ("interval", false),
    ] {
        let mut request = request(&format!("abort-{path}"));
        request.url = format!("https://example.test/{path}");
        let (mut host, guest) = HostIngress::pair(
            &request.request_id,
            false,
            Duration::from_secs(10),
            InvocationCancellation::default(),
        )
        .unwrap();
        let owner_worker = worker.clone();
        let (reply, result) = mpsc::channel();
        let owner = std::thread::spawn(move || {
            let mut resident = owner_worker.restore_resident().unwrap();
            let outcome = resident.execute_stream(request, false, guest, Duration::from_secs(10));
            if path == "tracked" && outcome.is_ok() {
                let mut read = crate::request("tracked-read");
                read.url = "https://example.test/read".into();
                let response = resident.execute(read, Duration::from_secs(5)).0.unwrap();
                assert_eq!(STANDARD.decode(response.body_base64).unwrap(), b"1");
            }
            if outcome.is_ok() {
                resident
                    .checkpoint(
                        &owner_worker,
                        &format!("deadline-{path}"),
                        Duration::from_secs(5),
                    )
                    .expect("request-owned deadline timers must release actual host handles");
            }
            reply.send(outcome).unwrap();
        });
        host.try_send(FrameKind::End, &[], None).unwrap();
        let mut ended = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(12);
        while !ended && std::time::Instant::now() < deadline {
            match host.receive(Duration::from_millis(1)) {
                Ok(Some(frame)) => ended = frame.kind == FrameKind::End,
                Ok(None) => {}
                Err(Error::Cancelled) => break,
                Err(error) => panic!("probe {path} transport failed: {error}"),
            }
        }
        let outcome = result.recv_timeout(Duration::from_secs(12)).unwrap();
        owner.join().unwrap();
        eprintln!("AbortSignal probe {path}: end={ended}, outcome={outcome:?}");
        assert!(ended, "probe must deliver a real response end");
        successful &= outcome.is_ok() == expected;
    }
    assert!(
        successful,
        "request-scoped timeout signals must not taint completed work"
    );
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_half_open_websocket_eof_retires_owner_and_releases_max_one_pool() {
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::time::Instant;
    use tungstenite::Message;
    let (rootfs, executor) = artifacts();
    let temporary = tempfile::tempdir().unwrap();
    let bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-half-open-eof").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"export default {fetch(){
 const pair=new WebSocketPair();pair[1].accept();
 pair[1].addEventListener('message',()=>pair[1].send('echo'));
 return new Response(null,{status:101,webSocket:pair[0]});
}};"#,
    )
    .unwrap();
    let path = temporary.path().join("bundle.json");
    std::fs::write(&path, serde_json::to_vec(&bundle).unwrap()).unwrap();
    for resident in [false, true] {
        let registry = registry(
            rootfs.clone(),
            executor.clone(),
            path.clone(),
            resident,
            true,
        );
        let app = registry.app("legacy-app").unwrap();
        for attempt in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let client = std::thread::spawn(move || {
                let stream = TcpStream::connect(address).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let (mut socket, response) =
                    tungstenite::client(format!("ws://{address}/"), stream).unwrap();
                assert_eq!(response.status(), 101);
                socket.send(Message::Text("hello".into())).unwrap();
                assert_eq!(socket.read().unwrap(), Message::Text("echo".into()));
                socket
                    .send(Message::Close(Some(tungstenite::protocol::CloseFrame {
                        code: 1000.into(),
                        reason: "peer".into(),
                    })))
                    .unwrap();
                socket.get_mut().shutdown(Shutdown::Both).unwrap();
            });
            let (mut stream, _) = listener.accept().unwrap();
            let id = format!("half-open-{resident}-{attempt}");
            let request = read_http_request(&mut stream, id.clone(), 16_384)
                .unwrap()
                .envelope;
            let (host, guest) = HostIngress::pair(
                id,
                true,
                Duration::from_secs(30),
                InvocationCancellation::default(),
            )
            .unwrap();
            let (reply, done) = mpsc::channel();
            app.try_submit_stream(
                request.clone(),
                true,
                guest,
                Duration::from_secs(30),
                move |execution| {
                    reply.send(execution).unwrap();
                },
            )
            .unwrap();
            let started = Instant::now();
            let result = pump_http_stream(stream, &request, &[], host, done);
            assert!(matches!(result, Err(Error::Cancelled)), "{result:?}");
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "EOF must release the slot before the 30s invocation deadline"
            );
            client.join().unwrap();
            assert_eq!(app.status_json()["active"], 0);
        }
    }
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_crypto_entropy_is_fresh_across_template_clones_and_durable_resume() {
    let (rootfs, executor) = artifacts();
    let bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-crypto-entropy").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"export default {
 async fetch() {
  const aes = await crypto.subtle.generateKey(
   {name:'AES-GCM',length:256}, true, ['encrypt','decrypt']);
  const ec = await crypto.subtle.generateKey(
   {name:'ECDSA',namedCurve:'P-256'}, true, ['sign','verify']);
  return Response.json({
   uuid: crypto.randomUUID(),
   bytes: Array.from(crypto.getRandomValues(new Uint8Array(32))),
   aes_key: Array.from(new Uint8Array(await crypto.subtle.exportKey('raw',aes))),
   ec_public: Array.from(new Uint8Array(await crypto.subtle.exportKey('raw',ec.publicKey)))
  });
 }
};"#,
    )
    .unwrap();
    let worker =
        WorkerVersionSandbox::initialize(bundle, rootfs, executor, 512, Duration::from_secs(30))
            .unwrap();
    worker
        .negotiate_extensions(&["secure-entropy-v1"], Duration::from_secs(5))
        .unwrap();
    let mut uuids = std::collections::BTreeSet::new();
    let mut random_bytes = std::collections::BTreeSet::new();
    let mut check_entropy = |response: ResponseEnvelope| {
        let value = json(response);
        let uuid = value["uuid"].as_str().unwrap();
        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.as_bytes()[14], b'4');
        assert!(
            uuids.insert(uuid.to_owned()),
            "snapshot restore repeated a UUID"
        );
        for (name, length) in [("bytes", 32), ("aes_key", 32), ("ec_public", 65)] {
            let bytes: Vec<u8> = serde_json::from_value(value[name].clone()).unwrap();
            assert_eq!(bytes.len(), length);
            assert!(
                random_bytes.insert((name, bytes)),
                "snapshot restore repeated {name}"
            );
        }
    };
    for sequence in 0..2 {
        check_entropy(
            worker
                .execute(
                    worker.worker_version(),
                    request(&format!("entropy-clone-{sequence}")),
                    Duration::from_secs(5),
                )
                .unwrap(),
        );
    }
    let mut resident = worker.restore_resident().unwrap();
    check_entropy(
        resident
            .execute(request("entropy-resident"), Duration::from_secs(5))
            .0
            .unwrap(),
    );
    let changed = resident
        .checkpoint(&worker, "entropy-park", Duration::from_secs(5))
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("entropy.db");
    let identity = resident.identity().clone();
    let store = CheckpointStore::open(&database, &[7; 32], 2 * 1024 * 1024 * 1024).unwrap();
    store
        .register(&identity, worker.snapshot().binding())
        .unwrap();
    let checkpoint = store.commit(&identity, &changed).unwrap();
    resident.retire();
    store.publish_parked(&identity, &checkpoint).unwrap();
    drop(store);
    let store = CheckpointStore::open(&database, &[7; 32], 2 * 1024 * 1024 * 1024).unwrap();
    let claim = store.claim(&identity, worker.snapshot().binding()).unwrap();
    assert_eq!(claim.identity.generation, 2);
    let mut resumed = worker.resume_checkpoint_claim(&store, claim).unwrap();
    check_entropy(
        resumed
            .execute(request("entropy-resumed"), Duration::from_secs(5))
            .0
            .unwrap(),
    );
    check_entropy(
        resumed
            .execute(request("entropy-resumed-next"), Duration::from_secs(5))
            .0
            .unwrap(),
    );
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_sequential_http_streams_reuse_residents_after_bodyless_responses() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    let (rootfs, executor) = artifacts();
    let mut bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-bodyless-reuse").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"let requests=0;
export default {fetch(request) {
 requests++;
 const status=request.method==='POST'?201:
  request.method==='PATCH'||request.method==='DELETE'?204:200;
 return new Response(status===204?null:'ok',{
  status,headers:{'x-requests':String(requests)}
 });
}};"#,
    )
    .unwrap();
    bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
    let worker =
        WorkerVersionSandbox::initialize(bundle, rootfs, executor, 512, Duration::from_secs(30))
            .unwrap();
    for resident_mode in [false, true] {
        let owner_worker = worker.clone();
        let (jobs, requests) = mpsc::channel::<(
            RequestEnvelope,
            GuestIngress,
            mpsc::Sender<InvocationExecution>,
        )>();
        let owner = std::thread::spawn(move || {
            let mut resident = resident_mode.then(|| owner_worker.restore_resident().unwrap());
            while let Ok((request, ingress, completion)) = requests.recv() {
                let request_id = request.request_id.clone();
                let result = if let Some(resident) = &mut resident {
                    resident.execute_stream(request, false, ingress, Duration::from_secs(5))
                } else {
                    owner_worker.execute_stream(request, false, ingress, Duration::from_secs(5))
                };
                completion
                    .send(InvocationExecution {
                        request_id,
                        result: result.map(|()| InvocationResponse::StreamComplete),
                        profile: Default::default(),
                        submit_error: None,
                    })
                    .unwrap();
            }
        });
        for (sequence, method) in ["GET", "POST", "GET", "PATCH", "GET", "DELETE", "GET"]
            .into_iter()
            .enumerate()
        {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let client = std::thread::spawn(move || {
                let mut stream = TcpStream::connect(address).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                write!(
                    stream,
                    "{method} /api/todos HTTP/1.1\r\nHost: example.test\r\nContent-Length: 0\r\n\r\n"
                )
                .unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).unwrap();
                response
            });
            let (mut stream, _) = listener.accept().unwrap();
            let id = format!("bodyless-{resident_mode}-{sequence}");
            let request = read_http_request(&mut stream, id.clone(), 16_384)
                .unwrap()
                .envelope;
            let (host, guest) = HostIngress::pair(
                id,
                false,
                Duration::from_secs(10),
                InvocationCancellation::default(),
            )
            .unwrap();
            let (completion, done) = mpsc::channel();
            jobs.send((request.clone(), guest, completion)).unwrap();
            pump_http_stream(stream, &request, &[], host, done).unwrap();
            let response = client.join().unwrap();
            let status = match method {
                "POST" => 201,
                "PATCH" | "DELETE" => 204,
                _ => 200,
            };
            assert!(response.starts_with(&format!("HTTP/1.1 {status} ")));
            let served = if resident_mode { sequence + 1 } else { 1 };
            assert!(
                response
                    .to_ascii_lowercase()
                    .contains(&format!("x-requests: {served}\r\n"))
            );
            if status == 204 {
                let (headers, body) = response.split_once("\r\n\r\n").unwrap();
                assert!(!headers.to_ascii_lowercase().contains("transfer-encoding"));
                assert!(body.is_empty());
            }
        }
        drop(jobs);
        owner.join().unwrap();
    }
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_large_init4_both_modes_tracked_work_and_changed_heap_durable_resume() {
    let worker = worker();
    for sequence in 0..2 {
        let result = worker
            .execute(
                worker.worker_version(),
                request(&format!("disposable-{sequence}")),
                Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(json(result), serde_json::json!({"count":1,"completed":0}));
    }
    let mut resident = worker.restore_resident().unwrap();
    assert_eq!(
        json(
            resident
                .execute(request("resident-1"), Duration::from_secs(5))
                .0
                .unwrap()
        ),
        serde_json::json!({"count":1,"completed":0})
    );
    assert_eq!(
        json(
            resident
                .execute(request("resident-2"), Duration::from_secs(5))
                .0
                .unwrap()
        ),
        serde_json::json!({"count":2,"completed":1}),
        "fetch must finish tracked waitUntil before reuse"
    );
    let changed = resident
        .checkpoint(&worker, "real-park", Duration::from_secs(5))
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("durable.db");
    let identity = resident.identity().clone();
    let store = CheckpointStore::open(&path, &[6; 32], 2 * 1024 * 1024 * 1024).unwrap();
    store
        .register(&identity, worker.snapshot().binding())
        .unwrap();
    let checkpoint = store.commit(&identity, &changed).unwrap();
    resident.retire();
    store.publish_parked(&identity, &checkpoint).unwrap();
    drop(store);
    let store = CheckpointStore::open(&path, &[6; 32], 2 * 1024 * 1024 * 1024).unwrap();
    let claim = store.claim(&identity, worker.snapshot().binding()).unwrap();
    assert_eq!(claim.identity.generation, 2);
    let mut resumed = worker.resume_checkpoint_claim(&store, claim).unwrap();
    assert_eq!(
        json(
            resumed
                .execute(request("resumed-3"), Duration::from_secs(5))
                .0
                .unwrap()
        ),
        serde_json::json!({"count":3,"completed":2}),
        "checkpoint must retain changed JavaScript heap and tracked completion"
    );
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_scheduled_queue_native_results_and_tracked_work_in_both_modes() {
    let worker = worker();
    let scheduled = ScheduledRequest {
        protocol_version: 1,
        request_id: "scheduled-1".into(),
        scheduled_time_unix_ms: 1767225600000,
        cron: "0 0 * * *".into(),
    };
    let queue = QueueRequest {
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
    };
    let result = worker
        .execute_scheduled(
            worker.worker_version(),
            scheduled.clone(),
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(result.outcome, "ok");
    let result = worker
        .execute_queue(
            worker.worker_version(),
            queue.clone(),
            Duration::from_secs(5),
        )
        .unwrap();
    assert!(result.ack_all || result.explicit_acks.contains(&"message-1".into()));
    let mut resident = worker.restore_resident().unwrap();
    assert!(matches!(
        resident
            .execute_invocation(
                InvocationRequest::Scheduled(scheduled),
                Duration::from_secs(5)
            )
            .0
            .unwrap(),
        InvocationResponse::Scheduled(_)
    ));
    assert!(matches!(
        resident
            .execute_invocation(InvocationRequest::Queue(queue), Duration::from_secs(5))
            .0
            .unwrap(),
        InvocationResponse::Queue(_)
    ));
    assert_eq!(
        json(
            resident
                .execute(request("after-events"), Duration::from_secs(5))
                .0
                .unwrap()
        ),
        serde_json::json!({"count":111,"completed":2}),
        "native event waitUntil must finish before resident reuse"
    );
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_literal_no_io_wait_until_never_cannot_be_checkpointed_as_quiescent() {
    for protocol in [PROTOCOL_VERSION, PACKAGE_PROTOCOL_VERSION] {
        let require_abort = |result: Result<ResponseEnvelope>| match result {
            Err(Error::Timeout) => {}
            // Control failures have separate context; this is a handler abort.
            Err(Error::Guest(hyperlight_unikraft::Error::CallFailed { status: -1 })) => {}
            Err(Error::State(message)) => {
                assert!(
                    matches!(
                        message.as_str(),
                        "guest has not reached a checkpoint safe point"
                            | "guest control checkpoint failed with status -1"
                    ),
                    "unexpected abort failure: {message}"
                );
            }
            other => {
                panic!("protocol {protocol} expected tracked abort or deadline, got {other:?}")
            }
        };
        let (rootfs, executor) = artifacts();
        let mut bundle = WorkerBundle::single_script(
            WorkerVersionId::new("real-literal-never").unwrap(),
            "2025-01-01",
            "worker.js",
            "export default {fetch(request,env,ctx){if(request.url.endsWith('/healthy'))return new Response('healthy');ctx.waitUntil(new Promise(()=>{}));return new Response('handler-response');}}",
        )
        .unwrap();
        bundle.protocol_version = protocol;
        let worker = WorkerVersionSandbox::initialize(
            bundle,
            rootfs,
            executor,
            512,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut healthy = request("healthy-disposable");
        healthy.url = "https://example.test/healthy".into();
        assert_eq!(
            worker
                .execute(
                    worker.worker_version(),
                    healthy.clone(),
                    Duration::from_secs(5)
                )
                .unwrap()
                .status,
            200
        );
        require_abort(worker.execute(
            worker.worker_version(),
            request("disposable-never"),
            Duration::from_secs(1),
        ));
        let mut resident = worker.restore_resident().unwrap();
        healthy.request_id = "healthy-resident".into();
        assert_eq!(
            resident
                .execute(healthy, Duration::from_secs(5))
                .0
                .unwrap()
                .status,
            200
        );
        let (result, _) = resident.execute(request("literal-never"), Duration::from_secs(1));
        require_abort(result);
        assert!(
            resident
                .checkpoint(&worker, "literal-never-checkpoint", Duration::from_secs(1))
                .is_err(),
            "protocol {protocol} aborted resident must be retired, not parkable"
        );
    }
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_get_head_body_absence_post_stream_and_sse_use_both_modes() {
    use std::sync::mpsc;
    let (rootfs, executor) = artifacts();
    let mut bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-ingress").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"export default {
 async fetch(request,env,ctx) {
  if(request.method==='POST') {
   const body=await request.text();
   return new Response('x'.repeat(80000),{headers:{'x-input-bytes':String(body.length)}});
  }
  const cloned=request.clone();const reconstructed=new Request(request);
  const valid=request.body===null&&cloned.body===null&&reconstructed.body===null;
  ctx.waitUntil(new Promise(resolve=>setTimeout(resolve,20)));
  return new Response(valid?'body-null':'invalid-body',{headers:{'x-body-null':String(valid)}});
 }
};"#,
    )
    .unwrap();
    bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
    let scratch = std::env::var("WORKERD_REAL_SCRATCH_MB")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(512);
    let worker = WorkerVersionSandbox::initialize(
        bundle,
        rootfs,
        executor,
        scratch,
        Duration::from_secs(30),
    )
    .unwrap();
    for resident_mode in [false, true] {
        let execute_worker = worker.clone();
        let (job_tx, job_rx) =
            mpsc::channel::<(RequestEnvelope, GuestIngress, mpsc::Sender<Result<()>>)>();
        let owner = std::thread::spawn(move || {
            let mut resident = resident_mode.then(|| execute_worker.restore_resident().unwrap());
            while let Ok((request, ingress, reply)) = job_rx.recv() {
                let result = if let Some(resident) = &mut resident {
                    resident.execute_stream(request, false, ingress, Duration::from_secs(5))
                } else {
                    execute_worker.execute_stream(request, false, ingress, Duration::from_secs(5))
                };
                reply.send(result).unwrap();
            }
        });
        for (sequence, method) in ["GET", "HEAD", "POST"].into_iter().enumerate() {
            let id = format!("stream-{resident_mode}-{sequence}");
            let mut envelope = request(&id);
            envelope.method = method.into();
            let body = if method == "POST" {
                vec![b'p'; 40000]
            } else {
                Vec::new()
            };
            envelope.headers.push(Header {
                name: "content-length".into(),
                value: body.len().to_string(),
            });
            let (mut host, guest) = HostIngress::pair(
                &id,
                false,
                Duration::from_secs(5),
                InvocationCancellation::default(),
            )
            .unwrap();
            let (reply, done) = mpsc::channel();
            job_tx.send((envelope, guest, reply)).unwrap();
            let mut offset = 0;
            let mut input_end = false;
            let mut response = Vec::new();
            let mut output_end = false;
            let mut headers = Vec::new();
            while !output_end {
                if offset < body.len() {
                    let end = (offset + MAX_FRAME_BYTES).min(body.len());
                    if host
                        .try_send(FrameKind::Data, &body[offset..end], None)
                        .unwrap()
                    {
                        offset = end;
                    }
                } else if !input_end && host.try_send(FrameKind::End, &[], None).unwrap() {
                    input_end = true;
                }
                if let Some(frame) = host.receive(Duration::from_millis(1)).unwrap() {
                    match frame.kind {
                        FrameKind::Headers => {
                            assert_eq!(frame.status, Some(200));
                            headers = frame.headers.unwrap();
                        }
                        FrameKind::Data => response.extend(frame.decoded_body().unwrap()),
                        FrameKind::End => output_end = true,
                        _ => panic!("unexpected real guest frame:{frame:?}"),
                    }
                }
            }
            done.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
            if method == "POST" {
                assert_eq!(response.len(), 80000);
                assert!(
                    headers
                        .iter()
                        .any(|header| header.name.eq_ignore_ascii_case("x-input-bytes")
                            && header.value == "40000")
                );
            } else {
                assert!(
                    headers
                        .iter()
                        .any(|header| header.name.eq_ignore_ascii_case("x-body-null")
                            && header.value == "true")
                );
                if method == "GET" {
                    assert_eq!(response, b"body-null");
                }
            }
        }
        drop(job_tx);
        owner.join().unwrap();
    }
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_inbound_websocket_handshake_greeting_echo_close_and_resident_reuse() {
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use tungstenite::Message;
    let (rootfs, executor) = artifacts();
    let mut bundle = WorkerBundle::single_script(
        WorkerVersionId::new("real-websocket").unwrap(),
        "2025-01-01",
        "worker.js",
        r#"export default {fetch(request){
 if(new URL(request.url).pathname==='/ws'){
  const pair=new WebSocketPair();const server=pair[1];server.accept();server.send('greeting');
  server.addEventListener('message',event=>{
   if(event.data==='close')server.close(1000,'done');else server.send('echo:'+event.data);
  });
  return new Response(null,{status:101,webSocket:pair[0]});
 }
 return new Response('after-websocket');
}};"#,
    )
    .unwrap();
    bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
    let scratch = std::env::var("WORKERD_REAL_SCRATCH_MB")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(512);
    let worker = WorkerVersionSandbox::initialize(
        bundle,
        rootfs,
        executor,
        scratch,
        Duration::from_secs(30),
    )
    .unwrap();
    for resident_mode in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            let stream = TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let (mut socket, handshake) =
                tungstenite::client(format!("ws://{address}/ws"), stream).unwrap();
            assert_eq!(handshake.status(), 101);
            assert_eq!(socket.read().unwrap(), Message::Text("greeting".into()));
            socket.send(Message::Text("client".into())).unwrap();
            assert_eq!(socket.read().unwrap(), Message::Text("echo:client".into()));
            socket.send(Message::Text("close".into())).unwrap();
            assert!(matches!(socket.read().unwrap(), Message::Close(_)));
            match socket.flush() {
                Ok(()) | Err(tungstenite::Error::ConnectionClosed) => {}
                Err(error) => panic!("{error}"),
            }
        });
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let request = read_http_request(&mut stream, "real-websocket".into(), 16384)
            .unwrap()
            .envelope;
        let edge_request = request.clone();
        let (host, guest) = HostIngress::pair(
            "real-websocket",
            true,
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .unwrap();
        let (done, completion) = mpsc::channel();
        let owner_worker = worker.clone();
        let owner = std::thread::spawn(move || {
            let mut resident = resident_mode.then(|| owner_worker.restore_resident().unwrap());
            let result = if let Some(resident) = &mut resident {
                resident.execute_stream(request.clone(), true, guest, Duration::from_secs(5))
            } else {
                owner_worker.execute_stream(request.clone(), true, guest, Duration::from_secs(5))
            };
            let success = result.is_ok();
            done.send(InvocationExecution {
                request_id: request.request_id,
                result: result.map(|()| InvocationResponse::StreamComplete),
                profile: Default::default(),
                submit_error: None,
            })
            .unwrap();
            if success && let Some(resident) = &mut resident {
                let response = resident
                    .execute(crate::request("after-ws"), Duration::from_secs(5))
                    .0
                    .unwrap();
                assert_eq!(
                    STANDARD.decode(response.body_base64).unwrap(),
                    b"after-websocket"
                );
            }
        });
        let result = pump_http_stream(stream, &edge_request, &[], host, completion);
        assert!(result.is_ok(), "{result:?}");
        client.join().unwrap();
        owner.join().unwrap();
    }
}

#[test]
#[ignore = "requires frozen real Workerd executor and native KVM"]
fn real_native_d1_and_host_only_webhook_reconstruct_after_durable_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let secret = tmp.path().join("webhook-secret");
    std::fs::write(&secret, b"synthetic-real-webhook-secret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let (rootfs, executor) = artifacts();
    let mut bundle=WorkerBundle::single_script(WorkerVersionId::new("real-host-bindings").unwrap(),
        "2025-01-01","worker.js",r#"let nextId=0;
export default {async fetch(request,env){
 if(new URL(request.url).pathname==='/verify'){
  const body=await request.arrayBuffer();
  return Response.json(await env.webhook.verify(body,request.headers.get('stripe-signature')));
 }
 const query=async(sql,parameters=[])=>{
  const response=await env.store.fetch('https://logical.invalid/',{method:'POST',body:JSON.stringify({
   version:2,request_id:'d1-'+(++nextId),binding:'store',
   operation:{kind:'d1_batch',statements:[{sql,parameters}]}
  })});
  const value=await response.json();if(value.status!=='ok')throw new Error('D1:'+JSON.stringify(value));
  return value;
 };
 await query('CREATE TABLE IF NOT EXISTS items(id TEXT PRIMARY KEY,value TEXT)');
 await query('INSERT INTO items(id,value) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value RETURNING value',
  [{type:'text',value:'key'},{type:'text',value:'persisted'}]);
 const result=await query('SELECT value FROM items WHERE id=?',[{type:'text',value:'key'}]);
 return Response.json(result);
}};"#).unwrap();
    bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
    let policy = WorkerCapabilityPolicy::default()
        .with_logical_bindings(
            LogicalBindingsPolicy::new(
                "tenant-bindings",
                "real-host-bindings",
                vec![
                    LogicalBindingConfig::D1 {
                        name: "store".into(),
                        backing_path: tmp.path().join("data.sqlite"),
                        read_only: false,
                        limits: Default::default(),
                    },
                    LogicalBindingConfig::Webhook(WebhookPolicy {
                        name: "webhook".into(),
                        secret_reference: "stripe-test".into(),
                        value_file: secret.clone(),
                        tolerance_secs: 300,
                    }),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let scratch = std::env::var("WORKERD_REAL_SCRATCH_MB")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(512);
    let worker = WorkerVersionSandbox::initialize_with_policy(
        bundle,
        rootfs,
        executor,
        scratch,
        Duration::from_secs(30),
        policy,
    )
    .unwrap();
    let (direct, profile) = worker.execute_profiled(
        worker.worker_version(),
        request("d1-disposable"),
        Duration::from_secs(5),
    );
    let direct = direct.unwrap_or_else(|error| {
        panic!("disposable D1/webhook probe failed: {error}; profile={profile:?}")
    });
    let direct = json(direct);
    assert!(
        direct.to_string().contains("persisted"),
        "actual native D1 all() shape: {direct}"
    );
    eprintln!("actual D1 all shape={direct}");
    let store = std::sync::Arc::new(
        CheckpointStore::open(
            tmp.path().join("checkpoints.sqlite"),
            &[5; 32],
            1024 * 1024 * 1024,
        )
        .unwrap(),
    );
    let mut instance =
        ResidentInstance::create(worker.clone(), CheckpointPolicy::Durable, Some(store), None)
            .unwrap();
    let identity = instance.identity().clone();
    let raw = br#"{"id":"evt-real","data":{}}"#;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut signed = format!("{now}.").into_bytes();
    signed.extend_from_slice(raw);
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"synthetic-real-webhook-secret");
    let digest = ring::hmac::sign(&key, &signed);
    let signature = format!(
        "t={now},v1={}",
        digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let verify = |id: &str, signature: &str| {
        let mut envelope = request(id);
        envelope.url = "https://example.test/verify".into();
        envelope.method = "POST".into();
        envelope.body_base64 = STANDARD.encode(raw);
        envelope.headers.push(Header {
            name: "stripe-signature".into(),
            value: signature.into(),
        });
        envelope
    };
    let InvocationResponse::Fetch(response) = instance
        .invoke(
            identity.generation,
            verify("verify-before", &signature).into(),
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .0
        .unwrap()
    else {
        panic!("wrong response")
    };
    assert_eq!(
        json(response),
        serde_json::json!({"valid":true,"event":{"id":"evt-real","data":{}}})
    );
    instance
        .park(identity.generation, Duration::from_secs(5))
        .unwrap();
    instance.resume(identity.generation).unwrap();
    let generation = instance.identity().generation;
    let InvocationResponse::Fetch(response) = instance
        .invoke(
            generation,
            request("d1-after").into(),
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .0
        .unwrap()
    else {
        panic!("wrong response")
    };
    assert!(json(response).to_string().contains("persisted"));
    let InvocationResponse::Fetch(response) = instance
        .invoke(
            generation,
            verify("verify-after", &signature).into(),
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .0
        .unwrap()
    else {
        panic!("wrong response")
    };
    assert_eq!(json(response)["valid"], true);
    std::fs::write(&secret, b"rotated-synthetic-key").unwrap();
    let InvocationResponse::Fetch(response) = instance
        .invoke(
            generation,
            verify("verify-rotated", &signature).into(),
            Duration::from_secs(5),
            InvocationCancellation::default(),
        )
        .0
        .unwrap()
    else {
        panic!("wrong response")
    };
    assert_eq!(
        json(response)["valid"],
        false,
        "old signing key must not be embedded in template/checkpoint"
    );
}
