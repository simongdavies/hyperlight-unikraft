// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::actor::{ActorId, ActorKey, ActorLifecycle, ActorNamespace, ActorRoute};
use hyperlight_unikraft::broker::RequestIdentity;
use hyperlight_unikraft::broker_adapter::BrokerHostError;
use hyperlight_unikraft::broker_runtime::{
    BrokerRuntime, LogicalInvocation, LogicalRuntimeStatus, LogicalServiceRouter, LogicalWireError,
    LogicalWireService,
};
use hyperlight_unikraft::data::{
    CacheBinding, CacheLimits, CacheService, DurableDeliveryContext, DurableObjectBinding,
    DurableObjectLimits, DurableObjectService, KvBinding, KvLimits, KvService, SqlBinding,
    SqlLimits, SqlOperation, SqlParameter, SqlService, SqlStatement,
};
use hyperlight_unikraft::workerd::{
    ExecutionProfile, FetchBroker, Header, ModuleType, PROTOCOL_VERSION, QueueMessage,
    QueueMetadata, QueueRequest, RequestEnvelope, ScheduledRequest, StoragePolicy, TimerLimits,
    WorkerBinding, WorkerBindingKind, WorkerBundle, WorkerCapabilityPolicy, WorkerModule,
    WorkerVersionId, WorkerVersionSandbox,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_ROOTFS: &str = "build-elfloader/workerd-executor/rootfs.img";
const DEFAULT_EXECUTOR: &str = "build-elfloader/workerd-executor/executor";
const DEFAULT_SCRATCH_MIB: usize = 344;
const TIMEOUT: Duration = Duration::from_secs(30);

const DATA_WORKER: &str = r#"
export default {
  async fetch(request, env) {
    const input = await request.json();
    const { kind, ...operationFields } = input.operation;
    const response = await env[input.binding].fetch("https://logical.invalid/", {
      method: "POST",
      body: JSON.stringify({
        version: 2,
        request_id: input.request_id,
        binding: input.binding,
        operation: { kind, ...operationFields },
      }),
    });
    return new Response(await response.text(), {
      status: response.status,
      headers: { "content-type": "application/json" },
    });
  },
};
"#;

const INGRESS_WORKER: &str = r#"
export default {
  scheduled(controller) {
    if (controller.cron !== "0 0 * * *") {
      throw new Error(`unexpected cron ${controller.cron}`);
    }
  },
  queue(batch) {
    batch.messages[0].ack();
    batch.messages[1].retry({ delaySeconds: 7 });
  },
};
"#;

#[derive(Serialize)]
struct ProofEvidence {
    schema_version: u16,
    runtime: &'static str,
    worker_version: String,
    scheduled_input: Value,
    scheduled: Value,
    queue_input: Value,
    queue: Value,
    node: Value,
    policy: Value,
    kv: Value,
    cache: Value,
    sql: Value,
    durable_object: Value,
}

struct DurableProofService {
    service: DurableObjectService,
    context: DurableDeliveryContext,
}

impl DurableProofService {
    fn new(
        identity: RequestIdentity,
        backing: &PathBuf,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let namespace = ActorNamespace::new("rooms")?;
        let binding = DurableObjectBinding::persistent(
            "rooms",
            namespace.clone(),
            backing,
            DurableObjectLimits::default(),
        )?;
        let route = ActorRoute::new(
            ActorId::new(namespace, ActorKey::new("room-7")?),
            "cell-a",
            1,
        )?;
        let context = DurableDeliveryContext::new(route, identity, ActorLifecycle::Deliver)?;
        let mut service = DurableObjectService::new(binding)?;
        service.begin_delivery(context.clone())?;
        Ok(Self { service, context })
    }
}

impl LogicalWireService for DurableProofService {
    fn inspect(&self, payload: &[u8]) -> Result<LogicalInvocation, LogicalWireError> {
        self.service.inspect(payload)
    }

    fn dispatch(&mut self, identity: &RequestIdentity, payload: &[u8]) -> Vec<u8> {
        self.service.dispatch(identity, payload)
    }

    fn reject(&self, status: LogicalRuntimeStatus, code: &'static str) -> Vec<u8> {
        self.service.reject(status, code)
    }

    fn reset_for_fresh_vm(&mut self) -> Result<(), BrokerHostError> {
        self.service.reset_for_fresh_vm()?;
        self.service
            .begin_delivery(self.context.clone())
            .map_err(|_| BrokerHostError::new("durable_context"))
    }
}

fn request(id: &str, body: Value) -> Result<RequestEnvelope, Box<dyn std::error::Error>> {
    Ok(RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: id.into(),
        method: "POST".into(),
        url: "https://capability.test/".into(),
        headers: vec![Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
        body_base64: STANDARD.encode(serde_json::to_vec(&body)?),
    })
}

fn execute_profiled(
    worker: &WorkerVersionSandbox,
    version: &WorkerVersionId,
    id: &str,
    binding: &str,
    operation: Value,
) -> Result<(Value, ExecutionProfile), Box<dyn std::error::Error>> {
    let (response, profile) = worker.execute_profiled(
        version,
        request(
            id,
            json!({
                "request_id": id,
                "binding": binding,
                "operation": operation,
            }),
        )?,
        TIMEOUT,
    );
    let response = response?;
    if response.status != 200 {
        let body = STANDARD
            .decode(&response.body_base64)
            .ok()
            .and_then(|body| String::from_utf8(body).ok())
            .unwrap_or_else(|| "<non-UTF-8 response body>".into());
        return Err(format!("{id} returned HTTP {}: {body}", response.status).into());
    }
    Ok((
        serde_json::from_slice(&STANDARD.decode(response.body_base64)?)?,
        profile,
    ))
}

fn sql_batch_operation(statements: Vec<SqlStatement>) -> Result<Value, serde_json::Error> {
    serde_json::to_value(SqlOperation::Batch { statements })
}

fn data_worker(
    rootfs: &PathBuf,
    executor: &PathBuf,
    scratch_mib: usize,
    kv_path: &PathBuf,
    cache_path: &PathBuf,
    sql_path: &PathBuf,
    durable_path: &PathBuf,
) -> Result<(WorkerVersionSandbox, WorkerVersionId, BrokerRuntime), Box<dyn std::error::Error>> {
    let identity = RequestIdentity::new("capability-proof", "snapshot-1", 0)?;
    let router = LogicalServiceRouter::new()
        .with_service(
            "settings",
            KvService::new([KvBinding::persistent(
                "settings",
                kv_path,
                false,
                KvLimits {
                    max_operations: 2,
                    ..KvLimits::default()
                },
            )?])?,
        )?
        .with_service(
            "assets",
            CacheService::new([CacheBinding::persistent(
                "assets",
                cache_path,
                false,
                CacheLimits::default(),
            )?])?,
        )?
        .with_service(
            "database",
            SqlService::new([SqlBinding::persistent(
                "database",
                sql_path,
                false,
                SqlLimits::default(),
            )?])?,
        )?
        .with_service(
            "rooms",
            DurableProofService::new(identity.clone(), durable_path)?,
        )?;
    let runtime = BrokerRuntime::deny_all(identity).with_logical(
        [
            "settings".to_string(),
            "assets".to_string(),
            "database".to_string(),
            "rooms".to_string(),
        ],
        router,
    )?;
    let evidence_runtime = runtime.clone();
    let bindings = vec![
        WorkerBinding {
            name: "assets".into(),
            kind: WorkerBindingKind::Cache,
        },
        WorkerBinding::sql("database"),
        WorkerBinding {
            name: "rooms".into(),
            kind: WorkerBindingKind::DurableObject,
        },
        WorkerBinding {
            name: "settings".into(),
            kind: WorkerBindingKind::Kv,
        },
    ];
    let policy = WorkerCapabilityPolicy::new(
        FetchBroker::denied(),
        TimerLimits::default(),
        StoragePolicy::denied(),
    )
    .with_broker_runtime(runtime, bindings)?;
    let bundle = WorkerBundle::single_script(
        WorkerVersionId::new("capability-data-v1")?,
        "2025-12-31",
        "worker.js",
        DATA_WORKER,
    )?;
    let version = bundle.worker_version.clone();
    let worker = WorkerVersionSandbox::initialize_with_policy(
        bundle,
        rootfs,
        executor,
        scratch_mib,
        Duration::from_secs(90),
        policy,
    )?;
    Ok((worker, version, evidence_runtime))
}

fn run_sql_proof(
    rootfs: &PathBuf,
    executor: &PathBuf,
    scratch_mib: usize,
    state_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let kv_path = state_dir.join("kv.sqlite");
    let cache_path = state_dir.join("cache.sqlite");
    let sql_path = state_dir.join("sql.sqlite");
    let durable_path = state_dir.join("durable.sqlite");
    let (worker, version, _) = data_worker(
        rootfs,
        executor,
        scratch_mib,
        &kv_path,
        &cache_path,
        &sql_path,
        &durable_path,
    )?;
    let (batch, batch_profile) = execute_profiled(
        &worker,
        &version,
        "sql-batch",
        "database",
        sql_batch_operation(vec![
            SqlStatement {
                sql: "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: Vec::new(),
            },
            SqlStatement {
                sql: "INSERT INTO users(id, name) VALUES (?1, ?2)".into(),
                parameters: vec![SqlParameter::Integer(1), SqlParameter::Text("Ada".into())],
            },
        ])?,
    )?;
    let (query, query_profile) = execute_profiled(
        &worker,
        &version,
        "sql-query",
        "database",
        sql_batch_operation(vec![SqlStatement {
            sql: "SELECT id, name FROM users ORDER BY id".into(),
            parameters: Vec::new(),
        }])?,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version":1,
            "runtime":"workerd-on-hyperlight",
            "worker_version":version.as_str(),
            "sql":{
                "backing_file":std::fs::canonicalize(&sql_path)?,
                "statements":[
                    "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    "INSERT INTO users(id, name) VALUES (1, 'Ada')",
                    "SELECT id, name FROM users ORDER BY id"
                ],
                "batch":batch,
                "batch_vm_profile":batch_profile,
                "query_after_fresh_vm":query,
                "query_vm_profile":query_profile
            }
        }))?
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let rootfs = PathBuf::from(args.next().unwrap_or_else(|| DEFAULT_ROOTFS.into()));
    let executor = PathBuf::from(args.next().unwrap_or_else(|| DEFAULT_EXECUTOR.into()));
    let scratch_mib = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_SCRATCH_MIB);
    let state_dir = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "demo-output/capability-state".into()),
    );
    let scope = args.next().unwrap_or_else(|| "all".into());
    if args.next().is_some() {
        return Err(
            "usage: workerd-capability-proof [ROOTFS] [EXECUTOR] [SCRATCH_MIB] [STATE_DIR] [all|sql]"
                .into(),
        );
    }
    std::fs::create_dir_all(&state_dir)?;
    if scope == "sql" {
        return run_sql_proof(&rootfs, &executor, scratch_mib, &state_dir);
    }
    if scope != "all" {
        return Err(format!("unsupported proof scope: {scope}").into());
    }
    let kv_path = state_dir.join("kv.sqlite");
    let cache_path = state_dir.join("cache.sqlite");
    let sql_path = state_dir.join("sql.sqlite");
    let durable_path = state_dir.join("durable.sqlite");

    let ingress_bundle = WorkerBundle::single_script(
        WorkerVersionId::new("capability-ingress-v1")?,
        "2025-12-31",
        "worker.js",
        INGRESS_WORKER,
    )?;
    let ingress_version = ingress_bundle.worker_version.clone();
    let ingress = WorkerVersionSandbox::initialize(
        ingress_bundle,
        &rootfs,
        &executor,
        scratch_mib,
        Duration::from_secs(90),
    )?;
    let scheduled_input = ScheduledRequest {
        protocol_version: PROTOCOL_VERSION,
        request_id: "scheduled-proof".into(),
        scheduled_time_unix_ms: 1_767_225_600_000,
        cron: "0 0 * * *".into(),
    };
    let scheduled =
        ingress.execute_scheduled(&ingress_version, scheduled_input.clone(), TIMEOUT)?;
    let queue_input = QueueRequest {
        protocol_version: PROTOCOL_VERSION,
        request_id: "queue-proof".into(),
        queue: "jobs".into(),
        messages: vec![
            QueueMessage {
                id: "message-1".into(),
                timestamp_unix_ms: 1_767_225_600_000,
                body_base64: STANDARD.encode("one"),
                content_type: Some("text".into()),
                attempts: 1,
            },
            QueueMessage {
                id: "message-2".into(),
                timestamp_unix_ms: 1_767_225_601_000,
                body_base64: STANDARD.encode("two"),
                content_type: Some("text".into()),
                attempts: 2,
            },
        ],
        metadata: QueueMetadata {
            backlog_count: 2.0,
            backlog_bytes: 6.0,
            oldest_message_timestamp_unix_ms: Some(1_767_225_600_000),
        },
    };
    let queue = ingress.execute_queue(&ingress_version, queue_input.clone(), TIMEOUT)?;

    let node_bundle = WorkerBundle::from_json(
        WorkerBundle {
            protocol_version: PROTOCOL_VERSION,
            worker_version: WorkerVersionId::new("capability-node-v1")?,
            compatibility_date: "2025-12-31".into(),
            compatibility_flags: vec!["nodejs_compat".into()],
            main_module: "worker.js".into(),
            modules: vec![
                WorkerModule {
                    name: "worker.js".into(),
                    module_type: ModuleType::EsModule,
                    source: r#"
import legacy from "legacy.cjs";
import { Buffer } from "node:buffer";
export default {
  fetch() {
    return Response.json({
      commonjs: legacy.answer,
      bufferHex: Buffer.from("hyperlight").toString("hex"),
    });
  },
};
"#
                    .into(),
                },
                WorkerModule {
                    name: "legacy.cjs".into(),
                    module_type: ModuleType::CommonJsModule,
                    source: "module.exports = { answer: 42 };".into(),
                },
            ],
        }
        .to_canonical_json()?
        .as_bytes(),
    )?;
    let node_version = node_bundle.worker_version.clone();
    let node_worker = WorkerVersionSandbox::initialize(
        node_bundle,
        &rootfs,
        &executor,
        scratch_mib,
        Duration::from_secs(90),
    )?;
    let node_response = node_worker.execute(
        &node_version,
        RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: "node-proof".into(),
            method: "GET".into(),
            url: "https://node.test/".into(),
            headers: Vec::new(),
            body_base64: String::new(),
        },
        TIMEOUT,
    )?;
    let node: Value = serde_json::from_slice(&STANDARD.decode(node_response.body_base64)?)?;

    let (data, data_version, policy_runtime) = data_worker(
        &rootfs,
        &executor,
        scratch_mib,
        &kv_path,
        &cache_path,
        &sql_path,
        &durable_path,
    )?;
    let (kv_put, kv_put_profile) = execute_profiled(
        &data,
        &data_version,
        "kv-put",
        "settings",
        json!({"kind":"kv_put","key":"theme","value_base64":STANDARD.encode("dark")}),
    )?;
    let (kv_get, kv_get_profile) = execute_profiled(
        &data,
        &data_version,
        "kv-get",
        "settings",
        json!({"kind":"kv_get","key":"theme"}),
    )?;
    let (cache_put, cache_put_profile) = execute_profiled(
        &data,
        &data_version,
        "cache-put",
        "assets",
        json!({
            "kind":"cache_put",
            "key":"https://example.test/app.js",
            "status":200,
            "headers":[{"name":"content-type","value":"text/javascript"}],
            "body_base64":STANDARD.encode("console.log(1)"),
            "ttl_ms":60000
        }),
    )?;
    let (cache_match, cache_match_profile) = execute_profiled(
        &data,
        &data_version,
        "cache-match",
        "assets",
        json!({"kind":"cache_match","key":"https://example.test/app.js"}),
    )?;
    let (sql_batch, sql_batch_profile) = execute_profiled(
        &data,
        &data_version,
        "sql-batch",
        "database",
        sql_batch_operation(vec![
            SqlStatement {
                sql: "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: Vec::new(),
            },
            SqlStatement {
                sql: "INSERT INTO users(id, name) VALUES (?1, ?2)".into(),
                parameters: vec![SqlParameter::Integer(1), SqlParameter::Text("Ada".into())],
            },
        ])?,
    )?;
    let (sql_query, sql_query_profile) = execute_profiled(
        &data,
        &data_version,
        "sql-query",
        "database",
        sql_batch_operation(vec![SqlStatement {
            sql: "SELECT id, name FROM users ORDER BY id".into(),
            parameters: Vec::new(),
        }])?,
    )?;
    let (durable_put, durable_put_profile) = execute_profiled(
        &data,
        &data_version,
        "do-put",
        "rooms",
        json!({"kind":"do_storage_put","key":"topic","value_base64":STANDARD.encode("hyperlight")}),
    )?;
    let (durable_get, durable_get_profile) = execute_profiled(
        &data,
        &data_version,
        "do-get",
        "rooms",
        json!({"kind":"do_storage_get","key":"topic"}),
    )?;
    let logical_request = |id: &str, binding: &str| {
        serde_json::to_vec(&json!({
            "version":2,
            "request_id":id,
            "binding":binding,
            "operation":{"kind":"kv_get","key":"theme"}
        }))
        .unwrap()
    };
    let wrong_identity = RequestIdentity::new("other-worker", "snapshot-1", 0)?;
    let identity_denied: Value = serde_json::from_slice(&policy_runtime.dispatch_logical_as(
        &wrong_identity,
        &logical_request("identity-denied", "settings"),
    ))?;
    let binding_denied: Value = serde_json::from_slice(
        &policy_runtime.dispatch_logical(&logical_request("binding-denied", "unknown")),
    )?;
    policy_runtime.reset_for_fresh_vm()?;
    let first = policy_runtime.dispatch_logical(&logical_request("quota-1", "settings"));
    let second = policy_runtime.dispatch_logical(&logical_request("quota-2", "settings"));
    let quota_denied: Value = serde_json::from_slice(
        &policy_runtime.dispatch_logical(&logical_request("quota-3", "settings")),
    )?;

    let evidence = ProofEvidence {
        schema_version: 1,
        runtime: "workerd-on-hyperlight",
        worker_version: data_version.as_str().into(),
        scheduled_input: serde_json::to_value(scheduled_input)?,
        scheduled: serde_json::to_value(scheduled)?,
        queue_input: serde_json::to_value(queue_input)?,
        queue: serde_json::to_value(queue)?,
        node,
        policy: json!({
            "trusted_identity":"capability-proof/snapshot-1/0",
            "identity_denied":identity_denied,
            "binding_denied":binding_denied,
            "quota_denied":quota_denied,
            "successful_operations_before_quota":[
                serde_json::from_slice::<Value>(&first)?,
                serde_json::from_slice::<Value>(&second)?
            ]
        }),
        kv: json!({
            "backing_file":std::fs::canonicalize(&kv_path)?,
            "put":kv_put,
            "put_vm_profile":kv_put_profile,
            "get_after_fresh_vm":kv_get,
            "get_vm_profile":kv_get_profile
        }),
        cache: json!({
            "backing_file":std::fs::canonicalize(&cache_path)?,
            "put":cache_put,
            "put_vm_profile":cache_put_profile,
            "match_after_fresh_vm":cache_match,
            "match_vm_profile":cache_match_profile
        }),
        sql: json!({
            "backing_file":std::fs::canonicalize(&sql_path)?,
            "statements":[
                "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                "INSERT INTO users(id, name) VALUES (1, 'Ada')",
                "SELECT id, name FROM users ORDER BY id"
            ],
            "batch":sql_batch,
            "batch_vm_profile":sql_batch_profile,
            "query_after_fresh_vm":sql_query,
            "query_vm_profile":sql_query_profile
        }),
        durable_object: json!({
            "backing_file":std::fs::canonicalize(&durable_path)?,
            "put":durable_put,
            "put_vm_profile":durable_put_profile,
            "get_after_fresh_vm":durable_get,
            "get_vm_profile":durable_get_profile
        }),
    };
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    Ok(())
}
