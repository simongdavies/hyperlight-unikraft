// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const SCRIPT: &str = r#"
let calls=0;
const assert=(value,message)=>{if(!value)throw new Error(message)};
export default {async fetch(request,env){
 calls++;
 const path=new URL(request.url).pathname;
 if(path==='/simple')return Response.json(await env.DB.prepare('SELECT 42 AS value').all());
 assert(typeof env.DB.prepare==='function','missing native SQL API');
 assert(typeof env.DB.fetch==='undefined','private Fetcher exposed');
 assert(typeof env.store.fetch==='function','legacy Fetcher missing');
 assert(env.db===undefined,'binding case folded');
 if(path==='/retained')return Response.json({calls,value:await env.DB.prepare('SELECT t FROM records').first('t')});
 if(path==='/read-only'){
  let denied=false;try{await env.DB.prepare('INSERT INTO records(i) VALUES(99)').run()}catch{denied=true}
  return Response.json({denied,count:await env.DB.prepare('SELECT COUNT(*) AS n FROM records').first('n')});
 }
 await env.DB.prepare('CREATE TABLE IF NOT EXISTS records(i INTEGER,r REAL,t TEXT,b BLOB)').run();
 await env.DB.prepare('DELETE FROM records').run();
 const inserted=await env.DB.prepare('INSERT INTO records(i,r,t,b) VALUES(?,?,?,?) RETURNING i')
   .bind(7,1.25,'héllo',new Uint8Array([0,255,65])).run();
 assert(Array.isArray(inserted.results)&&inserted.results[0].i===7,'run RETURNING must return object rows');
 assert(inserted.meta.changes===1&&Number.isFinite(inserted.meta.duration),'unmeasured statement metadata');
 for(const name of ['rows_read','rows_written','changed_db','size_after'])
  assert(!Object.hasOwn(inserted.meta,name),'invented metadata '+name);
 const selected=await env.DB.prepare('SELECT i,r,t,b,NULL AS n FROM records').all();
 const row=selected.results[0];
 assert(row.i===7&&row.r===1.25&&row.t==='héllo'&&row.n===null,'SQL value fidelity');
 assert(Array.from(row.b).join(',')==='0,255,65','blob bytes differ');
 assert(selected.meta.changes===0&&!Object.hasOwn(selected.meta,'last_row_id'),'SELECT leaked previous mutation metadata');
 assert(await env.DB.prepare('SELECT t FROM records').first('t')==='héllo','first');
 const raw=await env.DB.prepare('SELECT i,t FROM records').raw({columnNames:true});
 assert(raw[0].join(',')==='i,t'&&raw[1][0]===7,'raw');
 const batch=await env.DB.batch([
  env.DB.prepare('SELECT ? AS value').bind(11),
  env.DB.prepare('SELECT ? AS value').bind(12)]);
 assert(batch.length===2&&batch[0].results[0].value===11&&batch[1].results[0].value===12,'batch');
 const execution=await env.DB.exec('SELECT 1;\nSELECT 2;');
 assert(execution.count===2&&Number.isFinite(execution.duration),'exec duration');
 const legacy=await (await env.store.fetch('https://logical.invalid/',{method:'POST',
  body:JSON.stringify({version:2,request_id:'legacy-query',binding:'store',
   operation:{kind:'d1_batch',statements:[{sql:'SELECT 9 AS value',parameters:[]}]}})})).json();
 assert(legacy.status==='ok'&&legacy.results[0].rows[0][0].value===9,'legacy query');
 assert(!Object.hasOwn(legacy.results[0],'meta'),'legacy response gained metadata');
 let denied=false;try{
  await env.store.fetch('https://logical.invalid/',{method:'POST',
   body:JSON.stringify({version:2,request_id:'wrong-binding',binding:'DB',
    operation:{kind:'d1_batch',statements:[{sql:'DELETE FROM records',parameters:[]}]}})});
 }catch{denied=true}
 assert(denied,'legacy binding escalated into DB');
 const count=()=>env.DB.prepare('SELECT COUNT(*) AS n FROM records').first('n');
 for(const sql of [
  'INSERT INTO records(i) VALUES(9007199254740992) RETURNING i',
  'INSERT INTO records(i) VALUES(99) RETURNING randomblob(20000) AS b',
  'INSERT INTO records(i) VALUES '+Array(70).fill('(99)').join(',')+' RETURNING i AS "'+'column'.repeat(171)+'"'
 ]){
  let rejected=false;try{await env.DB.prepare(sql).run()}catch{rejected=true}
  assert(rejected&&await count()===1,'unsafe result or native expansion committed mutation');
 }
 let rolledBack=false;try{
  await env.DB.batch([env.DB.prepare('INSERT INTO records(i) VALUES(99)'),env.DB.prepare('SELECT missing FROM records')]);
 }catch{rolledBack=true}
 assert(rolledBack&&await count()===1,'failed batch was not atomic');
 return Response.json({ok:true,calls,changes:inserted.meta.changes});
}};
"#;

fn request(id: &str, path: &str) -> RequestEnvelope {
    RequestEnvelope {
        protocol_version: 1,
        request_id: id.into(),
        method: "GET".into(),
        url: format!("https://example.test{path}"),
        headers: vec![],
        body_base64: String::new(),
    }
}

fn body(response: ResponseEnvelope) -> serde_json::Value {
    assert_eq!(response.status, 200);
    serde_json::from_slice(&STANDARD.decode(response.body_base64).unwrap()).unwrap()
}

fn policy(directory: &Path, version: &WorkerVersionId, read_only: bool) -> WorkerCapabilityPolicy {
    WorkerCapabilityPolicy::default()
        .with_logical_bindings(
            LogicalBindingsPolicy::new(
                "standard-sql",
                version.as_str(),
                vec![
                    LogicalBindingConfig::D1 {
                        name: "DB".into(),
                        api: Some(WorkerBindingApi::D1),
                        backing_path: directory.join("native.sqlite"),
                        read_only,
                        limits: Default::default(),
                    },
                    LogicalBindingConfig::D1 {
                        name: "store".into(),
                        api: None,
                        backing_path: directory.join("legacy.sqlite"),
                        read_only: false,
                        limits: Default::default(),
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap()
}

#[test]
#[ignore = "requires corrected standard SQL guest and native KVM"]
fn actual_standard_sql_api_and_legacy_fetcher_are_isolated_in_both_modes() {
    let executor =
        PathBuf::from(std::env::var("WORKERD_REAL_EXECUTOR").expect("set exact SQL executor"));
    let rootfs = PathBuf::from(std::env::var("WORKERD_REAL_ROOTFS").expect("set exact SQL rootfs"));
    for protocol in [PROTOCOL_VERSION, PACKAGE_PROTOCOL_VERSION] {
        let directory = tempfile::tempdir().unwrap();
        let version = WorkerVersionId::new(format!("standard-sql-{protocol}")).unwrap();
        let mut bundle =
            WorkerBundle::single_script(version.clone(), "2025-01-01", "worker.js", SCRIPT)
                .unwrap();
        bundle.protocol_version = protocol;
        let worker = WorkerVersionSandbox::initialize_with_policy(
            bundle.clone(),
            &rootfs,
            &executor,
            512,
            Duration::from_secs(30),
            policy(directory.path(), &version, false),
        )
        .unwrap();
        worker
            .negotiate_extensions(
                &["binding-d1-api-v1", "invocation-budget-v2"],
                Duration::from_secs(10),
            )
            .unwrap();
        let simple = worker.execute(
            &version,
            request("sql-simple", "/simple"),
            Duration::from_secs(10),
        );
        assert_eq!(body(simple.unwrap())["results"][0]["value"], 42);
        let disposable = worker
            .execute(
                &version,
                request("sql-disposable", "/probe"),
                Duration::from_secs(20),
            )
            .unwrap();
        assert_eq!(
            body(disposable),
            serde_json::json!({"ok":true,"calls":1,"changes":1})
        );
        let mut resident = worker.restore_resident().unwrap();
        let response = resident
            .execute(request("sql-resident", "/probe"), Duration::from_secs(20))
            .0
            .unwrap();
        assert_eq!(
            body(response),
            serde_json::json!({"ok":true,"calls":1,"changes":1})
        );
        resident
            .checkpoint(&worker, "sql-safe-point", Duration::from_secs(5))
            .unwrap();
        drop(resident);
        let store = Arc::new(
            CheckpointStore::open(
                directory.path().join("checkpoints.db"),
                &[9; 32],
                1024 * 1024 * 1024,
            )
            .unwrap(),
        );
        let mut instance =
            ResidentInstance::create(worker.clone(), CheckpointPolicy::Durable, Some(store), None)
                .unwrap();
        let generation = instance.identity().generation;
        let InvocationResponse::Fetch(response) = instance
            .invoke(
                generation,
                request("sql-before-park", "/probe").into(),
                Duration::from_secs(20),
                InvocationCancellation::default(),
            )
            .0
            .unwrap()
        else {
            panic!("wrong invocation response")
        };
        assert_eq!(body(response)["calls"], 1);
        instance.park(generation, Duration::from_secs(10)).unwrap();
        instance.resume(generation).unwrap();
        assert_eq!(instance.identity().generation, generation + 1);
        let InvocationResponse::Fetch(response) = instance
            .invoke(
                generation + 1,
                request("sql-after-resume", "/retained").into(),
                Duration::from_secs(10),
                InvocationCancellation::default(),
            )
            .0
            .unwrap()
        else {
            panic!("wrong invocation response")
        };
        assert_eq!(
            body(response),
            serde_json::json!({"calls":2,"value":"héllo"})
        );
        let readonly = WorkerVersionSandbox::initialize_with_policy(
            bundle,
            &rootfs,
            &executor,
            512,
            Duration::from_secs(30),
            policy(directory.path(), &version, true),
        )
        .unwrap();
        let response = readonly
            .execute(
                &version,
                request("sql-read-only", "/read-only"),
                Duration::from_secs(10),
            )
            .unwrap();
        assert_eq!(body(response), serde_json::json!({"denied":true,"count":1}));
    }
}
