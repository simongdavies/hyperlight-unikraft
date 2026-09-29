// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    Header, PROTOCOL_VERSION, RequestEnvelope, WorkerBundle, WorkerVersionSandbox,
};
use serde::Serialize;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Serialize)]
struct Result {
    bundle_sha256: String,
    worker_version: String,
    status: u16,
    headers: Vec<hyperlight_unikraft::workerd::Header>,
    body: String,
    profile: hyperlight_unikraft::workerd::ExecutionProfile,
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bundle_path = PathBuf::from(args.next().ok_or(
        "usage: workerd-bundle-probe BUNDLE_JSON [URL] [SCRATCH_MIB] \
                 [METHOD] [HEADER_NAME:VALUE]",
    )?);
    let url = args
        .next()
        .unwrap_or_else(|| "https://example.test/".into());
    let scratch_mb = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(512);
    let method = args.next().unwrap_or_else(|| "GET".into());
    let headers = args
        .next()
        .map(|header| {
            let (name, value) = header.split_once(':').ok_or("header must use NAME:VALUE")?;
            Ok::<_, Box<dyn std::error::Error>>(vec![Header {
                name: name.into(),
                value: value.into(),
            }])
        })
        .transpose()?
        .unwrap_or_default();
    if args.next().is_some() {
        return Err("too many arguments".into());
    }
    let bundle = WorkerBundle::from_path(bundle_path)?;
    let bundle_sha256 = bundle.sha256()?;
    let version = bundle.worker_version.clone();
    let mut worker = WorkerVersionSandbox::initialize(
        bundle,
        "build-elfloader/workerd-executor/rootfs.img",
        "build-elfloader/workerd-executor/executor",
        scratch_mb,
        Duration::from_secs(90),
    )?;
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: "probe-1".into(),
        method,
        url,
        headers,
        body_base64: String::new(),
    };
    let (response, profile) = worker.execute_profiled(&version, request, Duration::from_secs(10));
    let response = response?;
    println!(
        "{}",
        serde_json::to_string_pretty(&Result {
            bundle_sha256,
            worker_version: version.as_str().into(),
            status: response.status,
            headers: response.headers,
            body: String::from_utf8(STANDARD.decode(response.body_base64)?)?,
            profile,
        })?
    );
    Ok(())
}
