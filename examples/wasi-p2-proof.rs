// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use hyperlight_unikraft::wasi_preview2::{
    AdapterError, CliAdapter, CliPolicy, ClockAdapter, ClockBackend, ClockPolicy, HttpAdapter,
    IncomingHttpHandler, InputStream, OutputStream, P2AdapterRegistry, P2Policy, RandomAdapter,
    RandomPolicy, SecureRandom,
};
use hyperlight_unikraft::workerd::{FetchBroker, RequestEnvelope, ResponseEnvelope};
use serde_json::json;
use std::collections::BTreeMap;

struct FixedClock;

impl ClockBackend for FixedClock {
    fn wall_clock_ns(&self) -> Result<u64, AdapterError> {
        Ok(99)
    }

    fn monotonic_clock_ns(&self) -> Result<u64, AdapterError> {
        Ok(100)
    }
}

struct FixedRandom(u8);

impl SecureRandom for FixedRandom {
    fn fill_secure(&mut self, output: &mut [u8]) -> Result<(), AdapterError> {
        output.fill(self.0);
        Ok(())
    }
}

struct EchoIngress;

impl IncomingHttpHandler for EchoIngress {
    fn handle(&mut self, request: RequestEnvelope) -> Result<ResponseEnvelope, AdapterError> {
        Ok(ResponseEnvelope {
            protocol_version: request.protocol_version,
            request_id: request.request_id,
            status: 204,
            headers: Vec::new(),
            body_base64: String::new(),
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let denied = P2Policy::deny_all();
    let registry: P2AdapterRegistry<'_, FixedClock, FixedRandom> = P2AdapterRegistry::new();
    let Err(unknown_import) = registry.plan(&denied, ["wasi:unknown/ambient@0.2.12"]) else {
        return Err("unknown import was unexpectedly allowed".into());
    };
    let Err(udp) = registry.plan(&denied, ["wasi:sockets/udp@0.2.12"]) else {
        return Err("UDP import was unexpectedly allowed".into());
    };

    let mut http = HttpAdapter::new(FetchBroker::denied()).with_incoming_handler(EchoIngress);
    let response = http.handle_incoming(RequestEnvelope {
        protocol_version: 1,
        request_id: "request-1".into(),
        method: "GET".into(),
        url: "https://worker.invalid/".into(),
        headers: Vec::new(),
        body_base64: String::new(),
    })?;

    let mut input = InputStream::new([1, 2, 3], 4, 2)?;
    let input_chunk = input.read(8)?;
    input.close();
    let closed_read = input.read(1).unwrap_err();

    let mut output = OutputStream::new(3, 2)?;
    let first_write = output.write(&[1, 2, 3])?;
    let quota_write = output.write(&[3, 4]).unwrap_err();

    let mut clock = ClockAdapter::new(
        ClockPolicy::Deterministic {
            wall_epoch_ns: 42,
            monotonic_step_ns: 5,
            timezone: Some("UTC".into()),
        },
        FixedClock,
    );
    let wall_ns = clock.wall_clock_ns()?;
    let monotonic = [clock.monotonic_clock_ns()?, clock.monotonic_clock_ns()?];

    let mut random = RandomAdapter::new(
        RandomPolicy::SecureAndDeterministicInsecure {
            max_secure_bytes: 4,
            insecure_seed: [9; 32],
        },
        FixedRandom(0xa5),
    );
    let secure = random.secure_bytes(4)?;
    let random_quota = random.secure_bytes(1).unwrap_err();

    let cli_policy = CliPolicy::new(
        BTreeMap::from([("LANG".into(), "C.UTF-8".into())]),
        vec!["component".into()],
        16,
        16,
        16,
        true,
        true,
    )?;
    let mut cli = CliAdapter::new(&cli_policy, vec![1, 2])?;
    cli.claim_run()?;
    let duplicate_run = cli.claim_run().unwrap_err();
    let stdout_bytes = cli.stdout().write(&[0; 32])?;
    cli.exit(true)?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "proof_path": "executable typed host-interface contract",
            "http": {
                "request_id": response.request_id,
                "status": response.status
            },
            "streams": {
                "input_chunk": input_chunk,
                "closed_read": format!("{closed_read:?}"),
                "first_write_bytes": first_write,
                "quota_write": format!("{quota_write:?}")
            },
            "clock": {
                "wall_ns": wall_ns,
                "monotonic_ns": monotonic,
                "timezone": clock.timezone()?
            },
            "random": {
                "secure_bytes": secure,
                "quota": format!("{random_quota:?}")
            },
            "cli": {
                "environment": cli.environment(),
                "arguments": cli.arguments(),
                "stdout_bytes": stdout_bytes,
                "duplicate_run": format!("{duplicate_run:?}"),
                "exit_status": format!("{:?}", cli.exit_status())
            },
            "denials": {
                "unknown_import": format!("{unknown_import:?}"),
                "udp": format!("{udp:?}")
            }
        }))?
    );
    Ok(())
}
