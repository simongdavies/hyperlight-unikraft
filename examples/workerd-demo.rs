// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::{
    Header, MAX_BODY_BYTES, MAX_HEADER_BYTES, PROTOCOL_VERSION, RequestEnvelope, WorkerBundle,
    WorkerVersionId, WorkerVersionSandbox,
};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const DEFAULT_ROOTFS: &str = "build-elfloader/workerd-executor/rootfs.img";
const DEFAULT_EXECUTOR: &str = "build-elfloader/workerd-executor/executor";
const DEFAULT_BUNDLE: &str = "examples/workerd-bundles/helloworld_esm.json";
const MAX_REQUEST_HEAD_BYTES: usize = MAX_HEADER_BYTES + 8 * 1024;

struct Options {
    bind: String,
    rootfs: PathBuf,
    executor: PathBuf,
    bundle: Option<PathBuf>,
    script: Option<PathBuf>,
    version: WorkerVersionId,
    compatibility_date: String,
    scratch_mb: usize,
    init_timeout: Duration,
    request_timeout: Duration,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            bind: "0.0.0.0:8787".into(),
            rootfs: DEFAULT_ROOTFS.into(),
            executor: DEFAULT_EXECUTOR.into(),
            bundle: Some(DEFAULT_BUNDLE.into()),
            script: None,
            version: WorkerVersionId::new("demo-v1").map_err(|e| e.to_string())?,
            compatibility_date: "2025-01-01".into(),
            scratch_mb: 512,
            init_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(2),
        };
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = || {
                args.next()
                    .ok_or_else(|| format!("missing value for {arg}"))
            };
            match arg.as_str() {
                "--bind" => options.bind = value()?,
                "--rootfs" => options.rootfs = value()?.into(),
                "--executor" => options.executor = value()?.into(),
                "--bundle" => {
                    options.bundle = Some(value()?.into());
                    options.script = None;
                }
                "--script" => {
                    options.script = Some(value()?.into());
                    options.bundle = None;
                }
                "--version" => {
                    options.version = WorkerVersionId::new(value()?).map_err(|e| e.to_string())?
                }
                "--compatibility-date" => options.compatibility_date = value()?,
                "--scratch-mb" => {
                    options.scratch_mb = value()?
                        .parse()
                        .map_err(|_| "invalid --scratch-mb".to_string())?
                }
                "--init-timeout-ms" => {
                    options.init_timeout = duration(value()?, "--init-timeout-ms")?
                }
                "--request-timeout-ms" => {
                    options.request_timeout = duration(value()?, "--request-timeout-ms")?
                }
                "--help" | "-h" => {
                    return Err(format!(
                        "usage: workerd-demo [--bind ADDR] [--rootfs CPIO] \
                         [--executor ELF] [--version ID] [--scratch-mb MIB] \
                         [--bundle JSON | --script JS] [--compatibility-date YYYY-MM-DD] \
                         [--init-timeout-ms MS] [--request-timeout-ms MS]\n\
                         defaults: --bind 0.0.0.0:8787 --rootfs {DEFAULT_ROOTFS} \
                         --executor {DEFAULT_EXECUTOR} --version demo-v1 \
                         --bundle {DEFAULT_BUNDLE} \
                         --scratch-mb 512 --init-timeout-ms 30000 \
                         --request-timeout-ms 2000"
                    ));
                }
                _ => return Err(format!("unknown argument: {arg}")),
            }
        }
        Ok(options)
    }
}

fn duration(value: String, flag: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<u64>()
        .map_err(|_| format!("invalid {flag}"))?;
    if millis == 0 {
        return Err(format!("{flag} must be nonzero"));
    }
    Ok(Duration::from_millis(millis))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = match Options::parse() {
        Ok(options) => options,
        Err(message) if env::args().any(|arg| arg == "--help" || arg == "-h") => {
            println!("{message}");
            return Ok(());
        }
        Err(message) => return Err(message.into()),
    };
    let bundle = if let Some(path) = &options.bundle {
        WorkerBundle::from_path(path)?
    } else {
        let path = options
            .script
            .as_ref()
            .expect("bundle or script is required");
        WorkerBundle::single_script(
            options.version.clone(),
            &options.compatibility_date,
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("worker.js"),
            std::fs::read_to_string(path)?,
        )?
    };
    let bundle_sha256 = bundle.sha256()?;
    let version = bundle.worker_version.clone();
    let mut worker = WorkerVersionSandbox::initialize(
        bundle,
        &options.rootfs,
        &options.executor,
        options.scratch_mb,
        options.init_timeout,
    )?;
    let listener = TcpListener::bind(&options.bind)?;
    eprintln!(
        "workerd demo listening on http://{} (Worker {}, bundle {})",
        listener.local_addr()?,
        version.as_str(),
        bundle_sha256
    );
    let sequence = AtomicU64::new(1);
    for connection in listener.incoming() {
        let mut stream = match connection {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("accept failed: {error}");
                continue;
            }
        };
        let request_id = format!("http-{}", sequence.fetch_add(1, Ordering::Relaxed));
        if let Err(error) = serve(
            &mut stream,
            &mut worker,
            &version,
            request_id,
            options.request_timeout,
        ) {
            eprintln!("request failed: {error}");
            let _ = write_error(&mut stream, 500, "worker request failed");
        }
    }
    Ok(())
}

fn serve(
    stream: &mut TcpStream,
    worker: &mut WorkerVersionSandbox,
    version: &WorkerVersionId,
    request_id: String,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let request = match read_request(stream, request_id.clone()) {
        Ok(request) => request,
        Err(error) => {
            write_error(stream, 400, &error)?;
            return Ok(());
        }
    };
    let (result, profile) = worker.execute_profiled(version, request, timeout);
    eprintln!(
        "workerd request {request_id}: {}",
        serde_json::to_string(&profile)?
    );
    match result {
        Ok(response) => {
            let body = STANDARD.decode(response.body_base64)?;
            write!(
                stream,
                "HTTP/1.1 {} {}\r\n",
                response.status,
                reason(response.status)
            )?;
            for header in response.headers {
                if !header.name.eq_ignore_ascii_case("content-length")
                    && !header.name.eq_ignore_ascii_case("connection")
                {
                    write!(stream, "{}: {}\r\n", header.name, header.value)?;
                }
            }
            write!(
                stream,
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )?;
            stream.write_all(&body)?;
        }
        Err(hyperlight_unikraft::workerd::Error::Timeout) => {
            write_error(stream, 504, "Worker timed out")?
        }
        Err(error) => {
            eprintln!("Worker {request_id} failed: {error}");
            write_error(stream, 502, "Worker execution failed")?;
        }
    }
    Ok(())
}

fn read_request(stream: &mut TcpStream, request_id: String) -> Result<RequestEnvelope, String> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if bytes.len() > MAX_REQUEST_HEAD_BYTES {
            return Err("request headers exceed limit".into());
        }
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request headers".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = std::str::from_utf8(&bytes[..head_end])
        .map_err(|_| "headers are not UTF-8")?
        .to_owned();
    let mut lines = head[..head.len() - 4].split("\r\n");
    let mut request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?
        .split_ascii_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| "missing method".to_string())?;
    let target = request_line
        .next()
        .ok_or_else(|| "missing request target".to_string())?;
    if request_line.next() != Some("HTTP/1.1") || request_line.next().is_some() {
        return Err("only HTTP/1.1 is supported".into());
    }
    let mut headers = Vec::new();
    let mut host = None;
    let mut content_length = 0usize;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "malformed header".to_string())?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("host") {
            host = Some(value);
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse()
                .map_err(|_| "invalid Content-Length".to_string())?;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("Transfer-Encoding is not supported".into());
        }
        headers.push(Header {
            name: name.into(),
            value: value.into(),
        });
    }
    if content_length > MAX_BODY_BYTES {
        return Err("request body exceeds limit".into());
    }
    while bytes.len() - head_end < content_length {
        let count = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("connection closed before request body".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() - head_end > content_length {
            return Err("bytes after request body".into());
        }
    }
    if bytes.len() - head_end != content_length {
        return Err("bytes after request body".into());
    }
    let url = if target.starts_with("http://") || target.starts_with("https://") {
        target.into()
    } else {
        let host = host.ok_or_else(|| "missing Host header".to_string())?;
        format!("http://{host}{target}")
    };
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id,
        method: method.into(),
        url,
        headers,
        body_base64: STANDARD.encode(&bytes[head_end..]),
    };
    request.validate().map_err(|error| error.to_string())?;
    Ok(request)
}

fn write_error(stream: &mut TcpStream, status: u16, message: &str) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{message}",
        reason(status),
        message.len()
    )
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        504 => "Gateway Timeout",
        _ => "Worker Response",
    }
}
