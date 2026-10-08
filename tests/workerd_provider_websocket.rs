// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

use base64::{Engine, engine::general_purpose::STANDARD};
use hyperlight_unikraft::workerd::*;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tungstenite::Message;

#[test]
#[ignore = "requires qualified provider guest and native KVM"]
#[expect(
    clippy::result_large_err,
    reason = "Tungstenite requires its fixed HTTP handshake error response"
)]
fn real_tls_provider_duplex_scope_close_and_durable_quiescence() {
    let directory = tempfile::tempdir().unwrap();
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let certificate = cert.der().clone();
    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der())),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let receiver = std::thread::spawn(move || {
        for session in 0..4 {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let transport = rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(tls.clone())).unwrap(),
                stream,
            );
            let mut socket = tungstenite::accept_hdr(
                transport,
                |request: &tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri(), "/realtime?intent=transcription");
                    assert_eq!(
                        request.headers()["authorization"],
                        "Bearer test-only-provider"
                    );
                    Ok(response)
                },
            )
            .unwrap();
            let frame = socket.read().unwrap();
            assert_eq!(frame, Message::Binary(vec![42; 16384].into()));
            socket.send(frame).unwrap();
            let Message::Close(Some(close)) = socket.read().unwrap() else {
                panic!("expected a normal provider close frame");
            };
            assert_eq!(
                close.code,
                tungstenite::protocol::frame::coding::CloseCode::Normal
            );
            assert_eq!(
                close.reason.as_str(),
                if session % 2 == 0 { "complete" } else { "" }
            );
            match socket.flush() {
                Ok(()) | Err(tungstenite::Error::ConnectionClosed) => {}
                other => panic!("{other:?}"),
            }
        }
    });
    let key = directory.path().join("credential");
    std::fs::write(&key, b"Bearer test-only-provider").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let roots = directory.path().join("roots.pem");
    std::fs::write(&roots, cert.pem()).unwrap();
    let version = WorkerVersionId::new("provider-kvm-v1").unwrap();
    let mut bundle = WorkerBundle::single_script(version.clone(), "2025-01-01", "worker.js", format!(
        r#"export default {{async fetch(request,env) {{
 for(let session=0;session<2;session++) {{
 const ws=env.transcribe.connect('wss://127.0.0.1:{port}/realtime?intent=transcription');
 if(typeof ws.bufferedAmount!=='number'||ws.bufferedAmount!==0) throw new Error('non-numeric bufferedAmount');
 ws.binaryType='arraybuffer';
 await new Promise((resolve,reject)=>{{ws.addEventListener('open',resolve,{{once:true}});ws.addEventListener('error',()=>reject(new Error('provider open failed')),{{once:true}})}});
 const message=new Promise(resolve=>ws.addEventListener('message',resolve,{{once:true}}));
 ws.send(new Uint8Array(16384).fill(42));
 const event=await message;const bytes=new Uint8Array(event.data);
 if(bytes.length!==16384||bytes[0]!==42) throw new Error('provider bytes differ');
 const closed=new Promise(resolve=>ws.addEventListener('close',resolve,{{once:true}}));
 if(session===0) ws.close(1000,'complete'); else ws.close();
 const closeEvent=await closed;
 if(closeEvent.code!==1000||closeEvent.reason!==(session===0?'complete':'')) throw new Error('provider close differs');
 }}
 return new Response('provider-ok');
}}}};"#
    )).unwrap();
    bundle.protocol_version = PACKAGE_PROTOCOL_VERSION;
    let policy = WorkerCapabilityPolicy::default()
        .with_logical_bindings(
            LogicalBindingsPolicy::new(
                "provider-kvm",
                version.as_str(),
                vec![LogicalBindingConfig::ProviderWebsocket(Box::new(
                    ProviderWebSocketConfig {
                        name: "transcribe".into(),
                        hosts: vec!["127.0.0.1".into()],
                        ports: vec![port],
                        paths: vec!["/realtime".into()],
                        query_parameters: BTreeMap::from([(
                            "intent".into(),
                            vec!["transcription".into()],
                        )]),
                        subprotocols: vec![],
                        ip_ranges: vec!["127.0.0.0/8".parse().unwrap()],
                        resolver_identity: Ipv4Addr::LOCALHOST.into(),
                        allow_loopback: true,
                        allow_private: false,
                        credential: FetchCredential {
                            reference: "provider".into(),
                            value_file: key,
                            header: "authorization".into(),
                            scheme: "https".into(),
                            host: "127.0.0.1".into(),
                            port,
                        },
                        limits: ProviderWebSocketLimits {
                            max_connections: 4,
                            queue_frames: 4,
                            max_frame_bytes: 16384,
                            max_total_bytes: 8388608,
                            connect_timeout_ms: 5000,
                        },
                        session_policy: None,
                        trusted_ca_file: Some(roots),
                    },
                ))],
            )
            .unwrap(),
        )
        .unwrap();
    let executor =
        PathBuf::from(std::env::var("WORKERD_REAL_EXECUTOR").expect("set exact provider executor"));
    let rootfs = PathBuf::from(
        std::env::var("WORKERD_REAL_ROOTFS").expect("set exact matching provider rootfs"),
    );
    let worker = WorkerVersionSandbox::initialize_with_policy(
        bundle,
        rootfs,
        executor,
        512,
        Duration::from_secs(30),
        policy,
    )
    .unwrap();
    worker
        .negotiate_extensions(
            &["authenticated-egress-websocket-v1"],
            Duration::from_secs(10),
        )
        .unwrap();
    for mode in [false, true] {
        let request = RequestEnvelope {
            protocol_version: 1,
            request_id: format!("provider-{mode}"),
            method: "GET".into(),
            url: "https://example.test/".into(),
            headers: vec![],
            body_base64: String::new(),
        };
        let response = if mode {
            let mut resident = worker.restore_resident().unwrap();
            let response = resident
                .execute(request, Duration::from_secs(10))
                .0
                .unwrap();
            resident
                .checkpoint(&worker, "provider-closed", Duration::from_secs(5))
                .unwrap();
            response
        } else {
            worker
                .execute(&version, request, Duration::from_secs(10))
                .unwrap()
        };
        assert_eq!(response.status, 200);
        assert_eq!(
            STANDARD.decode(response.body_base64).unwrap(),
            b"provider-ok"
        );
    }
    receiver.join().unwrap();
}
